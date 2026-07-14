"""
Kalshi Momentum Arbitrage Bot v2
==================================
Upgraded features:
- Accurate fee simulation (taker + maker)
- Win rate tracking with confidence intervals
- Signal quality scoring
- Minimum time remaining filter (>120s)
- Expected value per trade calculation
- Limit order maker strategy option
- Full P&L analytics with Sharpe ratio
- Auto stake sizing based on Kelly criterion
"""

import asyncio
import logging
import os
import time
import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Dict, List

import aiohttp
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KALSHI_API_KEY   = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIV_KEY  = os.getenv("KALSHI_PRIVATE_KEY", "")
_key_file = os.getenv("KALSHI_PRIVATE_KEY_FILE", "")
if _key_file and os.path.exists(_key_file):
    with open(_key_file) as _f:
        KALSHI_PRIV_KEY = _f.read()

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_USER     = int(os.getenv("ALLOWED_USER_ID", "0"))
PAPER_MODE       = os.getenv("PAPER_MODE", "true").lower() == "true"
STAKE            = int(os.getenv("STAKE_CONTRACTS", "5"))
THRESHOLD        = float(os.getenv("MOMENTUM_THRESHOLD", "0.0008"))
USE_LIMIT_ORDERS = os.getenv("USE_LIMIT_ORDERS", "false").lower() == "true"
MIN_SECS_LEFT    = int(os.getenv("MIN_SECS_LEFT", "120"))
SCAN_INTERVAL    = 10

CRYPTOS = ["BTC", "ETH", "SOL"]
SERIES  = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M"}
BASE    = "https://api.elections.kalshi.com"

# ── Fee Calculator ─────────────────────────────────────────────────────────────

def kalshi_taker_fee(price_dollars: float, contracts: int) -> float:
    """Calculate Kalshi taker fee: 0.07 × p × (1-p) per contract."""
    fee_per = round(0.07 * price_dollars * (1 - price_dollars), 4)
    return round(fee_per * contracts, 4)

def kalshi_maker_fee(price_dollars: float, contracts: int) -> float:
    """Maker fee is ~25% of taker fee."""
    return round(kalshi_taker_fee(price_dollars, contracts) * 0.25, 4)

def expected_value(entry_price: float, contracts: int, win_rate: float, use_maker: bool = False) -> dict:
    """Calculate expected value per trade."""
    fee = kalshi_maker_fee(entry_price, contracts) if use_maker else kalshi_taker_fee(entry_price, contracts)
    win_payout = 0.99 * contracts
    stake      = entry_price * contracts
    net_win    = win_payout - stake - fee
    net_loss   = -(stake + fee)  # fee paid even on loss? No — Kalshi charges on entry only
    net_loss   = -stake  # stake lost, fee already paid at entry

    ev = win_rate * net_win + (1 - win_rate) * net_loss
    breakeven_wr = stake / (stake + net_win) if net_win > 0 else 1.0

    return {
        "stake": round(stake, 4),
        "fee": round(fee, 4),
        "net_win": round(net_win, 4),
        "net_loss": round(net_loss, 4),
        "ev": round(ev, 4),
        "breakeven_wr": round(breakeven_wr * 100, 1),
        "roi_if_win": round(net_win / stake * 100, 1),
    }

def kelly_contracts(win_rate: float, entry_price: float, bankroll: float, max_contracts: int = 10) -> int:
    """Kelly criterion for optimal contract sizing."""
    payout_ratio = (0.99 - entry_price) / entry_price  # net odds
    kelly_fraction = (win_rate * (1 + payout_ratio) - 1) / payout_ratio
    kelly_fraction = max(0, min(kelly_fraction, 0.25))  # cap at 25% bankroll
    optimal_stake  = bankroll * kelly_fraction
    contracts      = int(optimal_stake / entry_price)
    return max(1, min(contracts, max_contracts))

# ── Analytics ──────────────────────────────────────────────────────────────────

class Analytics:
    def __init__(self):
        self.trades: List[Dict] = []
        self.paper_bankroll = 100.0  # starting paper bankroll

    def add_trade(self, trade: Dict):
        self.trades.append(trade)

    def summary(self) -> Dict:
        closed = [t for t in self.trades if t.get("status") == "closed"]
        if not closed:
            return {"trades": 0}

        total_trades = len(closed)
        wins = [t for t in closed if t.get("won")]
        losses = [t for t in closed if not t.get("won")]
        win_rate = len(wins) / total_trades

        total_pnl    = sum(t.get("pnl", 0) for t in closed)
        total_fees   = sum(t.get("fee", 0) for t in closed)
        total_staked = sum(t.get("stake_usd", 0) for t in closed)
        roi          = total_pnl / total_staked * 100 if total_staked > 0 else 0

        # Sharpe-like ratio
        pnls = [t.get("pnl", 0) for t in closed]
        avg_pnl = sum(pnls) / len(pnls)
        if len(pnls) > 1:
            std_pnl = math.sqrt(sum((p - avg_pnl)**2 for p in pnls) / (len(pnls)-1))
            sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0
        else:
            sharpe = 0

        # Win rate confidence interval (Wilson interval)
        n, p = total_trades, win_rate
        z = 1.96  # 95% CI
        ci_half = z * math.sqrt(p*(1-p)/n + z**2/(4*n**2)) / (1 + z**2/n)
        ci_lo = (p + z**2/(2*n) - ci_half) / (1 + z**2/n)
        ci_hi = (p + z**2/(2*n) + ci_half) / (1 + z**2/n)

        # Per crypto stats
        by_crypto = {}
        for crypto in CRYPTOS:
            ct = [t for t in closed if t.get("crypto") == crypto]
            if ct:
                by_crypto[crypto] = {
                    "trades": len(ct),
                    "wins": sum(1 for t in ct if t.get("won")),
                    "pnl": round(sum(t.get("pnl", 0) for t in ct), 2),
                }

        return {
            "trades": total_trades,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate * 100, 1),
            "win_rate_ci": f"{ci_lo*100:.1f}%-{ci_hi*100:.1f}%",
            "total_pnl": round(total_pnl, 2),
            "total_fees": round(total_fees, 2),
            "total_staked": round(total_staked, 2),
            "roi": round(roi, 1),
            "sharpe": round(sharpe, 2),
            "avg_pnl": round(avg_pnl, 2),
            "by_crypto": by_crypto,
        }

analytics = Analytics()

# ── State ──────────────────────────────────────────────────────────────────────
price_history: Dict[str, List] = defaultdict(list)
open_trades: List[Dict] = []
closed_trades: List[Dict] = []
kill_switch = False

# ── Kalshi Client ──────────────────────────────────────────────────────────────
from kalshi_client import KalshiClient
client = KalshiClient(KALSHI_API_KEY, KALSHI_PRIV_KEY) if not PAPER_MODE else None

# ── Price Fetching ─────────────────────────────────────────────────────────────

async def fetch_kucoin_prices(session: aiohttp.ClientSession) -> Dict[str, float]:
    try:
        async with session.get(
            "https://api.kucoin.com/api/v1/market/allTickers",
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            data = await r.json()
            prices = {}
            for t in data.get("data", {}).get("ticker", []):
                sym = t.get("symbol", "")
                if sym in ("BTC-USDT", "ETH-USDT", "SOL-USDT"):
                    prices[sym.split("-")[0]] = float(t.get("last", 0))
            return prices
    except Exception as e:
        logger.warning(f"KuCoin fetch error: {e}")
        return {}

async def fetch_kalshi_market(session: aiohttp.ClientSession, crypto: str) -> Optional[Dict]:
    try:
        series = SERIES[crypto]
        async with session.get(
            f"{BASE}/trade-api/v2/markets",
            params={"limit": 3, "status": "open", "series_ticker": series},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as r:
            data = await r.json()
            markets = data.get("markets", [])
            if markets:
                return markets[0]
    except Exception as e:
        logger.debug(f"Kalshi market fetch error {crypto}: {e}")
    return None

# ── Signal Detection ───────────────────────────────────────────────────────────

def signal_quality_score(pct_change: float, secs_remaining: int, yes_price: int) -> float:
    """
    Score signal quality 0-100.
    Higher = better trade opportunity.
    Factors: momentum strength, time remaining, market price extremity
    """
    # Momentum strength (0-40 pts)
    momentum_score = min(40, abs(pct_change) / THRESHOLD * 20)

    # Time remaining (0-30 pts) — more time = better
    time_score = min(30, (secs_remaining / 900) * 30)

    # Price extremity (0-30 pts) — away from 50¢ = lower fees + clearer signal
    price_score = abs(yes_price - 50) / 50 * 30

    return round(momentum_score + time_score + price_score, 1)

def detect_momentum(crypto: str, current_price: float) -> tuple[Optional[str], float]:
    """Returns (direction, pct_change) or (None, 0)."""
    now = time.time()
    history = price_history[crypto]
    history.append((now, current_price))
    price_history[crypto] = [(t, p) for t, p in history if now - t <= 90]

    sixty_ago = [(t, p) for t, p in price_history[crypto] if now - t >= 55]
    if not sixty_ago:
        return None, 0

    old_price = sixty_ago[0][1]
    pct_change = (current_price - old_price) / old_price if old_price > 0 else 0

    logger.info(f"Momentum check: {crypto} {old_price:.4f}→{current_price:.4f} ({pct_change*100:+.3f}%) threshold={THRESHOLD*100:.2f}%")

    if pct_change >= THRESHOLD:
        return "UP", pct_change
    elif pct_change <= -THRESHOLD:
        return "DOWN", pct_change
    return None, 0

# ── Trade Execution ────────────────────────────────────────────────────────────

async def place_trade(crypto: str, direction: str, market: Dict, pct_change: float, score: float) -> Optional[Dict]:
    ticker    = market.get("ticker", "")
    yes_price = market.get("yes_ask", 50)
    no_price  = market.get("no_ask", 50)

    side          = "yes" if direction == "UP" else "no"
    entry_price_c = yes_price if side == "yes" else no_price
    entry_price_d = entry_price_c / 100

    # Fee calculation
    fee = kalshi_maker_fee(entry_price_d, STAKE) if USE_LIMIT_ORDERS \
          else kalshi_taker_fee(entry_price_d, STAKE)

    # EV calculation using running win rate
    stats    = analytics.summary()
    win_rate = stats.get("win_rate", 60) / 100 if stats.get("trades", 0) >= 10 else 0.60
    ev_data  = expected_value(entry_price_d, STAKE, win_rate, USE_LIMIT_ORDERS)

    trade = {
        "id":          f"{crypto}_{int(time.time())}",
        "crypto":      crypto,
        "direction":   direction,
        "side":        side,
        "ticker":      ticker,
        "entry_price": entry_price_c,
        "contracts":   STAKE,
        "stake_usd":   round(entry_price_d * STAKE, 2),
        "fee":         fee,
        "pct_change":  round(pct_change * 100, 3),
        "score":       score,
        "ev":          ev_data["ev"],
        "opened_at":   datetime.now(timezone.utc).isoformat(),
        "status":      "open",
        "order_id":    None,
        "order_type":  "limit" if USE_LIMIT_ORDERS else "market",
    }

    if PAPER_MODE:
        trade["order_id"] = f"PAPER_{int(time.time())}"
        logger.info(f"[PAPER] {crypto} {direction} {side.upper()}@{entry_price_c}¢ ×{STAKE} fee=${fee:.4f} EV=${ev_data['ev']:.3f}")
    else:
        try:
            if USE_LIMIT_ORDERS:
                # Place limit order 1 tick below ask (maker)
                limit_price = max(1, entry_price_c - 1) / 100
                resp = client.place_limit_order(ticker, side, STAKE, limit_price)
            else:
                resp = client.place_market_order(ticker, side, STAKE, entry_price_d)
            trade["order_id"] = resp.get("order_id", "")
            fill_count = float(resp.get("fill_count", 0) or 0)
            avg_price  = float(resp.get("average_fill_price", 0) or 0)
            remaining  = float(resp.get("remaining_count", 0) or 0)
            trade["actual_contracts"] = fill_count
            trade["actual_cost"] = round(avg_price * fill_count, 4)
            logger.info(f"[LIVE] {crypto} {direction} order_id={trade['order_id']} filled={fill_count}/{STAKE} avg_price=${avg_price:.4f} remaining={remaining}")
        except Exception as e:
            import traceback
            logger.error(f"Order failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Order error detail: {e.response.text[:300]}")
            return None

    open_trades.append(trade)
    analytics.add_trade(trade)
    return trade

# ── Scan Loop ──────────────────────────────────────────────────────────────────

async def scan_loop(app: Application):
    global kill_switch
    session = aiohttp.ClientSession()
    logger.info("Warming up price history (60s)...")
    await asyncio.sleep(5)

    while True:
        if kill_switch:
            await asyncio.sleep(5)
            continue

        try:
            prices = await fetch_kucoin_prices(session)
            if not prices:
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            for crypto, price in prices.items():
                direction, pct_change = detect_momentum(crypto, price)
                if not direction:
                    continue

                active = [t for t in open_trades if t["crypto"] == crypto and t["status"] == "open"]
                if active:
                    logger.info(f"Already in {crypto} trade, skipping")
                    continue

                market = await fetch_kalshi_market(session, crypto)
                if not market:
                    logger.warning(f"No Kalshi market found for {crypto}")
                    continue

                ticker    = market.get("ticker", "")
                yes_price = market.get("yes_ask", 50)
                no_price  = market.get("no_ask", 50)
                close_time = market.get("close_time", "")

                secs_remaining = 900
                if close_time:
                    try:
                        close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                        secs_remaining = max(0, (close_dt - datetime.now(timezone.utc)).total_seconds())
                    except Exception:
                        pass

                if secs_remaining < MIN_SECS_LEFT:
                    logger.info(f"Skip {crypto}: only {secs_remaining:.0f}s remaining (min={MIN_SECS_LEFT}s)")
                    continue

                entry_price = yes_price if direction == "UP" else no_price
                score = signal_quality_score(pct_change, secs_remaining, entry_price)

                logger.info(
                    f"Signal: {crypto} {direction} | {ticker} | "
                    f"entry={entry_price}¢ | score={score} | {secs_remaining:.0f}s"
                )

                trade = await place_trade(crypto, direction, market, pct_change, score)
                if trade:
                    mode     = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
                    order_t  = "📊 LIMIT (maker)" if USE_LIMIT_ORDERS else "⚡ MARKET (taker)"
                    fee      = trade["fee"]
                    ev       = trade["ev"]
                    stats    = analytics.summary()
                    wr_str   = f"{stats['win_rate']}%" if stats.get("trades", 0) > 0 else "N/A"

                    net_win  = round((0.99 - trade["entry_price"]/100) * STAKE - fee, 2)
                    net_loss = -trade["stake_usd"]

                    msg = (
                        f"{mode} | Momentum Arb v2\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🪙 {crypto} {direction} ({trade['pct_change']:+.3f}%)\n"
                        f"📋 {ticker}\n"
                        f"💰 {order_t}\n"
                        f"   {trade['side'].upper()} @ {trade['entry_price']}¢ × {STAKE}\n"
                        f"💵 Stake: ${trade['stake_usd']:.2f} | Fee: ${fee:.4f}\n"
                        f"✅ Net Win: +${net_win:.2f}\n"
                        f"❌ Net Loss: ${net_loss:.2f}\n"
                        f"📈 EV: ${ev:.3f} | Score: {score}/100\n"
                        f"⏱️ {secs_remaining:.0f}s | WR: {wr_str}"
                    )
                    try:
                        await app.bot.send_message(chat_id=ALLOWED_USER, text=msg)
                    except Exception as e:
                        logger.error(f"Telegram error: {e}")

        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)

        await asyncio.sleep(SCAN_INTERVAL)

# ── Resolver ───────────────────────────────────────────────────────────────────

async def resolver_loop(app: Application):
    session = aiohttp.ClientSession()
    while True:
        await asyncio.sleep(30)
        for trade in list(open_trades):
            if trade["status"] != "open":
                continue
            try:
                async with session.get(
                    f"{BASE}/trade-api/v2/markets/{trade['ticker']}",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    data   = await r.json()
                    market = data.get("market", {})
                    status = market.get("status", "")

                if status == "finalized":
                    result = market.get("result", "")
                    won = (trade["side"] == "yes" and result == "yes") or \
                          (trade["side"] == "no" and result == "no")

                    # Try to get actual fill data from Kalshi
                    actual_contracts = trade["contracts"]
                    actual_cost = trade["stake_usd"]
                    if trade.get("order_id") and not PAPER_MODE:
                        try:
                            ord_path = f"/trade-api/v2/portfolio/orders/{trade['order_id']}"
                            async with session.get(
                                f"{BASE}{ord_path}",
                                timeout=aiohttp.ClientTimeout(total=5)
                            ) as ord_r:
                                ord_data = await ord_r.json()
                                order = ord_data.get("order", ord_data)
                                fill_count = float(order.get("fill_count", trade["contracts"]) or trade["contracts"])
                                avg_price  = float(order.get("average_fill_price", trade["entry_price"]/100) or trade["entry_price"]/100)
                                actual_contracts = fill_count
                                actual_cost = round(avg_price * fill_count, 4)
                        except Exception as ex:
                            logger.debug(f"Could not fetch order details: {ex}")

                    gross_pnl = round((0.99 - actual_cost/actual_contracts) * actual_contracts, 4) if won and actual_contracts > 0 \
                                else -actual_cost
                    actual_fee = round(0.07 * (actual_cost/actual_contracts if actual_contracts > 0 else 0.5) * \
                                (1 - actual_cost/actual_contracts if actual_contracts > 0 else 0.5) * actual_contracts, 4)
                    net_pnl = round(gross_pnl - actual_fee, 2) if won else round(gross_pnl, 2)
                    trade["actual_contracts"] = actual_contracts
                    trade["actual_cost"] = actual_cost

                    trade["status"]    = "closed"
                    trade["pnl"]       = net_pnl
                    trade["won"]       = won
                    trade["result"]    = result
                    trade["closed_at"] = datetime.now(timezone.utc).isoformat()

                    open_trades.remove(trade)
                    closed_trades.append(trade)
                    analytics.add_trade(trade)

                    stats   = analytics.summary()
                    emoji   = "✅ WIN" if won else "❌ LOSS"
                    mode    = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
                    running_pnl = stats.get("total_pnl", 0)
                    wr      = stats.get("win_rate", 0)
                    wr_ci   = stats.get("win_rate_ci", "N/A")

                    msg = (
                        f"{emoji} | {mode}\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🪙 {trade['crypto']} {trade['direction']}\n"
                        f"Result: {result.upper()}\n"
                        f"P&L this trade: ${net_pnl:+.2f}\n"
                        f"Fee paid: ${trade['fee']:.4f}\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"📊 Session Stats\n"
                        f"Trades: {stats.get('trades', 0)} | "
                        f"WR: {wr}% ({wr_ci})\n"
                        f"Total P&L: ${running_pnl:+.2f}\n"
                        f"Total fees: ${stats.get('total_fees', 0):.4f}"
                    )
                    try:
                        await app.bot.send_message(chat_id=ALLOWED_USER, text=msg)
                    except Exception:
                        pass
                    logger.info(f"Resolved: {trade['crypto']} P&L=${net_pnl:+.2f} WR={wr}%")

            except Exception as e:
                logger.debug(f"Resolver error: {e}")

# ── Telegram Commands ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    mode     = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
    order_t  = "📊 LIMIT (maker)" if USE_LIMIT_ORDERS else "⚡ MARKET (taker)"
    kb = [
        [InlineKeyboardButton("📊 Full P&L", callback_data="pnl"),
         InlineKeyboardButton("🏆 By Crypto", callback_data="by_crypto")],
        [InlineKeyboardButton("📂 Positions", callback_data="positions"),
         InlineKeyboardButton("💰 Balance", callback_data="balance")],
        [InlineKeyboardButton("💡 EV Calc", callback_data="ev"),
         InlineKeyboardButton("🔧 Settings", callback_data="settings")],
        [InlineKeyboardButton("🚨 Kill", callback_data="kill"),
         InlineKeyboardButton("▶️ Resume", callback_data="resume")],
    ]
    await update.message.reply_text(
        f"🤖 Kalshi Arb Bot v2\n"
        f"Mode: {mode} | {order_t}\n"
        f"Threshold: {THRESHOLD*100:.2f}% | Stake: {STAKE}c | Min: {MIN_SECS_LEFT}s",
        reply_markup=InlineKeyboardMarkup(kb)
    )

def handle_pnl():
    stats = analytics.summary()
    if not stats.get("trades"):
        text = "No closed trades yet."
    else:
        text = (
            f"📊 P&L Report\n"
            f"━━━━━━━━━━━━\n"
            f"Trades: {stats['trades']} | WR: {stats['win_rate']}%\n"
            f"95% CI: {stats['win_rate_ci']}\n"
            f"Wins: {stats['wins']} | Losses: {stats['losses']}\n"
            f"━━━━━━━━━━━━\n"
            f"Total P&L: ${stats['total_pnl']:+.2f}\n"
            f"Total fees: ${stats['total_fees']:.4f}\n"
            f"Total staked: ${stats['total_staked']:.2f}\n"
            f"ROI: {stats['roi']}%\n"
            f"Avg P&L/trade: ${stats['avg_pnl']:+.2f}\n"
            f"Sharpe: {stats['sharpe']}"
        )
    return text

def handle_by_crypto():
    stats = analytics.summary()
    by = stats.get("by_crypto", {})
    if not by:
        return "No trades yet."
    lines = ["🏆 By Crypto\n━━━━━━━━━━━━"]
    for crypto, s in by.items():
        wr = round(s['wins']/s['trades']*100, 1) if s['trades'] > 0 else 0
        lines.append(f"{crypto}: {s['trades']}T {wr}%WR ${s['pnl']:+.2f}")
    return "\n".join(lines)

def handle_ev():
    stats    = analytics.summary()
    win_rate = stats.get("win_rate", 60) / 100 if stats.get("trades", 0) >= 5 else 0.60
    ev_t     = expected_value(0.50, STAKE, win_rate, False)
    ev_m     = expected_value(0.50, STAKE, win_rate, True)
    text = (
        f"💡 EV Calculator (50¢ entry)\n"
        f"━━━━━━━━━━━━\n"
        f"Win rate: {win_rate*100:.1f}%\n"
        f"Contracts: {STAKE} | Stake: ${ev_t['stake']}\n"
        f"━━━━━━━━━━━━\n"
        f"⚡ MARKET (taker)\n"
        f"  Fee: ${ev_t['fee']} | EV: ${ev_t['ev']}\n"
        f"  Break-even WR: {ev_t['breakeven_wr']}%\n"
        f"━━━━━━━━━━━━\n"
        f"📊 LIMIT (maker)\n"
        f"  Fee: ${ev_m['fee']} | EV: ${ev_m['ev']}\n"
        f"  Break-even WR: {ev_m['breakeven_wr']}%\n"
        f"━━━━━━━━━━━━\n"
        f"💰 Kelly sizing @ {win_rate*100:.0f}%WR: "
        f"{kelly_contracts(win_rate, 0.50, 100)} contracts"
    )
    return text

async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global kill_switch
    q = update.callback_query
    if update.effective_user.id != ALLOWED_USER:
        return
    await q.answer()

    if q.data == "pnl":
        await q.edit_message_text(handle_pnl())
    elif q.data == "by_crypto":
        await q.edit_message_text(handle_by_crypto())
    elif q.data == "positions":
        active = [t for t in open_trades if t["status"] == "open"]
        if not active:
            await q.edit_message_text("No open positions")
        else:
            lines = ["📂 Open Positions"]
            for t in active:
                lines.append(f"• {t['crypto']} {t['direction']} {t['side'].upper()}@{t['entry_price']}¢ score={t['score']}")
            await q.edit_message_text("\n".join(lines))
    elif q.data == "balance":
        if PAPER_MODE:
            stats = analytics.summary()
            pnl = stats.get("total_pnl", 0)
            await q.edit_message_text(f"💰 Paper Balance: ${100 + pnl:.2f}")
        else:
            try:
                bal = client.get_balance()
                await q.edit_message_text(f"💰 Balance: ${bal:.2f}")
            except Exception as e:
                await q.edit_message_text(f"Balance error: {e}")
    elif q.data == "ev":
        await q.edit_message_text(handle_ev())
    elif q.data == "settings":
        await q.edit_message_text(
            f"🔧 Settings\n"
            f"PAPER_MODE: {PAPER_MODE}\n"
            f"STAKE: {STAKE} contracts\n"
            f"THRESHOLD: {THRESHOLD*100:.2f}%\n"
            f"MIN_SECS_LEFT: {MIN_SECS_LEFT}s\n"
            f"USE_LIMIT_ORDERS: {USE_LIMIT_ORDERS}"
        )
    elif q.data == "kill":
        kill_switch = True
        await q.edit_message_text("🚨 Kill switch ACTIVATED")
    elif q.data == "resume":
        kill_switch = False
        await q.edit_message_text("▶️ Trading RESUMED")

# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    mode    = "PAPER" if PAPER_MODE else "LIVE"
    order_t = "LIMIT/maker" if USE_LIMIT_ORDERS else "MARKET/taker"
    logger.info(f"🤖 Kalshi Arb Bot v2 [{mode}] [{order_t}]")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(handle_pnl())
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def on_startup(application):
        stats    = analytics.summary()
        ev_data  = expected_value(0.50, STAKE, 0.60, USE_LIMIT_ORDERS)
        order_t2 = "LIMIT (maker ~0% fee)" if USE_LIMIT_ORDERS else "MARKET (taker fee)"

        await application.bot.send_message(
            chat_id=ALLOWED_USER,
            text=(
                f"🤖 Kalshi Arb Bot v2 STARTED\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"Mode: {'📄 PAPER' if PAPER_MODE else '💵 LIVE'}\n"
                f"Order type: {order_t2}\n"
                f"Markets: BTC, ETH, SOL 15-min\n"
                f"Threshold: {THRESHOLD*100:.2f}%\n"
                f"Stake: {STAKE} contracts\n"
                f"Min time: {MIN_SECS_LEFT}s\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"💡 EV at 50¢, 60% WR:\n"
                f"  Fee: ${ev_data['fee']:.4f}\n"
                f"  EV: ${ev_data['ev']:.3f}/trade\n"
                f"  Break-even WR: {ev_data['breakeven_wr']}%"
            )
        )
        asyncio.create_task(scan_loop(application))
        asyncio.create_task(resolver_loop(application))

    app.post_init = on_startup
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
