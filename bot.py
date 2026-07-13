"""
Kalshi Momentum Arbitrage Bot
================================
Detects momentum on BTC/ETH/SOL spot (KuCoin) and trades the lagging
Kalshi 15-minute prediction markets.

Strategy:
  - Scan KuCoin for price moves > THRESHOLD% in 60 seconds
  - Find the corresponding Kalshi 15-min market
  - If market price lags the momentum signal → buy YES or NO
  - Contracts pay $0.99 per contract if correct, $0 if wrong

Environment variables (.env):
  KALSHI_API_KEY          - Kalshi API key ID
  KALSHI_PRIVATE_KEY      - RSA private key (PEM, full block)
  TELEGRAM_BOT_TOKEN      - Telegram bot token
  ALLOWED_USER_ID         - Your Telegram user ID
  PAPER_MODE              - true/false
  STAKE_CONTRACTS         - number of contracts per trade (default 5)
  MOMENTUM_THRESHOLD      - % move to trigger signal (default 0.08)
"""

import asyncio
import logging
import os
import time
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
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_USER     = int(os.getenv("ALLOWED_USER_ID", "0"))
PAPER_MODE       = os.getenv("PAPER_MODE", "true").lower() == "true"
STAKE            = int(os.getenv("STAKE_CONTRACTS", "5"))
THRESHOLD        = float(os.getenv("MOMENTUM_THRESHOLD", "0.0008"))
SCAN_INTERVAL    = 10  # seconds

CRYPTOS = ["BTC", "ETH", "SOL"]
SERIES  = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M"}

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
                    crypto = sym.split("-")[0]
                    prices[crypto] = float(t.get("last", 0))
            return prices
    except Exception as e:
        logger.warning(f"KuCoin fetch error: {e}")
        return {}


async def fetch_kalshi_market(session: aiohttp.ClientSession, crypto: str) -> Optional[Dict]:
    """Get the current active 15-min Kalshi market for a crypto."""
    try:
        series = SERIES[crypto]
        r = await session.get(
            f"https://api.elections.kalshi.com/trade-api/v2/markets",
            params={"limit": 3, "status": "open", "series_ticker": series},
            timeout=aiohttp.ClientTimeout(total=5)
        )
        data = await r.json()
        markets = data.get("markets", [])
        if markets:
            return markets[0]
    except Exception as e:
        logger.debug(f"Kalshi market fetch error {crypto}: {e}")
    return None


# ── Signal Detection ───────────────────────────────────────────────────────────

def detect_momentum(crypto: str, current_price: float) -> Optional[str]:
    """Returns 'UP', 'DOWN', or None."""
    now = time.time()
    history = price_history[crypto]
    history.append((now, current_price))
    # Keep last 90 seconds
    price_history[crypto] = [(t, p) for t, p in history if now - t <= 90]

    sixty_ago = [(t, p) for t, p in price_history[crypto] if now - t >= 55]
    if not sixty_ago:
        return None

    old_price = sixty_ago[0][1]
    pct_change = (current_price - old_price) / old_price if old_price > 0 else 0

    logger.info(f"Momentum check: {crypto} {old_price:.4f}→{current_price:.4f} ({pct_change*100:+.3f}%) threshold={THRESHOLD*100:.2f}%")

    if pct_change >= THRESHOLD:
        return "UP"
    elif pct_change <= -THRESHOLD:
        return "DOWN"
    return None


# ── Trade Execution ────────────────────────────────────────────────────────────

async def place_trade(crypto: str, direction: str, market: Dict) -> Optional[Dict]:
    """Place a trade on Kalshi. direction = 'UP' or 'DOWN'."""
    ticker    = market.get("ticker", "")
    yes_price = market.get("yes_ask", 50)   # cents
    no_price  = market.get("no_ask", 50)

    # UP momentum → market should go UP → buy YES
    # DOWN momentum → market should go DOWN → buy NO
    side = "yes" if direction == "UP" else "no"
    entry_price = yes_price if side == "yes" else no_price

    trade = {
        "id":          f"{crypto}_{int(time.time())}",
        "crypto":      crypto,
        "direction":   direction,
        "side":        side,
        "ticker":      ticker,
        "entry_price": entry_price,
        "contracts":   STAKE,
        "stake_usd":   round(entry_price * STAKE / 100, 2),
        "opened_at":   datetime.now(timezone.utc).isoformat(),
        "status":      "open",
        "order_id":    None,
    }

    if PAPER_MODE:
        trade["order_id"] = f"PAPER_{int(time.time())}"
        logger.info(f"[PAPER] Trade: {crypto} {direction} {side.upper()}@{entry_price}¢ x{STAKE}")
    else:
        try:
            resp = client.place_market_order(ticker, side, STAKE)
            trade["order_id"] = resp.get("order", {}).get("order_id", "")
            logger.info(f"[LIVE] Trade placed: {crypto} {direction} order_id={trade['order_id']}")
        except Exception as e:
            logger.error(f"Order failed: {e}")
            return None

    open_trades.append(trade)
    return trade


# ── Scan Loop ──────────────────────────────────────────────────────────────────

async def scan_loop(app: Application):
    """Main scanning loop."""
    global kill_switch
    session = aiohttp.ClientSession()

    # Warm up price history
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
                direction = detect_momentum(crypto, price)
                if not direction:
                    continue

                # Check if already in a trade for this crypto
                active = [t for t in open_trades if t["crypto"] == crypto and t["status"] == "open"]
                if active:
                    logger.info(f"Already in {crypto} trade, skipping")
                    continue

                # Get Kalshi market
                market = await fetch_kalshi_market(session, crypto)
                if not market:
                    logger.warning(f"No Kalshi market found for {crypto}")
                    continue

                ticker    = market.get("ticker", "")
                yes_price = market.get("yes_ask", 50)
                no_price  = market.get("no_ask", 50)
                close_time = market.get("close_time", "")

                # Calculate time remaining
                secs_remaining = 0
                if close_time:
                    try:
                        close_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                        secs_remaining = (close_dt - datetime.now(timezone.utc)).total_seconds()
                    except Exception:
                        pass

                if secs_remaining < 60:
                    logger.info(f"Skip {crypto}: only {secs_remaining:.0f}s remaining")
                    continue

                logger.info(f"Signal: {crypto} {direction} | Market {ticker} YES={yes_price}¢ NO={no_price}¢ | {secs_remaining:.0f}s left")

                trade = await place_trade(crypto, direction, market)
                if trade:
                    mode = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
                    side = trade["side"].upper()
                    entry = trade["entry_price"]
                    pnl_if_win = round((99 - entry) * STAKE / 100, 2)
                    pnl_if_lose = round(-entry * STAKE / 100, 2)

                    msg = (
                        f"{mode} | Momentum Arb\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🪙 {crypto} {direction}\n"
                        f"📋 {ticker}\n"
                        f"💰 Buy {side} @ {entry}¢ × {STAKE} contracts\n"
                        f"💵 Stake: ${trade['stake_usd']:.2f}\n"
                        f"✅ Win: +${pnl_if_win:.2f} | ❌ Lose: ${pnl_if_lose:.2f}\n"
                        f"⏱️ {secs_remaining:.0f}s remaining"
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
    """Check if open trades have resolved."""
    session = aiohttp.ClientSession()
    while True:
        await asyncio.sleep(30)
        for trade in list(open_trades):
            if trade["status"] != "open":
                continue
            try:
                r = await session.get(
                    f"https://api.elections.kalshi.com/trade-api/v2/markets/{trade['ticker']}",
                    timeout=aiohttp.ClientTimeout(total=5)
                )
                data = await r.json()
                market = data.get("market", {})
                status = market.get("status", "")

                if status == "finalized":
                    result = market.get("result", "")
                    won = (trade["side"] == "yes" and result == "yes") or \
                          (trade["side"] == "no" and result == "no")
                    pnl = round((99 - trade["entry_price"]) * trade["contracts"] / 100, 2) if won \
                          else round(-trade["entry_price"] * trade["contracts"] / 100, 2)

                    trade["status"]   = "closed"
                    trade["pnl"]      = pnl
                    trade["won"]      = won
                    trade["closed_at"] = datetime.now(timezone.utc).isoformat()
                    open_trades.remove(trade)
                    closed_trades.append(trade)

                    emoji = "✅" if won else "❌"
                    mode  = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
                    msg = (
                        f"{emoji} Trade Resolved | {mode}\n"
                        f"━━━━━━━━━━━━━━━━\n"
                        f"🪙 {trade['crypto']} {trade['direction']}\n"
                        f"Result: {result.upper()}\n"
                        f"P&L: ${pnl:+.2f}"
                    )
                    try:
                        await app.bot.send_message(chat_id=ALLOWED_USER, text=msg)
                    except Exception:
                        pass
                    logger.info(f"Trade resolved: {trade['crypto']} P&L=${pnl:+.2f}")

            except Exception as e:
                logger.debug(f"Resolver error for {trade['ticker']}: {e}")


# ── Telegram Commands ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    mode = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
    kb = [
        [InlineKeyboardButton("📊 P&L", callback_data="pnl"),
         InlineKeyboardButton("📂 Positions", callback_data="positions")],
        [InlineKeyboardButton("💰 Balance", callback_data="balance"),
         InlineKeyboardButton("🚨 Kill Switch", callback_data="kill")],
        [InlineKeyboardButton("▶️ Resume", callback_data="resume")],
    ]
    await update.message.reply_text(
        f"🤖 Kalshi Momentum Bot\nMode: {mode}\nThreshold: {THRESHOLD*100:.2f}%\nStake: {STAKE} contracts",
        reply_markup=InlineKeyboardMarkup(kb)
    )


async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    total = sum(t.get("pnl", 0) for t in closed_trades)
    wins  = sum(1 for t in closed_trades if t.get("won"))
    total_trades = len(closed_trades)
    wr = f"{wins/total_trades*100:.1f}%" if total_trades > 0 else "N/A"
    msg = (
        f"📊 P&L Summary\n"
        f"━━━━━━━━━━━━\n"
        f"Total: ${total:+.2f}\n"
        f"Trades: {total_trades} | WR: {wr}\n"
        f"Open: {len([t for t in open_trades if t['status']=='open'])}"
    )
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(msg)
    else:
        await update.message.reply_text(msg)


async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global kill_switch
    q = update.callback_query
    if update.effective_user.id != ALLOWED_USER:
        return
    await q.answer()

    if q.data == "pnl":
        await cmd_pnl(update, ctx)
    elif q.data == "positions":
        active = [t for t in open_trades if t["status"] == "open"]
        if not active:
            await q.edit_message_text("No open positions")
        else:
            lines = []
            for t in active:
                lines.append(f"• {t['crypto']} {t['direction']} {t['side'].upper()}@{t['entry_price']}¢")
            await q.edit_message_text("📂 Open Positions\n" + "\n".join(lines))
    elif q.data == "balance":
        if PAPER_MODE:
            await q.edit_message_text("💰 Balance: PAPER MODE")
        else:
            try:
                bal = client.get_balance()
                await q.edit_message_text(f"💰 Balance: ${bal:.2f}")
            except Exception as e:
                await q.edit_message_text(f"Balance error: {e}")
    elif q.data == "kill":
        kill_switch = True
        await q.edit_message_text("🚨 Kill switch ACTIVATED — no new trades")
    elif q.data == "resume":
        kill_switch = False
        await q.edit_message_text("▶️ Trading RESUMED")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    mode = "PAPER" if PAPER_MODE else "LIVE"
    logger.info(f"🤖 Kalshi Momentum Bot starting [{mode}]")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CallbackQueryHandler(button_handler))

    async def on_startup(application):
        await application.bot.send_message(
            chat_id=ALLOWED_USER,
            text=f"🤖 Kalshi Momentum Bot STARTED\n"
                 f"Mode: {'📄 PAPER' if PAPER_MODE else '💵 LIVE'}\n"
                 f"Markets: BTC, ETH, SOL 15-min\n"
                 f"Threshold: {THRESHOLD*100:.2f}%\n"
                 f"Stake: {STAKE} contracts/trade"
        )
        asyncio.create_task(scan_loop(application))
        asyncio.create_task(resolver_loop(application))

    app.post_init = on_startup
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
