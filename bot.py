"""
Kalshi Bot v3 — Convergence + Lag engine for BTC/ETH/SOL short-horizon markets
==============================================================================
Why v2 lost money live (fixed here):

 1. ORDER SEMANTICS BUG — the V2 endpoint quotes everything on the YES leg.
    v2 sent NO orders with the NO price on the YES leg, so every DOWN trade
    either crossed the book instantly at a terrible price or rested at a
    nonsense level. Fixed in kalshi_client.py.
 2. UNAUTHENTICATED CANCELS — resolver DELETEs had no signature → 401 → stale
    resting orders sat in the book and got picked off. Cancels now signed,
    and resting orders carry a server-side expiration so they die on their own.
 3. WRONG-SIDE DEPTH — v2 read book["yes"] as YES ask liquidity. Both book
    arrays are BIDS; asks must be derived from the opposite side. Fixed.
 4. FANTASY PAPER FILLS — v2 assumed 100% fills at the quoted price. v3 paper
    mode walks the real book for takers and only fills makers when real trades
    print through our price. Paper now predicts live.
 5. MOMENTUM CHASING — buying after the move at prices market makers already
    updated is structurally -EV. v3 only ever trades when its own fair-value
    model says the AVAILABLE price is mispriced net of fees.

Strategies:
  CONVERGENCE (fill workhorse): in the final minutes of 15M *and* 1H markets
    (1H traded only inside its last minutes → 1H liquidity, 15-min exposure),
    buy the heavy favorite when model p exceeds ask + fee + edge. If the ask
    is too rich, post a self-expiring post-only bid at our price instead.
  LAG (opportunist): on a vol-normalized spot burst, take liquidity only if
    the book still lags fair value by a large margin net of taker fees.
"""

import asyncio
import logging
import math
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler,
                          ContextTypes)

import engine
from engine import (MarketMeta, PaperBroker, RiskManager, SpotFeed, Store,
                    depth_at, in_blackout, maker_fee, maker_fee_pc,
                    parse_blackouts, parse_book, parse_market, taker_fee,
                    taker_fee_pc, walk_book)
from kalshi_client import KalshiClient

load_dotenv()
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO)
# httpx logs full Telegram URLs at INFO — which contain the bot token.
# Silence it (and its transport) so credentials never hit the logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("bot")

# ── Config ──────────────────────────────────────────────────────────────────
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY", "")
KALSHI_PRIV_KEY = os.getenv("KALSHI_PRIVATE_KEY", "")
_kf = os.getenv("KALSHI_PRIVATE_KEY_FILE", "")
if _kf and os.path.exists(_kf):
    KALSHI_PRIV_KEY = open(_kf).read()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_USER = int(os.getenv("ALLOWED_USER_ID", "0"))
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"
MODE = "paper" if PAPER_MODE else "live"

CRYPTOS = [c.strip().upper() for c in
           os.getenv("CRYPTOS", "BTC,ETH,SOL").split(",") if c.strip()]
SERIES_15M = {c: f"KX{c}15M" for c in CRYPTOS}
# Hourly above/below series: KXBTCD family (NOT KX{c}1H — that returns nothing)
_HOURLY_DEFAULT = {"BTC": "KXBTCD", "ETH": "KXETHD", "SOL": "KXSOLD"}
SERIES_1H = {}
for _pair in os.getenv("SERIES_1H_MAP", "").split(","):
    if ":" in _pair:
        _k, _v = _pair.split(":", 1)
        SERIES_1H[_k.strip().upper()] = _v.strip().upper()
for _c in CRYPTOS:
    SERIES_1H.setdefault(_c, _HOURLY_DEFAULT.get(_c, f"KX{_c}D"))
BASE = "https://api.elections.kalshi.com"

# Sizing / risk
MAX_CONTRACTS = int(os.getenv("STAKE_MAX_CONTRACTS",
                              os.getenv("STAKE_CONTRACTS", "10")))
MAX_STAKE_USD = float(os.getenv("MAX_STAKE_USD", "9"))
MIN_FILL = int(os.getenv("MIN_FILL_CONTRACTS", "2"))
DEPTH_FRACTION = float(os.getenv("DEPTH_FRACTION", "0.30"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "3"))
PER_CRYPTO = int(os.getenv("PER_CRYPTO_MAX", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS_USD", "15"))

# Model
TAIL_MULT = float(os.getenv("TAIL_MULT", "1.25"))
SPIKE_MAX = float(os.getenv("SPIKE_MAX", "2.5"))
CHOP_MAX_BURSTS = int(os.getenv("CHOP_MAX_BURSTS", "3"))
CHOP_WINDOW_S = float(os.getenv("CHOP_WINDOW_S", "600"))
BLACKOUTS = parse_blackouts(os.getenv(
    "NEWS_BLACKOUT_UTC", "12:25-12:45,13:55-14:15,18:00-18:20"))

# Convergence strategy
CONV_MIN_P = float(os.getenv("CONV_MIN_P", "0.90"))
CONV_MIN_EV = float(os.getenv("CONV_MIN_EV", "0.02"))
CONV_MIN_TAU = float(os.getenv("CONV_MIN_TAU", "45"))
CONV_MAX_TAU = float(os.getenv("CONV_MAX_TAU", "600"))
CONV_PRICE_MIN = int(os.getenv("CONV_PRICE_MIN", "72"))
CONV_PRICE_MAX = int(os.getenv("CONV_PRICE_MAX", "97"))
MAX_DIVERGENCE = float(os.getenv("MAX_DIVERGENCE", "0.12"))
CONV_MAKER = os.getenv("CONV_MAKER", "true").lower() == "true"
MAKER_TTL = float(os.getenv("MAKER_TTL_S", "45"))

# Lag strategy
LAG_ENABLED = os.getenv("LAG_ENABLED", "true").lower() == "true"
LAG_Z = float(os.getenv("LAG_Z", "3.0"))
LAG_LOOKBACK = float(os.getenv("LAG_LOOKBACK_S", "8"))
LAG_MIN_MOVE = float(os.getenv("LAG_MIN_MOVE", "0.0008"))
LAG_MIN_EV = float(os.getenv("LAG_MIN_EV", "0.04"))
LAG_MIN_P = float(os.getenv("LAG_MIN_P", "0.55"))   # only trade the side the
# model favors — never longshots (they only look cheap because the burst
# inflated the fast vol estimate), and never below the stop-loss threshold
LAG_MIN_TAU = float(os.getenv("LAG_MIN_TAU", "90"))
LAG_MAX_TAU = float(os.getenv("LAG_MAX_TAU", "900"))
LAG_COOLDOWN = float(os.getenv("LAG_COOLDOWN_S", "60"))

# Exits
STOP_P = float(os.getenv("STOP_P", "0.30"))
STOP_MIN_TAU = float(os.getenv("STOP_MIN_TAU", "35"))

DB_PATH = os.getenv("DB_PATH", "kalshi_v3.db")

# ── Globals ─────────────────────────────────────────────────────────────────
store = Store(DB_PATH)
risk = RiskManager(store, MODE, MAX_DAILY_LOSS, MAX_CONCURRENT, PER_CRYPTO)
spot = SpotFeed(CRYPTOS)
client: Optional[KalshiClient] = None
if not PAPER_MODE:
    client = KalshiClient(KALSHI_API_KEY, KALSHI_PRIV_KEY)

http: Optional[aiohttp.ClientSession] = None
paper = PaperBroker(lambda: http)

market_cache: Dict[str, List[MarketMeta]] = {c: [] for c in CRYPTOS}
open_positions: List[dict] = []
pending_makers: Dict[str, dict] = {}      # key → intent
consumed_fill_ids: set = set()
lag_cooldown: Dict[str, float] = {c: 0.0 for c in CRYPTOS}
recent_bursts: Dict[str, List[float]] = {c: [] for c in CRYPTOS}
_halt_notified: Dict[str, str] = {"day": ""}
_book_cache: Dict[str, tuple] = {}
_book_shape_logged: Dict[str, bool] = {}
tg_app: Optional[Application] = None
arb = None   # ArbScanner, built in on_startup


async def notify(text: str):
    if tg_app and ALLOWED_USER:
        try:
            await tg_app.bot.send_message(chat_id=ALLOWED_USER, text=text)
        except Exception as e:
            logger.warning(f"telegram error: {e}")


# ── Data helpers ────────────────────────────────────────────────────────────
async def fetch_json(path: str, params: dict = None) -> dict:
    async with http.get(f"{BASE}{path}", params=params,
                        timeout=aiohttp.ClientTimeout(total=6)) as r:
        return await r.json()


async def get_book(ticker: str, max_age: float = 1.5) -> Optional[dict]:
    now = time.time()
    hit = _book_cache.get(ticker)
    if hit and now - hit[0] < max_age:
        return hit[1]
    try:
        raw = await fetch_json(f"/trade-api/v2/markets/{ticker}/orderbook",
                               {"depth": 16})
        parsed = parse_book(raw)
        if not _book_shape_logged.get("done"):
            _book_shape_logged["done"] = True
            fp = raw.get("orderbook_fp") or {}
            ys = (fp.get("yes_dollars") or [None])[0]
            ns = (fp.get("no_dollars") or [None])[0]
            logger.info(
                f"first book {ticker}: keys={list(raw.keys())} "
                f"sample yes0={ys} no0={ns} "
                f"→ yes {parsed['yes_bid']}/{parsed['yes_ask']} "
                f"no {parsed['no_bid']}/{parsed['no_ask']} "
                f"levels y{len(parsed['yes_bids'])}/n{len(parsed['no_bids'])}")
        _book_cache[ticker] = (now, parsed)
        return parsed
    except Exception as e:
        logger.debug(f"book fetch {ticker}: {e}")
        return None


_series_note: Dict[str, bool] = {}


async def market_refresh_loop():
    while True:
        for crypto in CRYPTOS:
            metas: List[MarketMeta] = []
            for series in (SERIES_15M[crypto], SERIES_1H[crypto]):
                got, priced = 0, 0
                try:
                    data = await fetch_json(
                        "/trade-api/v2/markets",
                        {"limit": 8, "status": "open",
                         "series_ticker": series})
                    mkts = data.get("markets", []) or []
                    got = len(mkts)
                    for m in mkts:
                        meta = parse_market(m, crypto, series)
                        if meta and 0 < meta.tau() < 7200:
                            metas.append(meta)
                            priced += 1
                except Exception as e:
                    logger.debug(f"market refresh {series}: {e}")
                if series not in _series_note:
                    _series_note[series] = True
                    lvl = logger.info if got else logger.warning
                    lvl(f"series {series}: {got} open markets, "
                        f"{priced} priced"
                        + ("" if got else " — CHECK SERIES TICKER"))
            metas.sort(key=lambda x: x.close_ts)
            market_cache[crypto] = metas
        await asyncio.sleep(20)


def has_exposure(ticker: str) -> bool:
    if any(p["ticker"] == ticker for p in open_positions):
        return True
    return any(pm["ticker"] == ticker for pm in pending_makers.values())


def sized(price_c: int, depth: int, cap_usd: float) -> int:
    if price_c <= 0:
        return 0
    by_usd = int(cap_usd / (price_c / 100.0))
    by_depth = int(DEPTH_FRACTION * depth)
    return max(0, min(MAX_CONTRACTS, by_usd, by_depth))


def _norm_avg_price(v: float, fallback_c: int) -> float:
    """Kalshi may return avg fill price in dollars or cents. → dollars."""
    if v <= 0:
        return fallback_c / 100.0
    return v / 100.0 if v > 1.5 else v


# ── Position lifecycle ──────────────────────────────────────────────────────
def open_position(strategy: str, meta: MarketMeta, side: str, avg_d: float,
                  contracts: int, fee: float, p_model: float) -> dict:
    pos = {
        "id": f"{meta.crypto}_{strategy}_{uuid.uuid4().hex[:8]}",
        "ts": time.time(), "mode": MODE, "strategy": strategy,
        "crypto": meta.crypto, "ticker": meta.ticker, "side": side,
        "entry_cents": round(avg_d * 100, 2), "contracts": contracts,
        "cost": round(avg_d * contracts, 4), "fee": fee, "status": "open",
        "model_p": round(p_model, 4), "tau": round(meta.tau(), 1),
        "close_ts": meta.close_ts, "kind": meta.kind, "strike": meta.strike,
    }
    open_positions.append(pos)
    store.insert_trade(pos)
    return pos


async def announce_entry(pos: dict, how: str, note: str = ""):
    mode = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
    win = pos["contracts"] * 1.00 - pos["cost"] - pos["fee"]
    loss = -(pos["cost"] + pos["fee"])
    await notify(
        f"{mode} | {pos['strategy'].upper()} {how}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🪙 {pos['crypto']} {pos['side'].upper()} @ "
        f"{pos['entry_cents']:.0f}¢ ×{pos['contracts']}\n"
        f"📋 {pos['ticker']}\n"
        f"🎯 model p={pos['model_p']:.3f} | τ={pos['tau']:.0f}s\n"
        f"💵 cost ${pos['cost']:.2f} | fee ${pos['fee']:.2f}\n"
        f"✅ win +${win:.2f} | ❌ loss ${loss:.2f}"
        + (f"\n{note}" if note else ""))


async def execute_taker(strategy: str, meta: MarketMeta, side: str,
                        limit_c: int, contracts: int, p_model: float,
                        book: dict) -> Optional[dict]:
    store.incr("taker_attempts")
    store.incr("taker_req_contracts", contracts)
    if PAPER_MODE:
        filled, vwap_c = PaperBroker.taker_ioc(book, side, limit_c, contracts)
        if filled < MIN_FILL:
            logger.info(f"[PAPER taker] {meta.ticker} {side}@≤{limit_c}¢ "
                        f"insufficient fill ({filled})")
            return None
        avg_d = vwap_c / 100.0
        fee = taker_fee(avg_d, filled)
    else:
        try:
            resp = await asyncio.to_thread(
                client.buy, meta.ticker, side, limit_c / 100.0, contracts)
        except Exception as e:
            logger.error(f"live taker failed {meta.ticker}: {e}")
            return None
        filled = int(resp.get("fill_count", 0))
        if filled < 1:
            logger.info(f"[LIVE taker] {meta.ticker} {side}@≤{limit_c}¢ "
                        f"no fill (book moved)")
            return None
        yes_avg = _norm_avg_price(resp.get("avg_fill_price", 0), limit_c
                                  if side == "yes" else 100 - limit_c)
        avg_d = yes_avg if side == "yes" else 1.0 - yes_avg
        fee = taker_fee(avg_d, filled)
    store.incr("taker_filled_contracts", filled)
    pos = open_position(strategy, meta, side, avg_d, filled, fee, p_model)
    await announce_entry(pos, "TAKER")
    return pos


async def place_maker(meta: MarketMeta, side: str, price_c: int,
                      contracts: int, p_model: float, ttl: float):
    key = f"{meta.ticker}:{side}"
    intent = {
        "ticker": meta.ticker, "side": side, "price_c": price_c,
        "count": contracts, "filled": 0, "p_model": p_model,
        "expires": time.time() + ttl, "meta": meta, "order_id": "",
    }
    if PAPER_MODE:
        o = paper.place_maker(meta.ticker, side, price_c, contracts, ttl,
                              {"key": key})
        intent["order_id"] = o.oid
    else:
        try:
            resp = await asyncio.to_thread(
                client.post_bid, meta.ticker, side, price_c / 100.0,
                contracts, int(time.time() + ttl))
        except Exception as e:
            logger.error(f"live maker post failed {meta.ticker}: {e}")
            return
        if int(resp.get("fill_count", 0)) > 0:
            # post_only should prevent this; safety net
            logger.warning("post_only order partially crossed?!")
        intent["order_id"] = resp.get("order_id", "")
        if not intent["order_id"]:
            return
    pending_makers[key] = intent
    store.incr("maker_placed")
    store.incr("maker_req_contracts", contracts)
    logger.info(f"[{MODE} maker] posted {meta.ticker} {side}@{price_c}¢ "
                f"×{contracts} ttl={ttl:.0f}s")


async def on_paper_maker_fill(order, new_fill: int):
    key = order.meta.get("key")
    intent = pending_makers.get(key)
    if not intent:
        return
    meta: MarketMeta = intent["meta"]
    avg_d = order.price_c / 100.0
    fee = maker_fee(avg_d, new_fill)
    store.incr("maker_filled_contracts", new_fill)
    pos = open_position("conv", meta, intent["side"], avg_d, new_fill, fee,
                        intent["p_model"])
    await announce_entry(pos, "MAKER fill")
    intent["filled"] += new_fill
    if intent["filled"] >= intent["count"]:
        pending_makers.pop(key, None)


paper.on_maker_fill = on_paper_maker_fill


async def poll_live_maker_fills():
    if PAPER_MODE or not pending_makers:
        return
    try:
        fills = await asyncio.to_thread(client.get_fills, 50)
    except Exception as e:
        logger.debug(f"fills poll: {e}")
        return
    by_order: Dict[str, List[dict]] = {}
    for f in fills or []:
        fid = f.get("trade_id") or f.get("fill_id") or str(f)
        if fid in consumed_fill_ids:
            continue
        oid = f.get("order_id", "")
        if oid:
            by_order.setdefault(oid, []).append((fid, f))
    for key, intent in list(pending_makers.items()):
        rows = by_order.get(intent["order_id"], [])
        for fid, f in rows:
            consumed_fill_ids.add(fid)
            cnt = int(f.get("count", 0))
            yes_p = int(f.get("yes_price", 0))
            side = intent["side"]
            avg_d = (yes_p if side == "yes" else 100 - yes_p) / 100.0
            fee = maker_fee(avg_d, cnt)
            store.incr("maker_filled_contracts", cnt)
            meta: MarketMeta = intent["meta"]
            pos = open_position("conv", meta, side, avg_d, cnt, fee,
                                intent["p_model"])
            await announce_entry(pos, "MAKER fill")
            intent["filled"] += cnt
        if intent["filled"] >= intent["count"]:
            pending_makers.pop(key, None)


async def purge_expired_makers():
    now = time.time()
    for key, intent in list(pending_makers.items()):
        if now > intent["expires"] + 3:
            # Belt-and-braces: even though orders carry a server-side
            # expiration_time, force-cancel in live mode in case the venue
            # rejected/ignored that field. Stale resting orders get picked off.
            if not PAPER_MODE and intent.get("order_id"):
                try:
                    ok = await asyncio.to_thread(
                        client.cancel_order, intent["order_id"])
                    logger.info(f"live maker cancel {key}: "
                                f"{'ok' if ok else 'already gone'}")
                except Exception as e:
                    logger.warning(f"live maker cancel {key} failed: {e}")
            pending_makers.pop(key, None)
            logger.info(f"maker expired: {key} "
                        f"({intent['filled']}/{intent['count']} filled)")


async def maybe_notify_halt(why: str):
    if "daily loss" not in why:
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _halt_notified["day"] != today:
        _halt_notified["day"] = today
        await notify(f"🛑 DAILY LOSS HALT — {why}. No new entries until "
                     f"UTC midnight. Open positions still managed.")


# ── CONVERGENCE strategy ────────────────────────────────────────────────────
def maker_target_price(p_side: float) -> int:
    """Highest cent price x with p − x − maker_fee(x) ≥ CONV_MIN_EV."""
    x = p_side - CONV_MIN_EV
    for _ in range(3):
        x = p_side - CONV_MIN_EV - maker_fee_pc(max(0.01, min(0.99, x)))
    return int(math.floor(x * 100))


near_miss = {"ev": -9.9, "txt": "none yet"}


def _note_near_miss(ev, meta, side, p, ask_c, depth, n):
    if ev > near_miss["ev"]:
        near_miss["ev"] = ev
        near_miss["txt"] = (f"{meta.crypto} {side}@{ask_c}¢ p={p:.3f} "
                            f"ev={ev*100:+.1f}¢ depth={depth} size={n} "
                            f"τ={meta.tau():.0f}s")


async def convergence_loop():
    while True:
        await asyncio.sleep(2.0)
        try:
            if in_blackout(BLACKOUTS):
                store.incr("f_blackout_cycles")
                continue
            for crypto in CRYPTOS:
                if not spot.ready(crypto):
                    continue
                vol = spot.vol[crypto]
                if vol.spike_ratio > SPIKE_MAX:
                    store.incr("f_spike")
                    continue          # vol regime unstable → model unreliable
                now_t = time.time()
                recent_bursts[crypto] = [
                    t for t in recent_bursts[crypto]
                    if now_t - t <= CHOP_WINDOW_S]
                if len(recent_bursts[crypto]) >= CHOP_MAX_BURSTS:
                    store.incr("f_chop")
                    continue          # choppy tape: favorites get run over
                S = spot.price[crypto]
                for meta in market_cache[crypto]:
                    tau = meta.tau()
                    if not (CONV_MIN_TAU <= tau <= CONV_MAX_TAU):
                        continue
                    if has_exposure(meta.ticker):
                        continue
                    store.incr("f_eval")
                    p_yes = meta.p_yes(S, vol.sigma_for_tau(tau), TAIL_MULT)
                    if p_yes >= CONV_MIN_P:
                        side, p_side = "yes", p_yes
                    elif (1 - p_yes) >= CONV_MIN_P:
                        side, p_side = "no", 1 - p_yes
                    else:
                        store.incr("f_p_fail")
                        continue
                    ok, why = risk.check(crypto, open_positions)
                    if not ok:
                        store.incr("f_risk")
                        await maybe_notify_halt(why)
                        logger.debug(f"risk block: {why}")
                        continue
                    book = await get_book(meta.ticker)
                    if not book:
                        continue
                    ask_c = book[f"{side}_ask"]
                    ladder = book[f"{side}_asks"]
                    ask_d = ask_c / 100.0
                    ev_taker = p_side - ask_d - taker_fee_pc(ask_d)
                    depth = depth_at(ladder, ask_c)
                    n = sized(ask_c, depth, MAX_STAKE_USD)
                    _note_near_miss(ev_taker, meta, side, p_side, ask_c,
                                    depth, n)

                    if ask_c >= 99:
                        store.incr("f_no_offers")
                        continue      # winning side has no sellers — nothing
                                      # will trade down to a bid either
                    if p_side - ask_d > MAX_DIVERGENCE:
                        store.incr("f_diverge")
                        logger.info(f"divergence guard: {meta.ticker} {side} "
                                    f"p={p_side:.3f} vs ask {ask_c}¢ — "
                                    f"assuming WE are wrong; skipping")
                        continue

                    band_ok = CONV_PRICE_MIN <= ask_c <= CONV_PRICE_MAX
                    if band_ok and ev_taker >= CONV_MIN_EV and n >= MIN_FILL:
                        await execute_taker("conv", meta, side, ask_c, n,
                                            p_side, book)
                        continue
                    # why did taker not fire? (funnel)
                    if not band_ok:
                        store.incr("f_t_band")
                    elif ev_taker < CONV_MIN_EV:
                        store.incr("f_t_ev")
                    elif n < MIN_FILL:
                        store.incr("f_t_depth")
                        logger.info(f"depth-blocked: {meta.ticker} {side} "
                                    f"ask={ask_c}¢ depth={depth} → size {n} "
                                    f"< {MIN_FILL}")

                    if not CONV_MAKER:
                        continue
                    tgt = maker_target_price(p_side)
                    tgt = min(tgt, ask_c - 1, CONV_PRICE_MAX)
                    bid_c = book[f"{side}_bid"]
                    if tgt < max(CONV_PRICE_MIN, bid_c) or tgt < 1:
                        store.incr("f_m_room")
                        continue      # our price wouldn't be top-of-book
                    ttl = min(MAKER_TTL, tau - (STOP_MIN_TAU - 5))
                    if ttl < 6:
                        store.incr("f_m_room")
                        continue
                    n_m = min(MAX_CONTRACTS,
                              int(MAX_STAKE_USD / (tgt / 100.0)))
                    if n_m < MIN_FILL:
                        store.incr("f_m_room")
                        continue
                    await place_maker(meta, side, tgt, n_m, p_side, ttl)
        except Exception:
            logger.exception("convergence loop error")


# ── LAG strategy ────────────────────────────────────────────────────────────
async def lag_loop():
    while True:
        await asyncio.sleep(0.5)
        if not LAG_ENABLED:
            continue
        try:
            if in_blackout(BLACKOUTS):
                continue
            now = time.time()
            for crypto in CRYPTOS:
                if now < lag_cooldown[crypto] or not spot.ready(crypto):
                    continue
                r, dt = spot.move_over(crypto, LAG_LOOKBACK)
                if dt <= 0:
                    continue
                sig = spot.vol[crypto].sigma_1s * math.sqrt(max(dt, 0.5))
                z = abs(r) / sig if sig > 0 else 0
                if z < LAG_Z or abs(r) < LAG_MIN_MOVE:
                    continue
                lag_cooldown[crypto] = now + LAG_COOLDOWN
                store.incr("f_lag_burst")
                recent_bursts[crypto].append(now)
                logger.info(f"⚡ burst {crypto}: {r*100:+.3f}% in {dt:.1f}s "
                            f"(z={z:.1f}) — scanning for lagging quotes")
                await lag_evaluate(crypto)
        except Exception:
            logger.exception("lag loop error")


async def lag_evaluate(crypto: str):
    S = spot.price[crypto]
    vol = spot.vol[crypto]
    # Horizon-matched vol, EXCLUDING the fast EWMA: the burst that triggered
    # us inflates it, drags p toward 0.5 and makes longshots look cheap.
    best = None
    for meta in market_cache[crypto]:
        tau = meta.tau()
        if not (LAG_MIN_TAU <= tau <= LAG_MAX_TAU):
            continue
        if has_exposure(meta.ticker):
            continue
        p_yes = meta.p_yes(S, vol.sigma_for_tau(tau, include_fast=False),
                           TAIL_MULT)
        book = await get_book(meta.ticker, max_age=0.0)   # must be fresh
        if not book:
            continue
        for side, p_side in (("yes", p_yes), ("no", 1 - p_yes)):
            if p_side < LAG_MIN_P:
                continue          # entry must clear the stop threshold + favor
            ask_c = book[f"{side}_ask"]
            if not (5 <= ask_c <= 95):
                continue
            ask_d = ask_c / 100.0
            if p_side - ask_d > MAX_DIVERGENCE:
                store.incr("f_diverge")
                logger.info(f"lag divergence guard: {meta.ticker} {side} "
                            f"p={p_side:.3f} vs ask {ask_c}¢ — assuming WE "
                            f"are wrong; skipping")
                continue
            ev = p_side - ask_d - taker_fee_pc(ask_d)
            if ev < LAG_MIN_EV:
                continue
            depth = depth_at(book[f"{side}_asks"], ask_c)
            n = sized(ask_c, depth, MAX_STAKE_USD)
            if n < MIN_FILL:
                continue
            if best is None or ev > best[0]:
                best = (ev, meta, side, ask_c, n, p_side, book)
    if not best:
        store.incr("f_lag_standdown")
        logger.info(f"{crypto}: burst but no lagging quotes (MMs already "
                    f"repriced) — correctly standing down")
        return
    ev, meta, side, ask_c, n, p_side, book = best
    ok, why = risk.check(crypto, open_positions)
    if not ok:
        logger.info(f"lag blocked: {why}")
        return
    await execute_taker("lag", meta, side, ask_c, n, p_side, book)


# ── Resolver: settlement, stops, maker bookkeeping ──────────────────────────
async def close_position(pos: dict, result: str, pnl: float, reason: str):
    pos["status"] = "closed"
    store.close_trade(pos["id"], result, round(pnl, 2), reason)
    if pos in open_positions:
        open_positions.remove(pos)
    s = store.summary(MODE)
    emoji = "✅ WIN" if pnl > 0 else "❌ LOSS"
    mode = "📄 PAPER" if PAPER_MODE else "💵 LIVE"
    await notify(
        f"{emoji} | {mode} | {pos['strategy'].upper()} ({reason})\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"🪙 {pos['crypto']} {pos['side'].upper()} "
        f"@{pos['entry_cents']:.0f}¢ ×{pos['contracts']}\n"
        f"P&L: ${pnl:+.2f}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"📊 {s.get('trades',0)} trades | WR {s.get('win_rate',0)}% "
        f"({s.get('wr_ci','–')})\n"
        f"Total P&L: ${s.get('pnl',0):+.2f} | fees ${s.get('fees',0):.2f}\n"
        f"Today: ${store.daily_pnl(MODE):+.2f}")


async def try_stop_loss(pos: dict):
    crypto = pos["crypto"]
    if not spot.ready(crypto):
        return
    tau = max(0.0, pos["close_ts"] - time.time())
    if tau <= STOP_MIN_TAU:
        return
    meta = MarketMeta(pos["ticker"], crypto, "", pos["kind"], pos["strike"],
                      pos["close_ts"], {})
    p_now = meta.p_yes(spot.price[crypto],
                       spot.vol[crypto].sigma_for_tau(tau), TAIL_MULT)
    p_side = p_now if pos["side"] == "yes" else 1 - p_now
    if p_side >= STOP_P:
        return
    book = await get_book(pos["ticker"], max_age=0.0)
    if not book:
        return
    bid_c = book[f"{pos['side']}_bid"]
    if bid_c < 3:
        return   # nothing to salvage
    n = pos["contracts"]
    if PAPER_MODE:
        bids = book[f"{pos['side']}_bids"]
        avail = sum(c for p, c in bids if p >= bid_c)
        filled = min(n, avail)
        avg_d = bid_c / 100.0
    else:
        try:
            resp = await asyncio.to_thread(
                client.exit, pos["ticker"], pos["side"], bid_c / 100.0, n)
        except Exception as e:
            logger.error(f"stop exit failed: {e}")
            return
        filled = int(resp.get("fill_count", 0))
        yes_avg = _norm_avg_price(resp.get("avg_fill_price", 0),
                                  bid_c if pos["side"] == "yes"
                                  else 100 - bid_c)
        avg_d = yes_avg if pos["side"] == "yes" else 1.0 - yes_avg
    if filled < 1:
        return
    exit_fee = taker_fee(avg_d, filled)
    entry_pc = pos["cost"] / pos["contracts"]
    fee_pc = pos["fee"] / pos["contracts"]
    realized = filled * (avg_d - entry_pc - fee_pc) - exit_fee
    if filled >= n:
        await close_position(pos, "stopped", realized, "stop-loss")
    else:
        rem = n - filled
        residual = dict(pos)
        residual["id"] = pos["id"] + "-r"
        residual["contracts"] = rem
        residual["cost"] = round(entry_pc * rem, 4)
        residual["fee"] = round(fee_pc * rem, 4)
        store.insert_trade(residual)
        open_positions.append(residual)
        await close_position(pos, "stopped-partial", realized,
                             "stop-loss partial")


async def resolver_loop():
    while True:
        await asyncio.sleep(12)
        try:
            await purge_expired_makers()
            await poll_live_maker_fills()
            for pos in list(open_positions):
                try:
                    data = await fetch_json(
                        f"/trade-api/v2/markets/{pos['ticker']}")
                    market = data.get("market", {})
                except Exception:
                    continue
                status = market.get("status", "")
                if status in ("finalized", "settled"):
                    result = market.get("result", "")
                    won = (result == pos["side"])
                    pnl = (pos["contracts"] * 1.00 - pos["cost"] - pos["fee"]
                           if won else -(pos["cost"] + pos["fee"]))
                    await close_position(pos, result, pnl, "settlement")
                elif status in ("open", "active"):
                    await try_stop_loss(pos)
        except Exception:
            logger.exception("resolver error")


async def heartbeat_loop():
    while True:
        await asyncio.sleep(300)
        try:
            prices = " ".join(
                f"{c}:{spot.price.get(c, 0):,.0f}" for c in CRYPTOS)
            windows = sum(
                1 for c in CRYPTOS for m in market_cache[c]
                if CONV_MIN_TAU <= m.tau() <= CONV_MAX_TAU)
            logger.info(
                f"💓 {prices} | ws={'up' if spot.ws_alive else 'FALLBACK'}"
                f" | mkts={sum(len(v) for v in market_cache.values())}"
                f" in-window={windows}"
                f" | evald={store.get_kv('f_eval'):.0f}"
                f" | open={len(open_positions)}"
                f" resting={len(pending_makers)}"
                f" | today=${store.daily_pnl(MODE):+.2f}")
        except Exception:
            logger.exception("heartbeat error")


async def paper_maker_poll_loop():
    while True:
        await asyncio.sleep(2.5)
        if PAPER_MODE:
            try:
                await paper.tick()
            except Exception:
                logger.exception("paper tick error")


# ── Telegram UI ─────────────────────────────────────────────────────────────
def panel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 P&L", callback_data="pnl"),
         InlineKeyboardButton("🧠 Strategies", callback_data="strat")],
        [InlineKeyboardButton("📂 Positions", callback_data="pos"),
         InlineKeyboardButton("🎯 Fill stats", callback_data="fills")],
        [InlineKeyboardButton("💰 Balance", callback_data="bal"),
         InlineKeyboardButton("🔧 Settings", callback_data="cfg")],
        [InlineKeyboardButton("🔬 Why no trades?", callback_data="diag"),
         InlineKeyboardButton("♻️ Reset calib", callback_data="calibreset")],
        [InlineKeyboardButton("🔀 Arb scanner", callback_data="arb")],
        [InlineKeyboardButton("🚨 Kill", callback_data="kill"),
         InlineKeyboardButton("▶️ Resume", callback_data="resume")],
    ])


def txt_pnl() -> str:
    s = store.summary(MODE)
    if not s.get("trades"):
        return "No closed trades yet."
    return (f"📊 P&L [{MODE.upper()}]\n━━━━━━━━━━━━\n"
            f"Trades: {s['trades']} | WR {s['win_rate']}% ({s['wr_ci']})\n"
            f"W/L: {s['wins']}/{s['losses']}\n"
            f"P&L: ${s['pnl']:+.2f} | fees ${s['fees']:.2f}\n"
            f"Staked: ${s['staked']:.2f} | ROI {s['roi']}%\n"
            f"Avg/trade ${s['avg']:+.3f} | Sharpe {s['sharpe']}\n"
            f"🎓 Calibration: model said {s.get('model_p_avg', 0)}% "
            f"| reality {s['win_rate']}%\n"
            f"Today: ${store.daily_pnl(MODE):+.2f}"
            + _calib_window_txt())


def _calib_window_txt() -> str:
    rt = store.get_kv("calib_since")
    if rt <= 0:
        return ""
    s2 = store.summary(MODE, rt)
    if not s2.get("trades"):
        return "\n— since calib reset: no closed trades yet —"
    return (f"\n— since calib reset ({s2['trades']}T) —\n"
            f"WR {s2['win_rate']}% ({s2['wr_ci']}) | "
            f"P&L ${s2['pnl']:+.2f}\n"
            f"🎓 model {s2.get('model_p_avg', 0)}% vs real "
            f"{s2['win_rate']}%")


def txt_strat() -> str:
    s = store.summary(MODE)
    lines = [f"🧠 By strategy [{MODE.upper()}]", "━━━━━━━━━━━━"]
    for name, d in (s.get("by_strategy") or {}).items():
        wr = d["w"] / d["n"] * 100 if d["n"] else 0
        lines.append(f"{name}: {d['n']}T {wr:.0f}%WR ${d['pnl']:+.2f}")
    lines.append("━━━━━━━━━━━━")
    for name, d in (s.get("by_crypto") or {}).items():
        wr = d["w"] / d["n"] * 100 if d["n"] else 0
        lines.append(f"{name}: {d['n']}T {wr:.0f}%WR ${d['pnl']:+.2f}")
    return "\n".join(lines) if len(lines) > 3 else "No closed trades yet."


def txt_fills() -> str:
    mp = store.get_kv("maker_placed")
    mrc = store.get_kv("maker_req_contracts")
    mfc = store.get_kv("maker_filled_contracts")
    ta = store.get_kv("taker_attempts")
    trc = store.get_kv("taker_req_contracts")
    tfc = store.get_kv("taker_filled_contracts")
    mr = (mfc / mrc * 100) if mrc else 0
    tr = (tfc / trc * 100) if trc else 0
    return (f"🎯 Fill stats [{MODE.upper()}]\n━━━━━━━━━━━━\n"
            f"Maker: {mp:.0f} posted | {mfc:.0f}/{mrc:.0f} contracts "
            f"({mr:.0f}%)\n"
            f"Taker: {ta:.0f} sent | {tfc:.0f}/{trc:.0f} contracts "
            f"({tr:.0f}%)\n"
            f"Pending makers: {len(pending_makers)}\n"
            f"Tape polls: {paper.poll_ok} ok / {paper.poll_err} err\n\n"
            f"These are REAL fill rates — the number paper mode\n"
            f"used to lie to you about.")


def txt_diag() -> str:
    ev = store.get_kv("f_eval")
    pf = store.get_kv("f_p_fail")
    rk = store.get_kv("f_risk")
    tb = store.get_kv("f_t_band")
    te = store.get_kv("f_t_ev")
    td = store.get_kv("f_t_depth")
    mr = store.get_kv("f_m_room")
    mp = store.get_kv("maker_placed")
    sp = store.get_kv("f_spike")
    bo = store.get_kv("f_blackout_cycles")
    lb = store.get_kv("f_lag_burst")
    ls = store.get_kv("f_lag_standdown")
    return (f"🔬 Signal funnel [{MODE.upper()}]\n━━━━━━━━━━━━\n"
            f"Windows evaluated: {ev:.0f}\n"
            f"├ no {CONV_MIN_P:.0%}+ favorite: {pf:.0f}\n"
            f"├ risk-blocked: {rk:.0f}\n"
            f"├ taker: ask outside {CONV_PRICE_MIN}–{CONV_PRICE_MAX}¢: "
            f"{tb:.0f}\n"
            f"├ taker: edge < {CONV_MIN_EV*100:.1f}¢: {te:.0f}\n"
            f"├ taker: depth too thin: {td:.0f}\n"
            f"├ no offers (ask ≥99¢): "
            f"{store.get_kv('f_no_offers'):.0f}\n"
            f"├ divergence guard: {store.get_kv('f_diverge'):.0f}\n"
            f"├ maker: no room to post: {mr:.0f}\n"
            f"└ maker posted: {mp:.0f}\n"
            f"Vol-spike skips: {sp:.0f} | chop skips: "
            f"{store.get_kv('f_chop'):.0f} | blackout cycles: {bo:.0f}\n"
            f"Lag bursts: {lb:.0f} | stood down: {ls:.0f}\n"
            f"━━━━━━━━━━━━\n"
            f"Best near-miss (since restart):\n{near_miss['txt']}")


def txt_cfg() -> str:
    return (f"🔧 v3 config\nMode: {MODE.upper()}\n"
            f"CONV: p≥{CONV_MIN_P} ev≥{CONV_MIN_EV*100:.1f}¢ "
            f"τ∈[{CONV_MIN_TAU:.0f},{CONV_MAX_TAU:.0f}]s "
            f"px∈[{CONV_PRICE_MIN},{CONV_PRICE_MAX}]¢ "
            f"maker={'on' if CONV_MAKER else 'off'} ttl={MAKER_TTL:.0f}s\n"
            f"LAG: z≥{LAG_Z} p≥{LAG_MIN_P} ev≥{LAG_MIN_EV*100:.0f}¢ "
            f"τ∈[{LAG_MIN_TAU:.0f},{LAG_MAX_TAU:.0f}]s "
            f"{'on' if LAG_ENABLED else 'off'}\n"
            f"Size: ≤{MAX_CONTRACTS}c ≤${MAX_STAKE_USD:.0f} "
            f"depth≤{DEPTH_FRACTION*100:.0f}% minfill={MIN_FILL}\n"
            f"Risk: concurrent≤{MAX_CONCURRENT} percrypto≤{PER_CRYPTO} "
            f"dayloss≤${MAX_DAILY_LOSS:.0f} stop@p<{STOP_P}\n"
            f"Model: tail×{TAIL_MULT} spike<{SPIKE_MAX}\n"
            f"Blackouts UTC: {os.getenv('NEWS_BLACKOUT_UTC','default')}")


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    await update.message.reply_text(
        f"🤖 Kalshi Bot v3 [{MODE.upper()}]\n"
        f"Convergence + Lag | {', '.join(CRYPTOS)}", reply_markup=panel())


async def cmd_arb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    await update.message.reply_text(
        arb.txt_status() if arb else "Arb scanner not started yet.")


async def cmd_arbpairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER or not arb:
        return
    rows = arb.pending_pairs()
    if not rows:
        await update.message.reply_text(
            "No pending pairs. New candidates are nominated automatically "
            "as matching markets appear.")
        return
    for r in rows:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ approve", callback_data=f"arbok:{r['id']}"),
            InlineKeyboardButton("🚫 reject", callback_data=f"arbno:{r['id']}"),
        ]])
        await update.message.reply_text(
            f"#{r['id']} (match {r['score']:.0%})\n"
            f"KALSHI: {r['k_title']}\n  ticker {r['k_ticker']}\n"
            f"POLY:   {r['pm_title']}\n  slug {r['pm_slug']}\n"
            f"⚠️ approve ONLY after reading both rule pages — resolution "
            f"mismatch is the one real risk.", reply_markup=kb)


async def cmd_arblog(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    await update.message.reply_text(
        arb.txt_log() if arb else "Arb scanner not started yet.")


async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ALLOWED_USER:
        return
    await update.message.reply_text(txt_pnl())


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if update.effective_user.id != ALLOWED_USER:
        return
    await q.answer()
    d = q.data
    if d == "pnl":
        await q.edit_message_text(txt_pnl())
    elif d == "strat":
        await q.edit_message_text(txt_strat())
    elif d == "pos":
        if not open_positions and not pending_makers:
            await q.edit_message_text("No open positions or resting orders.")
        else:
            lines = ["📂 Open"]
            for p in open_positions:
                tau = max(0, p["close_ts"] - time.time())
                lines.append(f"• {p['crypto']} {p['side'].upper()}"
                             f"@{p['entry_cents']:.0f}¢×{p['contracts']} "
                             f"[{p['strategy']}] τ{tau:.0f}s")
            for k, m in pending_makers.items():
                lines.append(f"◦ resting {m['ticker']} {m['side']}"
                             f"@{m['price_c']}¢×{m['count']}")
            await q.edit_message_text("\n".join(lines))
    elif d == "fills":
        await q.edit_message_text(txt_fills())
    elif d == "diag":
        await q.edit_message_text(txt_diag())
    elif d == "arb":
        await q.edit_message_text(
            arb.txt_status() if arb else "Arb scanner not started yet.")
    elif d.startswith("arbok:") or d.startswith("arbno:"):
        pid = int(d.split(":")[1])
        ok = (arb.approve(pid) if d.startswith("arbok:")
              else arb.reject(pid)) if arb else False
        await q.edit_message_text(
            (f"✅ pair #{pid} approved — executable-lock checks begin on "
             f"the next cycle." if d.startswith("arbok:") else
             f"🚫 pair #{pid} rejected.") if ok
            else f"pair #{pid} not pending (already handled?).")
    elif d == "calibreset":
        store.set_kv("calib_since", time.time())
        await q.edit_message_text(
            "♻️ Calibration window reset. The P&L panel now shows a clean "
            "'since reset' block — only trades from this moment forward. "
            "All-time stats are preserved above it.")
    elif d == "bal":
        if PAPER_MODE:
            s = store.summary(MODE)
            await q.edit_message_text(
                f"💰 Paper P&L: ${s.get('pnl', 0):+.2f}")
        else:
            try:
                bal = await asyncio.to_thread(client.get_balance)
                await q.edit_message_text(f"💰 Balance: ${bal:.2f}")
            except Exception as e:
                await q.edit_message_text(f"Balance error: {e}")
    elif d == "cfg":
        await q.edit_message_text(txt_cfg())
    elif d == "kill":
        risk.kill = True
        await q.edit_message_text("🚨 Kill switch ON — no new entries.")
    elif d == "resume":
        risk.kill = False
        risk.halted_reason = ""
        await q.edit_message_text("▶️ Trading resumed.")


# ── Main ────────────────────────────────────────────────────────────────────
_conflict_notify = {"t": 0.0}


async def on_tg_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    from telegram.error import Conflict, NetworkError, TimedOut
    err = context.error
    if isinstance(err, Conflict):
        logger.warning(
            "Telegram Conflict: ANOTHER instance is polling this token "
            "(duplicate Railway deployment/replica, or an old copy running "
            "elsewhere). This instance keeps retrying; trading loops are "
            "unaffected.")
        now = time.time()
        if now - _conflict_notify["t"] > 1800:
            _conflict_notify["t"] = now
            await notify(
                "⚠️ Duplicate bot instance detected (Telegram getUpdates "
                "conflict). Check Railway Deployments for two Active "
                "deploys / replicas>1, or an old copy running elsewhere. "
                "If the other copy is the old v2 bot, kill it — it has the "
                "broken order logic.")
        return
    if isinstance(err, (NetworkError, TimedOut)):
        logger.warning(f"Telegram network hiccup: {err}")
        return
    logger.error("Unhandled Telegram error", exc_info=err)


async def on_startup(app: Application):
    global http, tg_app
    tg_app = app
    http = aiohttp.ClientSession()

    for row in store.open_trades(MODE):
        row["status"] = "open"
        open_positions.append(row)

    await spot.start()
    asyncio.create_task(market_refresh_loop())
    asyncio.create_task(convergence_loop())
    asyncio.create_task(lag_loop())
    asyncio.create_task(resolver_loop())
    asyncio.create_task(paper_maker_poll_loop())
    asyncio.create_task(heartbeat_loop())

    global arb
    from arb_scanner import ArbScanner
    arb_db = os.path.join(os.path.dirname(DB_PATH) or "/tmp", "arb.db")
    arb = ArbScanner(arb_db, lambda: http, notify)
    asyncio.create_task(arb.universe_loop())
    asyncio.create_task(arb.books_loop())

    await notify(
        f"🤖 Kalshi Bot v3 STARTED [{MODE.upper()}]\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Strategy 1 — CONVERGENCE: final {CONV_MIN_TAU:.0f}–"
        f"{CONV_MAX_TAU:.0f}s of 15M *and* 1H markets, buy p≥"
        f"{CONV_MIN_P} favorites with ≥{CONV_MIN_EV*100:.1f}¢ edge "
        f"(taker or self-expiring maker)\n"
        f"Strategy 2 — LAG: spot bursts z≥{LAG_Z}, take only if book "
        f"lags fair by ≥{LAG_MIN_EV*100:.0f}¢ net of fees\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"⚠️ v3 trades much less often than v2 — that is the point.\n"
        f"Every skipped trade is a bad fill you're not paying for.\n"
        f"Fills, fees and paper results are now real. /start for panel.\n"
        f"🔀 Arb scanner ON (read-only): nominating Kalshi⇄Polymarket "
        f"pairs for your approval; logging fee-adjusted executable locks."
        + (f"\n\n♻️ Restored {len(open_positions)} open position(s)."
           if open_positions else ""))


def main():
    logger.info(f"Kalshi Bot v3 [{MODE}] cryptos={CRYPTOS}")
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("pnl", cmd_pnl))
    app.add_handler(CommandHandler("arb", cmd_arb))
    app.add_handler(CommandHandler("arbpairs", cmd_arbpairs))
    app.add_handler(CommandHandler("arblog", cmd_arblog))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_error_handler(on_tg_error)
    app.post_init = on_startup
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
