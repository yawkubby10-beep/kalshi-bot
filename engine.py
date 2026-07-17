"""
engine.py — Kalshi Bot v3 core engine
======================================
- SpotFeed: Coinbase WebSocket (primary) + KuCoin REST (fallback), sub-second ticks
- VolEstimator: EWMA realized vol (fast + slow) per crypto
- Fair value: P(settle YES) from live spot, strike, time-to-expiry, fat-tail multiplier
- Fees: exact Kalshi schedule (ceil to next cent), taker 0.07 / maker 0.0175, payout $1.00
- MarketMeta: strike + direction parsing (never trade a market we can't price)
- PaperBroker: HONEST simulation — taker walks the real book, maker fills only
  when real trades print through our price (with queue haircut)
- Store: SQLite persistence (survives Railway restarts)
- RiskManager: daily loss halt, concurrency caps, kill switch
"""

import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger("engine")

# ────────────────────────────────────────────────────────────────────────────
# Fees & math  (official Kalshi schedule, effective Feb 5 2026)
#   taker:  ceil_cents(0.07   × C × P × (1−P))
#   maker:  ceil_cents(0.0175 × C × P × (1−P))
#   settlement fee: none — winning contracts pay exactly $1.00
# ────────────────────────────────────────────────────────────────────────────

TAKER_MULT = float(os.getenv("FEE_TAKER_MULT", "0.07"))
MAKER_MULT = float(os.getenv("FEE_MAKER_MULT", "0.0175"))


def _ceil_cents(x: float) -> float:
    return math.ceil(round(x * 100, 6)) / 100.0


def taker_fee(price_d: float, contracts: int) -> float:
    """Total taker fee in dollars for a fill of `contracts` at price_d."""
    if contracts <= 0:
        return 0.0
    return _ceil_cents(TAKER_MULT * contracts * price_d * (1.0 - price_d))


def maker_fee(price_d: float, contracts: int) -> float:
    if contracts <= 0:
        return 0.0
    return _ceil_cents(MAKER_MULT * contracts * price_d * (1.0 - price_d))


def taker_fee_pc(price_d: float) -> float:
    """Per-contract taker fee (un-rounded, for EV math)."""
    return TAKER_MULT * price_d * (1.0 - price_d)


def maker_fee_pc(price_d: float) -> float:
    return MAKER_MULT * price_d * (1.0 - price_d)


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def fair_prob_above(spot: float, strike: float, tau_s: float,
                    sigma_1s: float, tail_mult: float) -> float:
    """P(spot_T > strike) under driftless diffusion with fat-tail widening."""
    if spot <= 0 or strike <= 0:
        return 0.5
    tau_s = max(tau_s, 1.0)
    denom = sigma_1s * math.sqrt(tau_s) * tail_mult
    if denom <= 0:
        return 1.0 if spot > strike else 0.0
    d = math.log(spot / strike) / denom
    d = max(-8.0, min(8.0, d))
    return norm_cdf(d)


# ────────────────────────────────────────────────────────────────────────────
# Volatility estimator — EWMA of per-second variance from tick data
# ────────────────────────────────────────────────────────────────────────────

class VolEstimator:
    """fast: ~60s half-life (pricing) | slow: ~15min half-life (regime/spike)"""

    SIGMA_FLOOR = 2.0e-5    # ≈ 11% annualized
    SIGMA_CAP = 2.0e-3      # ≈ 1120% annualized (chaos guard)

    GRID_S = 10.0          # downsampled price grid step
    GRID_KEEP = 300        # ≈ 50 minutes of history

    def __init__(self):
        prior = (0.55 / math.sqrt(365 * 86400)) ** 2   # ~55% annual vol prior
        self.var_fast = prior
        self.var_slow = prior
        self.last_p: Optional[float] = None
        self.last_t: Optional[float] = None
        self.n_ticks = 0
        self.grid: List[Tuple[float, float]] = []
        self._hcache: Dict[int, Tuple[float, float]] = {}

    def tick(self, t: float, p: float):
        if p <= 0:
            return
        if self.last_p is not None and self.last_t is not None:
            dt = t - self.last_t
            if 0.05 <= dt <= 15.0:
                r = math.log(p / self.last_p)
                sample = (r * r) / dt   # per-second variance sample
                a_f = 0.5 ** (dt / 60.0)    # 60s half-life
                a_s = 0.5 ** (dt / 900.0)   # 15min half-life
                self.var_fast = a_f * self.var_fast + (1 - a_f) * sample
                self.var_slow = a_s * self.var_slow + (1 - a_s) * sample
                self.n_ticks += 1
        self.last_p, self.last_t = p, t
        if not self.grid or t - self.grid[-1][0] >= self.GRID_S:
            self.grid.append((t, p))
            if len(self.grid) > self.GRID_KEEP:
                self.grid.pop(0)

    @property
    def sigma_1s(self) -> float:
        s = math.sqrt(max(self.var_fast, 1e-12))
        return min(max(s, self.SIGMA_FLOOR), self.SIGMA_CAP)

    @property
    def sigma_slow(self) -> float:
        s = math.sqrt(max(self.var_slow, 1e-12))
        return min(max(s, self.SIGMA_FLOOR), self.SIGMA_CAP)

    @property
    def spike_ratio(self) -> float:
        return self.sigma_1s / self.sigma_slow if self.sigma_slow > 0 else 1.0

    def _sigma_grid(self, h: float) -> float:
        """Realized per-√s vol from non-overlapping ~h-second grid returns."""
        if len(self.grid) < 4:
            return 0.0
        now = self.grid[-1][0]
        hit = self._hcache.get(int(h))
        if hit and now - hit[0] < 15.0:
            return hit[1]
        samples = []
        j = len(self.grid) - 1
        while j >= 0 and len(samples) < 24:
            tj, pj = self.grid[j]
            k = j
            while k >= 0 and tj - self.grid[k][0] < h:
                k -= 1
            if k < 0:
                break
            tk, pk = self.grid[k]
            dt = tj - tk
            if dt > 0 and pk > 0 and pj > 0:
                r = math.log(pj / pk)
                samples.append(r * r / dt)
            j = k
        sig = math.sqrt(sum(samples) / len(samples)) if len(samples) >= 4             else 0.0
        self._hcache[int(h)] = (now, sig)
        return sig

    def sigma_for_tau(self, tau: float, include_fast: bool = True) -> float:
        """Per-√s vol matched to horizon tau: MAX of fast/slow EWMA and
        directly-measured 60s / 300s realized vol. 1-second wiggle badly
        underestimates multi-minute movement in trending crypto; taking the
        max across horizons is the conservative (less overconfident) choice.
        """
        cands = [self.sigma_slow]
        if include_fast:
            cands.append(self.sigma_1s)
        for h in (60.0, 300.0):
            if h <= max(tau, 60.0) * 2.0:
                s = self._sigma_grid(h)
                if s > 0:
                    cands.append(s)
        s = max(cands)
        return min(max(s, self.SIGMA_FLOOR), self.SIGMA_CAP)


# ────────────────────────────────────────────────────────────────────────────
# Spot feed — Coinbase WS primary, KuCoin REST fallback
# ────────────────────────────────────────────────────────────────────────────

CB_WS = "wss://ws-feed.exchange.coinbase.com"
CB_PRODUCTS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD"}
KUCOIN_ALL = "https://api.kucoin.com/api/v1/market/allTickers"


class SpotFeed:
    def __init__(self, cryptos: List[str]):
        self.cryptos = cryptos
        self.price: Dict[str, float] = {}
        self.last_ts: Dict[str, float] = {c: 0.0 for c in cryptos}
        self.vol: Dict[str, VolEstimator] = {c: VolEstimator() for c in cryptos}
        self.ticks: Dict[str, List[Tuple[float, float]]] = {c: [] for c in cryptos}
        self._callbacks: List[Callable[[str, float, float], None]] = []
        self._session: Optional[aiohttp.ClientSession] = None
        self.ws_alive = False

    def on_tick(self, cb: Callable[[str, float, float], None]):
        self._callbacks.append(cb)

    def _ingest(self, crypto: str, t: float, p: float):
        if p <= 0:
            return
        self.price[crypto] = p
        self.last_ts[crypto] = t
        self.vol[crypto].tick(t, p)
        buf = self.ticks[crypto]
        buf.append((t, p))
        cutoff = t - 30.0
        while buf and buf[0][0] < cutoff:
            buf.pop(0)
        for cb in self._callbacks:
            try:
                cb(crypto, t, p)
            except Exception:
                logger.exception("tick callback error")

    def ready(self, crypto: str) -> bool:
        fresh = (time.time() - self.last_ts.get(crypto, 0)) < 6.0
        return fresh and self.vol[crypto].n_ticks >= 40

    def move_over(self, crypto: str, lookback_s: float) -> Tuple[float, float]:
        """Returns (log_return, actual_dt) over ~lookback_s using tick buffer."""
        buf = self.ticks[crypto]
        if len(buf) < 2:
            return 0.0, 0.0
        now_t, now_p = buf[-1]
        base = None
        for t, p in buf:
            if now_t - t <= lookback_s:
                base = (t, p)
                break
        if base is None:
            base = buf[0]
        dt = now_t - base[0]
        if dt <= 0.2 or base[1] <= 0:
            return 0.0, 0.0
        return math.log(now_p / base[1]), dt

    async def start(self):
        self._session = aiohttp.ClientSession()
        asyncio.create_task(self._coinbase_loop())
        asyncio.create_task(self._kucoin_fallback_loop())

    async def _coinbase_loop(self):
        sub = {
            "type": "subscribe",
            "product_ids": [CB_PRODUCTS[c] for c in self.cryptos if c in CB_PRODUCTS],
            "channels": ["ticker"],
        }
        rev = {v: k for k, v in CB_PRODUCTS.items()}
        backoff = 1
        while True:
            try:
                async with self._session.ws_connect(CB_WS, heartbeat=20) as ws:
                    await ws.send_json(sub)
                    self.ws_alive = True
                    backoff = 1
                    logger.info("Coinbase WS connected")
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        d = json.loads(msg.data)
                        if d.get("type") == "ticker":
                            c = rev.get(d.get("product_id", ""))
                            if c:
                                try:
                                    self._ingest(c, time.time(), float(d["price"]))
                                except (KeyError, ValueError):
                                    pass
            except Exception as e:
                logger.warning(f"Coinbase WS error: {e}")
            self.ws_alive = False
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _kucoin_fallback_loop(self):
        wanted = {f"{c}-USDT": c for c in self.cryptos}
        while True:
            await asyncio.sleep(2.0)
            now = time.time()
            stale = [c for c in self.cryptos if now - self.last_ts.get(c, 0) > 5.0]
            if not stale:
                continue
            try:
                async with self._session.get(
                    KUCOIN_ALL, timeout=aiohttp.ClientTimeout(total=5)
                ) as r:
                    data = await r.json()
                for tkr in data.get("data", {}).get("ticker", []):
                    c = wanted.get(tkr.get("symbol", ""))
                    if c and c in stale:
                        try:
                            self._ingest(c, time.time(), float(tkr.get("last", 0)))
                        except ValueError:
                            pass
            except Exception as e:
                logger.debug(f"KuCoin fallback error: {e}")


# ────────────────────────────────────────────────────────────────────────────
# Orderbook parsing — CORRECT Kalshi semantics
#   book["yes"] = resting YES *bids*   [[price_cents, count], ...]
#   book["no"]  = resting NO  *bids*
#   To BUY YES you consume NO bids:  yes_ask = 100 − best_no_bid
#   To BUY NO  you consume YES bids: no_ask  = 100 − best_yes_bid
# ────────────────────────────────────────────────────────────────────────────

def parse_book(raw: dict) -> dict:
    """Parse a Kalshi orderbook (fp-first).

    Canonical format (docs.kalshi.com): {"orderbook_fp": {"yes_dollars":
    [[price_dollars, count_fp], ...], "no_dollars": [...]}} — BOTH arrays are
    BIDS (the API never returns asks; they're derived from the other side).
    Prices may be sub-penny (e.g. "0.995"). We floor bids to whole cents
    (conservative: a 99.5¢ bid is treated as 99¢) and merge levels.
    Legacy cents format is still accepted as a fallback.
    """
    outer = raw or {}
    legacy = outer.get("orderbook") if isinstance(
        outer.get("orderbook"), dict) else {}
    fp = outer.get("orderbook_fp") or (legacy or {}).get("orderbook_fp") or {}

    def _emit(acc: dict, p_cents_f: float, c: int):
        pc = int(math.floor(p_cents_f + 1e-9))
        if 1 <= pc <= 99 and c > 0:
            acc[pc] = acc.get(pc, 0) + c

    def norm_fp(levels) -> List[Tuple[int, int]]:
        acc: dict = {}
        for lv in levels or []:
            try:
                if isinstance(lv, (list, tuple)) and len(lv) >= 2:
                    p_d, c = float(lv[0]), int(float(lv[1]))
                elif isinstance(lv, dict):
                    p_d = float(lv.get("price",
                                       lv.get("price_dollars", 0)))
                    c = int(float(lv.get("quantity",
                                         lv.get("count",
                                                lv.get("count_fp", 0)))))
                else:
                    continue
                _emit(acc, p_d * 100.0, c)
            except (TypeError, ValueError):
                continue
        return sorted(acc.items(), key=lambda x: -x[0])

    def norm_legacy(levels) -> List[Tuple[int, int]]:
        acc: dict = {}
        for lv in levels or []:
            try:
                if isinstance(lv, (list, tuple)) and len(lv) >= 2:
                    p, c = float(lv[0]), int(float(lv[1]))
                elif isinstance(lv, dict):
                    p = float(lv.get("price", 0))
                    c = int(float(lv.get("quantity", lv.get("count", 0))))
                else:
                    continue
                _emit(acc, p * 100.0 if 0 < p < 1.0 else p, c)
            except (TypeError, ValueError):
                continue
        return sorted(acc.items(), key=lambda x: -x[0])

    if fp and (fp.get("yes_dollars") or fp.get("no_dollars")):
        yes_bids = norm_fp(fp.get("yes_dollars"))
        no_bids = norm_fp(fp.get("no_dollars"))
    else:
        yes_bids = norm_legacy((legacy or {}).get("yes"))
        no_bids = norm_legacy((legacy or {}).get("no"))
        if not yes_bids and not no_bids and fp:
            yes_bids = norm_fp(fp.get("yes"))
            no_bids = norm_fp(fp.get("no"))

    # Ask ladders (ascending price) derived from opposite-side bids
    yes_asks = sorted([(100 - p, c) for p, c in no_bids], key=lambda x: x[0])
    no_asks = sorted([(100 - p, c) for p, c in yes_bids], key=lambda x: x[0])

    return {
        "yes_bid": yes_bids[0][0] if yes_bids else 0,
        "no_bid": no_bids[0][0] if no_bids else 0,
        "yes_ask": yes_asks[0][0] if yes_asks else 100,
        "no_ask": no_asks[0][0] if no_asks else 100,
        "yes_asks": yes_asks,   # ladder to BUY YES
        "no_asks": no_asks,     # ladder to BUY NO
        "yes_bids": yes_bids,
        "no_bids": no_bids,
    }


def depth_at(ask_ladder: List[Tuple[int, int]], limit_c: int) -> int:
    return sum(c for p, c in ask_ladder if p <= limit_c)


def walk_book(ask_ladder: List[Tuple[int, int]], limit_c: int,
              want: int) -> Tuple[int, float]:
    """Simulated IOC: fill up to `want` at prices ≤ limit_c. → (filled, vwap_c)"""
    filled, cost = 0, 0.0
    for p, c in ask_ladder:
        if p > limit_c or filled >= want:
            break
        take = min(c, want - filled)
        filled += take
        cost += take * p
    return filled, (cost / filled if filled else 0.0)


# ────────────────────────────────────────────────────────────────────────────
# Market metadata — strike & direction. Refuse to trade what we can't price.
# ────────────────────────────────────────────────────────────────────────────

_NUM_RE = re.compile(r"\$?([\d,]+(?:\.\d+)?)")


# Kalshi crypto markets settle on a 60-second AVERAGE of the CF Benchmarks
# Real-Time Index (sampled 1/s over the final minute). The variance of that
# average equals the variance of a point price ~40s earlier, so the effective
# diffusion horizon is tau − 40s.
SETTLE_TWAP_ADJUST_S = float(os.getenv("SETTLE_TWAP_ADJUST_S", "40"))


class MarketMeta:
    __slots__ = ("ticker", "crypto", "series", "kind", "strike",
                 "close_ts", "raw")

    def __init__(self, ticker, crypto, series, kind, strike, close_ts, raw):
        self.ticker = ticker
        self.crypto = crypto
        self.series = series
        self.kind = kind          # "above" | "below"
        self.strike = strike
        self.close_ts = close_ts
        self.raw = raw

    def tau(self, now: Optional[float] = None) -> float:
        return max(0.0, self.close_ts - (now or time.time()))

    def p_yes(self, spot: float, sigma_1s: float, tail: float,
              now: Optional[float] = None) -> float:
        tau_eff = max(self.tau(now) - SETTLE_TWAP_ADJUST_S, 1.0)
        p_above = fair_prob_above(spot, self.strike, tau_eff,
                                  sigma_1s, tail)
        return p_above if self.kind == "above" else 1.0 - p_above


def parse_market(m: dict, crypto: str, series: str) -> Optional[MarketMeta]:
    ticker = m.get("ticker", "")
    close_time = m.get("close_time") or m.get("expected_expiration_time") or ""
    try:
        close_ts = datetime.fromisoformat(
            close_time.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None

    st = (m.get("strike_type") or "").lower()
    floor_s = m.get("floor_strike")
    cap_s = m.get("cap_strike")

    kind, strike = None, None
    if st in ("greater", "greater_or_equal") and floor_s:
        kind, strike = "above", float(floor_s)
    elif st in ("less", "less_or_equal") and cap_s:
        kind, strike = "below", float(cap_s)
    elif st in ("between", "custom"):
        return None   # range markets: skip, we don't price them
    elif floor_s and not cap_s:
        kind, strike = "above", float(floor_s)
    elif cap_s and not floor_s:
        kind, strike = "below", float(cap_s)
    else:
        # Last resort: parse subtitle text like "$118,250 or above"
        sub = " ".join(str(m.get(k, "")) for k in
                       ("yes_sub_title", "subtitle", "title")).lower()
        mt = _NUM_RE.search(sub)
        if mt:
            try:
                strike = float(mt.group(1).replace(",", ""))
            except ValueError:
                strike = None
        if strike:
            if "below" in sub or "under" in sub or "less" in sub:
                kind = "below"
            elif "above" in sub or "over" in sub or "up" in sub or "higher" in sub:
                kind = "above"
    if not kind or not strike or strike <= 0:
        return None
    return MarketMeta(ticker, crypto, series, kind, strike, close_ts, m)


# ────────────────────────────────────────────────────────────────────────────
# SQLite store
# ────────────────────────────────────────────────────────────────────────────

class Store:
    def __init__(self, path: str):
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS trades(
            id TEXT PRIMARY KEY, ts REAL, mode TEXT, strategy TEXT,
            crypto TEXT, ticker TEXT, side TEXT, entry_cents REAL,
            contracts INTEGER, cost REAL, fee REAL, status TEXT,
            result TEXT, pnl REAL, model_p REAL, tau REAL,
            exit_reason TEXT, close_ts REAL, kind TEXT, strike REAL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS kv(
            k TEXT PRIMARY KEY, v REAL)""")
        self.db.commit()

    def insert_trade(self, t: dict):
        with self.lock:
            self.db.execute(
                """INSERT OR REPLACE INTO trades
                (id,ts,mode,strategy,crypto,ticker,side,entry_cents,contracts,
                 cost,fee,status,result,pnl,model_p,tau,exit_reason,close_ts,
                 kind,strike)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (t["id"], t["ts"], t["mode"], t["strategy"], t["crypto"],
                 t["ticker"], t["side"], t["entry_cents"], t["contracts"],
                 t["cost"], t["fee"], t["status"], t.get("result", ""),
                 t.get("pnl", 0.0), t.get("model_p", 0.0), t.get("tau", 0.0),
                 t.get("exit_reason", ""), t.get("close_ts", 0.0),
                 t.get("kind", ""), t.get("strike", 0.0)))
            self.db.commit()

    def close_trade(self, tid: str, result: str, pnl: float, reason: str):
        with self.lock:
            self.db.execute(
                "UPDATE trades SET status='closed', result=?, pnl=?, "
                "exit_reason=? WHERE id=?", (result, pnl, reason, tid))
            self.db.commit()

    def open_trades(self, mode: str) -> List[dict]:
        with self.lock:
            cur = self.db.execute(
                "SELECT id,ts,strategy,crypto,ticker,side,entry_cents,"
                "contracts,cost,fee,model_p,tau,close_ts,kind,strike "
                "FROM trades WHERE status='open' AND mode=?", (mode,))
            cols = ["id", "ts", "strategy", "crypto", "ticker", "side",
                    "entry_cents", "contracts", "cost", "fee", "model_p",
                    "tau", "close_ts", "kind", "strike"]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def summary(self, mode: str, since_ts: float = 0.0) -> dict:
        with self.lock:
            cur = self.db.execute(
                "SELECT pnl, fee, cost, result, strategy, crypto, model_p "
                "FROM trades WHERE status='closed' AND mode=? AND ts>=?",
                (mode, since_ts))
            rows = cur.fetchall()
        n = len(rows)
        if not n:
            return {"trades": 0}
        pnls = [r[0] for r in rows]
        wins = sum(1 for r in rows if r[0] > 0)
        total = sum(pnls)
        fees = sum(r[1] for r in rows)
        staked = sum(r[2] for r in rows)
        avg = total / n
        std = (sum((p - avg) ** 2 for p in pnls) / (n - 1)) ** 0.5 if n > 1 else 0
        wr = wins / n
        z = 1.96
        den = 1 + z * z / n
        ci_h = z * math.sqrt(wr * (1 - wr) / n + z * z / (4 * n * n)) / den
        mid = (wr + z * z / (2 * n)) / den
        by_strat, by_crypto = {}, {}
        for pnl, _f, _c, res, strat, cr, _mp in rows:
            s = by_strat.setdefault(strat, {"n": 0, "w": 0, "pnl": 0.0})
            s["n"] += 1
            s["w"] += 1 if pnl > 0 else 0
            s["pnl"] += pnl
            c = by_crypto.setdefault(cr, {"n": 0, "w": 0, "pnl": 0.0})
            c["n"] += 1
            c["w"] += 1 if pnl > 0 else 0
            c["pnl"] += pnl
        return {
            "trades": n, "wins": wins, "losses": n - wins,
            "win_rate": round(wr * 100, 1),
            "wr_ci": f"{max(0,(mid-ci_h))*100:.1f}%–{min(1,(mid+ci_h))*100:.1f}%",
            "pnl": round(total, 2), "fees": round(fees, 2),
            "staked": round(staked, 2),
            "roi": round(total / staked * 100, 1) if staked else 0.0,
            "sharpe": round(avg / std, 2) if std > 0 else 0.0,
            "avg": round(avg, 3),
            "by_strategy": by_strat, "by_crypto": by_crypto,
            "model_p_avg": round(
                sum(r[6] for r in rows) / n * 100, 1),
        }

    def daily_pnl(self, mode: str) -> float:
        day0 = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0).timestamp()
        with self.lock:
            cur = self.db.execute(
                "SELECT COALESCE(SUM(pnl),0) FROM trades WHERE status='closed' "
                "AND mode=? AND ts>=?", (mode, day0))
            return float(cur.fetchone()[0] or 0.0)

    def set_kv(self, key: str, val: float):
        with self.lock:
            self.db.execute(
                "INSERT INTO kv(k,v) VALUES(?,?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, val))
            self.db.commit()

    def incr(self, key: str, by: float = 1.0):
        with self.lock:
            self.db.execute(
                "INSERT INTO kv(k,v) VALUES(?,?) "
                "ON CONFLICT(k) DO UPDATE SET v=v+excluded.v", (key, by))
            self.db.commit()

    def get_kv(self, key: str) -> float:
        with self.lock:
            cur = self.db.execute("SELECT v FROM kv WHERE k=?", (key,))
            r = cur.fetchone()
            return float(r[0]) if r else 0.0


# ────────────────────────────────────────────────────────────────────────────
# Risk manager
# ────────────────────────────────────────────────────────────────────────────

class RiskManager:
    def __init__(self, store: Store, mode: str, max_daily_loss: float,
                 max_concurrent: int, per_crypto: int):
        self.store = store
        self.mode = mode
        self.max_daily_loss = max_daily_loss
        self.max_concurrent = max_concurrent
        self.per_crypto = per_crypto
        self.kill = False
        self.halted_reason = ""

    def check(self, crypto: str, open_positions: List[dict]) -> Tuple[bool, str]:
        if self.kill:
            return False, "kill switch"
        dp = self.store.daily_pnl(self.mode)
        if dp <= -abs(self.max_daily_loss):
            self.halted_reason = f"daily loss limit (${dp:+.2f})"
            return False, self.halted_reason
        if len(open_positions) >= self.max_concurrent:
            return False, "max concurrent"
        if sum(1 for p in open_positions
               if p["crypto"] == crypto) >= self.per_crypto:
            return False, f"{crypto} position cap"
        return True, ""


# ────────────────────────────────────────────────────────────────────────────
# Blackout windows (UTC "HH:MM-HH:MM,HH:MM-HH:MM")
# ────────────────────────────────────────────────────────────────────────────

def parse_blackouts(spec: str) -> List[Tuple[int, int]]:
    out = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part or "-" not in part:
            continue
        try:
            a, b = part.split("-")
            h1, m1 = map(int, a.split(":"))
            h2, m2 = map(int, b.split(":"))
            out.append((h1 * 60 + m1, h2 * 60 + m2))
        except ValueError:
            continue
    return out


def in_blackout(blackouts: List[Tuple[int, int]]) -> bool:
    now = datetime.now(timezone.utc)
    cur = now.hour * 60 + now.minute
    return any(a <= cur <= b for a, b in blackouts)


# ────────────────────────────────────────────────────────────────────────────
# Honest paper broker
# ────────────────────────────────────────────────────────────────────────────

KALSHI_BASE = "https://api.elections.kalshi.com"
QUEUE_FACTOR_AT_PRICE = 0.4   # assume we capture 40% of volume printing AT our price


class PaperMakerOrder:
    __slots__ = ("oid", "ticker", "want", "price_c", "count", "filled",
                 "created", "expires", "meta")

    def __init__(self, ticker, want, price_c, count, ttl_s, meta):
        self.oid = str(uuid.uuid4())
        self.ticker = ticker
        self.want = want            # "yes" | "no"
        self.price_c = price_c      # price in that side's cents
        self.count = count
        self.filled = 0
        self.created = time.time()
        self.expires = self.created + ttl_s
        self.meta = meta


class PaperBroker:
    """Fills only what the real market would have given us."""

    def __init__(self, session_getter: Callable[[], aiohttp.ClientSession]):
        self._session = session_getter
        self.makers: Dict[str, PaperMakerOrder] = {}
        self._last_trade_ts: Dict[str, str] = {}
        self.on_maker_fill: Optional[Callable] = None
        self.poll_ok = 0
        self.poll_err = 0

    # taker: caller supplies a freshly fetched parsed book
    @staticmethod
    def taker_ioc(parsed: dict, want: str, limit_c: int,
                  count: int) -> Tuple[int, float]:
        ladder = parsed["yes_asks"] if want == "yes" else parsed["no_asks"]
        return walk_book(ladder, limit_c, count)

    def place_maker(self, ticker: str, want: str, price_c: int, count: int,
                    ttl_s: float, meta: dict) -> PaperMakerOrder:
        o = PaperMakerOrder(ticker, want, price_c, count, ttl_s, meta)
        self.makers[o.oid] = o
        return o

    async def tick(self):
        """Poll public trade prints; fill resting paper orders honestly."""
        if not self.makers:
            return
        now = time.time()
        by_ticker: Dict[str, List[PaperMakerOrder]] = {}
        for o in list(self.makers.values()):
            if now >= o.expires:
                del self.makers[o.oid]
                logger.info(f"[PAPER maker] expired unfilled {o.ticker} "
                            f"{o.want}@{o.price_c}¢ ({o.filled}/{o.count})")
                continue
            by_ticker.setdefault(o.ticker, []).append(o)

        sess = self._session()
        for ticker, orders in by_ticker.items():
            try:
                async with sess.get(
                    f"{KALSHI_BASE}/trade-api/v2/markets/trades",
                    params={"ticker": ticker, "limit": 100},
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as r:
                    if r.status != 200:
                        self.poll_err += 1
                        logger.warning(f"trade tape {ticker} HTTP {r.status}")
                        continue
                    data = await r.json()
                    self.poll_ok += 1
            except Exception as e:
                self.poll_err += 1
                logger.debug(f"paper trades poll error {ticker}: {e}")
                continue
            trades = data.get("trades", []) or []
            for o in orders:
                vol_through, vol_at = 0, 0
                for tr in trades:
                    try:
                        ct = datetime.fromisoformat(
                            tr["created_time"].replace("Z", "+00:00")
                        ).timestamp()
                    except Exception:
                        continue
                    if ct < o.created:
                        continue
                    yp = int(tr.get("yes_price", 0))
                    cnt = int(tr.get("count", 0))
                    px = yp if o.want == "yes" else 100 - yp
                    if px < o.price_c:
                        vol_through += cnt
                    elif px == o.price_c:
                        vol_at += cnt
                avail = vol_through + int(vol_at * QUEUE_FACTOR_AT_PRICE)
                new_fill = min(o.count - o.filled, max(0, avail - o.filled))
                if new_fill > 0:
                    o.filled += new_fill
                    logger.info(f"[PAPER maker] FILL {o.ticker} {o.want}"
                                f"@{o.price_c}¢ ×{new_fill} "
                                f"({o.filled}/{o.count})")
                    if self.on_maker_fill:
                        try:
                            await self.on_maker_fill(o, new_fill)
                        except Exception:
                            logger.exception("maker fill callback error")
                    if o.filled >= o.count:
                        self.makers.pop(o.oid, None)
