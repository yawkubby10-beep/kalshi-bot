"""
ARB SCANNER — Kalshi ⇄ Polymarket cross-venue lock detector (READ-ONLY)
========================================================================
Finds pairs of markets on Kalshi and Polymarket that price the SAME event
differently, and logs every fee-adjusted, size-executable lock it sees.
It never trades. Its job is to turn "arb profit" from a projection into a
count.

Doctrine (each rule bought with tuition elsewhere in the fleet):
- HUMAN-VERIFIED pairs only. Text matching only nominates candidates; you
  approve each pair once in Telegram after checking both rule pages.
  Resolution-criteria mismatch is the strategy's only real risk, so no
  machine gets to decide two markets are "the same".
- Crypto price-threshold markets are excluded outright (Kalshi settles on
  CF Benchmarks, Polymarket on other feeds — same words, different oracle).
- Edges are computed from EXECUTABLE asks with visible size on both books,
  net of Kalshi's real taker fee and a conservative Polymarket fee model.
  Mid-price fantasies don't get logged.
"""

import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

from engine import taker_fee as kalshi_taker_fee

logger = logging.getLogger(__name__)

GAMMA = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"

ARB_MIN_EDGE = float(os.getenv("ARB_MIN_EDGE", "0.010"))      # log at ≥1%
ARB_ALERT_EDGE = float(os.getenv("ARB_ALERT_EDGE", "0.020"))  # ping at ≥2%
ARB_MIN_SIZE = float(os.getenv("ARB_MIN_SIZE", "20"))         # shares/contracts
ARB_MAX_PAIR = float(os.getenv("ARB_MAX_PAIR", "300"))
PM_FEE_BPS_DEFAULT = float(os.getenv("PM_FEE_BPS_DEFAULT", "250"))
BOOK_INTERVAL = float(os.getenv("ARB_BOOK_INTERVAL", "25"))
UNIVERSE_INTERVAL = float(os.getenv("ARB_UNIVERSE_INTERVAL", "900"))
ALERT_COOLDOWN = float(os.getenv("ARB_ALERT_COOLDOWN", "1800"))
MAX_PENDING = 40

STOP_WORDS = {"will", "the", "be", "in", "on", "at", "of", "a", "an", "to",
              "by", "for", "or", "and", "vs", "market", "before", "after",
              "with", "is", "does", "do", "than", "more", "less", "who",
              "what", "how", "many", "much", "yes", "no", "2026", "2025"}
MONTHS = ("january february march april may june july august september "
          "october november december jan feb mar apr jun jul aug sep sept "
          "oct nov dec").split()
CRYPTO_BLOCK = {"bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp",
                "dogecoin", "doge", "cardano", "ada"}


# ── text matching primitives ──────────────────────────────────────────────

def extract_numbers(text: str) -> frozenset:
    """Threshold-style numbers only. Years are handled separately —
    'September 2026' on one venue vs 'September' on the other must not
    kill a valid pair."""
    out = set()
    for m in re.finditer(r"(\d[\d,]*\.?\d*)\s*([kKmM%]?)", text):
        try:
            v = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        suf = m.group(2).lower()
        if suf == "k":
            v *= 1_000
        elif suf == "m":
            v *= 1_000_000
        if not suf and v.is_integer() and 2024 <= v <= 2035:
            continue                      # a year, not a threshold
        out.add(round(v, 6))
    return frozenset(out)


def extract_years(text: str) -> frozenset:
    return frozenset(int(y) for y in re.findall(r"\b(20[2-3]\d)\b", text))


def _compatible(a: frozenset, b: frozenset) -> bool:
    """Equal, or absent on one side (venues phrase dates differently)."""
    return a == b or not a or not b


def extract_months(text: str) -> frozenset:
    low = text.lower()
    got = set()
    for i, name in enumerate(MONTHS):
        if re.search(rf"\b{name}\b", low):
            got.add(name[:3] if len(name) > 3 else name)
    canon = {"jan": "jan", "feb": "feb", "mar": "mar", "apr": "apr",
             "may": "may", "jun": "jun", "jul": "jul", "aug": "aug",
             "sep": "sep", "sept": "sep", "oct": "oct", "nov": "nov",
             "dec": "dec"}
    return frozenset(canon.get(g, g) for g in got)


def tokens_of(text: str) -> frozenset:
    words = re.findall(r"[a-zA-Z]{2,}", text.lower())
    return frozenset(w for w in words
                     if w not in STOP_WORDS and w not in MONTHS)


def match_score(title_a: str, title_b: str) -> float:
    """0.0 = no match. Requires identical number sets AND identical month
    sets; score is then the Jaccard overlap of remaining tokens."""
    if extract_numbers(title_a) != extract_numbers(title_b):
        return 0.0                    # thresholds must match exactly
    if not _compatible(extract_months(title_a), extract_months(title_b)):
        return 0.0
    if not _compatible(extract_years(title_a), extract_years(title_b)):
        return 0.0
    ta, tb = tokens_of(title_a), tokens_of(title_b)
    if not ta or not tb:
        return 0.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return inter / union if union else 0.0


def is_crypto_price_market(title: str) -> bool:
    toks = tokens_of(title)
    return bool(toks & CRYPTO_BLOCK) and bool(extract_numbers(title))


def template_key(k_ticker: str, pm_slug: str) -> str:
    series = k_ticker.split("-")[0]
    slug_t = re.sub(r"\d+", "#", pm_slug)
    return f"{series}|{slug_t}"


# ── fee + edge math ──────────────────────────────────────────────────────

def pm_fee_usd(shares: float, price: float, fee_bps: float) -> float:
    return (fee_bps / 10_000.0) * shares * price * (1.0 - price)


def lock_edge(k_price: float, pm_price: float, size: float,
              pm_fee_bps: float) -> Tuple[float, float]:
    """Buy one side on Kalshi at k_price + the complement on Polymarket at
    pm_price. Returns (edge_per_share, profit_at_size), fee-adjusted.
    Guaranteed payout is $1.00 per pair if the pair truly matches."""
    if size <= 0 or k_price <= 0 or pm_price <= 0:
        return 0.0, 0.0
    gross = 1.0 - (k_price + pm_price)
    k_fee = kalshi_taker_fee(k_price, int(size))
    p_fee = pm_fee_usd(size, pm_price, pm_fee_bps)
    profit = gross * size - k_fee - p_fee
    return (profit / size if size else 0.0), profit


def parse_kalshi_book(raw: dict) -> Optional[dict]:
    """Kalshi orderbook_fp: BOTH arrays are BIDS (yes_dollars / no_dollars).
    Asks derive from the opposite side: yes_ask = 1 − best_no_bid, and the
    size you can take at that ask is the no-bid's size."""
    ob = (raw or {}).get("orderbook", {})
    fp = ob.get("orderbook_fp") or ob
    yes = fp.get("yes_dollars") or fp.get("yes") or []
    no = fp.get("no_dollars") or fp.get("no") or []

    def best(levels):
        b_px, b_sz = 0.0, 0.0
        for lv in levels:
            try:
                px, sz = float(lv[0]), float(lv[1])
            except (TypeError, ValueError, IndexError):
                continue
            if px > b_px:
                b_px, b_sz = px, sz
        return b_px, b_sz

    yb, ybs = best(yes)
    nb, nbs = best(no)
    if yb <= 0 and nb <= 0:
        return None
    return {
        "yes_ask": (1.0 - nb) if nb > 0 else 1.0, "yes_ask_sz": nbs,
        "no_ask": (1.0 - yb) if yb > 0 else 1.0, "no_ask_sz": ybs,
    }


def parse_pm_book(raw: dict) -> Optional[dict]:
    asks = []
    for lv in (raw or {}).get("asks", []):
        try:
            asks.append((float(lv["price"]), float(lv["size"])))
        except (TypeError, ValueError, KeyError):
            continue
    if not asks:
        return None
    asks.sort(key=lambda x: x[0])
    return {"ask": asks[0][0], "ask_sz": asks[0][1]}


# ── the scanner ──────────────────────────────────────────────────────────

class ArbScanner:
    def __init__(self, db_path: str, session_getter, notify_fn,
                 kalshi_client=None):
        # Kalshi market data is PUBLIC — no auth needed for the scanner.
        # (Paper-mode kalshi-bot has been reading it authless all along;
        # the authed client stays optional for a future execution phase.)
        self.kc = kalshi_client
        self._sess = session_getter
        self.notify = notify_fn
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS pairs(
            id INTEGER PRIMARY KEY, k_ticker TEXT, k_title TEXT,
            pm_slug TEXT, pm_cond TEXT, pm_title TEXT,
            pm_yes_tid TEXT, pm_no_tid TEXT, pm_fee_bps REAL,
            score REAL, template TEXT, status TEXT DEFAULT 'pending',
            created REAL, UNIQUE(k_ticker, pm_slug));
        CREATE TABLE IF NOT EXISTS sightings(
            id INTEGER PRIMARY KEY, ts REAL, pair_id INTEGER,
            direction TEXT, k_price REAL, pm_price REAL,
            size REAL, edge REAL, profit REAL, alerted INTEGER DEFAULT 0);
        """)
        self.db.commit()
        self._last_alert: Dict[int, float] = {}
        self.stats = {"k_markets": 0, "pm_markets": 0, "checks": 0,
                      "last_universe": 0.0}

    # ── universe refresh + candidate nomination ─────────────────────────
    async def universe_loop(self):
        await asyncio.sleep(20)
        while True:
            try:
                await self._refresh_universe()
            except Exception as e:
                logger.error(f"arb universe: {e}", exc_info=True)
            await asyncio.sleep(UNIVERSE_INTERVAL)

    async def _refresh_universe(self):
        k_markets = await self._kalshi_universe()
        pm_markets = await self._pm_universe()
        self.stats.update(k_markets=len(k_markets),
                          pm_markets=len(pm_markets),
                          last_universe=time.time())
        approved_templates = {
            r["template"] for r in self.db.execute(
                "SELECT template FROM pairs WHERE status='approved'")}
        pending = self.db.execute(
            "SELECT COUNT(*) c FROM pairs WHERE status='pending'"
        ).fetchone()["c"]
        new = 0
        for km in k_markets:
            if pending + new >= MAX_PENDING:
                break
            kt = km["title"]
            if is_crypto_price_market(kt):
                continue
            for pm in pm_markets:
                if is_crypto_price_market(pm["title"]):
                    continue
                s = match_score(kt, pm["title"])
                if s < 0.45:
                    continue
                tpl = template_key(km["ticker"], pm["slug"])
                try:
                    cur = self.db.execute(
                        "INSERT OR IGNORE INTO pairs(k_ticker,k_title,"
                        "pm_slug,pm_cond,pm_title,pm_yes_tid,pm_no_tid,"
                        "pm_fee_bps,score,template,created) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (km["ticker"], kt, pm["slug"], pm["cond"],
                         pm["title"], pm["yes_tid"], pm["no_tid"],
                         pm["fee_bps"], s, tpl, time.time()))
                    if cur.rowcount:
                        new += 1
                        note = (" (template previously approved ✅)"
                                if tpl in approved_templates else "")
                        await self.notify(
                            f"🔀 ARB CANDIDATE #{cur.lastrowid} "
                            f"(match {s:.0%}){note}\n"
                            f"K: {kt}\nP: {pm['title']}\n"
                            f"Verify both rule pages, then /arbpairs "
                            f"to approve or reject.")
                except Exception as e:
                    logger.debug(f"pair insert: {e}")
        self.db.commit()
        if new:
            logger.info(f"arb: {new} new candidate pair(s)")

    async def _kalshi_universe(self) -> List[dict]:
        out, cursor = [], None
        sess = self._sess()
        for _ in range(8):
            params = {"status": "open", "limit": 500}
            if cursor:
                params["cursor"] = cursor
            try:
                async with sess.get(f"{KALSHI_BASE}/markets", params=params,
                                    timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        logger.warning(f"kalshi markets HTTP {r.status}")
                        break
                    resp = await r.json()
            except Exception as e:
                logger.debug(f"kalshi universe: {e}")
                break
            for m in resp.get("markets", []):
                try:
                    if float(m.get("volume") or 0) < 50:
                        continue
                    out.append({"ticker": m["ticker"],
                                "title": m.get("title") or ""})
                except (TypeError, KeyError):
                    continue
            cursor = resp.get("cursor")
            if not cursor:
                break
        return out

    async def _kalshi_orderbook(self, ticker: str) -> Optional[dict]:
        sess = self._sess()
        try:
            async with sess.get(f"{KALSHI_BASE}/markets/{ticker}/orderbook",
                                params={"depth": 16},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return None
                return await r.json()
        except Exception as e:
            logger.debug(f"kalshi book {ticker}: {e}")
            return None

    async def _pm_universe(self) -> List[dict]:
        out = []
        sess = self._sess()
        for offset in (0, 500):
            try:
                async with sess.get(
                        GAMMA, params={"closed": "false", "limit": 500,
                                       "offset": offset, "order": "volumeNum",
                                       "ascending": "false"},
                        timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        break
                    data = await r.json()
            except Exception as e:
                logger.debug(f"gamma universe: {e}")
                break
            for m in data or []:
                try:
                    if float(m.get("liquidityNum") or 0) < 500:
                        continue
                    tids = m.get("clobTokenIds")
                    if isinstance(tids, str):
                        tids = json.loads(tids)
                    if not tids or len(tids) < 2:
                        continue
                    fee_bps = (PM_FEE_BPS_DEFAULT
                               if m.get("feesEnabled") else 0.0)
                    out.append({"slug": m.get("slug", ""),
                                "cond": m.get("conditionId", ""),
                                "title": m.get("question", ""),
                                "yes_tid": tids[0], "no_tid": tids[1],
                                "fee_bps": fee_bps})
                except Exception:
                    continue
            if not data or len(data) < 500:
                break
        return out

    # ── executable-lock checks on approved pairs ────────────────────────
    async def books_loop(self):
        await asyncio.sleep(35)
        while True:
            try:
                rows = self.db.execute(
                    "SELECT * FROM pairs WHERE status='approved'").fetchall()
                for p in rows:
                    await self._check_pair(p)
                    self.stats["checks"] += 1
            except Exception as e:
                logger.error(f"arb books: {e}", exc_info=True)
            await asyncio.sleep(BOOK_INTERVAL)

    async def _check_pair(self, p):
        raw_k = await self._kalshi_orderbook(p["k_ticker"])
        if raw_k is None:
            return
        kb = parse_kalshi_book(raw_k)
        if not kb:
            return
        sess = self._sess()
        pm_yes = pm_no = None
        for tid, slot in ((p["pm_yes_tid"], "yes"), (p["pm_no_tid"], "no")):
            try:
                async with sess.get(CLOB_BOOK, params={"token_id": tid},
                                    timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        parsed = parse_pm_book(await r.json())
                        if slot == "yes":
                            pm_yes = parsed
                        else:
                            pm_no = parsed
            except Exception:
                pass
        now = time.time()
        for direction, k_px, k_sz, pmb in (
                ("K_YES+PM_NO", kb["yes_ask"], kb["yes_ask_sz"], pm_no),
                ("K_NO+PM_YES", kb["no_ask"], kb["no_ask_sz"], pm_yes)):
            if not pmb:
                continue
            size = min(k_sz, pmb["ask_sz"], ARB_MAX_PAIR)
            if size < ARB_MIN_SIZE:
                continue
            edge, profit = lock_edge(k_px, pmb["ask"], size, p["pm_fee_bps"])
            if edge < ARB_MIN_EDGE:
                continue
            alerted = 0
            if (edge >= ARB_ALERT_EDGE
                    and now - self._last_alert.get(p["id"], 0)
                    > ALERT_COOLDOWN):
                self._last_alert[p["id"]] = now
                alerted = 1
                await self.notify(
                    f"🔒 EXECUTABLE LOCK {edge:.1%} — pair #{p['id']}\n"
                    f"{p['k_title']}\n"
                    f"{direction}: Kalshi @{k_px:.2f} + Poly "
                    f"@{pmb['ask']:.2f} ×{size:.0f}\n"
                    f"combined {k_px + pmb['ask']:.3f} → locked "
                    f"${profit:.2f} at resolution (fee-adj, READ-ONLY log)")
            self.db.execute(
                "INSERT INTO sightings(ts,pair_id,direction,k_price,"
                "pm_price,size,edge,profit,alerted) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (now, p["id"], direction, k_px, pmb["ask"], size, edge,
                 profit, alerted))
            self.db.commit()

    # ── telegram surfaces ───────────────────────────────────────────────
    def approve(self, pid: int) -> bool:
        c = self.db.execute("UPDATE pairs SET status='approved' "
                            "WHERE id=? AND status='pending'", (pid,))
        self.db.commit()
        return bool(c.rowcount)

    def reject(self, pid: int) -> bool:
        c = self.db.execute("UPDATE pairs SET status='rejected' "
                            "WHERE id=? AND status='pending'", (pid,))
        self.db.commit()
        return bool(c.rowcount)

    def pending_pairs(self, limit: int = 6) -> List[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM pairs WHERE status='pending' "
            "ORDER BY score DESC, id LIMIT ?", (limit,)).fetchall()

    def txt_status(self) -> str:
        c = {r["status"]: r["c"] for r in self.db.execute(
            "SELECT status, COUNT(*) c FROM pairs GROUP BY status")}
        day = time.time() - 86400
        s = self.db.execute(
            "SELECT COUNT(*) n, MAX(edge) best, SUM(profit) tot "
            "FROM sightings WHERE ts>?", (day,)).fetchone()
        week = self.db.execute(
            "SELECT COUNT(*) n, SUM(profit) tot FROM sightings "
            "WHERE ts>? AND edge>=?",
            (time.time() - 7 * 86400, ARB_ALERT_EDGE)).fetchone()
        age = ((time.time() - self.stats["last_universe"]) / 60
               if self.stats["last_universe"] else -1)
        return (
            "🔀 ARB SCANNER [read-only]\n━━━━━━━━━━━━\n"
            f"Universe: {self.stats['k_markets']} Kalshi × "
            f"{self.stats['pm_markets']} Poly (refreshed "
            f"{age:.0f}m ago)\n"
            f"Pairs: {c.get('approved', 0)} approved | "
            f"{c.get('pending', 0)} pending | {c.get('rejected', 0)} "
            f"rejected\n"
            f"24h sightings ≥{ARB_MIN_EDGE:.0%}: {s['n'] or 0} "
            f"(best {(s['best'] or 0):.1%}, sum ${(s['tot'] or 0):.2f})\n"
            f"7d locks ≥{ARB_ALERT_EDGE:.0%}: {week['n'] or 0} "
            f"(${(week['tot'] or 0):.2f} would-be profit)\n"
            f"/arbpairs to review candidates | /arblog for history")

    def txt_log(self, limit: int = 10) -> str:
        rows = self.db.execute(
            "SELECT s.*, p.k_title FROM sightings s "
            "JOIN pairs p ON p.id=s.pair_id "
            "ORDER BY s.ts DESC LIMIT ?", (limit,)).fetchall()
        if not rows:
            return "No sightings yet — the scanner logs every fee-adjusted " \
                   "executable lock ≥" + f"{ARB_MIN_EDGE:.0%}."
        out = ["🔒 Recent lock sightings\n━━━━━━━━━━━━"]
        for r in rows:
            ago = (time.time() - r["ts"]) / 3600
            out.append(f"{r['edge']:.1%} ×{r['size']:.0f} "
                       f"(${r['profit']:.2f}) {ago:.1f}h ago\n"
                       f"  {r['k_title'][:52]}")
        return "\n".join(out)
