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
- Series-scoped universe. Kalshi's flat market list (120k+ open inside
  21 days after the sports-singles explosion) cannot be paged whole; the
  scanner censuses the series catalogue by category instead (cached),
  keeps the lock heartland plus any series sharing real tokens with the
  live Polymarket universe, and fetches markets series-by-series. Nothing
  is silently unscanned — what's skipped is skipped by name — and every
  kept market arrives stamped with series + category, letting the matcher
  veto cross-league city collisions (Guadalajara ≠ Dodgers).
"""

import asyncio
import json
import logging
import math
import os
import random
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

# ── intra-Kalshi ladder locks (same exchange, same rulebook, no oracle risk) ──
LADDER_SERIES = [s.strip() for s in os.getenv(
    "LADDER_SERIES", "KXBTCD,KXETHD,KXSOLD").split(",") if s.strip()]
LADDER_MIN_EDGE = float(os.getenv("LADDER_MIN_EDGE", "0.010"))   # log ≥1%
LADDER_ALERT_EDGE = float(os.getenv("LADDER_ALERT_EDGE", "0.020"))  # ping ≥2%
LADDER_MIN_SIZE = float(os.getenv("LADDER_MIN_SIZE", "10"))
LADDER_MAX = float(os.getenv("LADDER_MAX_PAIR", "200"))
LADDER_INTERVAL = float(os.getenv("LADDER_INTERVAL", "30"))

# series-scoped fetch knobs
SERIES_TTL = float(os.getenv("ARB_SERIES_TTL", "43200"))   # census cache 12h
MAX_SERIES = int(os.getenv("ARB_MAX_SERIES", "400"))       # shelf size cap
REQ_SLEEP = float(os.getenv("ARB_REQ_SLEEP", "0.12"))      # ~8 req/s of the
                                                           # 20/s Basic budget
REQ_BUDGET = int(os.getenv("ARB_REQ_BUDGET", "700"))       # hard cap per scan

FALLBACK_CATEGORIES = [
    "Politics", "Economics", "Financials", "Companies", "World",
    "Science and Technology", "Climate and Weather", "Health",
    "Entertainment", "Culture", "Sports", "Crypto", "Transportation",
]
# lock heartland: always fetched, no PM-token gate needed
CORE_CATEGORIES = ("econom", "politic", "financ", "world", "compan",
                   "climate", "weather", "science", "health")
# Heartland seeds: lock-territory vocabulary that must never slip the
# token net on phrasing (FOMC vs Fed, etc). Unioned with live PM tokens.
HEARTLAND_TOKENS = frozenset((
    "fed", "fomc", "rate", "rates", "cpi", "inflation", "gdp", "jobs",
    "unemployment", "payrolls", "recession", "tariff", "tariffs",
    "shutdown", "election", "president", "presidential", "senate",
    "congress", "governor", "nominee", "impeachment", "war", "ceasefire",
    "nato", "ukraine", "israel", "iran", "gaza", "taiwan", "oscars",
    "grammys", "emmys", "nobel"))

STOP_WORDS = {"will", "the", "be", "in", "on", "at", "of", "a", "an", "to",
              "by", "for", "or", "and", "vs", "market", "before", "after",
              "with", "is", "does", "do", "than", "more", "less", "who",
              "what", "how", "many", "much", "yes", "no", "2026", "2025"}
MONTHS = ("january february march april may june july august september "
          "october november december jan feb mar apr jun jul aug sep sept "
          "oct nov dec").split()
CRYPTO_BLOCK = {"bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp",
                "dogecoin", "doge", "cardano", "ada"}

# league identifiers as they appear in Kalshi series tickers/titles and
# Polymarket slugs — used to veto city-name collisions across sports
# Kalshi market families that bundle many games/outcomes into one ticker.
# They share city/team words with countless single-event markets but can
# never cleanly match one — pure false-positive factories. Skip on sight.
JUNK_KALSHI_PREFIXES = ("KXMVESPORTSMULTIGAME", "KXMULTIGAME",
                        "KXSPORTSMULTI", "KXMVE")

LEAGUE_IDS = {"mlb", "nba", "nfl", "nhl", "wnba", "mls", "epl", "ucl",
              "uel", "uefa", "ncaa", "ncaab", "ncaaf", "atp", "wta", "f1",
              "nascar", "ufc", "pga", "kbo", "npb", "ipl", "bundesliga",
              "ligue", "laliga", "ligamx", "seriea", "cfl", "afl"}


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


def features(title: str) -> tuple:
    """Precomputed match features: (numbers, months, years, tokens)."""
    return (extract_numbers(title), extract_months(title),
            extract_years(title), tokens_of(title))


def match_features(fa: tuple, fb: tuple) -> float:
    if fa[0] != fb[0]:
        return 0.0                    # thresholds must match exactly
    if not _compatible(fa[1], fb[1]) or not _compatible(fa[2], fb[2]):
        return 0.0
    ta, tb = fa[3], fb[3]
    if not ta or not tb:
        return 0.0
    union = len(ta | tb)
    return len(ta & tb) / union if union else 0.0


def match_score(title_a: str, title_b: str) -> float:
    return match_features(features(title_a), features(title_b))


def is_crypto_price_market(title: str) -> bool:
    toks = tokens_of(title)
    return bool(toks & CRYPTO_BLOCK) and bool(extract_numbers(title))


def is_crypto_price_series(text: str) -> bool:
    """Series-level version of the oracle-mismatch doctrine: price/level
    series on coins never get fetched at all (their titles carry the coin
    plus price language, usually without a literal number)."""
    toks = tokens_of(text)
    if not toks & CRYPTO_BLOCK:
        return False
    return bool(toks & {"price", "above", "below", "reach", "hit",
                        "between", "range", "high", "low", "close"})


def is_junk_kalshi(ticker: str) -> bool:
    """Multi-game bundle markets: real ticker, but never a clean 1:1 pair."""
    t = (ticker or "").upper()
    return any(t.startswith(p) for p in JUNK_KALSHI_PREFIXES)


def leagues_in(*texts) -> frozenset:
    """Best-effort league identity from any mix of tickers, titles, slugs.
    Token pass first, then a squashed-substring pass for compact ids that
    survive slug punctuation (ligamx, laliga, seriea)."""
    found = set()
    for t in texts:
        if not t:
            continue
        low = str(t).lower()
        found |= frozenset(re.findall(r"[a-z0-9]+", low)) & LEAGUE_IDS
        squashed = re.sub(r"[^a-z0-9]", "", low)
        for lid in ("ligamx", "laliga", "seriea", "ncaab", "ncaaf"):
            if lid in squashed:
                found.add(lid)
    return frozenset(found)


# Which sport each league belongs to — lets us block soccer-vs-baseball
# even when only one side names the league but the other names its teams.
SOCCER = {"mls", "epl", "ucl", "uel", "uefa", "ligamx", "laliga", "seriea",
          "bundesliga", "ligue"}
BASEBALL = {"mlb", "kbo", "npb"}
_SPORT_OF = {}
for _lg in SOCCER: _SPORT_OF[_lg] = "soccer"
for _lg in BASEBALL: _SPORT_OF[_lg] = "baseball"

def _sports_of(leagues) -> frozenset:
    return frozenset(_SPORT_OF[l] for l in leagues if l in _SPORT_OF)

def league_conflict(k_texts: tuple, pm_texts: tuple) -> bool:
    """Block when the two sides are clearly different competitions.
    Strong signal: both name leagues that share nothing. Also strong:
    the sports they map to differ (soccer market vs baseball market),
    even if only one side spells out the league."""
    ka, pa = leagues_in(*k_texts), leagues_in(*pm_texts)
    if ka and pa and not (ka & pa):
        return True
    sk, sp = _sports_of(ka), _sports_of(pa)
    if sk and sp and not (sk & sp):
        return True
    return False


_MON = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
        "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
_VERBS_SOFT = {"compete", "competes", "qualify", "qualifies", "appear",
               "appears", "play", "plays", "participate", "top", "make",
               "makes", "finish", "finishes"}
_VERBS_WIN = {"win", "wins", "winner", "champion"}


def _kalshi_deadline(ticker: str):
    m = re.search(r"-(\d{2})([A-Z]{3})(\d{2})?(?:-|$)", ticker)
    if not m or m.group(2) not in _MON:
        return None
    return (2000 + int(m.group(1)), _MON[m.group(2)],
            int(m.group(3)) if m.group(3) else None)


def _pm_deadline(text: str):
    low = text.lower().replace("-", " ")
    m = re.search(r"before\s+(20\d\d)\b", low)
    if m:
        return (int(m.group(1)) - 1, 12, 31)
    m = re.search(r"before\s+([a-z]+)\s+(20\d\d)", low)
    if m and m.group(1)[:3].upper() in _MON:
        mo = _MON[m.group(1)[:3].upper()]
        y = int(m.group(2))
        return (y, mo - 1, 31) if mo > 1 else (y - 1, 12, 31)
    m = re.search(r"by\s+([a-z]+)\s+(\d{1,2}),?\s+(20\d\d)", low)
    if m and m.group(1)[:3].upper() in _MON:
        return (int(m.group(3)), _MON[m.group(1)[:3].upper()],
                int(m.group(2)))
    return None


def pair_hints(k_ticker: str, k_title: str, pm_title: str,
               pm_slug: str, k_series_desc: str = "") -> str:
    hints = []
    if k_series_desc:
        hints.append(f"K series: {k_series_desc}")
    kd = _kalshi_deadline(k_ticker)
    pd = _pm_deadline(pm_title) or _pm_deadline(pm_slug)
    if kd and pd and (kd[0], kd[1]) != (pd[0], pd[1]):
        hints.append(f"⚠️ DEADLINES DIFFER: Kalshi ~{kd[0]}-{kd[1]:02d}"
                     f"{'-%02d' % kd[2] if kd[2] else ''} vs Poly "
                     f"~{pd[0]}-{pd[1]:02d} → almost certainly REJECT")
    kt, pt = tokens_of(k_title), tokens_of(pm_title)
    a_soft, a_win = kt & _VERBS_SOFT, kt & _VERBS_WIN
    b_soft, b_win = pt & _VERBS_SOFT, pt & _VERBS_WIN
    if (a_soft and b_win and not a_win) or (b_soft and a_win and not b_win):
        hints.append("⚠️ DIFFERENT QUESTIONS: 'compete/qualify/top' vs "
                     "'win' → REJECT")
    kl = leagues_in(k_ticker, k_title, k_series_desc)
    pl = leagues_in(pm_title, pm_slug)
    if kl and pl and kl & pl:
        hints.append(f"league match: {', '.join(sorted(kl & pl))} ✓")
    elif kl or pl:
        hints.append("⚠️ league visible on one side only — confirm same "
                     "competition before ✅")
    if len(hints) <= 1:
        hints.append("no obvious mismatch found — still read both rule "
                     "pages before ✅")
    return "\n".join(hints)


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
        CREATE TABLE IF NOT EXISTS ladder_sightings(
            id INTEGER PRIMARY KEY, ts REAL, series TEXT,
            lo_ticker TEXT, hi_ticker TEXT, lo_ask REAL, hi_no_ask REAL,
            size REAL, edge REAL, profit REAL, alerted INTEGER DEFAULT 0);
        """)
        self.db.commit()
        self._last_alert: Dict[int, float] = {}
        self._series_cache: Dict[str, object] = {"ts": 0.0, "series": []}
        self.stats = {"k_markets": 0, "pm_markets": 0, "checks": 0,
                      "last_universe": 0.0}

    # ── paced GET with token-bucket-aware backoff ───────────────────────
    async def _kget(self, path: str, params: dict,
                    budget: dict) -> Optional[dict]:
        """One paced request against the public API. 429s carry no
        Retry-After under the 2026 token-bucket model — exponential
        backoff with jitter, retry same request, give up after 6."""
        sess = self._sess()
        backoff = 0.8
        for _ in range(6):
            if budget["req"] >= budget["cap"]:
                budget["exhausted"] = True
                return None
            budget["req"] += 1
            try:
                async with sess.get(f"{KALSHI_BASE}{path}", params=params,
                                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 429:
                        await asyncio.sleep(backoff
                                            + random.uniform(0, backoff))
                        backoff = min(backoff * 2, 8.0)
                        continue
                    if r.status != 200:
                        logger.debug(f"kalshi {path} HTTP {r.status}")
                        return None
                    data = await r.json()
            except Exception as e:
                logger.debug(f"kalshi {path}: {e}")
                return None
            await asyncio.sleep(REQ_SLEEP)
            return data
        logger.warning(f"kalshi {path}: repeated 429s — skipping")
        return None

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
        # PM first — its token set drives which Kalshi series get fetched.
        pm_markets = await self._pm_universe()
        pm_tokens = frozenset()
        for pm in pm_markets:
            pm_tokens |= tokens_of(pm["title"] + " "
                                   + pm["slug"].replace("-", " "))
        k_markets = await self._kalshi_universe(pm_tokens)
        self.stats.update(k_markets=len(k_markets),
                          pm_markets=len(pm_markets),
                          last_universe=time.time())
        approved_templates = {
            r["template"] for r in self.db.execute(
                "SELECT template FROM pairs WHERE status='approved'")}
        pending = self.db.execute(
            "SELECT COUNT(*) c FROM pairs WHERE status='pending'"
        ).fetchone()["c"]
        # Precompute features once per side and bucket Polymarket by its
        # number-set — a Kalshi title can only ever match PM titles with
        # the identical threshold set, so we compare within buckets only.
        pm_buckets: Dict[frozenset, list] = {}
        for pm in pm_markets:
            if is_crypto_price_market(pm["title"]):
                continue
            fp = features(pm["title"])
            pm_buckets.setdefault(fp[0], []).append((fp, pm))
        new = 0
        for km in k_markets:
            if pending + new >= MAX_PENDING:
                break
            kt = km["title"]
            if is_crypto_price_market(kt):
                continue
            fk = features(kt)
            for fp, pm in pm_buckets.get(fk[0], []):
                s = match_features(fk, fp)
                if s < 0.45:
                    continue
                if league_conflict(
                        (km["ticker"], kt, km["series"], km["s_title"],
                         km["category"]),
                        (pm["slug"], pm["title"])):
                    continue      # city tokens, different sport — veto
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
                            + pair_hints(km["ticker"], kt, pm["title"],
                                         pm["slug"],
                                         f"{km['s_title']} "
                                         f"[{km['category']}]") + "\n"
                            f"/arbpairs to approve or reject — when "
                            f"unsure, REJECT (costs nothing).")
                except Exception as e:
                    logger.debug(f"pair insert: {e}")
        self.db.commit()
        if new:
            logger.info(f"arb: {new} new candidate pair(s)")

    # ── series census (cached) ──────────────────────────────────────────
    async def _series_census(self, budget: dict) -> List[dict]:
        now = time.time()
        cached = self._series_cache["series"]
        if cached and now - self._series_cache["ts"] < SERIES_TTL:
            return cached
        cats: List[str] = []
        data = await self._kget("/search/tags_by_categories", {}, budget)
        if isinstance(data, dict):
            inner = data.get("tags_by_categories", data)
            if isinstance(inner, dict):
                cats = [c for c in inner.keys() if isinstance(c, str)]
            elif isinstance(inner, list):
                for it in inner:
                    if isinstance(it, dict):
                        c = it.get("category") or it.get("name")
                        if c:
                            cats.append(c)
        if not cats:
            cats = list(FALLBACK_CATEGORIES)
            logger.info("arb census: category endpoint unhelpful — "
                        "using fallback category list")
        series: List[dict] = []
        for cat in cats[:24]:
            cursor = None
            for _ in range(8):
                params = {"category": cat, "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                data = await self._kget("/series", params, budget)
                if not data:
                    break
                for srow in data.get("series", []) or []:
                    tick = srow.get("ticker")
                    if not tick:
                        continue
                    tags = srow.get("tags") or []
                    if not isinstance(tags, list):
                        tags = []
                    series.append({"ticker": tick,
                                   "title": srow.get("title") or "",
                                   "category": srow.get("category") or cat,
                                   "tags": " ".join(str(x) for x in tags)})
                cursor = data.get("cursor")
                if not cursor:
                    break
            if cursor:
                logger.warning(f"arb census: category {cat} truncated at "
                               f"page cap — series list incomplete")
        if series:
            seen, uniq = set(), []
            for srow in series:
                if srow["ticker"] in seen:
                    continue
                seen.add(srow["ticker"])
                uniq.append(srow)
            self._series_cache = {"ts": now, "series": uniq}
            logger.info(f"arb kalshi census: {len(uniq)} series across "
                        f"{len(cats)} categories")
            return uniq
        logger.warning("arb kalshi census came back empty — reusing "
                       f"previous census ({len(cached)} series)")
        return cached

    def _select_series(self, all_series: List[dict],
                       pm_tokens: frozenset) -> List[dict]:
        """Every series earns its fetch via token overlap with the live
        PM universe (+ heartland seeds). Categories no longer grant a
        free pass — 'core' matched 9,926 series (city-weather explosion)
        and starved the shelf to 400 thermometers with 0 pm-matched.
        Overlap ranks; core category only breaks ties."""
        gate = pm_tokens | HEARTLAND_TOKENS
        scored = []
        for srow in all_series:
            blob = f"{srow['title']} {srow['tags']} {srow['ticker']}"
            if is_crypto_price_series(blob):
                continue
            overlap = len(tokens_of(blob) & gate)
            if overlap < 1:
                continue
            cat = (srow["category"] or "").lower()
            core = 1 if any(c in cat for c in CORE_CATEGORIES) else 0
            scored.append((overlap, core, srow))
        scored.sort(key=lambda x: (-x[0], -x[1]))
        picked = [s for _, _, s in scored[:MAX_SERIES]]
        n_core = sum(c for _, c, _ in scored[:MAX_SERIES])
        if len(scored) > MAX_SERIES:
            logger.info(f"arb shelf: {len(scored)} passed the token gate; "
                        f"keeping top {MAX_SERIES} by overlap")
        logger.info(f"arb kalshi shelf: {len(picked)}/{len(all_series)} "
                    f"series selected ({n_core} core-category, "
                    f"{len(picked) - n_core} other)")
        return picked


    async def _kalshi_universe(self, pm_tokens: frozenset) -> List[dict]:
        """Series-scoped fetch. The flat alphabetical walk of /markets
        died of scale: 120k+ open markets inside 21 days, every scan
        truncated at the page cap, and the liveness filter could only
        save memory — pages were spent on corpses before it ran. Now the
        series catalogue (small, cached) decides what gets fetched, and
        coverage of the selected shelf is complete by construction."""
        budget = {"req": 0, "cap": REQ_BUDGET, "exhausted": False}
        all_series = await self._series_census(budget)
        if not all_series:
            logger.warning("arb kalshi universe: no series census — "
                           "skipping this refresh")
            return []
        picked = self._select_series(all_series, pm_tokens)
        out: List[dict] = []
        raw, fetched, sampled = 0, 0, False
        max_close = int(time.time()) + 21 * 86400   # locks cycle capital fastest
        for srow in picked:
            if budget["exhausted"]:
                break
            cursor = None
            for _ in range(3):            # pages per series — plenty
                params = {"status": "open", "limit": 1000,
                          "series_ticker": srow["ticker"],
                          "max_close_ts": max_close}
                if cursor:
                    params["cursor"] = cursor
                data = await self._kget("/markets", params, budget)
                if not data:
                    break
                page = data.get("markets", []) or []
                raw += len(page)
                if not sampled and page:
                    sampled = True
                    m0 = page[0]
                    logger.info(f"arb kalshi sample: {m0.get('ticker')} "
                                f"last_px={m0.get('last_price_dollars')}")
                for m in page:
                    try:
                        lp = (m.get("last_price_dollars")
                              or m.get("last_price"))
                        try:
                            if not lp or float(lp) <= 0:
                                continue      # never traded = dead market
                        except (TypeError, ValueError):
                            continue
                        if not m.get("title"):
                            continue
                        if is_junk_kalshi(m.get("ticker", "")):
                            continue   # multi-game bundle — false-match factory
                        out.append({"ticker": m["ticker"],
                                    "title": m["title"],
                                    "series": srow["ticker"],
                                    "s_title": srow["title"],
                                    "category": srow["category"]})
                    except (TypeError, KeyError):
                        continue
                cursor = data.get("cursor")
                if not cursor:
                    break
            fetched += 1
        if budget["exhausted"]:
            logger.warning(f"arb kalshi universe: request budget hit — "
                           f"{len(picked) - fetched} selected series "
                           f"unfetched this cycle")
        logger.info(f"arb kalshi universe: kept {len(out)} of {raw} raw "
                    f"across {fetched} series ({budget['req']} requests)")
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
        """Gamma caps pages at 100 rows no matter the limit asked — walk
        offsets until empty, up to 1000 top-liquidity markets."""
        out = []
        sess = self._sess()
        offset = 0
        while offset < 1000:
            try:
                async with sess.get(
                        GAMMA, params={"closed": "false", "limit": 100,
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
            if not data:
                break
            offset += len(data)
        logger.info(f"arb pm universe: {len(out)} markets kept")
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

    # ── intra-Kalshi ladder lock watcher (READ-ONLY) ───────────────────
    @staticmethod
    def _strike_of(ticker: str):
        """The numeric strike at the tail of a KX*D ticker, e.g.
        KXBTCD-26JUL2412-T74799.99 → 74799.99."""
        import re as _re
        m = _re.search(r"-T(\d+(?:\.\d+)?)$", ticker or "")
        return float(m.group(1)) if m else None

    async def _ladder_markets(self, series: str):
        """Open markets in one ladder series, sorted by ascending strike."""
        data = await self._kget("/markets",
                                {"status": "open", "limit": 1000,
                                 "series_ticker": series},
                                {"req": 0, "cap": 50, "exhausted": False})
        rows = []
        for m in (data or {}).get("markets", []) or []:
            k = self._strike_of(m.get("ticker", ""))
            if k is not None:
                rows.append((k, m.get("ticker")))
        rows.sort(key=lambda x: x[0])
        return rows

    async def ladder_loop(self):
        await asyncio.sleep(45)
        while True:
            try:
                for series in LADDER_SERIES:
                    await self._scan_ladder(series)
            except Exception as e:
                logger.error(f"ladder scan: {e}", exc_info=True)
            await asyncio.sleep(LADDER_INTERVAL)

    async def _scan_ladder(self, series: str):
        rungs = await self._ladder_markets(series)
        if len(rungs) < 2:
            return
        # Pull each rung's book once.
        books = {}
        for _, tick in rungs:
            raw = await self._kalshi_orderbook(tick)
            kb = parse_kalshi_book(raw) if raw else None
            if kb:
                books[tick] = kb
        now = time.time()
        # For every lower/higher strike pair: P(lower) must be >= P(higher).
        # Lock = buy YES(lower) + buy NO(higher). If both settle YES, or both
        # NO, or lower-YES/higher-NO, you hold a guaranteed $1.00 on exactly
        # one leg. Cost = lower_yes_ask + higher_no_ask; profit if < $1 net.
        for i in range(len(rungs)):
            lo_strike, lo_tick = rungs[i]
            lb = books.get(lo_tick)
            if not lb:
                continue
            for j in range(i + 1, len(rungs)):
                hi_strike, hi_tick = rungs[j]
                hb = books.get(hi_tick)
                if not hb:
                    continue
                lo_yes = lb["yes_ask"]           # buy YES on lower strike
                hi_no = hb["no_ask"]             # buy NO on higher strike
                if lo_yes <= 0 or hi_no <= 0:
                    continue
                size = min(lb["yes_ask_sz"], hb["no_ask_sz"], LADDER_MAX)
                if size < LADDER_MIN_SIZE:
                    continue
                gross = 1.0 - (lo_yes + hi_no)
                fee = (kalshi_taker_fee(lo_yes, int(size))
                       + kalshi_taker_fee(hi_no, int(size)))
                profit = gross * size - fee
                edge = profit / size if size else 0.0
                if edge < LADDER_MIN_EDGE:
                    continue
                alerted = 0
                if edge >= LADDER_ALERT_EDGE:
                    alerted = 1
                    await self.notify(
                        f"🪜 LADDER LOCK {edge:.1%} — {series}\n"
                        f"buy YES {lo_tick} @{lo_yes:.2f}\n"
                        f"buy NO  {hi_tick} @{hi_no:.2f}\n"
                        f"combined {lo_yes + hi_no:.3f} < $1.00 → "
                        f"${profit:.2f} locked ×{size:.0f} "
                        f"(same exchange, fee-adj, READ-ONLY)")
                self.db.execute(
                    "INSERT INTO ladder_sightings(ts,series,lo_ticker,"
                    "hi_ticker,lo_ask,hi_no_ask,size,edge,profit,alerted) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (now, series, lo_tick, hi_tick, lo_yes, hi_no, size,
                     edge, profit, alerted))
                self.db.commit()

    def txt_ladder(self, limit: int = 10) -> str:
        day = time.time() - 86400
        s = self.db.execute(
            "SELECT COUNT(*) n, MAX(edge) best, SUM(profit) tot "
            "FROM ladder_sightings WHERE ts>?", (day,)).fetchone()
        rows = self.db.execute(
            "SELECT * FROM ladder_sightings ORDER BY ts DESC LIMIT ?",
            (limit,)).fetchall()
        out = ["🪜 Ladder locks [read-only, same-exchange]", "━━━━━━━━━━━━",
               f"24h: {s['n'] or 0} sightings "
               f"(best {(s['best'] or 0):.1%}, sum ${(s['tot'] or 0):.2f})"]
        if not rows:
            out.append("None yet. This is the SAFE arb — both legs settle on "
                       "Kalshi by the same rules, so no oracle mismatch. "
                       "Locks are rare; the watcher is patient.")
        else:
            for r in rows:
                ago = (time.time() - r["ts"]) / 3600
                out.append(f"{r['edge']:.1%} ×{r['size']:.0f} "
                           f"(${r['profit']:.2f}) {ago:.1f}h ago  {r['series']}")
        return "\n".join(out)

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
