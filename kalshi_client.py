"""
kalshi_client.py — Kalshi Trade API v2 client (RSA-PSS signed)
===============================================================
CRITICAL SEMANTICS — the /portfolio/events/orders endpoint quotes EVERYTHING
on the YES leg:

    side="bid"  → BUY  YES at `price` (YES dollars)
    side="ask"  → SELL YES at `price` (YES dollars)
                  Selling YES you don't hold == buying NO at (1 − price).

Therefore:
    BUY YES  N @ P_yes  →  side="bid", price=P_yes
    BUY NO   N @ P_no   →  side="ask", price=(1 − P_no)      ← v2 bug was here:
                            old code sent price=P_no, i.e. it SOLD YES at the
                            NO price — every DOWN trade crossed the book at a
                            terrible level instead of resting as a maker.
    EXIT YES @ P_yes    →  side="ask", price=P_yes, reduce_only
    EXIT NO  @ P_no     →  side="bid", price=(1 − P_no), reduce_only

Cancels MUST be signed (old resolver sent unauthenticated DELETEs → 401 →
stale resting orders sat in the book getting picked off).

TRANSPORT (the 2026-07-23 EOF incident): Create Order V2 takes a JSON body.
The old `_req(method, path, params=None, body=None)` signature let callers
pass the order dict POSITIONALLY into the `params` slot — every field rode
the query string, the body went out empty, and Kalshi answered
{"details":"EOF"} on every live maker post. `params`/`body` are now
KEYWORD-ONLY, so that whole bug class is a TypeError at the call site.

SAFETY RAILS (welded into the client, not the caller):
- PAPER_MODE (env, default TRUE): order creation and cancels raise
  PaperModeViolation. Paper is structurally incapable of touching the
  order endpoints, whatever the caller believes.
- LIVE_MAX_COUNT (env, default 1): tuition sizing. Live opening orders
  are clamped to this count; reduce_only exits are exempt so positions
  can always be closed in full.
- No retries on 4xx (bad requests don't heal). One short retry on
  429/5xx with a freshly signed timestamp. The expiration_time-stripping
  fallback is deleted: the field is spec-valid, and stripping it would
  silently post a NEVER-expiring maker.
"""

import base64
import logging
import os
import random
import time
import uuid
from typing import Dict, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger("kalshi")

BASE_URL = "https://api.elections.kalshi.com"
API_PATH = "/trade-api/v2"


class PaperModeViolation(RuntimeError):
    """Raised when anything tries to place/cancel real orders in paper."""


def _paper_mode() -> bool:
    return os.getenv("PAPER_MODE", "true").strip().lower() in (
        "1", "true", "yes", "on")


def _live_max_count() -> int:
    try:
        return max(1, int(float(os.getenv("LIVE_MAX_COUNT", "1"))))
    except (TypeError, ValueError):
        return 1


class KalshiClient:
    def __init__(self, api_key: str, private_key_pem: str):
        self.api_key = api_key
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode() if isinstance(private_key_pem, str)
            else private_key_pem,
            password=None,
        )
        self._s = requests.Session()

    # ── signing ──────────────────────────────────────────────────────────
    def _sign(self, method: str, path: str):
        ts = str(int(time.time() * 1000))
        msg = (ts + method.upper() + path.split("?")[0]).encode()
        sig = self.private_key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return ts, base64.b64encode(sig).decode()

    def _headers(self, method: str, path: str) -> Dict:
        ts, sig = self._sign(method, path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "Content-Type": "application/json",
        }

    def _req(self, method: str, path: str, *, params: Dict = None,
             body: Dict = None) -> Dict:
        """params/body are KEYWORD-ONLY on purpose — see module docstring."""
        full = API_PATH + path
        url = BASE_URL + full
        attempts = 0
        while True:
            attempts += 1
            r = self._s.request(method, url,
                                headers=self._headers(method, full),
                                params=params, json=body, timeout=10)
            if (r.status_code == 429 or r.status_code >= 500) and attempts < 2:
                time.sleep(0.7 + random.uniform(0.0, 0.4))
                continue                      # fresh signature next lap
            if r.status_code >= 400:
                logger.error(f"Kalshi {method} {path} → {r.status_code}: "
                             f"{r.text[:300]}")
            r.raise_for_status()
            return r.json() if r.text else {}

    # ── paper gate ───────────────────────────────────────────────────────
    def _mutation_gate(self, what: str):
        if _paper_mode():
            logger.critical(
                f"PAPER MODE: refused {what} — the client does not touch "
                f"order endpoints in paper. If you intended live trading, "
                f"set PAPER_MODE=false explicitly.")
            raise PaperModeViolation(what)

    # ── account / data ───────────────────────────────────────────────────
    def get_balance(self) -> float:
        resp = self._req("GET", "/portfolio/balance")
        return float(resp.get("balance", 0)) / 100.0   # API returns cents

    def get_market(self, ticker: str) -> Dict:
        return self._req("GET", f"/markets/{ticker}").get("market", {})

    def get_orderbook(self, ticker: str, depth: int = 16) -> Dict:
        return self._req("GET", f"/markets/{ticker}/orderbook",
                         params={"depth": depth})

    def get_positions(self) -> list:
        return self._req("GET", "/portfolio/positions",
                         params={"limit": 100}).get("market_positions", [])

    def get_fills(self, limit: int = 50) -> list:
        return self._req("GET", "/portfolio/fills",
                         params={"limit": limit}).get("fills", [])

    def get_order(self, order_id: str) -> Dict:
        resp = self._req("GET", f"/portfolio/orders/{order_id}")
        return resp.get("order", resp)

    # ── core order primitive (YES-leg quoting) ───────────────────────────
    def _create(self, ticker: str, yes_leg_side: str, price_yes_d: float,
                count: int, tif: str, post_only: bool,
                expiration_ts: Optional[int], reduce_only: bool) -> Dict:
        self._mutation_gate(f"order {yes_leg_side} {ticker}")
        if not reduce_only:
            cap = _live_max_count()
            if count > cap:
                logger.warning(
                    f"LIVE_MAX_COUNT: clamping {count} → {cap} contracts "
                    f"(tuition sizing — raise the env var to lift)")
                count = cap
        price_yes_d = min(0.99, max(0.01, round(price_yes_d, 2)))
        body = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4()),
            "side": yes_leg_side,               # "bid" | "ask" on YES leg
            "count": f"{int(count)}.00",
            "price": f"{price_yes_d:.4f}",
            "time_in_force": tif,
            "post_only": bool(post_only),
            "reduce_only": bool(reduce_only),
            "self_trade_prevention_type":
                "maker" if post_only else "taker_at_cross",
        }
        if expiration_ts and tif == "good_till_canceled":
            body["expiration_time"] = int(expiration_ts)
        resp = self._req("POST", "/portfolio/events/orders", body=body)
        o = resp.get("order", resp)
        return {
            "order_id": o.get("order_id") or o.get("id") or "",
            "client_order_id": body["client_order_id"],
            "fill_count": float(o.get("fill_count", 0) or 0),
            "remaining_count": float(o.get("remaining_count", 0) or 0),
            "avg_fill_price": float(o.get("average_fill_price", 0) or 0),
            "raw": o,
        }

    # ── friendly wrappers (prices in the SIDE'S OWN dollars) ─────────────
    def buy_yes_ioc(self, ticker: str, limit_yes_d: float, count: int) -> Dict:
        return self._create(ticker, "bid", limit_yes_d, count,
                            "immediate_or_cancel", False, None, False)

    def buy_no_ioc(self, ticker: str, limit_no_d: float, count: int) -> Dict:
        return self._create(ticker, "ask", 1.0 - limit_no_d, count,
                            "immediate_or_cancel", False, None, False)

    def post_yes_bid(self, ticker: str, price_yes_d: float, count: int,
                     expiration_ts: Optional[int] = None) -> Dict:
        return self._create(ticker, "bid", price_yes_d, count,
                            "good_till_canceled", True, expiration_ts, False)

    def post_no_bid(self, ticker: str, price_no_d: float, count: int,
                    expiration_ts: Optional[int] = None) -> Dict:
        return self._create(ticker, "ask", 1.0 - price_no_d, count,
                            "good_till_canceled", True, expiration_ts, False)

    def exit_yes_ioc(self, ticker: str, limit_yes_d: float,
                     count: int) -> Dict:
        """Sell held YES at ≥ limit (IOC, reduce-only)."""
        return self._create(ticker, "ask", limit_yes_d, count,
                            "immediate_or_cancel", False, None, True)

    def exit_no_ioc(self, ticker: str, limit_no_d: float, count: int) -> Dict:
        """Sell held NO at ≥ limit (IOC, reduce-only)."""
        return self._create(ticker, "bid", 1.0 - limit_no_d, count,
                            "immediate_or_cancel", False, None, True)

    def buy(self, ticker: str, want: str, limit_side_d: float,
            count: int) -> Dict:
        return (self.buy_yes_ioc if want == "yes"
                else self.buy_no_ioc)(ticker, limit_side_d, count)

    def post_bid(self, ticker: str, want: str, price_side_d: float,
                 count: int, expiration_ts: Optional[int] = None) -> Dict:
        return (self.post_yes_bid if want == "yes"
                else self.post_no_bid)(ticker, price_side_d, count,
                                       expiration_ts)

    def exit(self, ticker: str, want: str, limit_side_d: float,
             count: int) -> Dict:
        return (self.exit_yes_ioc if want == "yes"
                else self.exit_no_ioc)(ticker, limit_side_d, count)

    # ── cancels (SIGNED — this was broken before) ────────────────────────
    def cancel_order(self, order_id: str) -> bool:
        self._mutation_gate(f"cancel {order_id}")
        for path in (f"/portfolio/events/orders/{order_id}",
                     f"/portfolio/orders/{order_id}"):
            try:
                self._req("DELETE", path)
                return True
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 404:
                    continue
                logger.warning(f"cancel {order_id} via {path} failed: {e}")
        return False
