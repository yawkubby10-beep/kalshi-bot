"""
Kalshi API Client — RSA-PSS signed requests to Kalshi Trade API v2
Base URL: https://trading-api.kalshi.com
Auth: RSA-PSS SHA-256 with salt_length=digest_length (32 bytes)
Sign: timestamp_ms + METHOD + path (path includes /trade-api/v2, excludes query string)
"""
import time
import base64
import uuid
import requests
from typing import Optional, Dict, Any
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

BASE_URL = "https://api.elections.kalshi.com"
API_PATH = "/trade-api/v2"


class KalshiClient:
    def __init__(self, api_key: str, private_key_pem: str):
        self.api_key = api_key
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode() if isinstance(private_key_pem, str) else private_key_pem,
            password=None
        )

    def _sign(self, method: str, path: str) -> tuple:
        """Sign: timestamp_ms + METHOD.upper() + path (no query string)."""
        ts = str(int(time.time() * 1000))
        # Strip query string before signing
        sign_path = path.split("?")[0]
        msg = (ts + method.upper() + sign_path).encode()
        sig = self.private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
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

    def get(self, path: str, params: Dict = None) -> Dict:
        full_path = API_PATH + path
        url = BASE_URL + full_path
        if params:
            url += "?" + "&".join(f"{k}={v}" for k, v in params.items())
        r = requests.get(url, headers=self._headers("GET", full_path), timeout=10)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: Dict) -> Dict:
        full_path = API_PATH + path
        r = requests.post(
            BASE_URL + full_path,
            headers=self._headers("POST", full_path),
            json=body,
            timeout=10
        )
        r.raise_for_status()
        return r.json()

    # ── Market Data ────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        resp = self.get("/portfolio/balance")
        return float(resp.get("balance", 0))

    def get_15m_markets(self, crypto: str = "BTC") -> list:
        series = f"KX{crypto}15M"
        resp = self.get("/markets", {"limit": 3, "status": "open", "series_ticker": series})
        return resp.get("markets", [])

    def get_market(self, ticker: str) -> Dict:
        resp = self.get(f"/markets/{ticker}")
        return resp.get("market", {})

    # ── Order Placement ────────────────────────────────────────────────────────

    def place_market_order(self, ticker: str, side: str, count: int, price: float = 0.99) -> Dict:
        """
        Place a market order on Kalshi V2.
        side = "yes" buys YES (bid), "no" buys NO (ask)
        count = number of contracts (string)
        price = price in dollars e.g. 0.50 for 50 cents
        Endpoint: POST /trade-api/v2/portfolio/events/orders
        """
        # Kalshi V2 uses bid/ask not yes/no
        kalshi_side = "bid" if side == "yes" else "ask"
        body = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4())[:16],
            "type": "market",
            "action": "buy",
            "side": kalshi_side,
            "count": str(count),
            "price": f"{price:.4f}",
            "time_in_force": "immediate_or_cancel",
            "self_trade_prevention_type": "taker_at_cross",
        }
        return self.post("/portfolio/events/orders", body)

    def place_limit_order(self, ticker: str, side: str, count: int, price: float) -> Dict:
        """
        Place a GTC limit order.
        side = "yes" or "no"
        price = price in dollars e.g. 0.48
        """
        kalshi_side = "bid" if side == "yes" else "ask"
        body = {
            "ticker": ticker,
            "client_order_id": str(uuid.uuid4())[:16],
            "type": "limit",
            "action": "buy",
            "side": kalshi_side,
            "count": str(count),
            "price": f"{price:.4f}",
            "time_in_force": "good_till_canceled",
            "self_trade_prevention_type": "cancel_newest",
        }
        return self.post("/portfolio/events/orders", body)

    def get_positions(self) -> list:
        resp = self.get("/portfolio/positions", {"limit": 50})
        return resp.get("market_positions", [])

    def get_fills(self, limit: int = 20) -> list:
        resp = self.get("/portfolio/fills", {"limit": limit})
        return resp.get("fills", [])
