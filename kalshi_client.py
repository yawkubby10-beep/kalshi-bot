"""
Kalshi API Client
RSA-PSS signed requests to Kalshi Trade API v2
"""
import time
import base64
import hashlib
import requests
import json
from typing import Optional, Dict, Any
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiClient:
    def __init__(self, api_key: str, private_key_pem: str):
        self.api_key = api_key
        self.private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None
        )

    def _sign(self, method: str, path: str) -> tuple[str, str]:
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path
        sig = self.private_key.sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())
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
        full_path = path
        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            full_path = f"{path}?{qs}"
        r = requests.get(
            BASE_URL + full_path,
            headers=self._headers("GET", full_path),
            timeout=10
        )
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: Dict) -> Dict:
        r = requests.post(
            BASE_URL + path,
            headers=self._headers("POST", path),
            json=body,
            timeout=10
        )
        r.raise_for_status()
        return r.json()

    # ── Market Data ────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        """Return available balance in dollars."""
        resp = self.get("/portfolio/balance")
        return float(resp.get("balance", "0")) / 100  # cents to dollars

    def get_15m_markets(self, crypto: str = "BTC") -> list:
        """Get active 15-min markets for a crypto."""
        series = f"KX{crypto}15M"
        resp = self.get("/markets", {"limit": 5, "status": "open", "series_ticker": series})
        return resp.get("markets", [])

    def get_orderbook(self, ticker: str) -> Dict:
        """Get orderbook for a market ticker."""
        return self.get(f"/markets/{ticker}/orderbook")

    def get_market(self, ticker: str) -> Dict:
        """Get single market details."""
        resp = self.get(f"/markets/{ticker}")
        return resp.get("market", {})

    # ── Order Placement ────────────────────────────────────────────────────────

    def place_market_order(
        self,
        ticker: str,
        side: str,        # "yes" or "no"
        count: int,       # number of contracts
        order_type: str = "market",
    ) -> Dict:
        """Place a market order. count = number of $1 contracts."""
        body = {
            "ticker": ticker,
            "client_order_id": f"arb_{int(time.time()*1000)}",
            "type": order_type,
            "action": "buy",
            "side": side,
            "count": count,
            "buy_max_cost": count * 100,  # max cost in cents
        }
        return self.post("/portfolio/orders", body)

    def place_limit_order(
        self,
        ticker: str,
        side: str,
        count: int,
        yes_price: int,   # cents (1-99)
    ) -> Dict:
        """Place a limit order at yes_price cents."""
        body = {
            "ticker": ticker,
            "client_order_id": f"arb_{int(time.time()*1000)}",
            "type": "limit",
            "action": "buy",
            "side": side,
            "count": count,
            "yes_price": yes_price,
        }
        return self.post("/portfolio/orders", body)

    def get_positions(self) -> list:
        """Get current open positions."""
        resp = self.get("/portfolio/positions", {"limit": 50})
        return resp.get("market_positions", [])

    def get_fills(self, limit: int = 20) -> list:
        """Get recent fills."""
        resp = self.get("/portfolio/fills", {"limit": limit})
        return resp.get("fills", [])
