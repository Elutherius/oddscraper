"""
Kalshi API client for fetching sports markets.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx

# Try importing the official SDK, but fallback or wrap it
# Note: The official SDK might be 'kalshi_python' or similar depending on version.
# We will use direct HTTP requests for now if we want maximum control, 
# or use the SDK if confirmed working. 
# For now, let's assume we use the official SDK logic but wrapped in our client.
# Actually, looking at docs, direct HTTP is often cleaner for simple fetching.
# Let's start with a direct HTTP implementation to control rate limits and auth easily
# unless the SDK is strictly required. 
# BUT, the plan said "Use KalshiClient class wrapping kalshi-python". 
# So let's try to import it.

try:
    import kalshi_python
    from kalshi_python.models import *
    from kalshi_python.api import *
except ImportError:
    kalshi_python = None

from .utils import RateLimiter, utc_now_iso

logger = logging.getLogger(__name__)

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2" 

class KalshiClient:
    """
    Client for Kalshi API.
    """

    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
        rate_limiter: RateLimiter | None = None,
    ):
        self.email = email or os.getenv("KALSHI_EMAIL")
        self.password = password or os.getenv("KALSHI_PASSWORD")
        self.token = None
        self.member_id = None
        
        self.rate_limiter = rate_limiter or RateLimiter(requests_per_second=2.0)
        self.client = httpx.Client(
            base_url=KALSHI_BASE_URL,
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
        )

    def _request_with_retry(
        self, method: str, endpoint: str, params: dict[str, Any] | None = None, json: dict[str, Any] | None = None
    ) -> httpx.Response:
        """Make a request with retry logic and rate limiting."""
        for attempt in range(3):
            self.rate_limiter.wait()
            
            try:
                if method.lower() == "get":
                    response = self.client.get(endpoint, params=params)
                else:
                    response = self.client.request(method, endpoint, json=json, params=params)

                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limited, retrying after {retry_after}s")
                    self.rate_limiter.set_wait_until(retry_after)
                    continue
                
                if response.status_code >= 500:
                    time.sleep(1 * (2 ** attempt))
                    continue
                    
                response.raise_for_status()
                return response

            except httpx.RequestError as e:
                logger.warning(f"Request error: {e}")
                time.sleep(1)
                continue
                
        raise RuntimeError("Max retries exceeded")

    def login(self) -> None:
        """Login to get token (optional)."""
        if not self.email or not self.password:
            logger.info("No Kalshi credentials provided. Using public access.")
            return
            
        payload = {"email": self.email, "password": self.password}
        resp = self.client.post("/login", json=payload)
        resp.raise_for_status()
        data = resp.json()
        self.token = data.get("token")
        self.member_id = data.get("memberId")
        # Set auth header for future requests
        self.client.headers.update({"Authorization": f"Bearer {self.token}"})
        logger.info(f"Logged in to Kalshi as {self.member_id}")

    def get_sports_markets(self, limit: int = 1000) -> list[dict[str, Any]]:
        """
        Fetch active sports markets.
        Since there isn't a direct 'sports' filter, we'll fetch markets and filter by tags/series.
        """
        if not self.token and self.email and self.password:
            self.login()

        all_markets = []
        cursor = None
        
        # Series tickers that are definitely sports
        # fetching broad set then filtering
        
        # Endpoint: /markets
        while True:
            params = {
                "limit": 100,
                "status": "open",
                # "series_ticker": "NFL", # Example, maybe too specific
            }
            if cursor:
                params["cursor"] = cursor

            # self.rate_limiter.wait()
            # resp = self.client.get("/markets", params=params)
            # resp.raise_for_status()
            resp = self._request_with_retry("get", "/markets", params=params)
            data = resp.json()
            
            markets = data.get("markets", [])
            cursor = data.get("cursor")
            
            # TODO: Add filtering logic here
            for m in markets:
                # Basic heuristics for sports
                if self._is_sport_market(m):
                    all_markets.append(m)
            
            if not cursor or (limit and len(all_markets) >= limit):
                break
                
        return all_markets[:limit]

    def _is_sport_market(self, market: dict[str, Any]) -> bool:
        """Check if market is sports related."""
        # Check category, tags, series_ticker
        category = (market.get("category") or "").lower()
        series = (market.get("series_ticker") or "").lower()
        ticker = (market.get("ticker") or "").upper()
        title = (market.get("title") or "").lower()
        tags = [t.lower() for t in market.get("tags", [])] if market.get("tags") else []
        
        # Explicit sports categories if present
        if category == "sports":
            return True
            
        # Sports Keywords
        sports_keywords = [
            "nfl", "nba", "mlb", "nhl", "soccer", "tennis", "golf", "ufc", "mma", 
            "sport", "f1", "nascar", "boxing", "college football", "ncaa"
        ]
        
        # 1. Check Ticker Prefix/Content
        # "KXMVESPORTS" -> Kalshi Multi Vector Esports/Sports?
        if "SPORT" in ticker:
            return True
        if any(k.upper() in ticker for k in sports_keywords):
            return True

        # 2. Check Series/Category/Tags
        for k in sports_keywords:
            if k in series or k in category:
                return True
            for tag in tags:
                if k in tag:
                    return True

        # 3. Check Title for strong sports signals
        # "wins by", "points scored", "goals scored", "touchdown"
        strong_sports_terms = ["wins by", "points scored", "goals scored", "passing yards", "rushing yards", "touchdown"]
        if any(term in title for term in strong_sports_terms):
            return True

        return False

    def close(self):
        self.client.close()

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
