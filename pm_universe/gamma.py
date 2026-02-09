"""
Gamma API client for fetching Polymarket markets.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from .utils import RateLimiter, parse_json_string_field

logger = logging.getLogger(__name__)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_PAGE_SIZE = 500
DEFAULT_MAX_PAGES = 500
DEFAULT_TIMEOUT_CONNECT = 10.0
DEFAULT_TIMEOUT_READ = 30.0
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0


class GammaClient:
    """
    Client for Polymarket Gamma API with pagination, rate limiting, and retries.
    """

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        timeout_connect: float = DEFAULT_TIMEOUT_CONNECT,
        timeout_read: float = DEFAULT_TIMEOUT_READ,
    ):
        self.rate_limiter = rate_limiter or RateLimiter(requests_per_second=2.0)
        self.client = httpx.Client(
            base_url=GAMMA_BASE_URL,
            timeout=httpx.Timeout(connect=timeout_connect, read=timeout_read, write=10.0, pool=10.0),
        )

    def close(self) -> None:
        """Close the HTTP client."""
        self.client.close()

    def __enter__(self) -> "GammaClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _request_with_retry(
        self, endpoint: str, params: dict[str, Any], page_num: int
    ) -> httpx.Response:
        """Make a GET request with retry logic and rate limiting."""
        last_error: Exception | None = None
        
        for attempt in range(MAX_RETRIES):
            self.rate_limiter.wait()
            start_time = time.monotonic()
            
            try:
                response = self.client.get(endpoint, params=params)
                latency_ms = (time.monotonic() - start_time) * 1000
                
                logger.info(
                    f"GET {endpoint} page={page_num} status={response.status_code} "
                    f"latency={latency_ms:.0f}ms bytes={len(response.content)}"
                )

                if response.status_code == 429:
                    # Rate limited - respect Retry-After header
                    retry_after = float(response.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limited, retrying after {retry_after}s")
                    self.rate_limiter.set_wait_until(retry_after)
                    continue

                if response.status_code >= 500:
                    # Server error - exponential backoff
                    backoff = INITIAL_BACKOFF * (2 ** attempt)
                    logger.warning(f"Server error {response.status_code}, backing off {backoff}s")
                    time.sleep(backoff)
                    continue

                response.raise_for_status()
                return response

            except httpx.RequestError as e:
                last_error = e
                backoff = INITIAL_BACKOFF * (2 ** attempt)
                logger.warning(f"Request error: {e}, backing off {backoff}s")
                time.sleep(backoff)
                continue

        raise last_error or RuntimeError("Max retries exceeded")



    def fetch_all_events(
        self,
        tag_id: str | None = None,
        series_id: str | None = None,
        max_events: int | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: int = DEFAULT_MAX_PAGES,
        active: bool | None = None,
        closed: bool | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch all events from Gamma API using pagination.
        
        Args:
            tag_id: Filter by tag ID (optional)
            series_id: Filter by series ID (optional, returns game markets instead of event groups)
            max_events: Stop after this many events (approx)
            page_size: Number of events per page
            max_pages: Safety limit for pages
            active: Filter by active status (True/False)
            closed: Filter by closed status (True/False)
        """
        all_events = []
        offset = 0
        page = 0
        
        while page < max_pages:
            # Check max events limit
            if max_events and len(all_events) >= max_events:
                break
            
            # Prepare params for this page
            params = {
                "limit": page_size,
                "offset": offset,
            }
            if tag_id:
                params["tag_id"] = tag_id
            if series_id:
                params["series_id"] = series_id
            if active is not None:
                params["active"] = str(active).lower()
            if closed is not None:
                params["closed"] = str(closed).lower()

            response = self._request_with_retry("/events", params, page)
            events = response.json()

            if not events or not isinstance(events, list):
                break

            # Process nested markets in events
            for event in events:
                # Markets are usually already parsed objects in /events endpoint,
                # but we should ensure string fields in markets are parsed if they exist as strings
                if "markets" in event:
                    for market in event["markets"]:
                        market["_outcomes_parsed"] = parse_json_string_field(market.get("outcomes"))
                        market["_clobTokenIds_parsed"] = parse_json_string_field(market.get("clobTokenIds"))

            all_events.extend(events)
            
            logger.info(f"Fetched page {page}: {len(events)} events (total: {len(all_events)})")
            
            if len(events) < page_size:
                # Last page
                break
                
            offset += len(events)
            page += 1
            
        if max_events:
            all_events = all_events[:max_events]
            
        if page >= max_pages:
            logger.warning(f"Reached max_pages limit of {max_pages}, may have more events")

        return all_events

    def fetch_tags(self) -> list[dict[str, Any]]:
        """Fetch all available tags."""
        try:
            # Retry only once or twice for tags as it's a small request
            response = self.client.get("/tags")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch tags: {e}")
            return []
