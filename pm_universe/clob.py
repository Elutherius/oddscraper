"""
CLOB API client for fetching Polymarket prices.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from typing import Any

import httpx

from .models import PriceResult, TokenOutcome
from .utils import RateLimiter, chunk_list, parse_decimal, utc_now_iso

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"
DEFAULT_BATCH_SIZE = 500
DEFAULT_CONCURRENCY = 5
DEFAULT_TIMEOUT_CONNECT = 10.0
DEFAULT_TIMEOUT_READ = 30.0
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0


class ClobClient:
    """
    Client for Polymarket CLOB API with batching, rate limiting, and retries.
    """

    def __init__(
        self,
        rate_limiter: RateLimiter | None = None,
        timeout_connect: float = DEFAULT_TIMEOUT_CONNECT,
        timeout_read: float = DEFAULT_TIMEOUT_READ,
    ):
        self.rate_limiter = rate_limiter or RateLimiter(requests_per_second=1.0)
        self.timeout = httpx.Timeout(
            connect=timeout_connect, read=timeout_read, write=10.0, pool=10.0
        )

    def _create_client(self) -> httpx.Client:
        """Create a new HTTP client (for thread safety)."""
        return httpx.Client(base_url=CLOB_BASE_URL, timeout=self.timeout)

    def _request_with_retry(
        self, client: httpx.Client, request_items: list[dict[str, str]], batch_num: int
    ) -> tuple[dict[str, Any], int]:
        """
        Make a POST /prices request with retry logic and rate limiting.
        
        Returns (response_data, status_code).
        """
        last_error: Exception | None = None
        
        for attempt in range(MAX_RETRIES):
            self.rate_limiter.wait()
            start_time = time.monotonic()
            
            try:
                response = client.post("/prices", json=request_items)
                latency_ms = (time.monotonic() - start_time) * 1000
                
                logger.info(
                    f"POST /prices batch={batch_num} items={len(request_items)} "
                    f"status={response.status_code} latency={latency_ms:.0f}ms bytes={len(response.content)}"
                )

                if response.status_code == 429:
                    retry_after = float(response.headers.get("Retry-After", 5))
                    logger.warning(f"Rate limited, retrying after {retry_after}s")
                    self.rate_limiter.set_wait_until(retry_after)
                    continue

                if response.status_code >= 500:
                    backoff = INITIAL_BACKOFF * (2 ** attempt)
                    logger.warning(f"Server error {response.status_code}, backing off {backoff}s")
                    time.sleep(backoff)
                    continue

                if response.status_code == 200:
                    return response.json(), response.status_code
                else:
                    logger.warning(f"Unexpected status {response.status_code}")
                    return {}, response.status_code

            except httpx.RequestError as e:
                last_error = e
                backoff = INITIAL_BACKOFF * (2 ** attempt)
                logger.warning(f"Request error: {e}, backing off {backoff}s")
                time.sleep(backoff)
                continue

        logger.error(f"Max retries exceeded for batch {batch_num}: {last_error}")
        return {}, 0

    def _fetch_batch(
        self, batch_num: int, request_items: list[dict[str, str]]
    ) -> tuple[int, dict[str, Any], int]:
        """Fetch a single batch, returning (batch_num, response_data, status)."""
        with self._create_client() as client:
            data, status = self._request_with_retry(client, request_items, batch_num)
            return batch_num, data, status

    def fetch_all_prices(
        self,
        token_outcomes: list[TokenOutcome],
        concurrency: int = DEFAULT_CONCURRENCY,
        batch_size: int = DEFAULT_BATCH_SIZE,
        snapshot_ts: str | None = None,
    ) -> tuple[list[PriceResult], list[dict[str, Any]], dict[str, int]]:
        """
        Fetch prices for all tokens using concurrent batch requests.
        
        Returns:
            - List of PriceResult objects
            - List of raw batch responses (for storage)
            - Stats dict with counts
        """
        if snapshot_ts is None:
            snapshot_ts = utc_now_iso()

        # Build request items: one item per token requesting both sides
        # We request BUY only since response may include both sides
        request_items: list[dict[str, str]] = []
        for token in token_outcomes:
            request_items.append({"token_id": token.token_id, "side": "BUY"})
            request_items.append({"token_id": token.token_id, "side": "SELL"})

        # Create token lookup for easy access
        token_lookup = {t.token_id: t for t in token_outcomes}

        # Chunk into batches
        batches = list(chunk_list(request_items, batch_size))
        logger.info(f"Fetching prices for {len(token_outcomes)} tokens in {len(batches)} batches")

        all_raw_responses: list[dict[str, Any]] = []
        prices_by_token: dict[str, dict[str, str]] = {}  # token_id -> {BUY: price, SELL: price}
        api_error_tokens: set[str] = set()

        # Execute batches concurrently
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(self._fetch_batch, i, batch): i
                for i, batch in enumerate(batches)
            }

            for future in as_completed(futures):
                batch_num = futures[future]
                try:
                    _, data, status = future.result()
                    all_raw_responses.append({
                        "batch_num": batch_num,
                        "status": status,
                        "data": data,
                    })

                    if status == 200 and data:
                        # Response format: { token_id: { "BUY": "price", "SELL": "price" } }
                        for token_id, sides in data.items():
                            if isinstance(sides, dict):
                                if token_id not in prices_by_token:
                                    prices_by_token[token_id] = {}
                                for side, price in sides.items():
                                    if side in ("BUY", "SELL"):
                                        prices_by_token[token_id][side] = str(price)
                    elif status != 200:
                        # Mark all tokens in this batch as having API errors
                        batch = batches[batch_num]
                        for item in batch:
                            api_error_tokens.add(item["token_id"])

                except Exception as e:
                    logger.error(f"Batch {batch_num} failed: {e}")
                    batch = batches[batch_num]
                    for item in batch:
                        api_error_tokens.add(item["token_id"])

        # Build PriceResult objects
        results: list[PriceResult] = []
        stats = {
            "tokens_priced_ok": 0,
            "tokens_missing_price": 0,
            "api_errors": 0,
        }

        for token in token_outcomes:
            token_id = token.token_id
            price_data = prices_by_token.get(token_id, {})
            
            # bid = SELL price, ask = BUY price
            bid_str = price_data.get("SELL", "")
            ask_str = price_data.get("BUY", "")
            
            # Calculate mid if both exist
            mid_str = ""
            if bid_str and ask_str:
                bid_dec = parse_decimal(bid_str)
                ask_dec = parse_decimal(ask_str)
                if bid_dec is not None and ask_dec is not None:
                    mid_dec = (bid_dec + ask_dec) / Decimal(2)
                    mid_str = str(mid_dec)

            # Determine status
            if token_id in api_error_tokens:
                status = "api_error"
                stats["api_errors"] += 1
            elif not bid_str and not ask_str:
                status = "missing_price"
                stats["tokens_missing_price"] += 1
            else:
                status = "ok"
                stats["tokens_priced_ok"] += 1

            results.append(PriceResult(
                snapshot_ts_utc=snapshot_ts,
                source="polymarket_clob",
                market_id=token.market_id,
                slug=token.slug,
                question=token.question,
                token_id=token_id,
                outcome=token.outcome,
                bid=bid_str,
                ask=ask_str,
                mid=mid_str,
                active=token.active,
                status=status,
                volume_num=token.volume_num,
                liquidity_num=token.liquidity_num,
            ))

        return results, all_raw_responses, stats
