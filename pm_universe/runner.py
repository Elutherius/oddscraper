"""
Main runner that orchestrates the full fetch flow.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from .clob import ClobClient
from .gamma import GammaClient
from .io_store import (
    copy_to_latest,
    write_clob_batch,
    write_manifest,
    write_markets_csv,
    write_prices_csv,
    write_raw_json,
)
from .models import MarketRecord, PriceResult, RunManifest, TokenOutcome
from .utils import RateLimiter, ensure_dirs, safe_json_dumps, utc_date_str, utc_now, utc_now_iso

logger = logging.getLogger(__name__)


def extract_market_record(market: dict[str, Any], event: dict[str, Any], pulled_at: str) -> MarketRecord:
    """Convert raw Gamma market to MarketRecord."""
    outcomes = market.get("_outcomes_parsed") or []
    token_ids = market.get("_clobTokenIds_parsed") or []

    # Try to find category in order of preference
    # 1. Event 'category' field
    # 2. Primary tag (if available in tags list)
    category = event.get("category", "")
    
    # If category is missing but we have tags, try to use the first tag
    if not category and event.get("tags"):
        tags = event.get("tags")
        if isinstance(tags, list) and len(tags) > 0:
            first_tag = tags[0]
            if isinstance(first_tag, dict):
                category = first_tag.get("label") or first_tag.get("slug") or ""
            elif isinstance(first_tag, str):
                category = first_tag

    return MarketRecord(
        pulled_at_utc=pulled_at,
        source="polymarket_gamma",
        market_id=str(market.get("id", "")),
        slug=str(market.get("slug", "")),
        question=str(market.get("question", "")),
        category=str(category),
        condition_id=str(market.get("conditionId", "")),
        active=market.get("active"),
        closed=market.get("closed"),
        end_date_utc=str(market.get("endDateIso") or market.get("endDate") or ""),
        outcomes_json=safe_json_dumps(outcomes),
        clob_token_ids_json=safe_json_dumps(token_ids),
        volume_num=market.get("volumeNum"),
        liquidity_num=market.get("liquidityNum"),
    )


def extract_token_outcomes(market: dict[str, Any]) -> list[TokenOutcome] | None:
    """
    Extract token outcomes from market.
    
    Returns None if outcomes/tokenIds are missing or mismatched.
    """
    outcomes = market.get("_outcomes_parsed")
    token_ids = market.get("_clobTokenIds_parsed")

    if not outcomes or not token_ids:
        return None

    if len(outcomes) != len(token_ids):
        return None

    # Note: We don't check enableOrderBook here - old markets may have None
    # but still have valid token IDs that can be priced

    result = []
    for i, token_id in enumerate(token_ids):
        if not token_id:
            continue
        result.append(TokenOutcome(
            token_id=str(token_id),
            outcome=str(outcomes[i]) if i < len(outcomes) else "",
            market_id=str(market.get("id", "")),
            slug=str(market.get("slug", "")),
            question=str(market.get("question", "")),
            active=market.get("active"),
            volume_num=market.get("volumeNum"),
            liquidity_num=market.get("liquidityNum"),
        ))
    return result if result else None


def run_fetch(
    date_str: str | None = None,
    outdir: Path | None = None,
    tag_id: str | None = None,
    max_markets: int | None = None,
    concurrency: int = 5,
    batch_size: int = 500,
    dry_run: bool = False,
    gamma_rate: float = 2.0,
    gamma_page_size: int = 500,
    clob_rate: float = 1.0,
    active_only: bool = False,
    category_filter: str | None = None,
    sports_series_ids: list[int] | None = None,
) -> RunManifest:
    """
    Execute the full fetch flow.
    
    Args:
        date_str: Date string (YYYY-MM-DD), defaults to today UTC
        outdir: Output directory, defaults to ./data
        tag_id: Filter by tag ID (optional)
        max_markets: Limit number of markets (for testing)
        concurrency: CLOB batch concurrency
        batch_size: CLOB batch size
        dry_run: If True, only fetch Gamma and skip pricing
        gamma_rate: Gamma API requests per second
        gamma_page_size: Gamma API page size
        clob_rate: CLOB API requests per second
        active_only: If True, fetch only active (not closed) markets
        category_filter: If provided, only process markets matching this category (case-insensitive substring)
        sports_series_ids: If provided, fetch events for each series_id and combine results (sports-only mode, returns game markets)
    
    Returns:
        RunManifest with stats and file paths
    """
    start_time = utc_now()
    start_ts = utc_now_iso()
    
    if date_str is None:
        date_str = utc_date_str(start_time)
    
    if outdir is None:
        outdir = Path("data")
    
    # Create directory structure
    paths = ensure_dirs(outdir, date_str)
    
    manifest = RunManifest(
        start_ts_utc=start_ts,
        date=date_str,
    )

    # Setup rate limiters
    gamma_limiter = RateLimiter(requests_per_second=gamma_rate) # Kept requests_per_second for clarity
    clob_limiter = RateLimiter(requests_per_second=clob_rate) # Kept requests_per_second for clarity

    # Determine filter params
    # "active=true" AND "closed=false" is the best way to get currently tradable markets
    gamma_active = True if active_only else None
    gamma_closed = False if active_only else None

    # Calculate max_events for pagination
    max_events = None
    if max_markets:
        max_events = max_markets  # Safe upper bound

    # Step 1: Fetch events from Gamma
    # If sports_series_ids is provided, fetch for each series_id and combine
    # Otherwise use the single tag_id (or None for all events)
    logger.info(f"Fetching events from Gamma API (page_size={gamma_page_size}, active_only={active_only}, tag_id={tag_id}, sports_mode={sports_series_ids is not None})...")
    
    raw_events = []
    with GammaClient(rate_limiter=gamma_limiter) as gamma:
        if sports_series_ids:
            # Sports-only mode: fetch for each series_id (returns game markets)
            for sport_series_id in sports_series_ids:
                logger.info(f"Fetching events for series_id={sport_series_id}...")
                events = gamma.fetch_all_events(
                    series_id=str(sport_series_id),
                    max_events=max_events,
                    page_size=gamma_page_size,
                    active=gamma_active,
                    closed=gamma_closed,
                )
                logger.info(f"Fetched {len(events)} events for series_id={sport_series_id}")
                raw_events.extend(events)
        else:
            # Normal mode: single tag_id or all events
            raw_events = gamma.fetch_all_events(
                tag_id=tag_id,
                max_events=max_events,
                page_size=gamma_page_size,
                active=gamma_active,
                closed=gamma_closed,
            )

    logger.info(f"Fetched {len(raw_events)} events")

    # Save raw Gamma response
    raw_gamma_path = paths["raw_gamma"] / f"events_{date_str}.json"
    write_raw_json(raw_gamma_path, raw_events)
    manifest.files["raw_gamma"] = str(raw_gamma_path)

    # Step 2: Extract market records and token mappings from events
    pulled_at = utc_now_iso()
    market_records: list[MarketRecord] = []
    all_token_outcomes: list[TokenOutcome] = []
    
    markets_with_tokens = 0
    markets_skipped_no_tokens = 0
    markets_skipped_mismatched = 0
    markets_not_clob_tradable = 0
    
    total_markets_processed = 0

    for event in raw_events:
        event_markets = event.get("markets", [])
        if not isinstance(event_markets, list):
            continue

        for market in event_markets:
            if max_markets and total_markets_processed >= max_markets:
                break
            
            # Pass event to extract category info
            record = extract_market_record(market, event, pulled_at)
            
            # Apply category filter if requested
            if category_filter:
                cat = record.category.lower()
                if category_filter.lower() not in cat:
                    continue

            total_markets_processed += 1
            market_records.append(record)

            # Check for token extraction
            outcomes = market.get("_outcomes_parsed")
            token_ids = market.get("_clobTokenIds_parsed")

            if not outcomes or not token_ids:
                markets_skipped_no_tokens += 1
                continue

            if len(outcomes) != len(token_ids):
                markets_skipped_mismatched += 1
                logger.warning(
                    f"Market {market.get('id')} has mismatched arrays: "
                    f"{len(outcomes)} outcomes vs {len(token_ids)} token_ids"
                )
                continue

            # Track markets where enableOrderBook is explicitly False (not just None)
            if market.get("enableOrderBook") is False:
                markets_not_clob_tradable += 1
                # Still try to price them if they have tokens

            token_outcomes = extract_token_outcomes(market)
            if token_outcomes:
                all_token_outcomes.extend(token_outcomes)
                markets_with_tokens += 1
            else:
                markets_skipped_no_tokens += 1
        
        if max_markets and total_markets_processed >= max_markets:
            break

    manifest.markets_total = total_markets_processed
    manifest.markets_with_tokens = markets_with_tokens
    manifest.markets_skipped_no_tokens = markets_skipped_no_tokens
    manifest.markets_skipped_mismatched_arrays = markets_skipped_mismatched
    manifest.markets_not_clob_tradable = markets_not_clob_tradable
    manifest.tokens_total = len(all_token_outcomes)

    # Step 3: Write markets CSV
    markets_csv_path = paths["markets"] / f"markets_{date_str}.csv"
    write_markets_csv(markets_csv_path, market_records)
    manifest.files["markets_csv"] = str(markets_csv_path)
    logger.info(f"Wrote {len(market_records)} markets to {markets_csv_path}")

    # Step 4: Dry run check
    if dry_run:
        logger.info(f"Dry run: would price {len(all_token_outcomes)} tokens")
        print(f"\n[DRY RUN] Would fetch prices for {len(all_token_outcomes)} tokens from {markets_with_tokens} markets")
        print(f"Markets CSV written: {markets_csv_path}")
        
        manifest.end_ts_utc = utc_now_iso()
        manifest.duration_seconds = (utc_now() - start_time).total_seconds()
        
        manifest_path = paths["run"] / f"run_manifest_{date_str}.json"
        write_manifest(manifest_path, manifest)
        
        return manifest

    # Step 5: Fetch prices from CLOB
    if all_token_outcomes:
        logger.info(f"Fetching prices for {len(all_token_outcomes)} tokens...")
        clob = ClobClient(rate_limiter=clob_limiter)
        
        price_results, raw_batches, price_stats = clob.fetch_all_prices(
            all_token_outcomes,
            concurrency=concurrency,
            batch_size=batch_size,
            snapshot_ts=pulled_at,
        )

        # Save raw batch responses
        for batch_response in raw_batches:
            write_clob_batch(
                paths["raw_clob_batches"],
                batch_response["batch_num"],
                batch_response,
            )

        manifest.price_batches = len(raw_batches)
        manifest.tokens_priced_ok = price_stats["tokens_priced_ok"]
        manifest.tokens_missing_price = price_stats["tokens_missing_price"]
        manifest.api_errors = price_stats["api_errors"]

        # Step 6: Write prices CSV
        prices_csv_path = paths["prices"] / f"prices_{date_str}.csv"
        write_prices_csv(prices_csv_path, price_results)
        manifest.files["prices_csv"] = str(prices_csv_path)
        logger.info(f"Wrote {len(price_results)} price rows to {prices_csv_path}")

        # Step 7: Copy to latest.csv
        latest_csv_path = paths["prices"] / "latest.csv"
        copy_to_latest(prices_csv_path, latest_csv_path)
        manifest.files["latest_csv"] = str(latest_csv_path)
    else:
        logger.warning("No tokens to price")

    # Step 8: Write manifest
    manifest.end_ts_utc = utc_now_iso()
    manifest.duration_seconds = (utc_now() - start_time).total_seconds()
    
    manifest_path = paths["run"] / f"run_manifest_{date_str}.json"
    write_manifest(manifest_path, manifest)
    manifest.files["manifest"] = str(manifest_path)

    return manifest


def print_summary(manifest: RunManifest) -> None:
    """Print run summary to console."""
    print("\n" + "=" * 60)
    print("POLYMARKET UNIVERSE SNAPSHOT COMPLETE")
    print("=" * 60)
    print(f"Date: {manifest.date}")
    print(f"Duration: {manifest.duration_seconds:.1f}s")
    print()
    print("MARKETS:")
    print(f"  Total fetched:          {manifest.markets_total}")
    print(f"  With priceable tokens:  {manifest.markets_with_tokens}")
    print(f"  Skipped (no tokens):    {manifest.markets_skipped_no_tokens}")
    print(f"  Skipped (mismatch):     {manifest.markets_skipped_mismatched_arrays}")
    print(f"  Not CLOB tradable:      {manifest.markets_not_clob_tradable}")
    print()
    print("TOKENS:")
    print(f"  Total:                  {manifest.tokens_total}")
    print(f"  Priced OK:              {manifest.tokens_priced_ok}")
    print(f"  Missing price:          {manifest.tokens_missing_price}")
    print(f"  API errors:             {manifest.api_errors}")
    print()
    print("FILES:")
    for name, path in manifest.files.items():
        print(f"  {name}: {path}")
    print("=" * 60)
