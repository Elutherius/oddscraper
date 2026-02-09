"""
Runner for fetching Kalshi data.
"""
from __future__ import annotations

import csv
import logging
from pathlib import Path
from datetime import datetime

from .kalshi import KalshiClient
from .utils import ensure_dirs, utc_now_iso

logger = logging.getLogger(__name__)

def run_kalshi_fetch(
    outdir: Path | None = None,
    limit: int | None = None
) -> str:
    """
    Fetch all active sports markets from Kalshi and save to CSV.
    Returns path to the saved CSV.
    """
    if outdir is None:
        outdir = Path("data/kalshi")
    
    # Ensure directory exists
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    outdir.mkdir(parents=True, exist_ok=True)
    
    filename = outdir / f"markets_{date_str}.csv"
    
    logger.info(f"Starting Kalshi sports fetch. Output: {filename}")
    
    markets = []
    with KalshiClient() as client:
        # If credentials env vars are set, it will use them, otherwise public
        markets = client.get_sports_markets(limit=limit if limit else 10000)
    
    logger.info(f"Fetched {len(markets)} sports markets.")
    
    if not markets:
        logger.warning("No markets found.")
        return str(filename)

    # Flatten and normalize data for CSV
    # We want a format useful for analysis.
    rows = []
    pulled_at = utc_now_iso()
    
    for m in markets:
        # Extract best prices
        yes_bid = m.get("yes_bid", 0)
        yes_ask = m.get("yes_ask", 0)
        
        row = {
            "pulled_at": pulled_at,
            "ticker": m.get("ticker"),
            "event_ticker": m.get("event_ticker"),
            "market_type": m.get("market_type"),
            "title": m.get("title"),
            "subtitle": m.get("subtitle"),
            "yes_sub_title": m.get("yes_sub_title"),
            "no_sub_title": m.get("no_sub_title"),
            "status": m.get("status"),
            "open_time": m.get("open_time"),
            "close_time": m.get("close_time"),
            "expected_expiration_time": m.get("expected_expiration_time"),
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": m.get("no_bid"),
            "no_ask": m.get("no_ask"),
            "last_price": m.get("last_price"),
            "volume_24h": m.get("volume_24h"),
            "liquidity": m.get("liquidity"),
            "open_interest": m.get("open_interest"),
            "category": m.get("category"),
            "series_ticker": m.get("series_ticker"),
        }
        rows.append(row)

    # Write CSV
    if rows:
        fieldnames = list(rows[0].keys())
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            
    logger.info(f"Saved {len(rows)} rows to {filename}")
    return str(filename)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_kalshi_fetch()
