"""
IO and storage functions for writing CSVs, JSON, and manifest.
"""

from __future__ import annotations

import csv
import gzip
import json
import shutil
from pathlib import Path
from typing import Any

from .models import MarketRecord, PriceResult, RunManifest


def write_raw_json(path: Path, data: Any, compress: bool = False) -> None:
    """Write raw JSON data to file."""
    if compress:
        path = path.with_suffix(path.suffix + ".gz")
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def write_markets_csv(path: Path, records: list[MarketRecord]) -> None:
    """Write markets to CSV file."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(MarketRecord.csv_headers())
        for record in records:
            writer.writerow(record.to_csv_row())


def write_prices_csv(path: Path, records: list[PriceResult]) -> None:
    """Write prices to CSV file."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(PriceResult.csv_headers())
        for record in records:
            writer.writerow(record.to_csv_row())


def write_manifest(path: Path, manifest: RunManifest) -> None:
    """Write run manifest to JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest.to_dict(), f, ensure_ascii=False, indent=2)


def copy_to_latest(src: Path, dst: Path) -> None:
    """Copy file to latest location."""
    shutil.copy2(src, dst)


def write_clob_batch(batch_dir: Path, batch_num: int, data: dict[str, Any]) -> Path:
    """Write a single CLOB batch response to JSON file."""
    path = batch_dir / f"batch_{batch_num:04d}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    return path
