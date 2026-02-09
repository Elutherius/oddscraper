"""
Filtering logic for market CSVs.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


def filter_markets_by_category(
    input_path: Path, output_path: Path, category_filter: str
) -> int:
    """
    Filter a markets CSV by category and write matching rows to a new CSV.
    
    Args:
        input_path: Path to input CSV
        output_path: Path to output CSV
        category_filter: Category string to match (case-insensitive substring)
        
    Returns:
        Number of rows written
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    target = category_filter.lower().strip()
    match_count = 0

    with open(input_path, "r", newline="", encoding="utf-8") as fin, \
         open(output_path, "w", newline="", encoding="utf-8") as fout:
        
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            raise ValueError("Input CSV is empty or malformed")
            
        # Ensure category exists in input, or warn
        if "category" not in reader.fieldnames:
            logger.warning("Input CSV does not have a 'category' column. Filtering likely to fail/empty.")
        
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            cat_val = row.get("category", "").lower()
            if target in cat_val:
                writer.writerow(row)
                match_count += 1

    return match_count
