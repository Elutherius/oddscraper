"""
E2E Betting Data Pipeline Runner
--------------------------------
Runs the full data acquisition and consolidation process:
1. Fetch Polymarket Data (API)
2. Scrape DraftKings Data (Playwright)
3. Consolidate Odds (CSV Processing)
"""

import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, Optional

import pandas as pd


def run_command(command, description):
    """Runs a shell command and prints status."""
    print(f"\n[{time.strftime('%H:%M:%S')}] STARTING: {description}")
    print(f"Command: {command}")

    start = time.time()
    try:
        if isinstance(command, str):
            cmd_list = command.split()
        else:
            cmd_list = command

        result = subprocess.run(command, shell=True, check=True)

        duration = time.time() - start
        print(f"[{time.strftime('%H:%M:%S')}] COMPLETED: {description} (took {duration:.2f}s)")
        return True

    except subprocess.CalledProcessError as e:
        print(f"[{time.strftime('%H:%M:%S')}] FAILED: {description}")
        print(f"Error: {e}")
        return False


def _slugify(label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return slug or "source"


def _write_metadata(path: str, data: Dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def archive_run_outputs(
    consolidated_path: str = "consolidated_odds.csv",
    archive_dir: str = "downloads",
) -> Optional[Dict]:
    """Archive consolidated + per-source CSVs for the latest run."""
    if not os.path.exists(consolidated_path):
        print(f">> Consolidated file not found at {consolidated_path}. Skipping archive.")
        return None

    now = datetime.utcnow()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    pull_time_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    os.makedirs(archive_dir, exist_ok=True)
    history_dir = os.path.join(archive_dir, "history", timestamp)
    os.makedirs(history_dir, exist_ok=True)

    archived_latest = os.path.join(archive_dir, f"consolidated_odds_{timestamp}.csv")
    shutil.copy2(consolidated_path, archived_latest)
    run_consolidated = os.path.join(history_dir, "consolidated_odds.csv")
    shutil.copy2(consolidated_path, run_consolidated)

    metadata = {
        "timestamp": timestamp,
        "pull_time_iso": pull_time_iso,
        "files": {"Consolidated": os.path.basename(run_consolidated)},
    }

    try:
        df = pd.read_csv(consolidated_path)
    except Exception as exc:  # pylint: disable=broad-except
        print(f">> Unable to split per-source archives: {exc}")
        metadata_path = os.path.join(history_dir, "metadata.json")
        _write_metadata(metadata_path, metadata)
        return metadata

    if "Source" not in df.columns:
        metadata_path = os.path.join(history_dir, "metadata.json")
        _write_metadata(metadata_path, metadata)
        return metadata

    sources_created = []
    for source_name, source_df in df.groupby("Source"):
        if not isinstance(source_name, str) or source_df.empty:
            continue
        slug = _slugify(source_name)
        file_name = f"{slug}.csv"
        file_path = os.path.join(history_dir, file_name)
        source_df.to_csv(file_path, index=False)
        metadata["files"][source_name] = file_name
        sources_created.append(source_name)

    metadata_path = os.path.join(history_dir, "metadata.json")
    _write_metadata(metadata_path, metadata)

    print(f">> Archived consolidated snapshot to {archived_latest}")
    if sources_created:
        print(f">> Archived per-source files for: {', '.join(sorted(sources_created))}")
    else:
        print(">> No per-source files were created (no Source column found).")
    print(f">> Run history stored in: {history_dir}")
    return metadata


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print(f"Working Directory set to: {script_dir}")

    print("========================================")
    print("   BETTING ODDS E2E PIPELINE RUNNER     ")
    print("========================================")

    total_start = time.time()

    if not run_command("python -m pm_universe fetch --active --category Sports", "Polymarket Data Fetch"):
        print(">> Polymarket fetch failed (non-critical).")

    if not run_command("python fetch_dk_playwright.py", "DraftKings Scraper"):
        print(">> DraftKings scrape failed.")
    else:
        if not run_command("python convert_dk_json_to_csv.py", "DraftKings Conversion"):
            print(">> DraftKings conversion failed.")

    if not run_command("python fetch_fanduel.py", "FanDuel Scraper"):
        print(">> FanDuel step failed (non-critical), continuing...")

    if not run_command("python fetch_pointsbet.py", "PointsBet Scraper"):
        print(">> PointsBet scrape failed (non-critical), continuing...")
    else:
        if not run_command("python process_pointsbet_data.py", "PointsBet Processing"):
            print(">> PointsBet processing failed.")

    if not run_command("python fetch_kalshi.py", "Kalshi Fetch"):
         print(">> Kalshi fetch failed (non-critical), continuing...")

    if not run_command("python consolidate_odds.py", "Data Consolidation"):
        print("Pipeline aborted due to Consolidation failure.")
        return

    archive_run_outputs()

    total_duration = time.time() - total_start
    print("\n========================================")
    print("   ALL SYSTEMS GO - PIPELINE SUCCESS    ")
    print(f"   Total Duration: {total_duration:.2f}s   ")
    print("========================================")
    print("Output available in: consolidated_odds.csv")


if __name__ == "__main__":
    main()
