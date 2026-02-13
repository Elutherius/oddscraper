# Data Fetching Guide
This guide details how to fetch betting odds from various sportsbooks using the current codebase.
## 🚀 Quick Start: The E2E Pipeline
The easiest way to pull all data is to run the end-to-end pipeline. This orchestrates fetching from all sources and consolidates the results.
```bash
python run_e2e_pipeline.py
```
**What this does:**
1.  **Polymarket**: Fetches markets and prices via API.
2.  **DraftKings**: Scrapes NBA, NHL, NFL odds via Playwright.
3.  **FanDuel**: Intercepts internal API traffic via Playwright.
4.  **PointsBet**: Scrapes DOM elements via Playwright.
5.  **Kalshi**: Fetches public market data via API.
6.  **Consolidate**: Merges all sources into `consolidated_odds.csv`.
---
## 🛠️ Prerequisites
Ensure you have the necessary Python packages and browser drivers installed:
```bash
pip install playwright pandas httpx
python -m playwright install chromium
```
---
## 📊 Individual Data Sources
If you need to debug or run a specific fetcher, use the commands below.
### 1. Polymarket (API)
*   **Command**: `python -m pm_universe fetch`
*   **Method**: Official Gamma and CLOB APIs.
*   **Output**: 
    *   `data/markets/markets_YYYY-MM-DD.csv`
    *   `data/prices/prices_YYYY-MM-DD.csv`
*   **Notes**: Fast and reliable. No browser required.
### 2. DraftKings (Scraping)
*   **Command**: `python draftkings_runner.py`
    *   *Calls `fetch_dk_playwright.py` then `convert_dk_json_to_csv.py`*
*   **Method**: Playwright (Headless Browser). Navigates to sportsbook pages and scrapes table data.
*   **Output**: `draftkings_data/dk_nba_odds.csv` (and other sports)
*   **Quirks**: 
    *   **Geo-blocking**: Highly sensitive. Must be in a legal jurisdiction. VPNs often fail.
    *   **Anti-Bot**: If it hangs or fails, try changing `headless=True` to `headless=False` in `fetch_dk_playwright.py`.
### 3. FanDuel (Network Interception)
*   **Command**: `python fetch_fanduel.py`
*   **Method**: Playwright (Headless). Launches browser but **listens to network traffic** rather than parsing HTML. Captures the internal JSON payload sent to the frontend.
*   **Output**: `fanduel_data/fd_moneyline_odds.csv`
*   **Quirks**: 
    *   Robust against UI changes but sensitive to API endpoint changes.
    *   Hardcoded URLs for NBA, NFL, NHL.
### 4. PointsBet (DOM Scraping)
*   **Command**: 
    1. `python fetch_pointsbet.py` (Scrape)
    2. `python process_pointsbet_data.py` (Process)
*   **Method**: Playwright. Finds buttons with `data-category="oddsButton"`.
*   **Output**: 
    *   Interim: `pointsbet_data/pointsbet_scraped.json`
    *   Final: processed CSV in `pointsbet_data/`
*   **Quirks**: 
    *   Fragile. Relies on specific CSS classes/attributes.
    *   "Live" detection is a heuristic (checking parent elements for "Live" text).
### 5. Kalshi (API)
*   **Command**: `python fetch_kalshi.py`
*   **Method**: Internal `KalshiClient` (Official/Public API).
*   **Output**: `kalshi_data/kalshi_odds.csv`
*   **Quirks**: 
    *   Uses public endpoints (no auth required for basic market data).
    *   Markets are filtered for "Sports" categories.
---
## 🔄 Consolidation
To merge all partial datasets into one master file:
```bash
python consolidate_odds.py
```
**Output**: `consolidated_odds.csv`
**Columns to expect**:
*   `Sport`, `Date`, `Event`, `Market`
*   `HomeTeam`, `HomeOdds` (American)
*   `AwayTeam`, `AwayOdds` (American)
*   `Source` (e.g., 'DraftKings', 'FanDuel')
*   `ImpliedProbability` (Calculated)
