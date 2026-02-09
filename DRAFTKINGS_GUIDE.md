
# Pulling Odds Data from DraftKings

DraftKings implementation of strong anti-bot and geo-fencing measures makes standard HTTP requests (like Python's `requests`) difficult. They often result in `403 Access Denied` or redirection to a "Not Available" page.

## Recommended Approach: Browser Automation

The most reliable way to pull data programmatically is to use a browser automation tool like **Playwright** or **Selenium**. This mimics a real user, executing JavaScript and handling cookies properly.

### Prerequisites

1.  **Python Installed**: You already have this.
2.  **Playwright**: A powerful browser automation library.

### Setup

I have setup the environment for you. If you need to reinstall elsewhere:

```bash
pip install playwright
python -m playwright install chromium
```

### The Script

I have created a script `fetch_dk_playwright.py` in your workspace.

**How it works:**
1.  Launches a headless Chromium browser.
2.  Navigates to `https://sportsbook.draftkings.com/leagues/basketball/nba`.
3.  Waits for the `.sportsbook-table__body` to load (ensuring dynamic content is rendered).
4.  Scrapes the row data and saves it to `dk_nba_odds.json`.

### Running the Script

```bash
python fetch_dk_playwright.py
```

### Troubleshooting

*   **Geo-Blocking**: If you see "DraftKings is blocking this request", ensure you are running the script from a location where DraftKings Sportsbook is legal and accessible. VPNs are often blocked.
*   **Headless Mode**: If bot detection is aggressive, try changing `headless=True` to `headless=False` in the script to see the browser open and potentially manually solve a CAPTCHA if presented.

## Alternative: Official API?

DraftKings **does not** have a public API for Sportsbook odds. All third-party "APIs" are either scraping (like we are doing) or expensive aggregators (like Sportradar/OddsJam).

## Next Steps

*   Parse the raw text data in `dk_nba_odds.json` into a structured format (Team, Spread, Moneyline, Total).
*   Schedule the script to run periodically (e.g., via Cron or Windows Task Scheduler).
