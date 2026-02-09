import json
import time
import os
import csv
from datetime import datetime
from playwright.sync_api import sync_playwright

def parse_fanduel_data(data, league_name):
    """
    Parses the 'attachments' extracted from the FanDuel JSON.
    Returns a list of dicts: {Sport, Game_Date, Event, BetType, HomeTeam, HomeOdds, AwayTeam, AwayOdds, Is_Live, Fetched_At}
    """
    rows = []
    
    attachments = data.get("attachments", {})
    markets = attachments.get("markets", {})
    events = attachments.get("events", {})
    
    print(f"[{league_name}] Parsing {len(markets)} markets, {len(events)} events...")

    for market_id, market in markets.items():
        # Filter for Moneyline
        market_name = market.get("marketName")
        if market_name != "Moneyline":
            continue
            
        # Get Event details
        event_id = market.get("eventId")
        event = events.get(str(event_id), {})
        event_name = event.get("name", "Unknown Event")
        event_date = event.get("openDate", "")
        
        # Get Runners (Teams)
        runners = market.get("runners", [])
        if len(runners) != 2:
            continue # Skip 3-way markets or issues
            
        # Extract Live Status
        is_live = market.get("inplay", False)
        
        # Format Date (ensure ISO if possible, but FD usually gives ISO)
        
        fetch_time = datetime.now().isoformat()
        
        # Construct URL
        # FanDuel URL format: https://sportsbook.fanduel.com/navigation/{sport}?tab=upcoming&eventId={eventId}
        # Sport name needs to be lowercase for URL
        sport_url = league_name.lower()
        event_url = f"https://sportsbook.fanduel.com/navigation/{sport_url}?tab=upcoming&eventId={event_id}"

        row = {
            "Sport": league_name,
            "Game_Date": event_date,
            "Event": event_name,
            "BetType": market_name,
            "HomeTeam": None,
            "HomeOdds": None,
            "AwayTeam": None,
            "AwayOdds": None,
            "Is_Live": is_live,
            "Fetched_At": fetch_time,
            "Url": event_url
        }
        
        for runner in runners:
            name = runner.get("runnerName")
            result_type = runner.get("result", {}).get("type") # HOME or AWAY
            
            # Extract Odds
            win_odds = runner.get("winRunnerOdds", {})
            american = win_odds.get("americanDisplayOdds", {}).get("americanOdds")
            
            if result_type == "HOME":
                row["HomeTeam"] = name
                row["HomeOdds"] = american
            elif result_type == "AWAY":
                row["AwayTeam"] = name
                row["AwayOdds"] = american
            else:
                # Fallback if no type (rare for Moneyline)
                pass
                
        # Only add valid rows
        if row["HomeTeam"] and row["AwayTeam"]:
            rows.append(row)
            
    return rows

def scrape_fanduel():
    print("Starting FanDuel Scraper...")
    os.makedirs("fanduel_data", exist_ok=True)
    
    all_rows = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            channel="chrome", # Use system Chrome for trust
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
             viewport={'width': 1280, 'height': 720}
        )
        
        # Anti-detection
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = context.new_page()

        leagues = {
            "NBA": "https://sportsbook.fanduel.com/navigation/nba",
            "NFL": "https://sportsbook.fanduel.com/navigation/nfl",
            "NHL": "https://sportsbook.fanduel.com/navigation/nhl"
        }
        
        for league, url in leagues.items():
            print(f"Fetching {league} from {url}...")
            
            # Container for the extracted payload
            target_payload = None
            
            def handle_response(response):
                nonlocal target_payload
                try:
                    if target_payload: return # Already found
                    
                    if "json" in response.headers.get("content-type", ""):
                        # Check URL keywords
                        if "getMarketPrices" in response.url: return # Skip minor updates
                        
                        try:
                            data = response.json()
                            # Heuristic: must have 'attachments' and 'markets'
                            if "attachments" in data and "markets" in data["attachments"]:
                                print(f"[{league}] Found Master Payload: {response.url}")
                                target_payload = data
                        except:
                            pass
                except:
                    pass

            # Attach listener
            page.on("response", handle_response)
            
            try:
                page.goto(url, timeout=60000, wait_until="domcontentloaded")
                
                # Scroll to encourage loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                
                # Wait for payload capture
                start_time = time.time()
                while not target_payload and time.time() - start_time < 20:
                    page.wait_for_timeout(500)
                    
                if target_payload:
                    league_rows = parse_fanduel_data(target_payload, league)
                    all_rows.extend(league_rows)
                    print(f"[{league}] Extracted {len(league_rows)} odds.")
                else:
                     print(f"[{league}] FAILED to capture master payload.")
                     
                page.wait_for_timeout(2000) # Polite gap
                
            except Exception as e:
                print(f"[{league}] Error: {e}")
            
            # Remove listener for next iteration
            page.remove_listener("response", handle_response)

        browser.close()
        
    # Save to CSV
    if all_rows:
        csv_path = "fanduel_data/fd_moneyline_odds.csv"
        keys = ["Sport", "Game_Date", "Event", "BetType", "HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds", "Is_Live", "Fetched_At", "Url"]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\nSUCCESS: Saved {len(all_rows)} rows to {csv_path}")
    else:
        print("\nFAILURE: No data extracted.")

if __name__ == "__main__":
    scrape_fanduel()
