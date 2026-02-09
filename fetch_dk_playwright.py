from playwright.sync_api import sync_playwright
import json
import time
import random
import os

def scrape_dk_multisport():
    leagues = {
        "NBA": "leagues/basketball/nba",
        "NHL": "leagues/hockey/nhl",
        "NFL": "leagues/football/nfl",
        "NCAAB_Mens": "leagues/basketball/ncaab",
    }
    
    base_url = "https://sportsbook.draftkings.com/"
    all_data = {}
    
    print("Starting Multi-Sport Scraper (Robust V3)...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Randomize User Agent slightly
        ua = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(110,120)}.0.0.0 Safari/537.36"
        context = browser.new_context(
            user_agent=ua,
            viewport={'width': 1280, 'height': 720}
        )
        page = context.new_page()
        
        try:
            for league_name, path in leagues.items():
                url = base_url + path
                print(f"\n[{league_name}] Navigating...")
                time.sleep(random.uniform(3, 7)) # Increased delay
                
                try:
                    page.goto(url, timeout=45000, wait_until="domcontentloaded")
                    
                    # Explicit wait
                    time.sleep(5)
                    
                    # Try to find table with extended timeout
                    print(f"[{league_name}] Waiting for table...")
                    try:
                        page.wait_for_selector(".sportsbook-table__body", timeout=15000)
                    except:
                        print(f"[{league_name}] Primary selector failed. Checking for generic table...")
                        if page.query_selector("table"):
                             print(f"[{league_name}] Generic table found.")
                        else:
                             print(f"[{league_name}] No table element found. Title: {page.title()}")
                             # Dump snippet logic if needed, but keeping it clean
                             all_data[league_name] = {"error": "Timeout/NoTable"}
                             continue

                    # Scrape - Extract event rows to get URLs
                    event_rows = page.query_selector_all(".sportsbook-event-accordion__wrapper, tr.sportsbook-table__row")
                    
                    if event_rows:
                         print(f"[{league_name}] Found {len(event_rows)} event rows.")
                         events_data = []
                         
                         for row in event_rows:
                             try:
                                 # Extract event link
                                 event_link = row.query_selector("a.event-cell-link, a[href*='/event/']")
                                 event_url = ""
                                 if event_link:
                                     href = event_link.get_attribute("href")
                                     if href:
                                         # Make absolute URL
                                         if href.startswith("/"):
                                             event_url = f"https://sportsbook.draftkings.com{href}"
                                         else:
                                             event_url = href
                                 
                                 # Extract team labels
                                 labels = row.query_selector_all(".cb-market__label")
                                 label_text = [l.inner_text().replace('\n', ' ') for l in labels]
                                 
                                 # Extract odds
                                 odds_buttons = row.query_selector_all(".cb-market__button-odds")
                                 odds_text = [b.inner_text().replace('\n', ' ') for b in odds_buttons]
                                 
                                 if label_text or odds_text:
                                     events_data.append({
                                         "url": event_url,
                                         "labels": label_text,
                                         "odds": odds_text
                                     })
                             except Exception as e:
                                 print(f"[{league_name}] Error parsing row: {e}")
                                 continue
                         
                         all_data[league_name] = {
                             "events": events_data
                         }
                         print(f"[{league_name}] Extracted {len(events_data)} events with URLs.")
                    else:
                        print(f"[{league_name}] No event rows found (likely no games scheduled).")
                        all_data[league_name] = {"status": "empty"}

                except Exception as e:
                    print(f"[{league_name}] Error: {e}")
                    import traceback
                    traceback.print_exc()
                    all_data[league_name] = {"error": str(e)}

                # Save Checkpoint
                os.makedirs("draftkings_data", exist_ok=True)
                with open('draftkings_data/dk_all_sports.json', 'w') as f:
                    json.dump(all_data, f, indent=2)

        finally:
            browser.close()

if __name__ == "__main__":
    scrape_dk_multisport()
