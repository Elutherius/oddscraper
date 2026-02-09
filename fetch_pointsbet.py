from playwright.sync_api import sync_playwright
import time
import json
import re

def scrape_pointsbet():
    base_url = "https://on.pointsbet.ca"
    leagues = {
        "NBA": "/sports/basketball/NBA",
        "NHL": "/sports/ice-hockey/NHL",
        "NFL": "/sports/american-football/NFL",
        # Add more if needed found in discovery
    }
    
    all_data = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 720}
        )
        
        for league_name, path in leagues.items():
            url = base_url + path
            print(f"Scraping {league_name} from {url}...")
            
            page = context.new_page()
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                
                # Wait for at least some odds buttons to load
                try:
                    page.wait_for_selector('button[data-category="oddsButton"]', timeout=10000)
                except:
                    print(f"No odds buttons found for {league_name}")
                    all_data[league_name] = []
                    continue
                
                # Extract buttons
                buttons = page.query_selector_all('button[data-category="oddsButton"]')
                print(f"Found {len(buttons)} odds buttons for {league_name}")
                
                league_data = []
                
                # Try to extract event URLs by finding event containers
                # PointsBet typically has event links in parent containers
                event_containers = page.query_selector_all('[data-event]')
                event_urls_map = {}
                
                for container in event_containers:
                    evt_id = container.get_attribute("data-event")
                    if evt_id:
                        # Try to find a link within this container
                        link = container.query_selector('a[href*="/sports/"]')
                        if link:
                            href = link.get_attribute("href")
                            if href:
                                if href.startswith("/"):
                                    event_urls_map[evt_id] = f"https://on.pointsbet.ca{href}"
                                else:
                                    event_urls_map[evt_id] = href
                
                for btn in buttons:
                    # Get attributes
                    label = btn.get_attribute("data-label")
                    prop = btn.get_attribute("data-property")
                    value_decimal = btn.get_attribute("data-value")
                    market_id = btn.get_attribute("data-market")
                    outcome_id = btn.get_attribute("data-outcome")
                    event_id = btn.get_attribute("data-event")
                    text_content = btn.inner_text().strip()
                    
                    # Get URL from map if available
                    event_url = event_urls_map.get(event_id, "")
                    
                    # Heuristic for Live status: Check ancestor for "Live" text
                    # We search nearest 3 parent divs for efficiency
                    is_live = btn.evaluate("""element => {
                        let p = element.closest('div');
                        for(let i=0; i<3; i++) {
                            if(!p) break;
                            if(p.innerText.includes('Live') || p.innerText.includes('In-Play')) return true;
                            p = p.parentElement;
                        }
                        return false;
                    }""")
                    
                    # Heuristic for Date: Check ancestor for date pattern (e.g. "Feb 7th")
                    date_content = btn.evaluate("""element => {
                        let p = element.closest('div');
                        for(let i=0; i<10; i++) {
                            if(!p) break;
                            let text = p.innerText;
                            // Look for Month + Day pattern
                            if (text.match(/[A-Z][a-z]{2,8} \d{1,2}(st|nd|rd|th)?/)) {
                                return text;
                            }
                            p = p.parentElement;
                        }
                        return "";
                    }""")

                    item = {
                        "label": label,
                        "property": prop,
                        "decimal_odds": value_decimal,
                        "market_id": market_id,
                        "outcome_id": outcome_id,
                        "event_id": event_id,
                        "text_content": text_content,
                        "is_live": is_live,
                        "date_content": date_content,
                        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "url": event_url
                    }
                    league_data.append(item)
                    
                all_data[league_name] = league_data
                
            except Exception as e:
                print(f"Error scraping {league_name}: {e}")
            finally:
                page.close()
                
        browser.close()
        
    return all_data

if __name__ == "__main__":
    data = scrape_pointsbet()
    with open("pointsbet_data/pointsbet_scraped.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Scraping complete. Saved to pointsbet_data/pointsbet_scraped.json")
