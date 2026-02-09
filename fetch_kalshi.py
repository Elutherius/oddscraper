import csv
import os
import time
from datetime import datetime
from pm_universe.kalshi import KalshiClient
from utils.odds_conversion import prob_to_moneyline

OUTPUT_DIR = "kalshi_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "kalshi_odds.csv")

def fetch_kalshi_odds():
    print("Fetching Kalshi sports markets (Direct Series Fetch)...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    fetch_time = datetime.now().isoformat()
    events_map = {} # event_ticker -> { Info }

    with KalshiClient() as client:
        # Fetch NBA and NFL Game Markets
        series_to_fetch = ["KXNBAGAME", "KXNFLGAME"]
        
        for series in series_to_fetch:
            print(f"Fetching {series}...")
            try:
                # We use the internal http client to query specific series directly
                # to ensure we get the game markets we identified in debugging.
                # Pagination might be needed if there are many games.
                markets = []
                cursor = None
                while True:
                    client.rate_limiter.wait()
                    params = {"series_ticker": series, "limit": 100}
                    if cursor:
                        params["cursor"] = cursor
                        
                    resp = client.client.get("/markets", params=params)
                    resp.raise_for_status()
                    data = resp.json()
                    
                    batch = data.get("markets", [])
                    markets.extend(batch)
                    cursor = data.get("cursor")
                    
                    if not cursor:
                        break
                        
                print(f"  Found {len(markets)} markets for {series}")
                
                for m in markets:
                    # We expect markets like "Memphis at Golden State Winner?"
                    title = m.get("title", "")
                    yes_sub_title = m.get("yes_sub_title", "")
                    event_ticker = m.get("event_ticker")
                    
                    if not event_ticker or " at " not in title or "Winner" not in title:
                        continue
                        
                    # Parse Matchup
                    # Title: "TeamA at TeamB Winner?"
                    try:
                        clean_title = title.replace(" Winner?", "").strip()
                        parts = clean_title.split(" at ")
                        if len(parts) != 2:
                            continue
                            
                        away_team = parts[0].strip()
                        home_team = parts[1].strip()
                        
                        # Initialize event entry
                        if event_ticker not in events_map:
                            # Date
                            game_date = m.get("open_time", m.get("commence_time", ""))
                            if not game_date: game_date = m.get("expiration_time", "")

                            events_map[event_ticker] = {
                                "Sport": "NBA" if "NBA" in series else "NFL",
                                "Game_Date": game_date,
                                "Event": f"{away_team} vs {home_team}", # Standardize as Away vs Home
                                "BetType": "Moneyline",
                                "HomeTeam": home_team,
                                "AwayTeam": away_team,
                                "HomeOdds": None,
                                "AwayOdds": None,
                                "Is_Live": False, # Kalshi markets are usually pre-match futures style
                                "Fetched_At": fetch_time,
                                "Url": f"https://kalshi.com/markets/{event_ticker}" if event_ticker else ""
                            }
                        
                        entry = events_map[event_ticker]
                        
                        # Get Price (Yes Ask = Cost to buy Yes)
                        # Price is in cents.
                        yes_ask = m.get("yes_ask")
                        if yes_ask is None: continue
                        
                        prob = float(yes_ask) / 100.0
                        if prob <= 0 or prob >= 1:
                            moneyline = None
                        else:
                            moneyline = prob_to_moneyline(prob)
                            
                        # Assign to Home or Away
                        # yes_sub_title should match one of the teams
                        if yes_sub_title == home_team:
                            entry["HomeOdds"] = moneyline
                        elif yes_sub_title == away_team:
                            entry["AwayOdds"] = moneyline
                            
                    except Exception as e:
                        print(f"Error parsing market {m.get('ticker')}: {e}")
                        continue

            except Exception as e:
                print(f"Error fetching {series}: {e}")

    # Convert map to list
    rows = []
    for evt in events_map.values():
        if evt["HomeOdds"] is not None and evt["AwayOdds"] is not None:
            rows.append(evt)

    if rows:
        keys = ["Sport", "Game_Date", "Event", "BetType", "HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds", "Is_Live", "Fetched_At", "Url"]
        # Add BetType field constant (legacy compatibility)
        for r in rows:
            r["BetType"] = r.get("BetType", "Moneyline")
            
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Saved {len(rows)} Kalshi games to {OUTPUT_FILE}")
        return True
    else:
        print("No complete Kalshi game markets found.")
        return False

if __name__ == "__fetch_kalshi_odds__": # Fix for direct run? No, typically __main__
    fetch_kalshi_odds()

if __name__ == "__main__":
    fetch_kalshi_odds()
