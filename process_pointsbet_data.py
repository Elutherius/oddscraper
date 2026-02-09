import json
import csv
import re
import os
from datetime import datetime, timedelta

def parse_pointsbet_json(input_path, output_dir):
    with open(input_path, 'r') as f:
        data = json.load(f)
        
    os.makedirs(output_dir, exist_ok=True)
    
    csv_rows = []
    
    # Aggregate by Event ID
    events_map = {}
    
    for sport, buttons in data.items():
        for btn in buttons:
            event_id = btn.get('event_id')
            if not event_id: continue
            
            if event_id not in events_map:
                # Parse Teams from Label once
                label = btn.get('label', '')
                teams_match = re.search(r'\((.*?) @ (.*?)\)', label)
                away_team = teams_match.group(1) if teams_match else ""
                home_team = teams_match.group(2) if teams_match else ""
                # Parse Date from date_content
                date_content = btn.get('date_content', '')
                fetched_at = btn.get('fetched_at', datetime.now().isoformat())
                game_date = fetched_at # Default
                
                # Try to parse
                # Example: "Sat Feb 7th ... 10:00am"
                try:
                    # print(f"DEBUG: Parsing date_content: '{date_content}'")
                    now = datetime.fromisoformat(fetched_at)
                    candidate = None
                    
                    # 1. Check relative days
                    if "Today" in date_content:
                        candidate = now
                    elif "Tomorrow" in date_content:
                        candidate = now + timedelta(days=1)
                    
                    # 2. Check explicit date (Strict Month)
                    if not candidate:
                        d_match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2})(?:st|nd|rd|th)?', date_content, re.I)
                        if d_match:
                            month_str = d_match.group(1)
                            day = int(d_match.group(2))
                            # Parse month
                            month = datetime.strptime(month_str[:3], "%b").month
                            year = now.year
                            # Basic year rollover logic
                            candidate = datetime(year, month, day)
                            if (candidate - now).days < -30:
                                candidate = candidate.replace(year=year+1)

                    # 3. Parse and Attach Time (if candidate found or checking regardless?)
                    # If we found a date candidate (or it's Today/Tomorrow), we look for time.
                    # If we didn't find a date, we default to extracted time on 'now' date? No, default to 'fetched_at'.
                    
                    # Time regex
                    t_match = re.search(r'(\d{1,2}):(\d{2})\s*(am|pm)', date_content, re.I)
                    if t_match:
                        hour = int(t_match.group(1))
                        minute = int(t_match.group(2))
                        ampm = t_match.group(3).lower()
                        
                        if ampm == 'pm' and hour != 12: hour += 12
                        if ampm == 'am' and hour == 12: hour = 0
                        
                        if candidate:
                            candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)
                            game_date = candidate.isoformat()
                        else:
                            # If only time found? Assume today?
                            # Example: "7:00pm" (implicit today)
                            # But we should be careful.
                            # PointsBet usually says "Today 7:00pm" or "Tomorrow".
                            # If just "Sun 7:00pm", we might miss "Sun".
                            # Let's check Day of Week?
                            # For now, if no date found, stick to default (fetched_at), but maybe update time?
                            # If we update time on 'fetched_at', we might be wrong if it's tomorrow.
                            pass 

                except Exception as e:
                    # print(f"Date parse error: {e}")
                    pass 

                events_map[event_id] = {
                    "Sport": sport,
                    "Game_Date": game_date,
                    "Event": f"{away_team} vs {home_team}" if away_team else "Unknown",
                    "BetType": "Moneyline",
                    "HomeTeam": home_team,
                    "HomeOdds": None,
                    "AwayTeam": away_team,
                    "AwayOdds": None,
                    "Is_Live": btn.get('is_live', False),
                    "Fetched_At": fetched_at,
                    "Url": btn.get('url', '')
                }
            
            # Process Moneyline Odds
            # We assume the scraped buttons are "Moneyline" based on the label often containing "Moneyline"
            # Adjust if spread/total buttons are mixed in (the scraper seemed to target oddsButton generically, but usually main markets)
            
            prop = btn.get('property', '')
            text = btn.get('text_content', '')
            
            # Extract American Odds
            american_odds = None
            match = re.search(r'([+-]\d+)$', text)
            if match:
                 american_odds = int(match.group(1))
            
            # Map to Home/Away
            entry = events_map[event_id]
            if prop == entry["HomeTeam"]:
                entry["HomeOdds"] = american_odds
            elif prop == entry["AwayTeam"]:
                 entry["AwayOdds"] = american_odds
    
    # Convert map to list
    final_rows = []
    for ev in events_map.values():
        if ev["HomeTeam"] and ev["AwayTeam"] and ev["HomeOdds"] and ev["AwayOdds"]:
            final_rows.append(ev)

    # Write to CSV
    output_path = os.path.join(output_dir, 'pointsbet_odds.csv')
    keys = ["Sport", "Game_Date", "Event", "BetType", "HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds", "Is_Live", "Fetched_At", "Url"]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(final_rows)
        
    print(f"Processed {len(final_rows)} games. Saved to {output_path}")

if __name__ == "__main__":
    parse_pointsbet_json("pointsbet_data/pointsbet_scraped.json", "pointsbet_data")
