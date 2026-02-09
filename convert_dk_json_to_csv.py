import json
import csv
import re
import os
from datetime import datetime, timedelta

def clean_text(text):
    if not text:
        return None
    return text.replace('\u2212', '-').replace('\u2013', '-').strip()

def parse_odds(text):
    c = clean_text(text)
    if not c:
        return None
    try:
        return int(c)
    except:
        return None

def parse_moneyline_value(text):
    """
    Attempts to coerce a DraftKings odds button string into an integer moneyline.
    Handles cases like 'EVEN' and filters out spread/total style strings that
    include decimals or O/U prefixes.
    """
    cleaned = clean_text(text)
    if not cleaned:
        return None

    upper = cleaned.upper().strip()
    # Normalize common aliases
    if upper == "EVEN":
        upper = "+100"
    elif upper in {"PK", "PICK"}:
        upper = "0"

    # Remove whitespace to guard against strings like "+ 120"
    upper = upper.replace(" ", "")

    # Exclude obvious spread/total formats
    if "." in upper:
        return None
    if upper.startswith(("O", "U")) and len(upper) > 1:
        return None

    if not re.fullmatch(r'[+\-]?\d+', upper):
        return None

    try:
        return int(upper)
    except ValueError:
        return None

def extract_moneyline_candidates(values, needed=2):
    """
    Scans a list of odds button texts and returns up to `needed` parsed
    moneyline integers in the order they appear.
    """
    results = []
    for raw in values:
        ml = parse_moneyline_value(raw)
        if ml is not None:
            results.append(ml)
            if len(results) >= needed:
                break
    return results

def parse_dk_json():
    target_dir = "draftkings_data"
    input_file = os.path.join(target_dir, 'dk_all_sports.json')
    
    if not os.path.exists(input_file):
        print("Json not found.")
        return

    with open(input_file, 'r') as f:
        all_data = json.load(f)
        
    for league, data in all_data.items():
        # Handle new format attempt or old format
        if "labels_found" not in data or "odds_found" not in data:
            if "rows" in data and not data["rows"]:
                 print(f"[{league}] Skipping empty structured data.")
                 continue
            # Try to grab rows if flat mode
            if data.get("mode") != "flat":
                 continue

        labels = data.get("labels_found", [])
        odds = data.get("odds_found", [])
        
        # Determine Stride (Density Heuristic)
        # Filter labels to just teams to count games
        team_count = 0
        date_headers = []
        
        # Provisional pass to identify teams
        # We need to filter out Headers and Dates
        
        def is_header(lbl):
            l = lbl.lower()
            if l in ["spread", "total", "moneyline"]: return True
            if "today" in l or "tomorrow" in l: return True
            # date regex
            if re.match(r'^\w{3}\s+\w{3}\s+\d+', l): return True
            return False
            
        filtered_teams = [l for l in labels if not is_header(l)]
        num_games = len(filtered_teams) // 2
        num_odds = len(odds)
        
        stride = 6 # Default (legacy fallback)
        if num_games > 0:
            ratio = num_odds / num_games
            # This ratio is noisy for the new structured scraping, but keep it
            # available for debugging legacy dumps.
        if "error" in data or "events" not in data:
            print(f"Skipping {league}: No valid data found.")
            continue

        events = data.get("events", [])
        fetch_time = datetime.now().isoformat()
        
        # Helper to check if text is a date
        def get_date_from_label(lbl):
            lbl = lbl.lower()
            now = datetime.now()
            if "today" in lbl:
                return now.date()
            if "tomorrow" in lbl:
                return (now + timedelta(days=1)).date()
            # Check for "Fri Sep 29" etc.
            try:
                # Remove "Starting" or prefix
                clean = re.sub(r'starting\s+', '', lbl).strip()
                # Try format "%a %b %d"
                # We need year. Assume current year, or next year logic.
                # Example: "Fri Sep 29" matches %a %b %d
                dt = datetime.strptime(clean, "%a %b %d")
                dt = dt.replace(year=now.year)
                if dt.date() < (now.date() - timedelta(days=30)):
                     dt = dt.replace(year=now.year + 1)
                return dt.date()
            except:
                pass
            return None

        def detect_column_order(label_list):
            """
            Looks at the leading labels for an event to determine which markets
            (Spread, Total, Moneyline) are present and in what order. This lets
            us dynamically set the stride when DraftKings omits Spread/Total.
            """
            header_keywords = ["Spread", "Total", "Moneyline"]
            order = []
            seen = set()
            for lbl in label_list:
                clean_lbl = clean_text(lbl)
                if not clean_lbl:
                    continue
                if get_date_from_label(clean_lbl):
                    # Skip date blocks; they're not part of the column header row.
                    continue
                if clean_lbl in header_keywords:
                    if clean_lbl not in seen:
                        order.append(clean_lbl)
                        seen.add(clean_lbl)
                    continue
                # Once we hit the first non-header label we're into team rows.
                if order:
                    break
            return order

        rows = []
        current_game_date = datetime.now().date().isoformat() # Default
        
        for event in events:
            event_url = event.get("url", "")
            labels = event.get("labels", [])
            odds = event.get("odds", [])
            column_order = detect_column_order(labels)
            if not column_order:
                column_order = ["Spread", "Total", "Moneyline"]
            num_columns = max(len(column_order), 1)
            moneyline_index = column_order.index("Moneyline") if "Moneyline" in column_order else None
            stride = num_columns * 2
            
            # State machine
            # We expect teams to come in pairs: Away, Home.
            # Headers appear in between.
            
            odds_idx = 0
            label_idx = 0
            pending_team = None
            
            while label_idx < len(labels):
                lbl = labels[label_idx]
                clean_lbl = clean_text(lbl)
                
                # Check for ignorable headers that are NOT dates
                if clean_lbl in ["Spread", "Total", "Moneyline"]:
                    label_idx += 1
                    continue
                    
                # Check for Date Header
                date_obj = get_date_from_label(clean_lbl)
                if date_obj:
                    current_game_date = date_obj.isoformat()
                    label_idx += 1
                    continue
                
                # Assume it's a team
                if pending_team is None:
                    pending_team = clean_lbl
                    label_idx += 1
                else:
                    # We have a pair: pending_team vs clean_lbl (Home)
                    away_team = pending_team
                    home_team = clean_lbl
                    
                    # Now we need to consume 6 odds (Spread, Total, ML for Away, then Home)
                    # Determine how many odds entries we expect based on detected columns.
                    expected_block = stride if stride > 0 else 2
                    block = odds[odds_idx:odds_idx + expected_block]
                    if len(block) < expected_block and expected_block <= len(odds):
                        # Not enough data in the slice; fall back to remaining odds.
                        block = odds[odds_idx:]

                    away_ml = home_ml = None
                    if moneyline_index is not None and len(block) >= (num_columns + moneyline_index + 1):
                        away_ml = parse_moneyline_value(block[moneyline_index])
                        home_ml = parse_moneyline_value(block[moneyline_index + num_columns])

                    if away_ml is None or home_ml is None:
                        # Fallback: scan whatever odds are available for moneyline-looking values.
                        ml_candidates = extract_moneyline_candidates(block)
                        if len(ml_candidates) < 2:
                            ml_candidates = extract_moneyline_candidates(odds)
                        if len(ml_candidates) >= 2:
                            away_ml, home_ml = ml_candidates[0], ml_candidates[1]

                    if away_ml is None or home_ml is None:
                        print(f"[{league}] Not enough moneyline data for {away_team} vs {home_team}")
                        odds_idx += expected_block
                        pending_team = None
                        label_idx += 1
                        continue

                    odds_idx += expected_block
                    
                    # Live Check
                    is_live = False
                    score_pattern = r"^(.*?)\s+\d+$"
                    
                    aw_match = re.match(score_pattern, away_team)
                    if aw_match:
                        away_team = aw_match.group(1)
                        is_live = True
                        
                    ho_match = re.match(score_pattern, home_team)
                    if ho_match:
                        home_team = ho_match.group(1)
                        is_live = True
                        
                    rows.append({
                        "Sport": league,
                        "Game_Date": current_game_date, # Only date part usually
                        "Event": f"{away_team} vs {home_team}",
                        "BetType": "Moneyline",
                        "HomeTeam": home_team,
                        "HomeOdds": home_ml,
                        "AwayTeam": away_team,
                        "AwayOdds": away_ml,
                        "Is_Live": is_live,
                        "Fetched_At": fetch_time,
                        "Url": event_url
                    })
                    pending_team = None
                    label_idx += 1
        
        if rows:
            output_file = os.path.join(target_dir, f'dk_{league.lower()}_odds.csv')
            headers = ["Sport", "Game_Date", "Event", "BetType", "HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds", "Is_Live", "Fetched_At", "Url"]
                       
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
                
            print(f"[{league}] Saved {len(rows)} games to {output_file}")
        else:
            print(f"[{league}] Parsed 0 games.")

if __name__ == "__main__":
    parse_dk_json()
