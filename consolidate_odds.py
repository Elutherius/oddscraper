"""
Consolidate betting odds from DraftKings, Polymarket, FanDuel, PointsBet, and Kalshi into a single CSV.
"""

import sys
import pandas as pd
import glob
import os
import traceback
from datetime import datetime, timedelta, timezone
import re
from difflib import SequenceMatcher
from normalizer import normalize_event, normalize_team
from utils.odds_conversion import prob_to_moneyline, moneyline_to_prob

# --- Configuration ---
DRAFTKINGS_DIR = "draftkings_data"
POLYMARKET_PRICES_FILE = "data/prices/latest.csv"
POLYMARKET_MARKETS_DIR = "data/markets"
OUTPUT_FILE = "consolidated_odds.csv"
POLYMARKET_KEYWORD_MAP = [
    ("NBA", ["nba"]),
    ("WNBA", ["wnba"]),
    ("NFL", ["nfl", "super bowl"]),
    ("NHL", ["nhl"]),
    ("NCAAF", ["ncaaf", "college football", "cfb"]),
    ("NCAAB", ["ncaab", "college basketball", "march madness", "cbb", "cwbb", "wcbb", "ncaaw"]),
    ("MLB", ["mlb", "baseball"]),
    ("MLS", ["mls", "major league soccer"]),
    ("EPL", ["english premier league", "premier league", "epl"]),
    ("La Liga", ["la liga"]),
    ("Serie A", ["serie a"]),
    ("Bundesliga", ["bundesliga"]),
    ("Ligue 1", ["ligue 1"]),
    ("A-League", ["a-league", "a league", "aleague"]),
    ("EuroLeague", ["euroleague", "euro league", "euro basket"]),
    ("AHL", ["ahl", "american hockey league"]),
    ("KHL", ["khl", "kontinental hockey league"]),
    ("Champions League", ["champions league", "ucl"]),
    ("NWSL", ["nwsl", "national womens soccer league"]),
    ("WNIT", ["wnit"]),
    ("Tennis", ["tennis", "atp", "wta", "us open", "wimbledon", "french open", "australian open"]),
    ("MMA", ["ufc", "mma", "bellator", "pfl", "fighting"]),
    ("Boxing", ["boxing"]),
    ("Racing", ["f1", "formula 1", "nascar", "indycar", "motogp"]),
    ("Golf", ["golf", "pga", "masters"]),
    ("Cricket", ["cricket", "ipl"]),
    ("Rugby", ["rugby", "super rugby"]),
    ("Cycling", ["cycling", "tour de france"]),
    ("Esports", ["esports", "league of legends", "dota"]),
    ("Soccer", ["soccer", "uefa", "fa cup", "serie b", "championship"]),
]

def _simplify_event_string(event: str) -> str:
    if not isinstance(event, str):
        return ""
    simplified = re.sub(r'[^A-Z0-9 ]', ' ', event.upper())
    simplified = re.sub(r'\s+', ' ', simplified).strip()
    return simplified

def apply_fuzzy_event_alignment(df: pd.DataFrame, threshold: float = 0.965) -> pd.DataFrame:
    """
    Collapses near-identical event names (e.g., 'ST LOUIS vs DAYTON' vs
    'SAINT LOUIS vs DAYTON') within the same sport/day window so each matchup
    shares a canonical Event string.
    """
    if df.empty or "Event" not in df.columns:
        return df

    working = df.copy()
    date_key = working["Game_Date"].dt.strftime("%Y-%m-%d").fillna("UNKNOWN")
    working["_event_group_key"] = working["Sport"].astype(str) + "::" + date_key

    for _, idx in working.groupby("_event_group_key").groups.items():
        subset = working.loc[idx, "Event"]
        unique_events = [evt for evt in subset.dropna().unique()]
        if len(unique_events) <= 1:
            continue

        simplified_cache = {evt: _simplify_event_string(evt) for evt in unique_events}
        canonical = []
        mapping = {}

        for evt in unique_events:
            target = None
            for existing in canonical:
                ratio = SequenceMatcher(None, simplified_cache[evt], simplified_cache[existing]).ratio()
                if ratio >= threshold:
                    target = existing
                    break
            if target is None:
                canonical.append(evt)
                target = evt
            mapping[evt] = target

        working.loc[idx, "Event"] = subset.map(lambda evt: mapping.get(evt, evt))

    working = working.drop(columns=["_event_group_key"])
    return working

def get_latest_markets_file(markets_dir):
    """Finds the markets_YYYY-MM-DD.csv file with the latest date."""
    files = glob.glob(os.path.join(markets_dir, "markets_*.csv"))
    if not files:
        return None
    files.sort(reverse=True)
    return files[0]

def process_standard_csv(source_name, csv_pattern):
    """Generic processor for already standardized CSVs."""
    print(f"\n--- Processing {source_name} ---")
    files = glob.glob(csv_pattern)
    print(f"DEBUG: Glob pattern '{csv_pattern}' found {len(files)} files: {files}")
    if not files:
        print(f"No files found for {source_name}")
        return pd.DataFrame()
        
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # Ensure columns exist (new schema)
            required = ["Sport", "Game_Date", "Event", "BetType", "HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds", "Is_Live"]
            missing = [col for col in required if col not in df.columns]
            if missing and "BetType" in missing and "Market" in df.columns:
                df = df.rename(columns={"Market": "BetType"})
                missing = [col for col in required if col not in df.columns]
            if missing:
                print(f"Skipping {f}: Missing columns (Likely old format). Missing: {missing} | Found: {list(df.columns)}")
                continue
            
            # Ensure Url column exists (add empty if missing for backwards compatibility)
            if "Url" not in df.columns:
                df["Url"] = ""
                
            df["Source"] = source_name
            print(f"  Loaded {len(df)} rows from {f}")
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not dfs: return pd.DataFrame()
    final_df = pd.concat(dfs, ignore_index=True)
    print(f"Total rows for {source_name}: {len(final_df)}")
    return final_df

def process_polymarket():
    """Reads Polymarket markets and prices and standardizes them."""
    print("\n--- Processing Polymarket ---")
    market_files = sorted(glob.glob(os.path.join(POLYMARKET_MARKETS_DIR, "markets_*.csv")), reverse=True)[:3]
    if not market_files:
        print("No Polymarket markets file found.")
        return pd.DataFrame()
        
    try:
        dfs = []
        for mf in market_files:
            try:
                dfs.append(pd.read_csv(mf, dtype={'market_id': str}))
            except Exception as e:
                print(f"Error reading {mf}: {e}")
        
        if not dfs: return pd.DataFrame()
        
        df_markets = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["market_id"], keep="last")
        print(f"Loaded {len(df_markets)} unique markets.")

        if not os.path.exists(POLYMARKET_PRICES_FILE):
             print("No Polymarket prices file.")
             return pd.DataFrame()

        df_prices = pd.read_csv(POLYMARKET_PRICES_FILE, dtype={'market_id': str})

        merged = pd.merge(df_prices, df_markets, on="market_id", suffixes=('_price', '_market'))
        
        # We need to pivot outcomes to event rows
        events_map = {}
        
        sports_keywords = ["nba", "nfl", "nhl", "mlb", "soccer", "tennis", "ufc", "boxing", "f1", "golf"]
        fetch_time = datetime.now().isoformat()
        
        
        for _, row in merged.iterrows():
            def get_col(candidates):
                for c in candidates:
                    if c in row and pd.notna(row[c]):
                        return str(row[c])
                return ""

            category = get_col(["category", "category_market", "category_price"]).lower()
            slug = get_col(["slug_market", "slug_price", "slug"]).lower()
            question = get_col(["question_market", "question_price", "question"]).lower()
            
            # Filter
            # Improved Sport Mapping
            # Determine Sport Label
            text_to_check = f"{category} {slug} {question}"
            lower_text = text_to_check.lower()
            sport_label = "Sports" # Default
            for label, keywords in POLYMARKET_KEYWORD_MAP:
                if any(keyword in lower_text for keyword in keywords):
                    sport_label = label
                    break
            
            is_sport = sport_label != "Sports" or category == "sports"
            if not is_sport:
                continue
            
            # Normalization
            slug_clean = slug.replace("slug_market", "").replace("slug_price", "")
            event = normalize_event(slug_clean)
            if not event or event == "" or (" vs " not in event and " vs " not in slug_clean.replace("-", " ")):
                 e2 = normalize_event(question)
                 if e2: event = e2
            
            if not event or " vs " not in event: continue # Skip if we can't identify the matchup

            # Date
            # Try to extract from slug first (e.g. nba-atl-min-2026-02-08)
            slug_date = None
            if slug_clean:
                 # Look for YYYY-MM-DD
                 match = re.search(r'(\d{4}-\d{2}-\d{2})', slug_clean)
                 if match:
                     slug_date_str = match.group(1)
                     try:
                        # Slugs are UTC game day. If game is in evening US, slug is next day.
                        # We want the "Game Date" in local US time (DraftKings convention).
                        # Subtracting 6 hours from UTC midnight handles typical evening games.
                        dt_naive = datetime.fromisoformat(slug_date_str)
                        dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                        dt_adj = dt_utc - timedelta(hours=6)
                        slug_date = dt_adj.strftime("%Y-%m-%d")
                     except Exception as e:
                        # print(f"DEBUG: Date parse error {slug_date_str}: {e}")
                        slug_date = slug_date_str

            # Fallback to end_date_utc types if slug date not found
            game_date = str(slug_date if slug_date else row.get("end_date_utc", row.get("open_time", row.get("start_date", fetch_time))))
            
            # Price
            try:
                mid_price = float(row["mid"])
                moneyline = prob_to_moneyline(mid_price)
                if moneyline is None or abs(moneyline) > 10000: continue
            except: continue

            selection = row.get("outcome", "")
            
            # Store in map
            # Key = (Sport, Event, Date)
            # Key = (Sport, Event, Date)
            # sport_label is already determined above
            
            key = (sport_label, event, game_date)
            if key not in events_map:
                parts = event.split(" vs ")
                events_map[key] = {
                    "Sport": sport_label,
                    "Source": "Polymarket",
                    "Game_Date": game_date,
                    "Event": event,
                    "BetType": "Moneyline",
                    "HomeTeam": parts[1] if len(parts)>1 else "Unknown", 
                    "AwayTeam": parts[0] if len(parts)>1 else "Unknown",
                    "HomeOdds": None,
                    "AwayOdds": None,
                    "Is_Live": False,
                    "Fetched_At": fetch_time,
                    "Url": f"https://polymarket.com/event/{slug_clean}" if slug_clean else ""
                }
            
            entry = events_map[key]
            
            # Map Selection to Home/Away
            # Selection might be "Lakers" or "Celtics" or "Yes"
            norm_sel = normalize_team(selection)
            norm_home = normalize_team(entry["HomeTeam"])
            norm_away = normalize_team(entry["AwayTeam"])
            
            if not norm_home or not norm_away:
                print(f"Prop dropped: Bad teams {entry['HomeTeam']} / {entry['AwayTeam']}")
                continue
                
            if norm_sel == norm_home:
                entry["HomeOdds"] = moneyline
            elif norm_sel == norm_away:
                entry["AwayOdds"] = moneyline
            elif selection.lower() == "yes":
                # Assuming the question is "Will HomeTeam win?" or "Will AwayTeam win?"
                # This is heuristic and somewhat risky without parsing question structure
                # But typically Poly "Game" markets are "Team A vs Team B" and outcomes are "Team A", "Team B".
                pass
                
        # Flatten map
        rows = []
        for v in events_map.values():
            if v["HomeOdds"] and v["AwayOdds"]:
                rows.append(v)
                
        print(f"Polymarket: Extracted {len(rows)} matchups.")
        return pd.DataFrame(rows)

    except Exception as e:
        print(f"Error processing Polymarket data: {e}")
        return pd.DataFrame()

def main():
    print("Starting consolidation...")
    
    dfs = []
    
    # 1. DraftKings
    dfs.append(process_standard_csv("DraftKings", os.path.join(DRAFTKINGS_DIR, "dk_*_odds.csv")))
    
    # 2. FanDuel
    dfs.append(process_standard_csv("FanDuel", "fanduel_data/fd_moneyline_odds.csv"))
    
    # 3. PointsBet
    dfs.append(process_standard_csv("PointsBet", "pointsbet_data/pointsbet_odds.csv"))
    
    # 4. Kalshi
    dfs.append(process_standard_csv("Kalshi", "kalshi_data/kalshi_odds.csv"))
    
    # 5. Polymarket
    dfs.append(process_polymarket()) 
    
    combined = pd.concat(dfs, ignore_index=True)
    if combined.empty:
        print("No data collected.")
        return

    if "Vig" in combined.columns:
        combined = combined.drop(columns=["Vig"])

    # Global Filter: Upcoming Only
    print("\nApplying filters and normalization...")
    
    # Normalize Teams and Events to ensure matching across books
    combined["HomeTeam"] = combined["HomeTeam"].astype(str).apply(normalize_team)
    combined["AwayTeam"] = combined["AwayTeam"].astype(str).apply(normalize_team)
    
    # Re-normalize Event based on normalized teams to ensure "A vs B" consistency
    # (Some sources might have "B vs A" or different names)
    def re_normalize_event(row):
        t1 = row["HomeTeam"]
        t2 = row["AwayTeam"]
        # Sort to ensure canonical ordering (e.g. always alphabetical)
        # But wait, usually Home/Away implies venue. 
        # But for 'Event' matching, we want consistent string.
        # Let's use the verified normalize_event logic which sorts them.
        return normalize_event(f"{t1} vs {t2}")

    combined["Event"] = combined.apply(re_normalize_event, axis=1)

    print("DEBUG: Sample Dates BEFORE conversion:")
    for src in combined["Source"].unique():
        print(f"--- {src} ---")
        print(combined[combined["Source"]==src]["Game_Date"].head())
        print(combined[combined["Source"]==src]["Game_Date"].iloc[0])

    # combined["Game_Date"] = pd.to_datetime(combined["Game_Date"], errors='coerce')
    combined["Game_Date"] = pd.to_datetime(combined["Game_Date"], format='mixed', utc=True) # Mixed format for ISO/Date strings
    combined["Fetched_At"] = pd.to_datetime(combined["Fetched_At"], errors='coerce', utc=True)
    
    # Fuzzy match near-identical events on the same sport/day to ensure grouping.
    combined = apply_fuzzy_event_alignment(combined)
    
    now = pd.Timestamp.now(tz='UTC')
    print(f"DEBUG: Current Time (now): {now}")
    print(f"DEBUG: Current Time (now): {now}")
    
    # Debug Pre-Filter
    print("DEBUG: Sample Dates before filter:")
    print(combined[["Source", "Game_Date", "Is_Live"]].head())
    print(combined.groupby("Source")["Game_Date"].min())
    print(combined.groupby("Source")["Game_Date"].max())
    
    initial_len = len(combined)
    
    # Keep if Live OR Date > Now - buffer (e.g. 2 hours ago for finishing games)
    # Since we want strictly upcoming, maybe Date > Now - 15 mins?
    # User said: "only UPCOMING games".
    # But also "Date for each game... whether it's live or upcoming".
    # So we keep Live.
    
    mask_live = combined["Is_Live"].fillna(False)
    # Allow games from last 400 days to handle potential year mismatch (System 2026 vs Data 2025)
    cutoff = now - timedelta(days=400)
    mask_upcoming = combined["Game_Date"] >= cutoff.isoformat()
    
    print(f"DEBUG: Dropped breakdown:\n{combined[~mask_upcoming & ~mask_live]['Source'].value_counts()}")
    print(f"DEBUG: Valid breakdown:\n{combined[mask_upcoming | mask_live]['Source'].value_counts()}")
    
    combined = combined[mask_live | mask_upcoming]
    print(f"Filtered from {initial_len} to {len(combined)} rows (removed past games).")
    
    # Melt to Long Format (Selection level) for analysis
    # We want: Source, Sport, Game_Date, Event, Selection, Moneyline, Url
    
    # 1. Melt Home
    df_home = combined.copy()
    df_home["Selection"] = df_home["HomeTeam"]
    df_home["Moneyline"] = df_home["HomeOdds"]
    df_home = df_home.drop(columns=["HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds"])
    
    # 2. Melt Away
    df_away = combined.copy()
    df_away["Selection"] = df_away["AwayTeam"]
    df_away["Moneyline"] = df_away["AwayOdds"]
    df_away = df_away.drop(columns=["HomeTeam", "HomeOdds", "AwayTeam", "AwayOdds"])
    
    # Concatenate
    long_df = pd.concat([df_home, df_away], ignore_index=True)
    long_df = long_df.dropna(subset=["Moneyline"])
    
    # Sort
    long_df = long_df.sort_values(by=["Game_Date", "Sport", "Event", "Selection", "Source"])
    
    long_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSuccessfully wrote {len(long_df)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
