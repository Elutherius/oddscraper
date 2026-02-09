
import pandas as pd
import numpy as np

DATA_FILE = "consolidated_odds.csv"

def calculate_implied_prob(odds):
    """Calculate implied probability from American odds."""
    if pd.isna(odds):
        return np.nan
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return -odds / (-odds + 100)

def main():
    print(f"Loading data from {DATA_FILE}...")
    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print("Data file not found.")
        return

    # Ensure correct types
    df["Moneyline"] = pd.to_numeric(df["Moneyline"], errors='coerce')
    df["Game_Date"] = pd.to_datetime(df["Game_Date"], errors='coerce')
    
    # Calculate Implied Probability
    df["Implied_Prob"] = df["Moneyline"].apply(calculate_implied_prob)

    sources = sorted([str(s) for s in df["Source"].unique() if pd.notna(s)])
    
    for source in sources:
        print(f"\n--- {source} ---")
        source_df = df[df["Source"] == source]
        
        vig_stats = []
        
        for sport, sport_group in source_df.groupby("Sport"):
            sport_vigs = []
            # Using same grouping as app.py
            for _, event_group in sport_group.groupby(["Event", "Game_Date"]):
                if len(event_group) >= 2:
                    total_implied = event_group["Implied_Prob"].sum()
                    vig_pct = (total_implied - 1) * 100
                    sport_vigs.append(vig_pct)
            
            if sport_vigs:
                avg_vig = np.mean(sport_vigs)
                min_vig = np.min(sport_vigs)
                max_vig = np.max(sport_vigs)
                count = len(sport_vigs)
                
                vig_stats.append({
                    "Sport": sport,
                    "Avg Vig": avg_vig,
                    "Min Vig": min_vig,
                    "Max Vig": max_vig,
                    "Markets": count
                })
        
        if vig_stats:
            vig_stats_df = pd.DataFrame(vig_stats).sort_values("Avg Vig")
            print(vig_stats_df.to_string(index=False, float_format="{:.2f}".format))
        else:
            print("No valid vig data found (need >= 2 outcomes per event).")

if __name__ == "__main__":
    main()
