import json
import os
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

# Set page config to wide mode for professional data view
st.set_page_config(page_title="Odds Dashboard", layout="wide", initial_sidebar_state="collapsed")

# Custom CSS for professional look
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.4rem;
    }
</style>
""", unsafe_allow_html=True)

st.title("Sportsbook Odds Dashboard")

# --- Data Loading ---
DATA_FILE = "consolidated_odds.csv"
ARCHIVE_DIR = Path("downloads")
HISTORY_DIR = ARCHIVE_DIR / "history"


def get_file_signature(file_path: str) -> float:
    try:
        return os.path.getmtime(file_path)
    except OSError:
        return 0.0

def calculate_implied_prob(odds):
    """Calculate implied probability from American odds."""
    if pd.isna(odds):
        return np.nan
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return -odds / (-odds + 100)

@st.cache_data(show_spinner=False)
def load_data(file_path: str, file_signature: float):
    if not os.path.exists(file_path):
        return None
    df = pd.read_csv(file_path)
    
    # Ensure correct types
    df["Moneyline"] = pd.to_numeric(df["Moneyline"], errors='coerce')
    df["Game_Date"] = pd.to_datetime(df["Game_Date"], errors='coerce')
    df["Fetched_At"] = pd.to_datetime(df["Fetched_At"], errors='coerce')
    
    # Calculate Implied Probability
    df["Implied_Prob"] = df["Moneyline"].apply(calculate_implied_prob)
    
    return df

def format_pull_label(timestamp_str: str, iso_str: Optional[str]) -> str:
    dt = None
    if iso_str:
        try:
            iso_norm = iso_str.replace("Z", "+00:00") if iso_str.endswith("Z") else iso_str
            dt = datetime.fromisoformat(iso_norm)
        except ValueError:
            dt = None
    if dt is None:
        for pattern in ("%Y%m%d_%H%M%S", "%Y-%m-%d_%H-%M-%S"):
            try:
                dt = datetime.strptime(timestamp_str, pattern)
                break
            except ValueError:
                continue
    if dt:
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    return timestamp_str

def load_history_runs(limit: Optional[int] = None):
    """Return archived pull rows and column order for the history table."""
    if not HISTORY_DIR.exists():
        return [], []
    
    run_dirs = sorted([p for p in HISTORY_DIR.iterdir() if p.is_dir()], reverse=True)
    if limit:
        run_dirs = run_dirs[:limit]
    
    rows = []
    for run_dir in run_dirs:
        metadata = {}
        metadata_path = run_dir / "metadata.json"
        if metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                metadata = {}
        
        timestamp = metadata.get("timestamp") or run_dir.name
        pull_label = format_pull_label(timestamp, metadata.get("pull_time_iso"))
        
        files = {}
        file_map = metadata.get("files", {})
        for display_name, relative_path in file_map.items():
            candidate = run_dir / relative_path
            if candidate.exists():
                files[display_name] = candidate
        
        fallback = run_dir / "consolidated_odds.csv"
        if "Consolidated" not in files and fallback.exists():
            files["Consolidated"] = fallback
        
        if not files:
            continue
        
        rows.append(
            {
                "timestamp": timestamp,
                "pull_label": pull_label,
                "files": files,
            }
        )
    
    if not rows:
        return [], []
    
    extra_sources = []
    for row in rows:
        for label in row["files"].keys():
            if label == "Consolidated":
                continue
            if label not in extra_sources:
                extra_sources.append(label)
    
    columns = ["Consolidated"] + extra_sources
    return rows, columns

def render_history_table():
    runs, columns = load_history_runs()
    if not runs:
        st.info("No archived runs found yet. Re-run the pipeline to create historical snapshots.")
        return
    
    st.write("Historical pulls are grouped by run. Download any snapshot directly from the table below.")
    
    header_cols = st.columns(len(columns) + 1)
    header_cols[0].markdown("**Pull Time**")
    for idx, label in enumerate(columns, start=1):
        header_cols[idx].markdown(f"**{label}**")
    
    for entry in runs:
        row_cols = st.columns(len(columns) + 1)
        row_cols[0].write(entry["pull_label"])
        for idx, label in enumerate(columns, start=1):
            file_path = entry["files"].get(label)
            if file_path and file_path.exists():
                try:
                    data = file_path.read_bytes()
                except OSError:
                    row_cols[idx].error("Missing file")
                    continue
                
                row_cols[idx].download_button(
                    label="Download",
                    data=data,
                    file_name=file_path.name,
                    mime="text/csv",
                    key=f"history-{entry['timestamp']}-{label}",
                )
            else:
                row_cols[idx].markdown("—")

df = load_data(DATA_FILE, get_file_signature(DATA_FILE))

if df is None:
    st.error(f"Data file `{DATA_FILE}` not found. Please run the scraping pipeline first.")
    st.stop()

LINK_BUTTON_SUPPORTED = hasattr(st, "link_button")
VERIFICATION_CSS_INJECTED = False

def ensure_verification_link_styles():
    """Inject CSS for verification links once per run."""
    global VERIFICATION_CSS_INJECTED
    if VERIFICATION_CSS_INJECTED:
        return
    st.markdown(
        """
        <style>
            div.stButton > button {
                width: 100%;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }
            a.verification-link-fallback {
                display: inline-flex;
                width: 100%;
                align-items: center;
                justify-content: center;
                padding: 0.5rem 0.75rem;
                border-radius: 0.5rem;
                border: 1px solid #1c6dd0;
                background-color: #1c6dd0;
                color: #fff !important;
                text-decoration: none;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                transition: background-color 0.15s ease;
            }
            a.verification-link-fallback:hover {
                background-color: #1557a0;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    VERIFICATION_CSS_INJECTED = True

def render_verification_link(label, url):
    """Render a verification link with graceful fallback for older Streamlit versions."""
    display_label = str(label).strip() if pd.notna(label) else "View Event"
    display_label = display_label or "View Event"

    if LINK_BUTTON_SUPPORTED:
        st.link_button(
            label=display_label,
            url=url,
            help=f"Go to {display_label}",
            use_container_width=True,
        )
    else:
        safe_label = escape(display_label)
        safe_url = escape(str(url), quote=True)
        st.markdown(
            f'<a class="verification-link-fallback" href="{safe_url}" target="_blank" rel="noopener noreferrer">{safe_label}</a>',
            unsafe_allow_html=True,
        )

# --- Analytics Section ---
st.header("📊 Market Analytics")

# Calculate Vig Statistics
try:
    # Calculate Vig per Event and Source
    grouped = df.groupby(["Sport", "Event", "Game_Date", "Source"])["Implied_Prob"]
    vig_df = grouped.sum().reset_index()
    counts = grouped.count().reset_index(name="Count")
    
    # Merge count back
    vig_df = pd.merge(vig_df, counts, on=["Sport", "Event", "Game_Date", "Source"])
    
    # Filter: Must have at least 2 outcomes to calculate valid vig
    vig_df = vig_df[vig_df["Count"] >= 2]
    
    vig_df["Vig_Pct"] = (vig_df["Implied_Prob"] - 1) * 100
    
    # Average Vig by Source
    avg_vig = vig_df.groupby("Source")["Vig_Pct"].mean().sort_values()
    
    # Key Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Markets", f"{len(df['Event'].unique()):,}")
    
    with col2:
        st.metric("Total Odds Entries", f"{len(df):,}")
    
    with col3:
        latest_fetch = df["Fetched_At"].max()
        if pd.notna(latest_fetch):
            st.metric("Last Updated", latest_fetch.strftime("%Y-%m-%d %H:%M"))
        else:
            st.metric("Last Updated", "N/A")
    
    with col4:
        # Best (lowest) average vig
        if not avg_vig.empty:
            best_book = avg_vig.index[0]
            st.metric("Best Vig", f"{best_book}: {avg_vig.iloc[0]:.2f}%")
        else:
            st.metric("Best Vig", "N/A")
    
    # Vig Comparison Chart
    st.subheader("Average Vig by Sportsbook")
    col_chart, col_table = st.columns([2, 1])
    
    with col_chart:
        st.bar_chart(avg_vig, height=300)
    
    with col_table:
        st.dataframe(
            avg_vig.to_frame(name="Avg Vig %").style.format("{:.2f}%").background_gradient(cmap="RdYlGn_r"),
            use_container_width=True
        )

except Exception as e:
    st.warning(f"Unable to calculate Vig statistics: {e}")


# --- Data Download Section ---
with st.expander("📥 Download Data CSVs"):
    st.write("Download the full consolidated dataset or individual sportsbook data.")
    
    # 1. Consolidated Data
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Full Consolidated Odds (CSV)",
        data=csv,
        file_name="consolidated_odds.csv",
        mime="text/csv",
        key="download-consolidated"
    )
    
    st.markdown("---")
    
    # 2. Individual Sportsbooks
    st.write("**Individual Sportsbooks:**")
    
    # Get unique scores and create columns for buttons
    current_sources = sorted([str(s) for s in df["Source"].unique() if pd.notna(s)])
    
    # Create rows of 3 columns each
    cols = st.columns(3)
    for i, source in enumerate(current_sources):
        col = cols[i % 3]
        with col:
            # Filter data for this source
            source_df = df[df["Source"] == source]
            source_csv = source_df.to_csv(index=False).encode('utf-8')
            
            st.download_button(
                label=f"Download {source}",
                data=source_csv,
                file_name=f"{source.lower().replace(' ', '_')}_odds.csv",
                mime="text/csv",
                key=f"download-{source}"
            )

if "show_history_table" not in st.session_state:
    st.session_state["show_history_table"] = False

if st.button("See Previous Data"):
    st.session_state["show_history_table"] = True

if st.session_state["show_history_table"]:
    render_history_table()

st.divider()


# --- Main Interface ---

# Get unique sources for tabs
sources = sorted([str(s) for s in df["Source"].unique() if pd.notna(s)])
tabs = st.tabs(["💰 Arbitrage Matrix"] + [f"🏛️ {s}" for s in sources])

# --- Tab 1: Arbitrage / Discrepancies ---
with tabs[0]:
    st.subheader("Market Discrepancies")
    
    # Create a row-based structure instead of pivot
    # Each row = Sport, Event, Game_Date, Selection, [Sportsbook Odds], Best_Odds, Total_Implied_Prob, Vig
    
    # Get unique sportsbooks
    sportsbooks = sorted([str(s) for s in df["Source"].unique() if pd.notna(s)])
    
    # Build the display dataframe row by row
    rows = []
    
    for (sport, event, game_date), event_df in df.groupby(["Sport", "Event", "Game_Date"]):
        for selection in event_df["Selection"].unique():
            if pd.isna(selection):
                continue
                
            selection_df = event_df[event_df["Selection"] == selection]
            
            row = {
                "Sport": sport,
                "Event": event,
                "Game_Date": game_date,
                "Selection": selection,
                "Is_Live": selection_df["Is_Live"].iloc[0] if len(selection_df) > 0 else False
            }
            
            # Add odds from each sportsbook
            odds_values = []
            for book in sportsbooks:
                book_odds = selection_df[selection_df["Source"] == book]["Moneyline"].values
                if len(book_odds) > 0:
                    row[book] = book_odds[0]
                    odds_values.append(book_odds[0])
                else:
                    row[book] = np.nan
            
            # Calculate statistics
            if len(odds_values) >= 2:  # Only include if at least 2 books have odds
                row["Best_Odds"] = max(odds_values)
                row["Worst_Odds"] = min(odds_values)
                row["Spread"] = max(odds_values) - min(odds_values)
                row["Books"] = len(odds_values)
                
                # Calculate average implied probability for this selection across all books
                implied_probs = [calculate_implied_prob(odds) for odds in odds_values]
                row["Avg_Implied_Prob"] = np.mean(implied_probs) * 100  # As percentage
                
                rows.append(row)
    
    # Create dataframe
    display_df = pd.DataFrame(rows)
    
    if len(display_df) > 0:
        # Calculate Vig per event (sum of implied probabilities for all selections in that event)
        vig_rows = []
        for (sport, event, game_date), event_group in display_df.groupby(["Sport", "Event", "Game_Date"]):
            # For each sportsbook, calculate vig if they have odds for multiple selections
            for book in sportsbooks:
                if book in display_df.columns:
                    book_selections = event_group[event_group[book].notna()]
                    if len(book_selections) >= 2:
                        # Calculate total implied probability
                        total_implied = sum([calculate_implied_prob(odds) for odds in book_selections[book]])
                        vig_pct = (total_implied - 1) * 100
                        
                        # Add to first selection row for this event
                        mask = (display_df["Sport"] == sport) & (display_df["Event"] == event) & (display_df["Game_Date"] == game_date)
                        first_idx = display_df[mask].index[0]
                        
                        if f"{book}_Vig" not in display_df.columns:
                            display_df[f"{book}_Vig"] = np.nan
                        display_df.loc[first_idx, f"{book}_Vig"] = vig_pct
        
        # Sort by Spread descending
        display_df = display_df.sort_values(by="Spread", ascending=False)
        
        # Reorder columns
        base_cols = ["Sport", "Event", "Game_Date", "Selection", "Is_Live"]
        stat_cols = ["Best_Odds", "Worst_Odds", "Spread", "Books", "Avg_Implied_Prob"]
        
        # Reorder: base info, sportsbook odds, stats
        display_df = display_df[base_cols + sportsbooks + stat_cols]
        
        # Format and display
        st.dataframe(
            display_df.style
            .format("{:.0f}", subset=[c for c in sportsbooks if c in display_df.columns], na_rep="—")
            .format("{:.0f}", subset=["Best_Odds", "Worst_Odds", "Spread"], na_rep="—")
            .format("{:.1f}%", subset=["Avg_Implied_Prob"], na_rep="—")
            .format({"Game_Date": lambda t: t.strftime("%m/%d %H:%M") if pd.notnull(t) else ""}) 
            .format({"Is_Live": lambda x: "🔴 LIVE" if x else "📅"})
            .background_gradient(subset=["Spread"], cmap="RdYlGn", vmin=0, vmax=100),
            use_container_width=True,
            height=800
        )
    else:
        st.warning("No data available with at least 2 sportsbooks.")

# --- Individual Sportsbook Tabs ---
for i, source in enumerate(sources):
    with tabs[i + 1]:
        st.subheader(f"{source} Odds")
        
        # Filter for this source
        source_df = df[df["Source"] == source].copy()
        
        if source_df.empty:
            st.warning("No data available for this source.")
        else:
            # --- Vig Analysis Section ---
            st.markdown("### 📊 Vig Analysis by Sport")
            
            # Calculate Vig Stats by Sport
            vig_stats = []
            
            for sport, sport_group in source_df.groupby("Sport"):
                # Group by event to calculate vig per event
                sport_vigs = []
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
                
                # Display Metrics and Chart
                c1, c2 = st.columns([1, 2])
                
                with c1:
                    overall_avg = source_df.groupby(["Event", "Game_Date"]).apply(
                        lambda x: (x["Implied_Prob"].sum() - 1) * 100 if len(x) >= 2 else np.nan
                    ).mean()
                    
                    st.metric("Overall Average Vig", f"{overall_avg:.2f}%")
                    
                    st.dataframe(
                        vig_stats_df.style.format({
                            "Avg Vig": "{:.2f}%",
                            "Min Vig": "{:.2f}%",
                            "Max Vig": "{:.2f}%"
                        }).background_gradient(subset=["Avg Vig"], cmap="RdYlGn_r"),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                with c2:
                    st.bar_chart(vig_stats_df.set_index("Sport")["Avg Vig"])
            
            # Links - Display actual URLs from the data
            st.write("### Verification Links")
            
            event_urls = source_df[["Event", "Url"]].drop_duplicates()
            
            if not event_urls.empty:
                valid_urls = event_urls[event_urls["Url"].notna() & (event_urls["Url"] != "")]
                valid_urls = valid_urls.sort_values("Event")
                
                if not valid_urls.empty:
                    ensure_verification_link_styles()
                    st.write(f"Found {len(valid_urls)} verification links:")
                    
                    max_columns = min(4, len(valid_urls))
                    if max_columns > 1:
                        slider_key = f"verification-cols-{''.join(ch.lower() if ch.isalnum() else '-' for ch in source)}"
                        columns_per_row = st.slider(
                            "Links per row",
                            min_value=1,
                            max_value=max_columns,
                            value=max_columns,
                            key=slider_key,
                            help="Reduce the number of columns if the grid feels cramped on smaller screens.",
                        )
                    else:
                        columns_per_row = 1
                    
                    for start in range(0, len(valid_urls), columns_per_row):
                        row_links = valid_urls.iloc[start:start + columns_per_row]
                        row_cols = st.columns(len(row_links))
                        for col, (_, link_row) in zip(row_cols, row_links.iterrows()):
                            with col:
                                render_verification_link(link_row["Event"], link_row["Url"])
                else:
                    st.info("No direct links available for events in this source.")
            else:
                st.info("No events found for this source.")
            
            # --- Detailed Odds Section ---
            st.subheader("Detailed Odds")

            # Display one row per selection (team/side)
            # Calculate vig per event
            
            # Group by event and calculate vig
            vig_by_event = {}
            for (sport, event, game_date), event_group in source_df.groupby(["Sport", "Event", "Game_Date"]):
                if len(event_group) >= 2:
                    total_implied = event_group["Implied_Prob"].sum()
                    vig_pct = (total_implied - 1) * 100
                    vig_by_event[(sport, event, game_date)] = vig_pct
            
            # Add vig column
            source_df["Vig"] = source_df.apply(
                lambda row: vig_by_event.get((row["Sport"], row["Event"], row["Game_Date"]), np.nan),
                axis=1
            )
            
            # Select and reorder columns
            display_cols = ["Sport", "Event", "Game_Date", "Selection", "Moneyline", "Implied_Prob", "Vig", "Url", "Fetched_At", "Is_Live"]
            source_display = source_df[display_cols].copy()
            
            # Sort by Game_Date
            source_display = source_display.sort_values(by="Game_Date")
            
            # Display
            st.dataframe(
                source_display.style
                .format("{:.0f}", subset=["Moneyline"], na_rep="—")
                .format("{:.2%}", subset=["Implied_Prob"], na_rep="—")
                .format("{:.2f}%", subset=["Vig"], na_rep="—")
                .format({"Game_Date": lambda t: t.strftime("%m/%d %H:%M") if pd.notnull(t) else ""})
                .format({"Fetched_At": lambda t: t.strftime("%m/%d %H:%M:%S") if pd.notnull(t) else ""})
                .format({"Is_Live": lambda x: "🔴 LIVE" if x else "📅"}),
                column_config={
                    "Url": st.column_config.LinkColumn("Link", display_text="Open")
                },
                use_container_width=True,
                height=800
            )

