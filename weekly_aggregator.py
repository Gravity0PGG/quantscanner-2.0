"""
Weekly Watchlist Aggregator
===========================
Identifies stocks with sustained fundamental strength (G2 Pass) but technical consolidation (G3 Fail)
over the past week.
Criteria: Appeared in 'watchlist_daily_*.json' at least 3 times in the last 7 days.
"""

import json
import glob
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
import pandas as pd

LOG_DIR = Path("logs")
TODAY = datetime.now()

def aggregate_watchlists():
    logger.info("Starting Weekly Watchlist Aggregation...")
    
    # 1. Find relevant files (last 7 days)
    # Pattern: watchlist_daily_YYYYMMDD.json
    all_files = glob.glob(str(LOG_DIR / "watchlist_daily_*.json"))
    
    relevant_files = []
    cutoff_date = TODAY - timedelta(days=7)
    
    for f in all_files:
        try:
            # Extract date from filename
            date_str = Path(f).stem.split("_")[-1]
            file_date = datetime.strptime(date_str, "%Y%m%d")
            
            if file_date >= cutoff_date:
                relevant_files.append(f)
        except ValueError:
            continue
            
    if not relevant_files:
        logger.warning("No watchlist files found for the last 7 days.")
        return

    logger.info(f"Processing {len(relevant_files)} daily logs...")
    
    # 2. Aggregate Data
    ticker_counts = Counter()
    ticker_data = {} # Store latest data for each ticker
    
    for f_path in relevant_files:
        try:
            with open(f_path, "r") as f:
                data = json.load(f)
                
            for item in data:
                t = item['ticker']
                ticker_counts[t] += 1
                # Update with latest data (assuming files processed in order or random, latest is fine)
                ticker_data[t] = item
        except Exception as e:
            logger.error(f"Error reading {f_path}: {e}")

    # 3. Filter (Count >= 3)
    MIN_OCCURRENCES = 3
    coiling_springs = []
    
    for t, count in ticker_counts.items():
        if count >= MIN_OCCURRENCES:
            entry = ticker_data[t]
            entry['days_on_watchlist'] = count
            coiling_springs.append(entry)
            
    logger.success(f"Found {len(coiling_springs)} stocks with sustained consolidation (3+ days).")

    # 4. Save Weekly Digest
    digest_path = LOG_DIR / "watchlist_weekly_digest.json"
    try:
        with open(digest_path, "w") as f:
            json.dump(coiling_springs, f, indent=2)
        logger.info(f"Weekly Digest saved to {digest_path}")
    except Exception as e:
        logger.error(f"Failed to save digest: {e}")

    # 5. Console Summary
    if coiling_springs:
        print("\n" + "="*80)
        print(f"WEEKLY COILING SPRINGS (Aggregated 3+ Days)")
        print("-" * 80)
        
        display_data = []
        for item in coiling_springs:
            display_data.append({
                "Ticker": item['ticker'],
                "Days": item['days_on_watchlist'],
                "Sector": item['sector'][:20],
                "Latest Close": item['close'],
                "Reason": item['reason'][:30]
            })
        
        df = pd.DataFrame(display_data)
        # Sort by Days descending
        df = df.sort_values(by="Days", ascending=False)
        print(df.to_string(index=False))
        print("="*80 + "\n")
    else:
        print("\nNo stocks met the weekly aggregation criteria (yet).\n")

if __name__ == "__main__":
    aggregate_watchlists()
