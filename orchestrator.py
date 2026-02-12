"""
Orchestrator
============
Coordinates the entire scan process:
1. Health Check
2. Data Fetching (Manager) -- Parallel I/O
3. Swing Filtering (Gates) -- Vectorized CPU
4. Logging (Auditor) -- Compliance
5. Reporting
"""

import os
import sys
import time
from datetime import datetime
from loguru import logger
import pandas as pd

from config import CONFIG
from data_manager import DataManager
from swing_filter import SwingFilter
from log_auditor import LogAuditor
from health_check import HealthCheck
from services.chart_engine import ChartEngine
from ta.volatility import AverageTrueRange

class Orchestrator:
    def __init__(self):
        self.dm = DataManager()
        self.filter = SwingFilter()
        self.auditor = LogAuditor()
        self.health = HealthCheck()
        self.charts = ChartEngine()
        
    async def run_scan(self, full_universe: bool = False, progress_callback=None):
        """
        Executes the full scan workflow.
        :param full_universe: If True, scans the entire BSE/NSE mainboard.
        :param progress_callback: Optional function(message, progress_pct) for real-time reporting.
        """
        start_time = time.time()
        def report(msg, pct=None):
            logger.info(msg)
            if progress_callback:
                progress_callback(msg, pct)

        report("Engine Ignition: Starting Discovery Engine...", 5)
        
        # 1. Discovery Phase
        if full_universe:
            universe = await self.dm.get_full_universe()
        else:
            universe = self.dm.get_institutional_universe()
            
        tickers = list(universe.keys())
        total_count = len(tickers)
        report(f"Discovery Complete: {total_count} scrips detected.", 10)

        # 2. Data Fetching
        report(f"Phase 1: Multi-Core Data Fetch for {total_count} stocks...", 15)
        ticker_data = await self.dm.batch_fetch_ohlcv(tickers)
        report(f"Data Fetch Complete. Mapping Sectors...", 25)
        
        # Ensure we have sectors for Z-Score mapping in Gate 1
        ticker_sectors = await self.dm.batch_fetch_sector_map(list(ticker_data.keys()))
        # Merge fetched sectors with discovery metadata
        for t, sector in ticker_sectors.items():
            if t in universe:
                universe[t]['sector'] = sector

        report(f"Sector Mapping Complete. Analyzing {len(ticker_data)} stocks.", 30)

        # 3. Pipeline Execution (Integrated Funnel)
        report("Phase 2: Gate 1 (Liquidity & Spread) - Sifting...", 35)
        g1_passed, g1_rationale = self.filter.gate1.run(ticker_data, universe)
        report(f"Gate 1 Complete: {len(g1_passed)} survivors.", 45)
        
        if not g1_passed:
            return []

        report("Phase 3: Gate 2 (Fundamentals & Governance) - Analyzing...", 50)
        g2_passed, g2_rationale = self.filter.gate2.run(g1_passed)
        g2b_passed, g2b_rationale = self.filter.gate2b.run(g2_passed, universe)
        report(f"Gate 2 Complete: {len(g2b_passed)} survivors.", 70)

        if not g2b_passed:
            return []

        g3_data = {t: ticker_data[t] for t in g2b_passed}
        finds, g3_rationale = self.filter.gate3.run(g3_data)
        
        # --- Coiling Spring Tagging ---
        # Stocks that passed Gate 2B but failed Gate 3
        for ticker in g2b_passed:
            if ticker not in finds:
                if ticker in g3_rationale:
                    g3_rationale[ticker]["status"] = "COILING_SPRING"
                else:
                    g3_rationale[ticker] = {"passed": False, "status": "COILING_SPRING", "reason": "Technical filters not met"}
        
        # 4. Final Processing & Chart Generation
        report("Phase 5: Generating Visualization & Trade Metadata...", 90)
        self.charts.cleanup_old_charts() # Clean up 24h old charts
        
        results = []
        # Process ALL stocks that passed Fundamentals (Gate 2B)
        for ticker in g2b_passed:
            try:
                df = ticker_data[ticker]
                meta = universe.get(ticker, {})
                rat = g3_rationale.get(ticker, {})
                
                # Trade Metadata Calculation
                entry = df['Close'].iloc[-1]
                
                # ATR-based SL (14-day)
                atr = AverageTrueRange(high=df['High'], low=df['Low'], close=df['Close'], window=14).average_true_range().iloc[-1]
                sl = entry - (2 * atr)
                risk = entry - sl
                target = entry + (2 * risk)
                
                pattern = rat.get('pattern', '')
                period = "Swing (2-6 Weeks)" if 'VCP' in (pattern or '') else "Positional (1-3 Months)"
                
                trade_meta = {
                    "entry": round(float(entry), 2),
                    "sl": round(float(sl), 2),
                    "target": round(float(target), 2),
                    "period": period,
                    "risk_reward": "1:2"
                }
                
                # Generate Triple-Timeframe Charts
                chart_daily = self.charts.generate_trade_chart(ticker, df, trade_meta, pattern_info=pattern, timeframe='1D')
                chart_weekly = self.charts.generate_trade_chart(ticker, df, trade_meta, pattern_info=pattern, timeframe='1W')
                chart_monthly = self.charts.generate_trade_chart(ticker, df, trade_meta, pattern_info=pattern, timeframe='1M')
                
                results.append({
                    "ticker": ticker,
                    "status": "BUY" if ticker in finds else "COILING_SPRING",
                    "name": meta.get("name", "Unknown"),
                    "cap": meta.get("market_cap", "SMALL"),
                    "sector": meta.get("sector", "Diversified"),
                    "adx": rat.get("adx"),
                    "mrs": rat.get("mrs"),
                    "mrs_slope": rat.get("mrs_slope"),
                    "pattern": pattern,
                    "reason": rat.get("reason", "Consolidating"),
                    "trade": trade_meta,
                    "charts": {
                        "daily": chart_daily,
                        "weekly": chart_weekly,
                        "monthly": chart_monthly
                    }
                })
            except Exception as e:
                logger.error(f"Post-processing failed for {ticker}: {e}")

        # Snapshot and Duration
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_rationale = {**g1_rationale, **g2_rationale, **g2b_rationale, **g3_rationale}
        self.save_snapshot(results, all_rationale, timestamp)
        
        report(f"Scan Completed. {len(results)} Candidates identified.", 100)
        return results

        # 8. Reporting
        elapsed = time.time() - start_time
        universe_for_display = inst_metadata if inst_metadata else universe
        self._print_summary(candidates, universe_for_display, elapsed, coiling_springs, metrics=metrics)
        report(f"Scan Completed in {elapsed:.2f}s. {len(candidates)} candidates found.", 100)

    def save_snapshot(self, candidates, rationale, timestamp):
        """Saves scan results as a timestamped snapshot for comparison."""
        os.makedirs("data/snapshots", exist_ok=True)
        snapshot_file = f"data/snapshots/snapshot_{timestamp}.json"
        try:
            import json
            payload = {
                "timestamp": timestamp,
                "candidates": candidates,
                "rationale": rationale
            }
            with open(snapshot_file, "w") as f:
                json.dump(payload, f, indent=2)
            logger.info(f"Snapshot saved: {snapshot_file}")
        except Exception as e:
            logger.error(f"Failed to save snapshot: {e}")

    def _generate_watchlist_report(self, full_rationale):
        """Deprecated: Logic moved to SwingFilter and JSON export."""
        pass

    def _print_summary(self, candidates, universe_map, elapsed, coiling_springs=None, metrics=None):
        """Prints a clean summary to console with Consolidated Heatmap."""
        print("\n" + "="*80)
        print(" CONSOLIDATED HEATMAP - INSTITUTIONAL SCAN")
        print("="*80)
        
        if metrics:
            print(f"Total Stocks Scanned: {metrics.get('total_scanned', 0)}")
            print(f"Passed G1 (Liquidity): {metrics.get('passed_g1', 0)}")
            print(f"Passed G2 (Quality):   {metrics.get('passed_g2', 0)}")
            print(f"Passed G2B (Inst.):    {metrics.get('passed_g2b', 0)}")
            print(f"Total Candidates:      {metrics.get('passed_all', 0)}")
        else:
            print(f"SCAN COMPLETE in {elapsed:.2f}s")
            print(f"Candidates Found: {len(candidates)}")
        
        print("-" * 80)
        
        if candidates:
            print("TOP PICKS BY CATEGORY (Rationale IDs)")
            cap_groups = {"LARGE": [], "MID": [], "SMALL": []}
            for ticker in candidates:
                meta = universe_map.get(ticker, {}) if isinstance(universe_map.get(ticker), dict) else {}
                cap = meta.get("market_cap", "SMALL") # Default to small if unknown
                cap_groups[cap].append(ticker)
            
            for cap_type in ["LARGE", "MID", "SMALL"]:
                if cap_groups[cap_type]:
                    # Sort candidates by MRS or other metric if possible
                    top_picks = cap_groups[cap_type][:3] # Show top 3
                    for top_pick in top_picks:
                        rat_id = f"RAT-{top_pick.split('.')[0]}-{cap_type}-2026"
                        print(f"{cap_type:8} | Pick: {top_pick:12} | Rationale ID: {rat_id}")
            
            print("-" * 80)
        else:
            print("No candidates met all criteria.")
            
        if coiling_springs:
            # Filter for MID/SMALL caps only
            mid_small_springs = []
            for item in coiling_springs:
                cap = item.get('market_cap', 'UNKNOWN')
                if cap in ['MID', 'SMALL']:
                    mid_small_springs.append(item)
            
            if mid_small_springs:
                print("\n" + "-" * 80)
                print(f"COILING SPRINGS - MID/SMALL CAPS (Daily Watchlist): {len(mid_small_springs)}")
                print("-" * 80)
                
                display_wl = []
                for item in mid_small_springs:
                    display_wl.append({
                        "Ticker": item['ticker'],
                        "Cap": item.get('market_cap', 'UNK'),
                        "Sector": item['sector'][:15],
                        "Close": item['close'],
                        "Inst%": item.get('inst_ownership', 0),
                        "Reason": item['reason'][:30]
                    })
                df_wl = pd.DataFrame(display_wl)
                print(df_wl.to_string(index=False))
            
        print("="*80 + "\n")

if __name__ == "__main__":
    # Test run
    import os
    orch = Orchestrator()
    orch.run_scan(dry_run=True)
