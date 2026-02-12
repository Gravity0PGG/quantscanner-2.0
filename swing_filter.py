"""
Swing Filter Orchestrator
=========================
Runs the 4-gate pipeline sequentially.
Stocks must pass all gates to be selected.
Order optimized for performance:
1. Gate 1 (Spread) - Fast, Technical
2. Gate 3 (Technicals) - Fast, Technical
3. Gate 4 (Execution) - Fast, Technical/Volume
4. Gate 2 (Fundamentals) - Slow, Financials
"""

import pandas as pd
from loguru import logger
from gates.gate1_spread import Gate1Spread
from gates.gate2_fundamentals import Gate2Fundamentals
from gates.gate2_institutional import Gate2Institutional
from gates.gate3_technicals import Gate3Technicals
from gates.gate4_execution import Gate4Execution

class SwingFilter:
    def __init__(self):
        self.gate1 = Gate1Spread()
        self.gate2 = Gate2Fundamentals()
        self.gate2b = Gate2Institutional()
        self.gate3 = Gate3Technicals()
        self.gate4 = Gate4Execution()

    def run_pipeline(self, ticker_data: dict[str, pd.DataFrame], sector_map: dict[str, str] = None) -> tuple[list[str], dict]:
        """
        Executes the full Swing Trading Filter pipeline.
        Args:
            ticker_data: {ticker: OHLCV DataFrame}
            sector_map: {ticker: Sector Name}
        Returns:
            candidates: List of tickers that passed all gates.
            full_rationale: {ticker: {gate_name: result_dict}}
        """
        full_rationale = {}
        initial_count = len(ticker_data)
        logger.info(f"Starting Swing Filter Pipeline with {initial_count} stocks.")

        # --- Gate 1: Spread ---
        passed_g1, rationale_g1 = self.gate1.run(ticker_data, sector_map)
        self._log_gate_results(full_rationale, "Gate1_Spread", rationale_g1)
        logger.info(f"Gate 1 Survivors: {len(passed_g1)}")
        
        if not passed_g1:
            return [], full_rationale, []

        # Filter data for next gate
        data_g1 = {t: ticker_data[t] for t in passed_g1}

        # --- Gate 2: Fundamentals (Operator Shield & Quality) ---
        # Moved to Step 2 to capture "Fundamentally Strong but Technically Weak"
        passed_g2, rationale_g2 = self.gate2.run(passed_g1)
        self._log_gate_results(full_rationale, "Gate2_Fundamentals", rationale_g2)
        logger.info(f"Gate 2 Survivors (Fundamentally Strong): {len(passed_g2)}")
        
        if not passed_g2:
            return [], full_rationale, []

        # --- Gate 2B: Institutional Ownership & Float ---
        # Requires market cap metadata from institutional universe
        # Extract market cap map from sector_map if available
        market_cap_map = {}
        if sector_map and isinstance(list(sector_map.values())[0] if sector_map else None, dict):
            # Institutional universe format: {ticker: {"name": ..., "market_cap": ...}}
            for ticker, meta in sector_map.items():
                if isinstance(meta, dict):
                    market_cap_map[ticker] = meta.get("market_cap", "SMALL")
        else:
            # Legacy format or no metadata, default all to SMALL (strictest)
            market_cap_map = {t: "SMALL" for t in passed_g2}
        
        passed_g2b, rationale_g2b = self.gate2b.run(passed_g2, market_cap_map)
        self._log_gate_results(full_rationale, "Gate2B_Institutional", rationale_g2b)
        logger.info(f"Gate 2B Survivors (Institutional Backing): {len(passed_g2b)}")
        
        if not passed_g2b:
            return [], full_rationale, []

        # --- Gate 3: Technicals (Sniper Setup) ---
        # Run on G2B survivors (passed institutional checks)
        data_g2b = {t: ticker_data[t] for t in passed_g2b}
        passed_g3, rationale_g3 = self.gate3.run(data_g2b)
        self._log_gate_results(full_rationale, "Gate3_Technicals", rationale_g3)
        logger.info(f"Gate 3 Survivors (Technical Breakout): {len(passed_g3)}")
        
        # Capture Watchlist (Coiling Springs: Passed G2B, Failed G3 due to ADX/Slope)
        coiling_springs = []
        for ticker in passed_g2b:
            if ticker not in passed_g3:
                # Get G3 failure reason
                g3_res = rationale_g3.get(ticker, {})
                reason = g3_res.get("reason", "")
                
                # Check for specific "Sniper" technical failures
                # Reasons from Gate3: "ADX ... < ...", "RS Slope ... <= ...", "RS Negative"
                # User wants: "specifically due to ADX < 25 or Mansfield Slope < 0.05"
                # We check substrings.
                is_sniper_fail = "ADX" in reason or "RS Slope" in reason or "RS Negative" in reason
                
                if is_sniper_fail:
                    # Enrich with data
                    g2_res = rationale_g2.get(ticker, {})
                    g2b_res = rationale_g2b.get(ticker, {})
                    g1_res = rationale_g1.get(ticker, {})
                    
                    # Get market cap from market_cap_map
                    cap_category = market_cap_map.get(ticker, "UNKNOWN")
                    
                    coiling_springs.append({
                        "ticker": ticker,
                        "close": g3_res.get("close", 0),
                        "sector": g1_res.get("sector", "Unknown"),
                        "reason": reason,
                        "f_score": g2_res.get("f_score", 0),
                        "mrs": g3_res.get("mrs", 0),
                        "market_cap": cap_category,
                        "inst_ownership": g2b_res.get("inst_ownership", 0)
                    })
                    
                    # Tag in rationale for Audit (as requested)
                    if ticker in full_rationale:
                        full_rationale[ticker]["status"] = "COILING_SPRING"

        if not passed_g3:
            return [], full_rationale, coiling_springs

        # --- Gate 4: Execution (Entry Trigger) ---
        data_g3 = {t: ticker_data[t] for t in passed_g3}
        passed_g4, rationale_g4 = self.gate4.run(data_g3)
        self._log_gate_results(full_rationale, "Gate4_Execution", rationale_g4)
        logger.info(f"Gate 4 Survivors (Final Candidates): {len(passed_g4)}")

        return passed_g4, full_rationale, coiling_springs

    def _log_gate_results(self, main_log, gate_name, gate_results):
        """Helper to merge gate results into main rationale dict."""
        for ticker, result in gate_results.items():
            if ticker not in main_log:
                main_log[ticker] = {}
            main_log[ticker][gate_name] = result

if __name__ == "__main__":
    # Test logic
    # Mock data
    pass
