"""
QuantScanner 2026 — Data Manager
================================
Handles fetching and caching of BSE/NSE tickers and OHLCV data.
Uses diskcache for persistence to minimize API calls.
"""

from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from bsedata.bse import BSE
from nsetools import Nse
from diskcache import Cache
from loguru import logger
import multiprocessing
import warnings
import os
from pathlib import Path

# Suppress yfinance warnings about "no price data" for delisted stocks
warnings.filterwarnings("ignore", category=FutureWarning)

from config import CONFIG, CACHE_DIR

# Local indices cache directory
INDICES_DIR = Path(__file__).parent / "data" / "indices"
INDICES_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------
#  Standalone Worker Function (Windows Pickling Safe)
# ------------------------------------------------------------------
def fetch_worker(args):
    """
    Standalone worker function to fetch OHLCV for a single ticker.
    Args:
        args (tuple): (ticker, period)
    Returns:
        tuple: (ticker, DataFrame, error_msg)
    """
    ticker, period = args
    try:
        # Re-initialize cache inside worker process to avoid pickling connection
        # cache is thread-safe and process-safe via file locking
        worker_cache = Cache(directory=str(CACHE_DIR), size_limit=int(CONFIG.cache.CACHE_SIZE_LIMIT_GB * 1e9))
        
        cache_key = f"ohlcv_{ticker}_{period}"
        cached_df = worker_cache.get(cache_key)
        
        if cached_df is not None:
            return (ticker, cached_df, None)

        # Fetch via yfinance
        df = yf.download(
            ticker, 
            period=period, 
            progress=False, 
            auto_adjust=True,
            multi_level_index=False
        )
        
        if df.empty:
            return (ticker, pd.DataFrame(), "Empty DataFrame returned")

        # Column standardization
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df.columns for col in required_cols):
            if isinstance(df.columns, pd.MultiIndex):
                try:
                    df.columns = df.columns.get_level_values(0)
                except Exception:
                    pass
            
            # Check again
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                return (ticker, pd.DataFrame(), f"Missing columns: {missing}")

        # Cache the result
        worker_cache.set(cache_key, df, expire=CONFIG.cache.OHLCV_TTL)
        return (ticker, df, None)

    except Exception as e:
        return (ticker, pd.DataFrame(), str(e))


# ------------------------------------------------------------------
#  Sector Fetch Worker
# ------------------------------------------------------------------
def fetch_sector_worker(args):
    """
    Fetches Sector for a ticker.
    Args: (ticker)
    Returns: (ticker, sector_name)
    """
    ticker = args
    try:
        info_cache = Cache(directory=str(CACHE_DIR), size_limit=int(CONFIG.cache.CACHE_SIZE_LIMIT_GB * 1e9))
        cache_key = f"sector_{ticker}"
        cached_sector = info_cache.get(cache_key)
        
        if cached_sector:
            return (ticker, cached_sector)
            
        t = yf.Ticker(ticker)
        sector = t.info.get('sector', 'Unknown')
        
        # Cache it for a long time (e.g. 30 days)
        info_cache.set(cache_key, sector, expire=86400 * 30)
        return (ticker, sector)
    except Exception:
        return (ticker, "Unknown")

class DataManager:
    def __init__(self):
        self.cache = Cache(directory=str(CACHE_DIR), size_limit=int(CONFIG.cache.CACHE_SIZE_LIMIT_GB * 1e9))
        self.bse = BSE()
        self.nse = Nse()
        self.hardware = CONFIG.hardware
        self.user_agents = [
            # Chrome 130+ (2026 standard)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
        ]

    # ------------------------------------------------------------------
    #  NSE Index Constituent Fetching (Institutional Universe)
    # ------------------------------------------------------------------
    def _fetch_nifty_index_constituents(self, index_name: str) -> dict:
        """
        Fetches constituent list for a specific Nifty index from NSE.
        Args:
            index_name: "NIFTY 50", "NIFTY NEXT 50", "NIFTY MIDCAP 150", "NIFTY SMALLCAP 250"
        Returns:
            {ticker: {"name": name, "market_cap": cap_category}}
        """
        try:
            import requests
            import io
            
            # NSE Index Data URLs (as of 2026)
            # These are the official CSV download endpoints
            url_map = {
                "NIFTY 50": "https://www.nse india.com/api/equity-stockIndices?index=NIFTY%2050",
                "NIFTY NEXT 50": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20NEXT%2050",
                "NIFTY MIDCAP 150": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MIDCAP%20150",
                "NIFTY SMALLCAP 250": "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20SMLCAP%20250"
            }
            
            # Market cap categorization
            cap_map = {
                "NIFTY 50": "LARGE",
                "NIFTY NEXT 50": "LARGE",
                "NIFTY MIDCAP 150": "MID",
                "NIFTY SMALLCAP 250": "SMALL"
            }
            
            url = url_map.get(index_name)
            if not url:
                logger.error(f"Unknown index: {index_name}")
                return {}
            
            logger.info(f"Fetching {index_name} constituents from NSE...")
            
            # NSE requires session cookies and headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.nseindia.com/"
            }
            
            s = requests.Session()
            s.headers.update(headers)
            
            # Visit homepage first to establish cookies
            try:
                s.get("https://www.nseindia.com", timeout=10)
            except Exception:
                pass
            
            # Fetch index data
            response = s.get(url, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            # Parse response (structure varies by index)
            # Typically: {"data": [{"symbol": "TCS", "meta": {...}}]}
            constituents = {}
            
            if "data" in data:
                for item in data["data"]:
                    symbol = item.get("symbol")
                    company_name = item.get("meta", {}).get("companyName", symbol)
                    
                    if symbol:
                        ticker = f"{symbol}.NS"
                        constituents[ticker] = {
                            "name": company_name,
                            "market_cap": cap_map[index_name]
                        }
            
            logger.success(f"Fetched {len(constituents)} stocks from {index_name}")
            return constituents
            
        except Exception as e:
            logger.error(f"Failed to fetch {index_name} constituents: {e}")
            return {}

    def get_institutional_universe(self) -> dict:
        """
        Creates a deduplicated 'Institutional Universe' of ~500 stocks from:
        - Nifty 50
        - Nifty Next 50
        - Nifty Midcap 150
        - Nifty Smallcap 250
        
        Returns:
            {ticker: {"name": name, "market_cap": "LARGE|MID|SMALL"}}
        """
        cache_key = "institutional_universe"
        cached_universe = self.cache.get(cache_key)
        
        if cached_universe:
            logger.info(f"Loaded Institutional Universe ({len(cached_universe)} stocks) from cache.")
            return cached_universe
        
        logger.info("Building Institutional Universe from Nifty indices...")
        
        # Fetch all indices
        nifty_50 = self._fetch_nifty_index_constituents("NIFTY 50")
        nifty_next_50 = self._fetch_nifty_index_constituents("NIFTY NEXT 50")
        midcap_150 = self._fetch_nifty_index_constituents("NIFTY MIDCAP 150")
        smallcap_250 = self._fetch_nifty_index_constituents("NIFTY SMALLCAP 250")
        
        # Merge with priority (larger cap overwrites smaller cap if duplicate)
        # Priority: LARGE > MID > SMALL
        universe = {}
        
        # Start with SMALL, then MID, then LARGE (reverse priority for overwrite)
        for idx_data in [smallcap_250, midcap_150, nifty_next_50, nifty_50]:
            for ticker, meta in idx_data.items():
                universe[ticker] = meta
        
        # Cache for 24 hours
        self.cache.set(cache_key, universe, expire=CONFIG.cache.TICKER_LIST_TTL)
        
        logger.success(f"Institutional Universe created: {len(universe)} stocks")
        logger.info(f"  LARGE: {sum(1 for v in universe.values() if v['market_cap'] == 'LARGE')}")
        logger.info(f"  MID: {sum(1 for v in universe.values() if v['market_cap'] == 'MID')}")
        logger.info(f"  SMALL: {sum(1 for v in universe.values() if v['market_cap'] == 'SMALL')}")
        
        return universe

    # ------------------------------------------------------------------
    #  Legacy Methods (BSE/NSE Full Universe - Retained for Compatibility)
    # ------------------------------------------------------------------
    def _fetch_bse_tickers_from_source(self) -> dict:
        """Fetches active BSE equity scrip codes via direct BSE API."""
        try:
            logger.info("Fetching BSE tickers via direct API...")
            url = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&Industry=&Segment=Equity&Status=Active"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://www.bseindia.com/",
                "Origin": "https://www.bseindia.com"
            }
            import requests
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            tickers = {}
            for item in data:
                code = item.get('scrip_cd')
                name = item.get('scrip_name')
                if code and name:
                    ticker = f"{code}.BO"
                    tickers[ticker] = name.strip()
            
            if not tickers:
                logger.warning("BSE API returned empty list. Trying fallback CSV...")
                # Fallback: Equity.csv (sometimes works)
                url_csv = "https://www.bseindia.com/downloads/Mkt_Service/Equity/Equity.csv"
                response = requests.get(url_csv, headers=headers, timeout=15)
                # Parse CSV logic if needed, but API usually works.
                # If both fail, return empty.
            
            logger.success(f"Fetched {len(tickers)} BSE tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Failed to fetch BSE tickers: {e}")
            return {}

    def _fetch_nse_tickers_from_source(self) -> dict:
        """Fetches active NSE stock codes via direct CSV download."""
        try:
            logger.info("Fetching NSE tickers via direct CSV...")
            url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            import requests
            import io
            
            # NSE sometimes blocks without cookies. A session helps.
            s = requests.Session()
            s.headers.update(headers)
            
            # Visit homepage first to set cookies
            try:
                s.get("https://www.nseindia.com", timeout=10)
            except Exception:
                pass # Proceed anyway
            
            response = s.get(url, timeout=15)
            response.raise_for_status()
            
            # Parse CSV
            df = pd.read_csv(io.StringIO(response.text))
            
            tickers = {}
            if 'SYMBOL' in df.columns:
                for _, row in df.iterrows():
                    symbol = row['SYMBOL']
                    name = row.get('NAME OF COMPANY', symbol) # Col name varies
                    ticker = f"{symbol}.NS"
                    tickers[ticker] = name
            
            logger.success(f"Fetched {len(tickers)} NSE tickers.")
            return tickers
        except Exception as e:
            logger.error(f"Failed to fetch NSE tickers: {e}")
            return {}

    def get_universe(self) -> dict:
        """
        Returns a unified dictionary of {ticker: name} for BSE+NSE.
        Cached for 24 hours.
        """
        cache_key = "universe_tickers"
        cached_universe = self.cache.get(cache_key)

        if cached_universe:
            logger.info(f"Loaded {len(cached_universe)} tickers from cache.")
            return cached_universe

        bse_tickers = self._fetch_bse_tickers_from_source()
        nse_tickers = self._fetch_nse_tickers_from_source()

        universe = {**bse_tickers, **nse_tickers}
        
        self.cache.set(cache_key, universe, expire=CONFIG.cache.TICKER_LIST_TTL)
        logger.info(f"Cached universe of {len(universe)} tickers for 24h.")
        return universe

    def batch_fetch_ohlcv(self, tickers: list[str], period: str = "1y") -> dict[str, pd.DataFrame]:
        """
        Fetches OHLCV data for a list of tickers in parallel.
        Returns a dict of {ticker: DataFrame}.
        """
        logger.info(f"Fetching OHLCV for {len(tickers)} tickers using {self.hardware.CPU_CORES} workers...")
        
        results = {}
        args_list = [(t, period) for t in tickers]
        
        # Use simple map
        # Windows requires careful pickling. Top-level `fetch_worker` is safe.
        with multiprocessing.Pool(processes=self.hardware.CPU_CORES) as pool:
            # Map returns list of (ticker, df, error) in order
            for i, (ticker, df, error) in enumerate(pool.imap_unordered(fetch_worker, args_list)):
                if not df.empty:
                    results[ticker] = df
                elif error:
                    # Log silently or debug level to avoid spam
                    # logger.debug(f"Failed {ticker}: {error}")
                    pass
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Processed {i + 1}/{len(tickers)} tickers...")
        
        logger.success(f"Successfully fetched/loaded data for {len(results)}/{len(tickers)} stocks.")
        return results

    def batch_fetch_sector_map(self, tickers: list[str]) -> dict[str, str]:
        """
        Fetches sector information for a list of tickers in parallel.
        Returns: {ticker: sector_name}
        """
        logger.info(f"Fetching Sector Map for {len(tickers)} tickers...")
        
        sector_map = {}
        with multiprocessing.Pool(processes=self.hardware.CPU_CORES) as pool:
            for ticker, sector in pool.imap_unordered(fetch_sector_worker, tickers):
                sector_map[ticker] = sector
                
        logger.success(f"Fetched sectors for {len(sector_map)} stocks.")
        return sector_map




if __name__ == "__main__":
    multiprocessing.freeze_support() # Essential for PyInstaller or some envs
    
    dm = DataManager()
    
    print("Fetching Institutional Universe...")
    inst_univ = dm.get_institutional_universe()
    print(f"Total Stocks: {len(inst_univ)}")
    
    # Sample display
    sample = list(inst_univ.items())[:10]
    for ticker, meta in sample:
        print(f"{ticker:<15} | {meta['name']:<30} | {meta['market_cap']}")
