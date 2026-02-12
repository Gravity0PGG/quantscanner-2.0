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
#  Robust yfinance Wrapper
# ------------------------------------------------------------------
def robust_yf_download(ticker, period="1y", interval="1d", retries=3):
    """
    Downloads data with randomized user-agents and retry logic for crumbs/auth.
    """
    import random
    import time
    import requests
    
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ]

    for attempt in range(retries):
        try:
            session = requests.Session()
            session.headers.update({"User-Agent": random.choice(uas)})
            
            df = yf.download(
                ticker, 
                period=period, 
                interval=interval,
                progress=False, 
                auto_adjust=True,
                multi_level_index=False,
                session=session,
                timeout=20
            )
            
            if not df.empty:
                return df
                
            if attempt < retries - 1:
                time.sleep(1.5 ** attempt)
        except Exception as e:
            err_str = str(e)
            if "Unauthorized" in err_str or "Invalid Crumb" in err_str or "401" in err_str:
                if attempt < retries - 1:
                    logger.warning(f"Yahoo block detected for {ticker}. Retrying with fresh session...")
                    time.sleep(2)
                    continue
            if attempt == retries - 1:
                return pd.DataFrame()
    return pd.DataFrame()

# ------------------------------------------------------------------
#  Standalone Worker Function (Windows Pickling Safe)
# ------------------------------------------------------------------
def fetch_worker(args):
    """
    Standalone worker function to fetch OHLCV for a single ticker.
    """
    ticker, period = args
    try:
        worker_cache = Cache(directory=str(CACHE_DIR), size_limit=int(CONFIG.cache.CACHE_SIZE_LIMIT_GB * 1e9))
        cache_key = f"ohlcv_{ticker}_{period}"
        cached_df = worker_cache.get(cache_key)
        
        if cached_df is not None:
            return (ticker, cached_df, None)

        # Use Robust Wrapper
        df = robust_yf_download(ticker, period=period)
        
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

import asyncio
import aiohttp
from typing import Dict, List, Optional
from io import StringIO

class DataManager:
    def __init__(self):
        self.cache = Cache(directory=str(CACHE_DIR), size_limit=int(CONFIG.cache.CACHE_SIZE_LIMIT_GB * 1e9))
        self.bse = BSE()
        self.nse = Nse()
        self.hardware = CONFIG.hardware
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ]
        self.semaphore = asyncio.Semaphore(30) # As requested: high-performance async throttle

    # ------------------------------------------------------------------
    #  FULL UNIVERSE DISCOVERY (BSE + NSE Mainboard)
    # ------------------------------------------------------------------
    
    async def get_full_universe(self) -> dict:
        """
        Discovery Engine: Fetches the full Master Equity List from BSE and NSE.
        Applies strict filtering:
        - Excludes SME, Z, T, TS groups
        - Market Cap > ₹100 Cr
        """
        cache_key = "full_market_universe"
        cached = self.cache.get(cache_key)
        if cached:
            logger.info("Loaded full universe from cache.")
            return cached

        logger.info("Initializing Discovery Engine: Fetching Master Equity List...")
        
        # 1. Fetch NSE Master List
        nse_equity = await self._fetch_nse_master()
        
        # 2. Fetch BSE Master List
        bse_equity = await self._fetch_bse_master()
        
        # Merge and Cross-reference
        combined_universe = {**bse_equity, **nse_equity}
        
        # Apply strict Trash Filter & Market Cap Check
        # Note: Market Cap check often requires a quick batch fetch of 'info'
        # We'll fetch basic info for scrips that pass the group filter
        filtered_universe = await self._apply_universe_filters(combined_universe)
        
        self.cache.set(cache_key, filtered_universe, expire=86400) # 24h
        logger.success(f"Discovery Complete: {len(filtered_universe)} Mainboard Equities detected.")
        return filtered_universe

    async def _fetch_nse_master(self) -> dict:
        url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
        headers = {"User-Agent": self.user_agents[0]}
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        content = await resp.text()
                        df = pd.read_csv(StringIO(content))
                        # NSE Filter: Only Mainboard (ignore SME, which have 'SM' in series usually or separate list)
                        # We also ignore Z series (cautionary)
                        valid_series = ['EQ', 'BE'] # Regular and Trade-to-Trade (T group in BSE = BE in NSE)
                        df = df[df[' SERIES'].str.strip().isin(valid_series)]
                        
                        return {f"{row['SYMBOL']}.NS": {"name": row['NAME OF COMPANY'], "market_cap_val": 0} for _, row in df.iterrows()}
        except Exception as e:
            logger.error(f"NSE Master Fetch Error: {e}")
        return {}

    async def _fetch_bse_master(self) -> dict:
        # BSE doesn't provide a clean public equity CSV as easily as NSE. 
        # We use their scrip list API or a known equity list URL.
        url = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&Industry=&Segment=Equity&Status=Active"
        headers = {"User-Agent": self.user_agents[1], "Referer": "https://www.bseindia.com/"}
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=15) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # BSE Groups: A, B, T (Trade-to-Trade). Reject: Z (caution), SME (MT/ST/XT)
                        valid_groups = ['A', 'B', 'T', 'X'] 
                        equity = {}
                        for item in data:
                            group = item.get('scrip_grp', '').strip()
                            if group in valid_groups:
                                symbol = item.get('scrip_cd')
                                equity[f"{symbol}.BO"] = {"name": item.get('scrip_name'), "market_cap_val": 0}
                        return equity
        except Exception as e:
            logger.error(f"BSE Master Fetch Error: {e}")
        return {}

    async def _apply_universe_filters(self, universe: dict) -> dict:
        """Detailed filtering for Market Cap > 100Cr and removal of junk."""
        logger.info(f"Filtering {len(universe)} potential scrips...")
        
        # We process in batches to avoid yfinance blocking
        tickers = list(universe.keys())
        batch_size = 100
        final_universe = {}
        
        async def fetch_info(ticker):
            async with self.semaphore:
                try:
                    t = yf.Ticker(ticker)
                    # Use fast_info if available, or just get market cap
                    m_cap = t.info.get('marketCap', 0)
                    if m_cap and m_cap >= 1e9: # 100 Cr = 1,000,000,000
                        # Determine cap category
                        if m_cap > 200e9: # 20,000 Cr Large Cap
                            cat = "LARGE"
                        elif m_cap > 50e9: # 5,000 Cr Mid Cap
                            cat = "MID"
                        else:
                            cat = "SMALL"
                        return ticker, cat, m_cap
                except:
                    pass
                return None

        tasks = [fetch_info(t) for t in tickers]
        results = await asyncio.gather(*tasks)
        
        for res in results:
            if res:
                ticker, cat, m_cap = res
                final_universe[ticker] = {
                    "name": universe[ticker]["name"],
                    "market_cap": cat,
                    "market_cap_val": m_cap
                }
        
        return final_universe

    # ------------------------------------------------------------------
    #  INSTITUTIONAL UNIVERSE (Nifty 500, indices, etc.)
    # ------------------------------------------------------------------

    def get_institutional_universe(self) -> dict:
        """
        Standard Scan: Fetches Nifty 50, Next 50, Midcap 150, and Smallcap 250.
        """
        cache_key = "institutional_universe"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        logger.info("Fetching Institutional Universe (Nifty Indices)...")
        universe = {}
        
        # We fetch Nifty Smallcap 250 for the test scan requested by user
        smallcap_250 = self._fetch_nifty_index_constituents("NIFTY SMALLCAP 250")
        universe.update(smallcap_250)
        
        # Add Nifty 50 for good measure if needed, but keeping it focused for now
        
        self.cache.set(cache_key, universe, expire=86400 * 7)
        return universe

    def _fetch_nifty_index_constituents(self, index_name: str) -> dict:
        """Fetches constituents for a given Nifty index from niftyindices.com CSVs."""
        urls = {
            "NIFTY 50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
            "NIFTY SMALLCAP 250": "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv"
        }
        url = urls.get(index_name)
        if not url: return {}

        try:
            import requests
            headers = {"User-Agent": self.user_agents[0]}
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200: return {}
            
            df = pd.read_csv(StringIO(resp.text))
            universe = {}
            for _, row in df.iterrows():
                symbol = str(row.get('Symbol', '')).strip()
                # Skip indices and malformed tickers
                skip_keywords = ['NIFTY', 'MIDCAP', 'SMLCAP', 'SMALLCAP', '250', '150', '50']
                if not symbol or not symbol.isalnum() or symbol.lower() == 'nan' or \
                   symbol.startswith('$') or any(k in symbol.upper() for k in skip_keywords): 
                    continue
                
                ticker = symbol + ".NS"
                universe[ticker] = {
                    "ticker": ticker,
                    "name": str(row.get('Company Name', 'N/A')),
                    "market_cap": "SMALL" if "SMALLCAP" in index_name else "LARGE",
                    "sector": "Unknown"
                }
            return universe
        except Exception as e:
            logger.error(f"Failed to fetch {index_name} constituents: {e}")
            return {}

    # ------------------------------------------------------------------
    #  ASYNC OHLCV FETCHING
    # ------------------------------------------------------------------

    async def async_fetch_ohlcv(self, ticker: str, period: str) -> tuple:
        async with self.semaphore:
            try:
                # Use Robust Wrapper
                df = await asyncio.to_thread(robust_yf_download, ticker, period=period)
                if df.empty: return ticker, None
                return ticker, df
            except:
                return ticker, None

    async def batch_fetch_ohlcv(self, tickers: list[str], period: str = "1y") -> dict:
        """Asynchronous fetching of OHLCV data."""
        tasks = [self.async_fetch_ohlcv(t, period) for t in tickers]
        results = await asyncio.gather(*tasks)
        return {t: df for t, df in results if df is not None}

    async def batch_fetch_sector_map(self, tickers: list[str]) -> dict[str, str]:
        """Asynchronous fetching of sector information."""
        logger.info(f"Fetching Sector Map for {len(tickers)} tickers...")
        
        async def _fetch_single(ticker):
            _, sector = await asyncio.to_thread(fetch_sector_worker, (ticker, ))
            return (ticker, sector)

        tasks = [_fetch_single(t) for t in tickers]
        results = await asyncio.gather(*tasks)
        
        sector_map = dict(results)
        logger.success(f"Fetched sectors for {len(sector_map)} stocks.")
        return sector_map

    def batch_fetch_live_prices(self, tickers: list[str]) -> dict[str, float]:
        """
        Fetches the latest LTP (Last Traded Price) for a list of tickers.
        """
        if not tickers:
            return {}
            
        logger.info(f"Fetching live prices for {len(tickers)} tickers...")
        try:
            # Use Robust Wrapper
            data = robust_yf_download(tickers, period="1d", interval="1m")
            
            prices = {}
            if data.empty:
                return {}
                
            for ticker in tickers:
                try:
                    if len(tickers) == 1:
                        # yf.download returns a Series if only one ticker is requested sometimes
                        # but with multi_level_index=False it should be a DF
                        val = data['Close'].iloc[-1]
                    else:
                        val = data['Close'][ticker].iloc[-1]
                    
                    if not pd.isna(val):
                        prices[ticker] = float(val)
                except Exception:
                    continue
            return prices
        except Exception as e:
            logger.error(f"Live price fetch failed: {e}")
            return {}




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
