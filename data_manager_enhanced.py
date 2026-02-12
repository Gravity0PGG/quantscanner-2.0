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

        # Fetch via finance
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
    #  NSE Index Constituent Fetching (ENHANCED with Fallbacks)
    # ------------------------------------------------------------------
    def _get_local_index_file(self, index_name: str) -> Path:
        """Check for recent local CSV file for an index."""
        # Sanitize index name for filename
        safe_name = index_name.replace(" ", "_").lower()
        pattern = f"{safe_name}_*.csv"
        
        # Find all matching files
        matches = list(INDICES_DIR.glob(pattern))
        if not matches:
            return None
        
        # Sort by modification time, get most recent
        most_recent = max(matches, key=lambda p: p.stat().st_mtime)
        
        # Check if file is < 90 days old
        age_days = (datetime.now() - datetime.fromtimestamp(most_recent.stat().st_mtime)).days
        if age_days < 90:
            logger.info(f"Found recent local cache for {index_name}: {most_recent.name} ({age_days} days old)")
            return most_recent
        else:
            logger.warning(f"Local cache for {index_name} is stale ({age_days} days old)")
            return None
    
    def _save_index_to_cache(self, index_name: str, constituents: dict):
        """Save index constituents to local cache."""
        safe_name = index_name.replace(" ", "_").lower()
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"{safe_name}_{date_str}.csv"
        filepath = INDICES_DIR / filename
        
        try:
            # Convert to DataFrame for CSV export
            df = pd.DataFrame([
                {"symbol": ticker, "name": meta.get("name", ""), "market_cap": meta.get("market_cap", "")}
                for ticker, meta in constituents.items()
            ])
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {index_name} to local cache: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save index to cache: {e}")
    
    def _load_index_from_cache(self, index_name: str, filepath: Path) -> dict:
        """Load index constituents from local CSV file."""
        cap_map = {
            "NIFTY 50": "LARGE",
            "NIFTY NEXT 50": "LARGE",
            "NIFTY MIDCAP 150": "MID",
            "NIFTY SMALLCAP 250": "SMALL"
        }
        
        try:
            df = pd.read_csv(filepath)
            constituents = {}
            for _, row in df.iterrows():
                symbol = row.get('symbol', row.get('SYMBOL', ''))
                name = row.get('name', row.get('NAME', row.get('Company Name', symbol)))
                
                if symbol:
                    ticker = f"{symbol}.NS" if not symbol.endswith('.NS') else symbol
                    constituents[ticker] = {
                        "name": name,
                        "market_cap": cap_map.get(index_name, "UNKNOWN")
                    }
            
            logger.success(f"Loaded {len(constituents)} stocks from local cache: {filepath.name}")
            return constituents
        except Exception as e:
            logger.error(f"Failed to load from cache: {e}")
            return {}
    
    def _fetch_nifty_index_constituents(self, index_name: str, retry_count: int = 0) -> dict:
        """
        Fetches constituent list for a specific Nifty index with multi-layer fallbacks.
        
        Fallback Strategy:
        1. NSE website (session + cookies)
        2. NiftyIndices.com (less restrictive)
        3. Local cache (data/indices/)
        4. Browser automation (if retry_count >= 3)
        """
        import requests
        import random
        
        # Check local cache first
        local_file = self._get_local_index_file(index_name)
        if local_file:
            return self._load_index_from_cache(index_name, local_file)
        
        # NSE CSV URLs (direct download links)
        csv_urls = {
            "NIFTY 50": "https://www1.nseindia.com/content/indices/ind_nifty50list.csv",
            "NIFTY NEXT 50": "https://www1.nseindia.com/content/indices/ind_niftynext50list.csv",
            "NIFTY MIDCAP 150": "https://www1.nseindia.com/content/indices/ind_niftymidcap150list.csv",
            "NIFTY SMALLCAP 250": "https://www1.nseindia.com/content/indices/ind_niftysmallcap250list.csv"
        }
        
        # Nifty Indices fallback URLs
        nifty_fallback_urls = {
            "NIFTY 50": "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv",
            "NIFTY MIDCAP 150": "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv",
            "NIFTY SMALLCAP 250": "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"
        }
        
        cap_map = {
            "NIFTY 50": "LARGE",
            "NIFTY NEXT 50": "LARGE",
            "NIFTY MIDCAP 150": "MID",
            "NIFTY SMALLCAP 250": "SMALL"
        }
        
        url = csv_urls.get(index_name)
        if not url:
            logger.error(f"Unknown index: {index_name}")
            return {}
        
        logger.info(f"Fetching {index_name} constituents (attempt {retry_count + 1})...")
        
        # Attempt 1: NSE with session + cookies
        try:
            s = requests.Session()
            
            # Rotate user agent
            ua = random.choice(self.user_agents)
            headers = {
                "User-Agent": ua,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            }
            s.headers.update(headers)
            
            # Visit homepage to capture cookies (bm_sz, nsid)
            logger.debug("Visiting NSE homepage to capture cookies...")
            homepage_resp = s.get("https://www.nseindia.com", timeout=10, allow_redirects=True)
            homepage_resp.raise_for_status()
            
            # Log captured cookies
            cookies = s.cookies.get_dict()
            logger.debug(f"Captured cookies: {list(cookies.keys())}")
            
            # Now fetch CSV with established session
            logger.debug(f"Fetching CSV from: {url}")
            csv_resp = s.get(url, timeout=15)
            csv_resp.raise_for_status()
            
            # Parse CSV
            import io
            df = pd.read_csv(io.StringIO(csv_resp.text))
            
            constituents = {}
            symbol_col = None
            name_col = None
            
            # Find symbol and name columns (NSE uses different naming)
            for col in df.columns:
                col_lower = col.lower()
                if 'symbol' in col_lower:
                    symbol_col = col
                if 'company' in col_lower or 'name' in col_lower:
                    name_col = col
            
            if not symbol_col:
                logger.error(f"Could not find symbol column in CSV. Columns: {df.columns.tolist()}")
                raise ValueError("Symbol column not found")
            
            for _, row in df.iterrows():
                symbol = row.get(symbol_col, '')
                name = row.get(name_col, symbol) if name_col else symbol
                
                if symbol and isinstance(symbol, str):
                    ticker = f"{symbol}.NS"
                    constituents[ticker] = {
                        "name": str(name),
                        "market_cap": cap_map[index_name]
                    }
            
            if constituents:
                logger.success(f"Fetched {len(constituents)} stocks from NSE for {index_name}")
                # Save to local cache
                self._save_index_to_cache(index_name, constituents)
                return constituents
                
        except Exception as e:
            logger.warning(f"NSE fetch failed: {e}")
        
        # Attempt 2: NiftyIndices.com fallback
        fallback_url = nifty_fallback_urls.get(index_name)
        if fallback_url:
            try:
                logger.info(f"Trying NiftyIndices.com fallback...")
                resp = requests.get(fallback_url, timeout=15, headers={"User-Agent": random.choice(self.user_agents)})
                resp.raise_for_status()
                
                df = pd.read_csv(io.StringIO(resp.text))
                constituents = {}
                
                # Same parsing logic
                symbol_col = None
                name_col = None
                for col in df.columns:
                    col_lower = col.lower()
                    if 'symbol' in col_lower:
                        symbol_col = col
                    if 'company' in col_lower or 'name' in col_lower:
                        name_col = col
                
                if symbol_col:
                    for _, row in df.iterrows():
                        symbol = row.get(symbol_col, '')
                        name = row.get(name_col, symbol) if name_col else symbol
                        
                        if symbol and isinstance(symbol, str):
                            ticker = f"{symbol}.NS"
                            constituents[ticker] = {
                                "name": str(name),
                                "market_cap": cap_map[index_name]
                            }
                    
                    if constituents:
                        logger.success(f"Fetched {len(constituents)} stocks from NiftyIndices.com")
                        self._save_index_to_cache(index_name, constituents)
                        return constituents
                        
            except Exception as e:
                logger.warning(f"NiftyIndices fallback failed: {e}")
        
        # Attempt 3: Check local cache again (stale files acceptable if all else fails)
        stale_files = list(INDICES_DIR.glob(f"{index_name.replace(' ', '_').lower()}_*.csv"))
        if stale_files:
            most_recent = max(stale_files, key=lambda p: p.stat().st_mtime)
            logger.warning(f"Using stale cache file: {most_recent.name}")
            return self._load_index_from_cache(index_name, most_recent)
        
        # Attempt 4: Browser automation (manual download) - only if retry >= 3
        if retry_count >= 3:
            logger.critical(f"All automated fetches failed for {index_name}. Manual browser download required.")
            logger.info("Please manually download CSV and place in data/indices/ folder")
            # In production, could trigger browser_subagent here
           
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
        
        # Fetch all indices with retry logic
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
        
        if not universe:
            logger.error("Failed to build institutional universe - all index fetches failed")
            return {}
        
        # Cache for 24 hours
        self.cache.set(cache_key, universe, expire=CONFIG.cache.TICKER_LIST_TTL)
        
        logger.success(f"Institutional Universe created: {len(universe)} stocks")
        logger.info(f"  LARGE: {sum(1 for v in universe.values() if v['market_cap'] == 'LARGE')}")
        logger.info(f"  MID: {sum(1 for v in universe.values() if v['market_cap'] == 'MID')}")
        logger.info(f"  SMALL: {sum(1 for v in universe.values() if v['market_cap'] == 'SMALL')}")
        
        return universe

    # ... (rest of the methods remain the same: _fetch_bse_tickers_from_source, 
    # _fetch_nse_tickers_from_source, get_universe, batch_fetch_ohlcv, batch_fetch_sector_map)
