"""
QuantScanner 2026 — Central Configuration
==========================================
All tunable thresholds, hardware profiles, and system constants
live here. No magic numbers anywhere else in the codebase.
"""

from dataclasses import dataclass, field
from pathlib import Path

# ────────────────────────────────────────────────────────────
#  Paths
# ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "logs"
CACHE_DIR = PROJECT_ROOT / ".cache"

# Ensure dirs exist at import time
LOG_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)


# ────────────────────────────────────────────────────────────
#  Hardware Profile — Intel i7-14700K + RTX 4070 Super
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class HardwareProfile:
    """Tuned for i7-14700K (20 cores) + RTX 4070 Super."""
    CPU_CORES: int = 20
    USE_GPU: bool = True
    GPU_DEVICE_ID: int = 0
    MAX_MEMORY_GB: float = 32.0  # system RAM ceiling for data ops


# ────────────────────────────────────────────────────────────
#  Cache TTLs (seconds)
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class CacheConfig:
    TICKER_LIST_TTL: int = 86_400      # 24 hours
    OHLCV_TTL: int = 14_400            # 4 hours
    FUNDAMENTAL_TTL: int = 86_400      # 24 hours
    CACHE_SIZE_LIMIT_GB: float = 5.0   # max disk cache size


# ────────────────────────────────────────────────────────────
#  Scan Schedule (IST — UTC+05:30)
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class ScanSchedule:
    HEALTH_CHECK_HOUR: int = 14
    HEALTH_CHECK_MINUTE: int = 55
    SCAN_HOUR: int = 15
    SCAN_MINUTE: int = 0
    TIMEZONE: str = "Asia/Kolkata"


# ────────────────────────────────────────────────────────────
#  Gate 1 — Sector-Adjusted Spread Z-Score
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Gate1Config:
    """Stocks with abnormally wide spreads relative to sector peers."""
    MAX_SPREAD_Z_SCORE: float = 2.0
    MAX_ABS_SPREAD: float = 0.5         # cap at 50% spread
    ROLLING_WINDOW: int = 20            # days for sector mean/std


# ────────────────────────────────────────────────────────────
#  Gate 2 — Fundamentals
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Gate2Config:
    """Quality filter: Piotroski F-Score, cash-flow, pledge safety."""
    MIN_F_SCORE: int = 4
    MIN_CFO_PAT: float = 0.5           # CFO / PAT ratio
    MAX_PROMOTER_PLEDGE: float = 5.0    # % of promoter holding pledged


# ────────────────────────────────────────────────────────────
#  Gate 3 — Technicals
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Gate3Config:
    """Trend strength: Minervini template, ADX, Mansfield RS."""
    MIN_ADX: float = 10.0
    MIN_MANSFIELD_SLOPE: float = 0.01
    MA_SHORT: int = 50
    MA_MID: int = 150
    MA_LONG: int = 200
    RS_LOOKBACK_WEEKS: int = 52


# ────────────────────────────────────────────────────────────
#  Gate 4 — Execution
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Gate4Config:
    """Entry timing: volume confirmation, risk/reward, ATR stops."""
    VOL_PRORATE_FACTOR: float = 0.85    # prorated volume multiplier
    MIN_RR_RATIO: float = 2.0           # minimum reward : risk
    ATR_PERIOD: int = 14
    ATR_STOP_MULTIPLIER: float = 2.0    # stop = close - 2×ATR
    VOL_AVG_DAYS: int = 20              # baseline volume average
    MARKET_OPEN_MINUTES: int = 375      # 9:15 AM → 3:30 PM


# ────────────────────────────────────────────────────────────
#  Health Check
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class HealthCheckConfig:
    MIN_DISK_FREE_GB: float = 1.0
    MAX_CPU_TEMP_C: float = 85.0
    TEST_TICKER: str = "RELIANCE.NS"
    CONNECTIVITY_URL: str = "https://www.nseindia.com"
    CONNECTIVITY_TIMEOUT: int = 10      # seconds


# ────────────────────────────────────────────────────────────
#  Dry-Run Settings
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class DryRunConfig:
    SAMPLE_SIZE: int = 10
    SAMPLE_TICKERS: tuple = (
        "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
        "HINDUNILVR.NS", "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "KOTAKBANK.NS",
    )


# ────────────────────────────────────────────────────────────
#  Assembled Config (single import point)
# ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class AppConfig:
    hardware: HardwareProfile = field(default_factory=HardwareProfile)
    cache: CacheConfig = field(default_factory=CacheConfig)
    schedule: ScanSchedule = field(default_factory=ScanSchedule)
    gate1: Gate1Config = field(default_factory=Gate1Config)
    gate2: Gate2Config = field(default_factory=Gate2Config)
    gate3: Gate3Config = field(default_factory=Gate3Config)
    gate4: Gate4Config = field(default_factory=Gate4Config)
    health: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    dry_run: DryRunConfig = field(default_factory=DryRunConfig)


# Singleton — import this everywhere
CONFIG = AppConfig()
