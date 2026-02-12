"""
Health Check
============
Automated pre-flight checks to ensure system readiness.
Runs at 2:55 PM IST (or on demand).
Validates: Internet, data sources, disk space, and hardware.
"""

import sys
import psutil
from dataclasses import dataclass
from loguru import logger
from config import CONFIG, CACHE_DIR
import requests
import yfinance as yf
from datetime import datetime
import os

@dataclass
class HealthReport:
    passed: bool
    details: dict

class HealthCheck:
    def __init__(self):
        self.config = CONFIG.health

    def check_connectivity(self) -> bool:
        """Checks internet access via simple HTTP request."""
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            # Try NSE first (with headers)
            r = requests.get(self.config.CONNECTIVITY_URL, headers=headers, timeout=self.config.CONNECTIVITY_TIMEOUT)
            if r.status_code == 200: return True
        except Exception:
            pass
            
        try:
            # Fallback to Google
            r = requests.get("https://www.google.com", timeout=5)
            return r.status_code == 200
        except Exception:
            return False

    def check_yfinance(self) -> bool:
        """Fetches a single candle for test ticker."""
        try:
            df = yf.download(self.config.TEST_TICKER, period="1d", progress=False, auto_adjust=True)
            return not df.empty
        except Exception:
            return False

    def check_disk_space(self) -> bool:
        """Ensures enough free space for logs/cache."""
        try:
            free_gb = psutil.disk_usage(str(CACHE_DIR)).free / (1024**3)
            return free_gb >= self.config.MIN_DISK_FREE_GB
        except Exception:
            return False

    def check_cpu_temp(self) -> float:
        """
        Checks CPU temperature. Windows support is limited in psutil.
        Returns temp or -1 if unavailable.
        """
        try:
            temps = psutil.sensors_temperatures()
            if not temps:
                return -1.0 # Not supported on this OS/Hardware
            
            # Find coretemp or similar
            if 'coretemp' in temps:
                entries = temps['coretemp']
                return max(e.current for e in entries)
            return -1.0
        except Exception:
            # AttributeError or similar on Windows
            return -1.0

    def run(self) -> HealthReport:
        """
        Executes all health checks.
        Returns HealthReport.
        """
        logger.info("Running system health checks...")
        report = {}
        all_passed = True
        
        # 1. Connectivity
        net_ok = self.check_connectivity()
        report['internet'] = "OK" if net_ok else "FAIL"
        if not net_ok: all_passed = False
        
        # 2. API Source
        yf_ok = self.check_yfinance()
        report['yfinance'] = "OK" if yf_ok else "FAIL"
        if not yf_ok: all_passed = False
        
        # 3. Disk Space
        disk_ok = self.check_disk_space()
        report['disk'] = "OK" if disk_ok else "FAIL (< 1GB)"
        if not disk_ok: all_passed = False
        
        # 4. CPU Temp
        temp = self.check_cpu_temp()
        if temp > self.config.MAX_CPU_TEMP_C:
            report['cpu_temp'] = f"WARNING ({temp}°C)"
            # Don't fail unless critical? Config says MAX... let's fail if critical.
            all_passed = False
        else:
            report['cpu_temp'] = f"OK ({temp}°C)" if temp > 0 else "N/A"
            
        final_status = "PASSED" if all_passed else "FAILED"
        if all_passed:
            logger.success(f"Health Check: {final_status}")
        else:
            logger.error(f"Health Check: {final_status} - {report}")
            
        return HealthReport(passed=all_passed, details=report)

if __name__ == "__main__":
    hc = HealthCheck()
    print(hc.run())
