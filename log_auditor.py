"""
Log Auditor
===========
Purpose: Records SEBI-compliant trading rationale logs for all scanned stocks.
Ensures every trade signal is backed by a verifiable data trail.
"""

import json
from datetime import datetime
from pathlib import Path
from loguru import logger
from config import LOG_DIR
import sys

# Configure Loguru
logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>", level="INFO")
logger.add(LOG_DIR / "system.log", rotation="10 MB", retention="10 days", level="DEBUG")

class LogAuditor:
    def __init__(self):
        self.log_dir = LOG_DIR
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.audit_file = self.log_dir / f"audit_{self.session_id}.json"
        
    def write_scan_log(self, candidates: list[str], full_rationale: dict):
        """
        Writes the complete scan results to a JSON audit file.
        args:
            candidates: List of tickers that passed all gates.
            full_rationale: {ticker: {gate_name: result_dict}}
        """
        logger.info(f"Auditing scan results to {self.audit_file}...")
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "session_id": self.session_id,
            "total_scanned": len(full_rationale),
            "candidates_count": len(candidates),
            "candidates": candidates,
            "rationale": full_rationale
        }
        
        try:
            with open(self.audit_file, "w") as f:
                json.dump(report, f, indent=2)
            logger.success(f"Audit log written successfully. {len(candidates)} candidates found.")
        except Exception as e:
            logger.error(f"Failed to write audit log: {e}")

    def log_critical_error(self, module: str, error_msg: str):
        """Logs critical failures."""
        logger.critical(f"[{module}] {error_msg}")

    def log_warning(self, module: str, warning_msg: str):
        """Logs data warnings for SEBI compliance."""
        logger.warning(f"[{module}] {warning_msg}")
