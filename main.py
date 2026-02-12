"""
QuantScanner 2026 — Main Entry Point
====================================
Handles CLI arguments and scheduling.
Usage:
    python main.py --now       # Run scan immediately
    python main.py --dry-run   # Run test scan with sample tickers
    python main.py             # Startscheduler (runs daily at 3:00 PM)
"""

import argparse
import sys
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from loguru import logger
from orchestrator import Orchestrator
from config import CONFIG
import signal

def run_job(orchestrator, dry_run=False):
    """Wrapper for the scheduled job."""
    logger.info("Executing scheduled scan...")
    orchestrator.run_scan(dry_run=dry_run)

def main():

    parser = argparse.ArgumentParser(description="QuantScanner 2026")
    parser.add_argument("--now", action="store_true", help="Run scan immediately")
    parser.add_argument("--dry-run", action="store_true", help="Run with dry-run configuration (small sample)")
    parser.add_argument("--bse500", action="store_true", help="Target BSE 500 stocks only (and generate Watchlist)")
    parser.add_argument("--institutional", action="store_true", help="Use Institutional Universe (Nifty 50/Next50/Mid150/Small250)")
    args = parser.parse_args()

    # Initialize
    orchestrator = Orchestrator()
    
    # Flags override schedule
    if args.dry_run:
        logger.info("Starting DRY RUN...")
        run_job(orchestrator, dry_run=True)
        return

    if args.now:
        logger.info(f"Starting IMMEDIATE scan (BSE 500: {args.bse500}, Institutional: {args.institutional})...")
        orchestrator.run_scan(dry_run=False, target_bse500=args.bse500, institutional=args.institutional)
        return

    # Scheduling
    scheduler = BlockingScheduler(timezone=CONFIG.schedule.TIMEZONE)
    
    # Schedule Health Check 5 mins before
    scheduler.add_job(
        orchestrator.health.run,
        'cron',
        hour=CONFIG.schedule.HEALTH_CHECK_HOUR,
        minute=CONFIG.schedule.HEALTH_CHECK_MINUTE,
        name='HealthCheck'
    )
    
    # Schedule Main Scan
    scheduler.add_job(
        run_job,
        'cron',
        hour=CONFIG.schedule.SCAN_HOUR,
        minute=CONFIG.schedule.SCAN_MINUTE,
        args=[orchestrator],
        name='DailyScan'
    )
    
    logger.info(f"Scheduler started. Next scan at {CONFIG.schedule.SCAN_HOUR}:{CONFIG.schedule.SCAN_MINUTE:02d} IST.")
    logger.info("Press Ctrl+C to exit.")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    main()
