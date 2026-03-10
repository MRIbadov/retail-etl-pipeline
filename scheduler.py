"""
scheduler.py — Lightweight Windows-compatible pipeline scheduler
────────────────────────────────────────────────────────────────
A drop-in alternative to Apache Airflow for running the ETL pipeline
on Windows. Uses the `schedule` library (pip install schedule).

Usage:
    # Run pipeline once right now (yesterday's data):
    python scheduler.py --run-now

    # Start the continuous daily scheduler (runs every day at 02:00):
    python scheduler.py

    # Run for a specific date:
    python scheduler.py --date 20260309
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import schedule

# ── Path setup ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from scripts.pipeline import run_pipeline

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE_DIR / "logs" / "scheduler.log"),
    ],
)
logger = logging.getLogger("scheduler")


# ── Scheduled job ─────────────────────────────────────────────────────────────
def daily_job():
    """Runs every day — processes yesterday's data."""
    run_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    logger.info("⏰  Scheduled trigger fired  →  run_date=%s", run_date)
    try:
        report = run_pipeline(run_date)
        logger.info("✅  Pipeline finished: status=%s  clean_rows=%s",
                    report["status"],
                    report.get("transform", {}).get("output_rows", "?"))
    except Exception as exc:
        logger.error("❌  Pipeline failed: %s", exc)


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Retail ETL Scheduler (Windows-compatible)")
    parser.add_argument("--run-now",  action="store_true",
                        help="Run pipeline immediately for yesterday, then exit")
    parser.add_argument("--date", "-d", default=None,
                        help="Run pipeline for a specific date YYYYMMDD, then exit")
    parser.add_argument("--time", default="02:00",
                        help="Daily run time HH:MM in 24h format (default: 02:00)")
    args = parser.parse_args()

    # ── One-shot modes ─────────────────────────────────────────────────────────
    if args.date:
        logger.info("One-shot run for date=%s", args.date)
        run_pipeline(args.date)
        return

    if args.run_now:
        logger.info("One-shot run for yesterday")
        daily_job()
        return

    # ── Continuous scheduler ───────────────────────────────────────────────────
    (BASE_DIR / "logs").mkdir(exist_ok=True)
    schedule.every().day.at(args.time).do(daily_job)

    logger.info("=" * 55)
    logger.info("  Retail Sales ETL Scheduler started")
    logger.info("  Runs daily at %s  (Ctrl+C to stop)", args.time)
    logger.info("=" * 55)

    # Show next run time
    next_run = schedule.next_run()
    logger.info("  Next scheduled run: %s", next_run)

    while True:
        schedule.run_pending()
        time.sleep(30)   # check every 30 seconds


if __name__ == "__main__":
    main()
