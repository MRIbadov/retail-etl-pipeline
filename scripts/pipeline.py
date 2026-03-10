import argparse
import json
import logging
import sys
import traceback
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Make scripts importable when run from repo root or via Airflow
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scripts.extractor import Extractor
from scripts.transformer import Transformer
from scripts.loader import Loader

# ── Logging setup ──────────────────────────────────────────────────────────────
def _get_base() -> Path:
    """Return project root regardless of whether script is flat or in scripts/."""
    here = Path(__file__).resolve().parent
    return here.parent if here.name == "scripts" else here

LOG_DIR = _get_base() / "logs"
LOG_DIR.mkdir(exist_ok=True)


def setup_logging(run_date: str) -> logging.Logger:
    log_file = LOG_DIR / f"pipeline_{run_date}.log"
    fmt      = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger("pipeline")
    logger.info("Log file: %s", log_file)
    return logger


# ── Path helpers ───────────────────────────────────────────────────────────────
def get_paths() -> dict:
    # Works whether scripts live at project root OR inside a scripts/ subfolder
    here = Path(__file__).resolve().parent
    base = here.parent if here.name == "scripts" else here
    return {
        "raw_dir":       base / "data" / "raw",
        "processed_dir": base / "data" / "processed",
        "archive_dir":   base / "data" / "archive",
        "db_path":       base / "db"   / "sales_warehouse.db",
    }


# ── Main pipeline function ─────────────────────────────────────────────────────
def run_pipeline(run_date: str) -> dict:
    """
    Execute the full ETL pipeline for *run_date* (YYYYMMDD).
    Returns a summary dict (also written to logs/).
    """
    logger   = setup_logging(run_date)
    paths    = get_paths()
    started  = datetime.utcnow()
    run_id   = f"run_{run_date}_{started.strftime('%H%M%S')}"
    report   = {"run_id": run_id, "run_date": run_date, "status": "UNKNOWN"}

    logger.info("=" * 70)
    logger.info("PIPELINE START  run_id=%s  run_date=%s", run_id, run_date)
    logger.info("=" * 70)

    try:
        # ── EXTRACT ────────────────────────────────────────────────────────────
        logger.info("─── PHASE 1: EXTRACT ───")
        extractor      = Extractor(raw_dir=paths["raw_dir"], run_date=run_date)
        raw_df, e_meta = extractor.extract()
        logger.info("Extract metadata: %s", e_meta)

        # ── TRANSFORM ──────────────────────────────────────────────────────────
        logger.info("─── PHASE 2: TRANSFORM ───")
        transformer        = Transformer(run_date=run_date)
        clean_df, t_meta   = transformer.transform(raw_df)
        logger.info("Transform metadata: %s", t_meta)

        if len(clean_df) == 0:
            raise ValueError("Transformation produced zero clean rows – aborting load.")

        # ── LOAD ───────────────────────────────────────────────────────────────
        logger.info("─── PHASE 3: LOAD ───")
        loader  = Loader(
            db_path       = paths["db_path"],
            processed_dir = paths["processed_dir"],
            archive_dir   = paths["archive_dir"],
            run_date      = run_date,
        )
        l_meta  = loader.load(clean_df)
        logger.info("Load metadata: %s", l_meta)

        # Archive source files
        loader.archive_raw_files(e_meta.get("failed_files", []) + [])   # only archive known sources
        # (full archive of matched files would require path list from extractor – kept simple here)

        # ── SUMMARY ────────────────────────────────────────────────────────────
        completed = datetime.utcnow()
        report.update({
            "status":        "SUCCESS",
            "run_id":        run_id,
            "started_at":    started.isoformat(),
            "completed_at":  completed.isoformat(),
            "duration_secs": (completed - started).total_seconds(),
            "extract":       e_meta,
            "transform":     t_meta,
            "load":          l_meta,
        })

        logger.info("=" * 70)
        logger.info("PIPELINE SUCCESS  duration=%.1fs  clean_rows=%d",
                    report["duration_secs"], t_meta.get("output_rows", 0))
        logger.info("=" * 70)

    except Exception as exc:
        completed = datetime.utcnow()
        report.update({
            "status":       "FAILED",
            "error":        str(exc),
            "traceback":    traceback.format_exc(),
            "started_at":   started.isoformat(),
            "completed_at": completed.isoformat(),
        })
        logger.error("PIPELINE FAILED: %s", exc)
        logger.error(traceback.format_exc())

        raise 

    finally:
        # Always persist the run report
        report_path = LOG_DIR / f"report_{run_id}.json"
        with open(report_path, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        logger.info("Run report: %s", report_path)

    return report


# ── CLI entry-point ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Sales ETL Pipeline")
    parser.add_argument(
        "--date", "-d",
        default=(datetime.today() - timedelta(days=1)).strftime("%Y%m%d"),
        help="Run date YYYYMMDD (default: yesterday)",
    )
    args   = parser.parse_args()
    result = run_pipeline(args.date)
    print(json.dumps(result, indent=2, default=str))
