"""
extractor.py
Ingests raw CSV sales files from all store branches.
Handles file discovery, schema validation, and read errors gracefully.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import List, Tuple

logger = logging.getLogger(__name__)

# ── Expected raw schema ────────────────────────────────────────────────────────
REQUIRED_COLUMNS = {
    "transaction_id", "branch_id", "product_id", "product_name",
    "category", "quantity", "unit_price", "discount_pct",
    "payment_type", "customer_id", "sale_timestamp", "cashier_id",
}

DTYPE_MAP = {
    "transaction_id": str,
    "branch_id":      str,
    "product_id":     str,
    "product_name":   str,
    "category":       str,
    "quantity":       float,   # float first so negatives / NaN survive read
    "unit_price":     float,
    "discount_pct":   float,
    "payment_type":   str,
    "customer_id":    str,
    "sale_timestamp": str,
    "cashier_id":     str,
}


class Extractor:
    """Discovers and loads raw CSV files for a given date partition."""

    def __init__(self, raw_dir: str, run_date: str):
        """
        Parameters
        ----------
        raw_dir  : directory containing raw CSV files
        run_date : 'YYYYMMDD' string – used to match files for today's run
        """
        self.raw_dir  = Path(raw_dir)
        self.run_date = run_date

    # ── Public API ─────────────────────────────────────────────────────────────
    def extract(self) -> Tuple[pd.DataFrame, dict]:
        """
        Scan raw_dir for files matching *run_date*, load them, and return:
            (combined_dataframe, extraction_metadata_dict)
        """
        files   = self._discover_files()
        frames  = []
        meta    = {"files_found": len(files), "files_loaded": 0,
                   "files_failed": 0, "total_raw_rows": 0, "failed_files": []}

        for f in files:
            df, ok = self._load_file(f)
            if ok:
                frames.append(df)
                meta["files_loaded"]   += 1
                meta["total_raw_rows"] += len(df)
                logger.info("Loaded  %-55s  rows=%d", f.name, len(df))
            else:
                meta["files_failed"] += 1
                meta["failed_files"].append(str(f))

        if not frames:
            raise RuntimeError(
                f"No files could be loaded for run_date={self.run_date}. "
                f"Files found: {[f.name for f in files]}"
            )

        combined = pd.concat(frames, ignore_index=True)
        logger.info(
            "Extraction complete: %d files loaded, %d failed, %d total rows",
            meta["files_loaded"], meta["files_failed"], meta["total_raw_rows"],
        )
        return combined, meta

    # ── Private helpers ────────────────────────────────────────────────────────
    def _discover_files(self) -> List[Path]:
        pattern = f"sales_*_{self.run_date}.csv"
        files   = sorted(self.raw_dir.glob(pattern))
        if not files:
            logger.warning("No files matched pattern '%s' in %s", pattern, self.raw_dir)
        return files

    def _load_file(self, path: Path) -> Tuple[pd.DataFrame, bool]:
        try:
            df = pd.read_csv(path, dtype=DTYPE_MAP, na_values=["", "NULL", "N/A", "null", "na"])
            missing = REQUIRED_COLUMNS - set(df.columns)
            if missing:
                raise ValueError(f"Missing columns: {missing}")
            df["_source_file"] = path.name
            return df, True
        except Exception as exc:
            logger.error("Failed to load %s: %s", path.name, exc)
            return pd.DataFrame(), False
