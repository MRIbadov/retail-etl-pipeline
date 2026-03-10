"""
test_extractor.py
Unit tests for scripts/extractor.py

Tests cover:
  - File discovery by run_date pattern
  - Schema validation (required columns)
  - Graceful handling of missing directory
  - Graceful handling of no matching files
  - Multi-file ingestion and row count
"""

import pytest
import pandas as pd
from pathlib import Path
from extractor import Extractor
from conftest  import RUN_DATE


# ── Helpers ────────────────────────────────────────────────────────────────────
def _write_csv(path: Path, n_rows: int = 1) -> None:
    """Write a minimal valid sales CSV to *path*."""
    rows = [{
        "transaction_id": f"TXN-NYC_001-{RUN_DATE}-{i:05d}",
        "branch_id":      "NYC_001",
        "product_id":     "ELEC-1001",
        "product_name":   "Laptop Pro 15",
        "category":       "Electronics",
        "quantity":       1,
        "unit_price":     999.99,
        "discount_pct":   0.10,
        "payment_type":   "cash",
        "customer_id":    f"CUST-{i:05d}",
        "sale_timestamp": "2024-01-01 10:00:00",
        "cashier_id":     "EMP-001",
    } for i in range(n_rows)]
    pd.DataFrame(rows).to_csv(path, index=False)


# ══════════════════════════════════════════════════════════════════════════════
class TestExtractor:

    def test_discovers_single_file(self, tmp_path):
        """Extractor finds a file that matches the run_date pattern."""
        _write_csv(tmp_path / f"sales_NYC_001_{RUN_DATE}.csv")
        e = Extractor(raw_dir=str(tmp_path), run_date=RUN_DATE)
        df, meta = e.extract()
        assert meta["files_loaded"]   == 1
        assert meta["files_found"]    == 1
        assert meta["total_raw_rows"] == 1
        assert len(df) == 1

    def test_discovers_multiple_files(self, tmp_path):
        """All branch files for the same date are combined into one DataFrame."""
        branches = ["NYC_001", "LA_002", "CHI_003"]
        for branch in branches:
            _write_csv(tmp_path / f"sales_{branch}_{RUN_DATE}.csv", n_rows=5)
        e = Extractor(raw_dir=str(tmp_path), run_date=RUN_DATE)
        df, meta = e.extract()
        assert meta["files_loaded"]   == 3
        assert meta["total_raw_rows"] == 15
        assert len(df) == 15

    def test_source_file_column_added(self, tmp_path):
        """Each loaded row gets a _source_file column with the filename."""
        fname = f"sales_NYC_001_{RUN_DATE}.csv"
        _write_csv(tmp_path / fname)
        e = Extractor(raw_dir=str(tmp_path), run_date=RUN_DATE)
        df, _ = e.extract()
        assert "_source_file" in df.columns
        assert df["_source_file"].iloc[0] == fname

    def test_does_not_load_wrong_date(self, tmp_path):
        """Files from a different date must not be loaded."""
        _write_csv(tmp_path / f"sales_NYC_001_19990101.csv")   # wrong date
        e = Extractor(raw_dir=str(tmp_path), run_date=RUN_DATE)
        with pytest.raises(RuntimeError):
            e.extract()

    def test_missing_directory_raises(self, tmp_path):
        """A non-existent raw_dir must raise RuntimeError."""
        e = Extractor(raw_dir=str(tmp_path / "does_not_exist"), run_date=RUN_DATE)
        with pytest.raises(RuntimeError):
            e.extract()

    def test_no_matching_files_raises(self, tmp_path):
        """An empty directory must raise RuntimeError."""
        e = Extractor(raw_dir=str(tmp_path), run_date=RUN_DATE)
        with pytest.raises(RuntimeError):
            e.extract()

    def test_missing_required_column_skips_file(self, tmp_path):
        """A CSV that is missing a required column is skipped (files_failed += 1)."""
        bad_csv = tmp_path / f"sales_NYC_001_{RUN_DATE}.csv"
        pd.DataFrame({"transaction_id": ["T1"], "branch_id": ["NYC_001"]}).to_csv(
            bad_csv, index=False
        )
        # Add a second, valid file so extract() doesn't raise
        _write_csv(tmp_path / f"sales_LA_002_{RUN_DATE}.csv")
        e = Extractor(raw_dir=str(tmp_path), run_date=RUN_DATE)
        _, meta = e.extract()
        assert meta["files_failed"] == 1
        assert meta["files_loaded"] == 1
