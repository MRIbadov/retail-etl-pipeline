"""
test_integration.py
End-to-end integration tests for the full ETL pipeline.

These tests spin up a complete temporary environment (raw CSVs, SQLite,
processed output) and run all three stages in sequence to verify that
the pipeline works as a whole — not just individual units.

Tests cover:
  - Single branch end-to-end
  - Multi-branch end-to-end
  - Dirty data survives the full pipeline without crashing
  - Output row count is consistent across CSV, SQLite, and transform metadata
  - Pipeline is re-runnable (idempotent)
"""

import random
import sqlite3
import pytest
import pandas as pd
from pathlib  import Path
from datetime import datetime

from extractor   import Extractor
from transformer import Transformer
from loader      import Loader
from conftest    import RUN_DATE


# ── Helpers ────────────────────────────────────────────────────────────────────
def _write_branch_csv(path: Path, branch: str, date: str, n_rows: int = 20) -> None:
    """Write a clean minimal sales CSV for one branch."""
    rows = [{
        "transaction_id": f"TXN-{branch}-{date}-{i:05d}",
        "branch_id":      branch,
        "product_id":     "ELEC-1001",
        "product_name":   "Laptop Pro 15",
        "category":       "Electronics",
        "quantity":       random.randint(1, 5),
        "unit_price":     999.99,
        "discount_pct":   0.10,
        "payment_type":   "cash",
        "customer_id":    f"CUST-{i:05d}",
        "sale_timestamp": f"{date[:4]}-{date[4:6]}-{date[6:]} 10:00:00",
        "cashier_id":     "EMP-001",
    } for i in range(n_rows)]
    pd.DataFrame(rows).to_csv(path, index=False)


def _run_etl(raw_dir: Path, tmp_path: Path, date: str):
    """Run all 3 ETL stages and return (clean_df, e_meta, t_meta, l_meta)."""
    raw_df, e_meta  = Extractor(raw_dir=str(raw_dir), run_date=date).extract()
    clean_df, t_meta = Transformer(run_date=date).transform(raw_df)
    l_meta = Loader(
        db_path       = str(tmp_path / "warehouse.db"),
        processed_dir = str(tmp_path / "processed"),
        archive_dir   = str(tmp_path / "archive"),
        run_date      = date,
    ).load(clean_df)
    return clean_df, e_meta, t_meta, l_meta


# ══════════════════════════════════════════════════════════════════════════════
class TestIntegrationSingleBranch:

    def test_single_branch_end_to_end(self, tmp_path):
        """Basic smoke test: one branch file flows through all 3 stages."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_branch_csv(raw_dir / f"sales_NYC_001_{RUN_DATE}.csv", "NYC_001", RUN_DATE, 20)

        clean_df, e_meta, t_meta, l_meta = _run_etl(raw_dir, tmp_path, RUN_DATE)

        assert e_meta["files_loaded"]         == 1
        assert t_meta["output_rows"]          >  0
        assert l_meta["rows_upserted_sqlite"] == len(clean_df)

    def test_sqlite_row_count_matches_transform(self, tmp_path):
        """Rows in SQLite must match the transformer's output_rows exactly."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_branch_csv(raw_dir / f"sales_NYC_001_{RUN_DATE}.csv", "NYC_001", RUN_DATE, 30)

        _, _, t_meta, _ = _run_etl(raw_dir, tmp_path, RUN_DATE)

        with sqlite3.connect(tmp_path / "warehouse.db") as con:
            db_count = con.execute("SELECT COUNT(*) FROM sales_fact").fetchone()[0]
        assert db_count == t_meta["output_rows"]

    def test_processed_csv_matches_sqlite(self, tmp_path):
        """The processed CSV and SQLite must contain the same number of rows."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_branch_csv(raw_dir / f"sales_NYC_001_{RUN_DATE}.csv", "NYC_001", RUN_DATE, 15)

        _, _, _, l_meta = _run_etl(raw_dir, tmp_path, RUN_DATE)

        csv_rows = len(pd.read_csv(l_meta["processed_csv"]))
        with sqlite3.connect(tmp_path / "warehouse.db") as con:
            db_rows = con.execute("SELECT COUNT(*) FROM sales_fact").fetchone()[0]
        assert csv_rows == db_rows


# ══════════════════════════════════════════════════════════════════════════════
class TestIntegrationMultiBranch:

    def test_five_branches_combined(self, tmp_path):
        """All 5 branch files are extracted and loaded as a single batch."""
        raw_dir  = tmp_path / "raw"
        raw_dir.mkdir()
        branches = ["NYC_001", "LA_002", "CHI_003", "HOU_004", "PHX_005"]
        for b in branches:
            _write_branch_csv(raw_dir / f"sales_{b}_{RUN_DATE}.csv", b, RUN_DATE, 10)

        clean_df, e_meta, t_meta, l_meta = _run_etl(raw_dir, tmp_path, RUN_DATE)

        assert e_meta["files_loaded"] == 5
        assert t_meta["output_rows"]  == 50   # 5 branches × 10 rows each

    def test_branch_summary_covers_all_branches(self, tmp_path):
        """The daily_branch_summary table must have one row per branch."""
        raw_dir  = tmp_path / "raw"
        raw_dir.mkdir()
        branches = ["NYC_001", "LA_002", "CHI_003"]
        for b in branches:
            _write_branch_csv(raw_dir / f"sales_{b}_{RUN_DATE}.csv", b, RUN_DATE, 5)

        _run_etl(raw_dir, tmp_path, RUN_DATE)

        with sqlite3.connect(tmp_path / "warehouse.db") as con:
            rows = con.execute("SELECT branch_id FROM daily_branch_summary").fetchall()
        assert {r[0] for r in rows} == set(branches)


# ══════════════════════════════════════════════════════════════════════════════
class TestIntegrationDirtyData:

    def test_dirty_data_survives_pipeline(self, tmp_path):
        """
        A file with intentional dirty data (negatives, nulls, bad IDs)
        must not raise an exception and must produce at least some clean rows.
        """
        import numpy as np

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        df = pd.DataFrame({
            "transaction_id": [f"TXN-NYC_001-{RUN_DATE}-{i:05d}" for i in range(10)],
            "branch_id":      ["NYC_001"] * 10,
            "product_id":     ["ELEC-1001", "ELEC1002", "GROC3001", "APRL-2001",
                                "ELEC-1001", "FURN-4001", "SPRT5001", "ELEC-1003",
                                "GROC-3002", "APRL-2002"],
            "product_name":   ["Laptop"] * 10,
            "category":       ["Electronics", "Electronics", "Grocery", "Apparel",
                                "Electronics", "Furniture", "Sports", "Electronics",
                                "Grocery", "Apparel"],
            "quantity":       [1, -2, 0, 3, 2, 1, 4, -1, 2, 1],
            "unit_price":     [999.99, 79.99, 0.0, 89.99, 999.99,
                                299.99, 25.99, 399.99, 3.99, 59.99],
            "discount_pct":   [0.10, np.nan, 0.20, 0.05, np.nan,
                                30.0, 0.15, 0.0, np.nan, 0.10],
            "payment_type":   ["cash", np.nan, "credit_card", "debit_card", "cash",
                                "mobile_pay", np.nan, "cash", "credit_card", "debit_card"],
            "customer_id":    [f"CUST-{i:05d}" for i in range(10)],
            "sale_timestamp": [f"{RUN_DATE[:4]}-{RUN_DATE[4:6]}-{RUN_DATE[6:]} 10:00:00"] * 10,
            "cashier_id":     ["EMP-001"] * 10,
        })
        df.to_csv(raw_dir / f"sales_NYC_001_{RUN_DATE}.csv", index=False)

        clean_df, _, t_meta, l_meta = _run_etl(raw_dir, tmp_path, RUN_DATE)

        assert t_meta["output_rows"]          >  0
        assert l_meta["rows_upserted_sqlite"] == len(clean_df)
        assert (clean_df["quantity"] < 0).sum() == 0
        assert (clean_df["discount_pct"] > 1).sum() == 0


# ══════════════════════════════════════════════════════════════════════════════
class TestIntegrationIdempotency:

    def test_pipeline_reruns_without_error(self, tmp_path):
        """Running the pipeline twice for the same date must not raise errors."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_branch_csv(raw_dir / f"sales_NYC_001_{RUN_DATE}.csv", "NYC_001", RUN_DATE, 10)

        _run_etl(raw_dir, tmp_path, RUN_DATE)
        _run_etl(raw_dir, tmp_path, RUN_DATE)   # second run

        with sqlite3.connect(tmp_path / "warehouse.db") as con:
            count = con.execute("SELECT COUNT(*) FROM sales_fact").fetchone()[0]
        assert count == 10   # no duplicates from the second run
