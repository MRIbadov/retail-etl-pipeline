"""
test_loader.py
Unit tests for scripts/loader.py

Tests cover:
  - SQLite table creation (sales_fact, daily_branch_summary)
  - Row count integrity between DataFrame and database
  - Processed CSV file written to correct partition path
  - All 4 summary CSVs written (by_branch, by_category, by_product, by_hour)
  - QA flags CSV written when flagged rows exist
  - Idempotent load (running twice doesn't break anything)
"""

import sqlite3
import pytest
import pandas as pd
from pathlib import Path
from loader   import Loader
from conftest import RUN_DATE


# ── Helper: build a Loader pointed at tmp_path ────────────────────────────────
def _make_loader(tmp_path: Path) -> Loader:
    return Loader(
        db_path       = str(tmp_path / "test.db"),
        processed_dir = str(tmp_path / "processed"),
        archive_dir   = str(tmp_path / "archive"),
        run_date      = RUN_DATE,
    )


# ══════════════════════════════════════════════════════════════════════════════
class TestLoaderDatabase:

    def test_sales_fact_table_created(self, clean_df, tmp_path):
        """sales_fact table must exist in SQLite after a load."""
        _make_loader(tmp_path).load(clean_df)
        with sqlite3.connect(tmp_path / "test.db") as con:
            tables = {r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "sales_fact" in tables

    def test_branch_summary_table_created(self, clean_df, tmp_path):
        """daily_branch_summary table must exist in SQLite after a load."""
        _make_loader(tmp_path).load(clean_df)
        with sqlite3.connect(tmp_path / "test.db") as con:
            tables = {r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        assert "daily_branch_summary" in tables

    def test_row_count_matches_dataframe(self, clean_df, tmp_path):
        """Number of rows in sales_fact must equal the clean DataFrame length."""
        meta = _make_loader(tmp_path).load(clean_df)
        assert meta["rows_upserted_sqlite"] == len(clean_df)

    def test_sqlite_row_count_direct(self, clean_df, tmp_path):
        """Cross-check row count by querying SQLite directly."""
        _make_loader(tmp_path).load(clean_df)
        with sqlite3.connect(tmp_path / "test.db") as con:
            count = con.execute("SELECT COUNT(*) FROM sales_fact").fetchone()[0]
        assert count == len(clean_df)

    def test_load_is_idempotent(self, clean_df, tmp_path):
        """Running the loader twice on the same data must not cause errors."""
        loader = _make_loader(tmp_path)
        loader.load(clean_df)
        loader.load(clean_df)   # second run — replace, not duplicate
        with sqlite3.connect(tmp_path / "test.db") as con:
            count = con.execute("SELECT COUNT(*) FROM sales_fact").fetchone()[0]
        assert count == len(clean_df)


# ══════════════════════════════════════════════════════════════════════════════
class TestLoaderFiles:

    def test_processed_csv_written(self, clean_df, tmp_path):
        """A partitioned processed CSV must be written to processed/<date>/."""
        meta = _make_loader(tmp_path).load(clean_df)
        assert Path(meta["processed_csv"]).exists()

    def test_processed_csv_row_count(self, clean_df, tmp_path):
        """The processed CSV must contain the same number of rows as the DataFrame."""
        meta = _make_loader(tmp_path).load(clean_df)
        saved = pd.read_csv(meta["processed_csv"])
        assert len(saved) == len(clean_df)

    def test_all_four_summaries_written(self, clean_df, tmp_path):
        """All 4 summary CSVs (branch, category, product, hour) must exist."""
        meta    = _make_loader(tmp_path).load(clean_df)
        partition = tmp_path / "processed" / RUN_DATE
        summaries = list(partition.glob("summary_*.csv"))
        assert len(summaries) == 4, \
            f"Expected 4 summary files, found {len(summaries)}: {[f.name for f in summaries]}"

    def test_summary_names_correct(self, clean_df, tmp_path):
        """Each expected summary filename must be present."""
        _make_loader(tmp_path).load(clean_df)
        partition = tmp_path / "processed" / RUN_DATE
        names = {f.name for f in partition.glob("summary_*.csv")}
        for expected in ["by_branch", "by_category", "by_product", "by_hour"]:
            assert any(expected in n for n in names), \
                f"summary_{expected} not found in {names}"

    def test_qa_flags_written_when_flagged_rows_exist(self, clean_df, tmp_path):
        """A qa_flags CSV must be written when any row has quality_flag != 'OK'."""
        has_flags = (clean_df["quality_flag"] != "OK").any()
        _make_loader(tmp_path).load(clean_df)
        partition  = tmp_path / "processed" / RUN_DATE
        qa_files   = list(partition.glob("qa_flags_*.csv"))
        if has_flags:
            assert len(qa_files) == 1, "Expected qa_flags CSV to be written"
        # If no flagged rows, file simply won't exist — both outcomes are acceptable


# ══════════════════════════════════════════════════════════════════════════════
class TestLoaderSummaryContent:

    def test_branch_summary_has_all_branches(self, clean_df, tmp_path):
        """daily_branch_summary must have one row per branch present in the data."""
        _make_loader(tmp_path).load(clean_df)
        partition   = tmp_path / "processed" / RUN_DATE
        summary     = pd.read_csv(partition / f"summary_by_branch_{RUN_DATE}.csv")
        expected    = set(clean_df["branch_id"].unique())
        assert set(summary["branch_id"]) == expected

    def test_branch_summary_net_revenue_positive(self, clean_df, tmp_path):
        """Net revenue in the branch summary must be positive."""
        _make_loader(tmp_path).load(clean_df)
        partition = tmp_path / "processed" / RUN_DATE
        summary   = pd.read_csv(partition / f"summary_by_branch_{RUN_DATE}.csv")
        assert (summary["net_revenue"].dropna() >= 0).all()
