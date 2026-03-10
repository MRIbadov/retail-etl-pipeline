import logging
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class Loader:
    """Writes clean data to SQLite and flat-file outputs."""

    def __init__(self, db_path: str, processed_dir: str, archive_dir: str, run_date: str):
        self.db_path       = Path(db_path)
        self.processed_dir = Path(processed_dir)
        self.archive_dir   = Path(archive_dir)
        self.run_date      = run_date
        self._init_db()

    # ── Public API ─────────────────────────────────────────────────────────────
    def load(self, df: pd.DataFrame) -> dict:
        """Write data and return load metadata."""
        meta = {}
        partition = self.processed_dir / self.run_date
        partition.mkdir(parents=True, exist_ok=True)

        # 1. Write processed CSV
        csv_path = partition / f"sales_clean_{self.run_date}.csv"
        df.to_csv(csv_path, index=False)
        meta["processed_csv"] = str(csv_path)
        logger.info("Written processed CSV → %s  (%d rows)", csv_path.name, len(df))

        # 2. Write daily summaries
        summaries = self._build_summaries(df)
        for name, sdf in summaries.items():
            path = partition / f"summary_{name}_{self.run_date}.csv"
            sdf.to_csv(path, index=False)
            logger.info("Written summary '%s' → %s", name, path.name)

        # 3. Upsert into SQLite
        rows_inserted = self._upsert_sqlite(df)
        meta["rows_upserted_sqlite"] = rows_inserted

        # 4. Write QA flags table
        self._write_qa_flags(df)

        meta["summary_tables"] = list(summaries.keys())
        return meta

    def archive_raw_files(self, source_files: list):
        """Move raw CSV files to archive once successfully loaded."""
        archive_day = self.archive_dir / self.run_date
        archive_day.mkdir(parents=True, exist_ok=True)
        for f in source_files:
            src = Path(f)
            dst = archive_day / src.name
            if src.exists():
                shutil.move(str(src), dst)
                logger.info("Archived  %s  →  %s", src.name, dst)

    # ── Private helpers ────────────────────────────────────────────────────────
    def _init_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as con:
            cur = con.cursor()
            cur.executescript("""
                CREATE TABLE IF NOT EXISTS sales_fact (
                    transaction_id  TEXT PRIMARY KEY,
                    branch_id       TEXT,
                    sale_date       TEXT,
                    sale_timestamp  TEXT,
                    sale_hour       INTEGER,
                    day_of_week     TEXT,
                    week_number     INTEGER,
                    month           INTEGER,
                    year            INTEGER,
                    product_id      TEXT,
                    product_name    TEXT,
                    category        TEXT,
                    quantity        INTEGER,
                    unit_price      REAL,
                    discount_pct    REAL,
                    gross_amount    REAL,
                    net_amount      REAL,
                    is_discounted   INTEGER,
                    payment_type    TEXT,
                    customer_id     TEXT,
                    cashier_id      TEXT,
                    quality_flag    TEXT,
                    _source_file    TEXT,
                    _loaded_at      TEXT
                );

                CREATE TABLE IF NOT EXISTS daily_branch_summary (
                    summary_date    TEXT,
                    branch_id       TEXT,
                    total_transactions INTEGER,
                    total_units     INTEGER,
                    gross_revenue   REAL,
                    net_revenue     REAL,
                    avg_basket_size REAL,
                    loaded_at       TEXT,
                    PRIMARY KEY (summary_date, branch_id)
                );

                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id          TEXT PRIMARY KEY,
                    run_date        TEXT,
                    started_at      TEXT,
                    completed_at    TEXT,
                    status          TEXT,
                    raw_rows        INTEGER,
                    clean_rows      INTEGER,
                    notes           TEXT
                );
            """)
        logger.info("SQLite DB initialised: %s", self.db_path)

    def _upsert_sqlite(self, df: pd.DataFrame) -> int:
        df2 = df.copy()
        df2["sale_timestamp"] = df2["sale_timestamp"].astype(str)
        df2["is_discounted"]  = df2["is_discounted"].astype(int)
        df2["_loaded_at"]     = datetime.utcnow().isoformat()

        with sqlite3.connect(self.db_path) as con:
            df2.to_sql("sales_fact", con, if_exists="replace",
                       index=False, method="multi", chunksize=500)
        logger.info("Upserted %d rows into sales_fact", len(df2))

        # Branch summary
        summary = self._build_branch_summary(df)
        summary["loaded_at"] = datetime.utcnow().isoformat()
        with sqlite3.connect(self.db_path) as con:
            summary.to_sql("daily_branch_summary", con, if_exists="replace",
                           index=False, method="multi")
        return len(df2)

    def _write_qa_flags(self, df: pd.DataFrame):
        flagged = df[df["quality_flag"] != "OK"][
            ["transaction_id", "branch_id", "product_id", "quality_flag", "_source_file"]
        ]
        if not flagged.empty:
            partition = self.processed_dir / self.run_date
            path = partition / f"qa_flags_{self.run_date}.csv"
            flagged.to_csv(path, index=False)
            logger.info("QA flags written: %d flagged rows → %s", len(flagged), path.name)

    def _build_summaries(self, df: pd.DataFrame) -> dict:
        return {
            "by_branch":   self._build_branch_summary(df),
            "by_category": self._build_category_summary(df),
            "by_product":  self._build_product_summary(df),
            "by_hour":     self._build_hourly_summary(df),
        }

    def _build_branch_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        g = df.groupby("branch_id").agg(
            total_transactions=("transaction_id", "count"),
            total_units       =("quantity",       "sum"),
            gross_revenue     =("gross_amount",   "sum"),
            net_revenue       =("net_amount",     "sum"),
        ).reset_index()
        g["avg_basket_size"] = (g["net_revenue"] / g["total_transactions"]).round(2)
        g["gross_revenue"]   = g["gross_revenue"].round(2)
        g["net_revenue"]     = g["net_revenue"].round(2)
        g["summary_date"]    = self.run_date
        return g

    def _build_category_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby("category").agg(
            transactions=("transaction_id", "count"),
            units       =("quantity",       "sum"),
            net_revenue =("net_amount",     "sum"),
        ).reset_index().assign(summary_date=self.run_date)

    def _build_product_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        top = (
            df.groupby(["product_id", "product_name", "category"])
            .agg(units=("quantity", "sum"), net_revenue=("net_amount", "sum"))
            .reset_index()
            .sort_values("net_revenue", ascending=False)
            .head(20)
        )
        top["summary_date"] = self.run_date
        return top

    def _build_hourly_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby("sale_hour").agg(
            transactions=("transaction_id", "count"),
            net_revenue =("net_amount",     "sum"),
        ).reset_index().assign(summary_date=self.run_date)
