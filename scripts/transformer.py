"""
transformer.py
All data cleaning, validation, normalisation, and feature-engineering
logic for the Retail Sales ETL Pipeline.

Transformation steps
---------------------
1.  Deduplicate transaction IDs
2.  Normalise sale_timestamp  → UTC ISO-8601
3.  Validate & repair product_id format
4.  Validate category whitelist
5.  Fix negative quantities
6.  Fix zero / negative unit_price (flag as suspicious)
7.  Clamp discount_pct to [0, 1]
8.  Fill missing payment_type  → "unknown"
9.  Fill missing customer_id   → "GUEST"
10. Fill missing discount_pct  → 0.0
11. Derive: net_amount, gross_amount, is_discounted
12. Derive: sale_date, sale_hour, day_of_week, week_number
13. Validate branch_id whitelist
14. Build quality_flag column
"""

import logging
import re
import pandas as pd
import numpy as np
from datetime import timezone
from typing import Tuple

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
VALID_CATEGORIES   = {"Electronics", "Apparel", "Grocery", "Furniture", "Sports"}
VALID_BRANCHES     = {"NYC_001", "LA_002", "CHI_003", "HOU_004", "PHX_005"}
VALID_PAYMENTS     = {"cash", "credit_card", "debit_card", "mobile_pay", "unknown"}
PRODUCT_ID_PATTERN = re.compile(r"^[A-Z]{4}-\d{4}$")

# Multiple timestamp formats seen in the wild (from the generator)
TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%Y%m%d%H%M%S",
]


class Transformer:
    """Applies all cleaning and enrichment rules, returning a clean DataFrame + QA report."""

    def __init__(self, run_date: str):
        self.run_date = run_date

    # ── Public API ─────────────────────────────────────────────────────────────
    def transform(self, raw: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
        df   = raw.copy()
        qa   = {"input_rows": len(df)}

        df   = self._deduplicate(df, qa)
        df   = self._parse_timestamps(df, qa)
        df   = self._validate_product_ids(df, qa)
        df   = self._validate_categories(df, qa)
        df   = self._validate_branches(df, qa)
        df   = self._fix_quantities(df, qa)
        df   = self._fix_prices(df, qa)
        df   = self._fix_discounts(df, qa)
        df   = self._fill_missing(df, qa)
        df   = self._derive_financials(df)
        df   = self._derive_time_features(df)
        df   = self._build_quality_flag(df, qa)
        df   = self._final_select(df)

        qa["output_rows"] = len(df)
        logger.info(
            "Transform complete: %d in → %d out  (dropped=%d)",
            qa["input_rows"], qa["output_rows"],
            qa["input_rows"] - qa["output_rows"],
        )
        return df, qa

    # ── Step implementations ───────────────────────────────────────────────────
    def _deduplicate(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        before = len(df)
        df     = df.drop_duplicates(subset=["transaction_id"])
        qa["duplicates_removed"] = before - len(df)
        if qa["duplicates_removed"]:
            logger.warning("Removed %d duplicate transaction_ids", qa["duplicates_removed"])
        return df

    def _parse_timestamps(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        """Try multiple formats; rows that cannot be parsed are dropped."""
        parsed   = pd.Series(pd.NaT, index=df.index)
        unparsed = df.index.tolist()

        for fmt in TIMESTAMP_FORMATS:
            if not unparsed:
                break
            sub   = df.loc[unparsed, "sale_timestamp"]
            trial = pd.to_datetime(sub, format=fmt, errors="coerce")
            ok    = trial.notna()
            parsed.loc[trial[ok].index] = trial[ok]
            unparsed = [i for i in unparsed if pd.isna(parsed.loc[i])]

        n_bad = len(unparsed)
        if n_bad:
            logger.warning("Dropping %d rows with unparseable timestamps", n_bad)
            df = df.drop(index=unparsed)
            parsed = parsed.drop(index=unparsed)

        # Localise to UTC (no tz info in source → assume local store time is UTC for simplicity)
        df["sale_timestamp"] = pd.to_datetime(parsed.loc[df.index]).dt.tz_localize("UTC")
        qa["timestamp_parse_failures"] = n_bad
        return df

    def _validate_product_ids(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        """Attempt to repair compact IDs (e.g. ELEC1001 → ELEC-1001), flag the rest."""
        def repair(pid: str) -> str:
            if pd.isna(pid):
                return "UNKNOWN"
            pid = str(pid).strip().upper()
            if PRODUCT_ID_PATTERN.match(pid):
                return pid
            # Try inserting hyphen after 4 alpha chars
            m = re.match(r"^([A-Z]{4})(\d{4})$", pid)
            if m:
                return f"{m.group(1)}-{m.group(2)}"
            return "INVALID"

        original             = df["product_id"].copy()
        df["product_id"]     = df["product_id"].apply(repair)
        repaired             = ((original != df["product_id"]) & (df["product_id"] != "INVALID")).sum()
        invalid              = (df["product_id"] == "INVALID").sum()
        qa["product_ids_repaired"] = int(repaired)
        qa["product_ids_invalid"]  = int(invalid)
        if repaired:
            logger.info("Repaired %d malformed product IDs", repaired)
        if invalid:
            logger.warning("%d product IDs could not be repaired (flagged INVALID)", invalid)
        return df

    def _validate_categories(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        bad = ~df["category"].isin(VALID_CATEGORIES)
        qa["invalid_categories"] = int(bad.sum())
        df.loc[bad, "category"] = "Unknown"
        return df

    def _validate_branches(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        bad = ~df["branch_id"].isin(VALID_BRANCHES)
        qa["invalid_branches"] = int(bad.sum())
        if bad.any():
            logger.warning("Found %d rows with unrecognised branch_id", bad.sum())
        return df

    def _fix_quantities(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        neg = df["quantity"] < 0
        qa["negative_qty_fixed"] = int(neg.sum())
        df.loc[neg, "quantity"] = df.loc[neg, "quantity"].abs()
        # Zero quantities → drop (no sale)
        zero = df["quantity"] == 0
        qa["zero_qty_dropped"] = int(zero.sum())
        df = df[~zero]
        df["quantity"] = df["quantity"].astype(int)
        return df

    def _fix_prices(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        zero_or_neg = df["unit_price"] <= 0
        qa["suspicious_prices"] = int(zero_or_neg.sum())
        df.loc[zero_or_neg, "unit_price"] = np.nan   # will be excluded from revenue calcs
        return df

    def _fix_discounts(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        # Discount expressed as whole number (e.g. 25 instead of 0.25) → normalise
        big = df["discount_pct"] > 1
        df.loc[big, "discount_pct"] = df.loc[big, "discount_pct"] / 100
        df["discount_pct"] = df["discount_pct"].clip(0, 1)
        qa["discount_clamped"] = int(big.sum())
        return df

    def _fill_missing(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        fills = {
            "payment_type": "unknown",
            "customer_id":  "GUEST",
            "discount_pct": 0.0,
        }
        counts = {}
        for col, val in fills.items():
            n = df[col].isna().sum()
            df[col] = df[col].fillna(val)
            counts[f"filled_{col}"] = int(n)
        qa.update(counts)
        return df

    def _derive_financials(self, df: pd.DataFrame) -> pd.DataFrame:
        df["gross_amount"] = (df["unit_price"] * df["quantity"]).round(2)
        df["net_amount"]   = (df["gross_amount"] * (1 - df["discount_pct"])).round(2)
        df["is_discounted"] = df["discount_pct"] > 0
        return df

    def _derive_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        ts = df["sale_timestamp"]
        df["sale_date"]    = ts.dt.date.astype(str)
        df["sale_hour"]    = ts.dt.hour
        df["day_of_week"]  = ts.dt.day_name()
        df["week_number"]  = ts.dt.isocalendar().week.astype(int)
        df["month"]        = ts.dt.month
        df["year"]         = ts.dt.year
        return df

    def _build_quality_flag(self, df: pd.DataFrame, qa: dict) -> pd.DataFrame:
        flags = pd.Series("OK", index=df.index)
        flags[df["product_id"].isin(["INVALID", "UNKNOWN"])]   = "BAD_PRODUCT_ID"
        flags[df["unit_price"].isna()]                         = "MISSING_PRICE"
        flags[df["category"] == "Unknown"]                     = "UNKNOWN_CATEGORY"
        flags[~df["branch_id"].isin(VALID_BRANCHES)]           = "UNKNOWN_BRANCH"
        df["quality_flag"]              = flags
        qa["rows_flagged_non_ok"]       = int((flags != "OK").sum())
        return df

    def _final_select(self, df: pd.DataFrame) -> pd.DataFrame:
        ordered = [
            "transaction_id", "branch_id", "sale_date", "sale_timestamp",
            "sale_hour", "day_of_week", "week_number", "month", "year",
            "product_id", "product_name", "category",
            "quantity", "unit_price", "discount_pct",
            "gross_amount", "net_amount", "is_discounted",
            "payment_type", "customer_id", "cashier_id",
            "quality_flag", "_source_file",
        ]
        return df[[c for c in ordered if c in df.columns]]
