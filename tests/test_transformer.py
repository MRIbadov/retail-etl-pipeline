"""
test_transformer.py
Unit tests for scripts/transformer.py

Tests cover every cleaning and enrichment rule:
  - Deduplication
  - Timestamp parsing (4 formats)
  - Product ID repair and validation
  - Category validation
  - Negative / zero quantity handling
  - Suspicious price handling
  - Discount normalisation
  - Missing value imputation (payment, customer, discount)
  - Derived financial columns (gross, net, is_discounted)
  - Derived time features (sale_date, sale_hour, day_of_week, week_number)
  - Quality flag assignment
"""

import re
import numpy as np
import pytest
from transformer import Transformer
from conftest    import RUN_DATE


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerDeduplication:

    def test_duplicate_rows_removed(self, sample_raw_df):
        """Rows sharing the same transaction_id must be deduplicated."""
        t = Transformer(run_date=RUN_DATE)
        df, qa = t.transform(sample_raw_df)
        assert qa["duplicates_removed"] == 1
        assert df["transaction_id"].nunique() == len(df)


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerTimestamps:

    def test_all_timestamps_parsed(self, clean_df):
        """No NaT values should remain after transformation."""
        assert clean_df["sale_timestamp"].notna().all()

    def test_timestamp_is_utc(self, clean_df):
        """Parsed timestamps must be timezone-aware (UTC)."""
        import pandas as pd
        ts = pd.to_datetime(clean_df["sale_timestamp"], utc=True)
        assert ts.dt.tz is not None


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerProductIDs:

    def test_malformed_id_repaired(self, sample_raw_df):
        """ELEC1002 (missing hyphen) must be repaired to ELEC-1002."""
        t = Transformer(run_date=RUN_DATE)
        df, qa = t.transform(sample_raw_df)
        assert "ELEC-1002" in df["product_id"].values
        assert qa["product_ids_repaired"] >= 1

    def test_all_ids_have_valid_format(self, sample_raw_df):
        """
        Every product_id in the output must match XXXX-9999 format OR be the
        INVALID sentinel.  ZZZZ-9999 already matches the pattern so it stays.
        """
        pattern = re.compile(r"^[A-Z]{4}-\d{4}$")
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        for pid in df["product_id"]:
            assert pattern.match(pid) or pid == "INVALID", \
                f"product_id '{pid}' has unexpected format"


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerCategories:

    def test_invalid_category_relabelled(self, sample_raw_df):
        """'Alien' is not a valid category and must be relabelled 'Unknown'."""
        t = Transformer(run_date=RUN_DATE)
        df, qa = t.transform(sample_raw_df)
        assert "Alien"   not in df["category"].values
        assert "Unknown" in     df["category"].values
        assert qa["invalid_categories"] >= 1


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerQuantities:

    def test_negative_quantity_made_positive(self, sample_raw_df):
        """Negative quantities must be converted to their absolute value."""
        t = Transformer(run_date=RUN_DATE)
        df, qa = t.transform(sample_raw_df)
        assert (df["quantity"] < 0).sum() == 0
        assert qa["negative_qty_fixed"] >= 1

    def test_zero_quantity_rows_dropped(self, sample_raw_df):
        """Rows with quantity == 0 represent no sale and must be removed."""
        t = Transformer(run_date=RUN_DATE)
        df, qa = t.transform(sample_raw_df)
        assert (df["quantity"] == 0).sum() == 0
        assert qa["zero_qty_dropped"] >= 1


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerPrices:

    def test_zero_price_nullified(self, sample_raw_df):
        """A unit_price of 0 is suspicious and must be set to NaN."""
        t = Transformer(run_date=RUN_DATE)
        df, qa = t.transform(sample_raw_df)
        assert (df["unit_price"] == 0).sum() == 0
        assert qa["suspicious_prices"] >= 1


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerDiscounts:

    def test_discount_over_one_normalised(self, sample_raw_df):
        """A discount_pct of 25 means 25% — it must be normalised to 0.25."""
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        assert (df["discount_pct"] > 1).sum() == 0

    def test_discount_never_negative(self, sample_raw_df):
        """Discount cannot be negative after clamping."""
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        assert (df["discount_pct"] < 0).sum() == 0


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerMissingValues:

    def test_missing_payment_type_filled(self, sample_raw_df):
        """NaN payment_type must be imputed with the string 'unknown'."""
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        assert df["payment_type"].isna().sum() == 0
        assert "unknown" in df["payment_type"].values

    def test_missing_customer_id_filled(self, sample_raw_df):
        """
        NaN customer_id must be imputed with 'GUEST'.
        The NaN row in the base fixture also has qty=0 (dropped before fill),
        so we add an extra row with qty > 0 and NaN customer_id.
        """
        extra = sample_raw_df.copy()
        extra.loc[len(extra)] = {
            "transaction_id": "TXN-NYC_001-20240101-99999",
            "branch_id":      "NYC_001",
            "product_id":     "ELEC-1001",
            "product_name":   "Laptop Pro 15",
            "category":       "Electronics",
            "quantity":       1,
            "unit_price":     999.99,
            "discount_pct":   0.10,
            "payment_type":   "cash",
            "customer_id":    np.nan,            # <-- what we are testing
            "sale_timestamp": "2024-01-01 12:00:00",
            "cashier_id":     "EMP-101",
            "_source_file":   "f.csv",
        }
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(extra)
        assert df["customer_id"].isna().sum() == 0
        assert "GUEST" in df["customer_id"].values

    def test_missing_discount_filled_with_zero(self, sample_raw_df):
        """NaN discount_pct must be imputed with 0.0 (no discount)."""
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        assert df["discount_pct"].isna().sum() == 0


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerDerivedColumns:

    def test_gross_amount_calculated(self, clean_df):
        """gross_amount must equal unit_price × quantity for every row."""
        for _, row in clean_df.dropna(subset=["unit_price"]).iterrows():
            expected = round(row["unit_price"] * row["quantity"], 2)
            assert abs(row["gross_amount"] - expected) < 0.01

    def test_net_amount_calculated(self, clean_df):
        """net_amount must equal gross_amount × (1 - discount_pct)."""
        row = clean_df[clean_df["discount_pct"] > 0].iloc[0]
        expected = round(row["gross_amount"] * (1 - row["discount_pct"]), 2)
        assert abs(row["net_amount"] - expected) < 0.01

    def test_is_discounted_flag(self, clean_df):
        """is_discounted must be True iff discount_pct > 0."""
        for _, row in clean_df.iterrows():
            assert row["is_discounted"] == (row["discount_pct"] > 0)

    def test_time_features_present(self, clean_df):
        """All derived time columns must exist."""
        for col in ["sale_date", "sale_hour", "day_of_week", "week_number", "month", "year"]:
            assert col in clean_df.columns, f"Missing time feature column: {col}"

    def test_sale_hour_in_range(self, clean_df):
        """sale_hour must be between 0 and 23."""
        assert clean_df["sale_hour"].between(0, 23).all()


# ══════════════════════════════════════════════════════════════════════════════
class TestTransformerQualityFlags:

    def test_quality_flag_column_exists(self, clean_df):
        """quality_flag column must be present in output."""
        assert "quality_flag" in clean_df.columns

    def test_only_known_flag_values(self, clean_df):
        """quality_flag must only contain the defined set of values."""
        allowed = {"OK", "BAD_PRODUCT_ID", "MISSING_PRICE", "UNKNOWN_CATEGORY", "UNKNOWN_BRANCH"}
        assert set(clean_df["quality_flag"].unique()).issubset(allowed)

    def test_unknown_category_flagged(self, sample_raw_df):
        """Rows with an invalid category must have quality_flag = UNKNOWN_CATEGORY."""
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        unknown_rows = df[df["category"] == "Unknown"]
        assert (unknown_rows["quality_flag"] == "UNKNOWN_CATEGORY").all()

    def test_missing_price_flagged(self, sample_raw_df):
        """Rows where unit_price was 0 (now NaN) must be flagged MISSING_PRICE."""
        t = Transformer(run_date=RUN_DATE)
        df, _ = t.transform(sample_raw_df)
        missing_price_rows = df[df["unit_price"].isna()]
        assert (missing_price_rows["quality_flag"] == "MISSING_PRICE").all()
