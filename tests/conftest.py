"""
conftest.py
Shared pytest fixtures — automatically available in all test_*.py files.
No imports needed in individual test files.
"""

import sys
import pytest
import numpy as np
import pandas as pd
from pathlib  import Path
from datetime import datetime

# ── Make scripts/ importable from any test file ───────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from transformer import Transformer

# ── Shared constant ────────────────────────────────────────────────────────────
RUN_DATE = datetime.today().strftime("%Y%m%d")


# ── Raw data fixture ───────────────────────────────────────────────────────────
@pytest.fixture
def sample_raw_df():
    """
    Minimal but representative raw DataFrame that mirrors the CSV schema.
    Intentionally includes every dirty-data scenario the transformer must handle:
      - duplicate transaction_id
      - malformed product_id  (ELEC1002 → missing hyphen)
      - invalid category      (Alien)
      - negative quantity     (-3)
      - zero quantity         (0)  ← this row is also the NaN customer_id row
      - zero unit_price       (0.0)
      - missing discount_pct  (NaN)
      - missing payment_type  (NaN)
      - missing customer_id   (NaN)  ← on the qty=0 row (dropped before fill)
      - 4 different timestamp formats
      - discount > 1          (25.0 → should normalise to 0.25)
    """
    return pd.DataFrame({
        "transaction_id": [
            "TXN-NYC_001-20240101-11111",
            "TXN-NYC_001-20240101-11111",   # duplicate
            "TXN-LA_002-20240101-22222",
            "TXN-CHI_003-20240101-33333",
            "TXN-HOU_004-20240101-44444",
            "TXN-PHX_005-20240101-55555",
        ],
        "branch_id":      ["NYC_001", "NYC_001", "LA_002", "CHI_003", "HOU_004", "PHX_005"],
        "product_id":     [
            "ELEC-1001", "ELEC-1001",
            "ELEC1002",   # malformed – no hyphen, repairable
            "GROC-3001",
            "APRL-2001",
            "ZZZZ-9999",  # valid format but unknown product
        ],
        "product_name":   [
            "Laptop Pro 15", "Laptop Pro 15", "Wireless Headphones",
            "Organic Coffee", "Winter Jacket", "Unknown Item",
        ],
        "category":       ["Electronics", "Electronics", "Electronics",
                           "Grocery", "Apparel", "Alien"],   # Alien = invalid
        "quantity":       [2, 2, -3, 0, 1, 4],              # -3 fixed, 0 dropped
        "unit_price":     [999.99, 999.99, 79.99, 12.50, 0.0, 50.0],  # 0.0 = suspicious
        "discount_pct":   [0.10, 0.10, 0.20, np.nan, 0.05, 25.0],     # NaN filled, 25→0.25
        "payment_type":   ["credit_card", "credit_card", np.nan, "cash", "mobile_pay", "debit_card"],
        "customer_id":    ["CUST-11111", "CUST-11111", "CUST-22222",
                           np.nan, "CUST-44444", "CUST-55555"],
        "sale_timestamp": [
            "2024-01-01 10:30:00",   # ISO
            "2024-01-01 10:30:00",
            "01/01/2024 11:15",      # US format
            "2024-01-01 14:00:00",
            "20240101153045",        # compact
            "01-01-2024 09:00:00",   # EU format
        ],
        "cashier_id":     ["EMP-101", "EMP-101", "EMP-202", "EMP-303", "EMP-404", "EMP-505"],
        "_source_file":   ["f.csv"] * 6,
    })


# ── Clean data fixture (output of Transformer) ────────────────────────────────
@pytest.fixture
def clean_df(sample_raw_df):
    """Pre-transformed DataFrame — used by tests that only need clean output."""
    t = Transformer(run_date=RUN_DATE)
    df, _ = t.transform(sample_raw_df)
    return df
