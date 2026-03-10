"""
generate_sample_data.py
Generates realistic daily retail sales CSV files for multiple store branches.
Run this once to populate data/raw/ with test data.
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
BRANCHES      = ["NYC_001", "LA_002", "CHI_003", "HOU_004", "PHX_005"]
CATEGORIES    = ["Electronics", "Apparel", "Grocery", "Furniture", "Sports"]
PAYMENT_TYPES = ["cash", "credit_card", "debit_card", "mobile_pay"]
_HERE      = Path(__file__).resolve().parent
_BASE      = _HERE.parent if _HERE.name == "scripts" else _HERE
OUTPUT_DIR = _BASE / "data" / "raw"

PRODUCTS = {
    "Electronics": [("ELEC-1001", "Laptop Pro 15"),   ("ELEC-1002", "Wireless Headphones"),
                    ("ELEC-1003", "Smart Watch"),       ("ELEC-1004", "USB-C Hub")],
    "Apparel":     [("APRL-2001", "Winter Jacket"),    ("APRL-2002", "Running Shoes"),
                    ("APRL-2003", "Denim Jeans"),       ("APRL-2004", "Cotton T-Shirt")],
    "Grocery":     [("GROC-3001", "Organic Coffee"),   ("GROC-3002", "Almond Milk"),
                    ("GROC-3003", "Whole Grain Bread"), ("GROC-3004", "Mixed Nuts")],
    "Furniture":   [("FURN-4001", "Office Chair"),     ("FURN-4002", "Standing Desk"),
                    ("FURN-4003", "Bookshelf"),         ("FURN-4004", "Bed Frame")],
    "Sports":      [("SPRT-5001", "Yoga Mat"),         ("SPRT-5002", "Resistance Bands"),
                    ("SPRT-5003", "Dumbbell Set"),      ("SPRT-5004", "Cycling Helmet")],
}

PRICE_RANGE = {
    "ELEC-1001": (899, 1299), "ELEC-1002": (49, 199), "ELEC-1003": (199, 499),  "ELEC-1004": (29, 79),
    "APRL-2001": (89, 249),   "APRL-2002": (59, 179), "APRL-2003": (39, 99),    "APRL-2004": (15, 45),
    "GROC-3001": (12, 25),    "GROC-3002": (3, 8),    "GROC-3003": (4, 9),      "GROC-3004": (8, 18),
    "FURN-4001": (199, 699),  "FURN-4002": (299, 999),"FURN-4003": (79, 299),   "FURN-4004": (149, 599),
    "SPRT-5001": (25, 89),    "SPRT-5002": (15, 55),  "SPRT-5003": (99, 399),   "SPRT-5004": (39, 129),
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def random_timestamp(date: datetime) -> str:
    """Return a random store-hours timestamp on the given date (formats vary to simulate real-world messiness)."""
    hour   = random.randint(8, 21)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt     = date.replace(hour=hour, minute=minute, second=second)
    formats = [
        "%Y-%m-%d %H:%M:%S",   # ISO
        "%m/%d/%Y %H:%M",      # US
        "%d-%m-%Y %H:%M:%S",   # EU
        "%Y%m%d%H%M%S",        # compact
    ]
    return dt.strftime(random.choice(formats))


def inject_dirt(df: pd.DataFrame, dirty_pct: float = 0.08) -> pd.DataFrame:
    """Introduce realistic data quality issues for the pipeline to clean."""
    n = len(df)

    # Missing values
    for col in ["customer_id", "discount_pct", "payment_type"]:
        idx = df.sample(frac=dirty_pct / 3).index
        df.loc[idx, col] = np.nan

    # Negative quantities (data-entry errors)
    neg_idx = df.sample(frac=dirty_pct / 4).index
    df.loc[neg_idx, "quantity"] = df.loc[neg_idx, "quantity"] * -1

    # Malformed product IDs
    bad_pid = df.sample(frac=dirty_pct / 5).index
    df.loc[bad_pid, "product_id"] = df.loc[bad_pid, "product_id"].apply(lambda x: x.replace("-", ""))

    # Zero-price outliers
    zero_price = df.sample(frac=dirty_pct / 10).index
    df.loc[zero_price, "unit_price"] = 0.0

    return df


def generate_branch_day(branch: str, date: datetime, n_rows: int) -> pd.DataFrame:
    records = []
    for _ in range(n_rows):
        cat                   = random.choice(CATEGORIES)
        pid, pname            = random.choice(PRODUCTS[cat])
        lo, hi                = PRICE_RANGE[pid]
        qty                   = random.randint(1, 10)
        price                 = round(random.uniform(lo, hi), 2)
        discount              = round(random.uniform(0, 0.30), 2)
        records.append({
            "transaction_id": f"TXN-{branch}-{date.strftime('%Y%m%d')}-{random.randint(10000,99999)}",
            "branch_id":      branch,
            "product_id":     pid,
            "product_name":   pname,
            "category":       cat,
            "quantity":       qty,
            "unit_price":     price,
            "discount_pct":   discount,
            "payment_type":   random.choice(PAYMENT_TYPES),
            "customer_id":    f"CUST-{random.randint(10000, 99999)}",
            "sale_timestamp": random_timestamp(date),
            "cashier_id":     f"EMP-{random.randint(100, 999)}",
        })
    return pd.DataFrame(records)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    today  = datetime.today()
    dates  = [today - timedelta(days=d) for d in range(7, 0, -1)]  # last 7 days

    files_created = []
    for date in dates:
        for branch in BRANCHES:
            n_rows = random.randint(60, 200)
            df     = generate_branch_day(branch, date, n_rows)
            df     = inject_dirt(df)
            fname  = OUTPUT_DIR / f"sales_{branch}_{date.strftime('%Y%m%d')}.csv"
            df.to_csv(fname, index=False)
            files_created.append(fname)
            print(f"  ✓  {fname.name}  ({len(df)} rows)")

    print(f"\n✅  Generated {len(files_created)} raw data files in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
