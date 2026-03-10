## 📐 Architecture

```
       
         ┌───────────────▼───────────────┐
         │           EXTRACT             │
         │  • Discover CSV files by date  │
         │  • Schema validation           │
         │  • Multi-branch ingestion      │
         │  • Error isolation per file    │
         └───────────────┬───────────────┘
                         │
         ┌───────────────▼───────────────┐
         │          TRANSFORM            │
         │  • Deduplication              │
         │  • Timestamp normalisation    │
         │  • Product ID repair          │
         │  • Negative qty / price fix   │
         │  • Missing value imputation   │
         │  • Derived financials         │
         │  • Time feature engineering   │
         │  • QA flag assignment         │
         └───────────────┬───────────────┘
                         │
         ┌───────────────▼───────────────┐
         │             LOAD              │
         │  • SQLite warehouse upsert    │
         │  • Partitioned processed CSV  │
         │  • Daily summary aggregations │
         │  • QA flags report            │
         │  • Raw file archival          │
         └──────────────────────────────┘

```
---

## 🚀 Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate sample data (last 7 days)
```bash
python scripts/generate_sample_data.py
```

### 3. Run the pipeline for a specific date
```bash
python scripts/pipeline.py --date 20260309
```

### 4. Run tests
```bash
pytest tests/anyTest -v
```




## 🧹 Data Quality & Transformation Rules

| Issue | Rule Applied |
|---|---|
| Duplicate `transaction_id` | Keep first occurrence, drop rest |
| Multiple timestamp formats | Parse all 4 known formats, drop unparseable |
| Malformed product IDs (`ELEC1002`) | Auto-repair by inserting hyphen |
| Negative quantities | Take absolute value |
| Zero quantities | Drop row (no sale occurred) |
| Zero / negative unit price | Set to `NULL`, flag as `MISSING_PRICE` |
| Discount > 1 (e.g. `25`) | Normalise to decimal (`0.25`) |
| Missing `payment_type` | Impute `"unknown"` |
| Missing `customer_id` | Impute `"GUEST"` |
| Missing `discount_pct` | Impute `0.0` |
| Invalid `category` | Flag as `"Unknown"` |
| Unrecognised `branch_id` | Flag as `UNKNOWN_BRANCH` |

### Derived columns
| Column | Derivation |
|---|---|
| `gross_amount` | `unit_price × quantity` |
| `net_amount` | `gross_amount × (1 − discount_pct)` |
| `is_discounted` | `discount_pct > 0` |
| `sale_date` | Date part of timestamp |
| `sale_hour` | Hour of transaction |
| `day_of_week` | Monday … Sunday |
| `week_number` | ISO week |
| `quality_flag` | OK / BAD_PRODUCT_ID / MISSING_PRICE / … |

---

## 🗄️ Database Schema (SQLite)

### `sales_fact`
The primary fact table – one row per transaction.

### `daily_branch_summary`
Pre-aggregated daily KPIs per branch:
- `total_transactions`, `total_units`, `gross_revenue`, `net_revenue`, `avg_basket_size`

### `pipeline_runs`
Audit log of every pipeline execution with status, row counts, and timing.

---

## 📊 Output Files (per run date)

| File | Description |
|---|---|
| `sales_clean_YYYYMMDD.csv` | Full cleaned fact table |
| `summary_by_branch_*.csv` | Revenue & volume by store |
| `summary_by_category_*.csv` | Sales breakdown by product category |
| `summary_by_product_*.csv` | Top-20 products by revenue |
| `summary_by_hour_*.csv` | Hourly transaction volume |
| `qa_flags_*.csv` | Rows that received a non-OK quality flag |

---

## 🧪 Test Coverage

| Test Class | What's Tested |
|---|---|
| `TestExtractor` | File discovery, schema validation, missing-dir handling |
| `TestTransformer` | All 10 cleaning rules, derived columns, QA flags |
| `TestLoader` | SQLite table creation, row count, CSV output, summaries |
| `TestIntegration` | Full end-to-end pipeline with ephemeral temp directory |


