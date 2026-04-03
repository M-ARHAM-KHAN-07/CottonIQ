# 🌾 CottonIQ — Market Intelligence Platform

> An end-to-end automated system that collects global cotton market data from four distinct sources, trains XGBoost prediction models on that data, and surfaces everything through a live Streamlit analytics dashboard — all wired to a production PostgreSQL database on AWS RDS.

---

## What This Project Does

CottonIQ is a full-stack data and ML platform for cotton commodity markets. It has three layers:

1. **Data Pipeline** — Four automated scrapers pull from a REST API, PDF documents, a JavaScript-rendered web platform, and government report files, loading everything into a multi-schema PostgreSQL database daily.

2. **ML Models** — Two XGBoost regression models (price prediction + open interest prediction) were trained on the collected data, with all features hand-engineered from domain knowledge of how cotton market signals behave across time horizons.

3. **Streamlit Dashboard** — A five-view interactive dashboard connects live to the database and presents pipeline data, COT positioning, yarn prices, Cotlook index data, and model predictions in one place.

---

## System Overview

```
┌─────────────────────────────────────────────────┐
│                  DATA SOURCES                    │
│  USDA FAS API · Cotlook PDFs · CCF Web · CFTC   │
└──────────────────────┬──────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│               TRANSFORM LAYER                    │
│  Marketing year calc · Week numbers · Column     │
│  normalisation · Grand total aggregation ·       │
│  Holiday-aware FND · XGBoost feature engineering │
└──────────────────────┬──────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│            PostgreSQL (AWS RDS)                  │
│  staging schema  ──→  prod schema               │
│  Incremental table · Main table · COT · CCF      │
└──────────────────────┬──────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│           ANALYTICS & PREDICTIONS                │
│  8-CTE Materialized View · XGBoost Models        │
└──────────────────────┬──────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────┐
│           STREAMLIT DASHBOARD                    │
│  5 views: Price pred · OI pred · Export sales   │
│  COT positioning · Yarn & Cotlook index prices   │
└─────────────────────────────────────────────────┘
```

---

## Machine Learning

### Two XGBoost Models

Both models were trained entirely on data this pipeline collects. All features were engineered manually — no automated feature selection. Each feature group reflects a deliberate hypothesis about what drives cotton price movements and market participation.

#### Model 1 — Cotton Price Prediction
Predicts the Cotlook A Index price one week ahead.

#### Model 2 — Open Interest Prediction
Predicts cotton futures open interest, capturing expected changes in market participation.

---

### Feature Engineering

All features were built by hand across three groups:

**Time-Series Features**
- Lagged price values: t-1, t-2, t-4, t-8 weeks
- Lagged open interest: t-1, t-2, t-4 weeks
- Rolling averages: 4-week, 8-week, 12-week
- Week-over-week absolute and percentage change

**Market Positioning Features (from CFTC COT data)**
- Net commercial position (hedgers)
- Net non-commercial position (speculators)
- Gross long and gross short contracts
- Spreading positions
- % of open interest by trader category
- Week-over-week positioning change

**Fundamental & Seasonal Features**
- USDA CMY net sales (weekly)
- USDA accumulated exports (cumulative shipments)
- Cotlook A Index level (as input to OI model)
- Marketing year week number (captures seasonality within the Aug–Jul crop year)
- Calendar month
- Marketing year start / end proximity flag

**Why these features?**

Cotton is a deeply seasonal market. The USDA marketing year runs August–July, and export sales commitments in early weeks are a leading indicator of final shipment volumes. COT positioning data captures the speculative vs commercial balance, which historically leads price moves. Lagged values and rolling averages allow the model to learn from trend and momentum — not just point-in-time observations.

---

## Streamlit Dashboard

The dashboard has five views, all reading live from PostgreSQL:

| View | Data Source | Description |
|---|---|---|
| Price Predictions | XGBoost model + Cotlook DB | Actual vs. predicted Cotlook A Index over time |
| Open Interest Forecast | XGBoost model + CFTC DB | Weekly OI with model forecast overlay |
| Export Sales Pipeline | USDA materialized view | Net sales, accumulated exports, Shipment % of Final KPI |
| COT Positioning | CFTC COT DB | Net long / short bars by trader category |
| Yarn & Cotlook Prices | CCF historical + Cotlook DB | 70+ yarn price series + index prices, filterable |

---

## Data Pipeline Components

### 1. `usda_cotton_pipeline.py` — USDA FAS API

Fetches weekly U.S. cotton export sales from the USDA Foreign Agricultural Service API.

- 8 commodity codes (All Upland Cotton, Am Pima, Cottonseed, etc.) across two marketing years
- Compares API latest date vs. DB latest date — skips run if already up to date
- Calculates marketing year, next marketing year, and week number per record
- Incremental load: lands in staging → validates → promotes to main table
- Computes GRAND TOTAL rows for Am Pima and All Upland Cotton
- Refreshes the 8-CTE materialized view as the final step

**Marketing year logic:** Year starts August 1. Week 1 = first Thursday on or after August 1.

---

### 2. `cotlook_pdf_parser.py` — Adaptive PDF Extractor

Extracts cotton price data from Cotlook's market report PDFs — handling documents going back to 2006 without manual configuration.

- Auto-detects PDF structure: `single_year`, `dual_year`, `cif_europe`, `dual_index_system`, `multi_index_format`
- Routes each document to the correct extraction function
- Parses price, change, and shipment month for 20+ cotton growths
- Excludes long-staple varieties from the compositions table
- Produces two DataFrames: `compositions` and `other` (China Cotton Index, KCA Spot, CEPEA, etc.)
- Cleans space-separated numbers (`1 432` → `1432.0`) and validates shipment code format

---

### 3. `ccf_scraper.py` — Selenium Web Bot

Automates the CCF Group platform to extract global yarn price data.

- Headless Chrome (Firefox fallback) on Ubuntu
- Authenticates, navigates to ECharts dashboards, extracts 70+ yarn series via JS injection
- Maps raw feature names to DB column names
- Upserts to `prod.prices_ccf_historical` (INSERT new dates, UPDATE existing)
- Chrome → Firefox fallback, virtual display (`pyvirtualdisplay`), emergency JSON save on crash
- All execution steps logged to `staging_tables.cron_jobs_logs`

---

### 4. `cot_ingestion.py` — CFTC COT Reports

Ingests Commitments of Traders reports from the CFTC.

- Fetches legacy and disaggregated report types from a metadata table
- Normalises 30+ column name variants (e.g. `%_of_oi-noncommercial-long_(all)` → `pct_of_oi_noncommercial_long_all`)
- Filters for current-year records and bulk-loads to `prod.cftc_cot_report_legacy`

---

### 5. `contract_codes.py` — Futures Contract Metadata

- Calculates First Notice Dates for the next 6 active cotton futures contracts
- Accounts for US federal holidays (MLK Day, Presidents Day, Memorial Day, Labor Day, Columbus Day, Veterans Day, Thanksgiving, Christmas, New Year's)
- Inserts only net-new contracts

---

## Database Design

### Schema Layout
```
cotton-trade-db (AWS RDS)
├── staging
│   ├── Cotton_Exports_new                      ← main weekly export table
│   ├── cotton_exports_incremental              ← landing zone
│   ├── ExportSalesGrandTotalDataByCommodity    ← Am Pima + Upland totals
│   ├── cotton_export_summary_latest_week       ← materialized view
│   ├── contract_codes
│   └── cron_jobs_logs
└── prod
    ├── prices_ccf_historical                   ← 70-column yarn price history
    └── cftc_cot_report_legacy
```

### Incremental Load Pattern
```sql
-- 1. Clear landing zone
DELETE FROM staging.cotton_exports_incremental;

-- 2. Load latest week only
INSERT INTO staging.cotton_exports_incremental ...

-- 3. Calculate grand totals
INSERT INTO staging.ExportSalesGrandTotalDataByCommodity ...

-- 4. Promote to main (safe — no duplicates)
INSERT INTO staging.Cotton_Exports_new ON CONFLICT DO NOTHING;

-- 5. Refresh analytics layer
REFRESH MATERIALIZED VIEW staging.cotton_export_summary_latest_week;
```

### Materialized View — 8-CTE Analytics Layer

```sql
WITH
  latest_data_per_commodity AS (   -- deduplicate by marketing year
    SELECT *, ROW_NUMBER() OVER (PARTITION BY weekEndingDate, "Marketing Year",
    commodity_name ORDER BY market_year DESC) AS rn
    FROM staging."ExportSalesGrandTotalDataByCommodity"
    WHERE country = 'GRAND TOTAL'
  ),
  filtered_data            AS (...),   -- apply commodity filter
  latest_marketing_year    AS (...),   -- find current MY
  latest_week_number       AS (...),   -- find current week
  aggregated_data          AS (...),   -- sum CMY/NMY net sales, shipments
  max_weeks                AS (...),   -- each year's final week
  complete_years           AS (...),   -- years with >= 50 weeks
  historical_average       AS (...),   -- 5-yr avg final shipments (2019–2024)
  final_shipments          AS (        -- actual or avg final
    CASE WHEN max_week >= 50 THEN actual ELSE 5yr_average END
  )
SELECT
  shipments / final_shipments * 100 AS "Shipment % of Final",
  "CMY Net Sales", "NMY Net Sales", "CMY Outstanding Sales", ...
```

---

## Tech Stack

| Category | Tools |
|---|---|
| Language | Python 3.x |
| ML | XGBoost, scikit-learn |
| Dashboard | Streamlit, Plotly |
| HTTP / APIs | `requests` |
| PDF Parsing | `pdfplumber`, `re` |
| Web Automation | `selenium`, Chrome/Firefox, `pyvirtualdisplay` |
| Data Processing | `pandas`, `numpy` |
| Database | PostgreSQL (AWS RDS eu-north-1) |
| DB Drivers | `psycopg2`, `SQLAlchemy` |
| Scheduling | Cron (Linux) |
| Logging | Python `logging`, `staging_tables.cron_jobs_logs` |
| COT Data | `cot_reports` library |
| Calendar Logic | `holidays`, `pandas.tseries.offsets.BDay` |

---

## Project Structure

```
cottoniq/
├── pipelines/
│   ├── usda_cotton_pipeline.py     # USDA FAS API · incremental load · mat. view refresh
│   ├── cotlook_pdf_parser.py       # Adaptive PDF extractor · 5 structure types
│   ├── ccf_scraper.py              # Selenium bot · ECharts JS · 70+ yarn features
│   ├── cot_ingestion.py            # CFTC COT reports · column normalisation · bulk load
│   └── contract_codes.py           # Futures contract metadata · holiday-aware FND
├── models/
│   ├── feature_engineering.py      # Lag, rolling, COT, seasonal feature construction
│   ├── price_model.py              # XGBoost price prediction · training + inference
│   ├── open_interest_model.py      # XGBoost OI prediction · training + inference
│   └── saved/                      # Serialised model artefacts (.json / .pkl)
├── dashboard/
│   ├── app.py                      # Streamlit entry point · 5 views
│   └── views/
│       ├── price_predictions.py    # Actual vs predicted price chart
│       ├── oi_predictions.py       # Open interest forecast chart
│       ├── export_sales.py         # USDA pipeline + Shipment % KPI
│       ├── cot_report.py           # COT positioning visualisation
│       └── yarn_cotlook.py         # CCF yarn + Cotlook index charts
├── sql/
│   ├── materialized_view.sql       # 8-CTE analytics view DDL
│   ├── schema_staging.sql
│   └── schema_prod.sql
├── .env.example
├── requirements.txt
└── README.md
```

---

## Key Design Decisions

**Why hand-engineer all features instead of using automated selection?**
Cotton markets have domain-specific structure — the USDA marketing year, the role of COT positioning as a leading indicator, the way export commitments accumulate. Automated feature selection doesn't know any of this. Manual engineering lets each feature carry an explicit hypothesis that can be tested and explained.

**Why two separate models instead of one multi-output model?**
Price and open interest respond to different drivers and have different lag structures. Keeping them separate makes each model easier to tune, interpret, and retrain independently when new data arrives.

**Why an incremental staging table?**
The pipeline can fail mid-run without corrupting production data. Only after full validation does data get promoted to the main table.

**Why materialise the view instead of querying live?**
The 8-CTE query joins several large tables and computes rolling averages. Materialising it means dashboard queries are instant — the heavy work happens once per pipeline run.

**Why Chrome → Firefox fallback?**
Production runs on Ubuntu servers where Chrome availability isn't guaranteed. The scraper keeps running regardless.

---

*Built as a production data and ML platform for cotton commodity market analysis.*
