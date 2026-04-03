#!/usr/bin/env python3
import os
import sys
import uuid
import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# allow importing DB_CREDS from your project layout
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
try:
    from constants import DB_CREDS
except Exception:
    DB_CREDS = None

# third-party cot helper
import cot_reports as cot

# ---------- CONFIG ----------
load_dotenv()

JOB_NAME = "cot_ingestion_csv_to_db_workflow"
run_id = str(uuid.uuid4())

# setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DB connection values (fall back to environment if DB_CREDS not available)
if DB_CREDS:
    DB_USER = DB_CREDS.USER
    DB_PASS = DB_CREDS.PASSWORD
    DB_HOST = DB_CREDS.HOST
    DB_PORT = DB_CREDS.PORT
    DB_NAME = DB_CREDS.DATABASE
else:
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# ---------- helper functions ----------

def fetch_commodity_type(commodity_type_id=1):
    q = """
    SELECT commodity_type
    FROM staging_tables.commodity_type
    WHERE commodity_type_id = :ctid
    """
    with engine.begin() as conn:
        row = conn.execute(text(q), {'ctid': commodity_type_id}).fetchone()
        return row[0] if row else None

def fetch_report_types_from_metadata(source_id=4):
    """Fetch rows from staging_tables.reports_metadata where source_id = 4 and parse description to extract report type names."""
    q = """
    SELECT id, report_id, report_url, report_date, report_as_at, source_id, description
    FROM staging_tables.reports_metadata
    WHERE source_id = :sid
    """
    report_types = []
    with engine.begin() as conn:
        rows = conn.execute(text(q), {'sid': source_id}).fetchall()
    for r in rows:
        desc = r['description'] if isinstance(r, dict) and 'description' in r else (r[6] if len(r) > 6 else None)
        if not desc:
            continue
        parts = [p.strip().lower() for p in __import__('re').split('[,;\n\r/\\|]+', desc) if p.strip()]
        for p in parts:
            if len(p) < 3:
                continue
            report_types.append(p)
    seen = set()
    out = []
    for t in report_types:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


# ---------- helper functions ----------

def clean_column_names(df):
    """Clean and standardize DataFrame column names to match the DDL."""
    # This dictionary maps specific old names to new, cleaned ones
    name_map = {
        'as_of_date_in_form_yyyy-mm-dd': 'as_of_date_in_form_yyyy_mm_dd',
        '%_of_open_interest_(oi)_(all)': 'pct_of_open_interest_oi_all',
        '%_of_oi-noncommercial-long_(all)': 'pct_of_oi_noncommercial_long_all',
        '%_of_oi-noncommercial-short_(all)': 'pct_of_oi_noncommercial_short_all',
        '%_of_oi-noncommercial-spreading_(all)': 'pct_of_oi_noncommercial_spreading_all',
        '%_of_oi-commercial-long_(all)': 'pct_of_oi_commercial_long_all',
        '%_of_oi-commercial-short_(all)': 'pct_of_oi_commercial_short_all',
        '%_of_oi-total_reportable-long_(all)': 'pct_of_oi_total_reportable_long_all',
        '%_of_oi-total_reportable-short_(all)': 'pct_of_oi_total_reportable_short_all',
        '%_of_oi-nonreportable-long_(all)': 'pct_of_oi_nonreportable_long_all',
        '%_of_oi-nonreportable-short_(all)': 'pct_of_oi_nonreportable_short_all',
        '%_of_open_interest_(oi)(old)': 'pct_of_open_interest_oi_old',
        '%_of_oi-noncommercial-long_(old)': 'pct_of_oi_noncommercial_long_old',
        '%_of_oi-noncommercial-short_(old)': 'pct_of_oi_noncommercial_short_old',
        '%_of_oi-noncommercial-spreading_(old)': 'pct_of_oi_noncommercial_spreading_old',
        '%_of_oi-commercial-long_(old)': 'pct_of_oi_commercial_long_old',
        '%_of_oi-commercial-short_(old)': 'pct_of_oi_commercial_short_old',
        '%_of_oi-total_reportable-long_(old)': 'pct_of_oi_total_reportable_long_old',
        '%_of_oi-total_reportable-short_(old)': 'pct_of_oi_total_reportable_short_old',
        '%_of_oi-nonreportable-long_(old)': 'pct_of_oi_nonreportable_long_old',
        '%_of_oi-nonreportable-short_(old)': 'pct_of_oi_nonreportable_short_old',
        '%_of_open_interest_(oi)_(other)': 'pct_of_open_interest_oi_other',
        '%_of_oi-noncommercial-long_(other)': 'pct_of_oi_noncommercial_long_other',
        '%_of_oi-noncommercial-short_(other)': 'pct_of_oi_noncommercial_short_other',
        '%_of_oi-noncommercial-spreading_(other)': 'pct_of_oi_noncommercial_spreading_other',
        '%_of_oi-commercial-long_(other)': 'pct_of_oi_commercial_long_other',
        '%_of_oi-commercial-short_(other)': 'pct_of_oi_commercial_short_other',
        '%_of_oi-total_reportable-long_(other)': 'pct_of_oi_total_reportable_long_other',
        '%_of_oi-total_reportable-short_(other)': 'pct_of_oi_total_reportable_short_other',
        '%_of_oi-nonreportable-long_(other)': 'pct_of_oi_nonreportable_long_other',
        '%_of_oi-nonreportable-short_(other)': 'pct_of_oi_nonreportable_short_other',
        'cftc_contract_market_code_(quotes)': 'cftc_contract_market_code_quotes',
        'cftc_market_code_in_initials_(quotes)': 'cftc_market_code_in_initials_quotes',
        'cftc_commodity_code_(quotes)': 'cftc_commodity_code_quotes',
    }
    
    # First, apply a general cleaning function
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_').str.replace('(', '').str.replace(')', '')
    
    # Then, apply specific renames
    df = df.rename(columns=name_map)
    return df

# ---------- main flow ----------

def main():
    logger.info(f"Starting job {JOB_NAME} run_id={run_id}")
    
    commodity_type_val = fetch_commodity_type(commodity_type_id=1)
    if not commodity_type_val:
        logger.error('commodity_type not found for commodity_type_id=1')
        return
    logger.info(f'Using commodity_type: {commodity_type_val}')

    report_types = fetch_report_types_from_metadata(source_id=4)
    if not report_types:
        logger.error('No report types found in reports_metadata for source_id=4')
        return
    logger.info(f'Found report types: {report_types}')

    # Define the target table schema and name
    schema, tbl = 'prod', 'cftc_cot_report_legacy'
    
    try:
        # Truncate the table before starting to ingest
        logger.info(f"Connecting to database and truncating table {schema}.{tbl}...")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{tbl}"))
        
        # Loop through each report type
        for rt in report_types:
            logger.info(f'Fetching report: {rt} from API...')
            df = cot.cot_all(cot_report_type=rt)

            if df is None or df.empty:
                logger.warning(f'cot.cot_all returned empty for {rt}, skipping')
                continue

            # Define output CSV file name
            out_file = f"cot_{rt}_2025.csv"

            # Save the raw data to CSV
            df.to_csv(out_file, index=False)
            logger.info(f"Data for {rt} saved to {out_file}")

            # Now, read from the newly created CSV file
            logger.info(f"Reading data back from {out_file}...")
            df = pd.read_csv(out_file, nrows=10000)
            
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
            
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
            
            # Ensure consistent as_of_date column name
            if 'as_of_date' in df.columns:
                df['as_of_date'] = pd.to_datetime(df['as_of_date'], errors='coerce').dt.date
                df = df.rename(columns={'as_of_date': 'as_of_date_in_form_yyyy_mm_dd'})
            
                # filter for 2025 only
                df = df[df['as_of_date_in_form_yyyy_mm_dd'].apply(lambda d: d.year == 2025 if pd.notnull(d) else False)]
            
            df['commodity_type'] = commodity_type_val
            df['report_type'] = rt
            
            # Clean and normalize column names to match the database DDL
            if rt in ('legacy_fut', 'legacy_futopt'):
                print(f"Cleaning column names for report type {rt}...")
                df = clean_column_names(df)
            
            # Convert date column to datetime object for proper storage
            if 'as_of_date_in_form_yyyy_mm_dd' in df.columns:
                df['as_of_date_in_form_yyyy_mm_dd'] = pd.to_datetime(df['as_of_date_in_form_yyyy_mm_dd'], errors='coerce').dt.date
                
            # Ingest data into the database, appending after the initial truncate
            logger.info(f"Ingesting data for {rt} into {schema}.{tbl}...")
            with engine.begin() as conn:
                df.to_sql(tbl, conn, schema=schema, if_exists='append', index=False, chunksize=30000)
            
            logger.info(f"Successfully ingested {len(df)} rows for {rt}.")
            
    except SQLAlchemyError as db_err:
        logger.exception(f'DB error during ingestion: {db_err}')
    except Exception as e:
        logger.exception(f'General error during data processing: {e}')

if __name__ == '__main__':
    main()
