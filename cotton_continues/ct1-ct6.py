import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import holidays
import re
from bs4 import BeautifulSoup
import tempfile
import shutil
import time
import psycopg2
import uuid
import json
import logging
import pytz

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager

from sqlalchemy import create_engine, text

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# === DB CONFIG ===
DB_HOST = ""
DB_PORT = ""
DB_NAME = ""
DB_USER = ""
DB_PASS = ""
STAGING_TABLE = ""
SCHEMA_NAME = ""

def get_engine():
    """Create and return a SQLAlchemy engine instance."""
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
        raise

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# === Logging Class ===
class CronJobLogger:
    def __init__(self, job_name):
        self.job_name = job_name
        self.run_id = uuid.uuid4()
        self.run_label = f"run_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}_{job_name.lower()}"
        self.engine = get_engine()

    def log_job_execution(self, status, level, details, message=None):
        """Log job execution details to staging.cron_job_logs"""
        if not message:
            message = self.job_name

        log_details = {
            "run_label": self.run_label,
            "message": details
        }
        details_json = json.dumps(log_details)
        log_entry = [{
            "job_name": self.job_name,
            "status": status,
            "level": level,
            "message": message,
            "timestamp": datetime.now(pytz.UTC),
            "run_id": str(self.run_id),
            "script_path": "ct6_enhanced_pipeline.py",
            "details": details_json
        }]
        try:
            pd.DataFrame(log_entry).to_sql(
                'cron_job_logs',
                schema='staging',
                con=self.engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Logged {self.job_name} to cron_job_logs: {status} - {details}")
        except Exception as e:
            logger.error(f"Failed to log to cron_job_logs: {str(e)}", exc_info=True)

# === WEBDRIVER SETUP ===
URL = "https://futures.tradingcharts.com/marketquotes/CT.html"

def setup_webdriver(browser='chrome'):
    """Setup WebDriver with fallback options"""
    options = None
    service = None

    if browser == 'chrome':
        options = ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        try:
            service = ChromeService(ChromeDriverManager().install())
            logger.info("Using Chrome WebDriver.")
            return webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.warning(f"Chrome setup failed: {e}. Falling back to Firefox.")
            return setup_webdriver(browser='firefox')

    elif browser == 'firefox':
        options = FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        try:
            service = FirefoxService(GeckoDriverManager().install())
            logger.info("Using Firefox WebDriver.")
            return webdriver.Firefox(service=service, options=options)
        except Exception as e:
            logger.error(f"Firefox setup failed: {e}. No usable browser found.")
            return None

    else:
        logger.error(f"Browser '{browser}' not supported.")
        return None

# === Open Interest Scraping ===
month_map = {
    "Jan": "F", "Feb": "G", "Mar": "H", "Apr": "J", "May": "K", "Jun": "M",
    "Jul": "N", "Aug": "Q", "Sep": "U", "Oct": "V", "Nov": "X", "Dec": "Z"
}

def convert_to_ct_symbol(contract_str):
    match = re.match(r"([A-Za-z]+)'(\d{2})", contract_str)
    if not match:
        return None
    month_abbr, year_suffix = match.groups()
    month_code = month_map.get(month_abbr)
    return f"CT{month_code}{year_suffix}" if month_code else None

def scrape_open_interest():
    """Scrape open interest data for all contracts"""
    temp_profile = tempfile.mkdtemp()
    
    driver = setup_webdriver()
    if driver is None:
        logger.error("Failed to initialize any WebDriver.")
        return pd.DataFrame()

    try:
        driver.get(URL)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        rows = soup.find_all("table")[2].find_all("tr")

        data_rows = []
        for row in rows[2:]:
            cells = row.find_all("td")
            if len(cells) < 11:
                continue

            contract = cells[0].text.strip()
            if not contract or "Oct" in contract or not any(contract.endswith(x) for x in ["'25", "'26", "'27"]):
                continue

            symbol = convert_to_ct_symbol(contract)
            if not symbol:
                continue

            open_int = cells[-2].text.strip().replace(",", "")
            if open_int == "#######":
                open_int = None
            else:
                open_int = int(open_int) if open_int.isdigit() else None

            data_rows.append((symbol, open_int))

        df = pd.DataFrame(data_rows, columns=["Contract", "Open Interest"])
        return df

    except Exception as e:
        logger.error("Scraping failed:", exc_info=True)
        return pd.DataFrame()
    finally:
        if driver:
            driver.quit()
        shutil.rmtree(temp_profile, ignore_errors=True)

# === Date and Contract Management ===
us_holidays = holidays.US(years=range(2020, 2035))
MONTH_CODES = {"H": 3, "K": 5, "N": 7, "Z": 12}
YEARS = [2025, 2026, 2027]

def get_first_business_day(year, month):
    for day in range(1, 8):
        date = datetime(year, month, day).date()
        if date.weekday() < 5 and date not in us_holidays:
            return date
    return None

def get_fnd(year, month):
    first_biz_day = get_first_business_day(year, month)
    if not first_biz_day:
        return None
    fnd = first_biz_day
    days_counted = 0
    while days_counted < 5:
        fnd -= timedelta(days=1)
        if fnd.weekday() < 5 and fnd not in us_holidays:
            days_counted += 1
    return fnd

def fetch_yahoo(symbol, start, end):
    try:
        data = yf.Ticker(symbol).history(start=start, end=end)
        if not data.empty:
            data = data[["Open", "High", "Low", "Close", "Volume"]]
            data["Symbol"] = symbol
            data["Date"] = data.index.date
            
            # Round prices and handle volume
            data["Open"] = data["Open"].round(2)
            data["High"] = data["High"].round(2)
            data["Low"] = data["Low"].round(2)
            data["Close"] = data["Close"].round(2)
            data["Volume"] = data["Volume"].fillna(0).astype(int)

            return data.reset_index(drop=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Yahoo fetch error for {symbol}: {e}")
        return pd.DataFrame()

def get_active_contracts(target_date, max_contracts=6):
    """Get active contracts sorted by FND"""
    contracts = []
    for year in YEARS:
        for code, month in MONTH_CODES.items():
            symbol = f"CT{code}{str(year)[2:]}.NYB"
            try:
                fnd = get_fnd(year, month)
                if fnd and fnd > target_date:
                    contracts.append((symbol, fnd))
            except:
                continue
    return sorted(contracts, key=lambda x: x[1])[:max_contracts]

def get_ct6_contract(target_date):
    """Get the CT6 contract (6th in the series)"""
    contracts = get_active_contracts(target_date, max_contracts=6)
    if len(contracts) >= 6:
        return contracts[5]  # 6th contract (index 5)
    return None

def is_business_day(date):
    return date.weekday() < 5 and date not in us_holidays

# === CT6 Data Processing ===
def process_ct6_data(job_logger):
    """Process today's CT6 data and insert into staging table"""
    today = datetime.today().date()
    start_date = today.strftime("%Y-%m-%d")
    end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Processing CT6 data for today: {today}")

    ct6_contract = get_ct6_contract(today)
    if not ct6_contract:
        raise Exception("No active CT6 contract found")

    symbol, fnd = ct6_contract
    month_letter_to_number = {"H": 3, "K": 5, "N": 7, "Z": 12}

    df = fetch_yahoo(symbol, start_date, end_date)
    if df.empty:
        raise Exception(f"No Yahoo data for CT6 contract {symbol}")

    # Extract contract details
    code = symbol.replace(".NYB", "")
    month_letter = code[2]
    year = 2000 + int(code[3:])
    month = month_letter_to_number[month_letter]
    fnd_date = get_fnd(year, month)

    # Filter for today's data only
    df = df[df["Date"] == today].copy()
    if df.empty:
        raise Exception(f"No data for today ({today}) for CT6 contract {symbol}")

    # Rename columns to match database schema
    df = df.rename(columns={
        "Open": "open",
        "High": "high", 
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Date": "Date"
    })

    # Add CT6-specific columns
    df["Open Interest"] = None
    df["fnd"] = fnd_date
    df["D-FND"] = (fnd_date - pd.to_datetime(df["Date"]).dt.date).apply(lambda x: x.days)
    df["month"] = month
    df["year"] = year
    df["Contract Code"] = code
    df["contract"] = "ct6"

    # Reorder columns to match schema
    df = df[["Date", "open", "high", "low", "close", "volume", "Open Interest",
             "fnd", "D-FND", "month", "year", "Contract Code", "contract"]]

    # Insert into staging table
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Clear existing CT6 data for today
            cur.execute(f'DELETE FROM {SCHEMA_NAME}."{STAGING_TABLE}" WHERE "Date" = %s AND contract = %s', 
                       (today, 'ct6'))
            
            # Insert new data
            cols = list(df.columns)
            values = [tuple(x) for x in df.to_numpy()]
            
            insert_sql = f"""
            INSERT INTO {SCHEMA_NAME}."{STAGING_TABLE}" ({",".join([f'"{c}"' for c in cols])})
            VALUES %s
            """
            
            from psycopg2.extras import execute_values
            execute_values(cur, insert_sql, values)
            conn.commit()
            
            job_logger.log_job_execution(
                status="PROGRESS",
                level="INFO",
                details=f"Inserted CT6 data for {today} into staging table. Contract: {code}"
            )
            logger.info(f"Inserted CT6 data for {today} into staging table")
            
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to insert CT6 data: {str(e)}")
    finally:
        conn.close()

    return df

def transform_staging_to_historical():
    """Transform staging data to historical table format with all calculations"""
    logger.info("Transforming staging data to historical table format")
    
    conn = get_connection()
    try:
        # Get today's data from staging
        today = datetime.today().date()
        
        with conn.cursor() as cur:
            cur.execute(f'''
                SELECT * FROM {SCHEMA_NAME}."{STAGING_TABLE}" 
                WHERE "Date" = %s AND contract = 'ct6'
            ''', (today,))
            
            rows = cur.fetchall()
            if not rows:
                raise Exception("No CT6 data found in staging table for today")
            
            # Get column names
            cur.execute(f'''
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{STAGING_TABLE}'
                ORDER BY ordinal_position
            ''')
            columns = [row[0] for row in cur.fetchall()]
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # Transform to historical format - this is a simplified version
            # You'll need to add the specific calculations from the third code
            historical_df = pd.DataFrame()
            
            # Basic columns
            historical_df["Date"] = df["Date"]
            
            # CT6 specific columns (assuming CT6 maps to the 6th position in historical)
            historical_df["CT6 Open"] = df["open"]
            historical_df["CT6 High"] = df["high"]
            historical_df["CT6 Low"] = df["low"]
            historical_df["CT6 Close"] = df["close"]
            historical_df["CT6 Volume"] = df["volume"]
            historical_df["CT6 Open Interest"] = df["Open Interest"]
            historical_df["CT6 FND"] = df["fnd"]
            historical_df["CT6 D-FND"] = df["D-FND"]
            historical_df["CT6 Month"] = df["month"]
            historical_df["CT6 Year"] = df["year"]
            historical_df["CT6 Contract Code"] = df["Contract Code"]
            
            # Add placeholder columns for CT1-CT5 (you may need to fetch this data)
            for i in range(1, 6):
                for col in ["Open", "High", "Low", "Close", "Volume", "Open Interest", 
                           "FND", "D-FND", "Month", "Year", "Contract Code"]:
                    historical_df[f"CT{i} {col}"] = None
                    
            # Add spread calculations (placeholders - will be NULL until CT1-CT5 data is available)
            historical_df["CT1/CT2 Open"] = None
            historical_df["CT1/CT2 Close"] = None
            historical_df["CT1/CT2 High"] = None
            historical_df["CT1/CT2 Low"] = None
            
            return historical_df
            
    except Exception as e:
        raise Exception(f"Failed to transform staging data: {str(e)}")
    finally:
        conn.close()

def insert_into_historical_table(df, job_logger):
    """Insert transformed data into historical table"""
    logger.info("Inserting data into historical table")
    
    # Find the correct historical table name
    historical_table = get_historical_table_name()
    if not historical_table:
        raise Exception("No historical table found")
    
    try:
        engine = get_engine()
        df.to_sql(historical_table, engine, schema=SCHEMA_NAME, if_exists="append", index=False)
        
        job_logger.log_job_execution(
            status="PROGRESS",
            level="INFO",
            details=f"Inserted today's data into historical table {historical_table}"
        )
        logger.info(f"Successfully inserted data into {historical_table}")
        
    except Exception as e:
        raise Exception(f"Failed to insert into historical table: {str(e)}")

def update_yesterday_open_interest(job_logger):
    """Update yesterday's open interest in historical table"""
    yesterday = (datetime.today().date() - BDay(1)).date()
    logger.info(f"Updating open interest for yesterday: {yesterday}")
    
    # Get yesterday's open interest
    open_interest_df = scrape_open_interest()
    if open_interest_df.empty:
        job_logger.log_job_execution(
            status="WARNING",
            level="WARN",
            details="No open interest data scraped"
        )
        return
    
    # Find historical table
    historical_table = get_historical_table_name()
    if not historical_table:
        raise Exception("No historical table found")
    
    # Get active contracts for yesterday
    ct_metadata = get_active_contracts(yesterday, max_contracts=6)
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            yesterday_str = yesterday.strftime("%Y-%m-%d")
            
            # Check if yesterday's data exists
            cur.execute(f'SELECT COUNT(*) FROM {SCHEMA_NAME}."{historical_table}" WHERE "Date"::text = %s', 
                       (yesterday_str,))
            if cur.fetchone()[0] == 0:
                job_logger.log_job_execution(
                    status="WARNING",
                    level="WARN",
                    details=f"No data found for {yesterday} in historical table"
                )
                return
            
            # Update open interest for each contract
            for i, (symbol, fnd) in enumerate(ct_metadata, start=1):
                code = symbol.replace(".NYB", "")
                
                if code in open_interest_df["Contract"].values:
                    oi_value = open_interest_df.loc[
                        open_interest_df["Contract"] == code, "Open Interest"
                    ].values[0]
                    
                    update_sql = f'''
                        UPDATE {SCHEMA_NAME}."{historical_table}"
                        SET "CT{i} Open Interest" = %s
                        WHERE "Date"::text = %s AND "CT{i} Contract Code" = %s
                    '''
                    
                    cur.execute(update_sql, (oi_value, yesterday_str, code))
                    
                    logger.info(f"Updated CT{i} Open Interest for {code}: {oi_value}")
            
            conn.commit()
            
            job_logger.log_job_execution(
                status="PROGRESS",
                level="INFO",
                details=f"Updated open interest for {yesterday} in historical table"
            )
            
    except Exception as e:
        conn.rollback()
        raise Exception(f"Failed to update open interest: {str(e)}")
    finally:
        conn.close()

def get_historical_table_name():
    """Find the correct historical table name"""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            possible_tables = [
                "historical_data_ct1_ct6",
                "ct1_ct6_historical_data"
            ]
            
            for table_name in possible_tables:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    );
                """, (SCHEMA_NAME, table_name))
                
                if cur.fetchone()[0]:
                    logger.info(f"Found historical table: {SCHEMA_NAME}.{table_name}")
                    return table_name
            
            return None
    except Exception as e:
        logger.error(f"Error finding historical table: {e}")
        return None
    finally:
        conn.close()

def main():
    job_logger = CronJobLogger("CT6_ENHANCED_PIPELINE")
    job_logger.log_job_execution(
        status="STARTED",
        level="INFO",
        details="Enhanced CT6 pipeline started - processing today's CT6 data and updating yesterday's open interest"
    )
    
    today = datetime.today().date()
    
    if not is_business_day(today):
        job_logger.log_job_execution(
            status="SKIPPED",
            level="INFO",
            details=f"{today} is not a business day. Skipping run."
        )
        logger.info(f"{today} is not a business day. Skipping run.")
        return

    start_time = datetime.now()

    try:
        # Step 1: Process today's CT6 data and insert into staging
        logger.info("="*60)
        logger.info("STEP 1: PROCESSING TODAY'S CT6 DATA")
        logger.info("="*60)
        
        ct6_data = process_ct6_data(job_logger)

        # Step 2: Transform staging data to historical format
        logger.info("="*60)
        logger.info("STEP 2: TRANSFORMING DATA FOR HISTORICAL TABLE")
        logger.info("="*60)
        
        historical_data = transform_staging_to_historical()

        # Step 3: Insert into historical table
        logger.info("="*60)
        logger.info("STEP 3: INSERTING INTO HISTORICAL TABLE")
        logger.info("="*60)
        
        insert_into_historical_table(historical_data, job_logger)

        # Step 4: Update yesterday's open interest
        logger.info("="*60)
        logger.info("STEP 4: UPDATING YESTERDAY'S OPEN INTEREST")
        logger.info("="*60)
        
        update_yesterday_open_interest(job_logger)

        execution_time = (datetime.now() - start_time).total_seconds()
        job_logger.log_job_execution(
            status="SUCCESS",
            level="INFO",
            details=f"Enhanced CT6 pipeline completed successfully in {execution_time:.2f}s"
        )
        logger.info(f"Pipeline completed successfully in {execution_time:.2f}s!")

    except Exception as e:
        execution_time = (datetime.now() - start_time).total_seconds()
        job_logger.log_job_execution(
            status="FAILURE",
            level="ERROR",
            details=f"Enhanced CT6 pipeline failed after {execution_time:.2f}s: {str(e)}"
        )
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    
if __name__ == "__main__":
    main()
