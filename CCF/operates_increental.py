from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import pandas as pd
import json
import time
import os
import psycopg2
import psycopg2.extras
from datetime import datetime, date
from dotenv import load_dotenv
import platform
import subprocess
import tempfile
import shutil
import logging
import uuid
import pytz
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class UbuntuWebScraper:
    """Ubuntu-compatible web scraper with fallback browser support"""
    
    def __init__(self):
        self.driver = None
        self.wait = None
        self.browser_type = None
        self.virtual_display = None
        self.temp_user_data_dir = None
        self.system_info = {
            'platform': platform.system().lower(),
            'architecture': platform.machine().lower(),
            'is_linux': platform.system().lower() == 'linux'
        }
        
        # Add logging attributes
        self.run_id = uuid.uuid4()
        self.run_label = f"run_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}_ccf_scraper"

    def setup_virtual_display(self):
        """Setup virtual display for headless environments"""
        try:
            if not os.environ.get('DISPLAY') and self.system_info['is_linux']:
                try:
                    from pyvirtualdisplay import Display
                    self.virtual_display = Display(visible=0, size=(1920, 1080))
                    self.virtual_display.start()
                except ImportError:
                    pass
                except Exception as e:
                    pass
        except Exception as e:
            pass

    def check_browser_availability(self):
        """Check which browsers are available on the system"""
        available_browsers = {}
        driver_info = {}
        
        # Check Chrome/Chromium
        chrome_paths = [
            '/usr/bin/google-chrome',
            '/usr/bin/google-chrome-stable',
            '/usr/bin/chromium-browser',
            '/usr/bin/chromium',
            '/snap/bin/chromium',
            '/usr/bin/chrome'
        ]
        
        for path in chrome_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                available_browsers['chrome'] = path
                break
        
        # Check Firefox
        firefox_paths = [
            '/usr/bin/firefox',
            '/usr/bin/firefox-esr',
            '/snap/bin/firefox'
        ]
        
        for path in firefox_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                available_browsers['firefox'] = path
                break
        
        # Check for drivers
        chromedriver_paths = [
            '/usr/bin/chromedriver',
            '/usr/local/bin/chromedriver',
            '/opt/chromedriver',
            './chromedriver'
        ]
        
        for path in chromedriver_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                driver_info['chromedriver'] = path
                break
        
        geckodriver_paths = [
            '/usr/bin/geckodriver',
            '/usr/local/bin/geckodriver',
            '/opt/geckodriver',
            './geckodriver'
        ]
        
        for path in geckodriver_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                driver_info['geckodriver'] = path
                break
        
        return available_browsers, driver_info

    def install_missing_dependencies(self):
        """Install missing browser dependencies on Ubuntu"""
        try:
            subprocess.run(['sudo', 'apt', 'update'], check=True, capture_output=True)
            
            # Install Chrome
            try:
                subprocess.run(['google-chrome', '--version'], check=True, capture_output=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                subprocess.run([
                    'wget', '-q', '-O', '/tmp/google-chrome.deb',
                    'https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb'
                ], check=True)
                subprocess.run(['sudo', 'dpkg', '-i', '/tmp/google-chrome.deb'], check=True)
                subprocess.run(['sudo', 'apt', '-f', 'install', '-y'], check=True)
                
            # Install Firefox
            subprocess.run(['sudo', 'apt', 'install', '-y', 'firefox'], check=True)
            
            # Install ChromeDriver and GeckoDriver
            subprocess.run(['sudo', 'apt', 'install', '-y', 'chromium-chromedriver', 'firefox-geckodriver'], check=True)
            
        except subprocess.CalledProcessError as e:
            pass

    def setup_chrome_driver(self, driver_info):
        """Setup Chrome driver for Ubuntu"""
        try:
            chrome_options = ChromeOptions()
            
            # Create a unique temporary user data directory
            self.temp_user_data_dir = tempfile.mkdtemp()

            # Ubuntu-optimized Chrome options
            chrome_options.add_argument(f"--user-data-dir={self.temp_user_data_dir}")
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Download preferences
            prefs = {
                "download.default_directory": os.path.abspath(os.getcwd() + "/spun_yarn_data"),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True
            }
            chrome_options.add_experimental_option("prefs", prefs)

            # Try to create Chrome driver
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = ChromeService(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except ImportError:
                if 'chromedriver' in driver_info:
                    service = ChromeService(driver_info['chromedriver'])
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                else:
                    self.driver = webdriver.Chrome(options=chrome_options)
            
            self.browser_type = 'chrome'
            return True
            
        except Exception as e:
            return False

    def setup_firefox_driver(self, driver_info):
        """Setup Firefox driver for Ubuntu"""
        try:
            firefox_options = FirefoxOptions()
            
            # Ubuntu-optimized Firefox options
            firefox_options.add_argument("--headless")
            firefox_options.add_argument("--width=1920")
            firefox_options.add_argument("--height=1080")
            firefox_options.add_argument("--user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0")
            
            # Download preferences
            download_path = os.path.abspath(os.getcwd() + "/spun_yarn_data")
            firefox_options.set_preference("browser.download.folderList", 2)
            firefox_options.set_preference("browser.download.manager.showWhenStarting", False)
            firefox_options.set_preference("browser.download.dir", download_path)
            firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
            firefox_options.set_preference("pdfjs.disabled", True)
            
            # Try to create Firefox driver
            try:
                from webdriver_manager.firefox import GeckoDriverManager
                service = FirefoxService(GeckoDriverManager().install())
                self.driver = webdriver.Firefox(service=service, options=firefox_options)
            except ImportError:
                if 'geckodriver' in driver_info:
                    service = FirefoxService(driver_info['geckodriver'])
                    self.driver = webdriver.Firefox(service=service, options=firefox_options)
                else:
                    self.driver = webdriver.Firefox(options=firefox_options)
            
            self.browser_type = 'firefox'
            return True
            
        except Exception as e:
            return False

    def setup_driver(self):
        """Setup the best available WebDriver"""
        try:
            # Terminate existing browser processes to avoid conflicts
            if self.system_info['is_linux']:
                for proc in ['chrome', 'chromedriver', 'firefox', 'geckodriver']:
                    try:
                        subprocess.run(['pkill', '-9', proc], check=True, capture_output=True)
                    except subprocess.CalledProcessError:
                        pass

            # Setup virtual display if needed
            self.setup_virtual_display()
            
            # Check available browsers
            available_browsers, driver_info = self.check_browser_availability()
            
            if not available_browsers:
                self.install_missing_dependencies()
                available_browsers, driver_info = self.check_browser_availability()
            
            # Try Chrome first
            success = False
            if 'chrome' in available_browsers:
                success = self.setup_chrome_driver(driver_info)
            
            # Try Firefox if Chrome failed
            if not success and 'firefox' in available_browsers:
                success = self.setup_firefox_driver(driver_info)
            
            if not success:
                raise Exception("Failed to initialize any WebDriver")
            
            # Configure driver
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(10)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_window_size(1920, 1080)
            
            # Configure WebDriverWait
            self.wait = WebDriverWait(self.driver, 20)
            
            # Test connection
            self.driver.get("data:text/html,<html><body><h1>Test</h1></body></html>")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            raise
        finally:
            # Clean up temporary user data directory if created
            if self.temp_user_data_dir:
                try:
                    shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
                except Exception as e:
                    pass

    def get_database_engine(self):
        """Get SQLAlchemy engine for logging"""
        try:
            db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            return create_engine(db_url)
        except Exception as e:
            return None

    def log_job_execution(self, job_name, status, level, details, message=None):
        """Log job execution details to staging.cron_job_logs"""
        try:
            engine = self.get_database_engine()
            if not engine:
                return
                
            if not message:
                message = job_name

            # Convert details to JSON object
            log_details = {
                "run_label": self.run_label,
                "message": details
            }
            details_json = json.dumps(log_details)

            log_entry = [{
                "job_name": job_name,
                "status": status,
                "level": level,
                "message": message,
                "timestamp": datetime.now(pytz.UTC),
                "run_id": str(self.run_id),  # Convert UUID to string for safety
                "script_path": "ccf_operating_rates_scraper.py",
                "details": details_json  # Store as JSON string
            }]

            pd.DataFrame(log_entry).to_sql(
                'cron_job_logs',
                schema='staging',
                con=engine,
                if_exists='append',
                index=False
            )
        except Exception as e:
            pass

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
                
        if self.virtual_display:
            try:
                self.virtual_display.stop()
            except:
                pass
                
        if self.temp_user_data_dir:
            try:
                shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
            except Exception as e:
                pass

def debug_chart_data(option_data, product_name):
    """Debug function to understand the structure of chart data"""
    pass

# Database configuration with fallback values
DB_CONFIG = {
    'host': os.getenv('REMOTE_DB_HOST', ''),
    'database': os.getenv('REMOTE_DB_NAME', ''),
    'user': os.getenv('REMOTE_DB_USER', ''),
    'password': os.getenv('REMOTE_DB_PASS', ''),
    'port': os.getenv('REMOTE_DB_PORT', '')
}

# Login credentials
LOGIN_CREDENTIALS = {
    'username': os.getenv('CCF_USERNAME', ''),
    'password': os.getenv('CCF_PASSWORD', '')
}

def create_database_connection():
    """Create PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise

def verify_existing_table(conn):
    """Verify the existing table structure and get column information"""
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = 'prod'
    AND table_name = 'oprates_ccf_historical'
    ORDER BY ordinal_position;
""")
        
        columns = cursor.fetchall()
        
        if not columns:
            raise Exception("Table 'oprates_ccf_historical' not found")
        
        return [col[0] for col in columns]
        
    except psycopg2.Error as e:
        logger.error(f"Error checking table structure: {e}")
        raise
    finally:
        cursor.close()

def get_commodity_type(conn):
    """Get commodity type from staging_tables.commodity_type"""
    cursor = conn.cursor()
    try:
        select_query = """
            SELECT commodity_type 
            FROM staging_tables.commodity_type 
            WHERE commodity_type_id = 1
        """
        cursor.execute(select_query)
        result = cursor.fetchone()
        return result[0] if result else None
    except psycopg2.Error as e:
        logger.error(f"Error fetching commodity type: {e}")
        return None
    finally:
        cursor.close()

def auto_login(driver, wait):
    """Perform automatic login with correct login flow"""
    try:
        login_trigger = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "a.npx12wbtn2[onclick*=\"document.getElementById('light').style.display='block'\"]")
        ))
        login_trigger.click()
        
        time.sleep(2)
        
        username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        password_field = driver.find_element(By.NAME, "password")
        login_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
        
        username_field.clear()
        username_field.send_keys(LOGIN_CREDENTIALS['username'])
        
        password_field.clear()
        password_field.send_keys(LOGIN_CREDENTIALS['password'])
        
        login_button.click()
        
        try:
            wait.until(
                EC.any_of(
                    EC.url_contains("main"),
                    EC.presence_of_element_located((By.CLASS_NAME, "npx12b")),
                    EC.invisibility_of_element_located((By.ID, "light"))
                )
            )
            return True
        except TimeoutException:
            return True
        
    except TimeoutException:
        return False
    except Exception as e:
        return False

def select_database(driver, wait):
    """Select DATABASE from the navigation"""
    selectors_to_try = [
        "a.npx12b.data25b[href='/dynamic_graph/moreprod_compare.php']",
        "a[href='/dynamic_graph/moreprod_compare.php']",
        "a.data25b[href*='moreprod_compare.php']",
        "a[href*='moreprod_compare.php']"
    ]
    
    for attempt in range(3):
        try:
            time.sleep(2)
            
            for selector in selectors_to_try:
                try:
                    database_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    
                    driver.execute_script("arguments[0].scrollIntoView(true);", database_link)
                    time.sleep(1)
                    
                    try:
                        database_link.click()
                    except Exception:
                        driver.execute_script("arguments[0].click();", database_link)
                    
                    time.sleep(3)
                    return True
                    
                except (TimeoutException, NoSuchElementException):
                    continue
            
            current_url = driver.current_url
            base_url = current_url.split('/')[0] + '//' + current_url.split('/')[2]
            driver.get(f"{base_url}/dynamic_graph/moreprod_compare.php")
            time.sleep(3)
            return True
            
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            continue
    
    return False

def get_column_mapping_for_product(product_name):
    """Map product names from website to database column names"""
    product_to_column = {
        "O/R in 100% rayon yarn plants operating rate": "china_rayon_yarn_plant_operating_",
        "Cotton yarn plants operating rate": "china_cotton_yarn_plants_operating_rate",
        "Operating rate of Vietnamese spinners": "operating_rate_of_vietnamese_spinners", 
        "Operating rate of Indian spinners": "operating_rate_of_indian_spinners",
        "Operating rate of Pakistani spinners": "operating_rate_of_pakistani_spinners",
        "Operating rate of imported cotton yarn end-users": "china_operating_rate_of_imported_cotton_yarn_end_users",
        "Polyester yarn plants operating rate": "china_polyester_yarn_plants_operating_rate",
    }
    
    if product_name in product_to_column:
        return product_to_column[product_name]
    
    product_name_lower = product_name.lower()
    
    if "vietnam" in product_name_lower or "vietnamese" in product_name_lower:
        return "operating_rate_of_vietnamese_spinners"
    elif "india" in product_name_lower or "indian" in product_name_lower:
        return "operating_rate_of_indian_spinners"
    elif "pakistan" in product_name_lower or "pakistani" in product_name_lower:
        return "operating_rate_of_pakistani_spinners"
    elif "cotton" in product_name_lower and "import" in product_name_lower:
        return "china_operating_rate_of_imported_cotton_yarn_end_users"
    elif "cotton" in product_name_lower and "yarn" in product_name_lower:
        return "china_cotton_yarn_plants_operating_rate"
    elif "polyester" in product_name_lower and "yarn" in product_name_lower:
        return "china_polyester_yarn_plants_operating_rate"
    elif "rayon" in product_name_lower and "yarn" in product_name_lower:
        return "china_rayon_yarn_plant_operating_"
    
    return None

def should_skip_product(product_name):
    """Check if a specific product should be skipped"""
    return product_name.strip().lower() == "o/r in rayon yarn plants operating rate"

def extract_raw_data_from_chart(option_data, product_id, product_name):
    """Extract raw data from chart and structure it properly"""
    if not option_data or "series" not in option_data:
        return None

    db_column = get_column_mapping_for_product(product_name)
    if not db_column:
        return None

    product_data = {
        "product_name": product_name,
        "product_id": product_id,
        "db_column": db_column,
        "years_data": {}
    }

    for series_idx, series in enumerate(option_data["series"]):
        try:
            year = str(series["name"])

            if year != "2025":
                continue

            year_data = []
            for data_point in series["data"]:
                try:
                    date_str = None
                    value_str = None

                    if isinstance(data_point, dict) and "date" in data_point and "value" in data_point:
                        date_str = data_point["date"]
                        value_str = data_point["value"]
                    elif isinstance(data_point, list) and len(data_point) == 2:
                        date_str = data_point[0]
                        value_str = data_point[1]
                    else:
                        continue

                    if date_str and value_str is not None:
                        year_data.append({
                            "date": date_str,
                            "value": str(value_str)
                        })

                except Exception as e:
                    continue

            product_data["years_data"][year] = year_data

        except (ValueError, TypeError, KeyError) as e:
            continue

    return product_data

def get_latest_date_from_data(all_products_data):
    """Find the latest date across all products data"""
    latest_date = None
    
    for product_data in all_products_data:
        for year_str, year_data in product_data["years_data"].items():
            try:
                year = int(year_str)
                for data_point in year_data:
                    try:
                        date_str = data_point["date"]
                        month, day = map(int, date_str.split('/'))
                        data_date = date(year, month, day)
                        
                        if latest_date is None or data_date > latest_date:
                            latest_date = data_date
                            
                    except (ValueError, IndexError):
                        continue
            except ValueError:
                continue
    
    return latest_date

def convert_to_dataframe(all_products_data, only_latest_date=True):
    """Convert all products data to a single DataFrame, optionally filtering to only the latest date"""
    latest_date = None
    if only_latest_date:
        latest_date = get_latest_date_from_data(all_products_data)
    
    all_rows = []
    
    for product_data in all_products_data:
        product_name = product_data["product_name"]
        product_id = product_data["product_id"]
        db_column = product_data["db_column"]
        
        for year_str, year_data in product_data["years_data"].items():
            try:
                year = int(year_str)
                
                for data_point in year_data:
                    try:
                        date_str = data_point["date"]
                        value_str = data_point["value"]
                        
                        month, day = map(int, date_str.split('/'))
                        data_date = date(year, month, day)
                        
                        float_value = float(value_str)
                        
                        if only_latest_date and latest_date and data_date != latest_date:
                            continue
                        
                        row = {
                            'date': data_date,
                            'year': year,
                            'month': month,
                            'day': day,
                            'product_name': product_name,
                            'product_id': product_id,
                            'db_column': db_column,
                            'value': float_value
                        }
                        
                        row[db_column] = float_value
                        
                        all_rows.append(row)
                        
                    except (ValueError, IndexError) as e:
                        continue
                        
            except ValueError as e:
                continue
    
    df = pd.DataFrame(all_rows)
    
    if not df.empty:
        df = df.drop_duplicates(subset=['date', 'product_name'], keep='last')
        df = df.sort_values(['date', 'product_name'])
    
    return df

SCHEMA_NAME = 'prod'
TABLE_NAME = 'oprates_ccf_historical'
FULL_TABLE_NAME = f'"{SCHEMA_NAME}"."{TABLE_NAME}"'

def save_dataframe_to_database(df, conn, table_columns):
    """Save DataFrame to the new prod.oprates_ccf_historical table with commodity_type"""
    if df.empty:
        logger.info("DataFrame is empty, no data to save")
        return

    cursor = conn.cursor()

    try:
        # Get commodity type
        commodity_type = get_commodity_type(conn)
        if not commodity_type:
            logger.warning("Could not fetch commodity_type, using NULL")
            commodity_type = None

        # Filter columns that match the database table (lowercase, no quotes)
        db_columns_in_df = [
            col for col in df.columns 
            if col.startswith(('china_', 'operating_rate_')) and col in table_columns
        ]

        records_updated = 0
        records_inserted = 0

        for date_group, group_df in df.groupby('date'):
            try:
                date_value = str(date_group)
                column_updates = {
                    row['db_column']: row['value']
                    for _, row in group_df.iterrows()
                    if row['db_column'] in table_columns
                }

                if not column_updates:
                    logger.info(f"No valid column updates for date {date_value}")
                    continue

                # Add commodity_type to the updates
                column_updates['commodity_type'] = commodity_type

                # Check if a record exists for the date
                check_query = f'''
                    SELECT id FROM {FULL_TABLE_NAME}
                    WHERE date = %s
                '''
                cursor.execute(check_query, (date_value,))
                existing_record = cursor.fetchone()

                if existing_record:
                    # Update existing record
                    set_clause = ', '.join([f"{col} = %s" for col in column_updates.keys()])
                    update_query = f'''
                        UPDATE {FULL_TABLE_NAME}
                        SET {set_clause}
                        WHERE date = %s
                    '''
                    update_values = list(column_updates.values()) + [date_value]
                    cursor.execute(update_query, update_values)
                    records_updated += 1
                else:
                    # Insert new record
                    columns_list = ['date'] + list(column_updates.keys())
                    values_list = ['%s'] * len(columns_list)
                    insert_query = f'''
                        INSERT INTO {FULL_TABLE_NAME} ({', '.join(columns_list)})
                        VALUES ({', '.join(values_list)})
                    '''
                    insert_values = [date_value] + list(column_updates.values())
                    cursor.execute(insert_query, insert_values)
                    records_inserted += 1

            except psycopg2.Error as e:
                logger.error(f"Error processing date {date_value}: {e}")
                continue

        conn.commit()
        logger.info(f"Records inserted: {records_inserted}, updated: {records_updated}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database operation failed: {e}")
    finally:
        cursor.close()

def main():
    """Main function to run the scraper"""
    current_utc = datetime.now(pytz.UTC)
    if current_utc.weekday() >= 5:
        logger.info("Skipping execution on weekend (UTC)")
        return

    scraper = UbuntuWebScraper()
    conn = None
    start_time = datetime.now(pytz.UTC)
    
    try:
        scraper.setup_driver()
        driver = scraper.driver
        wait = scraper.wait

        conn = create_database_connection()
        table_columns = verify_existing_table(conn)

        os.makedirs("spun_yarn_data", exist_ok=True)

        driver.get("https://www.ccfgroup.com/")
        
        if not auto_login(driver, wait):
            logger.error("Auto login failed. Please log in manually...")
            input("Press Enter once you are logged in...")
        
        if not select_database(driver, wait):
            logger.error("Could not select DATABASE automatically")
            input("Please manually navigate to the database section and press Enter...")
        
        try:
            operating_link = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "a[href*='run_analysis_compare.php?cate=fhzs']")))
            operating_link.click()
        except TimeoutException:
            driver.get("https://www.ccfgroup.com/dynamic_graph/run_analysis_compare.php?cate=fhzs")
        
        time.sleep(5)
        
        yearly_link = None
        selectors_to_try = [
            ("CSS", "a[href*='run_analysis_yearly_compare.php']"),
            ("CSS", "a.npx14textsmall"),
            ("XPATH", "//a[contains(@href, 'yearly_compare')]"),
            ("XPATH", "//a[contains(text(), 'Yearly Trends')]"),
            ("PARTIAL_LINK_TEXT", "Yearly Trends Compared"),
        ]
        
        for selector_type, selector_value in selectors_to_try:
            try:
                if selector_type == "CSS":
                    yearly_link = driver.find_element(By.CSS_SELECTOR, selector_value)
                elif selector_type == "XPATH":
                    yearly_link = driver.find_element(By.XPATH, selector_value)
                elif selector_type == "PARTIAL_LINK_TEXT":
                    yearly_link = driver.find_element(By.PARTIAL_LINK_TEXT, selector_value)
                
                if yearly_link:
                    break
                    
            except NoSuchElementException:
                yearly_link = None
                continue
        
        if yearly_link:
            driver.execute_script("arguments[0].scrollIntoView(true);", yearly_link)
            time.sleep(1)
            yearly_link.click()
            time.sleep(3)
            
            try:
                wait.until(EC.presence_of_element_located((By.ID, "Category")))
                category_select = Select(driver.find_element(By.ID, "Category"))
                
                spun_yarn_found = False
                for option in category_select.options:
                    if "spun" in option.text.lower() or "yarn" in option.text.lower():
                        category_select.select_by_value(option.get_attribute('value'))
                        spun_yarn_found = True
                        break
                
                if not spun_yarn_found:
                    pass
                    
            except TimeoutException:
                pass

            time.sleep(2)
            
            try:
                prod_id_select = Select(wait.until(EC.presence_of_element_located((By.ID, "ProdId"))))
                prod_id_options = []
                
                for option in prod_id_select.options:
                    value = option.get_attribute('value')
                    text = option.text.strip()

                    if text == "O/R in rayon yarn plants operating rate":
                        continue

                    if value and value.strip():
                        db_column = get_column_mapping_for_product(text)
                        if db_column and db_column in table_columns:
                            prod_id_options.append((value, text))
                        else:
                            prod_id_options.append((value, text))
                
            except (NoSuchElementException, TimeoutException):
                prod_id_options = []

            all_products_data = []
            
            for i, (prod_value, prod_text) in enumerate(prod_id_options):
                try:
                    prod_id_select = Select(wait.until(EC.presence_of_element_located((By.ID, "ProdId"))))
                    prod_id_select.select_by_value(prod_value)
                    time.sleep(1)
                    
                    yr_start_select = Select(wait.until(EC.presence_of_element_located((By.NAME, "yr_start"))))
                    yr_end_select = Select(wait.until(EC.presence_of_element_located((By.NAME, "yr_end"))))
                    
                    start_options = [opt.get_attribute('value') for opt in yr_start_select.options if opt.get_attribute('value')]
                    end_options = [opt.get_attribute('value') for opt in yr_end_select.options if opt.get_attribute('value')]
                    
                    if start_options and end_options:
                        earliest_year = min(start_options)
                        latest_year = max(end_options)
                        yr_start_select.select_by_value(earliest_year)
                        yr_end_select.select_by_value(latest_year)
                    
                    submit_button = wait.until(EC.element_to_be_clickable((By.NAME, "graph_crt_submit")))
                    submit_button.click()
                    
                    wait.until(EC.presence_of_element_located((By.ID, "main")))
                    time.sleep(3)

                    option_data = driver.execute_script("return option")
                    
                    if option_data and "series" in option_data:
                        product_data = extract_raw_data_from_chart(option_data, prod_value, prod_text)
                        
                        if product_data:
                            all_products_data.append(product_data)
                            
                            backup_data = {
                                "product_name": prod_text,
                                "product_value": prod_value,
                                "db_column": product_data["db_column"],
                                "scrape_date": datetime.now().isoformat(),
                                "years_data": product_data["years_data"]
                            }
                            
                            filename = f"spun_yarn_data/{prod_value}_{prod_text.replace('/', '_').replace(' ', '_')}_raw.json"
                            with open(filename, "w", encoding="utf-8") as f:
                                json.dump(backup_data, f, indent=2, ensure_ascii=False)
                            
                    time.sleep(2)
                    
                except Exception as e:
                    try:
                        driver.refresh()
                        time.sleep(3)
                        wait.until(EC.presence_of_element_located((By.ID, "ProdId")))
                    except:
                        pass
                    continue

            final_df = convert_to_dataframe(all_products_data, only_latest_date=True)
            
            save_dataframe_to_database(final_df, conn, table_columns)

            execution_time = (datetime.now(pytz.UTC) - start_time).total_seconds()
            scraper.log_job_execution(
                job_name="CCF_operating_rates_scraper",
                status="SUCCESS",
                level="INFO",
                details=f"Pipeline completed successfully in {execution_time:.1f}s. Records processed: {len(final_df)}"
            )
            logger.info("Completed successfully")

    except Exception as e:
        execution_time = (datetime.now(pytz.UTC) - start_time).total_seconds()
        scraper.log_job_execution(
            job_name="CCF_operating_rates_scraper",
            status="FAILURE",
            level="ERROR",
            details=f"Pipeline failed after {execution_time:.1f}s: {str(e)}"
        )
        logger.error(f"Failed: {e}")

    finally:
        if conn:
            conn.close()
        scraper.cleanup()

if __name__ == "__main__":
    main()
