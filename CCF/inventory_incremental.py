import os
import sys
import time
import logging
import subprocess
import platform
import tempfile
import shutil
import uuid
import json
import pytz
from datetime import datetime
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
import psycopg2
from sqlalchemy import create_engine, text, inspect

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Configuration ===
LOGIN_USERNAME = ""
LOGIN_PASSWORD = ""

# PostgreSQL Database Configuration
DB_CONFIG = {
    'host': '',
    'database': '',
    'user': '',
    'password': '',
    'port': '',
    'schema': ''
}

# Define the desired products
DESIRED_PRODUCTS = [
    'Rayon yarn inventory',
    'Cotton yarn inventory of spinners', 
    'Imported cotton yarn port inventory',
    'Rayon yarn feedstock logical inventory',
    'Polyester yarn inventory',
    'PSF inventory of polyester yarn plants',
    'Rayon yarn feedstock physical inventory',
    'Total fabric turnover in China Textile City',
    'Chemical fiber fabric turnover in China Textile City',
    'Cotton-type fabric turnover in China Textile City',
    '100% cotton fabric turnover in China Textile City',
    'Rayon fabric turnover in China Textile City'
]

# Mapping from website product names to database column names
PRODUCT_TO_COLUMN_MAPPING = {
    'Rayon yarn inventory': 'rayon_yarn_inventory',
    'Cotton yarn inventory of spinners': 'cotton_yarn_inventory_of_spinners',
    'Imported cotton yarn port inventory': 'imported_cotton_yarn_port_inventory',
    'Rayon yarn feedstock logical inventory': 'rayon_yarn_feedstock_logical_inventory',
    'Polyester yarn inventory': 'polyester_yarn_inventory',
    'PSF inventory of polyester yarn plants': 'psf_inventory_of_polyester_yarn_plants',
    'Rayon yarn feedstock physical inventory': 'rayon_yarn_feedstock_physical_inventory',
    'Total fabric turnover in China Textile City': 'total_fabric_turnover_in_china_textile_city',
    'Chemical fiber fabric turnover in China Textile City': 'chemical_fiber_fabric_turnover_in_china_textile_city',
    'Cotton-type fabric turnover in China Textile City': 'cotton_type_fabric_turnover_in_china_textile_city',
    '100% cotton fabric turnover in China Textile City': 'cotton_fabric_turnover_in_china_textile_city',
    'Rayon fabric turnover in China Textile City': 'rayon_fabric_turnover_in_china_textile_city'
}

# Product mapping (value -> name)
TARGET_PRODUCTS = [
    {"value": "7A0000", "name": "Yarn inventory"},
    {"value": "7C0000", "name": "Rayon yarn inventory"},
    {"value": "7D0000", "name": "Cotton yarn inventory of spinners"},
    {"value": "7F0000", "name": "Imported cotton yarn port inventory"},
    {"value": "7G0000", "name": "Rayon yarn feedstock logical inventory"},
    {"value": "7H0000", "name": "Polyester yarn inventory"},
    {"value": "7I0000", "name": "PSF inventory of polyester yarn plants"},
    {"value": "7P0000", "name": "Rayon yarn feedstock physical inventory"},
    {"value": "870000", "name": "Total fabric turnover in China Textile City"},
    {"value": "880000", "name": "Chemical fiber fabric turnover in China Textile City"},
    {"value": "890000", "name": "Cotton-type fabric turnover in China Textile City"},
    {"value": "8A0000", "name": "100% cotton fabric turnover in China Textile City"},
    {"value": "8B0000", "name": "Rayon fabric turnover in China Textile City"}
]

class UbuntuWebScraper:
    """Ubuntu-compatible web scraper with minimal logging"""
    
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
        
        # Logging setup
        self.run_id = uuid.uuid4()
        self.run_label = f"run_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}_CCF_inventory_ingest"
        self.job_name = "CCF_inventory_ingest"
        
    def get_engine(self):
        """Create database engine for logging"""
        try:
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            return create_engine(connection_string)
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            return None

    def log_job_execution(self, status, level, details, message=None):
        """Log only essential job execution details to staging.cron_job_logs"""
        engine = self.get_engine()
        if not engine:
            logger.error("Cannot log to database: No engine available")
            return
            
        if not message:
            message = self.job_name

        # Convert details to JSON object
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
            "script_path": "incremental_inventory_final.py",
            "details": details_json
        }]

        try:
            pd.DataFrame(log_entry).to_sql(
                'cron_job_logs',
                schema='staging',
                con=engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Logged {self.job_name} to cron_job_logs: {status} - {details}")
        except Exception as e:
            logger.error(f"Failed to log to cron_job_logs: {str(e)}", exc_info=True)
        finally:
            engine.dispose()
        
    def setup_virtual_display(self):
        """Setup virtual display for headless environments"""
        try:
            if not os.environ.get('DISPLAY') and self.system_info['is_linux']:
                logger.info("No DISPLAY found, attempting to setup virtual display")
                try:
                    from pyvirtualdisplay import Display
                    self.virtual_display = Display(visible=0, size=(1920, 1080))
                    self.virtual_display.start()
                    logger.info("Virtual display started successfully")
                except ImportError:
                    logger.warning("pyvirtualdisplay not available, install with: pip install pyvirtualdisplay")
                except Exception as e:
                    logger.warning(f"Failed to start virtual display: {e}")
        except Exception as e:
            logger.error(f"Error setting up virtual display: {e}")

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
        
        logger.info(f"Available browsers: {available_browsers}")
        logger.info(f"Available drivers: {driver_info}")
        
        return available_browsers, driver_info

    def install_missing_dependencies(self):
        """Install missing browser dependencies on Ubuntu"""
        try:
            logger.info("Installing missing dependencies...")
            
            subprocess.run(['sudo', 'apt', 'update'], check=True, capture_output=True)
            
            # Install Chrome
            try:
                subprocess.run(['google-chrome', '--version'], check=True, capture_output=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.info("Installing Google Chrome...")
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
            logger.warning(f"Failed to install some dependencies: {e}")

    def setup_chrome_driver(self, driver_info):
        """Setup Chrome driver for Ubuntu"""
        try:
            chrome_options = ChromeOptions()
            
            # Create a unique temporary user data directory
            self.temp_user_data_dir = tempfile.mkdtemp()
            chrome_options.add_argument(f"--user-data-dir={self.temp_user_data_dir}")
            logger.info(f"Using temporary Chrome user data directory: {self.temp_user_data_dir}")

            # Ubuntu-optimized Chrome options
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
                "download.default_directory": os.path.abspath(os.getcwd() + "/downloads"),
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
                logger.info("Chrome WebDriver initialized with webdriver-manager")
            except ImportError:
                logger.warning("webdriver-manager not installed, trying system ChromeDriver")
                if 'chromedriver' in driver_info:
                    service = ChromeService(driver_info['chromedriver'])
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    logger.info(f"Chrome WebDriver initialized with service: {driver_info['chromedriver']}")
                else:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    logger.info("Chrome WebDriver initialized with system PATH")
            
            self.browser_type = 'chrome'
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Chrome driver: {e}")
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
            download_path = os.path.abspath(os.getcwd() + "/downloads")
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
                logger.info("Firefox WebDriver initialized with webdriver-manager")
            except ImportError:
                logger.warning("webdriver-manager not installed, trying system GeckoDriver")
                if 'geckodriver' in driver_info:
                    service = FirefoxService(driver_info['geckodriver'])
                    self.driver = webdriver.Firefox(service=service, options=firefox_options)
                    logger.info(f"Firefox WebDriver initialized with service: {driver_info['geckodriver']}")
                else:
                    self.driver = webdriver.Firefox(options=firefox_options)
                    logger.info("Firefox WebDriver initialized with system PATH")
            
            self.browser_type = 'firefox'
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Firefox driver: {e}")
            return False

    def setup_driver(self):
        """Setup the best available WebDriver"""
        try:
            # Terminate existing browser processes to avoid conflicts
            if self.system_info['is_linux']:
                for proc in ['chrome', 'chromedriver', 'firefox', 'geckodriver']:
                    try:
                        subprocess.run(['pkill', '-9', proc], check=True, capture_output=True)
                        logger.info(f"Terminated existing {proc} processes")
                    except subprocess.CalledProcessError:
                        pass

            # Setup virtual display if needed
            self.setup_virtual_display()
            
            # Check available browsers
            available_browsers, driver_info = self.check_browser_availability()
            
            if not available_browsers:
                logger.warning("No browsers found, attempting to install...")
                self.install_missing_dependencies()
                available_browsers, driver_info = self.check_browser_availability()
            
            # Try Chrome first
            success = False
            if 'chrome' in available_browsers:
                logger.info("Attempting to use Chrome...")
                success = self.setup_chrome_driver(driver_info)
            
            # Try Firefox if Chrome failed
            if not success and 'firefox' in available_browsers:
                logger.info("Chrome failed, attempting Firefox...")
                success = self.setup_firefox_driver(driver_info)
            
            if not success:
                raise Exception("Failed to initialize any WebDriver")
            
            # Configure driver
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(10)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_window_size(1920, 1080)
            
            # Configure WebDriverWait
            self.wait = WebDriverWait(self.driver, 30)
            
            # Test connection
            self.driver.get("data:text/html,<html><body><h1>Test</h1></body></html>")
            
            logger.info(f"WebDriver setup completed using {self.browser_type}")
            
        except Exception as e:
            logger.error(f"Failed to initialize WebDriver: {e}")
            self.log_job_execution("FAILURE", "ERROR", f"Failed to initialize WebDriver: {e}")
            raise
        finally:
            # Clean up temporary user data directory if created
            if self.temp_user_data_dir:
                try:
                    shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
                    logger.info(f"Cleaned up temporary user data directory: {self.temp_user_data_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary user data directory: {e}")

    def automatic_login(self, username, password):
        """Automatically login to the website with improved error handling"""
        logger.info("Attempting automatic login...")
        
        try:
            # Navigate to main page
            self.driver.get("https://www.ccfgroup.com/")
            time.sleep(5)
            
            # Click login link with multiple selectors
            login_selectors = [
                "a.npx12wbtn2[onclick*=\"document.getElementById('light').style.display='block'\"]",
                "a[onclick*='light']",
                ".login-btn",
                "a[href*='login']"
            ]
            
            login_clicked = False
            for selector in login_selectors:
                try:
                    login_link = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    login_link.click()
                    login_clicked = True
                    break
                except TimeoutException:
                    continue
            
            if not login_clicked:
                logger.error("Could not find login link")
                self.log_job_execution("FAILURE", "ERROR", "Could not find login link")
                return False
            
            time.sleep(3)
            
            # Wait for login modal
            self.wait.until(EC.visibility_of_element_located((By.ID, "light")))
            
            # Find and fill username
            username_selectors = [
                (By.NAME, "username"),
                (By.ID, "username"),
                (By.CSS_SELECTOR, "input[name='username']"),
                (By.CSS_SELECTOR, "input[type='text']")
            ]
            
            username_filled = False
            for by, selector in username_selectors:
                try:
                    username_field = self.driver.find_element(by, selector)
                    username_field.clear()
                    username_field.send_keys(username)
                    username_filled = True
                    break
                except NoSuchElementException:
                    continue
            
            if not username_filled:
                logger.error("Could not find username field")
                self.log_job_execution("FAILURE", "ERROR", "Could not find username field")
                return False
            
            # Find and fill password
            password_selectors = [
                (By.NAME, "password"),
                (By.ID, "password"),
                (By.CSS_SELECTOR, "input[name='password']"),
                (By.CSS_SELECTOR, "input[type='password']")
            ]
            
            password_filled = False
            for by, selector in password_selectors:
                try:
                    password_field = self.driver.find_element(by, selector)
                    password_field.clear()
                    password_field.send_keys(password)
                    password_filled = True
                    break
                except NoSuchElementException:
                    continue
            
            if not password_filled:
                logger.error("Could not find password field")
                self.log_job_execution("FAILURE", "ERROR", "Could not find password field")
                return False
            
            # Submit login
            submit_selectors = [
                "input[type='submit']",
                "button[type='submit']",
                ".login-submit",
                "input[value*='Login']"
            ]
            
            for selector in submit_selectors:
                try:
                    login_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    login_button.click()
                    break
                except NoSuchElementException:
                    continue
            
            # Wait for login completion
            time.sleep(8)
            
            # Check login success
            success_indicators = [
                "a[href*='logout']",
                "a[onclick*='logout']",
                ".user-info",
                ".welcome"
            ]
            
            for selector in success_indicators:
                try:
                    self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    logger.info("Login successful!")
                    return True
                except TimeoutException:
                    continue
            
            # Alternative check: modal disappeared
            try:
                light_element = self.driver.find_element(By.ID, "light")
                if light_element.value_of_css_property("display") == "none":
                    logger.info("Login successful (modal closed)!")
                    return True
            except:
                pass
            
            logger.error("Login failed - no success indicators found")
            self.log_job_execution("FAILURE", "ERROR", "Login failed - no success indicators found")
            return False
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            self.log_job_execution("FAILURE", "ERROR", f"Login error: {e}")
            return False

    def extract_today_data(self, product):
        """Extract today's data from chart with improved error handling"""
        today_str = datetime.now().strftime("%Y/%m/%d")
        logger.info(f"Extracting today's data ({today_str}) for {product['name']}...")
        
        try:
            # Wait for chart to load
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[data-zr-dom-id]")))
            time.sleep(5)
            
            # Try multiple methods to extract chart data
            data_extraction_scripts = [
                "return typeof option !== 'undefined' ? option : null",
                "return window.option || null",
                "return window.myChart ? window.myChart.getOption() : null",
                "return typeof myChart !== 'undefined' ? myChart.getOption() : null"
            ]

            option_data = None
            for script in data_extraction_scripts:
                try:
                    result = self.driver.execute_script(script)
                    if result and isinstance(result, dict) and "series" in result:
                        option_data = result
                        break
                except Exception:
                    continue

            if not option_data:
                logger.info(f"No chart data available for {product['name']}")
                # Log to database as informational - no data found
                self.log_job_execution("INFO", "INFO", f"No data found for product: {product['name']}")
                return None

            # Check if series data exists and is iterable
            series_data = option_data.get("series", [])
            if not series_data or not hasattr(series_data, '__iter__'):
                logger.info(f"No series data available for {product['name']}")
                self.log_job_execution("INFO", "INFO", f"No series data found for product: {product['name']}")
                return None

            today_data = {}
            for series in series_data:
                if not isinstance(series, dict):
                    continue
                    
                name = series.get("name", "series")
                data_points = series.get("data", [])
                
                # Ensure data_points is iterable
                if not data_points or not hasattr(data_points, '__iter__'):
                    continue
                
                # Look for today's data
                for point in data_points:
                    if isinstance(point, list) and len(point) >= 2:
                        point_date = point[0]
                        point_value = point[1]
                        
                        if point_date == today_str:
                            today_data[name] = {
                                "date": point_date,
                                "value": point_value
                            }
                            logger.info(f"  Found today's data: {point_date} -> {point_value}")
                            break
                
                # If no exact match, use most recent
                if name not in today_data and data_points:
                    try:
                        latest_point = data_points[-1]
                        if isinstance(latest_point, list) and len(latest_point) >= 2:
                            today_data[name] = {
                                "date": latest_point[0],
                                "value": latest_point[1]
                            }
                            logger.info(f"  Using latest: {latest_point[0]} -> {latest_point[1]}")
                    except (IndexError, TypeError):
                        continue
            
            if not today_data:
                logger.info(f"No valid data points found for {product['name']}")
                self.log_job_execution("INFO", "INFO", f"No valid data points found for product: {product['name']}")
                return None
                
            return today_data

        except Exception as e:
            logger.info(f"Unable to extract data for {product['name']}: {str(e)}")
            # Log as informational rather than error - data extraction issues are common
            self.log_job_execution("INFO", "INFO", f"Data extraction unsuccessful for product {product['name']}: {str(e)}")
            return None

    def check_and_prepare_table(self, engine, pivot_df, schema):
        """Check existing table structure and prepare for insertion"""
        try:
            inspector = inspect(engine)
            existing_columns = inspector.get_columns('inventories_ccf_historical', schema=schema)
            existing_column_names = [col['name'] for col in existing_columns]
            
            logger.info(f"Existing table columns: {existing_column_names}")
            logger.info(f"DataFrame columns: {list(pivot_df.columns)}")
            
            # Check missing columns
            missing_columns = [col for col in pivot_df.columns if col not in existing_column_names]
            
            if missing_columns:
                logger.warning(f"Missing columns: {missing_columns}")
                self.log_job_execution("FAILURE", "ERROR", f"Missing columns in database: {missing_columns}")
                available_columns = ['Date'] + [col for col in pivot_df.columns if col in existing_column_names]
                return available_columns
            else:
                logger.info("All required columns exist")
                return list(pivot_df.columns)
                
        except Exception as e:
            logger.error(f"Error checking table structure: {e}")
            self.log_job_execution("FAILURE", "ERROR", f"Error checking table structure: {e}")
            return None

    def transform_and_insert_data(self, scraped_data, db_config):
        """Transform scraped data and insert into PostgreSQL database"""
        logger.info("Transforming data...")
        
        # Convert to DataFrame
        rows = []
        for product_name, series_data in scraped_data.items():
            if product_name in DESIRED_PRODUCTS and series_data:
                for series_name, point_data in series_data.items():
                    if isinstance(point_data, dict) and "date" in point_data:
                        rows.append({
                            "Product_Name": product_name,
                            "Series": series_name,
                            "Date": point_data["date"],
                            "Value": point_data["value"]
                        })
        
        if not rows:
            logger.info("No data to transform - all products returned no data")
            self.log_job_execution("INFO", "INFO", "No data available for transformation - all products returned no data")
            return False
        
        df = pd.DataFrame(rows)
        df['Date'] = pd.to_datetime(df['Date'], format='%Y/%m/%d').dt.strftime('%Y-%m-%d')
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        df = df.dropna(subset=['Date', 'Value'])
        
        if df.empty:
            logger.info("No valid data after cleaning")
            self.log_job_execution("INFO", "INFO", "No valid data after cleaning - all values were invalid")
            return False
        
        # Pivot and map columns
        pivot_df = df.pivot(index='Date', columns='Product_Name', values='Value').reset_index()
        
        column_mapping = {'Date': 'Date'}
        for website_name, db_column in PRODUCT_TO_COLUMN_MAPPING.items():
            if website_name in pivot_df.columns:
                column_mapping[website_name] = db_column
        
        pivot_df = pivot_df.rename(columns=column_mapping)
        
        logger.info(f"Transformed data shape: {pivot_df.shape}")
        
        # Insert into database
        try:
            connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            engine = create_engine(connection_string)
            
            available_columns = self.check_and_prepare_table(engine, pivot_df, db_config['schema'])
            if available_columns is None:
                return False
            
            if len(available_columns) < len(pivot_df.columns):
                pivot_df = pivot_df[available_columns]
            
            # Check existing data
            today_date = datetime.now().strftime('%Y-%m-%d')
            schema = db_config['schema']
            
            with engine.connect() as conn:
                existing_data_query = text(f'SELECT COUNT(*) FROM {schema}.inventories_ccf_historical WHERE "Date" = :date_param')
                result = conn.execute(existing_data_query, {"date_param": today_date})
                existing_count = result.fetchone()[0]
                
                if existing_count > 0:
                    logger.info(f"Updating existing data for {today_date}")
                    delete_query = text(f'DELETE FROM {schema}.inventories_ccf_historical WHERE "Date" = :date_param')
                    conn.execute(delete_query, {"date_param": today_date})
                    conn.commit()
            
            # Insert data
            pivot_df.to_sql(
                'inventories_ccf_historical', 
                engine, 
                schema=schema,
                if_exists='append', 
                index=False, 
                method='multi'
            )
            
            engine.dispose()
            logger.info(f"Data inserted into {schema}.inventories_ccf_historical")
            return True
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            self.log_job_execution("FAILURE", "ERROR", f"Database error: {e}")
            return False

    def run_scraping(self):
        """Main scraping method"""
        start_time = datetime.now(pytz.UTC)
        scraped_data = {}
        products_with_no_data = []
        
        # Log only the start of the process
        self.log_job_execution("STARTED", "INFO", "CCF scraping pipeline started")
        
        try:
            logger.info("=" * 60)
            logger.info("STARTING CCF WEB SCRAPING PIPELINE")
            logger.info("=" * 60)

            logger.info("Step 1: Setting up Selenium WebDriver...")
            self.setup_driver()

            logger.info("Step 2: Logging into website...")
            if not self.automatic_login(LOGIN_USERNAME, LOGIN_PASSWORD):
                logger.error("Login failed")
                return False

            # Navigate to database page
            logger.info("Step 3: Navigating to database page...")
            try:
                database_link = self.wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "a.npx12b.data25b[href='/dynamic_graph/moreprod_compare.php']")))
                database_link.click()
            except TimeoutException:
                self.driver.get("https://www.ccfgroup.com/dynamic_graph/moreprod_compare.php")
            time.sleep(5)

            # Navigate to inventory page
            logger.info("Step 4: Navigating to inventory page...")
            try:
                link = self.wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "a[href*='run_analysis.php?cate=kczs']")))
                link.click()
            except TimeoutException:
                self.driver.get("https://www.ccfgroup.com/dynamic_graph/run_analysis.php?cate=kczs")
            time.sleep(5)

            # Select category
            logger.info("Step 5: Selecting 'Spun yarn' category...")
            Select(self.wait.until(EC.presence_of_element_located((By.ID, "Category")))).select_by_value("700000")
            time.sleep(2)

            # Select basic trends
            logger.info("Step 6: Selecting 'Basic Trends'...")
            basic_trends_radio = self.wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "input[type='radio'][name='type'][value='1']")))
            basic_trends_radio.click()
            time.sleep(2)

            # Set date range to today
            today = datetime.now()
            today_str = today.strftime("%Y/%m/%d")
            
            logger.info(f"Step 7: Setting date range to today: {today_str}")
            
            start_date_field = self.wait.until(EC.presence_of_element_located((By.ID, "startdate")))
            start_date_field.clear()
            start_date_field.send_keys(today_str)
            
            end_date_field = self.driver.find_element(By.ID, "enddate")
            end_date_field.clear()
            end_date_field.send_keys(today_str)
            
            time.sleep(2)

            # Process products
            target_products_filtered = [p for p in TARGET_PRODUCTS if p['name'] in DESIRED_PRODUCTS]
            logger.info(f"Step 8: Processing {len(target_products_filtered)} products...")
            
            # Log progress after navigation is complete
            self.log_job_execution("PROGRESS", "INFO", f"Setup complete - processing {len(target_products_filtered)} products")
            
            for idx, product in enumerate(target_products_filtered, 1):
                logger.info(f"\n=== Processing {idx}/{len(target_products_filtered)}: {product['name']} ===")
                
                try:
                    # Select product
                    Select(self.driver.find_element(By.ID, "ProdId")).select_by_value(product['value'])
                    time.sleep(2)

                    # Submit form
                    logger.info("Submitting form...")
                    self.driver.find_element(By.NAME, "graph_crt_submit").click()
                    time.sleep(5)

                    # Extract data
                    today_data = self.extract_today_data(product)
                    if today_data:
                        scraped_data[product['name']] = today_data
                    else:
                        products_with_no_data.append(product['name'])
                    
                except Exception as e:
                    logger.info(f"Unable to process {product['name']}: {e}")
                    products_with_no_data.append(product['name'])
                    # Log as informational rather than failure
                    self.log_job_execution("INFO", "INFO", f"Unable to process product {product['name']}: {e}")

            # Transform and insert data
            logger.info("Step 9: Transforming and inserting data...")
            if scraped_data:
                success = self.transform_and_insert_data(scraped_data, DB_CONFIG)
                if success:
                    execution_time = (datetime.now(pytz.UTC) - start_time).total_seconds()
                    # Log successful completion with summary
                    summary_details = {
                        "products_scraped": len(scraped_data),
                        "products_with_data": list(scraped_data.keys()),
                        "products_without_data": products_with_no_data,
                        "execution_time_seconds": round(execution_time, 1)
                    }
                    self.log_job_execution("SUCCESS", "INFO", f"Process completed successfully: {json.dumps(summary_details)}")
                    logger.info("Process completed successfully!")
                else:
                    logger.error("Failed to insert data")
                    return False
            else:
                self.log_job_execution("INFO", "INFO", f"No data scraped from any products. Products attempted: {[p['name'] for p in target_products_filtered]}")
                logger.info("No data scraped from any products")
                return False

            # Console summary (not logged to DB)
            logger.info(f"SUMMARY: {len(scraped_data)} products with data, {len(products_with_no_data)} without data")
            if scraped_data:
                logger.info("Products with data:")
                for name in scraped_data.keys():
                    logger.info(f"  - {name}")
            if products_with_no_data:
                logger.info("Products without data:")
                for name in products_with_no_data:
                    logger.info(f"  - {name}")

            return len(scraped_data) > 0

        except Exception as e:
            execution_time = (datetime.now(pytz.UTC) - start_time).total_seconds()
            logger.error(f"Scraping error: {e}")
            self.log_job_execution("FAILURE", "ERROR", f"Scraping failed after {execution_time:.1f}s: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info(f"{self.browser_type} browser closed")
            except:
                pass
                
        if self.virtual_display:
            try:
                self.virtual_display.stop()
                logger.info("Virtual display stopped")
            except:
                pass
                
        if self.temp_user_data_dir:
            try:
                shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary user data directory: {self.temp_user_data_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary user data directory: {e}")

def main():
    """Main function"""
    if datetime.now(pytz.UTC).weekday() >= 5:
        logger.info("Skipping execution on weekend (UTC)")
        return
        
    try:
        scraper = UbuntuWebScraper()
        
        success = scraper.run_scraping()
        
        if success:
            logger.info("Scraping completed successfully!")
        else:
            logger.info("Scraping completed with no data found!")
            # Don't exit with error code if no data is found - it's not necessarily a failure
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        try:
            scraper = UbuntuWebScraper()
            scraper.log_job_execution("INTERRUPTED", "WARN", "Process interrupted by user")
        except:
            pass
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        try:
            scraper = UbuntuWebScraper()
            scraper.log_job_execution("FAILURE", "ERROR", f"Unexpected error in main: {str(e)}")
        except:
            pass
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
