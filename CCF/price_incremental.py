import os
import random
import sys
import time
import logging
import hashlib
import json
import re
import glob
import traceback
import pytz
import uuid
import pandas as pd
import pdfplumber
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
import platform
import subprocess
import tempfile
import shutil
from dateutil.parser import parse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection for logging
def get_engine():
    """Create SQLAlchemy engine for database connection"""
    db_config = {
        'host': os.getenv('LOCAL_DB_HOST', ''),
        'database': os.getenv('LOCAL_DB_NAME', ''),
        'user': os.getenv('LOCAL_DB_USER', ''),
        'password': os.getenv('LOCAL_DB_PASS', ''),
        'port': os.getenv('LOCAL_DB_PORT', '')
    }
    connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    return create_engine(connection_string)

# Cotton features mapping
YARN_FEATURES = {
    "100001::17": "WTI Futures",
    "100003::17": "Brent Futures",
    "700059::20": "Pakistani forward combed C20S",
    "100056::14": "Polyester sewing thread 40S/2",
    "100037::0": "CNY closing selling rate",
    "100062::14": "CNY central parity rate",
    "700065::14": "Vortex-spun R40S",
    "700064::14": "Rayon ply yarn 30S",
    "700001::14": "Cotton open end yarn 10S",
    "700002::14": "Cotton carded yarn 32S",
    "700003::14": "Cotton combed yarn 40S",
    "700004::14": "Polyester yarn 32S",
    "700005::14": "Rayon yarn 30S",
    "700035::14": "Vortex-spun R30S",
    "700006::14": "Polyester/cotton yarn 45S",
    "700014::14": "Polyester/rayon yarn  32S",
    "700007::14": "Pakistani C20S",
    "700008::14": "Indian C32S",
    "700009::14": "Uzbekistani C32S",
    "700010::14": "Taiwanese open-end 10S",
    "700015::14": "Indian open-end C10S",
    "700016::14": "Pakistani siro-spun C10S",
    "700017::14": "Vietnamese C32S for rapier",
    "700018::14": "Ring-spun R60S",
    "700019::14": "Ring-spun combed C60S",
    "700020::20": "Pakistani forward siro-spun C10S",
    "700021::20": "Taiwanese forward OEC10S",
    "700022::20": "Indian forward C32S",
    "700023::20": "Indian forward OEC10S",
    "700024::20": "Pakistani forward C20S",
    "700025::20": "Uzbekistani forward C32S",
    "700026::20": "Vietnamese forward C32S",
    "700027::14": "Cotton carded 40S",
    "700028::14": "Cotton combed 32S",
    "700029::14": "Compact-spun combed C60S",
    "700030::14": "Compact-spun combed C80S made of long staples",
    "700031::14": "Open-end R10S",
    "700032::14": "Compact siro-spun R40S",
    "700033::14": "Siro-spun R40S",
    "700034::14": "Open-end R30S",
    "700038::14": "CVC60/40 32S",
    "700039::14": "T/C65/35 32S",
    "700041::14": "Indian open-end C16S",
    "700042::14": "Indian open-end C21S",
    "700043::14": "Vietnamese open-end C21S",
    "700044::14": "Pakistani C16S",
    "700045::14": "Indian C21S",
    "700046::14": "Thailand C32S",
    "700047::14": "Indonesian C32S",
    "700048::14": "Indian C40S",
    "700049::14": "Pakistani combed C20S",
    "700050::14": "Indian combed C32S",
    "700051::14": "Indian combed C40S",
    "700052::20": "Indian forward OEC16S",
    "700053::20": "Indian forward OEC21S",
    "700054::20": "Vietnamese forward OEC21S",
    "700055::20": "Pakistani forward C16S",
    "700056::20": "Indian forward C21S",
    "700057::20": "Thailand forward C32S",
    "700058::20": "Indonesian forward C32S",
    "700066::14": "Vietnamese C32S for air-jet",
    "700060::20": "Indian forward C40S",
    "700061::20": "Indian forward combed C32S",
    "B00018::14": "Polyester yarn 32S (close virgin PSF )",
    "700062::20": "Indian forward combed C40S",
}

# Column mapping for database format, aligned with new table schema
COLUMN_MAPPING = {
    "Date": "date",
    "CNY closing selling rate": "usd_rmb_exchange_rate",
    "Compact siro-spun R40S": "china_compact_siro_spun_r40s_",
    "Compact-spun combed C60S": "china_compact_spun_combed_c60s_",
    "Compact-spun combed C80S made of long staples": "china_compact_spun_combed_c80s_made_of_long_staples_",
    "Cotton carded 40S": "china_cotton_carded_40s_",
    "Cotton carded yarn 32S": "china_cotton_carded_yarn_32s",
    "Cotton combed 32S": "china_cotton_combed_32s_",
    "Cotton combed yarn 40S": "china_cotton_combed_yarn_40s_",
    "Cotton open end yarn 10S": "china_cotton_open_end_yarn_10s",
    "CVC60/40 32S": "china_cvc60_40_32s_",
    "Polyester yarn 32S": "china_polyester_yarn_32s",
    "Polyester yarn 32S (close virgin PSF )": "china_polyester_yarn_32s_close_virgin_psf",
    "Polyester/cotton yarn 45S": "china_polyester_cotton_yarn_45s",
    "Polyester/rayon yarn  32S": "china_polyester_rayon_yarn_32s",
    "Rayon ply yarn 30S": "china_rayon_ply_yarn_30s",
    "Rayon yarn 30S": "china_rayon_yarn_30s",
    "Ring-spun combed C60S": "china_ring_spun_combed_c60s_",
    "Ring-spun R60S": "china_ring_spun_r60s",
    "Siro-spun R40S": "china_siro_spun_r40s_",
    "T/C65/35 32S": "china_t_c65_35_32s_",
    "Vortex-spun R30S": "china_vortex_spun_r30s_",
    "Vortex-spun R40S": "china_vortex_spun_r40s",
    "Open-end R10S": "open_end_r10s_",
    "Open-end R30S": "open_end_r30s_",
    "Polyester sewing thread 40S/2": "polyester_sewing_thread_40s_2",
    "Indian C21S": "indian_c21s",
    "Indian C32S": "indian_c32s",
    "Indian C40S": "indian_c40s",
    "Indian combed C32S": "indian_combed_c32s",
    "Indian combed C40S": "indian_combed_c40s",
    "Indian forward C21S": "indian_forward_c21s_",
    "Indian forward C32S": "indian_forward_c32s_",
    "Indian forward C40S": "indian_forward_c40s_",
    "Indian forward combed C32S": "indian_forward_combed_c32s_",
    "Indian forward combed C40S": "indian_forward_combed_c40s_",
    "Indian forward OEC10S": "indian_forward_oec10s_",
    "Indian forward OEC16S": "indian_forward_oec16s",
    "Indian forward OEC21S": "indian_forward_oec21s",
    "Indian open-end C10S": "indian_open_end_c10s_",
    "Indian open-end C16S": "indian_open_end_c16s",
    "Indian open-end C21S": "indian_open_end_c21s",
    "Indonesian C32S": "indonesian_c32s",
    "Indonesian forward C32S": "indonesian_forward_c32s_",
    "Pakistani C16S": "pakistani_c16s",
    "Pakistani C20S": "pakistani_c20s",
    "Pakistani combed C20S": "pakistani_combed_c20s",
    "Pakistani forward C16S": "pakistani_forward_c16s_",
    "Pakistani forward C20S": "pakistani_forward_c20s_",
    "Pakistani forward combed C20S": "pakistani_forward_combed_c20s",
    "Pakistani forward siro-spun C10S": "pakistani_forward_siro_spun_c10s_",
    "Pakistani siro-spun C10S": "pakistani_siro_spun_c10s_",
    "Taiwanese forward OEC10S": "taiwanese_forward_oec10s_",
    "Taiwanese open-end 10S": "taiwanese_open_end_10s_",
    "Thailand C32S": "thailand_c32s",
    "Thailand forward C32S": "thailand_forward_c32s_",
    "Uzbekistani C32S": "uzbekistani_c32s_",
    "Uzbekistani forward C32S": "uzbekistani_forward_c32s_",
    "Vietnamese C32S for rapier": "vietnamese_c32s_for_rapier",
    "Vietnamese forward C32S": "vietnamese_forward_c32s_",
    "Vietnamese forward OEC21S": "vietnamese_forward_oec21s",
    "Vietnamese open-end C21S": "vietnamese_open_end_c21s"
}

# Get current year only
current_year = datetime.now().year
YEAR_RANGES = [(current_year, current_year)]

# Load environment variables for database connection
load_dotenv()

# Database configuration with fallback values
DB_CONFIG = {
    'host': os.getenv('LOCAL_DB_HOST', ''),
    'database': os.getenv('LOCAL_DB_NAME', ''),
    'user': os.getenv('LOCAL_DB_USER', ''),
    'password': os.getenv('LOCAL_DB_PASS', ''),
    'port': os.getenv('LOCAL_DB_PORT', '')
}

# Configuration flags
AUTO_INSERT_TO_DB = os.getenv('AUTO_INSERT_TO_DB', 'true').lower() == 'true'

# Get today's date only
TODAY_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

logger.info(f"Data filtering: Running with source = 5, will extract data for today's date only: {TODAY_DATE.strftime('%Y-%m-%d')}")

class UbuntuWebScraper:
    """Ubuntu-compatible web scraper with fallback browser support"""
    
    def __init__(self, headless=True):
        self.driver = None
        self.wait = None
        self.browser_type = None
        self.virtual_display = None
        self.temp_user_data_dir = None
        self.headless = headless
        self.system_info = {
            'platform': platform.system().lower(),
            'architecture': platform.machine().lower(),
            'is_linux': platform.system().lower() == 'linux'
        }
        # Initialize run tracking
        self.run_id = uuid.uuid4()
        self.run_label = f"run_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}_cotton_scraper"

    def log_job_execution(self, job_name, status, level, details, message=None):
        """Log job execution details to staging_tables.cron_jobs_logs"""
        engine = get_engine()
        if not message:
            message = job_name

        log_details = {
            "run_label": self.run_label,
            "message": details
        }
        details_json = json.dumps(log_details)

        log_entry = [{
            "job_name": job_name,
            "status": status,
            "level": level,
            "message": f"Running with source = 5: {message}",
            "timestamp": datetime.now(pytz.UTC),
            "run_id": str(self.run_id),
            "script_path": "cotton_scraper.py",
            "details": details_json,
            "source_id": 5,
            "commodity_id": None,
            "report_id": None
        }]

        try:
            logger.debug(f"Attempting to log with engine: {engine}")
            pd.DataFrame(log_entry).to_sql(
                'cron_jobs_logs',
                schema='staging_tables',
                con=engine,
                if_exists='append',
                index=False
            )
            logger.info(f"Running with source = 5: Logged {job_name} to cron_jobs_logs: {status} - {details}")
        except Exception as e:
            logger.error(f"Running with source = 5: Failed to log to cron_jobs_logs: {str(e)}", exc_info=True)

    def setup_virtual_display(self):
        """Setup virtual display for headless environments"""
        try:
            if not os.environ.get('DISPLAY') and self.system_info['is_linux'] and self.headless:
                logger.info("Running with source = 5: No DISPLAY found, attempting to setup virtual display")
                try:
                    from pyvirtualdisplay import Display
                    self.virtual_display = Display(visible=0, size=(1920, 1080))
                    self.virtual_display.start()
                    logger.info("Running with source = 5: Virtual display started successfully")
                    self.log_job_execution(
                        job_name="CCF_price_fetch",
                        status="PROGRESS",
                        level="INFO",
                        details="Virtual display setup successful"
                    )
                except ImportError:
                    logger.warning("Running with source = 5: pyvirtualdisplay not available, install with: pip install pyvirtualdisplay")
                    self.log_job_execution(
                        job_name="CCF_price_fetch",
                        status="WARNING",
                        level="WARN",
                        details="pyvirtualdisplay not available"
                    )
                except Exception as e:
                    logger.warning(f"Running with source = 5: Failed to start virtual display: {e}")
                    self.log_job_execution(
                        job_name="CCF_price_fetch",
                        status="WARNING",
                        level="WARN",
                        details=f"Failed to start virtual display: {str(e)}"
                    )
        except Exception as e:
            logger.error(f"Running with source = 5: Error setting up virtual display: {e}")
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details=f"Error setting up virtual display: {str(e)}"
            )

    def check_browser_availability(self):
        """Check which browsers are available on the system"""
        available_browsers = {}
        driver_info = {}
        
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
        
        firefox_paths = [
            '/usr/bin/firefox',
            '/usr/bin/firefox-esr',
            '/snap/bin/firefox'
        ]
        
        for path in firefox_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                available_browsers['firefox'] = path
                break
        
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
        
        logger.info(f"Running with source = 5: Available browsers: {available_browsers}")
        logger.info(f"Running with source = 5: Available drivers: {driver_info}")
        self.log_job_execution(
            job_name="CCF_price_fetch",
            status="PROGRESS",
            level="INFO",
            details=f"Browser availability check: {available_browsers}, Drivers: {driver_info}"
        )
        
        return available_browsers, driver_info

    def install_missing_dependencies(self):
        """Install missing browser dependencies on Ubuntu"""
        try:
            logger.info("Running with source = 5: Installing missing dependencies...")
            subprocess.run(['sudo', 'apt', 'update'], check=True, capture_output=True)
            
            try:
                subprocess.run(['google-chrome', '--version'], check=True, capture_output=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.info("Running with source = 5: Installing Google Chrome...")
                subprocess.run([
                    'wget', '-q', '-O', '/tmp/google-chrome.deb',
                    'https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb'
                ], check=True)
                subprocess.run(['sudo', 'dpkg', '-i', '/tmp/google-chrome.deb'], check=True)
                subprocess.run(['sudo', 'apt', '-f', 'install', '-y'], check=True)
                
            subprocess.run(['sudo', 'apt', 'install', '-y', 'firefox'], check=True)
            subprocess.run(['sudo', 'apt', 'install', '-y', 'chromium-chromedriver', 'firefox-geckodriver'], check=True)
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details="Dependencies installed successfully"
            )
        except subprocess.CalledProcessError as e:
            logger.warning(f"Running with source = 5: Failed to install some dependencies: {e}")
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="WARNING",
                level="WARN",
                details=f"Failed to install some dependencies: {str(e)}"
            )

    def setup_chrome_driver(self, driver_info):
        """Setup Chrome driver for Ubuntu"""
        try:
            chrome_options = ChromeOptions()
            self.temp_user_data_dir = tempfile.mkdtemp()
            chrome_options.add_argument(f"--user-data-dir={self.temp_user_data_dir}")
            logger.info(f"Running with source = 5: Using temporary Chrome user data directory: {self.temp_user_data_dir}")

            if self.headless:
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
            
            prefs = {
                "download.default_directory": os.path.abspath(os.getcwd() + "/price_data"),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True
            }
            chrome_options.add_experimental_option("prefs", prefs)

            try:
                from webdriver_manager.chrome import ChromeDriverManager
                service = ChromeService(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                logger.info("Running with source = 5: Chrome WebDriver initialized with webdriver-manager")
            except ImportError:
                logger.warning("Running with source = 5: webdriver-manager not installed, trying system ChromeDriver")
                if 'chromedriver' in driver_info:
                    service = ChromeService(driver_info['chromedriver'])
                    self.driver = webdriver.Chrome(service=service, options=chrome_options)
                    logger.info(f"Running with source = 5: Chrome WebDriver initialized with service: {driver_info['chromedriver']}")
                else:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    logger.info("Running with source = 5: Chrome WebDriver initialized with system PATH")
            
            self.browser_type = 'chrome'
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"Chrome WebDriver initialized successfully"
            )
            return True
            
        except Exception as e:
            logger.error(f"Running with source = 5: Failed to setup Chrome driver: {e}")
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details=f"Failed to setup Chrome driver: {str(e)}"
            )
            return False

    def setup_firefox_driver(self, driver_info):
        """Setup Firefox driver for Ubuntu"""
        try:
            firefox_options = FirefoxOptions()
            
            if self.headless:
                firefox_options.add_argument("--headless")
            firefox_options.add_argument("--width=1920")
            firefox_options.add_argument("--height=1080")
            firefox_options.add_argument("--user-agent=Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0")
            
            download_path = os.path.abspath(os.getcwd() + "/price_data")
            firefox_options.set_preference("browser.download.folderList", 2)
            firefox_options.set_preference("browser.download.manager.showWhenStarting", False)
            firefox_options.set_preference("browser.download.dir", download_path)
            firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
            firefox_options.set_preference("pdfjs.disabled", True)
            
            try:
                from webdriver_manager.firefox import GeckoDriverManager
                service = FirefoxService(GeckoDriverManager().install())
                self.driver = webdriver.Firefox(service=service, options=firefox_options)
                logger.info("Running with source = 5: Firefox WebDriver initialized with webdriver-manager")
            except ImportError:
                logger.warning("Running with source = 5: webdriver-manager not installed, trying system GeckoDriver")
                if 'geckodriver' in driver_info:
                    service = FirefoxService(driver_info['geckodriver'])
                    self.driver = webdriver.Firefox(service=service, options=firefox_options)
                    logger.info(f"Running with source = 5: Firefox WebDriver initialized with service: {driver_info['geckodriver']}")
                else:
                    self.driver = webdriver.Firefox(options=firefox_options)
                    logger.info("Running with source = 5: Firefox WebDriver initialized with system PATH")
            
            self.browser_type = 'firefox'
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"Firefox WebDriver initialized successfully"
            )
            return True
            
        except Exception as e:
            logger.error(f"Running with source = 5: Failed to setup Firefox driver: {e}")
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details=f"Failed to setup Firefox driver: {str(e)}"
            )
            return False

    def setup_driver(self):
        """Setup the best available WebDriver"""
        self.log_job_execution(
            job_name="CCF_price_fetch",
            status="STARTED",
            level="INFO",
            details="Initializing WebDriver setup"
        )
        try:
            if self.system_info['is_linux']:
                for proc in ['chrome', 'chromedriver', 'firefox', 'geckodriver']:
                    try:
                        subprocess.run(['pkill', '-9', proc], check=True, capture_output=True)
                        logger.info(f"Running with source = 5: Terminated existing {proc} processes")
                    except subprocess.CalledProcessError:
                        pass

            self.setup_virtual_display()
            available_browsers, driver_info = self.check_browser_availability()
            
            if not available_browsers:
                logger.warning("Running with source = 5: No browsers found, attempting to install...")
                self.install_missing_dependencies()
                available_browsers, driver_info = self.check_browser_availability()
            
            success = False
            if 'chrome' in available_browsers:
                logger.info("Running with source = 5: Attempting to use Chrome...")
                success = self.setup_chrome_driver(driver_info)
            
            if not success and 'firefox' in available_browsers:
                logger.info("Running with source = 5: Chrome failed, attempting Firefox...")
                success = self.setup_firefox_driver(driver_info)
            
            if not success:
                raise Exception("Failed to initialize any WebDriver")
            
            self.driver.set_page_load_timeout(60)
            self.driver.implicitly_wait(10)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_window_size(1920, 1080)
            self.wait = WebDriverWait(self.driver, 20)
            
            self.driver.get("data:text/html,<html><body><h1>Test</h1></body></html>")
            
            logger.info(f"Running with source = 5: WebDriver setup completed using {self.browser_type}")
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"WebDriver setup completed using {self.browser_type}"
            )
            
        except Exception as e:
            logger.error(f"Running with source = 5: Failed to initialize WebDriver: {e}")
            self.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details=f"Failed to initialize WebDriver: {str(e)}"
            )
            raise
        finally:
            if self.temp_user_data_dir:
                try:
                    shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
                    logger.info(f"Running with source = 5: Cleaned up temporary user data directory: {self.temp_user_data_dir}")
                    self.log_job_execution(
                        job_name="CCF_price_fetch",
                        status="PROGRESS",
                        level="INFO",
                        details=f"Cleaned up temporary user data directory"
                    )
                except Exception as e:
                    logger.warning(f"Running with source = 5: Failed to clean up temporary user data directory: {e}")
                    self.log_job_execution(
                        job_name="CCF_price_fetch",
                        status="WARNING",
                        level="WARN",
                        details=f"Failed to clean up temporary user data directory: {str(e)}"
                    )

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info(f"Running with source = 5: {self.browser_type} browser closed")
                self.log_job_execution(
                    job_name="CCF_price_fetch",
                    status="PROGRESS",
                    level="INFO",
                    details=f"{self.browser_type} browser closed"
                )
            except:
                pass
                
        if self.virtual_display:
            try:
                self.virtual_display.stop()
                logger.info("Running with source = 5: Virtual display stopped")
                self.log_job_execution(
                    job_name="CCF_price_fetch",
                    status="PROGRESS",
                    level="INFO",
                    details="Virtual display stopped"
                )
            except:
                pass
                
        if self.temp_user_data_dir:
            try:
                shutil.rmtree(self.temp_user_data_dir, ignore_errors=True)
                logger.info(f"Running with source = 5: Cleaned up temporary user data directory: {self.temp_user_data_dir}")
                self.log_job_execution(
                    job_name="CCF_price_fetch",
                    status="PROGRESS",
                    level="INFO",
                    details="Cleaned up temporary user data directory"
                )
            except Exception as e:
                logger.warning(f"Running with source = 5: Failed to clean up temporary user data directory: {e}")
                self.log_job_execution(
                    job_name="CCF_price_fetch",
                    status="WARNING",
                    level="WARN",
                    details=f"Failed to clean up temporary user data directory: {str(e)}"
                )

def create_progress_tracker():
    """Create a progress tracking system"""
    return {
        "start_time": datetime.now().isoformat(),
        "completed_features": 0,
        "total_features": len(YARN_FEATURES),
        "completed_year_ranges": 0,
        "total_year_ranges": len(YEAR_RANGES),
        "target_date": TODAY_DATE.isoformat(),
        "errors": []
    }

def handle_authentication(driver, wait, scraper):
    """Handle automatic authentication by clicking login link"""
    try:
        logger.info("Running with source = 5: Looking for login link...")
        login_link = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "a.npx12wbtn2[onclick*='light']")
        ))
        
        logger.info("Running with source = 5: Found login link, clicking...")
        driver.execute_script("arguments[0].click();", login_link)
        time.sleep(2)
        
        username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        password_field = driver.find_element(By.NAME, "password")
        
        username = os.getenv('CCF_USERNAME', 'davefranks')
        password = os.getenv('CCF_PASSWORD', '2394tb3Y')
        
        if not username or not password:
            logger.error("Running with source = 5: CCF_USERNAME and CCF_PASSWORD environment variables must be set")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details="CCF_USERNAME and CCF_PASSWORD environment variables must be set"
            )
            return False
        
        logger.info("Running with source = 5: Entering credentials...")
        username_field.clear()
        username_field.send_keys(username)
        password_field.clear()
        password_field.send_keys(password)
        
        login_button = driver.find_element(By.CSS_SELECTOR, "input[type='submit']")
        login_button.click()
        
        time.sleep(5)
        logger.info("Running with source = 5: Login submitted successfully")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="PROGRESS",
            level="INFO",
            details="Login submitted successfully"
        )
        return True
        
    except Exception as e:
        logger.error(f"Running with source = 5: Authentication failed: {e}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Authentication failed: {str(e)}"
        )
        return False

def navigate_to_database(driver, wait, scraper):
    """Navigate to database section using the specific link"""
    try:
        logger.info("Running with source = 5: Looking for DATABASE link...")
        database_link = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "a.npx12b.data25b[href='/dynamic_graph/moreprod_compare.php']")
        ))
        
        logger.info("Running with source = 5: Found DATABASE link, clicking...")
        driver.execute_script("arguments[0].click();", database_link)
        time.sleep(5)
        
        logger.info("Running with source = 5: Successfully navigated to database section")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="PROGRESS",
            level="INFO",
            details="Successfully navigated to database section"
        )
        return True
        
    except Exception as e:
        logger.error(f"Running with source = 5: Failed to navigate to database: {e}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Failed to navigate to database: {str(e)}"
        )
        return False

def navigate_to_yearly_trends(driver, wait, scraper):
    """Navigate to yearly trends page"""
    logger.info("Running with source = 5: Navigating to yearly trends page...")
    
    time.sleep(3)
    
    selectors_to_try = [
        ("CSS", "a[href*='select_time.php?subclassid=']"),
        ("CSS", "a.npx14textsmall"),
        ("XPATH", "//a[contains(@href, 'select_time.php')]"),
        ("XPATH", "//a[contains(text(), 'Yearly Trends Compared')]"),
        ("PARTIAL_LINK_TEXT", "Yearly Trends Compared"),
    ]
    
    for selector_type, selector_value in selectors_to_try:
        try:
            logger.info(f"Running with source = 5: Trying {selector_type}: '{selector_value}'")
            
            if selector_type == "CSS":
                yearly_link = driver.find_element(By.CSS_SELECTOR, selector_value)
            elif selector_type == "XPATH":
                yearly_link = driver.find_element(By.XPATH, selector_value)
            elif selector_type == "PARTIAL_LINK_TEXT":
                yearly_link = driver.find_element(By.PARTIAL_LINK_TEXT, selector_value)
            
            if yearly_link:
                logger.info(f"Running with source = 5: Found element with {selector_type}")
                driver.execute_script("arguments[0].scrollIntoView(true);", yearly_link)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", yearly_link)
                logger.info("Running with source = 5: Clicked yearly trends link")
                scraper.log_job_execution(
                    job_name="CCF_price_fetch",
                    status="PROGRESS",
                    level="INFO",
                    details="Successfully navigated to yearly trends page"
                )
                time.sleep(5)
                return True
                
        except NoSuchElementException:
            continue
        except Exception as e:
            logger.warning(f"Running with source = 5: Error with {selector_type}: {e}")
            continue
    
    logger.error("Running with source = 5: Could not find yearly trends link")
    scraper.log_job_execution(
        job_name="CCF_price_fetch",
        status="ERROR",
        level="ERROR",
        details="Could not find yearly trends link"
    )
    return False

def wait_for_chart_load(driver, timeout=60):
    """Wait for ECharts to load completely"""
    logger.info("Running with source = 5: Waiting for chart to load completely...")
    
    try:
        driver.switch_to.frame("dygraph")
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "main")))
        logger.info("Running with source = 5: Chart container found")
    except TimeoutException:
        logger.error("Running with source = 5: Chart container not found")
        return False
    
    try:
        wait = WebDriverWait(driver, 25)
        wait.until(lambda d: d.execute_script("return typeof echarts !== 'undefined'"))
        logger.info("Running with source = 5: ECharts library loaded")
    except TimeoutException:
        logger.error("Running with source = 5: ECharts library not loaded")
        return False
    
    for attempt in range(40):
        try:
            chart_exists = driver.execute_script("""
                try {
                    var chartDom = document.getElementById('main');
                    if (chartDom) {
                        var chart = echarts.getInstanceByDom(chartDom);
                        return chart !== null && chart !== undefined;
                    }
                    return false;
                } catch (e) {
                    return false;
                }
            """)
            
            if chart_exists:
                logger.info("Running with source = 5: Chart instance created")
                break
            time.sleep(1.5)
            
        except Exception as e:
            time.sleep(1.5)
    else:
        logger.error("Running with source = 5: Chart instance not created")
        return False
    
    for attempt in range(20):
        try:
            has_data = driver.execute_script("""
                try {
                    var chartDom = document.getElementById('main');
                    if (chartDom) {
                        var chart = echarts.getInstanceByDom(chartDom);
                        if (chart) {
                            var option = chart.getOption();
                            if (option && option.series && option.series.length > 0) {
                                return true;
                            }
                        }
                    }
                    if (window.option && window.option.series && window.option.series.length > 0) {
                        return true;
                    }
                    return false;
                } catch (e) {
                    return false;
                }
            """)
            
            if has_data:
                logger.info("Running with source = 5: Chart data populated")
                return True
            time.sleep(2)
            
        except Exception as e:
            time.sleep(2)
    
    logger.error("Running with source = 5: Chart data not populated")
    return False

def extract_chart_data_robust(driver):
    """Extract chart data with multiple fallback methods"""
    logger.info("Running with source = 5: Extracting chart data...")
    
    try:
        chart_data = driver.execute_script("""
            try {
                var chartDom = document.getElementById('main');
                if (chartDom) {
                    var chart = echarts.getInstanceByDom(chartDom);
                    if (chart && chart.getOption) {
                        var option = chart.getOption();
                        if (option && option.series && option.series.length > 0) {
                            return option;
                        }
                    }
                }
                return null;
            } catch (e) {
                return null;
            }
        """)
        
        if chart_data and isinstance(chart_data, dict) and "series" in chart_data:
            logger.info("Running with source = 5: Data extracted from ECharts instance")
            return chart_data
            
    except Exception as e:
        logger.warning(f"Running with source = 5: Method 1 failed: {e}")
    
    try:
        chart_data = driver.execute_script("return window.option || null;")
        if chart_data and isinstance(chart_data, dict) and "series" in chart_data:
            logger.info("Running with source = 5: Data extracted from global option variable")
            return chart_data
    except Exception as e:
        logger.warning(f"Running with source = 5: Method 2 failed: {e}")
    
    logger.error("Running with source = 5: Could not extract chart data")
    return None

def parse_date_from_timestamp(timestamp):
    """Parse date from timestamp"""
    try:
        if isinstance(timestamp, (int, float)) and timestamp > 1000000000000:
            return datetime.fromtimestamp(timestamp / 1000)
        elif isinstance(timestamp, (int, float)) and timestamp > 1000000000:
            return datetime.fromtimestamp(timestamp)
        elif isinstance(timestamp, str):
            return parse(timestamp)
        else:
            return None
    except Exception as e:
        logger.warning(f"Running with source = 5: Error parsing timestamp {timestamp}: {e}")
        return None

def filter_today_data(data_points):
    """Filter data points to only include today's data"""
    filtered_data = []
    
    for point in data_points:
        if isinstance(point, dict) and "date" in point:
            date_obj = parse_date_from_timestamp(point["date"])
            if date_obj and date_obj.date() == TODAY_DATE.date():
                filtered_data.append(point)
        elif isinstance(point, list) and len(point) >= 2:
            date_obj = parse_date_from_timestamp(point[0])
            if date_obj and date_obj.date() == TODAY_DATE.date():
                filtered_data.append({
                    "date": point[0],
                    "price": point[1]
                })
    
    return filtered_data

def extract_data_for_year_range(driver, wait, feature_value, feature_name, year_start, year_end, scraper):
    """Extract data for a specific feature and year range"""
    logger.info(f"Running with source = 5: Processing {feature_name} for {year_start}-{year_end}")
    
    try:
        driver.refresh()
        time.sleep(random.uniform(4, 6))
        
        yr_start_dropdown = wait.until(EC.element_to_be_clickable((By.NAME, "yr_start")))
        yr_end_dropdown = wait.until(EC.element_to_be_clickable((By.NAME, "yr_end")))
        
        Select(yr_start_dropdown).select_by_value(str(year_start))
        time.sleep(1)
        Select(yr_end_dropdown).select_by_value(str(year_end))
        time.sleep(1)
        logger.info("Running with source = 5: Year range selected successfully")
        
        category_dropdown = wait.until(EC.element_to_be_clickable((By.ID, "Category")))
        Select(category_dropdown).select_by_value("700000")
        logger.info("Running with source = 5: Spun Yarn category selected")
        time.sleep(4)
        
        product_dropdown = wait.until(EC.element_to_be_clickable((By.ID, "Product")))
        Select(product_dropdown).select_by_value(feature_value)
        logger.info("Running with source = 5: Feature selected successfully")
        time.sleep(1)
        
        submit_button = wait.until(EC.element_to_be_clickable((By.NAME, "graph_crt_submit")))
        driver.execute_script("arguments[0].click();", submit_button)
        logger.info("Running with source = 5: Form submitted successfully")
        time.sleep(2)
        
        if not wait_for_chart_load(driver):
            logger.error("Running with source = 5: Chart failed to load")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details=f"Chart failed to load for {feature_name}"
            )
            return None
        
        chart_data = extract_chart_data_robust(driver)
        
        if chart_data:
            logger.info("Running with source = 5: Data extracted successfully")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"Data extracted successfully for {feature_name}"
            )
            return chart_data
        else:
            logger.error("Running with source = 5: No data extracted")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="ERROR",
                level="ERROR",
                details=f"No data extracted for {feature_name}"
            )
            return None

    except Exception as e:
        logger.error(f"Running with source = 5: Error in extraction process: {e}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Error in extraction process for {feature_name}: {str(e)}"
        )
        return None

def format_data_for_database(all_price_data, scraper):
    """Format extracted data for database insertion"""
    logger.info("Running with source = 5: Formatting data for database insertion...")
    
    master_data = {}
    
    for feature_name, year_data in all_price_data.items():
        if feature_name not in COLUMN_MAPPING:
            logger.warning(f"Running with source = 5: Feature '{feature_name}' not in column mapping, skipping")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="WARNING",
                level="WARN",
                details=f"Feature '{feature_name}' not in column mapping, skipping"
            )
            continue
            
        logger.info(f"Running with source = 5: Processing feature: {feature_name}")
        
        for year_range, series_list in year_data.items():
            for series in series_list:
                filtered_data = filter_today_data(series.get("data", []))
                
                if not filtered_data:
                    logger.info(f"Running with source = 5: No today's data found for {feature_name} in {year_range}")
                    continue
                
                logger.info(f"Running with source = 5: Found {len(filtered_data)} data points for today for {feature_name}")
                
                for data_point in filtered_data:
                    date_obj = parse_date_from_timestamp(data_point["date"])
                    if date_obj:
                        date_str = date_obj.strftime('%Y-%m-%d')
                        
                        if date_str not in master_data:
                            master_data[date_str] = {}
                        
                        mapped_column = COLUMN_MAPPING[feature_name]
                        master_data[date_str][mapped_column] = data_point["price"]
    
    if not master_data:
        logger.warning("Running with source = 5: No data to format for database")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="WARNING",
            level="WARN",
            details="No data to format for database"
        )
        return None
    
    df = pd.DataFrame.from_dict(master_data, orient='index')
    df.index.name = 'date'
    df.reset_index(inplace=True)
    
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['commodity_type'] = 'yarn'  # Add commodity_type column with default value
    df.sort_values('date', inplace=True)
    
    logger.info(f"Running with source = 5: Database-formatted data created with {len(df)} rows and {len(df.columns)} columns")
    scraper.log_job_execution(
        job_name="CCF_price_fetch",
        status="PROGRESS",
        level="INFO",
        details=f"Database-formatted data created with {len(df)} rows and {len(df.columns)} columns"
    )
    
    return df

def insert_data_to_database(df, scraper):
    """Insert formatted data to database with proper insert/update tracking"""
    if not AUTO_INSERT_TO_DB:
        logger.info("Running with source = 5: Auto database insertion is disabled")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="INFO",
            level="INFO",
            details="Auto database insertion is disabled"
        )
        return False
        
    if df is None or df.empty:
        logger.warning("Running with source = 5: No data to insert into database")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="WARNING",
            level="WARN",
            details="No data to insert into database"
        )
        return False
    
    missing_configs = [key for key, value in DB_CONFIG.items() if not value]
    if missing_configs:
        logger.error(f"Running with source = 5: Missing database configuration: {missing_configs}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Missing database configuration: {missing_configs}"
        )
        return False
    
    try:
        logger.info(f"Running with source = 5: Connecting to database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                inserted_rows = 0
                updated_rows = 0
                
                for _, row in df.iterrows():
                    # Check if a record exists for the given date
                    cur.execute('SELECT COUNT(*) FROM prod.prices_ccf_historical WHERE date = %s', (row['date'],))
                    exists = cur.fetchone()[0] > 0
                    
                    # Filter columns to only those that exist in the target table
                    valid_columns = [
                        'date', 'usd_rmb_exchange_rate', 'china_compact_siro_spun_r40s_', 'china_compact_spun_combed_c60s_',
                        'china_compact_spun_combed_c80s_made_of_long_staples_', 'china_cotton_carded_40s_',
                        'china_cotton_carded_yarn_32s', 'china_cotton_combed_32s_', 'china_cotton_combed_yarn_40s_',
                        'china_cotton_open_end_yarn_10s', 'china_cvc60_40_32s_', 'china_polyester_yarn_32s',
                        'china_polyester_yarn_32s_close_virgin_psf', 'china_polyester_cotton_yarn_45s',
                        'china_polyester_rayon_yarn_32s', 'china_rayon_ply_yarn_30s', 'china_rayon_yarn_30s',
                        'china_ring_spun_combed_c60s_', 'china_ring_spun_r60s', 'china_siro_spun_r40s_',
                        'china_t_c65_35_32s_', 'china_vortex_spun_r30s_', 'china_vortex_spun_r40s', 'open_end_r10s_',
                        'open_end_r30s_', 'polyester_sewing_thread_40s_2', 'indian_c21s', 'indian_c32s', 'indian_c40s',
                        'indian_combed_c32s', 'indian_combed_c40s', 'indian_forward_c21s_', 'indian_forward_c32s_',
                        'indian_forward_c40s_', 'indian_forward_combed_c32s_', 'indian_forward_combed_c40s_',
                        'indian_forward_oec10s_', 'indian_forward_oec16s', 'indian_forward_oec21s', 'indian_open_end_c10s_',
                        'indian_open_end_c16s', 'indian_open_end_c21s', 'indonesian_c32s', 'indonesian_forward_c32s_',
                        'pakistani_c16s', 'pakistani_c20s', 'pakistani_combed_c20s', 'pakistani_forward_c16s_',
                        'pakistani_forward_c20s_', 'pakistani_forward_combed_c20s', 'pakistani_forward_siro_spun_c10s_',
                        'pakistani_siro_spun_c10s_', 'taiwanese_forward_oec10s_', 'taiwanese_open_end_10s_',
                        'thailand_c32s', 'thailand_forward_c32s_', 'uzbekistani_c32s_', 'uzbekistani_forward_c32s_',
                        'vietnamese_c32s_for_rapier', 'vietnamese_forward_c32s_', 'vietnamese_forward_oec21s',
                        'vietnamese_open_end_c21s', 'commodity_type'
                    ]
                    cols = [col for col in row.index if col in valid_columns]
                    vals = [row[col] if pd.notna(row[col]) else None for col in cols]
                    
                    if exists:
                        set_clause = ', '.join([f"{col} = %s" for col in cols if col != 'date'])
                        update_vals = [row[col] if pd.notna(row[col]) else None for col in cols if col != 'date']
                        update_vals.append(row['date'])
                        
                        query = f'''
                            UPDATE prod.prices_ccf_historical 
                            SET {set_clause}
                            WHERE date = %s
                        '''
                        cur.execute(query, update_vals)
                        updated_rows += 1
                        logger.info(f"Running with source = 5: Updated record for date: {row['date']}")
                    else:
                        placeholders = ', '.join(['%s'] * len(cols))
                        col_names = ', '.join([f"{c}" for c in cols])
                        
                        query = f'''
                            INSERT INTO prod.prices_ccf_historical ({col_names})
                            VALUES ({placeholders})
                        '''
                        cur.execute(query, vals)
                        inserted_rows += 1
                        logger.info(f"Running with source = 5: Inserted new record for date: {row['date']}")

        logger.info(f"Running with source = 5: Database operation complete!")
        logger.info(f"Running with source = 5: Inserted: {inserted_rows} new rows")
        logger.info(f"Running with source = 5: Updated: {updated_rows} existing rows")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="SUCCESS",
            level="INFO",
            details=f"Database operation complete: Inserted {inserted_rows} new rows, Updated {updated_rows} existing rows"
        )
        
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Running with source = 5: Database error: {e}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Database error: {str(e)}"
        )
        return False
    except Exception as e:
        logger.error(f"Running with source = 5: Error inserting data to database: {e}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Error inserting data to database: {str(e)}"
        )
        return False

def save_data_with_backup(data, filename, scraper):
    """Save data with backup and validation"""
    try:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        if Path(filename).exists():
            backup_filename = f"{filename}.backup"
            import shutil
            shutil.copy2(filename, backup_filename)
            logger.info(f"Running with source = 5: Backup created: {backup_filename}")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"Backup created: {backup_filename}"
            )
        
        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=json_serializer)
        
        logger.info(f"Running with source = 5: Data saved successfully to {filename}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="PROGRESS",
            level="INFO",
            details=f"Data saved successfully to {filename}"
        )
        return True
        
    except Exception as e:
        logger.error(f"Running with source = 5: Error saving data: {e}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="ERROR",
            level="ERROR",
            details=f"Error saving data: {str(e)}"
        )
        return False

def main():
    """Main execution function for today's data only"""
    start_time = datetime.now(pytz.UTC)
    scraper = UbuntuWebScraper(headless=os.getenv('HEADLESS', 'true').lower() == 'true')
    scraper.log_job_execution(
        job_name="CCF_price_fetch",
        status="STARTED",
        level="INFO",
        details="CCF cotton data scraper started"
    )
    
    all_price_data = {}
    progress = create_progress_tracker()
    
    try:
        scraper.setup_driver()
        driver = scraper.driver
        wait = scraper.wait
        
        logger.info("Running with source = 5: Navigating to CCF Group homepage")
        driver.get("https://www.ccfgroup.com/")
        
        if not handle_authentication(driver, wait, scraper):
            logger.error("Running with source = 5: Authentication failed")
            return
        
        if not navigate_to_database(driver, wait, scraper):
            logger.error("Running with source = 5: Could not navigate to database section")
            return
        
        if not navigate_to_yearly_trends(driver, wait, scraper):
            logger.error("Running with source = 5: Could not navigate to yearly trends page")
            return
        
        filtered_features = {k: v for k, v in YARN_FEATURES.items() if v in COLUMN_MAPPING}
        logger.info(f"Running with source = 5: Processing {len(filtered_features)} features for today's date: {TODAY_DATE.strftime('%Y-%m-%d')}")
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="PROGRESS",
            level="INFO",
            details=f"Processing {len(filtered_features)} features for today's date"
        )
        
        for feature_value, feature_name in filtered_features.items():
            logger.info(f"Running with source = 5: Processing feature: {feature_name}")
            
            if feature_name not in all_price_data:
                all_price_data[feature_name] = {}
            
            feature_success = False
            
            for year_start, year_end in YEAR_RANGES:
                chart_data = extract_data_for_year_range(driver, wait, feature_value, feature_name, year_start, year_end, scraper)
                
                if chart_data:
                    year_range_key = f"{year_start}-{year_end}"
                    all_price_data[feature_name][year_range_key] = []
                    
                    for series in chart_data.get("series", []):
                        series_data = {
                            "name": series.get("name", "Unknown"),
                            "type": series.get("type", "line"),
                            "data": []
                        }
                        
                        for data_point in series.get("data", []):
                            if isinstance(data_point, list) and len(data_point) >= 2:
                                date_value = data_point[0]
                                price_value = data_point[1]
                                
                                date_obj = parse_date_from_timestamp(date_value)
                                if date_obj and date_obj.date() == TODAY_DATE.date():
                                    series_data["data"].append({
                                        "date": date_value,
                                        "price": price_value
                                    })
                        
                        if series_data["data"]:
                            all_price_data[feature_name][year_range_key].append(series_data)
                    
                    if all_price_data[feature_name][year_range_key]:
                        logger.info(f"Running with source = 5: Found today's data for {feature_name} - {len(all_price_data[feature_name][year_range_key])} series")
                        feature_success = True
                        progress["completed_year_ranges"] += 1
                    else:
                        logger.info(f"Running with source = 5: No today's data found for {feature_name}")
                        del all_price_data[feature_name][year_range_key]
                    
                    if progress["completed_year_ranges"] % 10 == 0:
                        save_data_with_backup(all_price_data, "price_data/today_price_data_progress.json", scraper)
                        logger.info(f"Running with source = 5: Progress saved: {progress['completed_year_ranges']} features completed")
                
                else:
                    error_msg = f"Failed to extract data for {feature_name} ({year_start}-{year_end})"
                    logger.error(f"Running with source = 5: {error_msg}")
                    progress["errors"].append(error_msg)
                
                time.sleep(random.uniform(2, 4))
            
            if not all_price_data[feature_name]:
                del all_price_data[feature_name]
            elif feature_success:
                progress["completed_features"] += 1
            
            logger.info(f"Running with source = 5: Completed feature: {feature_name} ({progress['completed_features']}/{len(filtered_features)})")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"Completed feature: {feature_name} ({progress['completed_features']}/{len(filtered_features)})"
            )
        
        logger.info("Running with source = 5: Saving raw extracted data")
        save_data_with_backup(all_price_data, f"price_data/today_price_data_raw_{TODAY_DATE.strftime('%Y%m%d')}.json", scraper)
        
        logger.info("Running with source = 5: Formatting data for database...")
        formatted_df = format_data_for_database(all_price_data, scraper)
        
        if formatted_df is not None and not formatted_df.empty:
            csv_filename = f"price_data/today_price_data_{TODAY_DATE.strftime('%Y%m%d')}.csv"
            formatted_df.to_csv(csv_filename, index=False)
            logger.info(f"Running with source = 5: Database-ready CSV saved to {csv_filename}")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="PROGRESS",
                level="INFO",
                details=f"Database-ready CSV saved to {csv_filename}"
            )
            
            if AUTO_INSERT_TO_DB:
                logger.info("Running with source = 5: Attempting automatic database insertion...")
                db_success = insert_data_to_database(formatted_df, scraper)
                if db_success:
                    logger.info("Running with source = 5: Data successfully inserted/updated in database!")
                else:
                    logger.warning("Running with source = 5: Database insertion failed, but CSV file is available")
            else:
                logger.info("Running with source = 5: To enable automatic database insertion, set: export AUTO_INSERT_TO_DB=true")
            
            logger.info("Running with source = 5: Today's data summary:")
            logger.info(f"Running with source = 5: Columns: {list(formatted_df.columns)}")
            logger.info(f"Running with source = 5: Shape: {formatted_df.shape}")
            logger.info(f"Running with source = 5: Target date: {TODAY_DATE.strftime('%Y-%m-%d')}")
        
        else:
            logger.warning("Running with source = 5: No today's data available for database formatting")
            scraper.log_job_execution(
                job_name="CCF_price_fetch",
                status="WARNING",
                level="WARN",
                details="No today's data available for database formatting"
            )
        
        total_data_points = 0
        for feature_name, year_data in all_price_data.items():
            feature_points = sum(
                len(series["data"]) 
                for year_range in year_data.values() 
                for series in year_range
            )
            total_data_points += feature_points
        
        summary = {
            "extraction_date": datetime.now().isoformat(),
            "target_date": TODAY_DATE.isoformat(),
            "total_features_processed": len(all_price_data),
            "features_in_mapping": len(filtered_features),
            "database_ready": formatted_df is not None and not formatted_df.empty,
            "total_today_data_points": total_data_points,
            "progress": progress,
            "execution_mode": "headless" if scraper.headless else "non-headless"
        }
        
        if formatted_df is not None:
            summary["database_summary"] = {
                "rows": len(formatted_df),
                "columns": len(formatted_df.columns),
                "target_date": TODAY_DATE.strftime('%Y-%m-%d')
            }
        
        summary["features_summary"] = {}
        for feature_name, year_data in all_price_data.items():
            feature_points = sum(
                len(series["data"]) 
                for year_range in year_data.values() 
                for series in year_range
            )
            
            summary["features_summary"][feature_name] = {
                "year_ranges": list(year_data.keys()),
                "today_data_points": feature_points,
                "mapped_column": COLUMN_MAPPING.get(feature_name, "NOT_MAPPED")
            }
        
        save_data_with_backup(summary, f"price_data/today_extraction_summary_{TODAY_DATE.strftime('%Y%m%d')}.json", scraper)
        
        execution_time = (datetime.now(pytz.UTC) - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info("Running with source = 5: EXTRACTION COMPLETE!")
        logger.info(f"Running with source = 5: Target date: {TODAY_DATE.strftime('%Y-%m-%d')} (today's data only)")
        logger.info(f"Running with source = 5: Features processed: {len(all_price_data)}")
        logger.info(f"Running with source = 5: Today's data points extracted: {total_data_points}")
        if formatted_df is not None and not formatted_df.empty:
            logger.info(f"Running with source = 5: Database-ready rows: {len(formatted_df)}")
            logger.info(f"Running with source = 5: Database-ready columns: {len(formatted_df.columns)}")
        logger.info(f"Running with source = 5: Errors encountered: {len(progress['errors'])}")
        logger.info(f"Running with source = 5: Execution mode: {'Headless' if scraper.headless else 'Non-headless'}")
        logger.info(f"Running with source = 5: Execution time: {execution_time:.1f}s")
        logger.info("=" * 60)
        
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="SUCCESS",
            level="INFO",
            details=f"Pipeline completed in {execution_time:.1f}s: {len(all_price_data)} features processed, {total_data_points} data points extracted"
        )
        
        if progress["errors"]:
            logger.warning("Running with source = 5: Errors encountered:")
            for error in progress["errors"]:
                logger.warning(f"Running with source = 5:   - {error}")
        
        if formatted_df is not None and not formatted_df.empty:
            logger.info(f"Running with source = 5: \nSUCCESS: Today's data extracted and ready!")
            logger.info(f"Running with source = 5: CSV file: {csv_filename}")
            logger.info(f"Running with source = 5: {len(formatted_df)} rows for today ({TODAY_DATE.strftime('%Y-%m-%d')})")
        else:
            logger.warning("Running with source = 5: No today's data available")

    except Exception as e:
        logger.error(f"Running with source = 5: Critical error occurred: {e}")
        traceback.print_exc()
        scraper.log_job_execution(
            job_name="CCF_price_fetch",
            status="FAILURE",
            level="ERROR",
            details=f"Critical error occurred: {str(e)}"
        )
        
        if all_price_data:
            save_data_with_backup(all_price_data, f"price_data/today_emergency_data_{TODAY_DATE.strftime('%Y%m%d')}.json", scraper)
            logger.info("Running with source = 5: Emergency data save completed")

    finally:
        logger.info("Running with source = 5: Closing resources...")
        scraper.cleanup()

if __name__ == "__main__":
    Path("price_data").mkdir(exist_ok=True)
    main()
