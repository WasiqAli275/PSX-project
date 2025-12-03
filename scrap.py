from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime, timedelta
import sys
import io
import os
import psycopg2
from psycopg2 import sql
import pytz
import logging
from typing import List, Dict, Any
import schedule
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Fix Unicode encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Configuration
PSX_MARKET_OPEN = "09:30"  # PKT (Asia/Karachi)
PSX_MARKET_CLOSE = "15:30"  # PKT (Asia/Karachi)
SCRAPE_INTERVAL_MINUTES = 5  # Run every 5 minutes during market hours
PAKISTAN_TIMEZONE = pytz.timezone('Asia/Karachi')

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('PGDATABASE', 'psx_data'),
    'user': os.getenv('PGUSER', 'postgres'),
    'password': os.getenv('PGPASSWORD', ''),
    'host': os.getenv('PGHOST', 'localhost'),
    'port': os.getenv('PGPORT', '5432')
}

def setup_database():
    """Create database table if it doesn't exist"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS psx_data (
            id SERIAL PRIMARY KEY,
            scrape_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            symbol VARCHAR(50) NOT NULL,
            sector VARCHAR(100),
            listed_in VARCHAR(50),
            ldcp DECIMAL(10, 2),
            open DECIMAL(10, 2),
            high DECIMAL(10, 2),
            low DECIMAL(10, 2),
            current DECIMAL(10, 2),
            change DECIMAL(10, 2),
            change_percent DECIMAL(6, 2),
            volume BIGINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(scrape_timestamp, symbol)
        );
        
        CREATE INDEX IF NOT EXISTS idx_psx_data_timestamp ON psx_data(scrape_timestamp);
        CREATE INDEX IF NOT EXISTS idx_psx_data_symbol ON psx_data(symbol);
        CREATE INDEX IF NOT EXISTS idx_psx_data_sector ON psx_data(sector);
        """
        
        cur.execute(create_table_query)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database table setup completed")
        
    except Exception as e:
        logger.error(f"Database setup error: {e}")
        raise

def setup_driver():
    """Chrome driver setup - Optimized for cloud deployment"""
    logger.info("Setting up Chrome driver for cloud deployment...")
    
    chrome_options = Options()
    
    # Cloud-optimized options
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    # Performance optimizations for cloud
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-images')
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument('--disable-javascript')
    
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.images": 2,
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_settings.popups": 0,
    })
    
    # For Railway/Render cloud deployment
    chrome_options.binary_location = os.getenv('CHROME_BIN', '/usr/bin/chromium')
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.info("Chrome driver ready!")
        return driver
    except Exception as e:
        logger.error(f"Driver setup failed: {e}")
        # Fallback to direct path
        try:
            driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome driver ready (fallback)!")
            return driver
        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")
            raise

def extract_correct_psx_data(driver):
    """Extract PSX data with all columns"""
    try:
        url = "https://dps.psx.com.pk/market-watch"
        logger.info(f"Loading: {url}")
        
        driver.get(url)
        
        logger.info("Waiting for data to load...")
        wait = WebDriverWait(driver, 20)
        
        # Wait for table to be present
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        logger.info("Table found, extracting data...")
        
        # Give page extra time to render
        time.sleep(2)
        
        # Extract with JavaScript
        extract_script = """
            let allStocks = [];
            let tables = document.querySelectorAll('table');
            let targetTable = null;
            
            for (let table of tables) {
                let headers = table.querySelectorAll('th');
                if (headers.length >= 10) {
                    let headerText = Array.from(headers).map(h => h.innerText.trim()).join(' ');
                    if (headerText.includes('Symbol') || headerText.includes('Sector') || headerText.includes('LDCP')) {
                        targetTable = table;
                        break;
                    }
                }
            }
            
            if (!targetTable && tables.length > 0) {
                targetTable = tables[0];
            }
            
            if (!targetTable) return allStocks;
            
            let tbody = targetTable.querySelector('tbody');
            if (!tbody) return allStocks;
            
            let rows = tbody.querySelectorAll('tr');
            
            rows.forEach((row, rowIndex) => {
                let cells = row.querySelectorAll('td');
                
                if (cells.length >= 9) {
                    let symbol = cells[0]?.innerText.trim() || '';
                    
                    if (!symbol || symbol === '' || symbol.length > 20 || 
                        symbol.includes('Symbol') || symbol.includes('Last') ||
                        symbol === 'PSX' || symbol === 'KSE-100' ||
                        symbol.includes('Open') || symbol.includes('High') ||
                        symbol.includes('Low') || symbol.includes('Volume')) {
                        return;
                    }
                    
                    let stockData = {
                        symbol: symbol,
                        sector: cells[1]?.innerText.trim() || '',
                        listed_in: cells[2]?.innerText.trim() || '',
                        ldcp: cells[3]?.innerText.trim() || '0',
                        open: cells[4]?.innerText.trim() || '0',
                        high: cells[5]?.innerText.trim() || '0',
                        low: cells[6]?.innerText.trim() || '0',
                        current: cells[7]?.innerText.trim() || '0',
                        change: cells[8]?.innerText.trim() || '0',
                        change_percent: cells[9]?.innerText.trim() || '0',
                        volume: cells[10]?.innerText.trim() || '0'
                    };
                    
                    allStocks.push(stockData);
                }
            });
            
            return allStocks;
        """
        
        raw_data = driver.execute_script(extract_script)
        
        if not raw_data or len(raw_data) == 0:
            logger.warning("JavaScript extraction failed, trying manual extraction...")
            return extract_manual_complete(driver)
        
        logger.info(f"Extracted {len(raw_data)} stocks successfully")
        return raw_data
        
    except Exception as e:
        logger.error(f"Error in main extraction: {e}")
        return extract_manual_complete(driver)

def extract_manual_complete(driver):
    """Robust manual extraction"""
    logger.info("Using robust manual extraction...")
    
    try:
        stocks_data = []
        
        wait = WebDriverWait(driver, 25)
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        rows = table.find_elements(By.TAG_NAME, "tr")
        
        logger.info(f"Found {len(rows)} total rows")
        
        processed_count = 0
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                
                if len(cells) < 9:
                    continue
                
                symbol = cells[0].text.strip()
                
                if (not symbol or len(symbol) > 20 or 
                    any(keyword in symbol for keyword in ['Symbol', 'Last', 'Open', 'High', 'Low', 'Current', 'Change', 'Volume']) or
                    'PSX' in symbol or 'KSE' in symbol):
                    continue
                
                stock = {
                    'symbol': symbol,
                    'sector': cells[1].text.strip() if len(cells) > 1 else '',
                    'listed_in': cells[2].text.strip() if len(cells) > 2 else '',
                    'ldcp': cells[3].text.strip() if len(cells) > 3 else '0',
                    'open': cells[4].text.strip() if len(cells) > 4 else '0',
                    'high': cells[5].text.strip() if len(cells) > 5 else '0',
                    'low': cells[6].text.strip() if len(cells) > 6 else '0',
                    'current': cells[7].text.strip() if len(cells) > 7 else '0',
                    'change': cells[8].text.strip() if len(cells) > 8 else '0',
                    'change_percent': cells[9].text.strip() if len(cells) > 9 else '0',
                    'volume': cells[10].text.strip() if len(cells) > 10 else '0'
                }
                
                stocks_data.append(stock)
                processed_count += 1
                
                if processed_count % 50 == 0:
                    logger.info(f"   {processed_count} valid stocks processed...")
                
            except Exception:
                continue
        
        logger.info(f"Robust extraction: {len(stocks_data)} valid stocks")
        return stocks_data
        
    except Exception as e:
        logger.error(f"Robust extraction error: {e}")
        return []

def clean_numeric_value(value):
    """Clean and convert numeric values"""
    if not value or value in ['N/A', '-', '', ' ', 'NAN', 'NULL', 'N/S', 'N.S', 'n/s']:
        return '0'
    
    try:
        cleaned = str(value).strip()
        cleaned = cleaned.replace(',', '').replace(' ', '').replace('%', '')
        cleaned = cleaned.replace('(', '').replace(')', '').replace('$', '')
        cleaned = cleaned.replace('Rs.', '').replace('PKR', '').replace('\u20a8', '')  # Rupee symbol
        cleaned = cleaned.replace('--', '0').replace('---', '0')
        
        # Handle negative values
        if cleaned.startswith('-'):
            cleaned = '-' + cleaned[1:].lstrip()
        
        if not cleaned or cleaned == '-':
            return '0'
            
        float_val = float(cleaned)
        return str(float_val)
        
    except Exception:
        return '0'

def validate_stock_data(stock):
    """Validate stock data"""
    try:
        if not stock['symbol'] or len(stock['symbol']) > 20:
            return False
        
        current_price = float(clean_numeric_value(stock['current']))
        if current_price <= 0 or current_price > 100000:
            return False
            
        return True
    except Exception:
        return False

def save_to_postgresql(stocks_data: List[Dict[str, Any]], scrape_timestamp: datetime):
    """Save data to PostgreSQL database"""
    if not stocks_data:
        logger.warning("No data to save")
        return False
    
    conn = None
    cur = None
    saved_count = 0
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Prepare insert query
        insert_query = """
        INSERT INTO psx_data 
        (scrape_timestamp, symbol, sector, listed_in, ldcp, open, high, low, current, change, change_percent, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (scrape_timestamp, symbol) DO NOTHING
        """
        
        # Process and insert each stock
        for item in stocks_data:
            if validate_stock_data(item):
                try:
                    row_data = (
                        scrape_timestamp,
                        item['symbol'],
                        item['sector'],
                        item['listed_in'],
                        float(clean_numeric_value(item['ldcp'])) if clean_numeric_value(item['ldcp']) != '0' else 0,
                        float(clean_numeric_value(item['open'])) if clean_numeric_value(item['open']) != '0' else 0,
                        float(clean_numeric_value(item['high'])) if clean_numeric_value(item['high']) != '0' else 0,
                        float(clean_numeric_value(item['low'])) if clean_numeric_value(item['low']) != '0' else 0,
                        float(clean_numeric_value(item['current'])) if clean_numeric_value(item['current']) != '0' else 0,
                        float(clean_numeric_value(item['change'])) if clean_numeric_value(item['change']) != '0' else 0,
                        float(clean_numeric_value(item['change_percent'])) if clean_numeric_value(item['change_percent']) != '0' else 0,
                        int(float(clean_numeric_value(item['volume']))) if clean_numeric_value(item['volume']) != '0' else 0
                    )
                    
                    cur.execute(insert_query, row_data)
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing stock {item.get('symbol', 'unknown')}: {e}")
                    continue
        
        conn.commit()
        logger.info(f"Saved {saved_count} stocks to database at {scrape_timestamp}")
        
        # Log summary
        if saved_count > 0:
            summary_query = """
            SELECT 
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(current) as min_price,
                MAX(current) as max_price,
                SUM(volume) as total_volume
            FROM psx_data 
            WHERE scrape_timestamp = %s
            """
            cur.execute(summary_query, (scrape_timestamp,))
            summary = cur.fetchone()
            
            logger.info(f"Summary for {scrape_timestamp}:")
            logger.info(f"  Unique symbols: {summary[0]}")
            logger.info(f"  Price range: {summary[1]} - {summary[2]}")
            logger.info(f"  Total volume: {summary[3]:,}")
        
        return True
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def is_market_open() -> bool:
    """Check if current time is within PSX market hours"""
    try:
        # Get current time in Pakistan timezone
        now_pk = datetime.now(PAKISTAN_TIMEZONE)
        
        # Check if it's a weekday (Monday=0, Friday=4)
        if now_pk.weekday() >= 5:  # Saturday or Sunday
            return False
        
        # Parse market hours
        open_hour, open_minute = map(int, PSX_MARKET_OPEN.split(':'))
        close_hour, close_minute = map(int, PSX_MARKET_CLOSE.split(':'))
        
        # Create timezone-aware datetime objects for comparison
        market_open = now_pk.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
        market_close = now_pk.replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)
        
        # Check if current time is within market hours
        return market_open <= now_pk <= market_close
        
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return False

def get_next_scrape_time() -> datetime:
    """Calculate next scrape time (rounded to next 5-minute interval)"""
    now_pk = datetime.now(PAKISTAN_TIMEZONE)
    
    # Calculate minutes to add to round up to next 5-minute interval
    minutes_to_add = (SCRAPE_INTERVAL_MINUTES - (now_pk.minute % SCRAPE_INTERVAL_MINUTES)) % SCRAPE_INTERVAL_MINUTES
    if minutes_to_add == 0:
        minutes_to_add = SCRAPE_INTERVAL_MINUTES
    
    next_time = now_pk + timedelta(minutes=minutes_to_add)
    next_time = next_time.replace(second=0, microsecond=0)
    
    return next_time

def run_scraper():
    """Main scraping function to be scheduled"""
    if not is_market_open():
        logger.info("Market is closed. Skipping scrape.")
        return
    
    driver = None
    scrape_timestamp = datetime.now(PAKISTAN_TIMEZONE)
    
    logger.info(f"Starting scrape at {scrape_timestamp}")
    
    try:
        driver = setup_driver()
        stocks_data = extract_correct_psx_data(driver)
        
        if stocks_data and len(stocks_data) > 0:
            logger.info(f"Extracted {len(stocks_data)} stocks")
            
            # Save to PostgreSQL
            success = save_to_postgresql(stocks_data, scrape_timestamp)
            
            if success:
                logger.info(f"✓ Scrape completed successfully at {scrape_timestamp}")
                
                # Display sample data
                logger.info("Sample data (first 3 stocks):")
                for i in range(min(3, len(stocks_data))):
                    stock = stocks_data[i]
                    logger.info(f"  {stock['symbol']}: {stock['current']} ({stock['change_percent']}%)")
            else:
                logger.error("Failed to save data to database")
        else:
            logger.warning("No data extracted")
            
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("Browser closed")
            except Exception:
                pass

def schedule_scraper():
    """Schedule the scraper to run every 5 minutes during market hours"""
    logger.info("=" * 80)
    logger.info("PSX CLOUD SCRAPER - SCHEDULED DATABASE VERSION")
    logger.info("=" * 80)
    logger.info(f"Market Hours: {PSX_MARKET_OPEN} to {PSX_MARKET_CLOSE} PKT (Asia/Karachi)")
    logger.info(f"Scrape Interval: Every {SCRAPE_INTERVAL_MINUTES} minutes during market hours")
    logger.info("Data Storage: PostgreSQL (Cloud)")
    logger.info("=" * 80)
    
    # Setup database
    try:
        setup_database()
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        return
    
    # Function to run and schedule next
    def run_and_schedule():
        run_scraper()
        
        if is_market_open():
            # Schedule next run if market is still open
            next_time = get_next_scrape_time()
            wait_seconds = (next_time - datetime.now(PAKISTAN_TIMEZONE)).total_seconds()
            
            if wait_seconds > 0:
                logger.info(f"Next scrape scheduled at: {next_time.strftime('%H:%M:%S')} PKT")
                logger.info(f"Waiting {wait_seconds:.0f} seconds...")
                time.sleep(wait_seconds)
                run_and_schedule()
            else:
                # Should not happen, but just in case
                time.sleep(SCRAPE_INTERVAL_MINUTES * 60)
                run_and_schedule()
        else:
            # Market is closed, wait until next market day
            logger.info("Market closed for the day. Waiting for next market open...")
            
            # Calculate time until next market open
            now_pk = datetime.now(PAKISTAN_TIMEZONE)
            tomorrow = now_pk + timedelta(days=1)
            
            # Find next weekday
            while tomorrow.weekday() >= 5:  # Skip weekends
                tomorrow += timedelta(days=1)
            
            # Set to market open time
            open_hour, open_minute = map(int, PSX_MARKET_OPEN.split(':'))
            next_open = tomorrow.replace(hour=open_hour, minute=open_minute, second=0, microsecond=0)
            
            wait_seconds = (next_open - now_pk).total_seconds()
            logger.info(f"Next market opens at: {next_open.strftime('%Y-%m-%d %H:%M:%S')} PKT")
            logger.info(f"Waiting {wait_seconds/3600:.1f} hours...")
            time.sleep(wait_seconds)
            run_and_schedule()
    
    # Start the scheduling loop
    try:
        run_and_schedule()
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in scheduler: {e}")

def run_once():
    """Run scraper once (for testing or manual runs)"""
    logger.info("Running single scrape...")
    run_scraper()

if __name__ == "__main__":
    # Check if running in test mode
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        logger.info("Running in test mode (single execution)")
        run_once()
    else:
        # Start scheduled scraper
        schedule_scraper()
