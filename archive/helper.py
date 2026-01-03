import os
from datetime import datetime

# =========================================================
# BASE PATHS
# =========================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

DB_FILE = os.path.join(BASE_DIR, "database", "markets.db")
LOG_FILE = os.path.join(BASE_DIR, "price_loader.log")

SCANNER_FOLDER = os.path.join(BASE_DIR, "scanner_files")
NSE_BHAVCOPY_DAILY = os.path.join(BASE_DIR, "nse_bhav_copy", "daily")
BHAVCOPY_EQUITY_FOLDER = os.path.join(BASE_DIR, "nse_bhav_copy", "equity")
BHAVCOPY_INDEX_FOLDER = os.path.join(BASE_DIR, "nse_bhav_copy", "index")

YAHOO_FILES_EQUITY = os.path.join(BASE_DIR, "yahoo_files", "equity")
YAHOO_FILES_INDEX = os.path.join(BASE_DIR, "yahoo_files", "index")

CSV_FILE = os.path.join(BASE_DIR, "data.csv")
FREQUENCIES = ["1d", "1wk", "1mo"]

# =========================================================
# NSE BHAVCOPY URL
# =========================================================
NSE_URL_BHAV_DAILY = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

# =========================================================
# MENU AND COLORS
# =========================================================
FREQ_COLORS = {
    "Run Once": "bold blue",
    "Run Daily": "bold yellow",
    "Run As Required": "bold green",
    "": "white"
}

MAIN_MENU_ITEMS = [
    ("1", "Create Database, Update Equity and Index Symbols", "Run Once", "red"),
    ("2", "Download Equity and Index data (Yahoo) and update equity and index price tables", "Run Once", "blue"),
    ("3", "Update 52 weeks stats", "Run Once", "blue"),
    ("4", "Update Equity and Index Indicators", "Run Once", "blue"),
    ("5", "Download BhavCopy and Update Equity Price Table", "Run Daily", "yellow"),
    ("6", "Update 52 weeks stats Daily", "Run Daily", "yellow"),
    ("7", "Update Equity and Index Indicators Daily", "Run Daily", "yellow"),
    ("8", "SCANNER: Hilega Milega", "Run Daily", "yellow"),
    ("9", "SCANNER: WIP", "Run Daily", "yellow"),
    ("10", "Backtest SCANNER: File Name", "Run Daily", "yellow"),
    ("0", "Exit", "", "white"),
]

# =========================================================
# NSE INDICES
# Format: (index_code, index_name, exchange, yahoo_symbol, category)
# =========================================================
NSE_INDICES = [
    ("NIFTY50", "NIFTY 50", "NSE", "^NSEI", "Broad"),
    ("NIFTYNEXT50", "NIFTY Next 50", "NSE", "^NSMIDCP", "Broad"),
    ("NIFTY100", "NIFTY 100", "NSE", "^CNX100", "Broad"),
    ("NIFTY200", "NIFTY 200", "NSE", "^CNX200", "Broad"),
    ("NIFTY500", "NIFTY 500", "NSE", "^CRSLDX", "Broad"),
    ("BANKNIFTY", "NIFTY Bank", "NSE", "^NSEBANK", "Sectoral"),
    ("ITNIFTY", "NIFTY IT", "NSE", "^CNXIT", "Sectoral"),
    ("PHARMANIFTY", "NIFTY Pharma", "NSE", "^CNXPHARMA", "Sectoral"),
    ("FMCGNIFTY", "NIFTY FMCG", "NSE", "^CNXFMCG", "Sectoral"),
    ("AUTONIFTY", "NIFTY Auto", "NSE", "^CNXAUTO", "Sectoral"),
    ("METALNIFTY", "NIFTY Metal", "NSE", "^CNXMETAL", "Sectoral"),
    ("REALTYNIFTY", "NIFTY Realty", "NSE", "^CNXREALTY", "Sectoral"),
    ("PSUBANKNIFTY", "NIFTY PSU Bank", "NSE", "^CNXPSUBANK", "Sectoral"),
    ("INDIAVIX", "India VIX", "NSE", "^INDIAVIX", "Volatility"),
]

# =========================================================
# LOGGING
# =========================================================
def log(message: str):
    """Append a message to the log file with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def clear_log():
    """Clear the log file."""
    try:
        with open(LOG_FILE, "w") as f:
            f.write("")
        print(f"LOG CLEARED: {LOG_FILE}")
    except Exception as e:
        print(f"FAILED TO CLEAR LOG: {e}")

# =========================================================
# PATH HELPERS
# =========================================================
def ensure_folder(path):
    """Ensure the folder exists."""
    os.makedirs(path, exist_ok=True)