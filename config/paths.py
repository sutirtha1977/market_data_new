from pathlib import Path

# =========================================================
# BASE PATHS
# =========================================================
BASE_DIR = Path(__file__).parent.parent

# ---------------- Directories ----------------
DATA_DIR = BASE_DIR / "data"
BHAVCOPY_DIR = DATA_DIR / "bhavcopy" / "daily"
YAHOO_DIR = DATA_DIR / "yahoo"
YAHOO_EQUITY_DIR = YAHOO_DIR / "equity"
YAHOO_INDEX_DIR = YAHOO_DIR / "index"
EXPORT_DIR = DATA_DIR / "exports"

SCANNER_FOLDER = EXPORT_DIR  # for scanner exports

# ---------------- Database ----------------
DB_FILE = BASE_DIR / "db" / "markets.db"

# ---------------- CSV ----------------
CSV_FILE = BASE_DIR / "data.csv"

# ---------------- NSE URLs ----------------
NSE_URL_BHAV_DAILY = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

# ---------------- Frequencies ----------------
FREQUENCIES = ["1d", "1wk", "1mo"]

# ---------------- Logging ----------------
LOG_FILE = BASE_DIR / "audit_trail.log"

# =========================================================
# Helper Functions
# =========================================================
def ensure_folder(path: Path):
    """Ensure the folder exists."""
    path.mkdir(parents=True, exist_ok=True)