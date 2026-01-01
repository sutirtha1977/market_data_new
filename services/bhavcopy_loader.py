import os
import requests
import traceback
from datetime import datetime, timedelta
from db.connection import get_db_connection, close_db_connection
from services.symbol_service import retrieve_equity_symbol, get_latest_equity_date
from config.logger import log
from config.paths import BHAVCOPY_DIR,NSE_URL_BHAV_DAILY
#################################################################################################
# Downloads a single NSE bhavcopy CSV file for a specific date.
# - If no date is provided, defaults to today's date formatted as ddmmyyyy.
# - Ensures the bhavcopy download folder exists.
# - Builds the download URL and destination file path using the provided date.
# - Sends a GET request to download the CSV; includes headers to mimic a browser.
# - If the file is unavailable (e.g., holiday, weekend, or missing), logs the HTTP status and returns None.
# - Saves the downloaded CSV content to the local folder when successful.
# - Logs both download initiation and successful save location.
# - Returns the full save path on success, or None on failure/exceptions.
#################################################################################################
def download_bhavcopy(date_str=None):
    """Download NSE bhavcopy for given date (ddmmyyyy)."""
    try:
        # ---- Today's bhavcopy if no date passed ----
        if date_str is None:
            date_str = datetime.now().strftime("%d%m%Y")
        # ---- Ensure folder exists ----
        os.makedirs(BHAVCOPY_DIR, exist_ok=True)
        # ---- Prepare URL and save path ----
        save_path = os.path.join(
            BHAVCOPY_DIR,
            f"sec_bhavdata_full_{date_str}.csv"
        )
        url = NSE_URL_BHAV_DAILY.format(date_str)
        log(f"⬇ Downloading bhavcopy: {date_str} -> {url}")
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,*/*;q=0.8"
        }
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            log(f"❗ HTTP {response.status_code} | File missing (holiday / weekend / not available)")
            return None
        with open(save_path, "wb") as f:
            f.write(response.content)
        log(f"✔ Saved: {save_path}")
        return save_path
    except Exception as e:
        log(f"❗ Unexpected error: {e}")
        traceback.print_exc()
        return None
#################################################################################################
# Downloads daily NSE bhavcopy CSV files for all missing dates based on the latest date in the database.
# - Determines the most recent equity price date using get_latest_equity_date();
#   optionally overrides it when override_date is provided.
# - If the database has no price history, defaults to downloading files for the past 30 days.
# - Computes the date range to download: latest_date + 1 day → today.
# - Skips downloading when the database is already up to date.
# - Ensures the bhavcopy folder exists, then clears all existing files to avoid mixing old data.
# - Iterates through each date in the target range:
#       • formats date as ddmmyyyy string
#       • logs the processing date
#       • calls download_bhavcopy() to fetch the corresponding CSV
# - Tracks and logs the total number of bhavcopy files downloaded.
# - Prints completion summary to console.
# Returns nothing; downloaded files are saved directly to the configured NSE_BHAVCOPY_DAILY folder.
#################################################################################################
def download_missing_bhavcopies(override_date=None):
    log("🚀 Starting bhavcopy download process...")

    latest_date = get_latest_equity_date()
    if override_date:
        latest_date = datetime.strptime(override_date, "%Y-%m-%d").date()
        log(f"⚠ OVERRIDE latest date: {latest_date}")
    # if DB empty or override empty case
    if latest_date is None:
        log("⚠ No price data found in DB. Starting fresh from today-30days.")
        latest_date = datetime.now().date() - timedelta(days=30)

    start_date = latest_date + timedelta(days=1)
    today = datetime.now().date()

    if start_date > today:
        log("✔ No missing dates. Database already up to date.")
        return

    # ---- 🔥 CLEAR ONLY ONCE ----
    os.makedirs(BHAVCOPY_DIR, exist_ok=True)

    for filename in os.listdir(BHAVCOPY_DIR):
        file_path = os.path.join(BHAVCOPY_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            log(f"❗ Failed to delete {file_path}: {e}")

    log(f"🧹 Cleared old files in {BHAVCOPY_DIR}")

    # ---- 🔽 DOWNLOAD MULTIPLE FILES ----
    download_count = 0
    curr = start_date
    while curr <= today:
        formatted = curr.strftime("%d%m%Y")
        log(f"📌 Processing {curr.strftime('%Y-%m-%d')}")

        # download without clearing
        download_bhavcopy(formatted)

        download_count += 1
        curr += timedelta(days=1)

    log(f"🎉 Download completed. Total downloaded: {download_count}")
    print(f"🎉 Download completed. Total downloaded: {download_count}")
#################################################################################################
# Updates daily equity price data in the database using downloaded NSE bhavcopy CSV files.
# - Establishes a DB connection and cursor for executing SQL operations.
# - Retrieves the list of target equity symbols using retrieve_equity_symbol();
#   skips processing if no symbols are found.
# - Scans the bhavcopy folder for all CSV files matching the pattern "sec_bhavdata_full_*".
# - Defines an UPSERT SQL statement to insert/update daily price records in equity_price_data.
# - Iterates over each bhavcopy CSV file:
#       • extracts the trading date from the filename (ddmmyyyy → yyyy-mm-dd)
#       • loads the CSV into a DataFrame and normalizes column names
# - For each symbol in the symbol list:
#       • finds the matching record in the CSV based on SYMBOL column
#       • maps bhavcopy fields to target DB columns:
#            OPEN_PRICE  → open
#            HIGH_PRICE  → high
#            LOW_PRICE   → low
#            LAST_PRICE  → close
#            CLOSE_PRICE → adj_close
#            TTL_TRD_QNTY → volume
#            DELIV_PER   → delv_pct
#       • inserts or updates the record for timeframe "1d" in equity_price_data
#       • logs per-symbol update status
# - Commits the transaction once all files and symbols are processed.
# - Maintains a running count of all inserted/updated rows and logs the final total.
# - Rolls back the transaction on fatal errors to preserve data consistency.
# - Always closes the DB connection in the finally block.
# Returns nothing; writes updated price data directly into the database.
#################################################################################################
def update_equity_price_from_bhavcopy(symbol="ALL"):

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        log("🚀 Starting equity_price_data update from bhavcopy CSV files")

        # ---- Load symbols ----
        df_symbols = retrieve_equity_symbol(symbol, conn)
        if df_symbols.empty:
            log("❗ No symbols found to process")
            return

        log(f"🔎 Symbols to process: {len(df_symbols)}")

        # ---- Locate CSV files ----
        csv_files = sorted([
            f for f in os.listdir(NSE_BHAVCOPY_DAILY)
            if f.endswith(".csv") and "sec_bhavdata_full_" in f
        ])

        if not csv_files:
            log("❗ No bhavcopy CSV files found to process")
            return

        insert_sql = """
            INSERT INTO equity_price_data
            (symbol_id, timeframe, date, open, high, low, close, adj_close, volume, delv_pct, is_final)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(symbol_id, timeframe, date)
            DO UPDATE SET
                open      = excluded.open,
                high      = excluded.high,
                low       = excluded.low,
                close     = excluded.close,
                adj_close = excluded.adj_close,
                volume    = excluded.volume,
                delv_pct  = excluded.delv_pct
        """

        total_updates = 0

        # ---- Process each CSV ----
        for file in csv_files:
            csv_path = os.path.join(NSE_BHAVCOPY_DAILY, file)

            # ---- Extract date (ddmmyyyy) from filename ----
            try:
                date_str = file.split("_")[-1].split(".")[0]  # 31122025
                file_date = datetime.strptime(date_str, "%d%m%Y").strftime("%Y-%m-%d")
            except Exception:
                log(f"⚠ Skipping invalid filename format: {file}")
                continue

            log(f"\n📂 Processing: {file} | Date: {file_date}")

            # ---- Load CSV ----
            try:
                df_csv = pd.read_csv(csv_path)
            except Exception:
                log(f"❗ Failed reading CSV: {file}")
                continue

            if df_csv.empty:
                log(f"⚠ Empty CSV, skipping: {file}")
                continue

            # ---- Normalize column names ----
            df_csv.columns = [c.strip().upper() for c in df_csv.columns]

            # ---- Process each symbol ----
            for _, row_sym in df_symbols.iterrows():
                sid = row_sym["symbol_id"]
                sym = row_sym["symbol"]

                df_row = df_csv[df_csv["SYMBOL"] == sym]

                if df_row.empty:
                    log(f"⚠ {sym}: not found in CSV for {file_date}")
                    continue

                df_row = df_row.iloc[0]  # first match

                # ---- Map columns ----
                record = (
                    sid,
                    "1d",
                    file_date,
                    df_row.get("OPEN_PRICE", None),
                    df_row.get("HIGH_PRICE", None),
                    df_row.get("LOW_PRICE", None),
                    df_row.get("LAST_PRICE", None),
                    df_row.get("CLOSE_PRICE", None),
                    df_row.get("TTL_TRD_QNTY", None),
                    df_row.get("DELIV_PER", None),
                )

                cur.execute(insert_sql, record)
                total_updates += 1

                log(f"✔ {sym:<12} updated for {file_date}")

        conn.commit()
        log(f"\n🎉 Update complete — total DB rows inserted/updated: {total_updates}")
        print(f"\n🎉 Update complete — total DB rows inserted/updated: {total_updates}")

    except Exception as e:
        log(f"❗ ERROR during update: {e}")
        conn.rollback()

    finally:
        close_db_connection(conn)
        log("🔚 DB connection closed")