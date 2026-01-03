
import sqlite3
import pandas as pd
import os
import requests
import traceback
import shutil
import yfinance as yf
import time
import sys
from datetime import datetime, timedelta
from helper import (
    log, 
    DB_FILE,NSE_INDICES,
    CSV_FILE, FREQUENCIES,
    YAHOO_FILES_EQUITY,
    YAHOO_FILES_INDEX,
    NSE_URL_BHAV_DAILY,
    NSE_BHAVCOPY_DAILY
)
from indicators_helper import (
    calculate_rsi_series,
    calculate_bollinger,
    calculate_atr,
    calculate_macd,
    calculate_supertrend,
    calculate_ema,
    calculate_wma
)
from sql import SQL_INSERT
################################################################################################# 
# Opens and returns a SQLite database connection with performance and safety pragmas enabled, 
# logging and raising any connection errors
#################################################################################################
def get_db_connection():
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn
    except Exception as e:
        log(f"DB CONNECTION FAILED: {e}")
        raise
#################################################################################################
# Closes the given database connection safely, logging any errors if the close fails.
#################################################################################################
def close_db_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        log(f"DB CLOSE FAILED: {e}")
#################################################################################################       
# Refresh the `equity_symbols` table using values from a master CSV file.
# Process:
# 1. Opens a database connection and reads the CSV defined by `CSV_FILE`.
# 2. Automatically detects key columns such as:
#    - Symbol, Name, Series, Listing Date, ISIN
#    even when column names vary in the CSV.
# 3. Cleans and normalizes row data:
#    - Converts symbol to uppercase
#    - Strips whitespace
#    - Parses listing dates to ISO format
#    - Filters invalid placeholder values (NA, N/A, -)
# 4. Builds a list of all valid records and:
#    - Inserts new symbols (`INSERT OR IGNORE`)
#    - Updates missing fields for already existing symbols:
#         • series        → when NULL or empty
#         • listing_date  → when NULL
#         • isin          → when NULL or empty
# 5. Commits all changes and logs the number of inserted and updated rows.
# 6. Ensures the database connection is closed even on error.
# Purpose:
# Keeps the `equity_symbols` table up to date with symbol metadata from the latest CSV,
# without overwriting existing valid data.
# https://www.nseindia.com/static/market-data/securities-available-for-trading
#################################################################################################
def refresh_equity():
    try:
        conn = get_db_connection()
        df = pd.read_csv(CSV_FILE)

        # ---------- Column detection ----------
        symbol_col = next((c for c in df.columns if c.lower() == 'symbol'), 'Symbol')
        name_col = next((c for c in df.columns if c.lower() in ('stock name', 'name')), 'Stock Name')

        series_candidates = [c for c in df.columns if 'series' in c.lower()]
        series_col = series_candidates[0] if series_candidates else None

        listing_candidates = [
            c for c in df.columns
            if 'list' in c.lower() and 'date' in c.lower()
        ]
        listing_col = listing_candidates[0] if listing_candidates else None

        isin_candidates = [c for c in df.columns if 'isin' in c.lower()]
        isin_col = isin_candidates[0] if isin_candidates else None

        # ---------- Build column list ----------
        cols = [symbol_col, name_col]
        if series_col:
            cols.append(series_col)
        if listing_col:
            cols.append(listing_col)
        if isin_col:
            cols.append(isin_col)

        iterable = (
            df[cols]
            .dropna(subset=[symbol_col, name_col])
            .drop_duplicates()
        )

        records = []
        updates_series = []
        updates_listing = []
        updates_isin = []

        # ---------- Row processing ----------
        for _, row in iterable.iterrows():
            symbol = str(row[symbol_col]).strip().upper()
            name = str(row[name_col]).strip()

            # Series
            series = None
            if series_col:
                raw = row.get(series_col)
                if pd.notna(raw):
                    s = str(raw).strip().upper()
                    if s and s not in ('NA', 'N/A', '-'):
                        series = s

            # Listing date
            listing_date = None
            if listing_col:
                raw = row.get(listing_col)
                if pd.notna(raw):
                    dt = pd.to_datetime(raw, errors='coerce')
                    if pd.notna(dt):
                        listing_date = dt.date().isoformat()

            # ISIN
            isin = None
            if isin_col:
                raw = row.get(isin_col)
                if pd.notna(raw):
                    s = str(raw).strip().upper()
                    if s and s not in ('NA', 'N/A', '-'):
                        isin = s

            records.append(
                (symbol, name, 'NSE', series, listing_date, isin)
            )

            if series:
                updates_series.append((series, symbol))
            if listing_date:
                updates_listing.append((listing_date, symbol))
            if isin:
                updates_isin.append((isin, symbol))

        # ---------- Database write ----------
        if records:
            conn.executemany("""
                INSERT OR IGNORE INTO equity_symbols
                (symbol, name, exchange, series, listing_date, isin)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)

            if updates_series:
                conn.executemany(
                    "UPDATE equity_symbols SET series = ? "
                    "WHERE symbol = ? AND (series IS NULL OR series = '')",
                    updates_series
                )

            if updates_listing:
                conn.executemany(
                    "UPDATE equity_symbols SET listing_date = ? "
                    "WHERE symbol = ? AND listing_date IS NULL",
                    updates_listing
                )

            if updates_isin:
                conn.executemany(
                    "UPDATE equity_symbols SET isin = ? "
                    "WHERE symbol = ? AND (isin IS NULL OR isin = '')",
                    updates_isin
                )

            conn.commit()

            log(
                f"Inserted {len(records)} symbols | "
                f"Updated series:{len(updates_series)}, "
                f"listing_date:{len(updates_listing)}, "
                f"isin:{len(updates_isin)}"
            )
        else:
            log("No symbol records to insert")

    except Exception as e:
        log(f"Error refreshing stock symbols: {e}")
        raise
    finally:
        close_db_connection(conn)
#################################################################################################
# Refresh the `index_symbols` table with the latest predefined index list.
# Process:
# 1. Opens a database connection and validates that the `index_symbols` table
#    contains all expected schema columns.
# 2. Builds a list of index records from the predefined `NSE_INDICES` data.
# 3. Inserts any new indices into the table (`INSERT OR IGNORE`).
# 4. Reactivates existing indices by setting `is_active = 1` for all provided index codes.
# 5. Commits changes and logs how many index records are processed.
# 6. Ensures the database connection is closed even if errors occur.
# Purpose:
# Keeps the index symbol master table updated and active with all current indices.
#################################################################################################
def refresh_indices():
    try:
        conn = get_db_connection()
        # Safety check: ensure schema matches expectations
        cols = {row[1] for row in conn.execute("PRAGMA table_info(index_symbols)")}
        required = {
            "index_id",
            "index_code",
            "index_name",
            "exchange",
            "yahoo_symbol",
            "category",
            "is_active"
        }

        if not required.issubset(cols):
            raise RuntimeError(
                f"index_symbols table schema mismatch. Found columns: {cols}"
            )
        # Prepare records
        # NSE_INDICES format:
        # (index_code, index_name, exchange, yahoo_symbol, category)
        records = [
            (code, name, exch, yahoo, category, 1)
            for (code, name, exch, yahoo, category) in NSE_INDICES
        ]

        # Insert new indices
        conn.executemany("""
            INSERT OR IGNORE INTO index_symbols
            (index_code, index_name, exchange, yahoo_symbol, category, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, records)

        # Reactivate indices already present
        conn.executemany("""
            UPDATE index_symbols
            SET is_active = 1
            WHERE index_code = ?
        """, [(r[0],) for r in records])

        conn.commit()
        log(f"Index symbols refreshed: {len(records)} total")
    except Exception as e:
        log(f"Error refreshing index symbols: {e}")
        raise
    finally:
        close_db_connection(conn)
#################################################################################################
# Refreshes 52-week high and low statistics for both equity and index symbols.
# For each price table (equity and index):
#   - Collects all symbols that have daily price data.
#   - Computes the maximum high and minimum low from the last year (52 weeks).
#   - Inserts or updates these values into the corresponding 52-week stats table
#     using an UPSERT to keep the record current.
# Commits changes per table and handles errors, ensuring DB cleanup.
#################################################################################################      
def refresh_52week_stats():
    mapping = [
        ("equity_price_data", "equity_52week_stats", "symbol_id"),
        ("index_price_data",  "index_52week_stats",  "index_id")
    ]

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for price_table, stats_table, id_col in mapping:
            log(f"📊 Updating 52W stats for {price_table}")

            # Get all symbol/index ids having daily data
            cur.execute(f"""
                SELECT DISTINCT {id_col}
                FROM {price_table}
                WHERE timeframe='1d'
            """)
            ids = [r[0] for r in cur.fetchall()]
            if not ids:
                log(f"⚠ No daily data found in {price_table}, skipping")
                continue

            # Fetch 52W high/low for all
            placeholders = ",".join("?" * len(ids))
            cur.execute(f"""
                SELECT {id_col}, MAX(high), MIN(low)
                FROM {price_table}
                WHERE timeframe='1d'
                  AND {id_col} IN ({placeholders})
                  AND date >= date('now', '-1 year')
                GROUP BY {id_col}
            """, ids)

            results = [(sid, high, low) for sid, high, low in cur.fetchall() if high is not None]
            if not results:
                log(f"⚠ No 52W data found in {price_table}")
                continue

            # UPSERT
            for sid, high52, low52 in results:
                cur.execute(f"""
                    INSERT INTO {stats_table}
                        ({id_col}, week52_high, week52_low, as_of_date)
                    VALUES (?, ?, ?, date('now'))
                    ON CONFLICT({id_col}) DO UPDATE SET
                        week52_high = excluded.week52_high,
                        week52_low  = excluded.week52_low,
                        as_of_date  = excluded.as_of_date
                """, (sid, high52, low52))

            conn.commit()
            log(f"✅ {stats_table}: Updated {len(results)} rows")

    except Exception as e:
        conn.rollback()
        log(f"❌ 52W update failed: {e}")

    finally:
        cur.close()
        close_db_connection(conn)  
#################################################################################################
# Retrieve equity symbol records from the database based on user input.
# Behavior:
# 1. If `symbol` is empty → return an empty DataFrame.
# 2. If `symbol` is "ALL" (case-insensitive) → return all symbols from `equity_symbols`.
# 3. Otherwise:
#    - Parse the input as a comma-separated list (e.g., "TCS, INFY, RELIANCE").
#    - Validate and clean each symbol name.
#    - Query and return matching symbol records (symbol_id, symbol) in alphabetical order.
# Returns:
# - A pandas DataFrame with the selected symbols, or an empty frame when no match is found.
#################################################################################################
def retrieve_equity_symbol(symbol, conn):
    try:
        # --- Normalize input ---
        if not symbol or not symbol.strip():
            log("No symbol provided")
            return pd.DataFrame()

        symbol_clean = symbol.strip().upper()

        # --- Get all symbols ---
        if symbol_clean == "ALL":
            return pd.read_sql(
                "SELECT symbol_id, symbol FROM equity_symbols ORDER BY symbol",
                conn
            )

        # --- Parse comma-separated list ---
        symbols = {s.strip().upper() for s in symbol.split(",") if s.strip()}
        if not symbols:
            log("No valid symbols parsed")
            return pd.DataFrame()

        placeholders = ",".join("?" for _ in symbols)

        query = f"""
            SELECT symbol_id, symbol
            FROM equity_symbols
            WHERE symbol IN ({placeholders})
            ORDER BY symbol
        """
        return pd.read_sql(query, conn, params=list(symbols))

    except Exception as e:
        log(f"RETRIEVE SYMBOL FAILED: {e}")
        return pd.DataFrame()
#################################################################################################
# Download historical equity price data for one or more symbols across
# all configured timeframes and save each dataset as a CSV file.
# Process:
# 1. Retrieve the list of equity symbols matching the input filter (`symbol`).
# 2. For each timeframe defined in FREQUENCIES (e.g., 1d, 1wk, 1mo):
#    - Create the corresponding directory if missing.
#    - For every retrieved symbol:
#         * Build the Yahoo Finance ticker (symbol + ".NS").
#         * Download full historical price data (`period="max"`).
#         * Normalize the dataframe by flattening multi-index columns and
#           converting the index to a date column.
#         * Save the result as a CSV file named <symbol>.csv in the timeframe folder.
# 3. Log progress and skip symbols without data or download failures.
# This function prepares the raw price data required for subsequent
# database imports and indicator calculations.
#################################################################################################    
def download_equity_yahoo_data_all_timeframes(symbol):
    try:
        conn = get_db_connection()
        # --- fetch symbol list ---
        symbols_df = retrieve_equity_symbol(symbol, conn)
        if symbols_df.empty:
            log("NO SYMBOLS FOUND")
            return
        # Download yahoo data for all frequncy and save in csv format START
        for timeframe in FREQUENCIES:

            # --- create folder for timeframe ---
            timeframe_path = os.path.join(YAHOO_FILES_EQUITY, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)

            for _, row in symbols_df.iterrows():
                symbol_name = row["symbol"]
                yahoo_symbol = f"{symbol_name}.NS"
                csv_path = os.path.join(timeframe_path, f"{symbol_name}.csv")
                try:
                    # --- download full data from yahoo finance ---
                    print(f"Downloading {yahoo_symbol} | {timeframe}")
                    df = yf.download(
                        yahoo_symbol,
                        period="max",
                        interval=timeframe,
                        auto_adjust=False,
                        progress=False
                    )

                    if df is None or df.empty:
                        log(f"{yahoo_symbol} | {timeframe} | NO DATA")
                        continue
                    # --- fix: drop multi-index ticker level so only Open/High/Low/etc stay ---
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)
                    # --- save dataframe to csv with date index as column ---
                    df.reset_index(inplace=True)
                    df.to_csv(csv_path, index=False)

                    # log(f"{yahoo_symbol} | {timeframe} | SAVED -> {csv_path} ({len(df)} rows)")

                except Exception as e:
                    log(f"❌ DOWNLOAD FAILED | {yahoo_symbol} {timeframe} | {e}")

        log("🎉 CSV EXPORT COMPLETE FOR ALL TIMEFRAMES")
        # Download yahoo data for all frequncy and save in csv format END

    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")

    finally:
        close_db_connection(conn)
#################################################################################################
# Load and store historical equity price data from CSV files into the database.
# For each timeframe folder (e.g., 1d, 1wk, 1mo), this function:
# 1. Reads every CSV file downloaded previously from Yahoo Finance.
# 2. Matches the filename to a known equity symbol to retrieve its symbol_id.
# 3. Cleans and normalizes the CSV data (column trimming, date formatting, rounding).
# 4. Inserts or updates records into the `equity_price_data` table,
#    marking each row as final (`is_final = 1`).
# 5. Skips missing symbols or empty CSVs and logs each import result.
# All timeframes and symbols are processed in a batch, ensuring the database
# is continuously updated with the latest equity price history.
#################################################################################################
def import_equity_csv_to_db():
    try:
        conn = get_db_connection()

        for timeframe in FREQUENCIES:
            
            timeframe_path = os.path.join(YAHOO_FILES_EQUITY, timeframe)
            if not os.path.exists(timeframe_path):
                log(f"No folder for timeframe '{timeframe}', skipping")
                continue

            log(f"===== IMPORTING CSV DATA FOR TIMEFRAME '{timeframe}' =====")

            # Iterate over all CSV files in the timeframe folder
            for csv_file in os.listdir(timeframe_path):
                if not csv_file.endswith(".csv"):
                    continue

                csv_path = os.path.join(timeframe_path, csv_file)
                symbol_name = os.path.splitext(csv_file)[0]

                # Get symbol_id from equity_symbols
                cur = conn.cursor()
                cur.execute("SELECT symbol_id FROM equity_symbols WHERE symbol = ?", (symbol_name,))
                res = cur.fetchone()
                if not res:
                    log(f"Symbol '{symbol_name}' not found in equity_symbols table, skipping")
                    continue
                symbol_id = res[0]

                try:
                    df = pd.read_csv(csv_path)
                    if df.empty:
                        log(f"{symbol_name} | {timeframe} | CSV empty, skipping")
                        continue

                    # Ensure proper column names
                    df.columns = [c.strip() for c in df.columns]

                    # Convert Date column to proper format
                    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
                    # Round numeric columns to 2 decimals
                    for col in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
                        df[col] = df[col].round(2)
                    # Prepare rows
                    rows = [
                        (
                            symbol_id,
                            timeframe,
                            row["Date"],
                            row["Open"],
                            row["High"],
                            row["Low"],
                            row["Close"],
                            row["Adj Close"],
                            row["Volume"]
                        )
                        for _, row in df.iterrows()
                    ]

                    # Insert/Update into DB
                    insert_sql = """
                        INSERT INTO equity_price_data
                        (symbol_id, timeframe, date, open, high, low, close, adj_close, volume, is_final)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                        ON CONFLICT(symbol_id, timeframe, date) DO UPDATE SET
                            open=excluded.open,
                            high=excluded.high,
                            low=excluded.low,
                            close=excluded.close,
                            adj_close=excluded.adj_close,
                            volume=excluded.volume
                    """
                    conn.executemany(insert_sql, rows)
                    conn.commit()
                    # log(f"{symbol_name} | {timeframe} | INSERTED/UPDATED {len(rows)} rows")

                except Exception as e:
                    log(f"❌ FAILED {symbol_name} | {timeframe} | {e}")
                    traceback.print_exc()

        log("🎉 ALL CSV FILES IMPORTED INTO DATABASE")

    except Exception as e:
        log(f"CRITICAL FAILURE import_equity_csv_to_db | {e}")
        traceback.print_exc()
    finally:
        close_db_connection(conn)
#################################################################################################
# Remove inconsistent weekly records from price tables.
# Deletes all rows in the weekly timeframe ('1wk') where the date does not fall
# on a Monday, ensuring weekly data aligns with the start of the week.
# If `is_index` is True, cleanup is applied to `index_price_data`;
# otherwise, it is applied to `equity_price_data`.
# This maintains consistent weekly data before further processing.
#################################################################################################
def delete_non_monday_weekly(is_index=False):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # pick table based on flag
        table = "index_price_data" if is_index else "equity_price_data"

        log(f"Deleting non-Monday weekly rows from '{table}'...")

        sql = f"""
            DELETE FROM {table}
            WHERE timeframe = '1wk'
              AND strftime('%w', date) <> '1'
        """

        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()

        log(f"🗑️  Deleted {affected} non-Monday weekly rows from '{table}'")

    except Exception as e:
        log(f"❌ Failed to delete rows from '{table}': {e}")
        traceback.print_exc()

    finally:
        close_db_connection(conn)
#################################################################################################
# Delete files
#################################################################################################  
def delete_wkly_mthly_yahoo_files(folder_path):
    try:
        if not os.path.exists(folder_path):
            print(f"Folder does not exist: {folder_path}")
            return

        deleted = 0
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".csv"):   # match .csv or .CSV
                filepath = os.path.join(folder_path, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
                    deleted += 1

        print(f"Deleted {deleted} .csv files from: {folder_path}")
    except Exception as e:
        log(f"❌ Failed to delete .csv files from: {folder_path}: {e}")
        traceback.print_exc()
#################################################################################################
# End-to-end workflow to refresh equity price data for a specific symbol.
# Steps performed:
#   1. Download historical price data for the given equity symbol across all
#      supported timeframes using Yahoo Finance (`download_equity_yahoo_data_all_timeframes`).
#   2. Import all downloaded equity CSV files into the database
#      (`import_equity_csv_to_db`), inserting new records or updating existing ones.
#   3. Remove any weekly records that are not Monday to maintain consistent
#      week-start alignment (`delete_non_monday_weekly` with is_index=False).
# This function automates the full data refresh cycle — download ➝ import ➝ cleanup —
# for one equity symbol.
#################################################################################################
def insert_equity_price_data(symbol):
    try:
        log(f"===== YAHOO DOWNLOAD STARTED =====")
        download_equity_yahoo_data_all_timeframes(symbol)
        log(f"===== YAHOO DOWNLOAD FINISHED =====")
        log(f"===== CSV TO DATABASE IMPORT STARTED =====")
        import_equity_csv_to_db()
        log(f"===== CSV TO DATABASE IMPORT FINISHED =====")
        log(f"===== DELETE NON MONDAY FOR WEEK STARTED =====")
        delete_non_monday_weekly(is_index=False)
        log(f"===== DELETE NON MONDAY FOR WEEK FINISHED =====")
        # log(f"===== UPDATE 52 WEEK STAT STARTED =====")
        # refresh_52week_stats()
        # log(f"===== UPDATE 52 WEEK STAT FINISHED =====")
        log(f"===== DELETE FILES FROM FOLDERS STARTED =====")
        for timeframe in FREQUENCIES:
            folder_path = os.path.join(YAHOO_FILES_EQUITY, timeframe)
            delete_wkly_mthly_yahoo_files(folder_path)
        log(f"===== DELETE FILES FROM FOLDERS FINISHED =====")
    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")
#################################################################################################
# Downloads historical price data from Yahoo Finance for all active indices across
# all defined timeframes, and saves each dataset as a CSV file grouped by timeframe.
# Process:
#   1. Retrieve all active indices and their Yahoo symbols from `index_symbols`.
#   2. For each timeframe (1d, 1wk, 1mo, etc.):
#         - Ensure the timeframe folder exists under YAHOO_FILES_INDEX.
#         - Download full historical price data (`period="max"`) for each index.
#         - Flatten multi-index columns if present, reset index, and export to CSV.
#   3. Logs progress and errors throughout.

# This function generates fresh CSV data for indices, ready for database import.
#################################################################################################       
def download_index_yahoo_data_all_timeframes():
    try:
        conn = get_db_connection()

        # --- fetch active indices ---
        cur = conn.cursor()
        cur.execute("""
            SELECT index_id, index_code, yahoo_symbol
            FROM index_symbols
            WHERE is_active = 1
        """)
        indices = cur.fetchall()

        if not indices:
            log("NO ACTIVE INDICES FOUND")
            return

        for timeframe in FREQUENCIES:
            log(f"===== DOWNLOADING full '{timeframe}' data for all indices =====")

            # --- create folder for timeframe ---
            timeframe_path = os.path.join(YAHOO_FILES_INDEX, timeframe)
            os.makedirs(timeframe_path, exist_ok=True)

            for index_id, index_code, yahoo_symbol in indices:
                csv_path = os.path.join(timeframe_path, f"{index_code}.csv")
                log(f"Downloading {yahoo_symbol} | {timeframe}")

                try:
                    # full download from yahoo
                    df = yf.download(
                        yahoo_symbol,
                        period="max",
                        interval=timeframe,
                        auto_adjust=False,
                        progress=False
                    )

                    if df is None or df.empty:
                        log(f"{index_code} | {timeframe} | NO DATA")
                        continue

                    # drop multi-index column level
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)

                    # reset index for CSV export
                    df.reset_index(inplace=True)

                    df.to_csv(csv_path, index=False)
                    # log(f"{index_code} | {timeframe} | SAVED -> {csv_path} ({len(df)} rows)")

                except Exception as e:
                    log(f"❌ DOWNLOAD FAILED | {index_code} {timeframe} | {e}")

        log("🎉 CSV EXPORT COMPLETE FOR ALL INDICES + ALL TIMEFRAMES")

    except Exception as e:
        log(f"INDEX CSV DOWNLOAD FAILED: {e}")

    finally:
        close_db_connection(conn)  
#################################################################################################
# Reads downloaded index CSV files for each timeframe, matches them to index IDs,
# and imports the price data into the `index_price_data` table.
# Process:
#   1. Loop through all defined timeframes.
#   2. For each timeframe, scan its CSV files.
#   3. Validate each index code against `index_symbols` to get `index_id`.
#   4. Load and clean CSV data (standardize dates, round numeric values).
#   5. Insert or update rows in `index_price_data` using upsert logic.
#   6. Logs progress and errors throughout.

# Automatically creates or updates all historical index price records in the database.
#################################################################################################  
def import_index_csv_to_db():
    try:
        conn = get_db_connection()

        for timeframe in FREQUENCIES:
            timeframe_path = os.path.join(YAHOO_FILES_INDEX, timeframe)
            if not os.path.exists(timeframe_path):
                log(f"No folder for timeframe '{timeframe}', skipping")
                continue

            log(f"===== IMPORTING INDEX CSV DATA FOR TIMEFRAME '{timeframe}' =====")

            # Iterate over all CSV files in this timeframe folder
            for csv_file in os.listdir(timeframe_path):
                if not csv_file.endswith(".csv"):
                    continue

                csv_path = os.path.join(timeframe_path, csv_file)
                index_code = os.path.splitext(csv_file)[0]

                # Lookup index_id
                cur = conn.cursor()
                cur.execute("SELECT index_id FROM index_symbols WHERE index_code = ?", (index_code,))
                res = cur.fetchone()
                if not res:
                    log(f"Index '{index_code}' not found in index_symbols table, skipping")
                    continue
                index_id = res[0]

                try:
                    df = pd.read_csv(csv_path)
                    if df.empty:
                        log(f"{index_code} | {timeframe} | CSV empty, skipping")
                        continue

                    # Clean column names
                    df.columns = [c.strip() for c in df.columns]

                    # Normalize date column
                    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

                    # Round numeric columns (Volume not present for index data)
                    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
                        if col in df.columns:
                            df[col] = df[col].round(2)

                    # Prepare values
                    rows = [
                        (
                            index_id,
                            timeframe,
                            row["Date"],
                            row.get("Open"),
                            row.get("High"),
                            row.get("Low"),
                            row.get("Close"),
                            row.get("Adj Close")
                        )
                        for _, row in df.iterrows()
                    ]

                    # Insert or update
                    insert_sql = """
                        INSERT INTO index_price_data
                        (index_id, timeframe, date, open, high, low, close, adj_close)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(index_id, timeframe, date) DO UPDATE SET
                            open=excluded.open,
                            high=excluded.high,
                            low=excluded.low,
                            close=excluded.close,
                            adj_close=excluded.adj_close
                    """

                    conn.executemany(insert_sql, rows)
                    conn.commit()

                    # log(f"{index_code} | {timeframe} | INSERTED/UPDATED {len(rows)} rows")

                except Exception as e:
                    log(f"❌ FAILED {index_code} | {timeframe} | {e}")
                    traceback.print_exc()

        log("🎉 ALL INDEX CSV FILES IMPORTED INTO DATABASE")

    except Exception as e:
        log(f"CRITICAL FAILURE import_index_csv_to_db | {e}")
        traceback.print_exc()

    finally:
        close_db_connection(conn)
#################################################################################################
# Downloads index price data from Yahoo for all timeframes, imports the CSV data
# into the database, and cleans weekly data by deleting entries that are not Mondays.
# Execution flow:
#   1. Download index data from Yahoo.
#   2. Import downloaded CSV files into `index_price_data`.
#   3. Remove weekly rows where the date is not Monday.
# Logs progress and errors throughout.
#################################################################################################  
def insert_index_price_data():
    try:
        log(f"===== YAHOO DOWNLOAD STARTED =====")
        download_index_yahoo_data_all_timeframes()
        log(f"===== YAHOO DOWNLOAD FINISHED =====")
        log(f"===== CSV TO DATABASE IMPORT STARTED =====")
        import_index_csv_to_db()
        log(f"===== CSV TO DATABASE IMPORT FINISHED =====")
        log(f"===== DELETE NON MONDAY FOR WEEK STARTED =====")
        delete_non_monday_weekly(is_index=True)
        log(f"===== DELETE NON MONDAY FOR WEEK FINISHED =====")
        # log(f"===== UPDATE 52 WEEK STAT STARTED =====")
        # refresh_52week_stats()
        # log(f"===== UPDATE 52 WEEK STAT FINISHED =====")
        log(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS STARTED =====")
        for timeframe in FREQUENCIES:
            folder_path = os.path.join(YAHOO_FILES_INDEX, timeframe)
            delete_wkly_mthly_yahoo_files(folder_path)
        log(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS FINISHED =====")
    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")    
#################################################################################################
# Calculates a comprehensive set of technical indicators for a given price DataFrame.
# For each symbol’s price data:
#   - Computes Simple Moving Averages (SMA) for 20, 50, and 200 periods.
#   - Computes Relative Strength Index (RSI) for 3, 9, and 14 periods.
#   - Computes EMA and WMA of RSI-9 (periods 3 and 21 respectively) for smoothed momentum.
#   - Calculates Bollinger Bands, ATR-14, Supertrend with direction, and MACD with signal line.
#   - Calculates daily percentage price change based on adjusted close.
# Returns either the full DataFrame with indicators or only the latest row if requested.
# Handles exceptions by logging errors, printing traceback, and returning the original DataFrame.
#################################################################################################
def calculate_indicators(df, latest_only=False):
    try:
        # ---------------- SMA ----------------
        df["sma_20"] = df["adj_close"].rolling(20).mean().round(2)
        df["sma_50"] = df["adj_close"].rolling(50).mean().round(2)
        df["sma_200"] = df["adj_close"].rolling(200).mean().round(2)
        # ---------------- RSI ----------------
        df["rsi_3"] = calculate_rsi_series(df["close"], 3)
        df["rsi_9"] = calculate_rsi_series(df["close"], 9)
        df["rsi_14"] = calculate_rsi_series(df["close"], 14)
        # ---------------- Other Indicators ----------------
        df["ema_rsi_9_3"] = calculate_ema(df["rsi_9"], 3)
        df["wma_rsi_9_21"] = calculate_wma(df["rsi_9"], 21)
        # --------------- Bollinger Bands, ATR, Supertrend, MACD ----------------
        df["bb_upper"], df["bb_middle"], df["bb_lower"] = calculate_bollinger(df["close"])
        df["atr_14"] = calculate_atr(df)
        df["supertrend"], df["supertrend_dir"] = calculate_supertrend(df)
        df["macd"], df["macd_signal"] = calculate_macd(df["close"])
        # # --------------- Percentage Price Change ----------------
        df["pct_price_change"] = (df["adj_close"].pct_change() * 100).round(2)
        # --------------- Percentage Price Change ----------------
        # df["pct_price_change"] = (
        #     df["adj_close"].pct_change(fill_method=None).mul(100).round(2)
        # )

        # ---- Return only last row if requested ----
        if latest_only:
            return df.iloc[[-1]].reset_index(drop=True)

        return df

    except Exception as e:
        log(f"{sys._getframe().f_code.co_name} FAILED | {e}")
        traceback.print_exc()
        return df  # return original df on failure
#################################################################################################
# For each symbol or index:
# - Loads historical price data for all defined timeframes.
# - Computes a comprehensive set of technical indicators via calculate_indicators(), including:
# - Simple Moving Averages (SMA-20, SMA-50, SMA-200)
# - Relative Strength Index (RSI-3, RSI-9, RSI-14)
# - EMA of RSI-9 (period 3) and WMA of RSI-9 (period 21)
# - Bollinger Bands (upper, middle, lower)
# - Average True Range (ATR-14)
# - Supertrend and its direction
# - MACD and MACD signal line
# - Daily percentage price change
# - Prepares UPSERT SQL statements for equities and indices using SQL_INSERT templates.
# - Iterates over each symbol/index and each timeframe:
# - Reads price data from the database.
# - Skips if no data exists.
# - Applies indicator calculations to the DataFrame.
# - Converts the DataFrame to tuples for batch insertion/updating in the DB.
# - Commits results per symbol and logs inserted row count.
# - Handles exceptions per symbol/index to continue processing others.
# - Logs progress, errors, and elapsed time for each timeframe.
# - Closes cursor and database connection in the finally block.
# Returns nothing; indicators are written directly to the database.
#################################################################################################
def refresh_indicators():
    # ------------------------------
    # PART 1: EQUITIES
    # ------------------------------
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        table_symbols   = "equity_symbols"
        price_table     = "equity_price_data"
        indicator_table = "equity_indicators"
        col_id          = "symbol_id"

        # Load equity symbol IDs
        cur.execute(f"SELECT {col_id} FROM {table_symbols}")
        symbol_ids = [row[0] for row in cur.fetchall()]
        print(f"\n🔢 Loaded {len(symbol_ids)} equities")

        insert_sql_equity = f"""{SQL_INSERT['equity']}""".format(
            indicator_table=indicator_table,
            col_id=col_id
        )
        for timeframe in FREQUENCIES:
            print(f"\n⏳ Processing EQUITIES timeframe: {timeframe}")
            tf_start_time = time.time()
            inserted_rows = 0
            processed_symbols = 0

            for idx, symbol_id in enumerate(symbol_ids, start=1):
                if idx <= 3 or idx % 250 == 0:
                    print(f"  → {idx}/{len(symbol_ids)} symbols...", flush=True)

                try:
                    df = pd.read_sql(f"""
                        SELECT date, open, high, low, close, adj_close, is_final
                        FROM {price_table}
                        WHERE {col_id}=? AND timeframe=?
                        ORDER BY date
                    """, conn, params=(symbol_id, timeframe))

                    if df.empty:
                        continue
                    
                    calculate_indicators(df, latest_only=False)

                    records = [
                        (
                            symbol_id, timeframe, row["date"], row["is_final"],
                            row["sma_20"], row["sma_50"], row["sma_200"],
                            row["rsi_3"], row["rsi_9"], row["rsi_14"],
                            row["bb_upper"], row["bb_middle"], row["bb_lower"],
                            row["atr_14"], row["supertrend"], row["supertrend_dir"],
                            row["ema_rsi_9_3"], row["wma_rsi_9_21"], row["pct_price_change"],
                            row["macd"], row["macd_signal"]
                        )
                        for _, row in df.iterrows()
                    ]

                    cur.executemany(insert_sql_equity, records)
                    conn.commit()
                    inserted_rows += len(records)
                    processed_symbols += 1

                except Exception as e:
                    print(f"❌ ERROR EQUITY {symbol_id} T={timeframe} | {e}")
                    traceback.print_exc()

            print(f"  ✔ EQUITIES {timeframe} DONE | {processed_symbols} symbols | {inserted_rows} rows | {time.time()-tf_start_time:.1f}s")

        print("🎉 EQUITIES indicators refreshed successfully!")

    except Exception as e:
        print(f"❌ CRITICAL FAILURE — EQUITIES | {e}")
        traceback.print_exc()

    # ------------------------------
    # PART 2: INDICES
    # ------------------------------
    try:
        cur = conn.cursor()
        table_symbols   = "index_symbols"
        price_table     = "index_price_data"
        indicator_table = "index_indicators"
        col_id          = "index_id"

        cur.execute(f"SELECT {col_id} FROM {table_symbols}")
        symbol_ids = [row[0] for row in cur.fetchall()]
        print(f"\n🔢 Loaded {len(symbol_ids)} indices")

        insert_sql_index = f"""{SQL_INSERT['index']}""".format(
            indicator_table=indicator_table,
            col_id=col_id
        )
        for timeframe in FREQUENCIES:
            print(f"\n⏳ Processing INDICES timeframe: {timeframe}")
            tf_start_time = time.time()
            inserted_rows = 0
            processed_symbols = 0

            for idx, symbol_id in enumerate(symbol_ids, start=1):
                if idx <= 3 or idx % 250 == 0:
                    print(f"  → {idx}/{len(symbol_ids)} symbols...", flush=True)

                try:
                    df = pd.read_sql(f"""
                        SELECT date, open, high, low, close, adj_close
                        FROM {price_table}
                        WHERE {col_id}=? AND timeframe=?
                        ORDER BY date
                    """, conn, params=(symbol_id, timeframe))

                    if df.empty:
                        continue

                    calculate_indicators(df, latest_only=False)

                    records = [
                        (
                            symbol_id, timeframe, row["date"],
                            row["sma_20"], row["sma_50"], row["sma_200"],
                            row["rsi_3"], row["rsi_9"], row["rsi_14"],
                            row["bb_upper"], row["bb_middle"], row["bb_lower"],
                            row["atr_14"], row["supertrend"], row["supertrend_dir"],
                            row["ema_rsi_9_3"], row["wma_rsi_9_21"], row["pct_price_change"],
                            row["macd"], row["macd_signal"]
                        )
                        for _, row in df.iterrows()
                    ]

                    cur.executemany(insert_sql_index, records)
                    conn.commit()
                    inserted_rows += len(records)
                    processed_symbols += 1

                except Exception as e:
                    print(f"❌ ERROR INDEX {symbol_id} T={timeframe} | {e}")
                    traceback.print_exc()

            print(f"  ✔ INDICES {timeframe} DONE | {processed_symbols} symbols | {inserted_rows} rows | {time.time()-tf_start_time:.1f}s")

        print("🎉 INDICES indicators refreshed successfully!")

    except Exception as e:
        print(f"❌ CRITICAL FAILURE — INDICES | {e}")
        traceback.print_exc()
    finally:
        cur.close()
        close_db_connection(conn)         
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
        os.makedirs(NSE_BHAVCOPY_DAILY, exist_ok=True)
        # ---- Prepare URL and save path ----
        save_path = os.path.join(
            NSE_BHAVCOPY_DAILY,
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
# Retrieves the most recent available price date from the equity_price_data table.
# - Opens a database connection using get_db_connection().
# - Executes a SQL query to select the maximum `date` for the given timeframe (default: "1d").
# - Uses pandas read_sql() with parameter binding to safely substitute the timeframe.
# - Reads the query result into a DataFrame and extracts the `latest_date` value.
# - If a date exists:
#       • converts it from string format ("YYYY-MM-DD") into a Python `date` object
#       • returns the converted date for downstream use (e.g., finding missing days)
# - If no date is found (empty table), returns None.
# - If an exception occurs:
#       • logs the error
#       • returns None to avoid breaking calling workflows
# - Always closes the database connection in the `finally` block for safety.
# Returns either the latest available date as a `date` object, or None if not found.
#################################################################################################
def get_latest_equity_date(timeframe="1d"):
    conn = get_db_connection()
    try:
        sql = """
            SELECT MAX(date) AS latest_date
            FROM equity_price_data
            WHERE timeframe = ?
        """
        df = pd.read_sql(sql, conn, params=[timeframe])
        latest = df.iloc[0]['latest_date']
        if latest is None:
            return None
        # Convert string → datetime
        return datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception as e:
        log(f"❗ Error fetching latest date: {e}")
        return None

    finally:
        close_db_connection(conn)
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
    os.makedirs(NSE_BHAVCOPY_DAILY, exist_ok=True)

    for filename in os.listdir(NSE_BHAVCOPY_DAILY):
        file_path = os.path.join(NSE_BHAVCOPY_DAILY, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            log(f"❗ Failed to delete {file_path}: {e}")

    log(f"🧹 Cleared old files in {NSE_BHAVCOPY_DAILY}")

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
#################################################################################################
# UPDATE WEEKLY & MONTHLY EQUITY PRICE DATA FROM YAHOO FINANCE
#
# Steps:
# - Fetch all equity symbols (or filtered by input)
# - Determine latest weekly/monthly dates stored in DB
# - Download full Yahoo data for 1wk / 1mo interval
# - Flatten MultiIndex columns and normalize structure
# - Insert/Update into equity_price_data using UPSERT logic
#################################################################################################
def update_weekly_monthly_from_yahoo():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # ---- Load all symbols ----
        df_symbols = retrieve_equity_symbol("ALL", conn)
        if df_symbols.empty:
            log("❗ No symbols found to update weekly/monthly")
            return

        log(f"🔎 Total symbols to process: {len(df_symbols)}")

        # ---- timeframes to process ----
        yahoo_timeframes = ["1wk", "1mo"]

        # ---- SQL for inserting ----
        insert_sql = """
            INSERT INTO equity_price_data
            (symbol_id, timeframe, date, open, high, low, close, adj_close, volume, is_final)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(symbol_id, timeframe, date)
            DO UPDATE SET
                open      = excluded.open,
                high      = excluded.high,
                low       = excluded.low,
                close     = excluded.close,
                adj_close = excluded.adj_close,
                volume    = excluded.volume
        """

        total_updates = 0

        # ---- Process weekly & monthly ----
        for tf in yahoo_timeframes:
            log(f"\n📆 Updating timeframe: {tf}")

            latest_db_date = get_latest_equity_date(tf)
            if latest_db_date:
                log(f"🕒 Latest {tf} in DB: {latest_db_date}")
                yahoo_start = (latest_db_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                log(f"⚠ No {tf} data in DB — downloading full history")
                yahoo_start = None

            for _, row in df_symbols.iterrows():
                sid = row["symbol_id"]
                sym = row["symbol"]
                yahoo_symbol = f"{sym}.NS"

                try:
                    # ---- download missing data only ----
                    df = yf.download(
                        yahoo_symbol,
                        interval=tf,
                        start=yahoo_start,
                        auto_adjust=False,
                        progress=False
                    )

                    # ---- validate ----
                    if df is None or df.empty:
                        log(f"⚠ No new {tf} data for {sym}")
                        continue

                    # ---- fix multilevel columns ----
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = df.columns.droplevel(1)

                    df.reset_index(inplace=True)

                    # ---- clean columns ----
                    df.columns = [c.strip() for c in df.columns]

                    # ---- normalize date ----
                    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

                    # ---- numeric rounding ----
                    for col in ["Open", "High", "Low", "Close", "Adj Close"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

                    if "Volume" in df.columns:
                        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

                    # ---- db records ----
                    records = [
                        (
                            sid, tf, row["Date"],
                            row.get("Open"), row.get("High"), row.get("Low"),
                            row.get("Close"), row.get("Adj Close"), row.get("Volume")
                        )
                        for _, row in df.iterrows()
                    ]

                    cur.executemany(insert_sql, records)
                    conn.commit()

                    total_updates += len(records)
                    log(f"✔ {sym:<12} {tf:<3} | {len(records)} rows updated")

                except Exception as e:
                    log(f"❌ FAILED {sym} | {tf} | {e}")
                    traceback.print_exc()

        log(f"\n🎉 WEEKLY/MONTHLY UPDATE COMPLETE — {total_updates} total records inserted/updated")

    except Exception as e:
        log(f"❗ CRITICAL FAILURE update_weekly_monthly_from_yahoo | {e}")
        traceback.print_exc()
    finally:
        close_db_connection(conn)
        log("🔚 DB connection closed for weekly/monthly update")
#################################################################################################
# DOWNLOAD DAILY (NSE) and WEEKLY MONTHLY DATA (YAHOO)
#################################################################################################
def download_daily_weekly_monthly_data():
    try:
        # download bhavcopy from nse
        download_missing_bhavcopies()
        # update bhavcopy data for daily timeframe in equity_price_data table
        update_equity_price_from_bhavcopy()
        update_weekly_monthly_from_yahoo()
        delete_non_monday_weekly()
    except Exception as e:
        log(f"❗ Unexpected error: {e}")
        traceback.print_exc()
        return None  

