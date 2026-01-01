import yfinance as yf
import os
import traceback
import pandas as pd
from db.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_non_monday_weekly, delete_wkly_mthly_yahoo_files
from config.paths import YAHOO_INDEX_DIR
from config.logger import log
# from services.symbol_service import retrieve_equity_symbol
from config.paths import FREQUENCIES
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
            timeframe_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
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
            timeframe_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
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
            folder_path = os.path.join(YAHOO_INDEX_DIR, timeframe)
            delete_wkly_mthly_yahoo_files(folder_path)
        log(f"===== DELETE FILES FROM WEEKLY AND MONTHLY FOLDERS FINISHED =====")
    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")   