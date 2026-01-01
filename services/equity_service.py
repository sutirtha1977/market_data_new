import yfinance as yf
import os
import traceback
import pandas as pd
from db.connection import get_db_connection, close_db_connection
from services.cleanup_service import delete_non_monday_weekly, delete_wkly_mthly_yahoo_files
from config.paths import YAHOO_EQUITY_DIR
from config.logger import log
from services.symbol_service import retrieve_equity_symbol
from config.paths import FREQUENCIES
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
            timeframe_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
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
            
            timeframe_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
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
            folder_path = os.path.join(YAHOO_EQUITY_DIR, timeframe)
            delete_wkly_mthly_yahoo_files(folder_path)
        log(f"===== DELETE FILES FROM FOLDERS FINISHED =====")
    except Exception as e:
        log(f"DOWNLOAD FAILED: {e}")
        traceback.print_exc()

