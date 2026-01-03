import os
import pandas as pd
from config.logger import log, clear_log
from db.connection import get_db_connection, close_db_connection
from services.symbol_service import retrieve_equity_symbol

EQUITY_FOLDER = "./nse_bhav_copy/equity_bhav_29Dec2025/"

# CSV to DB column mapping
COLUMN_MAPPING = {
    "DELIV_PER": "delv_pct"
}

TIMEFRAME = "1d"

def normalize_columns(df):
    """Remove spaces, standardize case, and map CSV columns to DB columns."""
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

    # Mapping from CSV column names to DB column names
    COLUMN_MAPPING = {
        "deliv_per": "delv_pct",
        "date1": "date"
    }

    for csv_col, db_col in COLUMN_MAPPING.items():
        if csv_col in df.columns:
            df.rename(columns={csv_col: db_col}, inplace=True)

    return df

# def load_symbol_files(symbol):
#     """Load all CSV files starting with a symbol into one DF."""
#     files = [
#         f for f in os.listdir(EQUITY_FOLDER)
#         if f.upper().startswith(symbol.upper()) and f.lower().endswith(".csv")
#     ]

#     if not files:
#         print(f"⚠️ No files found for {symbol}")
#         return pd.DataFrame()

#     df_list = []
#     for file in files:
#         try:
#             df = pd.read_csv(os.path.join(EQUITY_FOLDER, file))
#             df["source_file"] = file
#             df_list.append(df)
#         except Exception as e:
#             print(f"❌ Error reading {file}: {e}")

#     return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def load_single_file(file_name):
    """Load one CSV file from EQUITY_FOLDER."""
    file_path = os.path.join(EQUITY_FOLDER, file_name)
    if not os.path.exists(file_path):
        print(f"⚠️ File not found: {file_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(file_path)
        df["source_file"] = file_name
        return df
    except Exception as e:
        print(f"❌ Error reading {file_name}: {e}")
        return pd.DataFrame()

def update_equity_delv_data(file_name):
    """Update delv_pct from a single CSV file."""
    clear_log()
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        df_symbols = retrieve_equity_symbol("ALL", conn)

        df_symbol = load_single_file(file_name)
        if df_symbol.empty:
            print("No data found in the file.")
            return

        df_symbol = normalize_columns(df_symbol)

        if "date" not in df_symbol.columns or "delv_pct" not in df_symbol.columns:
            print("⚠ Missing 'date' or 'delv_pct' in CSV. Skipping.")
            return

        # loop through CSV rows and update DB
        for _, r in df_symbol.iterrows():
            try:
                dt = pd.to_datetime(r["date"]).strftime("%Y-%m-%d")
                pct = r["delv_pct"]

                cur.execute("""
                    UPDATE equity_price_data
                    SET delv_pct = ?
                    WHERE symbol_id = (
                        SELECT symbol_id FROM equity_symbols WHERE symbol = ?
                    )
                      AND timeframe = ?
                      AND date = ?
                """, (pct, r["symbol"].upper().strip(), TIMEFRAME, dt))

            except Exception as db_err:
                log(f"[DB ERROR] {r.get('symbol','?')} {r['date']}: {db_err}")

        conn.commit()
        print(f"Updated rows: {cur.rowcount} from {file_name}")

    except Exception as e:
        log(f"[FATAL] update_equity_delv_data: {e}")

    finally:
        close_db_connection(conn)
        
if __name__ == "__main__":
    update_equity_delv_data("sec_bhavdata_full_30122025.csv")
    # check_blank()