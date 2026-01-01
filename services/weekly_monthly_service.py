from config.logger import log
from config.paths import FREQUENCIES
from db.connection import get_db_connection, close_db_connection
from services.symbol_service import retrieve_equity_symbol, get_latest_equity_date
from services.bhavcopy_loader import download_missing_bhavcopies, update_equity_price_from_bhavcopy
from services.equity_service import download_equity_yahoo_data_all_timeframes
from datetime import datetime, timedelta
import pandas as pd
import traceback
import yfinance as yf
#################################################################################################
# Fetches missing weekly and monthly price candles for all symbols from Yahoo Finance 
# and upserts them into equity_price_data while maintaining data continuity using latest DB dates. 
# Ensures accurate multi-timeframe updates for scanners and indicators.
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
        # yahoo_timeframes = ["1d", "1wk", "1mo"]

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
        for tf in FREQUENCIES:
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
def download_daily_weekly_monthly_data(override_date):
    try:
        # download bhavcopy from nse
        download_missing_bhavcopies(override_date)
        # update bhavcopy data for daily timeframe in equity_price_data table
        update_equity_price_from_bhavcopy()
        # update_weekly_monthly_from_yahoo()
        download_equity_yahoo_data_all_timeframes()
        # delete_non_monday_weekly()
    except Exception as e:
        log(f"❗ Unexpected error: {e}")
        traceback.print_exc()
        return None  