import pandas as pd
import traceback
import time
from db.connection import get_db_connection, close_db_connection
from config.paths import FREQUENCIES
from config.logger import log

from services.indicators.utils import sma, ema, wma
from services.indicators.momentum import rsi
from services.indicators.trend import macd, supertrend
from services.indicators.volatility import atr, bollinger_bands
from services.cleanup_service import delete_invalid_timeframe_rows

from db.sql import SQL_INSERT

# ------------------------------
# CONFIG
# ------------------------------
LOOKBACK_BUFFER = 300   # ensures indicators stabilize (RSI, SMA200, MACD etc.)

def cleanup_indicators():
    # print("🎉 Equity Indicator cleanup STARTED")
    # delete_invalid_timeframe_rows("1wk", data_type="indicator")
    # delete_invalid_timeframe_rows("1mo", data_type="indicator")
    # print("🎉 Equity Indicator cleanup FINISHED")
    print("🎉 Index Indicator cleanup STARTED")
    delete_invalid_timeframe_rows("1wk", data_type="indicator", is_index=True)
    delete_invalid_timeframe_rows("1mo", data_type="indicator", is_index=True)
    print("🎉 Index Indicator cleanup FINISHED") 

# ------------------------------
# CORE INDICATOR CALCULATION
# ------------------------------
def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]

    df["sma_20"]  = sma(close, 20).round(2)
    df["sma_50"]  = sma(close, 50).round(2)
    df["sma_200"] = sma(close, 200).round(2)

    df["rsi_3"]   = rsi(close, 3).round(2)
    df["rsi_9"]   = rsi(close, 9).round(2)
    df["rsi_14"]  = rsi(close, 14).round(2)

    df["ema_rsi_9_3"]  = ema(df["rsi_9"], 3).round(2)
    df["wma_rsi_9_21"] = wma(df["rsi_9"], 21).round(2)

    df["bb_upper"], df["bb_middle"], df["bb_lower"] = [
        s.round(2) for s in bollinger_bands(close)
    ]

    df["atr_14"] = atr(df["high"], df["low"], close).round(2)

    df["supertrend"], df["supertrend_dir"] = supertrend(df["high"], df["low"], close)
    df["supertrend"] = df["supertrend"].round(2)

    df["macd"], df["macd_signal"], _ = macd(close)
    df["macd"] = df["macd"].round(4)
    df["macd_signal"] = df["macd_signal"].round(4)

    df["pct_price_change"] = close.pct_change(fill_method=None).mul(100).round(2)

    return df
# ------------------------------
# MAIN REFRESH ENGINE
# ------------------------------
def indicators_refresh():

    mapping = [
        ("equity_symbols", "equity_price_data", "equity_indicators", "symbol_id", SQL_INSERT["equity"]),
        ("index_symbols",  "index_price_data",  "index_indicators",  "index_id",  SQL_INSERT["index"]),
    ]

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        for sym_table, price_table, ind_table, id_col, insert_sql in mapping:

            log(f"🚀 Refreshing indicators for {ind_table}")

            # ---- Get last indicator date ----
            cur.execute(f"SELECT MAX(date) FROM {ind_table}")
            last_date = cur.fetchone()[0]

            if last_date:
                print(f"🔁 Incremental run from {last_date}")
                log(f"🔁 Incremental run from {last_date}")
            else:
                print(f"🆕 Full historical run")
                log(f"🆕 Full historical run")

            # ---- Load symbols ----
            cur.execute(f"SELECT {id_col} FROM {sym_table}")
            ids = [r[0] for r in cur.fetchall()]
            print(f"🔢 Loaded {len(ids)} symbols")
            log(f"🔢 Loaded {len(ids)} symbols")

            for timeframe in FREQUENCIES:
                log(f"⏳ Timeframe: {timeframe}")
                print(f"⏳ Timeframe: {timeframe}")
                start = time.time()

                for sid in ids:
                    try:
                        # ---- Build date filter ----
                        date_filter = ""
                        params = [sid, timeframe]

                        if last_date:
                            date_filter = "AND date >= ?"
                            params.append(last_date)

                        df = pd.read_sql(f"""
                            SELECT date, open, high, low, close
                            FROM {price_table}
                            WHERE {id_col}=? AND timeframe=?
                            {date_filter}
                            ORDER BY date
                        """, conn, params=params)

                        if df.empty:
                            continue

                        # ---- Extend lookback if incremental ----
                        if last_date:
                            df_full = pd.read_sql(f"""
                                SELECT date, open, high, low, close
                                FROM {price_table}
                                WHERE {id_col}=? AND timeframe=?
                                ORDER BY date DESC
                                LIMIT ?
                            """, conn, params=(sid, timeframe, LOOKBACK_BUFFER))
                            df = (
                                pd.concat([df_full, df])
                                .drop_duplicates("date")
                                .sort_values("date")
                            )

                        df = calculate_indicators(df)

                        # ---- Keep only new rows ----
                        if last_date:
                            df = df[df["date"] > last_date]

                        if df.empty:
                            continue

                        records = [
                            (
                                sid, timeframe, row.date,
                                row.sma_20, row.sma_50, row.sma_200,
                                row.rsi_3, row.rsi_9, row.rsi_14,
                                row.bb_upper, row.bb_middle, row.bb_lower,
                                row.atr_14, row.supertrend, row.supertrend_dir,
                                row.ema_rsi_9_3, row.wma_rsi_9_21,
                                row.pct_price_change,
                                row.macd, row.macd_signal
                            )
                            for row in df.itertuples()
                        ]

                        cur.executemany(insert_sql.format(
                            indicator_table=ind_table,
                            col_id=id_col
                        ), records)
                        conn.commit()

                    except Exception as e:
                        log(f"❌ {ind_table} {sid} {timeframe} | {e}")
                        traceback.print_exc()

                log(f"✔ {ind_table} {timeframe} done in {time.time()-start:.1f}s")
                print(f"✔ {ind_table} {timeframe} done in {time.time()-start:.1f}s")

        log("🎉 Indicator refresh completed successfully")
        print("🎉 Indicator refresh completed successfully")
        print("🎉 Indicator cleanup STARTED")
        cleanup_indicators()
        print("🎉 Indicator cleanup FINISHED")
        
    except Exception as e:
        log(f"❌ Indicator refresh failed | {e}")
        traceback.print_exc()

    finally:
        if conn:
            close_db_connection(conn)