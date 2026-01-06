from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from db.connection import get_db_connection, close_db_connection
from services.symbol_service import retrieve_equity_symbol
from config.logger import log


#################################################################################################
# This function scans daily price data for equities and indices, calculates each symbol’s 
# 52-week high and low, and upserts those values into the corresponding 52-week stats tables.
#################################################################################################           
def refresh_week52_high_low_stats():
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
        
# ---------------------------------------------
# Utility functions
# ---------------------------------------------
def last_friday(ref_date):
    return ref_date - timedelta(days=(ref_date.weekday() - 4) % 7)

def month_end(d):
    return (d.replace(day=1) + relativedelta(months=1)) - timedelta(days=1)

# ---------------------------------------------
# Core function
# ---------------------------------------------
def generate_weekly_monthly_from_daily(symbol="ALL"):
    conn = get_db_connection()

    try:
        # ---------------------------------------------
        # Load symbols
        # ---------------------------------------------
        df_symbols = retrieve_equity_symbol(symbol, conn)

        if df_symbols.empty:
            log("❌ No symbols found")
            return

        log(f"🔍 Processing {len(df_symbols)} symbols")

        for _, sym in df_symbols.iterrows():
            symbol_id = sym["symbol_id"]

            # ---------------------------------------------
            # Load DAILY data
            # ---------------------------------------------
            df = pd.read_sql(
                """
                SELECT date, open, high, low, close, volume
                FROM equity_price_data
                WHERE symbol_id = ?
                  AND timeframe = '1d'
                ORDER BY date
                """,
                conn,
                params=(symbol_id,)
            )

            if df.empty:
                continue

            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)

            # ============================================================
            # 🔑 CUTOFF DATES BASED ON ACTUAL DATA
            # ============================================================
            last_available_date = df.index.max()

            last_complete_friday = last_friday(last_available_date)

            last_complete_month_end = month_end(
                last_available_date.replace(day=1) - timedelta(days=1)
            )

            log(f"📅 Symbol {symbol_id} | Weekly cutoff  : {last_complete_friday.date()}")
            log(f"📅 Symbol {symbol_id} | Monthly cutoff : {last_complete_month_end.date()}")

            # ============================================================
            # WEEKLY AGGREGATION  (Mon–Fri, date = Friday)
            # ============================================================
            weekly_rows = []

            for week_start, wk in df.groupby(pd.Grouper(freq="W-MON")):
                mon = week_start
                fri = week_start + timedelta(days=4)

                # ⛔ Skip incomplete / future week
                if fri > last_complete_friday:
                    continue

                wk_data = wk[(wk.index >= mon) & (wk.index <= fri)]

                if wk_data.empty:
                    continue

                weekly_rows.append({
                    "symbol_id": symbol_id,
                    "timeframe": "1wk",
                    "date": fri.date(),      # ✅ Friday
                    "open": wk_data.iloc[0]["open"],
                    "high": wk_data["high"].max(),
                    "low": wk_data["low"].min(),
                    "close": wk_data.iloc[-1]["close"],
                    "volume": wk_data["volume"].sum()
                })

            weekly_df = pd.DataFrame(weekly_rows)

            # ============================================================
            # MONTHLY AGGREGATION (date = month-end)
            # ============================================================
            monthly_rows = []

            for month_start, mo in df.groupby(pd.Grouper(freq="MS")):
                me = month_end(month_start)

                # ⛔ Skip incomplete / current month
                if me > last_complete_month_end:
                    continue

                if mo.empty:
                    continue

                monthly_rows.append({
                    "symbol_id": symbol_id,
                    "timeframe": "1mo",
                    "date": me.date(),     # ✅ Month end
                    "open": mo.iloc[0]["open"],
                    "high": mo["high"].max(),
                    "low": mo["low"].min(),
                    "close": mo.iloc[-1]["close"],
                    "volume": mo["volume"].sum()
                })

            monthly_df = pd.DataFrame(monthly_rows)

            # ============================================================
            # INSERT INTO DB
            # ============================================================
            final_df = pd.concat([weekly_df, monthly_df], ignore_index=True)

            if final_df.empty:
                continue

            final_df.to_sql(
                "equity_price_data",
                conn,
                if_exists="append",
                index=False
            )

            log(f"✅ Symbol {symbol_id} | Inserted {len(final_df)} rows")

        conn.commit()
        log("🎯 Weekly & Monthly generation completed successfully")

    except Exception as e:
        conn.rollback()
        log(f"❌ Error: {e}")
        raise

    finally:
        close_db_connection(conn)