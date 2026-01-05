import traceback
from datetime import datetime
import pandas as pd
from services.cleanup_service import delete_files_in_folder
from services.scanners.export_service import export_to_csv
from services.scanners.backtest_service import backtest_all_scanners
from services.scanners.data_service import get_base_data
from db.connection import get_db_connection, close_db_connection
from config.paths import SCANNER_FOLDER
from config.logger import log

LOOKBACK_DAYS = 365


#################################################################################################
# APPLY HILEGA-MILEGA SCANNER LOGIC
#################################################################################################
def apply_scanner_logic(start_date: str, end_date: str) -> pd.DataFrame:
    conn = None
    try:
        conn = get_db_connection()
        df_signals = get_base_data()

        log(f"🧪 Base data rows: {len(df_signals)}")
        log(f"🧪 Base data date range: {df_signals['date'].min()} → {df_signals['date'].max()}")

        df_filtered = df_signals[
            (df_signals['adj_close'] >= 100) &
            (df_signals['rsi_3'] > 65) &
            (df_signals['rsi_9'] > 55) &
            (df_signals['rsi_14'] > 55) &
            (df_signals['rsi_3'] / df_signals['rsi_9'] >= 1.15) &
            (df_signals['rsi_9'] / df_signals['ema_rsi_9_3'] >= 1.04) &
            (df_signals['ema_rsi_9_3'] / df_signals['wma_rsi_9_21'] >= 1) &
            (df_signals['ema_rsi_9_3'] > 50) &
            (df_signals['wma_rsi_9_21'] > 50) &
            (df_signals['rsi_3_weekly'] > 65) &
            (df_signals['rsi_9_weekly'] > 55) &
            (df_signals['rsi_14_weekly'] > 55) &
            (df_signals['rsi_3_monthly'] > 80) &
            (df_signals['rsi_9_monthly'] > 70) &
            (df_signals['rsi_14_monthly'] > 65)
        ].sort_values(['date', 'symbol'], ascending=[False, True])

        log(f"🧪 After RSI filter rows: {len(df_filtered)}")
        log(f"🧪 Sample symbols after filter: {df_filtered['symbol'].head(5).tolist()}")

        return df_filtered

    except Exception as e:
        log(f"❌ apply_scanner_logic failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

    finally:
        if conn:
            close_db_connection(conn)


#################################################################################################
# RUN SCANNER
#################################################################################################
def run_scanner(start_date: str, end_date: str, file_name: str = "HM") -> pd.DataFrame:
    conn = None
    try:
        log(f"\n🔍 Running scanner from {start_date} → {end_date}")

        # ---------------- BASE FILTER ----------------
        df_signals = apply_scanner_logic(start_date, end_date)

        if df_signals.empty:
            log("❌ STOP: No signals after base RSI filter")
            return pd.DataFrame()

        log(f"🧪 Symbols entering crossover stage: {df_signals['symbol'].nunique()}")

        conn = get_db_connection()

        # ---------------- MONTHLY CLOSE CROSSOVER QUERY ----------------
        sql = """
            SELECT
                d.symbol_id,
                d.date,
                d.close AS close_today,
                LAG(d.close, 1) OVER (
                    PARTITION BY d.symbol_id ORDER BY d.date
                ) AS close_yesterday,
                (
                    SELECT m.close
                    FROM equity_price_data m
                    WHERE m.symbol_id = d.symbol_id
                    AND m.timeframe = '1mo'
                    AND m.date < d.date
                    ORDER BY m.date DESC
                    LIMIT 1
                ) AS prev_month_close
            FROM equity_price_data d
            WHERE d.timeframe = '1d'
            AND d.date BETWEEN ? AND ?
        """

        df_cross = pd.read_sql(
            sql,
            conn,
            params=(start_date, end_date),
            parse_dates=["date"]
        )

        log(f"🧪 Daily/monthly crossover rows fetched: {len(df_cross)}")

        if df_cross.empty:
            log("❌ STOP: No rows returned from crossover SQL")
            return pd.DataFrame()

        # ---------------- MERGE CHECK ----------------
        df_merged = df_signals.merge(
            df_cross,
            on=["symbol_id", "date"],
            how="inner"
        )

        log(f"🧪 Rows after merge: {len(df_merged)}")

        if df_merged.empty:
            log("❌ STOP: No rows matched on symbol_id + date")
            log("🔍 DEBUG sample df_signals dates:")
            log(df_signals[['symbol_id', 'date']].head(5).to_string())
            log("🔍 DEBUG sample df_cross dates:")
            log(df_cross[['symbol_id', 'date']].head(5).to_string())
            return pd.DataFrame()

        # ---------------- CROSSOVER LOGIC ----------------
        df_final = df_merged[
            (df_merged["close_yesterday"] <= df_merged["prev_month_close"]) &
            (df_merged["close_today"] > df_merged["prev_month_close"])
        ]

        log(f"🧪 Rows after crossover condition: {len(df_final)}")

        if df_final.empty:
            log("❌ STOP: No TRUE crossovers detected")
            log("🔍 DEBUG crossover sample:")
            debug_cols = [
                "symbol", "date",
                "close_yesterday",
                "prev_month_close",
                "close_today"
            ]
            log(df_merged[debug_cols].head(10).to_string())
            return pd.DataFrame()

        # ---------------- CLEANUP ----------------
        df_final.drop(
            columns=["close_today", "close_yesterday", "prev_month_close"],
            inplace=True
        )

        path = export_to_csv(df_final, SCANNER_FOLDER, file_name)
        log(f"✅ Scanner results saved to: {path}")

        return df_final

    except Exception as e:
        log(f"❌ run_scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

    finally:
        if conn:
            close_db_connection(conn)


#################################################################################################
# MULTI-YEAR DRIVER
#################################################################################################
def scanner_play_multi_years(start_year: str, lookback_years: int):
    try:
        delete_files_in_folder(SCANNER_FOLDER)

        start_year_int = int(start_year)
        all_years_results = []

        for i in range(lookback_years):
            year = start_year_int - i
            start_date = f"{year}-01-01"
            end_date   = f"{year}-12-31"

            print(f"\n🔹 YEAR {year}")
            df_year = run_scanner(start_date, end_date, file_name=str(year))
            print(f"➡ Rows found: {len(df_year)}")

            if not df_year.empty:
                df_year["year"] = year
                all_years_results.append(df_year)

        if all_years_results:
            final_df = pd.concat(all_years_results, ignore_index=True)
            print(f"✅ TOTAL rows across years: {len(final_df)}")
        else:
            final_df = pd.DataFrame()
            print("⚠ No results across years")

        df_backtest = backtest_all_scanners()
        print(df_backtest)

        return final_df

    except Exception as e:
        print(f"❌ Multi-year scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()