import traceback
from datetime import datetime
import pandas as pd
from config.logger import log
from services.scanners.data_service import get_base_data
from services.scanners.export_service import export_to_csv
from services.scanners.backtest_service import backtest_all_scanners
from services.cleanup_service import delete_files_in_folder
from config.paths import SCANNER_FOLDER

#################################################################################################
# Filters stocks using daily, weekly, and monthly RSI along with SMA, EMA, 
# and WMA conditions, returning only those meeting all criteria and exporting the results to CSV.
#################################################################################################  
def scanner(start_date: str | None = None, file_name: str = "HM") -> pd.DataFrame:
    try:
        lookback_days = 365
        
        df = get_base_data(lookback_days=lookback_days, start_date=start_date)

        df_filtered = df[
            (df['adj_close'] >= 100) &
            # (df['adj_close'] < df['sma_20']) &
            (df['rsi_3'] / df['rsi_9'] >= 1.15) &
            (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.04) &
            (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1) &
            (df['rsi_3'] < 60) &
            (df['rsi_3_weekly'] > 50) &
            (df['rsi_3_monthly'] > 50) 
            # & (df['pct_price_change'] <= 5)
            # & (df['delv_pct'] > 50)
        ].sort_values(['date','symbol'], ascending=[False, True])

        export_to_csv(df_filtered, SCANNER_FOLDER, file_name)
        return df_filtered

    except Exception as e:
        print(f"❌ scanner_hilega_milega failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

#################################################################################################
# Runs the HM scanner iteratively for multiple years.
# - start_year: string, e.g., "2025" (used as the first year)
# - lookback_years: int, number of years to scan backward
#################################################################################################
def scanner_play_multi_years(start_year: str, lookback_years: int):
    try:
        # Clean scanner folder before starting
        print(f"===== DELETE FILES FROM SCANNER FOLDER STARTED =====")
        delete_files_in_folder(SCANNER_FOLDER)
        print(f"===== DELETE FILES FROM SCANNER FOLDER FINISHED =====")

        # Convert start_year to int safely
        try:
            start_year_int = int(start_year)
        except ValueError:
            print(f"❌ Invalid start year '{start_year}', defaulting to 2025")
            start_year_int = 2025

        for i in range(lookback_years):
            year = start_year_int - i
            # Last day of the year
            end_date = datetime(year, 12, 31).strftime("%Y-%m-%d")
            print(f"\n🔹 Running scanner for year {year} (end date {end_date})")
            
            # Pass year as file_name
            df = scanner(start_date=end_date, file_name=str(year))
            print(f"✅ Completed for {year} | Rows found: {len(df)}")
        df = backtest_all_scanners()
        print(df)
    except Exception as e:
        print(f"❌ ERROR | {e}")
        traceback.print_exc()