import traceback
import pandas as pd
from services.scanners.data_service import get_base_data
from services.scanners.export_service import export_to_csv
from services.cleanup_service import delete_files_in_folder
from config.paths import SCANNER_FOLDER

#################################################################################################
# Filters stocks using daily, weekly, and monthly RSI along with SMA, EMA, 
# and WMA conditions, returning only those meeting all criteria and exporting the results to CSV.
#################################################################################################  
def scanner_hilega_milega(start_date: str | None = None) -> pd.DataFrame:
    try:
        lookback_days = 365

        # Clean scanner folder before starting
        print(f"===== DELETE FILES FROM SCANNER FOLDER STARTED =====")
        delete_files_in_folder(SCANNER_FOLDER)
        print(f"===== DELETE FILES FROM SCANNER FOLDER FINISHED =====")
        
        df = get_base_data(lookback_days=lookback_days, start_date=start_date)
        
        if df is None or df.empty:
            print(f"⚠ No base data found for end date: {start_date}")
            return pd.DataFrame()

        required_cols = [
            'adj_close', 'rsi_3', 'rsi_9', 'ema_rsi_9_3', 
            'wma_rsi_9_21', 'rsi_3_weekly', 'rsi_3_monthly'
        ]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            print(f"❌ Missing required columns in base data: {missing_cols}")
            return pd.DataFrame()

        df_filtered = df[
            (df['adj_close'] >= 100) &
            (df['rsi_3'] / df['rsi_9'] >= 1.15) &
            (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.04) &
            (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1) &
            (df['rsi_3'] < 60) &
            (df['rsi_3_weekly'] > 50) &
            (df['rsi_3_monthly'] > 50)
        ].sort_values(['date','symbol'], ascending=[False, True])

        if df_filtered.empty:
            print(f"⚠ No stocks met scanner criteria for end date: {start_date}")
            return pd.DataFrame()

        export_to_csv(df_filtered, SCANNER_FOLDER, "HM")
        return df_filtered

    except Exception as e:
        print(f"❌ scanner_hilega_milega failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()