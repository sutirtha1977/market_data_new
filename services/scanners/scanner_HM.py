import traceback
import pandas as pd
from .data_service import get_base_data
from .export_service import export_to_csv

def scanner_hilega_milega() -> pd.DataFrame:
    """Multi-timeframe RSI & price scanner."""
    try:
        df = get_base_data()
        df_filtered = df[
            (df['adj_close'] >= 100) &
            (df['rsi_3'] / df['rsi_9'] >= 1.15) &
            (df['rsi_9'] / df['ema_rsi_9_3'] >= 1.05) &
            (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] >= 1) &
            (df['rsi_3'] < 60) &
            (df['rsi_3_weekly'] > 50) &
            (df['rsi_3_monthly'] > 50) &
            (df['pct_price_change'] <= 5)
        ].sort_values(['date','symbol'], ascending=[False, True])
        export_to_csv(df_filtered, "HM")
        return df_filtered
    except Exception as e:
        print(f"❌ scanner_hilega_milega failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()