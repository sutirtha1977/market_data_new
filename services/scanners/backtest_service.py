import os
import pandas as pd
import traceback
from config.logger import log
from config.paths import SCANNER_FOLDER
from db.connection import get_db_connection, close_db_connection

def backtest_scanner(file_name: str):
    """Backtest a scanner CSV by buying next day's open and selling 5 days later."""
    try:
        filepath = os.path.join(SCANNER_FOLDER, file_name)
        df = pd.read_csv(filepath, dtype={'date': 'object'})
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        required_cols = ['symbol_id', 'symbol', 'date']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"File must contain columns: {required_cols}")

        df = df.sort_values(['date', 'symbol']).reset_index(drop=True)
        conn = get_db_connection()
        gains = []

        for _, row in df.iterrows():
            symbol_id = row['symbol_id']
            signal_date = row['date']
            sql = """
                SELECT date, open, close
                FROM equity_price_data
                WHERE symbol_id = ? AND date >= ? AND timeframe='1d'
                ORDER BY date ASC
                LIMIT 6
            """
            df_price = pd.read_sql(sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
            if len(df_price) < 2:
                gains.append(None)
                continue
            buy_price = df_price.iloc[1]['open']
            sell_price = df_price.iloc[5]['close'] if len(df_price) >= 6 else df_price.iloc[-1]['close']
            gains.append(round((sell_price - buy_price)/buy_price*100, 2))

        df['gain_5d_pct'] = gains
        df['win_5d'] = df['gain_5d_pct'] > 0
        print(f"📈 Win Rate (5D): {df['win_5d'].mean()*100:.2f}%")
        print(f"❌ Max Loss (5D): {df['gain_5d_pct'].min():.2f}%")
        close_db_connection(conn)
        print("Backtest completed successfully!")

    except Exception as e:
        log(f"❌ backtest_scanner failed | {e}")
        traceback.print_exc()