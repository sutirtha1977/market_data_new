import os
import pandas as pd
import traceback
from config.logger import log
from config.paths import SCANNER_FOLDER
from db.connection import get_db_connection, close_db_connection

def backtest_scanner(file_name: str, holding_days: int = 5):
    """
    Backtest a scanner CSV:
    - Buy next day's open after signal.
    - Sell after `holding_days` trading days (or last available day if fewer).
    - Computes % gain, win rate, max loss, average gain.
    """
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
        trade_info = []

        for _, row in df.iterrows():
            symbol_id = row['symbol_id']
            signal_date = row['date']

            sql = """
                SELECT date, open, close
                FROM equity_price_data
                WHERE symbol_id = ? AND date >= ? AND timeframe='1d'
                ORDER BY date ASC
                LIMIT ?
            """
            df_price = pd.read_sql(sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d"), holding_days+1))

            if len(df_price) < 2:
                # Not enough data to trade
                gains.append(None)
                trade_info.append((symbol_id, signal_date, None, None))
                continue

            buy_price = df_price.iloc[1]['open']
            sell_idx = holding_days if len(df_price) > holding_days else len(df_price)-1
            sell_price = df_price.iloc[sell_idx]['close']

            gain_pct = round((sell_price - buy_price)/buy_price*100, 2)
            gains.append(gain_pct)
            trade_info.append((symbol_id, signal_date, buy_price, sell_price))

        df['gain_pct'] = gains
        df['win'] = df['gain_pct'] > 0

        # Filter out incomplete trades
        valid_gains = [g for g in gains if g is not None]

        print(f"📈 Total trades: {len(valid_gains)}")
        print(f"📈 Win Rate ({holding_days}D): {sum(g>0 for g in valid_gains)/len(valid_gains)*100:.2f}%")
        print(f"❌ Max Loss ({holding_days}D): {min(valid_gains):.2f}%")
        print(f"💰 Average Gain ({holding_days}D): {sum(valid_gains)/len(valid_gains):.2f}%")

        # Optional: save detailed trade info
        trade_df = pd.DataFrame(trade_info, columns=['symbol_id', 'signal_date', 'buy_price', 'sell_price'])
        trade_df['gain_pct'] = gains
        trade_df['win'] = df['win']

        close_db_connection(conn)
        print("✅ Backtest completed successfully!")

        return df, trade_df

    except Exception as e:
        log(f"❌ backtest_scanner failed | {e}")
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame()

# def backtest_scanner(file_name: str):
#     """Backtest a scanner CSV by buying next day's open and selling 5 days later."""
#     try:
#         filepath = os.path.join(SCANNER_FOLDER, file_name)
#         df = pd.read_csv(filepath, dtype={'date': 'object'})
#         df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
#         required_cols = ['symbol_id', 'symbol', 'date']
#         if not all(col in df.columns for col in required_cols):
#             raise ValueError(f"File must contain columns: {required_cols}")

#         df = df.sort_values(['date', 'symbol']).reset_index(drop=True)
#         conn = get_db_connection()
#         gains = []

#         for _, row in df.iterrows():
#             symbol_id = row['symbol_id']
#             signal_date = row['date']
#             sql = """
#                 SELECT date, open, close
#                 FROM equity_price_data
#                 WHERE symbol_id = ? AND date >= ? AND timeframe='1d'
#                 ORDER BY date ASC
#                 LIMIT 6
#             """
#             df_price = pd.read_sql(sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
#             if len(df_price) < 2:
#                 gains.append(None)
#                 continue
#             buy_price = df_price.iloc[1]['open']
#             sell_price = df_price.iloc[5]['close'] if len(df_price) >= 6 else df_price.iloc[-1]['close']
#             gains.append(round((sell_price - buy_price)/buy_price*100, 2))

#         df['gain_5d_pct'] = gains
#         df['win_5d'] = df['gain_5d_pct'] > 0
#         print(f"📈 Win Rate (5D): {df['win_5d'].mean()*100:.2f}%")
#         print(f"❌ Max Loss (5D): {df['gain_5d_pct'].min():.2f}%")
#         close_db_connection(conn)
#         print("Backtest completed successfully!")

#     except Exception as e:
#         log(f"❌ backtest_scanner failed | {e}")
#         traceback.print_exc()