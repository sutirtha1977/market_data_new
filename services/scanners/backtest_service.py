import os
import pandas as pd
import traceback
from datetime import datetime
from config.logger import log
from config.paths import SCANNER_FOLDER
from db.connection import get_db_connection, close_db_connection
from services.scanners.export_service import export_to_csv

#################################################################################################
# Backtests all CSV scanner files in SCANNER_FOLDER.
# Exports a summary Excel with each file as a row and statistics as columns.
#################################################################################################  
def backtest_all_scanners():
    try:
        results = []

        # Find all scanner CSVs
        csv_files = [
            f for f in os.listdir(SCANNER_FOLDER)
            if f.lower().endswith(".csv")
        ]

        if not csv_files:
            print(f"❌ No scanner CSV files found in {SCANNER_FOLDER}")
            return

        conn = get_db_connection()

        for file_name in csv_files:
            filepath = os.path.join(SCANNER_FOLDER, file_name)
            try:
                df_csv = pd.read_csv(filepath, dtype={'date': 'object'})
                df_csv['date'] = pd.to_datetime(df_csv['date'])
                required_cols = ['symbol_id', 'symbol', 'date']
                if not all(col in df_csv.columns for col in required_cols):
                    print(f"⚠ Skipping {file_name}: missing columns {required_cols}")
                    continue

                trades = []

                for _, row in df_csv.iterrows():
                    symbol_id = row['symbol_id']
                    symbol = row['symbol']
                    signal_date = row['date']

                    # Buy next trading day
                    price_sql = """
                        SELECT date, open, close
                        FROM equity_price_data
                        WHERE symbol_id = ? AND timeframe='1d' AND date > ?
                        ORDER BY date ASC
                        LIMIT 1
                    """
                    buy_df = pd.read_sql(price_sql, conn, params=(symbol_id, signal_date.strftime("%Y-%m-%d")))
                    if buy_df.empty or pd.isna(buy_df.iloc[0]['open']):
                        continue

                    buy_date = buy_df.iloc[0]['date']
                    buy_price = buy_df.iloc[0]['open']

                    # ATR
                    atr_sql = """
                        SELECT atr_14
                        FROM equity_indicators
                        WHERE symbol_id = ? AND timeframe='1d' AND date = ?
                    """
                    atr_df = pd.read_sql(atr_sql, conn, params=(symbol_id, buy_date))
                    if atr_df.empty or pd.isna(atr_df.iloc[0]['atr_14']):
                        continue
                    atr = atr_df.iloc[0]['atr_14']

                    # Swing low last 10 days
                    swing_sql = """
                        SELECT MIN(low) AS swing_low
                        FROM equity_price_data
                        WHERE symbol_id = ? AND timeframe='1d' AND date < ?
                          AND date >= date(?, '-10 day')
                    """
                    swing_df = pd.read_sql(swing_sql, conn, params=(symbol_id, buy_date, buy_date))
                    swing_low = swing_df.iloc[0]['swing_low'] if not swing_df.empty else buy_price - atr
                    if pd.isna(swing_low):
                        swing_low = buy_price - atr

                    stop_loss = min(buy_price - atr, swing_low)
                    target = buy_price * 1.10

                    # Forward scan for exit
                    forward_sql = """
                        SELECT date, close
                        FROM equity_price_data
                        WHERE symbol_id = ? AND timeframe='1d' AND date >= ?
                        ORDER BY date ASC
                    """
                    forward_df = pd.read_sql(forward_sql, conn, params=(symbol_id, buy_date))

                    exit_price = None
                    exit_date = None
                    exit_reason = "EOD"

                    for _, p in forward_df.iterrows():
                        close_price = p['close']
                        if close_price <= stop_loss:
                            exit_price = close_price
                            exit_date = p['date']
                            exit_reason = "STOP"
                            break
                        if close_price >= target:
                            exit_price = close_price
                            exit_date = p['date']
                            exit_reason = "TARGET"
                            break

                    if exit_price is None and not forward_df.empty:
                        exit_price = forward_df.iloc[-1]['close']
                        exit_date = forward_df.iloc[-1]['date']

                    if exit_price is None:
                        continue

                    gain_pct = round((exit_price - buy_price) / buy_price * 100, 2)
                    trades.append({
                        "win": gain_pct > 0,
                        "gain_pct": gain_pct,
                        "exit_reason": exit_reason
                    })

                # Compute summary
                if trades:
                    trades_df = pd.DataFrame(trades)
                    win_rate = trades_df['win'].mean() * 100
                    avg_return = trades_df['gain_pct'].mean()
                    max_gain = trades_df['gain_pct'].max()
                    max_loss = trades_df['gain_pct'].min()
                    cumulative = (1 + trades_df['gain_pct']/100).cumprod()
                    running_max = cumulative.cummax()
                    drawdown = (cumulative - running_max) / running_max * 100
                    max_drawdown = drawdown.min()
                    stop_count = len(trades_df[trades_df['exit_reason']=="STOP"])
                else:
                    win_rate = avg_return = max_gain = max_loss = max_drawdown = stop_count = 0

                results.append({
                    "file_name": file_name.replace(".csv",""),
                    "total_trades": len(trades),
                    "win_rate_%": round(win_rate,2),
                    "avg_return_%": round(avg_return,2),
                    "max_gain_%": round(max_gain,2),
                    "max_loss_%": round(max_loss,2),
                    "max_drawdown_%": round(max_drawdown,2),
                    "stop_loss_trades": stop_count
                })

            except Exception as e_file:
                print(f"❌ Error processing {file_name} | {e_file}")
                traceback.print_exc()

        close_db_connection(conn)

        # Export summary
        result_df = pd.DataFrame(results)
        export_to_csv(result_df, SCANNER_FOLDER, "backtest_results")
        # print(f"\n✅ Backtest summary saved: {output_file}")
        return result_df

    except Exception as e:
        log(f"❌ backtest_all_scanners failed | {e}")
        traceback.print_exc()
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