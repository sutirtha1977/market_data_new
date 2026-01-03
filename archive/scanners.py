import os
import pandas as pd
import traceback
from datetime import datetime
from helper import log, SCANNER_FOLDER
from data_manager import get_db_connection, close_db_connection

#################################################################################################
# EXPORT DATAFRAME TO CSV
#################################################################################################
def export_to_csv(df: pd.DataFrame, name: str) -> str:
    """
    Export DataFrame to a timestamped CSV in the scanner folder.
    Returns the full file path.
    """
    try:
        os.makedirs(SCANNER_FOLDER, exist_ok=True)
        ts = datetime.now().strftime("%d%b%Y")
        filename = f"Scanner_{name}_{ts}.csv"
        filepath = os.path.join(SCANNER_FOLDER, filename)
        df.to_csv(filepath, index=False)
        log(f"✔ Scanner saved at {filepath}")
        return filepath
    except Exception as e:
        log(f"❌ CSV export failed | {e}")
        traceback.print_exc()
        return ""

#################################################################################################
# GET BASE DATA FOR SCANNERS
#################################################################################################
def get_base_data(lookback_days: int = 365) -> pd.DataFrame:
    """
    Fetches daily equity indicators and aligns weekly & monthly RSI values.
    Returns a DataFrame ready for scanners.
    """
    conn = get_db_connection()
    try:
        # DAILY
        daily_sql = f"""
            SELECT d.symbol_id, s.symbol, d.date, p.adj_close,
                   d.rsi_3, d.rsi_9, d.ema_rsi_9_3, d.wma_rsi_9_21, d.pct_price_change, p.delv_pct, d.sma_20
            FROM equity_indicators d
            JOIN equity_price_data p ON p.symbol_id = d.symbol_id AND p.date = d.date AND p.timeframe='1d'
            JOIN equity_symbols s ON s.symbol_id = d.symbol_id
            WHERE d.timeframe = '1d' AND d.date >= date('now','-{lookback_days} days')
        """
        df_daily = pd.read_sql(daily_sql, conn).sort_values(['symbol_id','date'])
        df_daily['delv_pct'] = pd.to_numeric(df_daily['delv_pct'], errors='coerce')
        df_daily['prev_rsi_3'] = df_daily.groupby('symbol_id')['rsi_3'].shift(1)

        # WEEKLY
        df_weekly = pd.read_sql(f"""
            SELECT symbol_id, date, rsi_3
            FROM equity_indicators
            WHERE timeframe='1wk' AND date >= date('now','-{lookback_days} days')
        """, conn)

        # MONTHLY
        df_monthly = pd.read_sql(f"""
            SELECT symbol_id, date, rsi_3
            FROM equity_indicators
            WHERE timeframe='1mo' AND date >= date('now','-{lookback_days} days')
        """, conn)

        # Align weekly and monthly <= daily
        df_daily = (
            df_daily
            .merge(df_weekly.rename(columns={'rsi_3':'rsi_3_weekly','date':'weekly_date'}), on='symbol_id', how='left')
            .query("weekly_date <= date")
            .sort_values(['symbol_id','date','weekly_date'])
            .groupby(['symbol_id','date']).last().reset_index()
        )

        df_daily = (
            df_daily
            .merge(df_monthly.rename(columns={'rsi_3':'rsi_3_monthly','date':'monthly_date'}), on='symbol_id', how='left')
            .query("monthly_date <= date")
            .sort_values(['symbol_id','date','monthly_date'])
            .groupby(['symbol_id','date']).last().reset_index()
        )

        return df_daily

    finally:
        close_db_connection(conn)

#################################################################################################
# SCANNER: HILEGA MILEGA
#################################################################################################
def scanner_hilega_milega() -> pd.DataFrame:
    """
    Filters symbols based on multi-timeframe RSI, EMA/WMA and price conditions.
    Returns a filtered DataFrame and saves a CSV.
    """
    try:
        df = get_base_data()
        df_filtered = df[
            (df['adj_close'] >= 100) &
            (df['rsi_9'] / df['ema_rsi_9_3'] > 1.1) &
            (df['ema_rsi_9_3'] / df['wma_rsi_9_21'] > 1.1) &
            (df['prev_rsi_3'] <= 55) &
            (df['rsi_3'] > 55) &
            (df['rsi_3'] < 80) &
            (df['rsi_3_weekly'] > 50) &
            (df['rsi_3_monthly'] > 50) &
            (df['pct_price_change'] <= 5)
        ].sort_values(['date','symbol'], ascending=[False, True])
        export_to_csv(df_filtered, "HM")
        return df_filtered
    except Exception as e:
        log(f"❌ scanner_hilega_milega failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

#################################################################################################
# SCANNER: WIP (Work in Progress)
#################################################################################################
def scanner_WIP() -> pd.DataFrame:
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
        export_to_csv(df_filtered, "WIP")
        return df_filtered
    except Exception as e:
        log(f"❌ scanner_WIP failed | {e}")
        traceback.print_exc()
        return pd.DataFrame()

#################################################################################################
# BACKTEST SCANNER CSV
#################################################################################################
def backtest_scanner(file_name: str):
    """
    Backtest a scanner CSV by buying next day's open and selling 5 days later.
    Computes % gain/loss, win rate, and max loss.
    """
    try:
        filepath = os.path.join(SCANNER_FOLDER, file_name)
        print(f"Reading file: {filepath}")
        df = pd.read_csv(filepath, dtype={'date': 'object'})
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        required_cols = ['symbol_id', 'symbol', 'date']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"File must contain columns: {required_cols}")

        df = df.sort_values(['date', 'symbol']).reset_index(drop=True)
        print(f"Loaded {len(df)} rows")

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