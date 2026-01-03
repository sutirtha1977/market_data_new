import pandas as pd
import os
import shutil
import seaborn as sns
import matplotlib.pyplot as plt
from data_manager import get_db_connection, close_db_connection

# ---------------- Configuration ----------------
TIMEFRAME = "1d"
BIG_MOVE_PCT = 15  # threshold for big moves
OUTPUT_DIR = "./data_analysis/"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "big_moves_prev_day_with_ratios.csv")
HEATMAP_FILE_RATIO1 = os.path.join(OUTPUT_DIR, "rsi9_ratio1_heatmap.png")
HEATMAP_FILE_RATIO2 = os.path.join(OUTPUT_DIR, "rsi9_ratio2_heatmap.png")

# -----------------------------
# Fetch big moves and previous day indicators with ratios
# -----------------------------
def fetch_big_moves_prev_day_with_ratios(min_pct=BIG_MOVE_PCT):
    conn = get_db_connection()
    try:
        sql = f"""
        SELECT prev.symbol_id,
               prev.date AS prev_date,
               prev.rsi_3,
               prev.rsi_9,
               prev.rsi_14,
               prev.ema_rsi_9_3,
               prev.wma_rsi_9_21
        FROM equity_indicators curr
        JOIN equity_price_data e
          ON e.symbol_id = curr.symbol_id
         AND e.date = curr.date
         AND e.timeframe = '{TIMEFRAME}'
        JOIN equity_indicators prev
          ON prev.symbol_id = curr.symbol_id
         AND prev.date = date(curr.date, '-1 day')
         AND prev.timeframe = '1d'
        WHERE curr.timeframe = '{TIMEFRAME}'
          AND curr.pct_price_change >= {min_pct}
        ORDER BY curr.symbol_id, prev.date
        """
        df = pd.read_sql(sql, conn)

        if df.empty:
            print("No big moves found.")
            return df

        # Convert prev_date to datetime and filter out data before 2010
        df['prev_date'] = pd.to_datetime(df['prev_date'])
        df = df[df['prev_date'].dt.year >= 2010]

        # Convert numeric columns
        numeric_cols = ['rsi_3', 'rsi_9', 'ema_rsi_9_3', 'wma_rsi_9_21']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Compute ratios
        df['rsi9_over_ema_rsi9_3'] = df['rsi_9'] / df['ema_rsi_9_3']   # Ratio 1
        df['ema_rsi9_3_over_wma_rsi9_21'] = df['ema_rsi_9_3'] / df['wma_rsi_9_21']  # Ratio 2

        # Clear and create output directory
        if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Save CSV
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"Saved previous day indicators with ratios to {OUTPUT_FILE}")

        return df
    finally:
        close_db_connection(conn)

# -----------------------------
# Generate heatmap for RSI_9 vs ratio
# -----------------------------
def generate_heatmap(df, ratio_col, ratio_label, output_file):
    x_bins = [0, 30, 40, 50, 60, 70, 100]
    y_bins = [0, 0.9, 1.0, 1.1, 1.2, 2.0]
    x_labels = ['0-30','30-40','40-50','50-60','60-70','>70']
    y_labels = ['<0.9','0.9-1.0','1.0-1.1','1.1-1.2','>1.2']

    df['rsi9_bin'] = pd.cut(df['rsi_9'], bins=x_bins, labels=x_labels, include_lowest=True)
    df['ratio_bin'] = pd.cut(df[ratio_col], bins=y_bins, labels=y_labels, include_lowest=True)

    heatmap_data = df.groupby(['rsi9_bin', 'ratio_bin']).size().unstack(fill_value=0)
    heatmap_prob = heatmap_data / heatmap_data.sum().sum()

    plt.figure(figsize=(10, 6))
    sns.heatmap(heatmap_prob, annot=True, fmt=".2f", cmap="YlGnBu")
    plt.title(f"RSI_9 vs {ratio_label} Heatmap")
    plt.ylabel("RSI_9 bins")
    plt.xlabel(f"{ratio_label} bins")
    plt.tight_layout()
    plt.savefig(output_file)
    plt.show()
    print(f"Saved heatmap to {output_file}")

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    df_big_moves_prev = fetch_big_moves_prev_day_with_ratios(min_pct=BIG_MOVE_PCT)
    if not df_big_moves_prev.empty:
        print("Sample rows with ratios:")
        print(df_big_moves_prev.head())

        # Heatmap 1: RSI_9 vs Ratio 1
        generate_heatmap(df_big_moves_prev, 'rsi9_over_ema_rsi9_3', 'RSI9/EMA_RSI9_3', HEATMAP_FILE_RATIO1)

        # Heatmap 2: RSI_9 vs Ratio 2
        generate_heatmap(df_big_moves_prev, 'ema_rsi9_3_over_wma_rsi9_21', 'EMA_RSI9_3/WMA_RSI9_21', HEATMAP_FILE_RATIO2)