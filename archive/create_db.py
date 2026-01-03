# =========================================================
# THIS FILE CONTAINS THE FOLLOWING FUNCTIONS:
# create_stock_database(drop_existing=True)
# =========================================================
import sqlite3
import os
from helper import (
    log, DB_FILE
)
from data_manager import (
    get_db_connection,
    close_db_connection
)

def create_stock_database(drop_existing=True):
    # -----------------------------
    # DELETE DB FIRST (CRITICAL)
    # -----------------------------
    if drop_existing and os.path.exists(DB_FILE):
        print("Existing database found. Deleting:", DB_FILE)
        os.remove(DB_FILE)

    # -----------------------------
    # CONNECT DATABASE
    # -----------------------------
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # =========================================================
        # EQUITY SYMBOLS
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_symbols (
            symbol_id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL UNIQUE,
            series TEXT,
            exchange TEXT,
            name TEXT,
            sector TEXT, 
            listing_date DATE,
            isin TEXT
        );
        """)

        # =========================================================
        # INDEX SYMBOLS
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS index_symbols (
            index_id INTEGER PRIMARY KEY,
            index_code TEXT NOT NULL UNIQUE,
            index_name TEXT NOT NULL,
            exchange TEXT NOT NULL,
            yahoo_symbol TEXT NOT NULL UNIQUE,
            category TEXT,
            is_active INTEGER DEFAULT 1
        );
        """)

        # =========================================================
        # TIMEFRAMES
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS timeframes (
            timeframe TEXT PRIMARY KEY,
            description TEXT
        );
        """)

        # =========================================================
        # EQUITY PRICE DATA
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_price_data (
            symbol_id INTEGER NOT NULL,
            timeframe TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume REAL,
            delv_pct REAL,
            is_final BOOLEAN NOT NULL DEFAULT TRUE,
            PRIMARY KEY (symbol_id, timeframe, date),
            FOREIGN KEY (symbol_id) REFERENCES equity_symbols(symbol_id),
            FOREIGN KEY (timeframe) REFERENCES timeframes(timeframe)
        );
        """)

        # =========================================================
        # INDEX PRICE DATA
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS index_price_data (
            index_id INTEGER NOT NULL,
            timeframe TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            PRIMARY KEY (index_id, timeframe, date),
            FOREIGN KEY (index_id) REFERENCES index_symbols(index_id),
            FOREIGN KEY (timeframe) REFERENCES timeframes(timeframe)
        );
        """)

        # =========================================================
        # EQUITY INDICATORS
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_indicators (
            symbol_id INTEGER NOT NULL,
            timeframe TEXT NOT NULL,
            date DATE NOT NULL,
            sma_20 REAL,
            sma_50 REAL,
            sma_200 REAL,
            rsi_3 REAL,
            rsi_9 REAL,
            rsi_14 REAL,
            macd REAL,
            macd_signal REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            atr_14 REAL,
            supertrend REAL,
            supertrend_dir INTEGER,
            ema_rsi_9_3 REAL,
            wma_rsi_9_21 REAL,
            pct_price_change REAL,
            is_final BOOLEAN NOT NULL DEFAULT TRUE,
            PRIMARY KEY (symbol_id, timeframe, date),
            FOREIGN KEY (symbol_id) REFERENCES equity_symbols(symbol_id),
            FOREIGN KEY (timeframe) REFERENCES timeframes(timeframe)
        );
        """)

        # =========================================================
        # INDEX INDICATORS
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS index_indicators (
            index_id INTEGER NOT NULL,
            timeframe TEXT NOT NULL,
            date DATE NOT NULL,
            sma_20 REAL,
            sma_50 REAL,
            sma_200 REAL,
            rsi_3 REAL,
            rsi_9 REAL,
            rsi_14 REAL,
            macd REAL,
            macd_signal REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            atr_14 REAL,
            supertrend REAL,
            supertrend_dir INTEGER,
            ema_rsi_9_3 REAL,
            wma_rsi_9_21 REAL,
            pct_price_change REAL,
            PRIMARY KEY (index_id, timeframe, date),
            FOREIGN KEY (index_id) REFERENCES index_symbols(index_id),
            FOREIGN KEY (timeframe) REFERENCES timeframes(timeframe)
        );
        """)

        # =========================================================
        # INDEX 52-WEEK STATS (DERIVED)
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS index_52week_stats (
            index_id INTEGER PRIMARY KEY,
            week52_high REAL,
            week52_low REAL,
            as_of_date DATE,
            FOREIGN KEY (index_id) REFERENCES index_symbols(index_id)
        );
        """)

        # =========================================================
        # EQUITY 52-WEEK STATS (DERIVED)
        # =========================================================
        cur.execute("""
        CREATE TABLE IF NOT EXISTS equity_52week_stats (
            symbol_id INTEGER PRIMARY KEY,
            week52_high REAL,
            week52_low REAL,
            as_of_date DATE,
            FOREIGN KEY (symbol_id) REFERENCES equity_symbols(symbol_id)
        );
        """)

        # =========================================================
        # INDEXES
        # =========================================================
        cur.execute("CREATE INDEX IF NOT EXISTS idx_eq_price ON equity_price_data(symbol_id, timeframe, date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_idx_price ON index_price_data(index_id, timeframe, date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_eq_ind ON equity_indicators(symbol_id, timeframe, date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_idx_ind ON index_indicators(index_id, timeframe, date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_eq_52w ON equity_52week_stats(symbol_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_idx_52w ON index_52week_stats(index_id);")

        # =========================================================
        # DEFAULT TIMEFRAMES
        # =========================================================
        cur.executemany("""
            INSERT OR IGNORE INTO timeframes (timeframe, description)
            VALUES (?, ?)
        """, [
            ("1d", "Daily"),
            ("1wk", "Weekly"),
            ("1mo", "Monthly")
        ])

        conn.commit()
        print("✅ Database created successfully:", DB_FILE)

    except Exception as e:
        print("❌ Error creating database:", e)
        conn.rollback()

    finally:
        close_db_connection(conn)


if __name__ == "__main__":
    create_stock_database(drop_existing=True)