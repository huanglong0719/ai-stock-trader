import sqlite3
import datetime

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    print("Dropping old stock_indicators table...")
    cursor.execute("DROP TABLE IF EXISTS stock_indicators")
    
    print("Creating new stock_indicators table with correct schema...")
    cursor.execute("""
    CREATE TABLE stock_indicators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_code VARCHAR NOT NULL,
        trade_date DATE,
        ma5 FLOAT,
        ma10 FLOAT,
        ma20 FLOAT,
        ma60 FLOAT,
        vol_ma5 FLOAT,
        macd_diff FLOAT,
        macd_dea FLOAT,
        weekly_ma20 FLOAT,
        weekly_ma20_slope FLOAT,
        is_weekly_bullish INTEGER,
        monthly_ma20 FLOAT,
        is_monthly_bullish INTEGER,
        is_daily_bullish INTEGER,
        is_trend_recovering INTEGER,
        adj_factor FLOAT DEFAULT 1.0,
        updated_at DATETIME
    )
    """)
    
    print("Creating indices...")
    cursor.execute("CREATE INDEX ix_stock_indicators_ts_code ON stock_indicators (ts_code)")
    cursor.execute("CREATE INDEX ix_stock_indicators_trade_date ON stock_indicators (trade_date)")
    cursor.execute("CREATE UNIQUE INDEX idx_indicator_ts_code_date ON stock_indicators (ts_code, trade_date)")
    
    conn.commit()
    print("Schema fix complete!")
except Exception as e:
    conn.rollback()
    print(f"Error: {e}")
finally:
    conn.close()
