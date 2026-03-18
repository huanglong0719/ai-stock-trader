import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

code = '301282.SZ'
print(f"--- Weekly columns in daily records for {code} ---")

query = f"""
    SELECT trade_date, ma5, weekly_ma5, weekly_macd, adj_factor
    FROM stock_indicators
    WHERE ts_code = '{code}'
    AND trade_date >= '2025-04-01'
    AND trade_date <= '2025-04-30'
    ORDER BY trade_date ASC
"""
df = pd.read_sql_query(query, conn)
print(df)

conn.close()
