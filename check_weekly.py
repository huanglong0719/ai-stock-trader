import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

code = '301282.SZ'
print(f"--- Weekly indicators for {code} ---")

query = f"""
    SELECT trade_date, ma5, ma10, ma20, macd, adj_factor
    FROM stock_indicators
    WHERE ts_code = '{code}'
    AND trade_date LIKE '2025-04-%'
    ORDER BY trade_date ASC
"""
# Wait, this query is for DAILY indicators.
# Weekly indicators are stored with a specific date (usually Friday).

query_weekly = f"""
    SELECT trade_date, ma5, ma10, ma20, macd, adj_factor
    FROM stock_indicators
    WHERE ts_code = '{code}'
    AND trade_date >= '2025-01-01'
    ORDER BY trade_date ASC
"""
# Actually, I need to know which records are Weekly.
# Looking at indicator_service.py, it stores weekly/monthly in the same table?
# No, usually they are separate or have a flag.
# Let's check the schema of stock_indicators.

conn.close()
