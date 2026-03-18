
import sqlite3
import os

db_path = "backend/aitrader.db"
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    ts_code = '000001.SZ'
    print(f"--- Checking stock_indicators for {ts_code} ---")
    cursor.execute("""
        SELECT trade_date, ma5, macd, weekly_ma5, weekly_macd 
        FROM stock_indicators 
        WHERE ts_code = ? 
        ORDER BY trade_date DESC 
        LIMIT 10
    """, (ts_code,))
    rows = cursor.fetchall()
    for row in rows:
        print(f"Date: {row[0]}, MA5: {row[1]}, MACD: {row[2]}, W_MA5: {row[3]}, W_MACD: {row[4]}")
    
    conn.close()
else:
    print(f"DB not found at {db_path}")
