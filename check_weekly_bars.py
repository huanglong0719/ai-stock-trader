
import sqlite3
import os

db_path = "backend/aitrader.db"
if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    ts_code = '000001.SZ'
    print(f"--- Checking weekly_bars for {ts_code} ---")
    cursor.execute("""
        SELECT trade_date, open, high, low, close, vol, adj_factor 
        FROM weekly_bars 
        WHERE ts_code = ? 
        ORDER BY trade_date DESC 
        LIMIT 5
    """, (ts_code,))
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    conn.close()
else:
    print(f"DB not found at {db_path}")
