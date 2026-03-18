import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

ts_code = '301282.SZ'
cursor.execute("SELECT trade_date, adj_factor FROM daily_bars WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 5", (ts_code,))
print(f"Latest adj_factors: {cursor.fetchall()}")

cursor.execute("SELECT trade_date, adj_factor FROM daily_bars WHERE ts_code = ? AND trade_date = '2025-04-01'", (ts_code,))
print(f"2025-04-01 adj_factor: {cursor.fetchone()}")

conn.close()
