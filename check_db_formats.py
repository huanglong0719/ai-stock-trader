import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Checking date formats in stock_indicators...")

cursor.execute("SELECT DISTINCT trade_date FROM stock_indicators ORDER BY trade_date DESC LIMIT 20")
dates = cursor.fetchall()
print(f"Distinct dates: {dates}")

cursor.execute("PRAGMA index_list('daily_bars')")
print(f"Indices on daily_bars: {cursor.fetchall()}")

cursor.execute("PRAGMA index_info('idx_ts_code_date')")
print(f"Info for idx_ts_code_date: {cursor.fetchall()}")

cursor.execute("SELECT count(*) FROM stock_indicators")
print(f"Total indicators: {cursor.fetchone()[0]}")

conn.close()
