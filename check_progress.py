
import sqlite3
import os
import time

db_path = r'd:\木偶说\backend\aitrader.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT count(DISTINCT ts_code) FROM stock_indicators")
count = cursor.fetchone()[0]

cursor.execute("SELECT count(*) FROM stocks")
total = cursor.fetchone()[0]

print(f"Progress: {count}/{total} stocks processed.")

conn.close()
