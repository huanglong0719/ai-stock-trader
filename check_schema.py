
import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

for table in ['daily_bars', 'stock_indicators']:
    print(f"\nSchema for {table}:")
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
    row = cursor.fetchone()
    if row:
        print(row[0])
    else:
        print("Table not found")

conn.close()
