
import sqlite3
import os

db_path = 'backend/aitrader.db'
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
    exit(1)

print(f"--- Checking {db_path} ---")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

ts_code = '000533.SZ'
for table in ['monthly_bars', 'weekly_bars', 'daily_bars']:
    print(f"\nData from {table}:")
    try:
        cursor.execute(f"select trade_date, close from {table} where ts_code=? order by trade_date desc limit 20", (ts_code,))
        for row in cursor.fetchall():
            print(row)
    except Exception as e:
        print(f"Error querying {table}: {e}")
conn.close()
