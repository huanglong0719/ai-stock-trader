
import sqlite3
import os

db_path = 'data/stock_data.db'
if not os.path.exists(db_path):
    db_path = 'stock_data.db'

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

ts_code = '000533.SZ'
print(f"--- Monthly bars for {ts_code} ---")
cursor.execute("select trade_date, close from monthly_bars where ts_code=? order by trade_date desc limit 20", (ts_code,))
for row in cursor.fetchall():
    print(row)

print(f"\n--- Weekly bars for {ts_code} ---")
cursor.execute("select trade_date, close from weekly_bars where ts_code=? order by trade_date desc limit 20", (ts_code,))
for row in cursor.fetchall():
    print(row)

conn.close()
