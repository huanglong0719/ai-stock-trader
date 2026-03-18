
import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

ts_code = '600686.SH'
cursor.execute(f"SELECT trade_date, macd, macd_dea, macd_diff FROM stock_indicators WHERE ts_code = '{ts_code}' ORDER BY trade_date DESC LIMIT 5")
rows = cursor.fetchall()

print(f"Latest 5 rows for {ts_code} in stock_indicators:")
for row in rows:
    print(row)

conn.close()
