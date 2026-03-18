
import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT min(trade_date), max(trade_date), count(*) FROM daily_bars")
res = cursor.fetchone()
print(f"Daily Bars: {res}")

cursor.execute("SELECT min(trade_date), max(trade_date), count(*) FROM industry_data")
res = cursor.fetchone()
print(f"Industry Data: {res}")

conn.close()
