
import sqlite3
conn = sqlite3.connect('backend/aitrader.db')
cursor = conn.cursor()
ts_code = '000533.SZ'
cursor.execute("select trade_date from daily_bars where ts_code=? and trade_date like '2021-01-%'", (ts_code,))
print(f"Daily bars for {ts_code} in Jan 2021: {cursor.fetchall()}")
conn.close()
