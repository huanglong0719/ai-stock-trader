
import sqlite3
conn = sqlite3.connect('backend/aitrader.db')
cursor = conn.cursor()
ts_code = '002228.SZ'
cursor.execute("select trade_date, count(*) from daily_bars where ts_code=? group by trade_date having count(*) > 1", (ts_code,))
print(f"Duplicates for {ts_code}: {cursor.fetchall()}")
cursor.execute("select trade_date from daily_bars where ts_code=? and trade_date like '2021-01-%' order by trade_date", (ts_code,))
print(f"Daily dates for {ts_code} in Jan 2021: {cursor.fetchall()}")
conn.close()
