
import sqlite3
conn = sqlite3.connect('backend/aitrader.db')
cursor = conn.cursor()
ts_code = '000533.SZ'
cursor.execute("select trade_date, count(*) from daily_bars where ts_code=? group by trade_date having count(*) > 1", (ts_code,))
duplicates = cursor.fetchall()
if duplicates:
    print(f"Duplicates found for {ts_code}: {duplicates}")
else:
    print(f"No duplicates in daily_bars for {ts_code}")

cursor.execute("select count(*) from monthly_bars where ts_code=?", (ts_code,))
print(f"Monthly bars for {ts_code}: {cursor.fetchone()[0]}")

conn.close()
