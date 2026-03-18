import sqlite3
import pandas as pd

conn = sqlite3.connect('backend/aitrader.db')
df = pd.read_sql_query("SELECT id, ts_code, executed, date FROM trading_plans WHERE ts_code = '601611.SH'", conn)
print(df)
conn.close()
