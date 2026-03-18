import sqlite3
import pandas as pd

conn = sqlite3.connect('backend/aitrader.db')
query = "SELECT trade_date, macd, macd_dea, macd_diff FROM stock_indicators WHERE ts_code = '000009.SZ' ORDER BY trade_date DESC LIMIT 5"
df = pd.read_sql(query, conn)
print(df)
conn.close()
