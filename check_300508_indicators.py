
import sqlite3
import pandas as pd

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)

print("--- 300508.SZ 2025-04 指标数据 ---")
query = """
SELECT trade_date, close, ma5, ma10, ma20, macd, adj_factor 
FROM stock_indicators 
WHERE ts_code = '300508.SZ' AND trade_date >= '2025-03-20' AND trade_date <= '2025-04-15'
ORDER BY trade_date ASC
"""
df = pd.read_sql_query(query, conn)
print(df.to_string())
conn.close()
