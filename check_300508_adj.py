
import sqlite3
import pandas as pd

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)
query = """
SELECT trade_date, adj_factor 
FROM daily_bars 
WHERE ts_code = '300508.SZ' AND trade_date >= '2025-01-01' AND trade_date <= '2025-06-01'
GROUP BY adj_factor
ORDER BY trade_date ASC
"""
df = pd.read_sql_query(query, conn)
print("--- 300508.SZ 复权因子变动记录 ---")
print(df)
conn.close()
