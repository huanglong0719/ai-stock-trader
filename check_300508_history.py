
import sqlite3
import pandas as pd

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)
query = """
SELECT trade_date, open, high, low, close, adj_factor 
FROM daily_bars 
WHERE ts_code = '300508.SZ' AND trade_date <= '2025-04-10' 
ORDER BY trade_date DESC 
LIMIT 40
"""
df = pd.read_sql_query(query, conn)
print(df.to_string())
conn.close()
