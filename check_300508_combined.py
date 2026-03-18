
import sqlite3
import pandas as pd

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)

print("--- 300508.SZ 2025-04 联表查询 (原始数据) ---")
query = """
SELECT 
    b.trade_date, 
    b.close as raw_close, 
    i.ma5 as raw_ma5, 
    i.ma10 as raw_ma10, 
    i.ma20 as raw_ma20,
    i.adj_factor
FROM daily_bars b
JOIN stock_indicators i ON b.ts_code = i.ts_code AND b.trade_date = i.trade_date
WHERE b.ts_code = '300508.SZ' AND b.trade_date >= '2025-03-25' AND b.trade_date <= '2025-04-10'
ORDER BY b.trade_date ASC
"""
df = pd.read_sql_query(query, conn)
print(df.to_string())
conn.close()
