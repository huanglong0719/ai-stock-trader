
import sqlite3
import pandas as pd

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)

print("正在扫描 2025-04-07 指标异常的股票...")
# 寻找 ma20 刚好等于 close (或差距极小) 的记录，这通常意味着 fillna(close) 触发了
query = """
SELECT i.ts_code, i.trade_date, i.ma20, b.close
FROM stock_indicators i
JOIN daily_bars b ON i.ts_code = b.ts_code AND i.trade_date = b.trade_date
WHERE i.trade_date = '2025-04-07' 
  AND ABS(i.ma20 - b.close) < 0.0001
"""
df = pd.read_sql_query(query, conn)
print(f"找到 {len(df)} 只受影响的股票。")
if len(df) > 0:
    print("前 10 只股票:")
    print(df.head(10))

conn.close()
