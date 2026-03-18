
import sqlite3
import pandas as pd

conn = sqlite3.connect('backend/aitrader.db')
query = """
SELECT trade_date, adj_factor, close 
FROM daily_bars 
WHERE ts_code = '301282.SZ' 
AND trade_date BETWEEN '2025-01-01' AND '2025-06-30'
ORDER BY trade_date
"""
df = pd.read_sql_query(query, conn)
# 打印复权因子变化的点
diffs = df[df['adj_factor'].diff() != 0]
print("Adjustment Factor Changes:")
print(diffs)

# 打印 4 月份的数据
print("\nApril 2025 Data:")
print(df[df['trade_date'].str.contains('2025-04')])

conn.close()
