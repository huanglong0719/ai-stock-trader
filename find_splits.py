
import sqlite3
import pandas as pd

conn = sqlite3.connect('backend/aitrader.db')
# 找到 2024-2025 年间复权因子变化较大的股票
query = """
SELECT ts_code, trade_date, adj_factor
FROM daily_bars
WHERE trade_date >= '2024-01-01'
ORDER BY ts_code, trade_date
"""
df = pd.read_sql_query(query, conn)
df['prev_adj'] = df.groupby('ts_code')['adj_factor'].shift(1)
splits = df[df['adj_factor'] != df['prev_adj']].dropna()
# 计算变化比例
splits['ratio'] = splits['adj_factor'] / splits['prev_adj']
# 找比例显著偏离 1.0 的（说明是送转，不只是分红）
large_splits = splits[abs(splits['ratio'] - 1.0) > 0.1]
print(large_splits.head(20))
conn.close()
