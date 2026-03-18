
import sqlite3
import pandas as pd

conn = sqlite3.connect('backend/aitrader.db')
ts_code = '000153.SZ'
split_date = '2024-06-07'

# 获取原始价格和指标
query = f"""
SELECT b.trade_date, b.close, b.adj_factor, i.ma20
FROM daily_bars b
JOIN stock_indicators i ON b.ts_code = i.ts_code AND b.trade_date = i.trade_date
WHERE b.ts_code = '{ts_code}' 
AND b.trade_date BETWEEN '2024-05-15' AND '2024-06-30'
ORDER BY b.trade_date
"""
df = pd.read_sql_query(query, conn)

# 计算 QFQ 价格和 QFQ 指标 (锚点为 split_date 当天的 adj_factor)
anchor_adj = 8.2541
df['close_qfq'] = df['close'] * (df['adj_factor'] / anchor_adj)
df['ma20_qfq'] = df['ma20'] * (df['adj_factor'] / anchor_adj)

# 手动计算基于 close_qfq 的 MA20
df['ma20_manual'] = df['close_qfq'].rolling(window=20).mean()

print(f"Checking {ts_code} around split date {split_date}")
print(df)

conn.close()
