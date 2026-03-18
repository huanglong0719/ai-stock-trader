import sqlite3
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

dates = ['2025-04-03', '2025-04-07', '2025-04-10', '2025-04-17']
for d in dates:
    query = f"SELECT COUNT(*) FROM stock_indicators WHERE trade_date='{d}' AND macd IS NULL"
    count = conn.execute(query).fetchone()[0]
    print(f"Stocks with null MACD on {d}: {count}")

# 检查是否存在大量指标值为 0 的情况 (除了 MACD 偶尔为 0)
for d in dates:
    query = f"SELECT COUNT(*) FROM stock_indicators WHERE trade_date='{d}' AND ma5 = 0"
    count = conn.execute(query).fetchone()[0]
    print(f"Stocks with MA5 = 0 on {d}: {count}")

# 检查 301282.SZ 的复权因子一致性
query = """
    SELECT d.ts_code, d.trade_date, d.adj_factor as db_adj, i.adj_factor as ind_adj, i.macd
    FROM daily_bars d
    LEFT JOIN stock_indicators i ON d.ts_code = i.ts_code AND d.trade_date = i.trade_date
    WHERE d.trade_date = '2025-04-17'
    AND ABS(d.adj_factor - i.adj_factor) > 0.0001
    LIMIT 10
"""
mismatches = conn.execute(query).fetchall()
print(f"\nStocks with adj_factor mismatch on 2025-04-17: {len(mismatches)}")
for m in mismatches:
    print(m)

conn.close()
