import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

# 检查 2025-04-03 和 2025-04-07 之间价格变动剧烈但复权因子未变的股票
query = """
    SELECT 
        a.ts_code,
        a.trade_date as date1, a.close as close1, a.adj_factor as adj1,
        b.trade_date as date2, b.close as close2, b.adj_factor as adj2,
        (b.close / a.close - 1) * 100 as pct_drop
    FROM daily_bars a
    JOIN daily_bars b ON a.ts_code = b.ts_code
    WHERE a.trade_date = '2025-04-03' 
    AND b.trade_date = '2025-04-07'
    AND ABS(b.close / a.close - 1) > 0.1  -- 价格变动超过 10%
    AND ABS(a.adj_factor - b.adj_factor) < 0.0001 -- 复权因子没变
    LIMIT 20
"""

df = pd.read_sql_query(query, conn)
print("--- Stocks with >10% price change but NO adj_factor change (2025-04-03 to 2025-04-07) ---")
print(df)

conn.close()
