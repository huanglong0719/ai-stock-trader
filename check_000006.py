import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

code = '301282.SZ'
print(f"--- Data for {code} on 2025-04-17 ---")

query = f"""
    SELECT 
        d.trade_date, d.close, d.adj_factor as db_adj,
        i.ma5, i.adj_factor as ind_adj, i.macd
    FROM daily_bars d
    JOIN stock_indicators i ON d.ts_code = i.ts_code AND d.trade_date = i.trade_date
    WHERE d.ts_code = '{code}' AND d.trade_date = '2025-04-17'
"""
df = pd.read_sql_query(query, conn)
print(df)

# Check 5 days around it
query2 = f"""
    SELECT 
        d.trade_date, d.close, d.adj_factor as db_adj,
        i.ma5, i.adj_factor as ind_adj
    FROM daily_bars d
    JOIN stock_indicators i ON d.ts_code = i.ts_code AND d.trade_date = i.trade_date
    WHERE d.ts_code = '{code}' AND d.trade_date >= '2025-04-10' AND d.trade_date <= '2025-04-25'
    ORDER BY d.trade_date ASC
"""
df2 = pd.read_sql_query(query2, conn)
print("\n--- Surrounding days ---")
print(df2)

conn.close()
