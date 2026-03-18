import sqlite3
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

code = '301282.SZ'
query = f"""
    SELECT d.trade_date 
    FROM daily_bars d 
    LEFT JOIN stock_indicators i ON d.ts_code = i.ts_code AND d.trade_date = i.trade_date 
    WHERE d.ts_code = '{code}' 
    AND d.trade_date >= '2025-04-01' 
    AND d.trade_date <= '2025-04-30' 
    AND i.trade_date IS NULL
"""
missing = conn.execute(query).fetchall()
print(f"Missing indicator days for {code} in April: {missing}")

# 检查 300508.SZ
code2 = '300508.SZ'
query2 = query.replace(code, code2)
missing2 = conn.execute(query2).fetchall()
print(f"Missing indicator days for {code2} in April: {missing2}")

conn.close()
