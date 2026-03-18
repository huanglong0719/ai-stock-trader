import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

code = '300508.SZ'
print(f"--- Indicator comparison for {code} around 2025-04-07 ---")

query = f"""
SELECT trade_date, ma5, ma10, ma20, adj_factor 
FROM stock_indicators 
WHERE ts_code = '{code}' AND trade_date >= '2025-04-01' AND trade_date <= '2025-04-10'
ORDER BY trade_date
"""
df = pd.read_sql_query(query, conn)

# Get the latest adj_factor for QFQ simulation
latest_adj = 1.9789

# Simulating the frontend QFQ logic (_apply_qfq)
# Price_qfq = Price_raw * (adj_factor / latest_adj)
# For indicators, if stored as raw values: Ind_qfq = Ind_raw * (adj_factor / latest_adj)

for col in ['ma5', 'ma10', 'ma20']:
    df[f'{col}_qfq'] = df[col] * (df['adj_factor'] / latest_adj)

print(df)

# Check DailyBar price for these days
query_p = f"SELECT trade_date, close, adj_factor FROM daily_bars WHERE ts_code = '{code}' AND trade_date >= '2025-04-01' AND trade_date <= '2025-04-10' ORDER BY trade_date"
df_p = pd.read_sql_query(query_p, conn)
print("\nRaw Prices:")
print(df_p)

conn.close()
