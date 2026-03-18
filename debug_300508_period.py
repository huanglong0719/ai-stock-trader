import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), 'backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

code = '300508.SZ'
query = f"SELECT trade_date, close, adj_factor FROM daily_bars WHERE ts_code = '{code}' AND trade_date >= '2025-01-01' AND trade_date <= '2025-04-30' ORDER BY trade_date"
df = pd.read_sql_query(query, conn)

# Calculate daily price change
df['pct_chg'] = df['close'].pct_change()

# Find any large price gaps without adj_factor change
anomalies = df[(df['pct_chg'].abs() > 0.09) & (df['adj_factor'].diff().abs() < 1e-6)]
print("Potential price anomalies (limit moves or gaps):")
print(anomalies)

# Check for any missing dates (trading days)
df['trade_date'] = pd.to_datetime(df['trade_date'])
df = df.set_index('trade_date')
all_dates = pd.date_range(start=df.index.min(), end=df.index.max(), freq='B')
missing = all_dates[~all_dates.isin(df.index)]
print("\nMissing business days:")
print(missing)
conn.close()
