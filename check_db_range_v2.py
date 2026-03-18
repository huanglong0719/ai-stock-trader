
import sqlite3
import os
import pandas as pd

db_path = os.path.join('backend', 'aitrader.db')
conn = sqlite3.connect(db_path)

print("--- Overall Database Range ---")
df_overall = pd.read_sql("SELECT min(trade_date) as min_date, max(trade_date) as max_date, count(*) as count FROM daily_bars", conn)
print(df_overall)

print("\n--- Sample Stock Ranges (Top 5 by count) ---")
df_samples = pd.read_sql("""
    SELECT ts_code, min(trade_date) as min_date, max(trade_date) as max_date, count(*) as count 
    FROM daily_bars 
    GROUP BY ts_code 
    ORDER BY count DESC 
    LIMIT 5
""", conn)
print(df_samples)

print("\n--- Industry Data Range ---")
df_ind = pd.read_sql("SELECT min(trade_date) as min_date, max(trade_date) as max_date, count(*) as count FROM industry_data", conn)
print(df_ind)

print("\n--- Indicator Data Range ---")
df_indicators = pd.read_sql("SELECT min(trade_date) as min_date, max(trade_date) as max_date, count(*) as count FROM stock_indicators", conn)
print(df_indicators)

conn.close()
