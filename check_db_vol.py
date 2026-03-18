import sqlite3
import os

db_path = 'aitrader.db'
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("--- 002353.SZ Daily Bars in Dec 2025 ---")
cursor.execute("SELECT trade_date, vol FROM daily_bars WHERE ts_code = '002353.SZ' AND trade_date >= '20251201' ORDER BY trade_date DESC LIMIT 10")
for row in cursor.fetchall():
    print(row)

print("\n--- 002353.SZ Monthly Bars ---")
cursor.execute("SELECT trade_date, vol FROM monthly_bars WHERE ts_code = '002353.SZ' ORDER BY trade_date DESC LIMIT 5")
for row in cursor.fetchall():
    print(row)

print("\n--- 002353.SZ Daily Vol Sum for Nov 2025 ---")
cursor.execute("SELECT SUM(vol) FROM daily_bars WHERE ts_code = '002353.SZ' AND trade_date >= '20251101' AND trade_date <= '20251130'")
print(cursor.fetchone()[0])

print("\n--- 002353.SZ Daily Vol Sum for Dec 2025 ---")
cursor.execute("SELECT SUM(vol) FROM daily_bars WHERE ts_code = '002353.SZ' AND trade_date >= '20251201'")
print(cursor.fetchone()[0])

conn.close()
