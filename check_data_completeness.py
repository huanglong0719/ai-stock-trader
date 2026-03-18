import sqlite3

db_path = 'backend/aitrader.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

target_date = '2026-01-09'

print(f"Checking data integrity for {target_date}...")

# 1. Total Stocks and Industries
cursor.execute("SELECT count(*) FROM stocks")
total_stocks = cursor.fetchone()[0]
cursor.execute("SELECT count(distinct industry) FROM stocks WHERE industry IS NOT NULL AND industry != ''")
total_industries = cursor.fetchone()[0]

# 2. Daily Bars (K-lines)
cursor.execute("SELECT count(*) FROM daily_bars WHERE trade_date = ?", (target_date,))
daily_bars_count = cursor.fetchone()[0]

# 3. Industry Data
cursor.execute("SELECT count(*) FROM industry_data WHERE trade_date = ?", (target_date,))
industry_data_count = cursor.fetchone()[0]

# 4. Daily Basics
cursor.execute("SELECT count(*) FROM daily_basics WHERE trade_date = ?", (target_date,))
daily_basics_count = cursor.fetchone()[0]

print("-" * 40)
print(f"Stocks: {total_stocks}")
print(f"Industries: {total_industries}")
print("-" * 40)
print(f"Daily Bars for {target_date}: {daily_bars_count} / {total_stocks} ({daily_bars_count/total_stocks*100:.2f}%)")
print(f"Industry Data for {target_date}: {industry_data_count} / {total_industries} ({industry_data_count/total_industries*100:.2f}%)")
print(f"Daily Basics for {target_date}: {daily_basics_count} / {total_stocks} ({daily_basics_count/total_stocks*100:.2f}%)")
print("-" * 40)

if daily_bars_count < total_stocks or industry_data_count < total_industries or daily_basics_count < total_stocks:
    print("DATA INCOMPLETE! Need to sync missing data.")
else:
    print("DATA COMPLETE for 2026-01-09.")

conn.close()
