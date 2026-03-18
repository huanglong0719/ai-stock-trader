
import sqlite3
import os
from datetime import datetime

def check_20260109_integrity():
    db_path = os.path.join('backend', 'aitrader.db')
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    target_date = '2026-01-09'
    target_date_compact = '20260109'

    print(f"--- Data Integrity Check for {target_date} ---")

    # 1. Stocks with K-line data
    cursor.execute("SELECT count(distinct ts_code) FROM daily_bars WHERE trade_date = ?", (target_date,))
    kline_count = cursor.fetchone()[0]
    print(f"Stocks with Daily K-line: {kline_count}")

    # 2. Stocks with Daily Basic indicators
    cursor.execute("SELECT count(distinct ts_code) FROM daily_basics WHERE trade_date = ?", (target_date,))
    basic_count = cursor.fetchone()[0]
    print(f"Stocks with Daily Basic: {basic_count}")

    # 3. Stocks with calculated Indicators
    cursor.execute("SELECT count(distinct ts_code) FROM stock_indicators WHERE trade_date = ?", (target_date,))
    indicator_count = cursor.fetchone()[0]
    print(f"Stocks with Calculated Indicators (YYYY-MM-DD): {indicator_count}")

    # 4. Industry Data
    cursor.execute("SELECT count(distinct industry) FROM industry_data WHERE trade_date = ?", (target_date,))
    industry_data_count = cursor.fetchone()[0]
    print(f"Industries with Data: {industry_data_count}")

    # 5. Industry Indicators
    cursor.execute("SELECT count(distinct ts_code) FROM stock_indicators WHERE ts_code LIKE 'IND_%' AND trade_date = ?", (target_date,))
    industry_indicator_count = cursor.fetchone()[0]
    print(f"Industries with Calculated Indicators: {industry_indicator_count}")

    # Get total stocks count from stock table
    cursor.execute("SELECT count(*) FROM stocks")
    total_stocks = cursor.fetchone()[0]
    print(f"Total Stocks in database: {total_stocks}")

    # Identify some missing stocks for Daily K-line
    if kline_count < total_stocks:
        print("\nTop 10 missing stocks for Daily K-line:")
        cursor.execute("""
            SELECT ts_code FROM stocks 
            WHERE ts_code NOT IN (SELECT ts_code FROM daily_bars WHERE trade_date = ?)
            LIMIT 10
        """, (target_date,))
        missing = cursor.fetchall()
        for m in missing:
            print(f"  {m[0]}")

    conn.close()

if __name__ == "__main__":
    check_20260109_integrity()
