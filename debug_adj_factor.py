import sqlite3
import os

db_path = os.path.join(os.getcwd(), "backend", "aitrader.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

stocks = ['605338.SH', '002864.SZ']
target_date = '2026-01-09'

print(f"--- Checking for {target_date} ---")

for code in stocks:
    print(f"\n[Stock: {code}]")
    
    # Check DailyBar
    cursor.execute("SELECT trade_date, adj_factor FROM daily_bars WHERE ts_code = ? AND trade_date = ?", (code, target_date))
    db_row = cursor.fetchone()
    if db_row:
        print(f"DailyBar: date={db_row[0]}, adj_factor={db_row[1]}")
    else:
        print("DailyBar: No data for this date")
        
    # Check StockIndicator
    cursor.execute("SELECT trade_date, adj_factor FROM stock_indicators WHERE ts_code = ? ORDER BY trade_date DESC LIMIT 1", (code,))
    si_row = cursor.fetchone()
    if si_row:
        print(f"Latest StockIndicator: date={si_row[0]}, adj_factor={si_row[1]}")
    else:
        print("StockIndicator: No data")

conn.close()
