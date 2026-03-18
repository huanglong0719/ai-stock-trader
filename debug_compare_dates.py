import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')

def compare_codes():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    ts_code = '301282.SZ'
    print(f"Comparing ts_code for {ts_code}...")
    
    print("\n--- daily_bars ---")
    cursor.execute("SELECT DISTINCT ts_code FROM daily_bars WHERE ts_code = ?", (ts_code,))
    db_row = cursor.fetchone()
    print(f"'{db_row[0]}'" if db_row else "Not found")
        
    print("\n--- stock_indicators ---")
    cursor.execute("SELECT DISTINCT ts_code FROM stock_indicators WHERE ts_code = ?", (ts_code,))
    si_row = cursor.fetchone()
    print(f"'{si_row[0]}'" if si_row else "Not found")

    conn.close()

if __name__ == "__main__":
    compare_codes()
