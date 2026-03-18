import sqlite3
import os

db_path = os.path.join('backend', 'aitrader.db')

def check_indicators():
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    ts_code = '301282.SZ'
    print(f"Checking indicators for {ts_code} in April 2025...")
    
    query = """
    SELECT trade_date, ma5, ma10, ma20 
    FROM stock_indicators 
    WHERE ts_code = ? AND trade_date LIKE '2025-04%'
    ORDER BY trade_date ASC
    """
    
    cursor.execute(query, (ts_code,))
    rows = cursor.fetchall()
    
    if not rows:
        print("No records found in stock_indicators for this period.")
    else:
        print(f"{'Date':<12} | {'MA5':<8} | {'MA10':<8} | {'MA20':<8}")
        print("-" * 45)
        for row in rows:
            print(f"{row[0]:<12} | {row[1]:<8.2f} | {row[2]:<8.2f} | {row[3]:<8.2f}")

    conn.close()

if __name__ == "__main__":
    check_indicators()
