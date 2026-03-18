import sqlite3
import os

db_path = r'backend\aitrader.db'

def check_missing_macd():
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT count(DISTINCT ts_code) FROM stock_indicators WHERE macd IS NULL;")
        count = cursor.fetchone()[0]
        print(f"Number of stocks with missing MACD: {count}")
        
        cursor.execute("SELECT count(*) FROM stock_indicators;")
        total_records = cursor.fetchone()[0]
        print(f"Total indicator records: {total_records}")

        cursor.execute("SELECT count(*) FROM stock_indicators WHERE macd IS NULL;")
        missing_records = cursor.fetchone()[0]
        print(f"Total records with missing MACD: {missing_records}")
        
        if count > 0:
            cursor.execute("SELECT DISTINCT ts_code FROM stock_indicators WHERE macd IS NULL LIMIT 10;")
            stocks = cursor.fetchall()
            print(f"Sample stocks with missing MACD: {[s[0] for s in stocks]}")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_missing_macd()
