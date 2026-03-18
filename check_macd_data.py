
import sqlite3
import pandas as pd

def check_db():
    conn = sqlite3.connect('d:/木偶说/backend/aitrader.db')
    print("--- stock_indicators Table Info ---")
    info = pd.read_sql_query("PRAGMA table_info(stock_indicators)", conn)
    print(info)
    
    print("\n--- Sample Data for 000001.SZ ---")
    sample = pd.read_sql_query("SELECT * FROM stock_indicators WHERE ts_code = '000001.SZ' ORDER BY trade_date DESC LIMIT 5", conn)
    print(sample)
    
    print("\n--- Sample Data for 600686.SH ---")
    sample2 = pd.read_sql_query("SELECT * FROM stock_indicators WHERE ts_code = '600686.SH' ORDER BY trade_date DESC LIMIT 5", conn)
    print(sample2)
    
    conn.close()

if __name__ == "__main__":
    check_db()
