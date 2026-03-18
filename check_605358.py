
import sqlite3
import pandas as pd

def check_stock_data(ts_code):
    conn = sqlite3.connect('backend/aitrader.db')
    
    print(f"--- Monthly Bars for {ts_code} ---")
    monthly = pd.read_sql_query(f"SELECT trade_date, close, vol, amount FROM monthly_bars WHERE ts_code LIKE '%{ts_code}%' ORDER BY trade_date DESC LIMIT 12", conn)
    print(monthly)
    
    print(f"\n--- Weekly Bars for {ts_code} ---")
    weekly = pd.read_sql_query(f"SELECT trade_date, close, vol, amount FROM weekly_bars WHERE ts_code LIKE '%{ts_code}%' ORDER BY trade_date DESC LIMIT 12", conn)
    print(weekly)
    
    print(f"\n--- Daily Bars for {ts_code} ---")
    daily = pd.read_sql_query(f"SELECT trade_date, close, vol, amount FROM daily_bars WHERE ts_code LIKE '%{ts_code}%' ORDER BY trade_date DESC LIMIT 10", conn)
    print(daily)
    
    conn.close()

if __name__ == "__main__":
    check_stock_data("605358")
