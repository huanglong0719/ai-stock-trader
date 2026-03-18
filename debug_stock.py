import sqlite3
import pandas as pd

def check_stock_status(ts_code):
    conn = sqlite3.connect('backend/aitrader.db')
    
    print(f"--- Records for {ts_code} ---")
    
    print("\n[Positions]")
    df_pos = pd.read_sql_query(f"SELECT id, ts_code, vol, available_vol, avg_price, current_price, market_value, float_pnl, pnl_pct FROM positions WHERE ts_code = '{ts_code}'", conn)
    print(df_pos)
    
    print("\n[Trade Records (All)]")
    df_trades = pd.read_sql_query(f"SELECT * FROM trade_records WHERE ts_code = '{ts_code}' ORDER BY trade_time", conn)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df_trades)
    
    print("\n[Trading Plans]")
    df_plans = pd.read_sql_query(f"SELECT * FROM trading_plans WHERE ts_code = '{ts_code}' ORDER BY date DESC LIMIT 5", conn)
    print(df_plans)
    
    conn.close()

if __name__ == "__main__":
    check_stock_status('601611.SH')
