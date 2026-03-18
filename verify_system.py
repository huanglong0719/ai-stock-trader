import sqlite3
import pandas as pd
from datetime import datetime, date, time

def verify_system_state():
    conn = sqlite3.connect('backend/aitrader.db')
    
    print("=== System Verification Report ===")
    
    # 1. Check Positions
    print("\n[Position Consistency]")
    df_pos = pd.read_sql_query("SELECT * FROM positions", conn)
    if df_pos.empty:
        print("No active positions.")
    else:
        # Check for vol <= 0
        invalid_pos = df_pos[df_pos['vol'] <= 0]
        if not invalid_pos.empty:
            print(f"CRITICAL: Found {len(invalid_pos)} positions with vol <= 0!")
            print(invalid_pos)
        else:
            print("All positions have vol > 0.")
            
        # Check available_vol consistency (T+1)
        today_start = datetime.combine(date.today(), time.min).strftime('%Y-%m-%d %H:%M:%S')
        df_buys = pd.read_sql_query(f"SELECT ts_code, SUM(vol) as today_buy_vol FROM trade_records WHERE trade_type='BUY' AND trade_time >= '{today_start}' GROUP BY ts_code", conn)
        
        for _, pos in df_pos.iterrows():
            today_buy = df_buys[df_buys['ts_code'] == pos['ts_code']]['today_buy_vol'].sum()
            expected_available = max(0, pos['vol'] - today_buy)
            if pos['available_vol'] != expected_available:
                print(f"WARNING: {pos['ts_code']} available_vol mismatch! Database: {pos['available_vol']}, Expected: {expected_available} (Vol: {pos['vol']}, Today Buy: {today_buy})")
            else:
                print(f"OK: {pos['ts_code']} available_vol consistent with T+1 rules.")

    # 2. Check Account Balance
    print("\n[Account Balance]")
    df_acc = pd.read_sql_query("SELECT * FROM accounts LIMIT 1", conn)
    if not df_acc.empty:
        acc = df_acc.iloc[0]
        total = acc['total_assets']
        cash = acc['available_cash']
        frozen = acc.get('frozen_cash', 0) if hasattr(acc, 'get') else acc['frozen_cash']
        mv = df_pos['market_value'].sum()
        diff = abs(total - (cash + frozen + mv))
        print(f"Total Assets: {total:.2f}")
        print(f"Available Cash: {cash:.2f}")
        print(f"Frozen Cash: {frozen:.2f}")
        print(f"Total Market Value: {mv:.2f}")
        if diff > 1.0: # Allow small rounding difference
            print(f"CRITICAL: Balance mismatch! Diff: {diff:.2f}")
        else:
            print("Account balance is consistent (Total = Cash + Frozen + MarketValue).")
    else:
        print("No account record found.")

    # 3. Check Trade-Plan Association
    print("\n[Trade-Plan Association]")
    df_trades = pd.read_sql_query("SELECT id, ts_code, trade_type, plan_id FROM trade_records ORDER BY trade_time DESC LIMIT 10", conn)
    missing_plan = df_trades[df_trades['plan_id'].isna()]
    if not missing_plan.empty:
        print(f"Found {len(missing_plan)} recent trades without plan_id association.")
        print(missing_plan)
    else:
        print("All recent trades are correctly associated with plans.")

    conn.close()

if __name__ == "__main__":
    verify_system_state()
