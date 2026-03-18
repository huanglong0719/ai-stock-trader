import sqlite3

def fix_missing_plan_ids():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    # Associate 601611.SH SELL trades with its original BUY plan (id=41)
    cursor.execute("UPDATE trade_records SET plan_id = 41 WHERE ts_code = '601611.SH' AND trade_type = 'SELL' AND plan_id IS NULL")
    updated_trades = cursor.rowcount
    
    # Mark the plan as fully exited if possible (set exit_price)
    # Get the latest sell price
    cursor.execute("SELECT price FROM trade_records WHERE ts_code = '601611.SH' AND trade_type = 'SELL' ORDER BY trade_time DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        exit_price = row[0]
        cursor.execute("UPDATE trading_plans SET exit_price = ?, executed = 1 WHERE id = 41", (exit_price,))
    
    conn.commit()
    conn.close()
    print(f"Associated {updated_trades} trades with plan 41 and updated plan status.")

if __name__ == "__main__":
    fix_missing_plan_ids()
