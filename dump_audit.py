import sqlite3
import json
from datetime import date

def dump_audit_data():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    print("=== ACCOUNT INFO ===")
    cursor.execute("SELECT * FROM accounts")
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(cols, row)))
        
    print("\n=== CURRENT POSITIONS ===")
    cursor.execute("SELECT * FROM positions")
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(cols, row)))
        
    print("\n=== TRADE RECORDS (HISTORY) ===")
    cursor.execute("SELECT * FROM trade_records ORDER BY trade_time ASC")
    cols = [d[0] for d in cursor.description]
    trades = []
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        trades.append(d)
        print(f"{d['trade_time']} | {d['trade_type']} | {d['ts_code']} | Price: {d['price']} | Vol: {d['vol']} | Amount: {d['amount']} | Fee: {d['fee']}")
        
    print("\n=== TODAY'S TRADING PLANS ===")
    today = date.today().strftime('%Y-%m-%d')
    cursor.execute("SELECT * FROM trading_plans WHERE date = ?", (today,))
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        d = dict(zip(cols, row))
        print(f"{d['ts_code']} | Action: {d['ai_decision']} | Executed: {d['executed']} | Frozen: {d['frozen_amount']} | FrozenVol: {d['frozen_vol']}")

    conn.close()

if __name__ == "__main__":
    dump_audit_data()
