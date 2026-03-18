import sqlite3
from datetime import date

def inspect_stock(ts_code):
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    print(f"=== INSPECTING {ts_code} ===")
    
    print("\n--- TRADING PLANS ---")
    cursor.execute("SELECT * FROM trading_plans WHERE ts_code = ?", (ts_code,))
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(cols, row)))
        
    print("\n--- TRADE RECORDS ---")
    cursor.execute("SELECT * FROM trade_records WHERE ts_code = ? ORDER BY trade_time DESC", (ts_code,))
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(cols, row)))
        
    print("\n--- POSITION ---")
    cursor.execute("SELECT * FROM positions WHERE ts_code = ?", (ts_code,))
    cols = [d[0] for d in cursor.description]
    for row in cursor.fetchall():
        print(dict(zip(cols, row)))

    conn.close()

if __name__ == "__main__":
    inspect_stock("300508.SZ")
