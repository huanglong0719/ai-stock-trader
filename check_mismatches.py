
import sqlite3
import os

def check_mismatches():
    db_path = os.path.join(os.getcwd(), "backend", "aitrader.db")
    if not os.path.exists(db_path):
        print(f"DB not found at {db_path}")
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    target_date = '2026-01-09'
    
    print(f"Checking for adj_factor mismatches on {target_date}...")
    
    # Query to find mismatches between daily_bars and stock_indicators
    query = """
    SELECT d.ts_code, d.adj_factor as daily_adj, i.adj_factor as ind_adj
    FROM daily_bars d
    JOIN stock_indicators i ON d.ts_code = i.ts_code AND d.trade_date = i.trade_date
    WHERE d.trade_date = ? AND ABS(d.adj_factor - i.adj_factor) > 1e-6
    """
    
    cursor.execute(query, (target_date,))
    mismatches = cursor.fetchall()
    
    print(f"Found {len(mismatches)} stocks with mismatches.")
    
    if mismatches:
        print("\nTop 10 mismatches:")
        for code, daily_adj, ind_adj in mismatches[:10]:
            print(f"  {code}: DailyBar={daily_adj}, StockIndicator={ind_adj}")
    
    conn.close()

if __name__ == "__main__":
    check_mismatches()
