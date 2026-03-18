import sqlite3
import pandas as pd

db_path = 'd:/木偶说/backend/aitrader.db'

def forward_fill_adj_factors():
    print("Starting forward fill for missing adj_factors...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Find all stocks with NULL adj_factors
    cursor.execute("SELECT DISTINCT ts_code FROM daily_bars WHERE adj_factor IS NULL")
    stocks = [r[0] for r in cursor.fetchall()]
    print(f"Found {len(stocks)} stocks with NULL adj_factors.")
    
    updated_total = 0
    for ts_code in stocks:
        # Get all bars for this stock ordered by date
        df = pd.read_sql_query(f"SELECT id, trade_date, adj_factor FROM daily_bars WHERE ts_code = '{ts_code}' ORDER BY trade_date ASC", conn)
        
        # Forward fill
        df['adj_factor'] = df['adj_factor'].ffill()
        
        # Find which ones were NULL and now have a value
        to_update = df[df['adj_factor'].notnull() & (df['id'].isin(pd.read_sql_query(f"SELECT id FROM daily_bars WHERE ts_code = '{ts_code}' AND adj_factor IS NULL", conn)['id']))]
        
        if not to_update.empty:
            for _, row in to_update.iterrows():
                cursor.execute("UPDATE daily_bars SET adj_factor = ? WHERE id = ?", (row['adj_factor'], int(row['id'])))
                updated_total += 1
                
    conn.commit()
    conn.close()
    print(f"Forward fill completed. Updated {updated_total} records.")

if __name__ == "__main__":
    forward_fill_adj_factors()
