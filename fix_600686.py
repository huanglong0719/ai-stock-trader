
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import os

def rebuild_stock(ts_code):
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    
    print(f"Rebuilding {ts_code}...")
    
    # 1. Get daily data
    query = f"SELECT trade_date, open, high, low, close, vol, amount, adj_factor FROM daily_bars WHERE ts_code='{ts_code}' ORDER BY trade_date ASC"
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print(f"No daily data for {ts_code}")
        return

    df['trade_date_dt'] = pd.to_datetime(df['trade_date'])
    
    # 2. Weekly aggregation
    df['year_week'] = df['trade_date_dt'].apply(lambda x: f"{x.isocalendar()[0]}-{x.isocalendar()[1]:02d}")
    w_grouped = df.groupby('year_week').agg({
        'trade_date': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'vol': 'sum',
        'amount': 'sum',
        'adj_factor': 'last'
    }).reset_index()
    
    # 3. Monthly aggregation
    df['year_month'] = df['trade_date_dt'].dt.strftime('%Y-%m')
    m_grouped = df.groupby('year_month').agg({
        'trade_date': 'last',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'vol': 'sum',
        'amount': 'sum',
        'adj_factor': 'last'
    }).reset_index()
    
    # 4. Save to DB
    cursor = conn.cursor()
    
    # Delete old data
    cursor.execute("DELETE FROM weekly_bars WHERE ts_code=?", (ts_code,))
    cursor.execute("DELETE FROM monthly_bars WHERE ts_code=?", (ts_code,))
    
    # Insert weekly
    for _, row in w_grouped.iterrows():
        cursor.execute("""
            INSERT INTO weekly_bars (ts_code, trade_date, open, high, low, close, vol, amount, adj_factor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts_code, row['trade_date'], row['open'], row['high'], row['low'], row['close'], row['vol'], row['amount'], row['adj_factor']))
    
    # Insert monthly
    for _, row in m_grouped.iterrows():
        cursor.execute("""
            INSERT INTO monthly_bars (ts_code, trade_date, open, high, low, close, vol, amount, adj_factor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts_code, row['trade_date'], row['open'], row['high'], row['low'], row['close'], row['vol'], row['amount'], row['adj_factor']))
    
    conn.commit()
    print(f"Finished rebuilding {ts_code}. Weekly: {len(w_grouped)}, Monthly: {len(m_grouped)}")
    conn.close()

if __name__ == "__main__":
    rebuild_stock('600686.SH')
