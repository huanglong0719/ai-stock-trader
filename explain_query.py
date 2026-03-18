import sqlite3

def explain_query():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    
    symbol = '002245.SZ'
    
    print(f"=== Explain Query Plan for Weekly K-line of {symbol} ===")
    query = f"""
    EXPLAIN QUERY PLAN
    SELECT *
    FROM weekly_bars
    LEFT OUTER JOIN stock_indicators ON weekly_bars.ts_code = stock_indicators.ts_code AND weekly_bars.trade_date = stock_indicators.trade_date
    WHERE weekly_bars.ts_code = '{symbol}'
    ORDER BY weekly_bars.trade_date DESC
    LIMIT 200
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)
        
    print(f"\n=== Explain Query Plan for Daily K-line of {symbol} ===")
    query = f"""
    EXPLAIN QUERY PLAN
    SELECT *
    FROM daily_bars
    LEFT OUTER JOIN stock_indicators ON daily_bars.ts_code = stock_indicators.ts_code AND daily_bars.trade_date = stock_indicators.trade_date
    WHERE daily_bars.ts_code = '{symbol}'
    ORDER BY daily_bars.trade_date DESC
    LIMIT 200
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(row)
        
    conn.close()

if __name__ == "__main__":
    explain_query()
