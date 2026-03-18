
import sqlite3
import pandas as pd

def find_diff_stocks():
    conn_curr = sqlite3.connect('backend/aitrader.db')
    df1 = pd.read_sql_query("SELECT ts_code, count(*) as cnt1 FROM daily_bars WHERE adj_factor IS NOT NULL AND adj_factor != 1.0 GROUP BY ts_code", conn_curr)
    conn_curr.close()
    
    conn_ext = sqlite3.connect('temp_extract/木偶说/backend/aitrader.db')
    df2 = pd.read_sql_query("SELECT ts_code, count(*) as cnt2 FROM daily_bars WHERE adj_factor IS NOT NULL AND adj_factor != 1.0 GROUP BY ts_code", conn_ext)
    conn_ext.close()
    
    merged = pd.merge(df1, df2, on='ts_code', how='outer').fillna(0)
    diff = merged[merged['cnt1'] != merged['cnt2']]
    
    print("Stocks with different non-1.0 adj_factor counts:")
    print(diff.head(20))
    print(f"Total stocks with differences: {len(diff)}")

if __name__ == "__main__":
    find_diff_stocks()
