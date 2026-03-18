import tushare as ts
import sqlite3
from datetime import datetime
import os

def check_missing_stocks_status():
    token = '46af14e3cedaaefa40f1658929ccf3d2bf05d07aa83e9bb22742e923'
    pro = ts.pro_api(token)
    
    missing_codes = [
        '000608.SZ', '000670.SZ', '002049.SZ', '002554.SZ', '002969.SZ',
        '300169.SZ'
    ]
    
    print(f"Checking {len(missing_codes)} missing stocks status for 20260109...")
    
    # Check if they have trade data for 20260109
    df = pro.daily(ts_code=','.join(missing_codes), trade_date='20260109')
    
    if df is not None and not df.empty:
        print(f"Found {len(df)} stocks with data in Tushare but missing in DB:")
        print(df[['ts_code', 'close', 'vol']])
    else:
        print("No data found in Tushare for these stocks on 20260109. They might be suspended.")

if __name__ == "__main__":
    check_missing_stocks_status()
