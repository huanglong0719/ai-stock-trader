import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.config import settings
import tushare as ts
import pandas as pd

def check_dividend():
    pro = ts.pro_api(settings.TUSHARE_TOKEN)
    ts_code = '001311.SZ'
    print(f"Checking dividend data for {ts_code}...")
    
    try:
        df = pro.dividend(ts_code=ts_code)
        if df is not None and not df.empty:
            print(df[['ts_code', 'end_date', 'ann_date', 'div_proc', 'stk_div', 'stk_bo_rate', 'stk_co_rate', 'cash_div_tax', 'ex_date']])
        else:
            print("No dividend data found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_dividend()
