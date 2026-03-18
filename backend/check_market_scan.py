import pandas as pd
from app.services.data_provider import data_provider
from datetime import datetime, timedelta

def check_data_volume():
    # Get last trade date
    trade_date = data_provider.get_last_trade_date()
    print(f"Checking data volume for date: {trade_date}")
    
    # 1. Check Daily Basic
    df_basic = data_provider.get_daily_basic(trade_date=trade_date)
    print(f"Daily Basic count: {len(df_basic)}")
    
    # 2. Check MoneyFlow
    df_mf = data_provider.get_moneyflow(trade_date=trade_date)
    print(f"Money Flow count: {len(df_mf)}")
    
    # 3. Check Merge
    if not df_basic.empty and not df_mf.empty:
        df = pd.merge(df_basic, df_mf, on=['ts_code', 'trade_date'], how='inner')
        print(f"Merged count: {len(df)}")
    else:
        print("Merge failed due to empty data")

if __name__ == "__main__":
    check_data_volume()
