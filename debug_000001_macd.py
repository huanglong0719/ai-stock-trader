
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
import sqlite3

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicators.technical_indicators import technical_indicators

async def debug_000001():
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    ts_code = '000001.SZ'
    
    query = f"SELECT * FROM daily_bars WHERE ts_code = '{ts_code}' ORDER BY trade_date ASC"
    df_daily = pd.read_sql(query, conn)
    
    if df_daily.empty:
        print("No data for 000001.SZ")
        return

    def prep_for_calc(df):
        res = df.copy()
        res = res.rename(columns={'trade_date': 'time', 'vol': 'volume'})
        latest_adj = float(res['adj_factor'].iloc[-1])
        for col in ['open', 'high', 'low', 'close']:
            res[col] = res[col] * res['adj_factor'] / latest_adj
        return res.to_dict('records')

    daily_calc_data = prep_for_calc(df_daily)
    df_res = technical_indicators.calculate(daily_calc_data)
    
    print("Columns in df_res:", df_res.columns.tolist())
    print("Last 5 rows of MACD fields:")
    print(df_res[['time', 'macd', 'macd_dea', 'macd_diff', 'macd_signal']].tail())
    
    conn.close()

if __name__ == "__main__":
    asyncio.run(debug_000001())
