
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import asyncio

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicators.technical_indicators import technical_indicators
from app.db.session import SessionLocal, engine

async def debug_000009():
    db_path = os.path.join('backend', 'aitrader.db')
    conn = sqlite3.connect(db_path)
    ts_code = '000009.SZ'
    
    # 1. 读取日线数据
    query = f"SELECT * FROM daily_bars WHERE ts_code = '{ts_code}' ORDER BY trade_date ASC"
    df_daily = pd.read_sql(query, conn)
    
    print(f"Read {len(df_daily)} rows for {ts_code}")
    
    def prep_for_calc(df):
        if df.empty: return []
        res = df.copy()
        res = res.rename(columns={'trade_date': 'time', 'vol': 'volume'})
        latest_adj = float(res['adj_factor'].iloc[-1]) if not res.empty else 1.0
        for col in ['open', 'high', 'low', 'close']:
            res[col] = res[col] * res['adj_factor'] / latest_adj
        return res.to_dict('records')

    daily_calc_data = prep_for_calc(df_daily)
    
    # 计算指标
    df_res = technical_indicators.calculate(daily_calc_data)
    
    print("Columns in result:", df_res.columns.tolist())
    print("\nLast 5 rows of MACD columns:")
    print(df_res[['time', 'macd', 'macd_dea', 'macd_diff', 'macd_signal']].tail())
    
    conn.close()

if __name__ == "__main__":
    asyncio.run(debug_000009())
