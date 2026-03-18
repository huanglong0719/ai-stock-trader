
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
import asyncio
from sqlalchemy import text

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicators.technical_indicators import technical_indicators
from app.db.session import SessionLocal

async def fix_stocks(ts_codes):
    db = SessionLocal()
    try:
        for ts_code in ts_codes:
            print(f"Fixing {ts_code}...")
            # 1. 读取日线数据
            res = db.execute(text(f"SELECT * FROM daily_bars WHERE ts_code = :ts_code ORDER BY trade_date ASC"), {"ts_code": ts_code})
            rows = res.fetchall()
            if not rows:
                print(f"No data for {ts_code}")
                continue
                
            df_daily = pd.DataFrame([dict(row._mapping) for row in rows])
            
            # 2. 准备数据进行计算 (QFQ)
            def prep_for_calc(df):
                res = df.copy()
                res = res.rename(columns={'trade_date': 'time', 'vol': 'volume'})
                latest_adj = float(res['adj_factor'].iloc[-1])
                for col in ['open', 'high', 'low', 'close']:
                    res[col] = res[col] * res['adj_factor'] / latest_adj
                return res.to_dict('records')

            daily_calc_data = prep_for_calc(df_daily)
            df_res = technical_indicators.calculate(daily_calc_data)
            
            # 3. 更新数据库
            latest_adj = float(df_daily['adj_factor'].iloc[-1])
            
            for _, row in df_res.iterrows():
                d_date = row['time']
                # 获取当天的复权因子用于还原
                current_day_adj = float(df_daily[df_daily['trade_date'] == d_date]['adj_factor'].iloc[0])
                unadj_ratio = latest_adj / current_day_adj if current_day_adj != 0 else 1.0
                
                def unadjust(val):
                    if val is None or pd.isna(val): return None
                    return float(val * unadj_ratio)
                
                db.execute(text("""
                    UPDATE stock_indicators 
                    SET macd = :macd, macd_dea = :dea, macd_diff = :diff
                    WHERE ts_code = :ts_code AND trade_date = :t_date
                """), {
                    "macd": unadjust(row.get('macd')),
                    "dea": unadjust(row.get('macd_dea')),
                    "diff": unadjust(row.get('macd_diff')),
                    "ts_code": ts_code,
                    "t_date": d_date
                })
            
            db.commit()
            print(f"Successfully updated records for {ts_code}")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(fix_stocks(['600686.SH', '000009.SZ']))
