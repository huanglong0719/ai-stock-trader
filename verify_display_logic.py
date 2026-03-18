
import asyncio
import pandas as pd
from datetime import datetime
import sys
import os

# 将 backend 目录添加到 PYTHONPATH
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import MarketDataService

async def verify_stock_kline(ts_code):
    service = MarketDataService()
    print(f"\n--- 验证 {ts_code} ---")
    
    for freq in ['W', 'M']:
        print(f"\n频率: {freq}")
        kline = await service._get_kline_internal(ts_code, freq=freq, limit=10)
        if not kline:
            print(f"错误: 无法获取 {freq} 线数据")
            continue
            
        df = pd.DataFrame(kline)
        # 检查关键字段
        cols = ['time', 'open', 'close', 'high', 'low', 'volume', 'ma5', 'macd']
        available_cols = [c for c in cols if c in df.columns]
        
        print(df[available_cols].tail(5).to_string())
        
        # 检查日期是否重复
        if df['time'].duplicated().any():
            print(f"警告: {freq} 线存在重复日期!")
            print(df[df['time'].duplicated(keep=False)])
        else:
            print(f"{freq} 线日期无重复")

if __name__ == "__main__":
    asyncio.run(verify_stock_kline('600865.SH'))
    asyncio.run(verify_stock_kline('000533.SZ'))
