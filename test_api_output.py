import asyncio
import os
import sys
import json
from datetime import datetime

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.stock_data_service import stock_data_service
from app.services.market.market_data_service import market_data_service

async def test_api_output():
    code = '301282.SZ'
    target_date = '2025-04-17'
    
    print(f"--- Simulating API output for {code} on {target_date} ---")
    
    # 获取 K 线数据 (带指标)
    kline = stock_data_service.get_local_kline(
        ts_code=code,
        start_date='2025-04-01',
        end_date='2025-04-30',
        include_indicators=True
    )
    
    # 查找目标日期的数据
    target_bar = next((b for b in kline if b['time'] == target_date), None)
    
    if target_bar:
        print(json.dumps(target_bar, indent=2))
        
        # 验证均线是否平滑 (检查前后几天的 ma5)
        print("\n--- MA5 continuity check ---")
        for b in kline:
            print(f"Date: {b['time']}, Close: {b['close']:.2f}, MA5: {b['ma5']:.2f}, MA10: {b['ma10']:.2f}")
    else:
        print(f"No data found for {target_date}")

if __name__ == "__main__":
    asyncio.run(test_api_output())
