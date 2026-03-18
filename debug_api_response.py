
import asyncio
import json
from datetime import datetime
from backend.app.services.market.market_data_service import MarketDataService

async def debug_get_kline():
    service = MarketDataService()
    ts_code = '300508.SZ'
    
    # 模拟前端请求：获取日线，包含指标，前复权
    print(f"--- 模拟请求 {ts_code} ---")
    kline = await service.get_kline(
        ts_code=ts_code,
        freq='D',
        start_date='20250401',
        end_date='20250410',
        include_indicators=True,
        adj='qfq'
    )
    
    for bar in kline:
        if bar['time'] == '2025-04-07':
            print(f"日期: {bar['time']}")
            print(f"收盘价 (复权后): {bar['close']}")
            print(f"MA5: {bar.get('ma5')}")
            print(f"MA10: {bar.get('ma10')}")
            print(f"MA20: {bar.get('ma20')}")
            print(f"原始复权因子: {bar.get('adj_factor')}")
            print(f"指标计算参考因子 (ind_adj_factor): {bar.get('ind_adj_factor')}")

if __name__ == "__main__":
    asyncio.run(debug_get_kline())
