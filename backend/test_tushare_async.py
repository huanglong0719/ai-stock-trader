
import asyncio
import sys
import os
import time
import pandas as pd

# Add backend directory to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.market.tushare_client import tushare_client

async def test_async_tushare():
    print("--- 开始测试 Tushare 异步机制 ---")
    
    # 1. 测试并发请求 (触发限流逻辑)
    print("\n1. 测试并发请求 (10个请求)...")
    start_time = time.time()
    
    tasks = []
    for i in range(10):
        # 使用不同的接口或参数，避免缓存干扰 (虽然 client 层没缓存，但 data_provider 有)
        # 这里直接测试 client
        tasks.append(tushare_client.async_get_stock_basic())
    
    results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    print(f"并发请求完成，耗时: {end_time - start_time:.2f} 秒")
    print(f"成功获取结果数量: {len([r for r in results if not r.empty])}")

    # 2. 测试单个异步接口
    print("\n2. 测试单个异步接口 (async_get_daily_basic)...")
    df = await tushare_client.async_get_daily_basic(ts_code="000001.SZ")
    if not df.empty:
        print(f"成功获取 000001.SZ 每日指标，行数: {len(df)}")
    else:
        print("获取每日指标失败")

    # 3. 测试通用异步查询
    print("\n3. 测试通用异步查询 (trade_cal)...")
    df_cal = await tushare_client.async_query('trade_cal', params={
        'exchange': 'SSE', 
        'start_date': '20260101', 
        'end_date': '20260128',
        'is_open': '1'
    })
    if not df_cal.empty:
        print(f"成功获取交易日历，行数: {len(df_cal)}")
    else:
        print("获取交易日历失败")
        
    # 4. 关闭资源
    await tushare_client.close()

if __name__ == "__main__":
    asyncio.run(test_async_tushare())
