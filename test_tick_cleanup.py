
import asyncio
import sys
import os
from datetime import datetime, timedelta
import json

# 添加 backend 目录到 sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.tdx_data_service import tdx_service
from app.core.redis import redis_client

async def test_cache_cleanup():
    ts_code = '000001.SZ'
    
    # 1. 注入一个旧日期的缓存
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    old_key = f"ticks:{ts_code}:{yesterday}"
    redis_client.set(old_key, json.dumps([{"time": "15:00", "price": 10.0, "vol": 100, "num": 1}]))
    print(f"Injected old cache key: {old_key}")
    
    # 2. 调用 fetch_ticks，触发清理
    print(f"Fetching ticks for {ts_code} to trigger cleanup...")
    df = await asyncio.to_thread(tdx_service.fetch_ticks, ts_code, 10)
    
    # 3. 检查旧缓存是否被删除
    exists = redis_client.exists(old_key)
    if not exists:
        print(f"SUCCESS: Old cache key {old_key} was deleted.")
    else:
        print(f"FAILED: Old cache key {old_key} still exists.")
    
    # 4. 检查新缓存是否创建
    today_str = datetime.now().strftime('%Y-%m-%d')
    new_key = f"ticks:{ts_code}:{today_str}"
    if redis_client.exists(new_key):
        print(f"SUCCESS: New cache key {new_key} was created.")
    else:
        print(f"WARNING: New cache key {new_key} not found (maybe no trade data today?).")

if __name__ == "__main__":
    asyncio.run(test_cache_cleanup())
