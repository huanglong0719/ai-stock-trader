
import asyncio
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# 将 backend 目录添加到 sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import MarketDataService
from unittest.mock import patch

async def test_kline_sync():
    service = MarketDataService()
    ts_code = '000001.SZ'
    freq = '5min'
    
    print(f"--- Testing K-line Sync for {ts_code} ({freq}) ---")
    
    # 1. 模拟交易时间
    with patch('app.services.market.market_data_service.is_trading_time', return_value=True):
        # 强制设置服务实例的 is_trading_time 结果
        service.is_trading_time = lambda: True
        
        # 获取 K 线 (这会触发缓存逻辑)
        kline_data = await service.get_kline(ts_code=ts_code, freq=freq, is_ui_request=True)
        
        if not kline_data or len(kline_data) == 0:
            print("Error: No K-line data returned")
            return

        last_bar = kline_data[-1]
        print(f"Initial Last Bar: Time={last_bar['time']}, O={last_bar['open']}, H={last_bar['high']}, L={last_bar['low']}, C={last_bar['close']}")

        # 2. 模拟缓存中的 quote
        mock_quote = {
            'ts_code': ts_code,
            'price': 10.95,
            'high': 10.95,
            'low': 10.80,
            'open': 10.85,
            'vol': 1000000,
            'amount': 10000000,
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'pre_close': 10.80
        }
        
        # 写入缓存 (get_realtime_quote 会读取)
        service._quote_cache[ts_code] = (mock_quote, datetime.now().timestamp())
        
        print(f"Simulated New Price in Cache: {mock_quote['price']}")
        
        # 3. 再次获取 K 线 (触发命中缓存并同步逻辑)
        print("Requesting kline with cache hit sync logic...")
        updated_kline = await service.get_kline(ts_code=ts_code, freq=freq, is_ui_request=True)
        updated_last_bar = updated_kline[-1]
        print(f"Updated Last Bar: Time={updated_last_bar['time']}, O={updated_last_bar['open']}, H={updated_last_bar['high']}, L={updated_last_bar['low']}, C={updated_last_bar['close']}")
        
        if updated_last_bar['close'] == 10.95:
            print("SUCCESS: K-line synchronized with cached quote price (via cache-hit sync logic)!")
        else:
            print(f"FAILURE: K-line close ({updated_last_bar['close']}) did not match simulated price (10.95)")
        
        # 4. 测试“新桶起始点 OHLC 相等”逻辑
        print("\n--- Testing New Bucket Start Point OHLC Equality ---")
        # 模拟一个新的桶时间
        # 获取当前桶的时间，然后加 interval
        last_time = datetime.strptime(updated_last_bar['time'], '%Y-%m-%d %H:%M:%S')
        new_bucket_time_dt = last_time + timedelta(minutes=5)
        new_bucket_time_str = new_bucket_time_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        mock_quote['time'] = new_bucket_time_str
        mock_quote['price'] = 11.20
        # 必须确保 quote 里的 high/low/open 在此时也是 11.20 (因为是新桶第一笔)
        # 或者 merge_realtime_to_kline 逻辑能够处理好
        
        # 清理 K 线缓存以强制重新 merge (模拟跨越了缓存周期或新请求)
        cache_key = (ts_code, freq, None, None, 'qfq', True)
        if cache_key in service._kline_cache:
            del service._kline_cache[cache_key]
        
        # 模拟 TDX 返回的数据不包含最新的这个桶
        # 我们需要 mock _get_kline_internal 返回的历史数据，让它只到 last_time
        original_get_internal = service._get_kline_internal
        
        async def mock_get_internal(*args, **kwargs):
            # 获取真实数据
            res = await original_get_internal(*args, **kwargs)
            # 确保 res 中没有最新的这个 bucket_time
            return [r for r in res if r['time'] != new_bucket_time_str]
        
        with patch.object(service, '_get_kline_internal', side_effect=mock_get_internal):
            new_kline = await service.get_kline(ts_code=ts_code, freq=freq, is_ui_request=True)
            new_last_bar = new_kline[-1]
            print(f"New Bucket Bar: Time={new_last_bar['time']}, O={new_last_bar['open']}, H={new_last_bar['high']}, L={new_last_bar['low']}, C={new_last_bar['close']}")
            
            if new_last_bar['time'] == new_bucket_time_str and new_last_bar['open'] == new_last_bar['high'] == new_last_bar['low'] == new_last_bar['close'] == 11.20:
                print("SUCCESS: New bucket start point OHLC are all equal!")
            else:
                print(f"FAILURE: New bucket OHLC at {new_last_bar['time']} (expected {new_bucket_time_str}) are O={new_last_bar['open']}, H={new_last_bar['high']}, L={new_last_bar['low']}, C={new_last_bar['close']}")

if __name__ == "__main__":
    asyncio.run(test_kline_sync())
