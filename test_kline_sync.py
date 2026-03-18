
import asyncio
import sys
import os
from datetime import datetime

# 添加 backend 目录到 sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import market_data_service
from app.services.logger import logger

async def test_kline_tick_sync():
    ts_code = '000001.SZ'
    freq = '5min'
    
    print(f"Testing K-line tick sync for {ts_code} ({freq})...")
    
    # 模拟 UI 请求
    print("\n1. Testing with is_ui_request=True:")
    kline_ui = await market_data_service.get_kline(ts_code, freq=freq, limit=5, is_ui_request=True)
    if kline_ui:
        last_bar = kline_ui[-1]
        print(f"Last bar: {last_bar['time']}, O:{last_bar['open']}, H:{last_bar['high']}, L:{last_bar['low']}, C:{last_bar['close']}, Vol:{last_bar.get('volume')}")
    
    # 模拟 AI 请求
    print("\n2. Testing with is_ui_request=False (AI):")
    kline_ai = await market_data_service.get_kline(ts_code, freq=freq, limit=5, is_ui_request=False)
    if kline_ai:
        last_bar = kline_ai[-1]
        print(f"Last bar: {last_bar['time']}, O:{last_bar['open']}, H:{last_bar['high']}, L:{last_bar['low']}, C:{last_bar['close']}, Vol:{last_bar.get('volume')}")

if __name__ == "__main__":
    asyncio.run(test_kline_tick_sync())
