import asyncio
from app.services.data_provider import data_provider
import json
import time

async def test_kline_indicators():
    symbol = '002245.SZ'
    print(f"Testing indicators for {symbol}...")
    
    # Test Daily
    start_time = time.time()
    kline_d = await data_provider.get_kline(symbol, freq='D', limit=100)
    duration_d = time.time() - start_time
    print(f"Daily K-line fetched in {duration_d:.4f}s. Count: {len(kline_d)}")
    if kline_d:
        latest = kline_d[-1]
        indicators = ['ma5', 'ma10', 'ma20', 'vol_ma10', 'macd', 'macd_dea', 'macd_signal']
        print(f"Latest daily indicators:")
        for ind in indicators:
            print(f"  {ind}: {latest.get(ind)}")
            
    # Test Weekly
    start_time = time.time()
    kline_w = await data_provider.get_kline(symbol, freq='W', limit=50)
    duration_w = time.time() - start_time
    print(f"\nWeekly K-line fetched in {duration_w:.4f}s. Count: {len(kline_w)}")
    if kline_w:
        latest = kline_w[-1]
        print(f"Latest weekly bar time: {latest.get('time')}")
        indicators = ['ma5', 'ma10', 'ma20', 'ma60', 'vol_ma5', 'vol_ma10', 'macd', 'is_bullish']
        print(f"Latest weekly indicators:")
        for ind in indicators:
            print(f"  {ind}: {latest.get(ind)}")
        
        if len(kline_w) > 1:
            prev = kline_w[-2]
            print(f"Previous weekly bar time: {prev.get('time')}")
            print(f"Previous weekly indicators:")
            for ind in indicators:
                print(f"  {ind}: {prev.get(ind)}")

    # Test Monthly
    start_time = time.time()
    kline_m = await data_provider.get_kline(symbol, freq='M', limit=20)
    duration_m = time.time() - start_time
    print(f"\nMonthly K-line fetched in {duration_m:.4f}s. Count: {len(kline_m)}")
    if kline_m:
        latest = kline_m[-1]
        indicators = ['ma5', 'ma10', 'ma20', 'ma60', 'vol_ma5', 'vol_ma10', 'macd', 'is_bullish']
        print(f"Latest monthly indicators:")
        for ind in indicators:
            print(f"  {ind}: {latest.get(ind)}")

if __name__ == "__main__":
    asyncio.run(test_kline_indicators())
