
import asyncio
import json
from app.services.market.market_data_service import MarketDataService

async def test_minute():
    mds = MarketDataService()
    symbol = '300508.SZ'
    print(f"Testing 1min K-line for {symbol}...")
    res = await mds.get_kline(symbol, freq='1')
    print(f"1min count: {len(res)}")
    if res:
        print(f"Latest 5 bars:")
        for bar in res[-5:]:
            print(json.dumps(bar, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(test_minute())
