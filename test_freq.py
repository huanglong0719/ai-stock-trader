
import sys
import os
from datetime import datetime

# Add the project root to sys.path
sys.path.append(os.getcwd())

from backend.app.services.data_provider import data_provider

async def test_multi_freq():
    ts_code = '000001.SZ'
    print(f"Testing {ts_code} at {datetime.now()}")
    
    for freq in ['D', 'W', 'M']:
        data = await data_provider.get_kline(ts_code, freq=freq)
        if data:
            latest = data[-1]
            print(f"Freq {freq} - Latest Point: {latest['time']}, Close: {latest['close']}")
        else:
            print(f"Freq {freq} - No data")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_multi_freq())
