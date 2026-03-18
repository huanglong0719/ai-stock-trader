import sys
import os

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.services.data_provider import data_provider
from datetime import datetime
import asyncio


async def main():
    print(f"Current Time: {datetime.now()}")
    print(f"Is Trading Time: {data_provider.is_trading_time()}")

    try:
        print("\n--------------------------------")
        print("Testing Single Quote (002353.SZ)...")
        quote = await data_provider.get_realtime_quote('002353.SZ')
        print(f"Result: {quote}")
    except Exception as e:
        print(f"Error: {e}")

    try:
        print("\n--------------------------------")
        print("Testing Index Quote (000001.SH)...")
        quote = await data_provider.get_realtime_quote('000001.SH')
        print(f"Result: {quote}")
    except Exception as e:
        print(f"Error: {e}")

    try:
        print("\n--------------------------------")
        print("Testing Batch Quotes...")
        quotes = await data_provider.get_realtime_quotes(['002353.SZ', '600519.SH'])
        print(f"Result Count: {len(quotes)}")
        for k, v in quotes.items():
            print(f"{k}: {v['price']}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
