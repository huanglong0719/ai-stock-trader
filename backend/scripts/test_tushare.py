import sys
import os
import asyncio
from datetime import datetime, timedelta

# Add backend directory to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from app.services.data_provider import data_provider

def test_tushare_connection():
    print("Testing Tushare Connection...")
    
    # Test 1: Get Stock List (limit 5)
    print("\n1. Fetching Stock List (Top 5)...")
    try:
        stocks = asyncio.run(data_provider.get_stock_basic())
        if stocks:
            print(f"Success! Found {len(stocks)} stocks.")
            print(f"Sample: {stocks[0]}")
        else:
            print("Failed to fetch stock list.")
    except Exception as e:
        print(f"Failed to fetch stock list: {e}")

    # Test 2: Get Daily Kline for Ping An Bank (000001.SZ)
    print("\n2. Fetching Daily Kline for 000001.SZ (Last 365 days)...")
    try:
        end_date = asyncio.run(data_provider.get_last_trade_date(include_today=True))
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=365)).strftime("%Y%m%d")
        kline = asyncio.run(data_provider.get_kline("000001.SZ", freq="D", start_date=start_date, end_date=end_date))
        if kline:
            print(f"Success! Found {len(kline)} daily records.")
            print(f"Latest record: {kline[-1]}")
        else:
            print("Failed to fetch daily kline.")
    except Exception as e:
        print(f"Failed to fetch daily kline: {e}")

    # Test 3: Get Realtime Quote
    print("\n3. Fetching Realtime Quote for 000001.SZ...")
    try:
        quote = asyncio.run(data_provider.get_realtime_quote("000001.SZ"))
        if quote:
            print("Success!")
            print(f"Quote: {quote}")
        else:
            print("Failed to fetch realtime quote.")
    except Exception as e:
        print(f"Failed to fetch realtime quote: {e}")

if __name__ == "__main__":
    test_tushare_connection()
