
import asyncio
import sys
import os
from datetime import datetime

# Add the project root to sys.path
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.market.stock_data_service import stock_data_service
from app.services.market.market_data_service import market_data_service

async def check_db_stats():
    # 1. Check last trade date
    last_date = await market_data_service.get_last_trade_date(include_today=False)
    print(f"Last trade date (exclude today): {last_date}")
    
    # 2. Check local stats for that date
    local_stats = stock_data_service.get_market_counts_local(last_date)
    print(f"Local stats for {last_date}: {local_stats}")
    
    # 3. Check Redis cache
    cached = await market_data_service._get_close_counts(last_date)
    print(f"Redis cached stats for {last_date}: {cached}")

if __name__ == "__main__":
    asyncio.run(check_db_stats())
