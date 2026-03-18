import asyncio
from app.services.market.market_data_service import market_data_service

async def check():
    res = await market_data_service._fetch_market_counts()
    print(f"Market Counts: {res}")
    
    # Check specifically 880005
    res880 = await asyncio.to_thread(market_data_service._fetch_tdx_880_counts_sync)
    print(f"TDX 880005: {res880}")

if __name__ == "__main__":
    asyncio.run(check())
