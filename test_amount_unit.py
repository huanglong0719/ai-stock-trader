
import asyncio
import os
import sys

# Add backend to sys.path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.market.market_data_service import market_data_service

async def test():
    try:
        res = await market_data_service.get_turnover_top(['600519.SH'], 1)
        if res:
            print(f'TDX Realtime amount: {res[0].get("turnover_amount")}')
        else:
            print('No result')
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    asyncio.run(test())
