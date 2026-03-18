import asyncio
from app.services.data_provider import data_provider

async def test():
    date = await data_provider.get_last_trade_date()
    print(f'最新交易日: {date}')

asyncio.run(test())
