import asyncio

from app.services.market.market_data_service import market_data_service


async def main():
    items = await market_data_service.get_market_turnover_top(top_n=5)
    print(items)
    with open("backend/_tmp_turnover_check_last.txt", "w", encoding="utf-8") as f:
        f.write(str(items))


if __name__ == "__main__":
    asyncio.run(main())
