import asyncio

from app.services.data_provider import data_provider
from app.services.stock_selector import stock_selector


async def main():
    trade_date = await data_provider.get_last_trade_date()
    df_default = await stock_selector._filter_candidates(trade_date)
    df_pullback = await stock_selector._filter_pullback_candidates(trade_date)
    print("default_candidates", 0 if df_default is None else len(df_default))
    print("pullback_candidates", 0 if df_pullback is None else len(df_pullback))


if __name__ == "__main__":
    asyncio.run(main())
