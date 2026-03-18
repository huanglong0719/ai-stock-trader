
import asyncio
import sys
import os

# 将 backend 路径加入 sys.path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.ai_service import ai_service
from app.services.logger import logger

async def test_v3_signal():
    print("Testing analyze_realtime_trade_signal_v3...")
    try:
        result = await ai_service.analyze_realtime_trade_signal_v3(
            symbol="603097.SH",
            strategy="测试策略",
            current_price=28.81,
            buy_price=28.50,
            raw_trading_context="[测试数据] 30日K线...",
            plan_reason="测试原因",
            market_status="大盘震荡",
            search_info="无资讯",
            account_info={"total_assets": 1000000, "available_cash": 500000, "market_value": 500000}
        )
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()

async def test_v3_sell_signal():
    print("\nTesting analyze_selling_opportunity...")
    try:
        result = await ai_service.analyze_selling_opportunity(
            symbol="603097.SH",
            current_price=28.81,
            avg_price=27.50,
            pnl_pct=4.76,
            hold_days=3,
            market_status="大盘震荡",
            account_info={"total_assets": 1000000, "market_value": 500000, "total_pnl_pct": 2.5},
            handicap_info="卖一: 28.82 (100) 买一: 28.81 (50)",
            vol=1000,
            available_vol=1000
        )
        print(f"Sell Decision Result: {result}")
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_v3_signal())
    asyncio.run(test_v3_sell_signal())
