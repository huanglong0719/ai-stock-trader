
import asyncio
import os
import sys
from datetime import date
from typing import Any, cast

# 设置 PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.review_service import review_service
from app.services.data_provider import data_provider

async def test_get_review_result():
    print("Testing get_review_result for today...")
    today = date.today()
    
    # 模拟交易时间 (如果当前不是交易时间)
    dp = cast(Any, data_provider)
    original_is_trading_time = dp.is_trading_time
    dp.is_trading_time = lambda: True
    
    try:
        # 获取结果
        result = await review_service.get_review_result(today)
        
        if result:
            print(f"Date: {result['date']}")
            print(f"Up: {result['up']}")
            print(f"Down: {result['down']}")
            print(f"Limit Up: {result['limit_up']}")
            print(f"Limit Down: {result['limit_down']}")
            print(f"Volume: {result['total_volume']}")
            print(f"Temperature: {result['temp']}")
        else:
            print("No review record found for today in DB.")
            print("Creating a dummy record to test...")
            from app.db.session import SessionLocal
            from app.models.stock_models import MarketSentiment
            db = SessionLocal()
            try:
                sentiment = MarketSentiment(
                    date=today,
                    up_count=0,
                    down_count=0,
                    limit_up_count=0,
                    limit_down_count=0,
                    total_volume=0.0,
                    market_temperature=50.0,
                    main_theme="测试",
                    summary="测试摘要"
                )
                db.add(sentiment)
                db.commit()
                
                result = await review_service.get_review_result(today)
                print(f"After dummy record - Up: {result['up']}, Down: {result['down']}")
            finally:
                db.close()
    finally:
        dp.is_trading_time = original_is_trading_time

if __name__ == "__main__":
    asyncio.run(test_get_review_result())
