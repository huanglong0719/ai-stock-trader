
import asyncio
import logging
from datetime import date
from app.services.review_service import review_service
from app.db.session import SessionLocal
from app.models.stock_models import MarketSentiment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_daily_review():
    target_date = date(2026, 1, 26)
    print(f"Starting review for {target_date}...")
    
    # 模拟执行复盘
    result = await review_service.perform_daily_review(review_date=target_date)
    
    if result:
        print("Review completed successfully!")
        print(f"Main Theme: {result.get('main_theme')}")
        print(f"Ladder Count: {len(result.get('ladder', {}).get('stocks', []))}")
        print(f"Turnover Top Count: {len(result.get('turnover_top', []))}")
        
        # 检查数据库
        db = SessionLocal()
        try:
            sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == target_date).first()
            if sentiment:
                print(f"DB Sentiment Date: {sentiment.date}")
                print(f"Ladder JSON Length: {len(sentiment.ladder_json) if sentiment.ladder_json else 'None'}")
                print(f"Turnover JSON Length: {len(sentiment.turnover_top_json) if sentiment.turnover_top_json else 'None'}")
                print(f"Up: {sentiment.up_count}, Down: {sentiment.down_count}")
                print(f"Limit Up: {sentiment.limit_up_count}, Limit Down: {sentiment.limit_down_count}")
                print(f"Total Volume: {sentiment.total_volume} 亿")
            else:
                print("No sentiment record found in DB after review!")
        finally:
            db.close()
    else:
        print("Review failed!")

if __name__ == "__main__":
    asyncio.run(test_daily_review())
