import sys
import os
from datetime import date

# Add backend directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.stock_models import MarketSentiment, TradingPlan
from app.services.review_service import review_service

async def force_regenerate():
    print("Force regenerating review for 2025-12-31...")
    
    db = SessionLocal()
    try:
        # 1. Cleanup old data
        db.query(MarketSentiment).filter(MarketSentiment.date == date(2025, 12, 31)).delete()
        db.query(TradingPlan).filter(TradingPlan.date == date(2025, 12, 31)).delete()
        db.commit()
        print("Cleaned up old records.")
        
        # 2. Perform review
        result = await review_service.perform_daily_review(date(2025, 12, 31))
        
        if result:
            print("\nReview Generated Successfully!")
            print("-" * 50)
            print(f"Date: {result['date']}")
            print(f"Temp: {result['temp']}")
            print(f"Summary Length: {len(result['summary'])}")
            print("-" * 50)
            print("Summary Preview:")
            print(result['summary'][:200] + "...")
            print("-" * 50)
            
            if result.get('target_plan'):
                print(f"Target Plan: {result['target_plan']['ts_code']} - {result['target_plan']['strategy']}")
        else:
            print("Failed to generate review.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(force_regenerate())
