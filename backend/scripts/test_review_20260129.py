import sys
import os
import asyncio
import logging
from datetime import date

# Configure logging to see info messages
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Set all app loggers to INFO
logging.getLogger("app").setLevel(logging.INFO)

# Add backend directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.stock_models import MarketSentiment, TradingPlan
from app.services.review_service import review_service
from app.services.market.tushare_client import tushare_client

async def test_review():
    target_date = date(2026, 1, 29)
    print(f"Testing review for {target_date}...")
    
    db = SessionLocal()
    try:
        # 1. Cleanup old data for today to force re-generation
        db.query(MarketSentiment).filter(MarketSentiment.date == target_date).delete()
        db.query(TradingPlan).filter(TradingPlan.date >= target_date).delete()
        db.commit()
        print("Cleaned up old MarketSentiment and TradingPlan records.")
        
        # 2. Perform review
        # perform_daily_review is async
        result = await review_service.perform_daily_review(target_date)
        
        if result:
            print("\nReview Generated Successfully!")
            print("-" * 50)
            print(f"Date: {result.get('date')}")
            print(f"Temp: {result.get('temp')}")
            print(f"Summary Length: {len(result.get('summary', ''))}")
            print("-" * 50)
            print("Summary Preview:")
            print(result.get('summary', '')[:200] + "...")
            print("-" * 50)
            
            # Check for Target Plans in DB
            plans = db.query(TradingPlan).filter(TradingPlan.date >= target_date).all()
            print(f"Total Plans generated: {len(plans)}")
            for p in plans:
                print(f"Plan: {p.ts_code} | Strategy: {p.strategy_name} | Score: {p.score} | Action: {p.ai_decision}")
        else:
            print("Failed to generate review.")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
        # Ensure aiohttp sessions are closed
        await tushare_client.close()

if __name__ == "__main__":
    asyncio.run(test_review())
