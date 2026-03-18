import sys
import os
import asyncio
from datetime import datetime, date
import time

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.services.review_service import review_service
from app.db.session import SessionLocal
from app.models.stock_models import MarketSentiment
from app.services.logger import logger

async def diagnose_noon_review():
    print("=" * 50)
    print("Diagnosing Noon Review Issue")
    print("=" * 50)
    
    # 1. Check Time & Timezone
    now = datetime.now()
    print(f"Current System Time: {now}")
    print(f"Timezone Info: {time.tzname}")
    
    # 2. Check Database for today's sentiment
    db = SessionLocal()
    today = date.today()
    sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == today).first()
    if sentiment:
        print(f"Found existing sentiment for {today}:")
        print(f"  - Summary: {sentiment.summary[:50]}...")
        print(f"  - Updated At: {sentiment.updated_at}")
    else:
        print(f"No market sentiment record found for {today}.")
    db.close()
    
    # 3. Simulate Execution
    print("-" * 30)
    print("Simulating perform_noon_review()...")
    try:
        # 直接 await 异步方法
        result = await review_service.perform_noon_review()
        
        if result:
            print("Execution Successful!")
            print(f"  - Summary: {result.get('summary')}")
            print(f"  - Target Plan: {result.get('target_plan')}")
        else:
            print("Execution returned None (Failed silently?)")
            
    except Exception as e:
        print(f"Execution Failed with Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(diagnose_noon_review())
