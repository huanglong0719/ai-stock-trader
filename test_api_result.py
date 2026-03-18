
import asyncio
from datetime import date
from app.services.review_service import review_service
import json

async def test_api_result():
    today = date.today()
    print(f"Fetching review result for {today}...")
    result = review_service.get_review_result(today)
    if result:
        print("SUCCESS: Review result found.")
        # 移除可能包含大量文本的 summary 以便查看结构
        summary = result.pop('summary', '')
        print(f"Result Structure: {json.dumps(result, indent=2, default=str)}")
        print(f"Summary Length: {len(summary)} chars")
        if not result.get('target_plans'):
            print("WARNING: target_plans is EMPTY (this is likely why user thinks it failed)")
    else:
        print("FAILED: No review result found for today.")

if __name__ == "__main__":
    asyncio.run(test_api_result())
