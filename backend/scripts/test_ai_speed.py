import sys
import os
import time
import asyncio
from datetime import datetime

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.services.ai_service import ai_service
from app.services.chat_service import ChatService

# Initialize ChatService
chat_service = ChatService()

async def test_chat_speed():
    print("-" * 30)
    print("Testing Chat Interaction Speed...")
    user_msg = "我现在持有万科A，亏损5%，市场情绪很差，我该怎么办？"
    
    start_time = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] User: {user_msg}")
    
    try:
        # process_user_message is async
        response = await chat_service.process_user_message(user_msg)
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] AI Response ({duration:.2f}s):")
        print(f"Content: {response[:100]}..." if len(response) > 100 else f"Content: {response}")
    except Exception as e:
        print(f"Chat Error: {e}")

def test_portfolio_analysis_speed():
    print("-" * 30)
    print("Testing Portfolio Analysis Speed...")
    
    # 模拟市场环境：大跌
    market_status = "市场处于极度恐慌状态，上证指数大跌2%，全市场超4000家下跌，跌停家数超过50家。资金大幅流出，没有任何主线板块。"
    
    # 模拟持仓
    positions = [
        {"ts_code": "000002.SZ", "name": "万科A", "vol": 1000, "current_price": 8.5, "pnl_pct": -5.2},
        {"ts_code": "600519.SH", "name": "贵州茅台", "vol": 100, "current_price": 1650.0, "pnl_pct": 12.5},
        {"ts_code": "300750.SZ", "name": "宁德时代", "vol": 500, "current_price": 180.0, "pnl_pct": -1.0}
    ]
    
    account_info = {
        "total_assets": 1000000,
        "available_cash": 200000
    }
    
    start_time = time.time()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] System: Sending portfolio snapshot to AI...")
    
    try:
        # analyze_portfolio_adjustment is sync
        decisions = ai_service.analyze_portfolio_adjustment(market_status, positions, account_info)
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] AI Response received in {duration:.2f}s")
        print("Decisions:")
        if not decisions:
            print(" - No adjustments suggested (HOLD ALL)")
        else:
            for d in decisions:
                print(f" - {d}")
    except Exception as e:
        print(f"Analysis Error: {e}")

async def main():
    print("Starting AI Interaction Test...")
    
    # 1. Test Portfolio Analysis (Sync)
    test_portfolio_analysis_speed()
    
    # 2. Test Chat (Async)
    await test_chat_speed()

if __name__ == "__main__":
    asyncio.run(main())
