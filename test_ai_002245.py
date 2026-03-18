
import asyncio
import os
import sys

# 添加后端路径到系统路径
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.ai_service import ai_service
from app.services.data_provider import data_provider

async def test_002245_ai():
    symbol = '002245.SZ'
    print(f"Testing AI analysis for {symbol}...")
    
    try:
        # 获取数据
        klines = data_provider.get_kline(symbol, freq='D', count=250)
        quote = data_provider.get_realtime_quote(symbol)
        basic_info = data_provider.get_stock_basic_info(symbol)
        
        print(f"Data fetched: K-lines={len(klines)}, Quote={bool(quote)}, BasicInfo={bool(basic_info)}")
        
        # 调用 AI 分析
        result = await ai_service.analyze_stock_kline(symbol, klines)
        
        if result:
            print("AI Analysis successful!")
            print(f"Advice: {result.get('advice')}")
            print(f"Conclusion: {result.get('conclusion')[:100]}...")
        else:
            print("AI Analysis returned None or empty result.")
            
    except Exception as e:
        print(f"Error during AI analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_002245_ai())
