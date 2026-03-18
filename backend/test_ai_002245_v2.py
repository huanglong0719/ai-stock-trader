
import asyncio
import os
import sys

# 确保能找到 app 模块
sys.path.append(os.getcwd())

from app.services.ai_service import ai_service
from app.services.data_provider import data_provider

async def test_002245_ai():
    symbol = '002245.SZ'
    print(f"Testing AI analysis for {symbol}...")
    
    try:
        # 获取数据
        klines = await data_provider.get_kline(symbol, freq='D')
        
        if not klines:
            print(f"Error: No K-line data for {symbol}")
            return
            
        print(f"Data fetched: K-lines={len(klines)}")
        
        # 调用 AI 分析
        # 注意：analyze_stock 内部会自动获取 quote 和 basic_info
        result = await ai_service.analyze_stock(symbol, klines)
        
        if result:
            print("AI Analysis successful!")
            print(f"Source: {result.get('source')}")
            print(f"Score: {result.get('score')}")
            print("Full Analysis output to file: test_ai_output.txt")
            with open("test_ai_output.txt", "w", encoding="utf-8") as f:
                f.write(result.get('content', ''))
        else:
            print("AI Analysis returned None or empty result.")
            
    except Exception as e:
        print(f"Error during AI analysis: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_002245_ai())
