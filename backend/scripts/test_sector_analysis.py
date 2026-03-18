import asyncio
import os
import sys
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.sector_analysis import sector_analysis
from app.services.logger import selector_logger

async def test_sector_analysis():
    test_codes = ['000407.SZ', '000417.SZ']
    
    for code in test_codes:
        start_time = datetime.now()
        print(f"\n开始分析 {code}...")
        try:
            # 模拟分析
            result = await sector_analysis.analyze_sector(code)
            duration = (datetime.now() - start_time).total_seconds()
            print(f"分析 {code} 完成，耗时: {duration:.2f}s")
            print(f"结果预览: {str(result)[:200]}...")
        except Exception as e:
            print(f"分析 {code} 失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_sector_analysis())
