import asyncio
import sys
import os

# 将项目根目录添加到 python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.market.fundamental_service import fundamental_service
from app.services.logger import logger

async def test_fundamental():
    ts_code = "600519.SH" # 贵州茅台作为测试标的
    print(f"开始测试标的: {ts_code} 的基本面筛选...")
    
    try:
        context = await fundamental_service.get_fundamental_context(ts_code)
        
        print("\n=== 基本面数据 ===")
        print(f"报告期: {context['fina_indicators'].get('end_date')}")
        print(f"ROE: {context['fina_indicators'].get('roe')}%")
        print(f"毛利率: {context['fina_indicators'].get('grossprofit_margin')}%")
        print(f"资产负债率: {context['fina_indicators'].get('debt_to_assets')}%")
        
        print("\n=== 估值数据 ===")
        print(f"PE: {context['valuation'].get('pe')}")
        print(f"PB: {context['valuation'].get('pb')}")
        print(f"总市值: {context['valuation'].get('total_mv'):.2f} 亿")
        
        print("\n=== 5 步筛选结果 ===")
        scr = context['screening']
        print(f"总分: {scr['total_score']:.1f}")
        print(f"结论: {scr['conclusion']}")
        
        for step in ['step1_safety', 'step2_profitability', 'step4_growth', 'step5_valuation']:
            res = scr[step]
            status = "通过" if res['passed'] else "未通过"
            print(f"- {step} ({status}, 得分: {res['score']}): {', '.join(res['details'])}")
            
    except Exception as e:
        print(f"测试过程中出现错误: {e}")

if __name__ == "__main__":
    asyncio.run(test_fundamental())
