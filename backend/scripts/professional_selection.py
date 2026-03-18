
import sys
import os
from datetime import datetime

# 将 backend 目录添加到 sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.dirname(current_dir)
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

# 设置环境变量 (如果需要)
os.environ['TUSHARE_TOKEN'] = '46af14e3cedaaefa40f1658929ccf3d2bf05d07aa83e9bb22742e923'

from app.services.stock_selector import stock_selector
from app.services.data_provider import data_provider

import asyncio

async def professional_selection():
    print("="*50)
    print("【20年实战经验机构操盘手 - 每日复盘与选股】")
    print(f"日期: {datetime.now().strftime('%Y-%m-%d')}")
    print("="*50)

    # 1. 大盘环境判断
    print("\n--- 1. 大盘环境评估 ---")
    indices = ["000001.SH", "399001.SZ", "399006.SZ"]
    for idx_code in indices:
        quote = await data_provider.get_realtime_quote(idx_code)
        if quote:
            print(f"{quote['name']}: {quote['price']} ({quote['pct_chg']}%)")
        else:
            # 尝试从本地获取
            quote = await data_provider.get_local_quote(idx_code)
            if quote:
                print(f"{quote['name']}: {quote['price']} ({quote['pct_chg']}% - [本地离线数据])")
    
    print("\n操盘手笔记: 只要大盘不出现系统性风险（大跌超过2%且放量），我们就专注于捕捉强势个股的主升浪。")

    # 2. 执行策略选股
    print("\n--- 2. 正在执行主升浪捕捉策略 (多维综合 + 强势回调) ---")
    
    # 减少初选数量以加快速度，毕竟用户只需要最多5只
    results_default = await stock_selector.select_stocks(strategy="default", top_n=5)
    results_pullback = await stock_selector.select_stocks(strategy="pullback", top_n=5)

    all_candidates = results_default + results_pullback
    # 去重
    seen = set()
    unique_candidates = []
    for c in all_candidates:
        if c['ts_code'] not in seen:
            unique_candidates.append(c)
            seen.add(c['ts_code'])
    
    # 按评分排序
    unique_candidates.sort(key=lambda x: x['score'], reverse=True)
    
    final_selection = unique_candidates[:5]

    if not final_selection:
        print("\n今日市场环境复杂，暂未发现符合‘主升浪’确定性机会的个股，建议空仓观望。")
        return

    print(f"\n--- 3. 最终精选目标 (共 {len(final_selection)} 只) ---")
    for i, stock in enumerate(final_selection):
        print(f"\n【精选 {i+1}: {stock['name']} ({stock['ts_code']})】")
        print(f"综合评分: {stock['score']}")
        print(f"所属行业: {stock['industry']}")
        print("-" * 30)
        print(f"{stock['analysis']}")
        
        # 提取或生成买入点位
        price = stock['metrics'].get('realtime_price', 0)
        if price == 0:
            try:
                # 尝试从本地获取，增加重试
                import time
                for _ in range(3):
                    try:
                        local_q = await data_provider.get_local_quote(stock['ts_code'])
                        if local_q:
                            price = local_q['price']
                            break
                    except:
                        await asyncio.sleep(0.5)
            except:
                price = 0
            
        if price > 0:
            print(f"\n[操盘手指令]")
            print(f"- 建议买入点位: {price * 0.99:.2f} - {price:.2f} (分批建仓，控制在总仓位20%以内)")
            print(f"- 止损位: {price * 0.95:.2f} (坚决执行，不抱幻想)")
            print(f"- 目标位: {price * 1.15:.2f} (主升浪起爆点，后续看趋势走势)")
        else:
            print(f"\n[提示] 暂时无法获取该股最新价格，请参考当前市价。")

if __name__ == "__main__":
    asyncio.run(professional_selection())
