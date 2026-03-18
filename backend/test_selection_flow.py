import asyncio
import os
import sys
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.stock_selector import stock_selector
from app.services.logger import selector_logger

async def test_full_selection_flow():
    print("=== 开始执行选股全流程测试 ===")
    
    # 强制开启调试日志以观察细节
    os.environ["LOG_LEVEL"] = "DEBUG"
    
    try:
        # 执行默认策略选股 (多维综合)
        # 不传 trade_date，自动获取最新交易日
        print("正在调用 select_stocks...")
        results = await stock_selector.select_stocks(strategy="default", top_n=10)
        
        print("\n=== 内存日志记录 ===")
        logs = selector_logger.get_logs()
        for log in logs:
            print(log)
            
        print("\n=== 选股结果摘要 ===")
        print(f"最终入选股票数量: {len(results)}")
        for idx, res in enumerate(results):
            print(f"{idx+1}. {res['name']} ({res['ts_code']}) - 得分: {res['score']} - 行业: {res['industry']}")
            print(f"   理由: {res['reason'][:100]}...")
            
        print("\n=== 逻辑校验 ===")
        if len(results) <= 5:
            print("[OK] 最终输出结果符合 '最多5只' 的要求")
        else:
            print("[FAIL] 最终输出结果超过 5 只")
            
        # 检查日志中是否有 [海选], [技术筛选], [前10截取], [行业加强], [基本面过滤] 标签
        # 注意：日志存储在 selector_logger 内部或文件中，这里通过打印提示
        print("\n请检查控制台日志输出，确认以下标签是否存在且顺序正确:")
        print("1. [海选] 正在获取全市场成交额前200...")
        print("2. [技术筛选] 正在执行“多维综合”策略初选...")
        print("3. [前10截取] 从初选池中选取 Top 10...")
        print("4. [行业加强] 正在对比行业内成交额前10的活跃标的...")
        print("5. [基本面过滤] 正在对 X 只标的执行基本面一票否决校验...")
        print("6. [AI分析] 正在对 X 只入围标的进行 AI 深度分析...")

    except Exception as e:
        print(f"\n[ERROR] 测试过程中出现异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await stock_selector.close()

if __name__ == "__main__":
    asyncio.run(test_full_selection_flow())
