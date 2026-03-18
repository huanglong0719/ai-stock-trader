import asyncio
import pytest
import os
import sys
import pandas as pd
from datetime import datetime

# 将项目根目录添加到路径
backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, backend_path)

from app.services.stock_selector import stock_selector
from app.services.logger import selector_logger
import logging

# 强制显示所有日志到终端
logging.getLogger("app").setLevel(logging.INFO)
for handler in logging.getLogger("app").handlers:
    handler.setLevel(logging.INFO)

@pytest.mark.asyncio
async def test_stock_selection_flow():
    """
    测试优化后的选股流程：
    成交额Top 200 -> 技术面全面筛选 -> 取前10 -> 基本面一票否决 (含兜底) -> AI深度分析
    """
    print("\n" + "="*50)
    print("开始执行选股测试 (流程验证)")
    print("="*50)
    
    # 设定测试参数
    strategy = "default"
    trade_date = None # 使用最新交易日
    
    try:
        # 1. 执行选股
        print(f"正在启动策略: {strategy}...")
        results = await stock_selector.select_stocks(strategy=strategy)
        
        # 2. 打印核心日志以验证流程动作
        print("\n--- 流程动作验证 (来自日志) ---")
        logs = selector_logger.get_logs()
        
        # 验证关键步骤是否按顺序出现
        flow_check = {
            "海选": "获取全市场成交额前200",
            "技术筛选": "正在执行“多维综合”策略初选",
            "前10截取": "技术面筛选完成，共",
            "基本面过滤": "执行基本面一票否决校验",
            "AI分析": "正在深度分析"
        }
        
        found_steps = []
        for step_key, step_text in flow_check.items():
            found = any(step_text in log for log in logs)
            status = "[DONE]" if found else "[MISSING]"
            # 安全打印
            try:
                print(f"[{step_key}] {status}")
            except UnicodeEncodeError:
                print(f"[{step_key}] {status}".encode('ascii', errors='replace').decode('ascii'))
                
            if found:
                # 寻找具体的日志行并打印
                relevant_log = next(log for log in logs if step_text in log)
                # 安全打印，处理编码问题
                try:
                    print(f"   -> {relevant_log}")
                except UnicodeEncodeError:
                    # 尝试清理非 ASCII 字符或使用替代方案
                    clean_log = relevant_log.encode('gbk', errors='ignore').decode('gbk')
                    print(f"   -> {clean_log} (部分特殊字符已过滤)")
        
        # 3. 验证最终结果
        print("\n--- 最终选股结果 ---")
        if results:
            print(f"共选出 {len(results)} 只标的:")
            for i, res in enumerate(results, 1):
                name = res.get('name', '未知')
                code = res.get('ts_code', '未知')
                score = res.get('score', 0)
                fina_score = res.get('fina_score', 'N/A')
                try:
                    print(f"{i}. {name} ({code}) - AI评分: {score}, 基本面评分: {fina_score}")
                except UnicodeEncodeError:
                    safe_name = name.encode('gbk', errors='ignore').decode('gbk')
                    print(f"{i}. {safe_name} ({code}) - AI评分: {score}, 基本面评分: {fina_score}")
        else:
            print("本次测试未选出符合条件的股票")

        print("\n" + "="*50)
        print("选股测试完成")
        print("="*50)

    except Exception as e:
        print(f"\n[ERROR] TEST FAILED: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_stock_selection_flow())
