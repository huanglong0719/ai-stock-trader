import asyncio
import sys
import os
from datetime import datetime

# 添加项目根目录到路径
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)
# 针对 backend 目录的特殊处理
sys.path.append(os.path.join(root_dir, 'backend'))

from app.services.ai_service import AIService
from backend.app.services.logger import logger

ai_service = AIService()

async def run_regression_tests():
    """
    防退步核心链路测试：验证买入、卖出及参数传递的完整性
    """
    logger.info("开始执行防退步回归测试...")
    
    test_results = {
        "buy_signal_v3": False,
        "sell_signal_v3": False,
        "parameter_forwarding": False
    }

    try:
        # 1. 验证 V3 买入信号调用
        logger.info("测试点 1: analyze_realtime_trade_signal_v3")
        buy_res = await ai_service.analyze_realtime_trade_signal_v3(
            symbol="600519.SH",
            strategy="价值投资",
            current_price=1700.0,
            buy_price=1650.0,
            raw_trading_context="[测试] 趋势向上",
            market_status="震荡",
            account_info={"total_assets": 1000000, "available_cash": 500000}
        )
        if buy_res and 'action' in buy_res:
            test_results["buy_signal_v3"] = True
            logger.info("✅ 买入信号链路正常")

        # 2. 验证 V3 卖出信号调用 (包含新增加的盘口信息)
        logger.info("测试点 2: analyze_selling_opportunity")
        sell_res = await ai_service.analyze_selling_opportunity(
            symbol="600519.SH",
            current_price=1750.0,
            avg_price=1700.0,
            pnl_pct=2.94,
            hold_days=5,
            market_status="多头",
            account_info={"total_assets": 1000000, "market_value": 500000},
            handicap_info="卖一 1751, 买一 1749", # 验证新增参数
            vol=100,
            available_vol=100
        )
        if sell_res and 'action' in sell_res:
            test_results["sell_signal_v3"] = True
            logger.info("✅ 卖出信号链路正常")

        # 3. 验证参数转发一致性 (是否有 kwargs 丢失)
        if sell_res.get('order_type') and sell_res.get('price') is not None:
            test_results["parameter_forwarding"] = True
            logger.info("✅ 参数转发与默认值处理正常")

    except Exception as e:
        logger.error(f"❌ 回归测试中发现严重退步: {e}")
        import traceback
        traceback.print_exc()

    # 汇总结果
    final_success = all(test_results.values())
    logger.info("\n" + "="*30)
    logger.info(f"回归测试汇总: {'通过' if final_success else '失败'}")
    for k, v in test_results.items():
        logger.info(f"- {k}: {'PASS' if v else 'FAIL'}")
    logger.info("="*30)
    
    if not final_success:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run_regression_tests())
