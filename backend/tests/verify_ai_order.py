import asyncio
import os
import sys
from datetime import date

# 将项目根目录添加到路径
backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(f"Backend path: {backend_path}")
sys.path.insert(0, backend_path)
print(f"Sys path: {sys.path}")

try:
    from app.db.session import SessionLocal
except ImportError as e:
    print(f"Import Error: {e}")
    raise e
from app.models.stock_models import TradingPlan, Account, Position
from app.services.trading_service import trading_service
from app.services.ai_service import ai_service

async def test_ai_order_logic():
    print("开始验证 AI 挂单决策逻辑...")
    db = SessionLocal()
    try:
        # 1. 清理并准备测试数据
        db.query(TradingPlan).filter(TradingPlan.ts_code == "TEST.SH").delete()
        db.commit()

        # 创建一个测试计划
        plan = TradingPlan(
            date=date.today(),
            ts_code="TEST.SH",
            strategy_name="测试策略",
            buy_price_limit=10.0,
            position_pct=0.2,
            reason="测试理由",
            executed=False
        )
        db.add(plan)
        db.commit()
        db.refresh(plan)

        print(f"创建测试计划: {plan.ts_code}")

        # 2. 模拟 AI 决策返回 LIMIT 且未达限价
        print("\n场景 1: AI 决定 LIMIT 买入，但当前价高于限价")
        # 我们可以通过 mock ai_service 来实现，或者直接在 trading_service 里手动触发
        # 这里我们模拟 trading_service 的部分逻辑
        current_price = 10.5
        limit_price = 10.0
        
        # 模拟 decision
        decision: dict[str, float | str] = {
            "action": "BUY",
            "order_type": "LIMIT",
            "price": limit_price,
            "reason": "看好但要等回调"
        }
        
        # 验证逻辑：不应执行买入
        should_execute = False
        if decision['action'] == 'BUY':
            if decision['order_type'] == 'LIMIT':
                limit_value = float(decision.get("price") or 0.0)
                if current_price <= limit_value:
                    should_execute = True
        
        print(f"结果: should_execute = {should_execute} (预期: False)")

        # 3. 模拟 AI 决策返回 MARKET
        print("\n场景 2: AI 决定 MARKET 买入")
        decision_market: dict[str, float | str] = {
            "action": "BUY",
            "order_type": "MARKET",
            "price": current_price,
            "reason": "急拉，现价上车"
        }
        
        should_execute_market = False
        if decision_market['action'] == 'BUY':
            if decision_market['order_type'] == 'MARKET':
                should_execute_market = True
        
        print(f"结果: should_execute = {should_execute_market} (预期: True)")

        # 4. 检查数据库字段是否已更新 (TradingPlan 是否有新字段)
        print("\n检查模型字段...")
        p = db.query(TradingPlan).first()
        print(f"TradingPlan 包含 order_type 字段: {'order_type' in p.__dict__ or hasattr(p, 'order_type')}")
        print(f"TradingPlan 包含 limit_price 字段: {'limit_price' in p.__dict__ or hasattr(p, 'limit_price')}")

    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(test_ai_order_logic())
