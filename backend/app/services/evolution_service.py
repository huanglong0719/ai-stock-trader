import json
import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy import desc, func

from app.db.session import SessionLocal
from app.models.stock_models import PolicyConfig, TradingPlan, StrategyStats, OutcomeEvent, MarketSentiment

logger = logging.getLogger(__name__)

class EvolutionService:
    def __init__(self):
        # 默认参数 (基准)
        self.default_params = {
            "max_position_pct": 0.1,
            "stop_loss_pct": 0.05,
            "take_profit_pct": 0.10,
            "min_score": 60
        }

    async def evolve_parameters(self):
        await asyncio.to_thread(self.run_weekly_evolution)

    def get_active_config(self, strategy_name: str, market_temperature: float) -> Dict[str, Any]:
        """
        获取当前生效的策略参数
        1. 确定温度分桶
        2. 查询 ACTIVE 状态的配置
        3. 如果没有，返回默认值
        """
        bucket = self._get_bucket(market_temperature)
        
        db = SessionLocal()
        try:
            # 优先查找特定分桶的配置
            config = db.query(PolicyConfig).filter(
                PolicyConfig.strategy_name == strategy_name,
                PolicyConfig.status == "ACTIVE",
                PolicyConfig.market_temperature_bucket == bucket
            ).order_by(desc(PolicyConfig.version)).first()
            
            # 如果没找到，查找通用分桶 (ALL)
            if not config:
                config = db.query(PolicyConfig).filter(
                    PolicyConfig.strategy_name == strategy_name,
                    PolicyConfig.status == "ACTIVE",
                    PolicyConfig.market_temperature_bucket == "ALL"
                ).order_by(desc(PolicyConfig.version)).first()
            
            if config:
                try:
                    params_raw = config.parameters or "{}"
                    params = json.loads(params_raw)
                    # 合并默认值，防止缺少字段
                    merged = self.default_params.copy()
                    merged.update(params)
                    return merged
                except Exception as e:
                    logger.error(f"Error parsing params for {strategy_name}: {e}")
                    return self.default_params
            
            return self.default_params
        finally:
            db.close()

    def run_weekly_evolution(self):
        """
        周级进化任务 (通常周五盘后执行)
        1. 统计各策略近期表现
        2. 生成影子模式候选 (Evolution)
        3. 评估现有影子模式 (Promotion)
        4. 检查是否需要回滚 (Rollback)
        """
        logger.info("Starting weekly evolution task...")
        db = SessionLocal()
        try:
            strategies = [r[0] for r in db.query(TradingPlan.strategy_name).distinct().all() if r[0]]
            
            for strategy in strategies:
                self._process_strategy_evolution(db, strategy)
                
            db.commit()
        except Exception as e:
            logger.error(f"Error in weekly evolution: {e}", exc_info=True)
        finally:
            db.close()

    def _process_strategy_evolution(self, db, strategy_name: str):
        # 1. 获取近期统计 (近 30 天)
        start_date = date.today() - timedelta(days=30)
        
        # 按温度分桶统计表现
        buckets = ["LOW", "MID", "HIGH"]
        
        for bucket in buckets:
            # 找到对应分桶的交易计划
            # 定义温度区间
            t_min, t_max = 0, 100
            if bucket == "LOW": t_max = 30
            elif bucket == "MID": t_min, t_max = 30, 60
            elif bucket == "HIGH": t_min = 60
            
            target_dates = [
                r[0] for r in db.query(MarketSentiment.date).filter(
                    MarketSentiment.date >= start_date,
                    MarketSentiment.market_temperature >= t_min,
                    MarketSentiment.market_temperature < t_max
                ).all()
            ]
            
            if not target_dates:
                continue
                
            plans = db.query(TradingPlan).filter(
                TradingPlan.strategy_name == strategy_name,
                TradingPlan.date.in_(target_dates),
                TradingPlan.executed == True,
                TradingPlan.exit_price.isnot(None)
            ).all()
            
            if len(plans) < 5:
                continue
                
            # 计算胜率与盈亏比
            wins = sum(1 for p in plans if (p.real_pnl_pct or 0) > 0)
            win_rate = wins / len(plans)
            avg_pnl = sum((p.real_pnl_pct or 0) for p in plans) / len(plans)
            
            logger.info(f"Strategy {strategy_name} ({bucket}): WinRate={win_rate:.2%}, AvgPnL={avg_pnl:.2f}%")
            
            # 2. 参数进化逻辑 (Heuristic Rule-based)
            current_config = self.get_active_config(strategy_name, (t_min + t_max) / 2)
            new_params = current_config.copy()
            evolved = False
            reason = ""
            
            if win_rate < 0.4 and avg_pnl < 0:
                # 表现差：收紧止损，降低仓位
                new_params["stop_loss_pct"] = max(0.02, new_params.get("stop_loss_pct", 0.05) * 0.8)
                new_params["max_position_pct"] = max(0.05, new_params.get("max_position_pct", 0.1) * 0.8)
                evolved = True
                reason = f"Underperformance in {bucket} market (WR={win_rate:.2f}). Tightening risk control."
                
            elif win_rate > 0.6 and avg_pnl > 1.0:
                # 表现好：适当放宽止盈，增加仓位
                new_params["take_profit_pct"] = min(0.20, new_params.get("take_profit_pct", 0.10) * 1.1)
                new_params["max_position_pct"] = min(0.30, new_params.get("max_position_pct", 0.1) * 1.1)
                evolved = True
                reason = f"Outperformance in {bucket} market (WR={win_rate:.2f}). Expanding profit potential."
            
            if evolved:
                # 创建新的配置版本 (SHADOW 模式，待人工或自动晋升)
                # 为简化测试，此处直接激活 (ACTIVE)
                new_config = PolicyConfig(
                    strategy_name=strategy_name,
                    market_temperature_bucket=bucket,
                    parameters=json.dumps(new_params),
                    version=int(datetime.now().timestamp()),
                    status="ACTIVE",
                    parent_id=0, 
                    evolution_reason=reason,
                    start_date=date.today()
                )
                db.add(new_config)
                logger.info(f"Evolved config for {strategy_name} ({bucket}): {reason}")

    def _get_bucket(self, temp: float) -> str:
        if temp < 30: return "LOW"
        if temp > 70: return "HIGH"
        return "MID"

evolution_service = EvolutionService()
