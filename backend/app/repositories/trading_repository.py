"""
交易数据访问层
管理交易计划、持仓、账户等交易相关的数据库操作
"""
import asyncio
from typing import Optional, List
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc

from app.models.stock_models import (
    TradingPlan, Position, Account, TradeRecord
)


class TradingRepository:
    """交易数据仓储"""

    @staticmethod
    def _infer_plan_action(plan: TradingPlan) -> str:
        strategy_name = (plan.strategy_name or "").strip()
        decision = (getattr(plan, "ai_decision", "") or "").strip().upper()
        if decision in ["CANCEL", "WAIT", "HOLD"]:
            return "HOLD"
        if decision in ["SELL", "REDUCE"]:
            return "SELL"
        if decision == "BUY":
            return "BUY"
        if any(k in strategy_name for k in ["卖出", "减仓", "减持", "清仓", "止盈", "止损", "抛售"]):
            return "SELL"
        if any(k in strategy_name for k in ["持有", "观望", "待定"]):
            return "HOLD"
        return "BUY"
    
    # 交易计划相关
    @staticmethod
    async def get_today_plans(db: Session) -> List[TradingPlan]:
        """获取今日所有计划"""
        return await asyncio.to_thread(
            lambda: db.query(TradingPlan).filter(
                TradingPlan.date == date.today()
            ).all()
        )
    
    @staticmethod
    async def get_unexecuted_plans(db: Session, plan_date: date = None) -> List[TradingPlan]:
        """获取未执行的计划"""
        target_date = plan_date or date.today()
        return await asyncio.to_thread(
            lambda: db.query(TradingPlan).filter(
                and_(
                    TradingPlan.date == target_date,
                    TradingPlan.executed == False
                )
            ).all()
        )
    
    @staticmethod
    async def get_plan_by_code(
        db: Session, 
        ts_code: str, 
        plan_date: date = None,
        action: str = None,
    ) -> Optional[TradingPlan]:
        """根据代码和日期获取计划"""
        target_date = plan_date or date.today()
        action_norm = (action or "").strip().upper() if action else None

        plans = await asyncio.to_thread(
            lambda: db.query(TradingPlan).filter(
                and_(
                    TradingPlan.ts_code == ts_code,
                    TradingPlan.date == target_date,
                    TradingPlan.executed == False,
                )
            ).order_by(desc(TradingPlan.created_at), desc(TradingPlan.id)).all()
        )
        if not plans:
            return None
        if not action_norm:
            return plans[0]

        for p in plans:
            if TradingRepository._infer_plan_action(p) == action_norm:
                return p
        return None
    
    # 持仓相关
    @staticmethod
    async def get_position(db: Session, ts_code: str) -> Optional[Position]:
        """根据代码获取持仓"""
        return await asyncio.to_thread(
            lambda: db.query(Position).filter(Position.ts_code == ts_code).first()
        )
    
    @staticmethod
    async def get_all_positions(db: Session, active_only: bool = True) -> List[Position]:
        """获取所有持仓"""
        def _query():
            query = db.query(Position)
            if active_only:
                query = query.filter(Position.vol > 0)
            return query.all()
        
        return await asyncio.to_thread(_query)
    
    # 账户相关
    @staticmethod
    async def get_account(db: Session) -> Optional[Account]:
        """获取默认账户"""
        return await asyncio.to_thread(
            lambda: db.query(Account).first()
        )
    
    @staticmethod
    async def create_account(db: Session, initial_capital: float = 1000000.0) -> Account:
        """创建默认账户"""
        account = Account()
        account.total_assets = initial_capital
        account.available_cash = initial_capital
        account.frozen_cash = 0.0
        account.market_value = 0.0
        account.total_pnl = 0.0
        await asyncio.to_thread(db.add, account)
        await asyncio.to_thread(db.commit)
        await asyncio.to_thread(db.refresh, account)
        return account
    
    @staticmethod
    async def get_or_create_account(db: Session) -> Account:
        """获取或创建默认账户"""
        account = await TradingRepository.get_account(db)
        if not account:
            account = await TradingRepository.create_account(db)
        return account
    
    # 交易记录相关
    @staticmethod
    async def get_trade_records(
        db: Session,
        ts_code: str = None,
        start_date: date = None,
        end_date: date = None,
        limit: int = None
    ) -> List[TradeRecord]:
        """获取交易记录"""
        def _query():
            query = db.query(TradeRecord)
            
            if ts_code:
                query = query.filter(TradeRecord.ts_code == ts_code)
            if start_date:
                query = query.filter(TradeRecord.trade_time >= start_date)
            if end_date:
                query = query.filter(TradeRecord.trade_time <= end_date)
            
            query = query.order_by(TradeRecord.trade_time.desc())
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
        
        return await asyncio.to_thread(_query)
