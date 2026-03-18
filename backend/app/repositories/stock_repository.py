"""
股票数据访问层 (Repository Pattern)
统一管理所有股票相关的数据库操作，避免代码重复
"""
import asyncio
from typing import Optional, List
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc

from app.models.stock_models import (
    Stock, DailyBar, WeeklyBar, MonthlyBar,
    Position, TradingPlan, TradeRecord, Account
)
from app.core.config import settings


class StockRepository:
    """股票基础信息仓储"""
    
    @staticmethod
    async def get_by_code(db: Session, ts_code: str) -> Optional[Stock]:
        """
        根据代码获取股票信息
        
        Args:
            db: 数据库会话
            ts_code: 股票代码（如 000001.SZ）
        
        Returns:
            Stock 对象或 None
        """
        return await asyncio.to_thread(
            lambda: db.query(Stock).filter(Stock.ts_code == ts_code).first()
        )
    
    @staticmethod
    async def get_name(db: Session, ts_code: str) -> str:
        """
        获取股票名称
        
        Args:
            db: 数据库会话
            ts_code: 股票代码
        
        Returns:
            股票名称，如果不存在返回代码本身
        """
        stock = await StockRepository.get_by_code(db, ts_code)
        return (stock.name or ts_code) if stock else ts_code
    
    @staticmethod
    async def get_all(db: Session, limit: int = None) -> List[Stock]:
        """
        获取所有股票列表
        
        Args:
            db: 数据库会话
            limit: 限制返回数量
        
        Returns:
            股票列表
        """
        def _query():
            query = db.query(Stock)
            if limit:
                query = query.limit(limit)
            return query.all()
        
        return await asyncio.to_thread(_query)


class PositionRepository:
    """持仓数据仓储"""
    
    @staticmethod
    async def get_by_code(db: Session, ts_code: str) -> Optional[Position]:
        """根据代码获取持仓"""
        return await asyncio.to_thread(
            lambda: db.query(Position).filter(Position.ts_code == ts_code).first()
        )
    
    @staticmethod
    async def get_all_active(db: Session) -> List[Position]:
        """获取所有有效持仓（持仓量>0）"""
        return await asyncio.to_thread(
            lambda: db.query(Position).filter(Position.vol > 0).all()
        )
    
    @staticmethod
    async def update(db: Session, position: Position):
        """更新持仓"""
        await asyncio.to_thread(db.commit)
        await asyncio.to_thread(db.refresh, position)


class TradingPlanRepository:
    """交易计划仓储"""
    
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
    async def get_by_code_and_date(
        db: Session, 
        ts_code: str, 
        plan_date: date = None
    ) -> Optional[TradingPlan]:
        """根据代码和日期获取计划"""
        target_date = plan_date or date.today()
        return await asyncio.to_thread(
            lambda: db.query(TradingPlan).filter(
                and_(
                    TradingPlan.ts_code == ts_code,
                    TradingPlan.date == target_date,
                    TradingPlan.executed == False
                )
            ).first()
        )


class AccountRepository:
    """账户仓储"""
    
    @staticmethod
    async def get_default(db: Session) -> Optional[Account]:
        """获取默认账户"""
        return await asyncio.to_thread(
            lambda: db.query(Account).first()
        )
    
    @staticmethod
    async def create_default(db: Session, initial_capital: float = None) -> Account:
        """创建默认账户"""
        if initial_capital is None:
            initial_capital = settings.INITIAL_CAPITAL
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
    async def get_or_create(db: Session) -> Account:
        """获取或创建默认账户"""
        account = await AccountRepository.get_default(db)
        if not account:
            account = await AccountRepository.create_default(db)
        return account


class KlineRepository:
    """K线数据仓储"""
    
    @staticmethod
    async def get_daily_bars(
        db: Session,
        ts_code: str,
        start_date: date = None,
        end_date: date = None,
        limit: int = None
    ) -> List[DailyBar]:
        """获取日线数据"""
        def _query():
            query = db.query(DailyBar).filter(DailyBar.ts_code == ts_code)
            
            if start_date:
                query = query.filter(DailyBar.trade_date >= start_date)
            if end_date:
                query = query.filter(DailyBar.trade_date <= end_date)
            
            query = query.order_by(DailyBar.trade_date.asc())
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
        
        return await asyncio.to_thread(_query)
    
    @staticmethod
    async def get_weekly_bars(
        db: Session,
        ts_code: str,
        limit: int = None
    ) -> List[WeeklyBar]:
        """获取周线数据"""
        def _query():
            query = db.query(WeeklyBar).filter(
                WeeklyBar.ts_code == ts_code
            ).order_by(WeeklyBar.trade_date.asc())
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
        
        return await asyncio.to_thread(_query)
    
    @staticmethod
    async def get_monthly_bars(
        db: Session,
        ts_code: str,
        limit: int = None
    ) -> List[MonthlyBar]:
        """获取月线数据"""
        def _query():
            query = db.query(MonthlyBar).filter(
                MonthlyBar.ts_code == ts_code
            ).order_by(MonthlyBar.trade_date.asc())
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
        
        return await asyncio.to_thread(_query)
