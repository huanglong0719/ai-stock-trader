"""
数据访问层 (Repository Layer)
统一管理所有数据库操作，避免代码重复
"""
from .stock_repository import StockRepository
from .trading_repository import TradingRepository

__all__ = ['StockRepository', 'TradingRepository']
