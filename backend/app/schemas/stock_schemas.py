"""
API 请求/响应模型定义
使用 Pydantic 进行数据验证和文档生成
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any
from datetime import date as date_type, datetime
from enum import Enum


class FreqEnum(str, Enum):
    """K线周期枚举"""
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"


class TradeTypeEnum(str, Enum):
    """交易类型枚举"""
    BUY = "BUY"
    SELL = "SELL"


class OrderTypeEnum(str, Enum):
    """订单类型枚举"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"


# ============ 请求模型 ============

class AnalysisRequest(BaseModel):
    """AI 分析请求"""
    symbol: str = Field(..., description="股票代码，格式: 000001.SZ", examples=["000001.SZ"])
    freq: FreqEnum = Field(FreqEnum.DAILY, description="K线周期")
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """验证股票代码格式"""
        import re
        if not re.match(r'^\d{6}\.(SZ|SH|BJ)$', v):
            raise ValueError('Invalid stock code format')
        return v


class TradePlanRequest(BaseModel):
    """创建交易计划请求"""
    ts_code: str = Field(..., description="股票代码")
    strategy_name: str = Field(..., description="策略名称", examples=["低吸反包"])
    buy_price: float = Field(..., gt=0, description="买入限价")
    stop_loss: float = Field(..., gt=0, description="止损价")
    take_profit: float = Field(..., gt=0, description="止盈价")
    position_pct: float = Field(0.1, ge=0, le=1, description="仓位比例 (0-1)")
    reason: str = Field("", description="选股理由")
    order_type: OrderTypeEnum = Field(OrderTypeEnum.MARKET, description="订单类型")


# ============ 响应模型 ============

class StockInfo(BaseModel):
    """股票基本信息"""
    ts_code: str = Field(..., description="股票代码")
    symbol: str = Field(..., description="股票简称代码")
    name: str = Field(..., description="股票名称")
    industry: Optional[str] = Field(None, description="所属行业")
    area: Optional[str] = Field(None, description="地域")
    list_date: Optional[str] = Field(None, description="上市日期")
    
    class Config:
        from_attributes = True  # Pydantic V2


class QuoteData(BaseModel):
    """实时行情数据"""
    symbol: str = Field(..., description="股票代码")
    price: float = Field(..., description="最新价")
    pct_chg: float = Field(..., description="涨跌幅 (%)")
    open: float = Field(..., description="开盘价")
    high: float = Field(..., description="最高价")
    low: float = Field(..., description="最低价")
    vol: float = Field(..., description="成交量")
    amount: float = Field(..., description="成交额")
    vwap: Optional[float] = Field(None, description="分时均价")
    time: Optional[str] = Field(None, description="更新时间")


class KlineData(BaseModel):
    """K线数据"""
    time: str = Field(..., description="时间")
    open: float = Field(..., description="开盘价")
    high: float = Field(..., description="最高价")
    low: float = Field(..., description="最低价")
    close: float = Field(..., description="收盘价")
    volume: float = Field(..., description="成交量")
    pct_chg: Optional[float] = Field(None, description="涨跌幅 (%)")


class AnalysisResponse(BaseModel):
    """AI 分析响应"""
    symbol: str = Field(..., description="股票代码")
    is_worth_trading: bool = Field(..., description="是否值得交易")
    score: int = Field(..., ge=0, le=100, description="评分 (0-100)")
    analysis: str = Field(..., description="分析报告")
    rejection_reason: Optional[str] = Field(None, description="拒绝理由")
    timestamp: str = Field(..., description="分析时间")
    source: str = Field(..., description="AI 模型来源")


class TradePlanResponse(BaseModel):
    """交易计划响应"""
    id: int = Field(..., description="计划ID")
    date: date_type = Field(..., description="计划日期")
    ts_code: str = Field(..., description="股票代码")
    strategy_name: str = Field(..., description="策略名称")
    buy_price_limit: float = Field(..., description="买入限价")
    executed: bool = Field(..., description="是否已执行")
    entry_price: Optional[float] = Field(None, description="实际成交价")
    pnl_pct: Optional[float] = Field(None, description="盈亏比例 (%)")
    
    class Config:
        from_attributes = True  # Pydantic V2


class AccountInfo(BaseModel):
    """账户信息"""
    total_assets: float = Field(..., description="总资产")
    available_cash: float = Field(..., description="可用资金")
    market_value: float = Field(..., description="持仓市值")
    total_pnl: float = Field(..., description="总盈亏")
    total_pnl_pct: float = Field(..., description="总收益率 (%)")
    
    class Config:
        from_attributes = True  # Pydantic V2


class PositionInfo(BaseModel):
    """持仓信息"""
    ts_code: str = Field(..., description="股票代码")
    name: str = Field(..., description="股票名称")
    vol: int = Field(..., description="持仓数量")
    available_vol: int = Field(..., description="可卖数量")
    avg_price: float = Field(..., description="成本价")
    current_price: float = Field(..., description="最新价")
    market_value: float = Field(..., description="市值")
    float_pnl: float = Field(..., description="浮动盈亏")
    pnl_pct: float = Field(..., description="盈亏比例 (%)")
    
    class Config:
        from_attributes = True  # Pydantic V2


class ApiResponse(BaseModel):
    """通用 API 响应"""
    code: int = Field(0, description="状态码，0表示成功")
    message: str = Field("success", description="响应消息")
    data: Optional[Any] = Field(None, description="响应数据")


class ErrorResponse(BaseModel):
    """错误响应"""
    code: str = Field(..., description="错误代码")
    message: str = Field(..., description="错误消息")
    detail: Optional[str] = Field(None, description="详细信息")
