"""
统一的验证器和错误处理模块
"""
from typing import Dict, Any, Optional
from datetime import datetime, time as dt_time
from fastapi import HTTPException, status


class ValidationError(Exception):
    """自定义验证异常"""
    def __init__(self, message: str, code: str = "VALIDATION_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class TradingError(Exception):
    """交易相关异常"""
    def __init__(self, message: str, code: str = "TRADING_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)


class TradeValidator:
    """交易前置风控检查器"""
    
    @staticmethod
    def validate_trading_time() -> Dict[str, Any]:
        """检查是否在交易时间"""
        now = datetime.now()
        current_time = now.time()
        weekday = now.weekday()
        
        # 周末不交易
        if weekday >= 5:
            return {"valid": False, "reason": "Weekend - Market closed"}
        
        # 交易时间: 9:15-11:30 (含集合竞价拦截与 9:25 确认), 13:00-15:00
        morning_start = dt_time(9, 15)
        morning_end = dt_time(11, 35) # 稍微多留一点时间处理收盘数据
        afternoon_start = dt_time(13, 0)
        afternoon_end = dt_time(15, 1) # 15:00 收盘，多留 1 分钟处理最后一笔
        
        is_trading = (
            (morning_start <= current_time <= morning_end) or
            (afternoon_start <= current_time <= afternoon_end)
        )
        
        return {
            "valid": is_trading,
            "reason": "" if is_trading else "Not in trading hours"
        }
    
    @staticmethod
    def validate_stock_code(ts_code: str) -> Dict[str, Any]:
        """验证股票代码格式"""
        import re
        pattern = r'^\d{6}\.(SZ|SH|BJ)$'
        valid = bool(re.match(pattern, ts_code))
        return {
            "valid": valid,
            "reason": "" if valid else f"Invalid stock code format: {ts_code}"
        }
    
    @staticmethod
    def validate_price(price: float) -> Dict[str, Any]:
        """验证价格有效性"""
        valid = price > 0 and price < 10000
        return {
            "valid": valid,
            "reason": "" if valid else f"Invalid price: {price}"
        }
    
    @staticmethod
    def validate_volume(volume: int) -> Dict[str, Any]:
        """验证交易数量（必须是100的倍数）"""
        valid = volume > 0 and volume % 100 == 0
        return {
            "valid": valid,
            "reason": "" if valid else f"Volume must be multiple of 100: {volume}"
        }


def handle_exception(exc: Exception) -> HTTPException:
    """统一异常处理"""
    if isinstance(exc, ValidationError):
        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": exc.code, "message": exc.message}
        )
    elif isinstance(exc, TradingError):
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"code": exc.code, "message": exc.message}
        )
    else:
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"code": "INTERNAL_ERROR", "message": str(exc)}
        )
