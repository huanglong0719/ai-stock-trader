import asyncio
import re
import pandas as pd
from typing import List, Dict, Optional, Any, Union
from app.services.market.market_data_service import market_data_service
from app.services.market.stock_data_service import stock_data_service
from app.services.market.market_utils import is_after_market_close, is_trading_time
from app.services.logger import logger

class TushareProvider:
    """
    Facade class for backward compatibility.
    Delegates to specialized services in app/services/market/
    """
    def __init__(self):
        pass

    def _normalize_ts_code(self, ts_code: str) -> str:
        s = str(ts_code or "").strip().upper()
        if not s:
            return s
        if "." in s:
            return s
        if re.fullmatch(r"\d{6}", s):
            if s.startswith("6"):
                return f"{s}.SH"
            if s.startswith(("0", "3")):
                return f"{s}.SZ"
            if s.startswith(("43", "83", "87", "92", "8", "4")):
                return f"{s}.BJ"
        return s

    @property
    def market_data_service(self):
        return market_data_service

    @property
    def quote_buffer(self):
        return market_data_service.quote_buffer

    def is_trading_time(self):
        return is_trading_time()

    def is_after_market_close(self):
        return is_after_market_close()

    async def get_local_quote(self, ts_code: str) -> Optional[Dict[str, Any]]:
        return await asyncio.to_thread(stock_data_service.get_local_quote, ts_code)

    async def merge_realtime_to_kline(self, kline: List[Dict], quote: Dict, freq: str = 'D', ts_code: str = None):
        return await market_data_service.merge_realtime_to_kline(kline, quote, freq, ts_code=ts_code)

    async def query(self, api_name, params=None, fields="", silent=False):
        """[不推荐] 转发查询，现已不再支持 Tushare"""
        logger.info(f"警告: 尝试通过 data_provider 调用 Tushare API {api_name}，操作已拦截。")
        return None

    async def get_last_trade_date(self, include_today=False):
        return await market_data_service.get_last_trade_date(include_today)

    async def check_trade_day(self, date_str: str = None) -> dict:
        return await market_data_service.check_trade_day(date_str)

    async def get_stock_basic(self):
        # 优先由 stock_data_service 处理缓存逻辑
        stocks = await asyncio.to_thread(stock_data_service.get_stock_basic)
        if not stocks:
            logger.info("本地数据库中未发现股票基础信息，请先运行数据同步。")
            return []
        return stocks

    async def get_stock_basic_info(self, ts_code: str):
        # 简单桥接
        return await asyncio.to_thread(market_data_service._is_index, self._normalize_ts_code(ts_code))

    async def get_stock_concepts(self, ts_code: str):
        """
        获取股票概念列表
        """
        return await asyncio.to_thread(stock_data_service.get_stock_concepts, self._normalize_ts_code(ts_code))

    async def get_local_kline(self, *args, **kwargs):
        return await asyncio.to_thread(stock_data_service.get_local_kline, *args, **kwargs)

    async def get_kline(self, ts_code: str, freq: str = 'D', start_date: str = None, end_date: str = None, local_only: bool = False, limit: int = None, include_indicators: bool = True, adj: str = 'qfq', is_ui_request: bool = False, cache_scope: Optional[str] = None):
        return await market_data_service.get_kline(self._normalize_ts_code(ts_code), freq=freq, start_date=start_date, end_date=end_date, local_only=local_only, limit=limit, include_indicators=include_indicators, adj=adj, is_ui_request=is_ui_request, cache_scope=cache_scope)

    async def get_kline_batch(self, ts_codes: list, freq: str = 'D', start_date: str = None, end_date: str = None, local_only: bool = False, limit: int = None, include_indicators: bool = True, cache_scope: Optional[str] = None) -> dict:
        """
        批量获取K线数据 (并发优化)
        """
        # 优化点：如果不是 local_only，先批量获取实时行情，减少 get_kline 内部的碎片化请求
        if not local_only and freq == 'D':
             await market_data_service.get_realtime_quotes([self._normalize_ts_code(c) for c in ts_codes], cache_scope=cache_scope)
             
        tasks = [
            market_data_service.get_kline(self._normalize_ts_code(code), freq=freq, start_date=start_date, end_date=end_date, local_only=local_only, limit=limit, include_indicators=include_indicators, cache_scope=cache_scope)
            for code in ts_codes
        ]
        klines = await asyncio.gather(*tasks)
        return dict(zip([self._normalize_ts_code(c) for c in ts_codes], klines))

    async def get_batch_kline(self, ts_codes: list, freq: str = 'D', start_date: str = None, end_date: str = None, local_only: bool = False, limit: int = None, include_indicators: bool = True, cache_scope: Optional[str] = None) -> dict:
        return await self.get_kline_batch(
            ts_codes,
            freq=freq,
            start_date=start_date,
            end_date=end_date,
            local_only=local_only,
            limit=limit,
            include_indicators=include_indicators,
            cache_scope=cache_scope,
        )

    async def get_minute_data(self, ts_code: str, freq: str = '30min', start_date: str = None, end_date: str = None, limit: int = None) -> pd.DataFrame:
        """
        获取分钟线数据 (返回 DataFrame)
        为了兼容 auto_fix_missing_minute_data 调用
        """
        ts_code = self._normalize_ts_code(ts_code)
        
        # 规范化频率
        if freq in ['1min', '1m', '1']: freq = '1min'
        elif freq in ['5min', '5m', '5']: freq = '5min'
        elif freq in ['15min', '15m', '15']: freq = '15min'
        elif freq in ['30min', '30m', '30']: freq = '30min'
        elif freq in ['60min', '60m', '60']: freq = '60min'
        
        # 调用 get_kline (返回 List[Dict])
        # 注意: get_kline 对 limit=5 的处理可能不够精确，但这里主要用于检查是否存在数据
        kline_list = await self.get_kline(ts_code, freq=freq, start_date=start_date, end_date=end_date, limit=limit, include_indicators=False, local_only=True)
        
        if not kline_list:
            return pd.DataFrame()
            
        df = pd.DataFrame(kline_list)
        # 统一列名 trade_time 以兼容 data_sync 检查逻辑
        if 'time' in df.columns:
            df.rename(columns={'time': 'trade_time'}, inplace=True)
            
        return df

    async def get_realtime_quotes(self, ts_codes: List[str], save_minute_data: bool = False, local_only: bool = False, force_tdx: bool = False, cache_scope: Optional[str] = None) -> Dict[str, Dict]:
        ts_codes_norm = [self._normalize_ts_code(c) for c in (ts_codes or [])]
        # 获取原始行情
        quotes = await market_data_service.get_realtime_quotes(ts_codes_norm, save_minute_data=save_minute_data, local_only=local_only, force_tdx=force_tdx, cache_scope=cache_scope)
        
        # 补充股票名称（如果行情中没有名称或者名称是代码）
        if quotes:
            stocks = await self.get_stock_basic()
            name_map = {s['ts_code']: s['name'] for s in stocks}
            
            for ts_code, quote in quotes.items():
                # 如果名称缺失或名称等于代码，则从基础信息中获取
                if not quote.get('name') or quote.get('name') == ts_code:
                    quote['name'] = name_map.get(ts_code, ts_code)
                    
        return quotes

    async def get_realtime_quote(self, ts_code: str, save_minute_data: bool = False, cache_scope: Optional[str] = None):
        ts_code_norm = self._normalize_ts_code(ts_code)
        res = await self.get_realtime_quotes([ts_code_norm], save_minute_data=save_minute_data, cache_scope=cache_scope)
        return res.get(ts_code_norm)

    async def get_market_overview(self):
        return await market_data_service.get_market_overview()

    async def get_market_snapshot(self, target_date=None):
        return await market_data_service.get_market_snapshot(target_date)

    async def get_sector_context(self, ts_code: str):
        return await market_data_service.get_sector_context(self._normalize_ts_code(ts_code))

    async def get_moneyflow(self, *args, **kwargs):
        return await market_data_service.get_moneyflow(*args, **kwargs)

    async def get_fina_indicator(self, *args, **kwargs):
        return await market_data_service.get_fina_indicator(*args, **kwargs)

    async def buffer_realtime_quotes(self, *args, **kwargs):
        return await asyncio.to_thread(market_data_service.buffer_realtime_quotes, *args, **kwargs)

    async def get_daily_basic(self, *args, **kwargs):
        return await market_data_service.get_daily_basic(*args, **kwargs)

    async def get_turnover_top_codes(self, universe_codes: List[str], top_n: int = 100) -> List[str]:
        return await market_data_service.get_turnover_top_codes(universe_codes=universe_codes, top_n=top_n)

    async def get_market_turnover_top_codes(self, top_n: int = 200) -> List[str]:
        return await market_data_service.get_market_turnover_top_codes(top_n=top_n)

    async def get_ths_turnover_top_codes(self, top_n: int = 100) -> List[str]:
        return await market_data_service.get_ths_turnover_top_codes(top_n=top_n)

    async def get_realtime_speed_top(self, top_n: int = 10) -> List[Dict[str, Any]]:
        return await market_data_service.get_realtime_speed_top(top_n=top_n)

    async def flush_minute_buffer(self):
        return await asyncio.to_thread(market_data_service.flush_minute_buffer)

    async def is_local_data_updated(self):
        return await market_data_service.is_local_data_updated()

    def clear_cache(self):
        # 适配老接口
        pass

    async def purge_inactive_cache(self):
        """释放非活跃股票的内存缓存"""
        return await market_data_service.purge_inactive_cache()

# Global instance
data_provider = TushareProvider()
