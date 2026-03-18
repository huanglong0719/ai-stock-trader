import os
import json
import math
import asyncio
import httpx
import pandas as pd
from datetime import datetime, timedelta, date, time
from typing import List, Dict, Optional, Any
from contextvars import ContextVar
from contextlib import contextmanager
from sqlalchemy import desc, func, case, or_

from app.core.config import settings
from app.services.logger import logger
from app.services.market.market_utils import is_after_market_close, is_trading_time, normalize_date, get_limit_prices
from app.services.market.stock_data_service import stock_data_service
from app.services.tdx_vipdoc_service import TdxVipdocService
from app.models.stock_models import MinuteBar, DailyBar, WeeklyBar, MonthlyBar, StockIndicator, DailyBasic, Stock
from app.db.session import SessionLocal

market_cache_scope = ContextVar("market_cache_scope", default="global")

class MarketDataService:
    INDEX_NAMES = {
        '000001.SH': '上证指数',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '000300.SH': '沪深300',
        '000016.SH': '上证50',
        '000905.SH': '中证500',
        '000852.SH': '中证1000',
        '399005.SZ': '中小100',
        '880001.SH': '上证指数',
        '880005.SH': '上证指数',
    }

    def __init__(self):
        self._quote_cache = {}  # {ts_code: (quote, timestamp)}
        self._scoped_quote_cache = {}
        self._quote_cache_duration = 3  # Realtime quote cache (seconds)
        
        self._kline_cache = {} # {(ts_code, freq, start, end): (kline, timestamp)}
        self._scoped_kline_cache = {}
        self._kline_cache_duration = 15 # Kline cache (seconds)

        self._adj_factor_cache = {}  # {ts_code: (factor, timestamp)}
        self._adj_cache_duration = 300 

        self._market_overview_cache = None
        self._last_overview_time = 0.0
        self._market_stats_cache = None
        self._market_stats_cache_time = 0.0
        self._market_stats_cache_ttl = 45.0
        
        self._limit_up_codes_cache: List[str] = []
        self._last_limit_up_time = 0.0
        
        self._active_codes_cache = set()
        self._last_active_codes_update = 0.0
        
        self._quote_fetch_lock: asyncio.Lock | None = None
        
        # Fallback/Fuse for TDX
        self._tdx_quote_fail_until_ts = 0.0

    # --- Cache Scope Helpers ---
    def _resolve_cache_scope(self, cache_scope: Optional[str]) -> str:
        scope = cache_scope or market_cache_scope.get()
        return scope or "global"

    def _get_quote_cache_bucket(self, cache_scope: Optional[str]):
        scope = self._resolve_cache_scope(cache_scope)
        if scope == "global":
            return self._quote_cache
        return self._scoped_quote_cache.setdefault(scope, {})

    def _get_kline_cache_bucket(self, cache_scope: Optional[str]):
        scope = self._resolve_cache_scope(cache_scope)
        if scope == "global":
            return self._kline_cache
        return self._scoped_kline_cache.setdefault(scope, {})

    def clear_stock_cache(self, ts_code: str):
        if not ts_code:
            return
        for cache in [self._quote_cache, *self._scoped_quote_cache.values()]:
            if ts_code in cache:
                cache.pop(ts_code, None)
        for cache in [self._kline_cache, *self._scoped_kline_cache.values()]:
            for key in [k for k in cache.keys() if k and k[0] == ts_code]:
                cache.pop(key, None)

    @property
    def quote_buffer(self):
        return {}

    @contextmanager
    def cache_scope(self, scope: str):
        token = market_cache_scope.set(scope or "global")
        try:
            yield
        finally:
            market_cache_scope.reset(token)

    # --- Core: Realtime Quotes ---

    async def get_realtime_quote(self, ts_code: str, cache_scope: Optional[str] = None) -> Optional[Dict]:
        res = await self.get_realtime_quotes([ts_code], cache_scope=cache_scope)
        return res.get(ts_code)

    async def get_realtime_quotes(self, ts_codes: List[str], save_minute_data: bool = False, local_only: bool = False, force_tdx: bool = False, cache_scope: Optional[str] = None) -> Dict[str, Dict]:
        """
        Get realtime quotes for multiple stocks.
        Priority: Cache -> TDX Network -> Sina Network (Fallback) -> Local DB (Last Resort)
        """
        if not ts_codes:
            return {}
            
        now = datetime.now().timestamp()
        results = {}
        to_fetch = []
        scope = self._resolve_cache_scope(cache_scope)
        quote_cache = self._get_quote_cache_bucket(cache_scope)
        is_trading = is_trading_time()
        
        # 1. Check Cache
        for code in ts_codes:
            if code in quote_cache:
                data, ts = quote_cache[code]
                if is_trading:
                    if scope == "trading":
                        valid_duration = 8.0
                    elif scope in ("global", "realtime"):
                        valid_duration = 6.0
                    else:
                        valid_duration = max(self._quote_cache_duration, 6.0)
                else:
                    valid_duration = 60
                if now - ts < valid_duration:
                    results[code] = data
                    continue
            if not local_only:
                to_fetch.append(code)

        if not to_fetch:
            return results

        # 2. Network Fetch (TDX priority)
        if not local_only:
            if self._quote_fetch_lock is None:
                self._quote_fetch_lock = asyncio.Lock()
            fetched_quotes = {}
            try:
                async with self._quote_fetch_lock:
                    if now < self._tdx_quote_fail_until_ts and not force_tdx:
                        fetched_quotes = await asyncio.wait_for(self._fetch_sina_quotes(to_fetch), timeout=5.0)
                    else:
                        try:
                            tdx_timeout = 5.0 if force_tdx else 3.0
                            fetched_quotes = await self._fetch_tdx_quotes(to_fetch, timeout=tdx_timeout)
                            self._tdx_quote_fail_until_ts = 0
                        except Exception as e:
                            logger.warning(f"TDX quote fetch failed: {e}, falling back to Sina.")
                            self._tdx_quote_fail_until_ts = now + 6.0
                            fetched_quotes = await asyncio.wait_for(self._fetch_sina_quotes(to_fetch), timeout=5.0)
                            if not fetched_quotes and not force_tdx:
                                try:
                                    fetched_quotes = await self._fetch_tdx_quotes(to_fetch, timeout=5.0)
                                except Exception:
                                    fetched_quotes = {}
            except Exception as e:
                logger.error(f"All quote fetch methods failed: {e}")
            
            # Process and Cache
            if fetched_quotes:
                for code, q in fetched_quotes.items():
                    results[code] = q
                    if q.get('price', 0) > 0:
                        quote_cache[code] = (q, now)

        # 3. Local DB Fallback (Last Resort)
        invalid = [c for c in ts_codes if (results.get(c) or {}).get('price', 0) <= 0]
        if invalid and not local_only:
            try:
                sina_quotes = await asyncio.wait_for(self._fetch_sina_quotes(invalid), timeout=5.0)
                for code, q in (sina_quotes or {}).items():
                    if q.get('price', 0) > 0:
                        results[code] = q
                        quote_cache[code] = (q, now)
            except Exception as e:
                logger.warning(f"Sina quote fetch for invalid TDX quotes failed: {e}")

        missing = [c for c in ts_codes if c not in results or (results.get(c) or {}).get('price', 0) <= 0]
        if missing:
            for code in missing:
                local = await asyncio.to_thread(stock_data_service.get_local_quote, code)
                if local and local.get('price', 0) > 0:
                    results[code] = local

        await self._fill_turnover_rates(results)
        return results

    async def _fetch_tdx_quotes(self, codes: List[str], timeout: float = 3.0) -> Dict[str, Dict]:
        from app.services.tdx_data_service import tdx_service
        # Use a reasonable batch size
        batch_size = 80
        all_quotes = {}
        
        for i in range(0, len(codes), batch_size):
            batch = codes[i:i+batch_size]
            try:
                raw_quotes = await asyncio.wait_for(
                    asyncio.to_thread(tdx_service.fetch_realtime_quotes, batch),
                    timeout=timeout
                )
                if raw_quotes:
                    for q in raw_quotes:
                        # Normalize TDX format to Standard format
                        ts_code = self._normalize_tdx_code(q)
                        if ts_code:
                            std_quote = self._format_quote(q, ts_code, source="tdx")
                            all_quotes[ts_code] = std_quote
            except asyncio.TimeoutError:
                raise RuntimeError("TDX quote fetch timeout")
            except Exception as e:
                raise e # Let caller handle fallback
                
        return all_quotes

    async def _fetch_sina_quotes(self, codes: List[str]) -> Dict[str, Dict]:
        from app.services.market.sina_data_service import SinaDataService
        sina = SinaDataService()
        return await sina.fetch_quotes(codes)

    def _normalize_tdx_code(self, q: Dict) -> Optional[str]:
        code = str(q.get('code', ''))
        market = q.get('market')
        if not code: return None
        if market == 1: return f"{code}.SH"
        if market == 0: return f"{code}.SZ" # TDX SZ=0
        if market == 2: return f"{code}.BJ" # TDX BJ=2 (sometimes)
        # Fallback heuristic
        if code.startswith('6'): return f"{code}.SH"
        if code.startswith(('0','3')): return f"{code}.SZ"
        if code.startswith(('8','4')): return f"{code}.BJ"
        return None

    def _format_quote(self, q: Dict, ts_code: str, source: str) -> Dict:
        """Standardize quote structure"""
        try:
            price = float(q.get('price', 0))
            pre_close = float(q.get('last_close') or q.get('pre_close') or 0)
            open_p = float(q.get('open', 0))
            high = float(q.get('high', 0))
            low = float(q.get('low', 0))
            vol = float(q.get('vol', 0))
            amount = float(q.get('amount', 0))
            
            pct_chg = 0.0
            if pre_close > 0:
                pct_chg = round((price - pre_close) / pre_close * 100, 2)
            
            return {
                "ts_code": ts_code,
                "name": q.get('name', ''),
                "price": price,
                "pre_close": pre_close,
                "open": open_p,
                "high": high,
                "low": low,
                "vol": vol,
                "volume": vol, # Alias
                "amount": amount,
                "pct_chg": pct_chg,
                "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "source": source
            }
        except Exception:
            return {}

    async def _fill_turnover_rates(self, quotes: Dict[str, Dict]):
        if not quotes:
            return
        float_shares = await asyncio.to_thread(stock_data_service.get_all_float_shares)
        for ts_code, quote in quotes.items():
            if not quote:
                continue
            if quote.get("turnover_rate") is not None:
                continue
            if self._is_index(ts_code):
                continue
            vol = quote.get("vol") or quote.get("volume")
            try:
                vol_val = float(vol or 0.0)
            except Exception:
                vol_val = 0.0
            if vol_val <= 0:
                continue
            fs = float_shares.get(ts_code)
            if not fs:
                fs = await asyncio.to_thread(stock_data_service.get_float_share, ts_code)
            try:
                fs_val = float(fs or 0.0)
            except Exception:
                fs_val = 0.0
            if fs_val <= 0:
                continue
            quote["turnover_rate"] = round(vol_val / fs_val, 2)

    # --- Core: K-Line Data ---

    async def get_kline(self, ts_code: str, freq: str = 'D', start_date: str = None, end_date: str = None, 
                        limit: int = None, adj: str = 'qfq', include_indicators: bool = True, 
                        local_only: bool = False, is_ui_request: bool = False, cache_scope: Optional[str] = None) -> List[Dict]:
        """
        Get K-Line Data.
        Flow: 
        1. Check Cache
        2. Load from DB (History)
        3. If DB insufficient/outdated -> Fetch Network (History) -> Save to DB
        4. Fetch Realtime Quote -> Merge as "Today/Now" Bar
        5. Apply QFQ (Adjust Prices)
        6. Calculate Indicators (if requested)
        """
        # 0. Validate Params
        if not ts_code: return []
        
        # Normalize Frequency: 'D', 'W', 'M', '5min', '30min'
        freq = freq.upper()
        if freq in ['5', '5M', '5MIN']: freq = '5min'
        elif freq in ['30', '30M', '30MIN']: freq = '30min'
        elif freq in ['D', 'DAY']: freq = 'D'
        elif freq in ['W', 'WEEK']: freq = 'W'
        elif freq in ['M', 'MONTH']: freq = 'M'

        if freq not in ['D', 'W', 'M', '5min', '30min']:
            logger.warning(f"Unsupported frequency: {freq}")
            return [] # Unsupported freq
            
        # 1. Check Cache
        cache_key = (ts_code, freq, start_date, end_date, limit, adj, include_indicators)
        kline_cache = self._get_kline_cache_bucket(cache_scope)
        now_ts = datetime.now().timestamp()
        
        if cache_key in kline_cache:
            data, ts = kline_cache[cache_key]
            if now_ts - ts < self._kline_cache_duration:
                if is_ui_request and adj == 'qfq' and freq in ['D', 'W', 'M'] and self._is_adj_factor_suspicious(data):
                    pass
                else:
                    if is_ui_request and is_trading_time() and not local_only:
                        return await self._sync_latest_quote_to_kline(data, ts_code, freq)
                    return data

        # 2. Load History (DB + Network Fallback)
        kline_data = await self._load_history_kline(ts_code, freq, start_date, end_date, limit, local_only)
        
        if adj == 'qfq' and freq in ['D', 'W', 'M'] and self._is_adj_factor_suspicious(kline_data) and not local_only:
            target_limit = max(800, int(limit or 0), len(kline_data) or 0)
            repaired = await self._fetch_network_kline(ts_code, freq, start_date, end_date, limit=target_limit)
            if repaired:
                await self._save_kline_to_db(ts_code, freq, repaired)
                kline_data = repaired

        # 3. Merge Realtime (The "Today" or "Now" Bar)
        # Only merge if not strictly local_only and end_date covers today
        today_str = datetime.now().strftime('%Y%m%d')
        if not local_only and (not end_date or end_date >= today_str):
             kline_data = await self._merge_realtime_bar(kline_data, ts_code, freq)

        # 4. Apply QFQ (Forward Adjustment)
        if adj == 'qfq':
            kline_data = await self._apply_qfq(kline_data, ts_code)

        # 5. Indicators
        if include_indicators:
            kline_data = self._calculate_indicators(kline_data, freq)

        # 6. Cache
        kline_cache[cache_key] = (kline_data, now_ts)
        
        return kline_data

    async def _load_history_kline(self, ts_code: str, freq: str, start_date: str = None, end_date: str = None, 
                                  limit: int = None, local_only: bool = False) -> List[Dict]:
        """Load history from DB. If empty/stale, fetch from Network and save."""
        
        # Defaults
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y%m%d') # Default 5 years
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')

        if freq == 'D' and not local_only:
            local_data = await self._fetch_tdx_local_kline(ts_code, freq, start_date, end_date, limit)
            if local_data:
                latest_trade_date = await self.get_last_trade_date(include_today=False)
                last_bar_time = str(local_data[-1]['time'])
                last_bar_date = last_bar_time.split(' ')[0].replace('-', '')
                if last_bar_date >= latest_trade_date:
                    await self._save_kline_to_db(ts_code, freq, local_data)
                    return local_data
            fetched_data = await self._fetch_network_kline(ts_code, freq, start_date, end_date, limit)
            if fetched_data:
                await self._save_kline_to_db(ts_code, freq, fetched_data)
                return fetched_data

        if freq in ['5min', '30min'] and not local_only:
            local_data = await self._fetch_tdx_local_kline(ts_code, freq, start_date, end_date, limit)
            if local_data:
                latest_trade_date = await self.get_last_trade_date(include_today=False)
                last_bar_time = str(local_data[-1]['time'])
                last_bar_date = last_bar_time.split(' ')[0].replace('-', '')
                if last_bar_date >= latest_trade_date:
                    await self._save_kline_to_db(ts_code, freq, local_data)
                    return local_data
            fetched_data = await self._fetch_network_kline(ts_code, freq, start_date, end_date, limit)
            if fetched_data:
                await self._save_kline_to_db(ts_code, freq, fetched_data)
                return fetched_data

        if freq in ['W', 'M'] and not local_only:
            daily_local = await self._fetch_tdx_local_kline(ts_code, 'D', start_date, end_date, limit)
            if daily_local:
                aggregated = self._aggregate_kline_from_daily(daily_local, freq)
                if aggregated:
                    await self._save_kline_to_db(ts_code, freq, aggregated)
                    return aggregated
            
        # A. Try DB
        db_data = await self._query_db_kline(ts_code, freq, start_date, end_date, limit)
        
        # B. Check Sufficiency
        is_sufficient = True
        if not db_data:
            is_sufficient = False
        else:
            # Check if data is up-to-date (ignoring today)
            last_bar_time = str(db_data[-1]['time'])
            last_bar_date = last_bar_time.split(' ')[0].replace('-', '')
            
            latest_trade_date = await self.get_last_trade_date(include_today=False)
            if last_bar_date < latest_trade_date:
                is_sufficient = False
                
        # C. Network Fetch if insufficient
        if not is_sufficient and not local_only:
            fetched_data = await self._fetch_network_kline(ts_code, freq, start_date, end_date, limit)
            if fetched_data:
                # Save to DB
                await self._save_kline_to_db(ts_code, freq, fetched_data)
                # Use fetched data (it's fresher)
                return fetched_data
                
        return db_data

    async def _query_db_kline(self, ts_code: str, freq: str, start: str, end: str, limit: int = None) -> List[Dict]:
        """Query DB for K-Line data"""
        db = SessionLocal()
        try:
            # Map Freq to Model
            # Clean dates
            s_date = datetime.strptime(str(start).replace('-',''), '%Y%m%d')
            e_date = datetime.strptime(str(end).replace('-',''), '%Y%m%d') + timedelta(days=1)

            result: List[Dict[str, Any]] = []
            if freq == 'D':
                daily_query = db.query(DailyBar).filter(DailyBar.ts_code == ts_code)
                daily_query = daily_query.filter(DailyBar.trade_date >= s_date.date(), DailyBar.trade_date < e_date.date())
                daily_query = daily_query.order_by(DailyBar.trade_date.asc())
                daily_rows: List[DailyBar] = await asyncio.to_thread(daily_query.all)
                for daily_row in daily_rows:
                    item: Dict[str, Any] = {
                        "open": float(daily_row.open or 0),
                        "high": float(daily_row.high or 0),
                        "low": float(daily_row.low or 0),
                        "close": float(daily_row.close or 0),
                        "volume": float(daily_row.vol or 0),
                        "vol": float(daily_row.vol or 0),
                        "adj_factor": float(daily_row.adj_factor or 1.0),
                        "time": daily_row.trade_date.strftime('%Y-%m-%d'),
                    }
                    item["pct_chg"] = float(getattr(daily_row, "pct_chg", 0) or 0)
                    result.append(item)
                return result
            if freq == 'W':
                weekly_query = db.query(WeeklyBar).filter(WeeklyBar.ts_code == ts_code)
                weekly_query = weekly_query.filter(WeeklyBar.trade_date >= s_date.date(), WeeklyBar.trade_date < e_date.date())
                weekly_query = weekly_query.order_by(WeeklyBar.trade_date.asc())
                weekly_rows: List[WeeklyBar] = await asyncio.to_thread(weekly_query.all)
                for weekly_row in weekly_rows:
                    item = {
                        "open": float(weekly_row.open or 0),
                        "high": float(weekly_row.high or 0),
                        "low": float(weekly_row.low or 0),
                        "close": float(weekly_row.close or 0),
                        "volume": float(weekly_row.vol or 0),
                        "vol": float(weekly_row.vol or 0),
                        "adj_factor": float(weekly_row.adj_factor or 1.0),
                        "time": weekly_row.trade_date.strftime('%Y-%m-%d'),
                    }
                    result.append(item)
                return result
            if freq == 'M':
                monthly_query = db.query(MonthlyBar).filter(MonthlyBar.ts_code == ts_code)
                monthly_query = monthly_query.filter(MonthlyBar.trade_date >= s_date.date(), MonthlyBar.trade_date < e_date.date())
                monthly_query = monthly_query.order_by(MonthlyBar.trade_date.asc())
                monthly_rows: List[MonthlyBar] = await asyncio.to_thread(monthly_query.all)
                for monthly_row in monthly_rows:
                    item = {
                        "open": float(monthly_row.open or 0),
                        "high": float(monthly_row.high or 0),
                        "low": float(monthly_row.low or 0),
                        "close": float(monthly_row.close or 0),
                        "volume": float(monthly_row.vol or 0),
                        "vol": float(monthly_row.vol or 0),
                        "adj_factor": float(monthly_row.adj_factor or 1.0),
                        "time": monthly_row.trade_date.strftime('%Y-%m-%d'),
                    }
                    result.append(item)
                return result
            if freq in ['5min', '30min']:
                minute_query = db.query(MinuteBar).filter(MinuteBar.ts_code == ts_code)
                minute_query = minute_query.filter(MinuteBar.freq == freq)
                minute_query = minute_query.filter(MinuteBar.trade_time >= s_date, MinuteBar.trade_time < e_date)
                minute_query = minute_query.order_by(MinuteBar.trade_time.asc())
                minute_rows: List[MinuteBar] = await asyncio.to_thread(minute_query.all)
                for minute_row in minute_rows:
                    if not minute_row.trade_time:
                        continue
                    item = {
                        "open": float(minute_row.open or 0),
                        "high": float(minute_row.high or 0),
                        "low": float(minute_row.low or 0),
                        "close": float(minute_row.close or 0),
                        "volume": float(minute_row.vol or 0),
                        "vol": float(minute_row.vol or 0),
                        "adj_factor": float(minute_row.adj_factor or 1.0),
                        "time": minute_row.trade_time.strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    result.append(item)
                return result
            return []
        except Exception as e:
            logger.error(f"DB Query failed for {ts_code} {freq}: {e}")
            return []
        finally:
            db.close()

    async def _fetch_network_kline(self, ts_code: str, freq: str, start: str, end: str, limit: int = None) -> List[Dict]:
        """Fetch K-Line from TDX"""
        from app.services.tdx_data_service import tdx_service
        
        # Count calculation
        count = 800
        if limit: count = max(800, limit)
        
        try:
            # TDX fetch is standardized
            df = await asyncio.to_thread(tdx_service.fetch_bars, ts_code, freq, count=count)
            if df is None or df.empty:
                return []
                
            # Convert to list of dicts
            result: List[Dict[str, Any]] = []
            for _, row in df.iterrows():
                # Handle time
                tt = row['trade_time']
                time_str = ""
                if isinstance(tt, (pd.Timestamp, datetime, date)):
                    if freq in ['D', 'W', 'M']:
                        time_str = tt.strftime('%Y-%m-%d')
                    else:
                        time_str = tt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    time_str = str(tt)

                item: Dict[str, Any] = {
                    "time": time_str,
                    "open": float(row.get('open', 0)),
                    "high": float(row.get('high', 0)),
                    "low": float(row.get('low', 0)),
                    "close": float(row.get('close', 0)),
                    "volume": float(row.get('vol', 0)),
                    "vol": float(row.get('vol', 0)),
                    "amount": float(row.get('amount', 0)),
                    "adj_factor": float(row.get('adj_factor', 1.0))
                }
                # Fix pct_chg if missing
                if 'pct_chg' in row:
                    item['pct_chg'] = float(row['pct_chg'])
                
                result.append(item)
                
            # Calculate pct_chg if missing
            for i in range(1, len(result)):
                prev = result[i-1]['close']
                curr = result[i]['close']
                if 'pct_chg' not in result[i] and prev > 0:
                    result[i]['pct_chg'] = round((curr - prev) / prev * 100, 2)
            
            # Align adj_factor with DB's latest_adj
            # TDX returns factors relative to 1.0 (usually), but DB stores absolute accumulated factors.
            # We must scale the fetched factors so that the latest bar matches the DB's latest factor.
            if result:
                latest_adj = await self._get_latest_adj_factor(ts_code)
                # Use the last bar's factor as the "current" reference
                last_factor = result[-1].get('adj_factor', 1.0)
                
                # If latest_adj is 1.0, maybe DB is empty? Then we trust TDX.
                # If DB has 134.5, and TDX has 1.0. We scale by 134.5.
                if latest_adj > 0 and last_factor > 0:
                    scale = latest_adj / last_factor
                    if abs(scale - 1.0) > 0.001:
                        for item in result:
                            item['adj_factor'] = round(item.get('adj_factor', 1.0) * scale, 6)

            now = datetime.now()
            if now.weekday() >= 5 or now.time() < time(9, 15):
                today_str = now.strftime('%Y-%m-%d')
                if freq in ['D', 'W', 'M']:
                    result = [item for item in result if item.get('time') != today_str]
                else:
                    result = [item for item in result if not str(item.get('time', '')).startswith(today_str)]
            return result
        except Exception as e:
            logger.error(f"Network fetch failed for {ts_code} {freq}: {e}")
            return []

    async def _fetch_tdx_local_kline(self, ts_code: str, freq: str, start: str, end: str, limit: int = None) -> List[Dict]:
        if freq not in ['D', '5min', '30min']:
            return []
        vipdoc_root = str(getattr(settings, "TDX_VIPDOC_ROOT", "") or "").strip()
        if not vipdoc_root:
            return []
        vip = TdxVipdocService(vipdoc_root)
        if freq == 'D':
            df = await asyncio.to_thread(vip.read_day_bars, ts_code, limit or 1200)
        else:
            base_df = await asyncio.to_thread(vip.read_5min_bars, ts_code, limit or 6000)
            if base_df is None or base_df.empty:
                return []
            df = base_df if freq == '5min' else vip.aggregate_30min_from_5min(base_df)
        if df is None or df.empty:
            return []
        df = df.copy()
        df['trade_time'] = pd.to_datetime(df['trade_time'], errors='coerce')
        df = df.dropna(subset=['trade_time'])
        start_dt = None
        end_dt = None
        try:
            start_dt = datetime.strptime(str(start).replace('-', ''), '%Y%m%d').date() if start else None
        except Exception:
            start_dt = None
        try:
            end_dt = datetime.strptime(str(end).replace('-', ''), '%Y%m%d').date() if end else None
        except Exception:
            end_dt = None
        if start_dt:
            df = df[df['trade_time'].dt.date >= start_dt]
        if end_dt:
            df = df[df['trade_time'].dt.date <= end_dt]
        if df.empty:
            return []
        df['adj_factor'] = 1.0
        try:
            from app.services.tdx_data_service import tdx_service
            xdxr_df = await asyncio.to_thread(tdx_service.get_xdxr_info, ts_code)
            if xdxr_df is not None and not xdxr_df.empty:
                df = await asyncio.to_thread(tdx_service._calc_adjust_factor, df, xdxr_df)
        except Exception as e:
            logger.warning(f"Local TDX adjust factor calc failed for {ts_code}: {e}")
        try:
            latest_adj = await self._get_latest_adj_factor(ts_code)
            if latest_adj > 0 and not df.empty:
                last_factor = float(df['adj_factor'].iloc[-1] or 0.0)
                if last_factor > 0:
                    scale = latest_adj / last_factor
                    if abs(scale - 1.0) > 0.001:
                        df['adj_factor'] = (df['adj_factor'] * scale).round(6)
        except Exception as e:
            logger.warning(f"Local TDX adj_factor align failed for {ts_code}: {e}")
        result: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            tt = row.get('trade_time')
            if hasattr(tt, 'strftime'):
                time_str = tt.strftime('%Y-%m-%d') if freq in ['D', 'W', 'M'] else tt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                time_str = str(tt)
            item: Dict[str, Any] = {
                "time": time_str,
                "open": float(row.get('open', 0)),
                "high": float(row.get('high', 0)),
                "low": float(row.get('low', 0)),
                "close": float(row.get('close', 0)),
                "volume": float(row.get('vol', 0)),
                "vol": float(row.get('vol', 0)),
                "amount": float(row.get('amount', 0)),
                "adj_factor": float(row.get('adj_factor', 1.0))
            }
            result.append(item)
        for i in range(1, len(result)):
            prev = result[i - 1]['close']
            curr = result[i]['close']
            if prev > 0:
                result[i]['pct_chg'] = round((curr - prev) / prev * 100, 2)
        now = datetime.now()
        if now.weekday() >= 5 or now.time() < time(9, 15):
            today_str = now.strftime('%Y-%m-%d')
            if freq in ['D', 'W', 'M']:
                result = [item for item in result if item.get('time') != today_str]
            else:
                result = [item for item in result if not str(item.get('time', '')).startswith(today_str)]
        return result

    def _aggregate_kline_from_daily(self, daily_data: List[Dict], freq: str) -> List[Dict]:
        if not daily_data:
            return []
        df = pd.DataFrame(daily_data)
        if df.empty or 'time' not in df.columns:
            return []
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
        df = df.dropna(subset=['time']).sort_values('time')
        if df.empty:
            return []
        if 'volume' not in df.columns:
            df['volume'] = df.get('vol', 0)
        if 'vol' not in df.columns:
            df['vol'] = df.get('volume', 0)
        if 'amount' not in df.columns:
            df['amount'] = 0
        if 'adj_factor' not in df.columns:
            df['adj_factor'] = 1.0
        df = df.set_index('time')
        rule = 'W-FRI' if freq == 'W' else 'ME'
        agg = df.resample(rule).agg(
            {
                'open': 'first',
                'close': 'last',
                'high': 'max',
                'low': 'min',
                'volume': 'sum',
                'vol': 'sum',
                'amount': 'sum',
                'adj_factor': 'last'
            }
        )
        agg = agg.dropna(subset=['close'])
        if agg.empty:
            return []
        agg = agg.reset_index()
        result: List[Dict[str, Any]] = []
        for _, row in agg.iterrows():
            tt = row.get('time')
            time_str = tt.strftime('%Y-%m-%d') if hasattr(tt, 'strftime') else str(tt)
            result.append(
                {
                    "time": time_str,
                    "open": float(row.get('open', 0)),
                    "high": float(row.get('high', 0)),
                    "low": float(row.get('low', 0)),
                    "close": float(row.get('close', 0)),
                    "volume": float(row.get('volume', 0)),
                    "vol": float(row.get('vol', 0)),
                    "amount": float(row.get('amount', 0)),
                    "adj_factor": float(row.get('adj_factor', 1.0))
                }
            )
        for i in range(1, len(result)):
            prev = result[i - 1]['close']
            curr = result[i]['close']
            if prev > 0:
                result[i]['pct_chg'] = round((curr - prev) / prev * 100, 2)
        return result

    async def _save_kline_to_db(self, ts_code: str, freq: str, data: List[Dict]):
        """Async save to DB"""
        if not data: return
        asyncio.create_task(self._background_save(ts_code, freq, data))

    async def _background_save(self, ts_code: str, freq: str, data: List[Dict]):
        # Implementation similar to original _background_save_kline but simplified
        # Using INSERT OR IGNORE / UPSERT
        db = SessionLocal()
        try:
            from sqlalchemy.dialects.sqlite import insert
            model: type[Any]
            if freq == 'D':
                model = DailyBar
            elif freq == 'W':
                model = WeeklyBar
            elif freq == 'M':
                model = MonthlyBar
            elif freq in ['5min', '30min']:
                model = MinuteBar
            else:
                return

            records = []
            for item in data:
                rec = {
                    "ts_code": ts_code,
                    "open": item['open'],
                    "high": item['high'],
                    "low": item['low'],
                    "close": item['close'],
                    "vol": item['volume'],
                    "adj_factor": item.get('adj_factor', 1.0)
                }
                
                if freq in ['D', 'W', 'M']:
                    rec["trade_date"] = datetime.strptime(item['time'], '%Y-%m-%d').date()
                    if freq == 'D' and 'pct_chg' in item:
                        rec["pct_chg"] = item['pct_chg']
                    if 'amount' in item:
                        rec["amount"] = item['amount']
                else:
                    time_str = str(item.get('time') or '').strip()
                    if len(time_str) > 19:
                        time_str = time_str[:19]
                    if len(time_str) == 8 and time_str[2] == ':' and time_str[5] == ':':
                        time_str = f"{datetime.now().strftime('%Y-%m-%d')} {time_str}"
                    try:
                        rec["trade_time"] = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        continue
                    rec["freq"] = freq
                    if 'amount' in item: rec["amount"] = item['amount']
                
                records.append(rec)
            
            # Batch Insert
            chunk_size = 100
            for i in range(0, len(records), chunk_size):
                batch = records[i:i+chunk_size]
                stmt = insert(model).values(batch)
                
                # Upsert logic
                index_elements = ['ts_code', 'trade_date'] if freq in ['D','W','M'] else ['ts_code', 'trade_time', 'freq']
                update_dict = {k: v for k, v in batch[0].items() if k not in index_elements}
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=index_elements,
                    set_=update_dict
                )
                db.execute(stmt)
                db.commit()
                
        except Exception as e:
            logger.error(f"Background save failed: {e}")
            db.rollback()
        finally:
            db.close()

    async def _merge_realtime_bar(self, kline: List[Dict], ts_code: str, freq: str, quote: Dict = None) -> List[Dict]:
        """
        Merge the latest realtime quote into K-Line data.
        Handles creating a new bar or updating the last bar.
        """
        if not quote:
            quote = await self.get_realtime_quote(ts_code)
        if not quote: return kline

        price = quote.get('price', 0)
        if price <= 0: return kline
        
        now = datetime.now()
        quote_time_str = quote.get('time', now.strftime('%Y-%m-%d %H:%M:%S'))
        if isinstance(quote_time_str, str):
            quote_time_str = quote_time_str.strip()
            if len(quote_time_str) > 19:
                quote_time_str = quote_time_str[:19]
        if isinstance(quote_time_str, str) and len(quote_time_str) == 8 and quote_time_str[2] == ':' and quote_time_str[5] == ':':
            date_part = str(quote.get('date') or now.strftime('%Y-%m-%d')).strip()
            if len(date_part) == 8 and date_part.isdigit():
                date_part = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]}"
            if '-' not in date_part:
                date_part = now.strftime('%Y-%m-%d')
            quote_time_str = f"{date_part} {quote_time_str}"
        try:
            quote_dt = datetime.strptime(quote_time_str, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return kline

        if quote_dt.weekday() >= 5:
            return kline
        qt = quote_dt.time()
        in_trading_window = (time(9, 15) <= qt <= time(11, 35)) or (time(13, 0) <= qt <= time(15, 1))
        if not in_trading_window and not is_after_market_close(quote_dt):
            return kline
        
        # Validation: Ignore future quotes or non-trading days (roughly)
        if quote_dt > now + timedelta(minutes=5): return kline
        
        # Create a Realtime Bar Object
        # Fetch latest adj factor to ensure correct QFQ
        latest_adj = await self._get_latest_adj_factor(ts_code)
        
        rt_bar = {
            "time": quote_time_str,
            "open": quote.get('open', price),
            "high": quote.get('high', price),
            "low": quote.get('low', price),
            "close": price,
            "volume": quote.get('volume', 0),
            "amount": quote.get('amount', 0),
            "adj_factor": latest_adj, 
            "pct_chg": quote.get('pct_chg', 0)
        }
        
        # --- Logic for Day/Week/Month ---
        if freq in ['D', 'W', 'M']:
            # Normalize dates
            last_bar_date = kline[-1]['time'] if kline else "1900-01-01"
            rt_date = quote_dt.strftime('%Y-%m-%d')
            
            # Check if we should update last bar or append new
            should_append = True
            
            if freq == 'D':
                if last_bar_date == rt_date: should_append = False
            elif freq == 'W':
                # Check iso week
                lb_dt = datetime.strptime(last_bar_date, '%Y-%m-%d')
                if lb_dt.isocalendar()[1] == quote_dt.isocalendar()[1] and lb_dt.year == quote_dt.year:
                    should_append = False
            elif freq == 'M':
                lb_dt = datetime.strptime(last_bar_date, '%Y-%m-%d')
                if lb_dt.month == quote_dt.month and lb_dt.year == quote_dt.year:
                    should_append = False
            
            if should_append:
                rt_bar['time'] = rt_date # Use YYYY-MM-DD for daily
                kline.append(rt_bar)
            else:
                # Update last bar
                last = kline[-1]
                last['close'] = rt_bar['close']
                last['high'] = max(last['high'], rt_bar['high'])
                last['low'] = min(last['low'], rt_bar['low'])
                if rt_bar['volume'] > last['volume']: last['volume'] = rt_bar['volume'] # Accumulate
                last['pct_chg'] = rt_bar['pct_chg']
                
        # --- Logic for Minute ---
        else:
            # 5min or 30min
            # We need to bucket the quote time
            interval = 5 if freq == '5min' else 30

            def _bucket_minutes(dt_val: datetime) -> Optional[datetime]:
                total_min = dt_val.hour * 60 + dt_val.minute
                if dt_val.second > 0:
                    total_min += 1
                rem = total_min % interval
                if rem != 0:
                    total_min += (interval - rem)

                bucket = dt_val.replace(hour=total_min // 60, minute=total_min % 60, second=0, microsecond=0)

                if bucket.time() < time(9, 35):
                    bucket = bucket.replace(hour=9, minute=35)
                if time(11, 30) < bucket.time() < time(13, 0):
                    bucket = bucket.replace(hour=11, minute=30)
                if bucket.time() > time(15, 0):
                    bucket = bucket.replace(hour=15, minute=0)
                return bucket

            bucket_dt = _bucket_minutes(quote_dt)
            if bucket_dt is None:
                return kline
            bucket_time_str = bucket_dt.strftime('%Y-%m-%d %H:%M:%S')
            
            if kline and kline[-1]['time'] == bucket_time_str:
                # Update
                last = kline[-1]
                last['close'] = price
                last['high'] = max(last['high'], price)
                last['low'] = min(last['low'], price)
                # Volume logic is tricky for minute bars (cumulative vs delta)
                # Ideally, we calculate delta from previous minute bars
                # Here, Simplified: We assume MinuteBar in DB has 'vol', and realtime quote 'vol' is daily cumulative.
                # So we cannot easily update 'volume' for the current minute bar without knowing the volume at the start of the bar.
                # SKIP volume update for minute bars to avoid data corruption, only update Price.
            elif kline and kline[-1]['time'] < bucket_time_str:
                # New Bar
                # Reset volume for new bar? We don't know the delta.
                # Set volume = 0 to be safe, or estimate.
                rt_bar['time'] = bucket_time_str
                rt_bar['volume'] = 0 # Cannot calculate delta accurately without state
                kline.append(rt_bar)
            if kline:
                normalized = {}
                for item in kline:
                    time_str = item.get('time')
                    if not time_str:
                        continue
                    try:
                        dt_val = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        continue
                    total_min = dt_val.hour * 60 + dt_val.minute
                    in_morning = 570 <= total_min <= 690
                    in_afternoon = 780 <= total_min <= 900
                    if not (in_morning or in_afternoon):
                        continue
                    normalized[time_str] = item
                kline = [normalized[k] for k in sorted(normalized.keys())]
        return kline

    async def _sync_latest_quote_to_kline(self, kline: List[Dict], ts_code: str, freq: str) -> List[Dict]:
        """Helper to sync only price for UI requests on cached data"""
        return await self._merge_realtime_bar(kline, ts_code, freq)

    async def _apply_qfq(self, kline: List[Dict], ts_code: str) -> List[Dict]:
        """Apply Forward Adjustment (QFQ)"""
        if not kline: return []
        
        latest_adj = None
        for item in reversed(kline):
            try:
                val = float(item.get('adj_factor', 0) or 0)
            except Exception:
                val = 0
            if val > 0:
                latest_adj = val
                break
        if not latest_adj:
            latest_adj = await self._get_latest_adj_factor(ts_code)
        
        # 2. Adjust
        res = []
        for item in kline:
            new_item = item.copy()
            curr_adj = float(new_item.get('adj_factor', 1.0))
            if curr_adj <= 0: curr_adj = 1.0
            
            ratio = curr_adj / latest_adj
            
            new_item['open'] = round(new_item['open'] * ratio, 2)
            new_item['high'] = round(new_item['high'] * ratio, 2)
            new_item['low'] = round(new_item['low'] * ratio, 2)
            new_item['close'] = round(new_item['close'] * ratio, 2)
            
            res.append(new_item)
            
        return res

    def _is_adj_factor_suspicious(self, kline: List[Dict]) -> bool:
        if not kline or len(kline) < 50:
            return False
        adj_vals = []
        for item in kline:
            try:
                adj_vals.append(float(item.get('adj_factor', 1.0) or 1.0))
            except Exception:
                adj_vals.append(1.0)
        if not adj_vals:
            return False
        latest_adj = adj_vals[-1]
        if latest_adj < 3:
            return False
        adj_vals_sorted = sorted(adj_vals)
        p70 = adj_vals_sorted[int(len(adj_vals_sorted) * 0.7)]
        return p70 <= 1.2

    async def _get_latest_adj_factor(self, ts_code: str) -> float:
        """Get latest adjust factor with cache"""
        now = datetime.now().timestamp()
        if ts_code in self._adj_factor_cache:
            val, ts = self._adj_factor_cache[ts_code]
            if now - ts < self._adj_cache_duration:
                return val
                
        # Fetch from DB
        db = SessionLocal()
        try:
            row = db.query(DailyBar.adj_factor).filter(DailyBar.ts_code == ts_code).order_by(desc(DailyBar.trade_date)).first()
            val = float(row[0]) if row and row[0] else 1.0
            self._adj_factor_cache[ts_code] = (val, now)
            return val
        except:
            return 1.0
        finally:
            db.close()

    def _calculate_indicators(self, kline: List[Dict], freq: str) -> List[Dict]:
        """Calculate technical indicators"""
        if not kline: return []
        try:
            from app.services.indicators.technical_indicators import technical_indicators
            # Convert to DataFrame
            df = technical_indicators.calculate(kline)
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Indicator calc failed: {e}")
            return kline

    # --- Market Stats / Overview ---

    async def get_market_snapshot(self, target_date: date = None):
        """Get Market Overview Snapshot"""
        # 1. Cache Check
        now = datetime.now().timestamp()
        if self._market_overview_cache and (now - self._last_overview_time < 15) and not target_date:
            return self._market_overview_cache

        # 2. Fetch Index Quotes
        indices = await self.get_realtime_quotes(['000001.SH', '399001.SZ', '399006.SZ'])
        
        # Validate Indices (If TDX returns 0, fallback to Sina)
        price_sh = indices.get('000001.SH', {}).get('price', 0)
        if not indices or price_sh <= 0:
            logger.warning(f"Index quotes invalid (SH={price_sh}), forcing Sina fallback.")
            try:
                indices_sina = await self._fetch_sina_quotes(['000001.SH', '399001.SZ', '399006.SZ'])
                if indices_sina:
                    indices.update(indices_sina)
                else:
                    logger.error("Sina fallback returned empty.")
            except Exception as e:
                logger.error(f"Sina fallback for indices failed: {e}")

        up, down, limit_up, limit_down, flat, amount_yi, stats_source = await self._fetch_market_counts()
        
        sh = dict(indices.get('000001.SH', {}) or {})
        sz = dict(indices.get('399001.SZ', {}) or {})
        cy = dict(indices.get('399006.SZ', {}) or {})

        if sh and not sh.get('ts_code'):
            sh['ts_code'] = '000001.SH'
        if sz and not sz.get('ts_code'):
            sz['ts_code'] = '399001.SZ'
        if cy and not cy.get('ts_code'):
            cy['ts_code'] = '399006.SZ'

        if sh and not sh.get('name'):
            sh['name'] = self.INDEX_NAMES.get('000001.SH', '上证指数')
        if sz and not sz.get('name'):
            sz['name'] = self.INDEX_NAMES.get('399001.SZ', '深证成指')
        if cy and not cy.get('name'):
            cy['name'] = self.INDEX_NAMES.get('399006.SZ', '创业板指')

        indices_list = [v for v in [sh, sz, cy] if v]

        snapshot = {
            "sh_index": sh.get('price', 0),
            "sz_index": sz.get('price', 0),
            "cy_index": cy.get('price', 0),
            "sh_chg": sh.get('pct_chg', 0),
            "up": up,
            "down": down,
            "flat": flat,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "total_amount_亿元": amount_yi,
            "total_volume": amount_yi,
            "stats_source": stats_source,
            "time": datetime.now().strftime('%H:%M:%S'),
            "sh": sh or None,
            "sz": sz or None,
            "cy": cy or None,
            "indices": indices_list,
        }
        
        if not target_date:
            if snapshot.get('sh_index', 0) > 0:
                self._market_overview_cache = snapshot
                self._last_overview_time = now
            
        return snapshot

    def _get_limit_up_codes_local(self, target_date: date | None = None) -> List[str]:
        db = SessionLocal()
        try:
            latest_date = target_date or stock_data_service.get_latest_trade_date(db)
            if not latest_date:
                return []
            rows = (
                db.query(DailyBar.ts_code)
                .filter(DailyBar.trade_date == latest_date, DailyBar.pct_chg >= 9.5)
                .order_by(desc(DailyBar.amount))
                .all()
            )
            return [str(r[0]) for r in rows if r and r[0]]
        finally:
            db.close()

    def get_limit_up_stocks(self) -> List[str]:
        return self._get_limit_up_codes_local()

    async def get_realtime_limit_up_codes(self) -> List[str]:
        return await asyncio.to_thread(self._get_limit_up_codes_local)

    def _is_counts_plausible(self, counts: tuple[int, int, int, int, int]) -> bool:
        try:
            up, down, limit_up, limit_down, flat = counts
        except Exception:
            return False
        if min(up, down, limit_up, limit_down, flat) < 0:
            return False
        total = up + down + flat
        if total < 2500 or total > 7000:
            return False
        if limit_up > total or limit_down > total:
            return False
        return True

    async def _fetch_market_counts(self, force_tdx: bool = False) -> tuple[int, int, int, int, int, float, str]:
        try:
            from app.services.tdx_data_service import tdx_service
            now = datetime.now().timestamp()
            q = await asyncio.to_thread(tdx_service.fetch_realtime_quotes, ['880005'])
            if q and len(q) > 0:
                d = q[0]
                up = int(d.get('price', 0))
                down = int(d.get('open', 0))
                total = int(d.get('high', 0))
                flat = max(0, total - up - down)
                limit_up = int(d.get('bid_vol5', 0) or d.get('bid1', 0) or 0)
                limit_down = int(d.get('ask_vol5', 0) or d.get('ask1', 0) or 0)
                amount = float(d.get('amount', 0)) / 100000000.0
                if self._is_counts_plausible((up, down, limit_up, limit_down, flat)):
                    stats = (up, down, limit_up, limit_down, flat, round(amount, 2), "TDX_880005")
                    self._market_stats_cache = stats
                    self._market_stats_cache_time = now
                    return stats
        except Exception as e:
            logger.warning(f"TDX 880005 fetch failed: {e}")

        now = datetime.now().timestamp()
        if self._market_stats_cache and (now - self._market_stats_cache_time) < self._market_stats_cache_ttl:
            cached = self._market_stats_cache
            return (cached[0], cached[1], cached[2], cached[3], cached[4], cached[5], "TDX_880005_CACHE")

        if is_after_market_close():
            counts_local = await asyncio.to_thread(stock_data_service.get_market_counts_local)
            if counts_local:
                up, down, limit_up, limit_down, flat, amount_yi = counts_local
                stats = (up, down, limit_up, limit_down, flat, float(amount_yi or 0), "CLOSE_CACHE")
                self._market_stats_cache = stats
                self._market_stats_cache_time = now
                return stats

        if force_tdx:
            return (0, 0, 0, 0, 0, 0.0, "TDX_880005_EMPTY")

        return (0, 0, 0, 0, 0, 0.0, "EMPTY")

    # --- AI Context ---
    
    async def get_ai_context_data(self, ts_code: str, no_side_effect: bool = True, cache_scope: Optional[str] = None) -> Dict[str, Any]:
        """Get all context for AI"""
        
        # Parallel Fetch
        results = await asyncio.gather(
            self.get_realtime_quote(ts_code, cache_scope=cache_scope),
            self.get_kline(ts_code, 'D', limit=60, adj='qfq', include_indicators=True, cache_scope=cache_scope),
            self.get_kline(ts_code, 'W', limit=20, adj='qfq', include_indicators=True, cache_scope=cache_scope),
            self.get_kline(ts_code, 'M', limit=12, adj='qfq', include_indicators=True, cache_scope=cache_scope),
            self.get_kline(ts_code, '30min', limit=16, adj='qfq', include_indicators=True, cache_scope=cache_scope),
            return_exceptions=True
        )
        
        # Unpack safely
        quote = results[0] if not isinstance(results[0], Exception) else {}
        daily = results[1] if not isinstance(results[1], Exception) else []
        weekly = results[2] if not isinstance(results[2], Exception) else []
        monthly = results[3] if not isinstance(results[3], Exception) else []
        min30 = results[4] if not isinstance(results[4], Exception) else []
        
        stats = await self._get_ai_stats(ts_code)

        # Basic Info
        basic = await asyncio.to_thread(stock_data_service.get_stock_basic)
        info = next((b for b in basic if b['ts_code'] == ts_code), {})
        
        return {
            "ts_code": ts_code,
            "name": info.get('name', ''),
            "quote": quote,
            "kline_d": daily,
            "weekly_k": weekly,
            "monthly_k": monthly,
            "kline_30m": min30,
            "fundamental": {},
            "stats": stats,
        }

    async def _get_ai_stats(self, ts_code: str) -> Dict[str, Any]:
        def _query_stats() -> Dict[str, Any]:
            db = SessionLocal()
            try:
                today = date.today()
                start_5y = today - timedelta(days=365 * 5)
                start_6m = today - timedelta(days=183)

                def _fetch_extreme(is_high: bool, start_date: date):
                    order_col = DailyBar.high.desc() if is_high else DailyBar.low.asc()
                    row = (
                        db.query(DailyBar.high, DailyBar.low, DailyBar.trade_date)
                        .filter(DailyBar.ts_code == ts_code, DailyBar.trade_date >= start_date)
                        .order_by(order_col, DailyBar.trade_date.desc())
                        .first()
                    )
                    if not row:
                        return 0.0, ""
                    if is_high:
                        return float(row[0] or 0.0), row[2].strftime("%Y-%m-%d") if row[2] else ""
                    return float(row[1] or 0.0), row[2].strftime("%Y-%m-%d") if row[2] else ""

                h_5y, h_date = _fetch_extreme(True, start_5y)
                l_5y, l_date = _fetch_extreme(False, start_5y)
                h_6m, _ = _fetch_extreme(True, start_6m)
                l_6m, _ = _fetch_extreme(False, start_6m)

                avg_vol_6m = (
                    db.query(func.avg(DailyBar.vol))
                    .filter(DailyBar.ts_code == ts_code, DailyBar.trade_date >= start_6m)
                    .scalar()
                )

                return {
                    "h_5y": h_5y,
                    "l_5y": l_5y,
                    "h_6m": h_6m,
                    "l_6m": l_6m,
                    "h_date": h_date,
                    "l_date": l_date,
                    "avg_vol_6m": float(avg_vol_6m or 0.0),
                }
            finally:
                db.close()

        return await asyncio.to_thread(_query_stats)

    # --- Helpers ---
    async def get_last_trade_date(self, include_today=False) -> str:
        return await asyncio.to_thread(stock_data_service.get_latest_trade_date_local)
        
    async def check_trade_day(self, date_str: str = None) -> dict:
        if not date_str:
            date_str = datetime.now().strftime('%Y%m%d')
        # Simple check: Weekend?
        dt = datetime.strptime(date_str, '%Y%m%d')
        if dt.weekday() >= 5:
             return {"is_open": False, "reason": "Weekend"}
        return {"is_open": True}

    async def get_market_turnover_top(self, top_n: int = 100) -> List[Dict]:
        trade_date = await asyncio.to_thread(stock_data_service.get_latest_trade_date_local)
        rows = await asyncio.to_thread(stock_data_service.get_top_turnover_local, trade_date, top_n)
        if not rows:
            fallback_date = await asyncio.to_thread(stock_data_service.get_latest_trade_date)
            if fallback_date:
                trade_date = fallback_date.strftime("%Y%m%d")
                rows = await asyncio.to_thread(stock_data_service.get_top_turnover_local, trade_date, top_n)
        turnover_rate_map: Dict[str, float] = {}
        if not rows:
            basics = await asyncio.to_thread(stock_data_service.get_daily_basic_local, trade_date)
            if basics:
                turnover_rate_map = {
                    str(b.get("ts_code")): float(b.get("turnover_rate") or 0.0)
                    for b in basics
                    if b.get("ts_code")
                }
                basics_sorted = sorted(
                    basics,
                    key=lambda x: float(x.get("amount") or 0.0) if x.get("amount") is not None else float(x.get("circ_mv") or 0.0),
                    reverse=True,
                )
                rows = [
                    {
                        "ts_code": b.get("ts_code"),
                        "turnover_amount": float(b.get("amount") or 0.0),
                        "turnover_rate": float(b.get("turnover_rate") or 0.0),
                    }
                    for b in basics_sorted
                    if b.get("ts_code")
                ][:top_n]
        if not rows:
            def _fallback_basic():
                db = SessionLocal()
                try:
                    t_date = datetime.strptime(trade_date, "%Y%m%d").date()
                    query = db.query(DailyBasic.ts_code, DailyBasic.circ_mv, Stock.name, Stock.industry).join(
                        Stock, Stock.ts_code == DailyBasic.ts_code, isouter=True
                    ).filter(DailyBasic.trade_date == t_date).order_by(desc(DailyBasic.circ_mv)).limit(top_n * 3)
                    return query.all()
                finally:
                    db.close()
            basic_rows = await asyncio.to_thread(_fallback_basic)
            if basic_rows:
                rows = [
                    {"ts_code": ts_code, "turnover_amount": float(circ_mv or 0.0), "name": name, "industry": industry}
                    for ts_code, circ_mv, name, industry in basic_rows
                    if ts_code
                ][:top_n]
        if not rows:
            return []
        if not turnover_rate_map:
            basics = await asyncio.to_thread(stock_data_service.get_daily_basic_local, trade_date)
            if basics:
                turnover_rate_map = {
                    str(b.get("ts_code")): float(b.get("turnover_rate") or 0.0)
                    for b in basics
                    if b.get("ts_code")
                }
            else:
                fallback_date = await asyncio.to_thread(stock_data_service.get_latest_trade_date_local)
                if fallback_date and fallback_date != trade_date:
                    basics = await asyncio.to_thread(stock_data_service.get_daily_basic_local, fallback_date)
                    if basics:
                        turnover_rate_map = {
                            str(b.get("ts_code")): float(b.get("turnover_rate") or 0.0)
                            for b in basics
                            if b.get("ts_code")
                        }
        basic_list = await asyncio.to_thread(stock_data_service.get_stock_basic)
        basic_map = {b.get("ts_code"): b for b in basic_list if isinstance(b, dict)}
        out: List[Dict] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            ts_code = r.get("ts_code")
            if not ts_code:
                continue
            basic = basic_map.get(ts_code) or {}
            name_val = str(basic.get("name") or r.get("name") or "")
            if ts_code.startswith(("688", "8", "4")) or str(ts_code).endswith(".BJ"):
                continue
            if "ST" in name_val or "退" in name_val:
                continue
            turnover_rate = float(r.get("turnover_rate") or turnover_rate_map.get(ts_code) or 0.0)
            if turnover_rate < 5.0 or turnover_rate > 15.0:
                continue
            out.append(
                {
                    "ts_code": ts_code,
                    "turnover_amount": float(r.get("turnover_amount") or 0.0),
                    "name": name_val,
                    "industry": str(basic.get("industry") or r.get("industry") or ""),
                    "turnover_rate": turnover_rate,
                }
            )
        if not out and basic_map:
            basics_sorted = sorted(
                [b for b in basic_map.values() if isinstance(b, dict) and b.get("ts_code")],
                key=lambda x: float(x.get("circ_mv") or 0.0),
                reverse=True,
            )
            for b in basics_sorted:
                ts_code = str(b.get("ts_code") or "")
                name_val = str(b.get("name") or "")
                if ts_code.startswith(("688", "8", "4")) or ts_code.endswith(".BJ"):
                    continue
                if "ST" in name_val or "退" in name_val:
                    continue
                turnover_rate = float(turnover_rate_map.get(ts_code) or 0.0)
                if turnover_rate < 5.0 or turnover_rate > 15.0:
                    continue
                out.append(
                    {
                        "ts_code": ts_code,
                        "turnover_amount": float(b.get("circ_mv") or 0.0),
                        "name": name_val,
                        "industry": str(b.get("industry") or ""),
                        "turnover_rate": turnover_rate,
                    }
                )
                if len(out) >= top_n:
                    break
        return out

    # --- Compatibility Stubs for DataProvider ---
    
    async def get_market_overview(self):
        """Alias for get_market_snapshot"""
        return await self.get_market_snapshot()
        
    async def merge_realtime_to_kline(self, kline: List[Dict], quote: Dict, freq: str = 'D', ts_code: str = None):
        """Wrapper for _merge_realtime_bar"""
        ts_code_val = ts_code or str((quote or {}).get('ts_code') or "")
        if not ts_code_val:
            return kline
        return await self._merge_realtime_bar(kline, ts_code_val, freq, quote=quote)

    def _is_index(self, ts_code: str) -> bool:
        if not ts_code: return False
        return ts_code in self.INDEX_NAMES or ts_code.startswith('880')

    def _is_index_or_industry(self, ts_code: str) -> bool:
        return self._is_index(ts_code)

    async def get_sector_context(self, ts_code: str): return {}
    async def get_moneyflow(self, ts_code: str): return {}
    async def get_fina_indicator(self, ts_code: str): return {}
    async def buffer_realtime_quotes(self, codes): pass
    async def get_daily_basic(self, trade_date: str = None, ts_code: str = None, ts_codes: List[str] = None, allow_fallback_latest: bool = False):
        target_date = trade_date
        if not target_date and allow_fallback_latest:
            target_date = await asyncio.to_thread(stock_data_service.get_latest_trade_date_local)
        if not target_date:
            return pd.DataFrame()
        code_list = ts_codes or ([ts_code] if ts_code else None)
        rows = await asyncio.to_thread(stock_data_service.get_daily_basic_local, target_date, code_list)
        if not rows and allow_fallback_latest:
            fallback_date = await asyncio.to_thread(stock_data_service.get_latest_trade_date_local)
            if fallback_date and fallback_date != target_date:
                rows = await asyncio.to_thread(stock_data_service.get_daily_basic_local, fallback_date, code_list)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)
    async def get_turnover_top_codes(self, universe_codes: List[str] = None, top_n: int = 100) -> List[str]:
        if not universe_codes:
            return []
        trade_date = await asyncio.to_thread(stock_data_service.get_latest_trade_date_local)
        rows = await asyncio.to_thread(stock_data_service.get_top_turnover_local, trade_date, max(int(top_n or 100), 1))
        if not rows:
            return []
        universe = {str(c) for c in universe_codes if c}
        codes: List[str] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            ts_code = str(r.get("ts_code") or "")
            if ts_code and ts_code in universe:
                codes.append(ts_code)
            if len(codes) >= int(top_n or 100):
                break
        return codes

    async def get_market_turnover_top_codes(self, top_n: int = 200) -> List[str]:
        rows = await self.get_market_turnover_top(top_n=top_n)
        return [str(r.get("ts_code")) for r in rows if isinstance(r, dict) and r.get("ts_code")]
    async def get_ths_turnover_top_codes(self, top_n: int = 100) -> List[str]: return []
    async def get_realtime_speed_top(self, top_n: int = 10) -> List[Dict]: return []

    # --- Deprecated / Legacy Stubs ---
    async def get_active_stock_codes(self): return set()
    async def purge_inactive_cache(self): pass
    def flush_minute_buffer(self): pass
    async def is_local_data_updated(self): return True


market_data_service = MarketDataService()
