import pandas as pd
import numpy as np
import asyncio
import threading
from sqlalchemy import text
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from app.db.session import SessionLocal
from app.models.stock_models import StockIndicator, Stock
from app.services.data_provider import data_provider
from app.services.logger import logger
from app.services.indicators.technical_indicators import technical_indicators

class IndicatorService:
    def __init__(self):
        import os
        # 使用自定义线程池控制 CPU 负载，避免阻塞事件循环
        # 核心数的一半左右通常比较安全，既能并行又不至于压死 CPU
        max_workers = max(2, (os.cpu_count() or 4) // 2)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._db_lock = threading.Lock() # 进程内数据库写入锁

    def _get_last_indicator_date(self, ts_code: str) -> Optional[date]:
        """获取本地数据库中该股票最后一条指标记录的日期"""
        db = SessionLocal()
        try:
            from sqlalchemy import func
            last_date = db.query(func.max(StockIndicator.trade_date)).filter(StockIndicator.ts_code == ts_code).scalar()
            return last_date
        finally:
            db.close()

    async def _calculate_single_stock(self, ts_code: str, target_date: str, force_no_cache: bool = False, return_full_history: bool = False, local_only: bool = True) -> Optional[list[dict[str, Any]]]:
        """
        计算单个股票、指数或板块的所有指标
        :param target_date: 目标日期字符串
        :param force_no_cache: 是否强制不使用缓存（用于复权因子变动时）
        :param return_full_history: 是否返回全量历史指标（用于补全历史数据）
        :param local_only: 是否仅使用本地数据（False则会尝试获取实时行情合并）
        """
        try:
            # 1. 确定获取 K 线数据的起始日期
            # 默认获取 5 年以确保指标（如 MA60, MACD）有足够历史背景
            five_years_ago = (datetime.now() - timedelta(days=1825)).strftime('%Y%m%d')
            
            if return_full_history or force_no_cache:
                start_date = five_years_ago
            else:
                # 增量模式：获取最后一条记录，往前推 150 天以保证 MA60 等指标计算准确
                last_date = await asyncio.get_event_loop().run_in_executor(self.executor, self._get_last_indicator_date, ts_code)
                if last_date:
                    start_date = (last_date - timedelta(days=150)).strftime('%Y%m%d')
                    if start_date < five_years_ago:
                        start_date = five_years_ago
                else:
                    start_date = five_years_ago

            # 2. 获取日线数据 (使用前复权价格计算，以保证指标在复权因子变动时的准确性)
            daily_kline = await data_provider.get_kline(ts_code, freq='D', start_date=start_date, local_only=local_only, include_indicators=False, adj='qfq')
            
            if not daily_kline:
                logger.warning(f"{ts_code} daily_kline is empty (from {start_date})")
                return None
            
            # 获取计算时使用的最新复权因子，用于后续存储
            # 注意：get_kline(adj='qfq') 返回的数据中 adj_factor 是原始的，但价格是复权后的
            # 我们需要记录这个数据是基于哪个 latest_adj 复权的
            from app.services.market.market_data_service import MarketDataService
            m_service = MarketDataService()
            latest_adj = await m_service._get_latest_adj_factor(ts_code)
                
            # 3. 准备聚合和计算逻辑 (移至线程执行，防止阻塞事件循环)
            def _aggregate_and_calc_indicators(daily_kline, ts_code, target_date, force_no_cache):
                df_daily_all = pd.DataFrame(daily_kline)
                df_daily_all['time_dt'] = pd.to_datetime(df_daily_all['time'])
                
                def aggregate_kline(df, freq):
                    if df.empty: return []
                    df = df.copy()
                    if freq == 'W':
                        df['year'] = df['time_dt'].dt.isocalendar().year
                        df['week'] = df['time_dt'].dt.isocalendar().week
                        group_cols = ['year', 'week']
                    else:
                        df['year'] = df['time_dt'].dt.year
                        df['month'] = df['time_dt'].dt.month
                        group_cols = ['year', 'month']
                    
                    agg_df = df.groupby(group_cols).agg({
                        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
                        'volume': 'sum', 'adj_factor': 'last', 'time': 'last'
                    }).reset_index()
                    
                    if agg_df.empty: return []
                    agg_df['ts_code'] = ts_code
                    return agg_df.to_dict('records')

                weekly_kline = aggregate_kline(df_daily_all.copy(), 'W')
                monthly_kline = aggregate_kline(df_daily_all.copy(), 'M')
                
                # 4. 计算指标
                cache_key_d = None if force_no_cache else f"{ts_code}_D_{target_date}_{len(daily_kline)}"
                cache_key_w = None if force_no_cache else f"{ts_code}_W_{target_date}_{len(weekly_kline)}"
                cache_key_m = None if force_no_cache else f"{ts_code}_M_{target_date}_{len(monthly_kline)}"

                from app.services.indicators.technical_indicators import technical_indicators
                df_d = technical_indicators.calculate(daily_kline, cache_key_d)
                df_w = technical_indicators.calculate(weekly_kline, cache_key_w)
                df_m = technical_indicators.calculate(monthly_kline, cache_key_m)
                
                return df_d, df_w, df_m, df_daily_all

            # 将数据转换、聚合和计算全部移至线程 (使用自定义线程池)
            df_d, df_w, df_m, df_daily_all = await asyncio.get_event_loop().run_in_executor(
                self.executor, _aggregate_and_calc_indicators, daily_kline, ts_code, target_date, force_no_cache
            )
            
            if df_d.empty:
                logger.warning(f"{ts_code} technical indicators calculation returned empty DF")
                return None
            
            # 5. 结果聚合优化 (使用 pd.merge_asof 替代嵌套循环，同样移至线程)
            def _process_results_in_thread(df_d, df_w, df_m, df_daily_all, ts_code, latest_adj, return_full_history):
                df_d['trade_date_dt'] = pd.to_datetime(df_d['time'])
                
                # 计算趋势恢复信号 (基于月线)
                is_trend_recovering = 0
                if not df_m.empty:
                    is_trend_recovering = 1 if self._check_monthly_trend_logic(df_m.copy()) else 0

                df_w_renamed = pd.DataFrame()
                if not df_w.empty:
                    df_w['trade_date_dt'] = pd.to_datetime(df_w['time'])
                    w_cols = {col: f'weekly_{col}' for col in df_w.columns if col not in ['time', 'trade_date_dt', 'ts_code']}
                    df_w_renamed = df_w.rename(columns=w_cols)

                df_m_renamed = pd.DataFrame()
                if not df_m.empty:
                    df_m['trade_date_dt'] = pd.to_datetime(df_m['time'])
                    m_cols = {col: f'monthly_{col}' for col in df_m.columns if col not in ['time', 'trade_date_dt', 'ts_code']}
                    df_m_renamed = df_m.rename(columns=m_cols)

                df_final = df_d.sort_values('trade_date_dt')
                if not df_w_renamed.empty:
                    df_final = pd.merge_asof(df_final, df_w_renamed.sort_values('trade_date_dt'), on='trade_date_dt', direction='backward')
                if not df_m_renamed.empty:
                    df_final = pd.merge_asof(df_final, df_m_renamed.sort_values('trade_date_dt'), on='trade_date_dt', direction='backward')

                if not return_full_history:
                    df_final = df_final.tail(1)

                results = []
                for _, row in df_final.iterrows():
                    is_ma_bullish = (row['ma5'] > row['ma10'] > row['ma20'])
                    day_data = df_daily_all[df_daily_all['time_dt'].dt.date == row['trade_date_dt'].date()]
                    current_day_adj = float(day_data['adj_factor'].iloc[0]) if not day_data.empty else latest_adj
                    unadj_ratio = latest_adj / current_day_adj if current_day_adj != 0 else 1.0
                    
                    def unadjust(val):
                        if val is None or pd.isna(val): return None
                        return float(val * unadj_ratio)
                    
                    results.append({
                        "ts_code": ts_code,
                        "trade_date": row['trade_date_dt'].date(),
                        "ma5": unadjust(row['ma5']),
                        "ma10": unadjust(row['ma10']),
                        "ma20": unadjust(row['ma20']),
                        "ma60": unadjust(row['ma60']),
                        "vol_ma5": float(row.get('vol_ma5', 0)) if not pd.isna(row.get('vol_ma5')) else None,
                        "vol_ma10": float(row.get('vol_ma10', 0)) if not pd.isna(row.get('vol_ma10')) else None,
                        "macd": unadjust(row['macd']),
                        "macd_dea": unadjust(row['macd_dea']),
                        "macd_diff": unadjust(row['macd_diff']),
                        "bias5": float(row.get('bias5')) if not pd.isna(row.get('bias5')) else None,
                        "bias10": float(row.get('bias10')) if not pd.isna(row.get('bias10')) else None,
                        "bias20": float(row.get('bias20')) if not pd.isna(row.get('bias20')) else None,
                        "weekly_ma5": unadjust(row.get('weekly_ma5')),
                        "weekly_ma10": unadjust(row.get('weekly_ma10')),
                        "weekly_ma20": unadjust(row.get('weekly_ma20')),
                        "weekly_ma60": unadjust(row.get('weekly_ma60')),
                        "weekly_vol_ma5": float(row.get('weekly_vol_ma5', 0)) if not pd.isna(row.get('weekly_vol_ma5')) else None,
                        "weekly_vol_ma10": float(row.get('weekly_vol_ma10', 0)) if not pd.isna(row.get('weekly_vol_ma10')) else None,
                        "weekly_macd": unadjust(row.get('weekly_macd')),
                        "weekly_macd_dea": unadjust(row.get('weekly_macd_dea')),
                        "weekly_macd_diff": unadjust(row.get('weekly_macd_diff')),
                        "weekly_ma20_slope": unadjust(row.get('weekly_ma20_slope')),
                        "is_weekly_bullish": int(row.get('weekly_ma5', 0) > row.get('weekly_ma10', 0) > row.get('weekly_ma20', 0)) if 'weekly_ma5' in row else 0,
                        "monthly_ma5": unadjust(row.get('monthly_ma5')),
                        "monthly_ma10": unadjust(row.get('monthly_ma10')),
                        "monthly_ma20": unadjust(row.get('monthly_ma20')),
                        "monthly_ma60": unadjust(row.get('monthly_ma60')),
                        "monthly_vol_ma5": float(row.get('monthly_vol_ma5', 0)) if not pd.isna(row.get('monthly_vol_ma5')) else None,
                        "monthly_vol_ma10": float(row.get('monthly_vol_ma10', 0)) if not pd.isna(row.get('monthly_vol_ma10')) else None,
                        "monthly_macd": unadjust(row.get('monthly_macd')),
                        "monthly_macd_dea": unadjust(row.get('monthly_macd_dea')),
                        "monthly_macd_diff": unadjust(row.get('monthly_macd_diff')),
                        "is_monthly_bullish": int(row.get('monthly_ma5', 0) > row.get('monthly_ma10', 0) > row.get('monthly_ma20', 0)) if 'monthly_ma5' in row else 0,
                        "is_daily_bullish": int(is_ma_bullish),
                        "is_trend_recovering": is_trend_recovering,
                        "adj_factor": current_day_adj
                    })
                return results

            return await asyncio.get_event_loop().run_in_executor(
                self.executor, _process_results_in_thread, df_d, df_w, df_m, df_daily_all, ts_code, latest_adj, return_full_history
            )
        except Exception as e:
            logger.warning(f"计算 {ts_code} 指标失败: {e}")
            return None

    def _check_monthly_trend_logic(self, df: pd.DataFrame) -> bool:
        """
        Re-implementation of _check_monthly_trend logic
        """
        if len(df) < 20: return False
        
        # Ensure columns
        df['open'] = pd.to_numeric(df['open'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        # VMA5 already calculated? No, need to calc
        df['vma5'] = df['volume'].rolling(window=5).mean()
        
        # Calculate MAs needed for "Start Bar" check
        for w in [5, 10, 20, 30, 60]:
            if f'ma{w}' not in df.columns:
                df[f'ma{w}'] = df['close'].rolling(window=w).mean()
                
        n_bars = len(df)
        found_start_bar = False
        start_idx = -1
        
        # Check range: last 4 completed bars (excluding current one if it's unfinished? 
        # Usually get_kline returns completed days, but for monthly, the last one might be current month.
        # Logic says "exclude current month" -> range(n_bars - 2, ...)
        
        check_range = range(n_bars - 2, max(n_bars - 6, 0), -1)
        
        for i in check_range:
            bar = df.iloc[i]
            prev_bar = df.iloc[i-1]
            
            # 1. Yang line
            if bar['close'] <= bar['open']: continue
            
            # 2. Heavy Volume
            is_heavy_vol = False
            vma5 = bar['vma5']
            if not pd.isna(vma5) and vma5 > 0 and bar['volume'] > vma5 * 1.5:
                is_heavy_vol = True
            elif bar['volume'] > prev_bar['volume'] * 1.5:
                is_heavy_vol = True
                
            if not is_heavy_vol: continue
            
            # 3. Above all MAs
            mas = [bar[f'ma{w}'] for w in [5, 10, 20, 30, 60] if not pd.isna(bar.get(f'ma{w}'))]
            if not mas or any(bar['close'] <= ma for ma in mas): continue
            
            found_start_bar = True
            start_idx = i
            break
            
        if not found_start_bar:
            return False
            
        # Check pullback
        start_bar = df.iloc[start_idx]
        support_price = start_bar['open']
        
        for j in range(start_idx + 1, n_bars):
            curr = df.iloc[j]
            if curr['close'] < support_price:
                return False
                
        return True

    async def calculate_for_codes(self, ts_codes: list, trade_date=None, force_full=False, force_recalc_today=False):
        """
        按需计算指定股票/指数的指标
        :param force_full: 是否强制全量重算历史指标
        :param force_recalc_today: 是否强制重算今日指标 (即使 DB 已存在也重算，用于盘中实时更新)
        """
        if not ts_codes:
            return
            
        target_date_str = trade_date if trade_date else await data_provider.get_last_trade_date()
        target_date = datetime.strptime(target_date_str.replace('-', ''), '%Y%m%d').date()

        # 智能检查：过滤掉已经有最新指标且复权因子一致的股票
        def _filter_existing(codes, t_date):
            if force_full:
                return codes
            
            # 如果是强制重算今日，且目标日期就是今天，则不过滤
            if force_recalc_today and t_date == datetime.now().date():
                return codes

            db = SessionLocal()
            try:
                # 获取已存在的指标状态 (增加 macd 检查)
                existing = db.query(
                    StockIndicator.ts_code,
                    StockIndicator.trade_date,
                    StockIndicator.adj_factor,
                    StockIndicator.macd,
                    StockIndicator.weekly_ma20_slope,
                    StockIndicator.is_weekly_bullish,
                    StockIndicator.is_monthly_bullish,
                    StockIndicator.is_daily_bullish
                ).filter(
                    StockIndicator.ts_code.in_(codes),
                    StockIndicator.trade_date == t_date
                ).all()
                
                # 获取当前的复权因子
                from app.models.stock_models import DailyBar
                latest_adjs = db.query(DailyBar.ts_code, DailyBar.adj_factor).filter(
                    DailyBar.trade_date == t_date,
                    DailyBar.ts_code.in_(codes)
                ).all()
                latest_adj_map = {r.ts_code: r.adj_factor for r in latest_adjs}
                
                # existing_map 现在存储 (adj_factor, macd)
                existing_map = {r.ts_code: r for r in existing}
                
                needed = []
                for code in codes:
                    if code not in existing_map:
                        needed.append(code)
                        continue
                    
                    # 检查复权因子是否一致
                    record = existing_map[code]
                    last_adj = record.adj_factor
                    last_macd = record.macd
                    slope = record.weekly_ma20_slope
                    monthly = record.is_monthly_bullish
                    weekly = record.is_weekly_bullish
                    daily = record.is_daily_bullish
                    curr_adj = latest_adj_map.get(code)
                    
                    # 检查关键指标是否为空 (如果为空也需要重新计算)
                    if last_macd is None:
                        # 对于指数，如果没有 macd 可能是正常的，但对于个股通常不应该为空
                        from app.services.market.market_data_service import market_data_service
                        if not market_data_service._is_index_or_industry(code):
                            logger.info(f"Missing MACD for {code} on {t_date}. Triggering recalculation.")
                            needed.append(code)
                            continue
                    if slope is None or monthly is None or weekly is None or daily is None:
                        from app.services.market.market_data_service import market_data_service
                        if not market_data_service._is_index_or_industry(code):
                            logger.info(f"Missing trend fields for {code} on {t_date}. Triggering recalculation.")
                            needed.append(code)
                            continue

                    # 检查复权因子是否一致
                    from app.services.market.market_data_service import market_data_service
                    is_special = market_data_service._is_index_or_industry(code)
                    
                    if not is_special and curr_adj is not None and abs((last_adj or 0) - curr_adj) > 1e-6:
                        logger.info(f"Detected adj_factor change for {code} in on-demand calc. Triggering recalculation.")
                        needed.append(code)
                        continue
                        
                return needed
            finally:
                db.close()

        needed_codes = await asyncio.get_event_loop().run_in_executor(self.executor, _filter_existing, ts_codes, target_date)
        if not needed_codes:
            logger.info(f"All requested {len(ts_codes)} codes already have up-to-date indicators. Skipping.")
            return

        logger.info(f"On-demand calculation for {len(needed_codes)}/{len(ts_codes)} codes...")
        
        results = []
        semaphore = asyncio.Semaphore(30)
        
        async def _calc(code):
            async with semaphore:
                # 如果是 force_full，则 return_full_history=True
                # 如果 force_recalc_today 为 True，则 local_only=False (获取实时数据)
                local_only = not force_recalc_today
                return await self._calculate_single_stock(code, target_date_str, force_no_cache=force_full, return_full_history=force_full, local_only=local_only)
        
        tasks = [_calc(code) for code in needed_codes]
        results = await asyncio.gather(*tasks)
        
        # 过滤并平坦化结果 (因为 _calculate_single_stock 现在返回的是列表)
        valid_results = []
        for r in results:
            if r:
                valid_results.extend(r)
        
        if valid_results:
            await asyncio.get_event_loop().run_in_executor(self.executor, self._save_to_db, valid_results)
            logger.info(f"On-demand calculation finished for {len(valid_results)} records across {len(needed_codes)} codes.")

    async def calculate_intraday_indicators(self):
        def _load_focus_codes() -> list[str]:
            db = SessionLocal()
            try:
                from app.models.stock_models import Position, TradingPlan

                today = date.today()
                position_codes = [
                    str(r[0])
                    for r in db.query(Position.ts_code).filter(Position.vol > 0).all()
                    if r and r[0]
                ]
                plan_codes = [
                    str(r[0])
                    for r in db.query(TradingPlan.ts_code)
                    .filter(TradingPlan.date == today, TradingPlan.executed.is_(False))
                    .all()
                    if r and r[0]
                ]

                codes = set(position_codes + plan_codes)
                return [c for c in codes if c]
            finally:
                db.close()

        try:
            focus_codes = await asyncio.to_thread(_load_focus_codes)
        except Exception:
            focus_codes = []

        try:
            from app.services.market.market_data_service import market_data_service

            turnover_codes = await asyncio.wait_for(
                market_data_service.get_market_turnover_top_codes(top_n=200),
                timeout=20.0,
            )
        except Exception:
            turnover_codes = []

        codes_set = set()
        for c in (turnover_codes or []):
            if c:
                codes_set.add(str(c))
        for c in (focus_codes or []):
            if c:
                codes_set.add(str(c))

        codes = [c for c in codes_set if c]
        if not codes:
            return

        today_str = datetime.now().strftime('%Y-%m-%d')
        await self.calculate_for_codes(codes, trade_date=today_str, force_recalc_today=True)

    async def calculate_all_indicators(self, trade_date=None, force_full=False):
        """
        批量计算所有股票指标并存入数据库
        :param trade_date: 基准日期，默认最新交易日
        :param force_full: 是否强制全量重算（忽略增量逻辑）
        """
        logger.info(f"Starting {'full' if force_full else 'incremental'} indicator calculation...")
        
        # 1. 获取所有股票代码、指数代码和行业板块
        def _get_ts_codes():
            db = SessionLocal()
            try:
                # 1.1 股票 (只获取本地有的股票)
                stock_rows = db.query(Stock.ts_code).all()
                stocks = []
                for row in stock_rows:
                    stocks.append(row[0])
                
                # 1.2 指数 (只获取本地有的指数)
                from app.services.data_sync import MAJOR_INDICES
                from app.models.stock_models import DailyBar
                from sqlalchemy import distinct
                
                # 检查 DailyBar 中实际存在的指数代码
                index_rows: list[Any] = list(db.query(distinct(DailyBar.ts_code)).filter(DailyBar.ts_code.in_(MAJOR_INDICES)).all())
                existing_indices = []
                for row in index_rows:
                    existing_indices.append(row[0])
                
                # 1.3 行业板块 (从 IndustryData 获取实际存在的行业)
                from app.models.stock_models import IndustryData
                from sqlalchemy import distinct
                
                industry_rows: list[Any] = list(db.query(distinct(IndustryData.industry)).all())
                existing_industries = []
                for row in industry_rows:
                    if row[0]:
                        existing_industries.append(f"IND_{row[0]}")
                logger.info(f"Found {len(existing_industries)} industries with data in IndustryData.")
                
                return list(set(stocks + existing_indices + existing_industries))
            finally:
                db.close()
        
        ts_codes = await asyncio.get_event_loop().run_in_executor(self.executor, _get_ts_codes)
        if not ts_codes:
            logger.warning("No stocks found in DB.")
            return

        # 2. 获取目标日期
        target_date_str = trade_date if trade_date else await data_provider.get_last_trade_date()
        target_date = datetime.strptime(target_date_str.replace('-', ''), '%Y%m%d').date()

        # 3. 增量逻辑过滤
        final_ts_codes = ts_codes
        if not force_full:
            def _filter_needed(codes, t_date):
                db = SessionLocal()
                try:
                    # 性能优化：不再获取全量指标历史，只获取每个标的最新的指标记录
                    from sqlalchemy import func
                    subquery = db.query(
                        StockIndicator.ts_code,
                        func.max(StockIndicator.trade_date).label('max_date')
                    ).filter(StockIndicator.ts_code.in_(codes)).group_by(StockIndicator.ts_code).subquery()

                    latest_records = db.query(StockIndicator).join(
                        subquery,
                        (StockIndicator.ts_code == subquery.c.ts_code) & 
                        (StockIndicator.trade_date == subquery.c.max_date)
                    ).all()
                    
                    existing_map = {r.ts_code: (r.trade_date, r.adj_factor) for r in latest_records}
                    
                    # 获取最新一天的复权因子
                    from app.models.stock_models import DailyBar
                    latest_adjs = db.query(DailyBar.ts_code, DailyBar.adj_factor).filter(
                        DailyBar.trade_date == t_date
                    ).all()
                    latest_adj_map = {r.ts_code: r.adj_factor for r in latest_adjs}
                    
                    from app.services.market.market_data_service import market_data_service
                    
                    needed = {} # ts_code -> force_no_cache
                    for code in codes:
                        is_special = market_data_service._is_index_or_industry(code)
                        
                        if code not in existing_map:
                            needed[code] = True # 新股票/指数/板块强制全量
                            continue
                        
                        last_date, last_adj = existing_map[code]
                        
                        # 重要修复：如果目标日期已经存在指标，则跳过计算 (断点续传的关键)
                        # 只有在强制全量或者复权因子变动的情况下才重算
                        if last_date >= t_date:
                            # 即使日期够了，如果复权因子变了，也得重算
                            curr_adj = latest_adj_map.get(code, 1.0 if is_special else None)
                            if not is_special and curr_adj is not None and abs((last_adj or 0) - curr_adj) > 1e-6:
                                logger.info(f"Detected adj_factor change for {code} even with up-to-date date. Triggering full recalculation.")
                                needed[code] = True
                            continue
                        
                        # 优先检查复权因子变动
                        curr_adj = latest_adj_map.get(code, 1.0 if is_special else None)
                        if not is_special and curr_adj is not None and abs((last_adj or 0) - curr_adj) > 1e-6:
                            logger.info(f"Detected adj_factor change for {code}: {last_adj} -> {curr_adj}. Triggering full recalculation.")
                            needed[code] = True 
                        else:
                            needed[code] = False # 增量计算
                    
                    return needed
                finally:
                    db.close()

            needed_map = await asyncio.get_event_loop().run_in_executor(self.executor, _filter_needed, ts_codes, target_date)
            logger.info(f"Incremental mode: {len(needed_map)}/{len(ts_codes)} stocks need update.")
        else:
            needed_map = {code: True for code in ts_codes}
            logger.info(f"Force full mode: Processing all {len(ts_codes)} stocks.")

        if not needed_map:
            logger.info("All indicators are up to date.")
            return

        results = []
        # 使用 Semaphore 限制并发 (提高到 30 以加速 I/O)
        semaphore = asyncio.Semaphore(30) 
        
        async def _calc_with_limit(code, force_no_cache):
            async with semaphore:
                try:
                    # logger.info(f"Starting calculation for {code}...")
                    start_t = datetime.now()
                    res = await asyncio.wait_for(self._calculate_single_stock(code, target_date_str, force_no_cache), timeout=120.0)
                    elapsed = (datetime.now() - start_t).total_seconds()
                    if elapsed > 5.0:
                        logger.info(f"Slow calculation for {code}: {elapsed:.2f}s")
                    return res
                except asyncio.TimeoutError:
                    logger.warning(f"Calculation timeout for {code}")
                    return None
                except Exception as e:
                    logger.error(f"Error calculating {code}: {e}")
                    return None
        
        tasks = [_calc_with_limit(code, force) for code, force in needed_map.items()]
        
        # 分批处理并实时保存到数据库 (减小批次大小以降低瞬时 CPU/IO 压力)
        batch_size = 10 
        total_processed = 0
        total_batches = (len(tasks) + batch_size - 1) // batch_size
        
        logger.info(f"Starting batch indicator calculation for {len(needed_map)} stocks in {total_batches} batches...")
        
        for i in range(0, len(tasks), batch_size):
            # 检查是否有高优先级的 UI 请求正在等待数据库，如果有则短暂让出 CPU
            # 简单的让出控制：每批次处理后休眠一小段时间
            if i > 0:
                await asyncio.sleep(0.5) # 让出 0.5 秒给其他请求

            batch = tasks[i:i + batch_size]
            current_batch = i // batch_size + 1
            logger.info(f"Processing batch {current_batch}/{total_batches}...")
            
            try:
                batch_results = await asyncio.gather(*batch)
                
                # 过滤并平坦化结果
                valid_results = []
                for res in batch_results:
                    if res:
                        valid_results.extend(res)
                
                if valid_results:
                    # 将保存操作包装在更强的错误处理中
                    try:
                        await asyncio.get_event_loop().run_in_executor(self.executor, self._save_to_db, valid_results)
                        results.extend(valid_results)
                    except Exception as e:
                        logger.error(f"Failed to save batch {current_batch} to DB: {e}")
                
                total_processed += len(batch)
                logger.info(f"Progress: {total_processed}/{len(needed_map)} stocks processed.")
            except Exception as e:
                logger.error(f"Error processing batch {current_batch}: {e}")
                total_processed += len(batch)

        # 6. 保存缓存到磁盘 (性能优化的关键)
        await asyncio.get_event_loop().run_in_executor(self.executor, technical_indicators.save_cache)
        
        logger.info(f"Batch indicator calculation complete. {len(final_ts_codes)} codes processed.")

    def _save_to_db(self, results: list):
        if not results:
            return
            
        import time
        max_retries = 8 # 增加重试次数
        
        # 针对补全任务优化：按股票和子批次进行事务提交
        # 这样可以显著减小单个事务的大小，避免长时间锁定数据库
        data_by_code: dict[str, list[dict[str, Any]]] = {}
        for r in results:
            code = r['ts_code']
            if code not in data_by_code:
                data_by_code[code] = []
            data_by_code[code].append(r)
        
        total_stocks = len(data_by_code)
        for idx, (ts_code, items) in enumerate(data_by_code.items()):
            # 分批处理单只股票的日期，防止 in_ 语句过长，同时也作为事务边界
            sub_batch_size = 200 # 适中大小，兼顾性能和响应性
            for j in range(0, len(items), sub_batch_size):
                sub_batch = items[j:j + sub_batch_size]
                trade_dates = [r['trade_date'] for r in sub_batch]
                
                # 每一小批次的写入使用互斥锁保护
                # 注意：锁的范围仅限于实际的 DB 操作
                with self._db_lock:
                    for attempt in range(max_retries):
                        db = SessionLocal()
                        try:
                            from app.models.stock_models import StockIndicator
                            
                            # 在同一个事务中执行删除和插入
                            db.query(StockIndicator).filter(
                                StockIndicator.ts_code == ts_code,
                                StockIndicator.trade_date.in_(trade_dates)
                            ).delete(synchronize_session=False)
                            
                            from typing import cast
                            db.bulk_insert_mappings(cast(Any, StockIndicator), sub_batch)
                            
                            # 每一小批次提交一次，释放锁
                            db.commit()
                            break # 成功则跳出重试循环
                        except Exception as e:
                            db.rollback()
                            if "locked" in str(e).lower() and attempt < max_retries - 1:
                                time.sleep(0.5 * (attempt + 1))
                                continue
                            logger.error(f"Database error in _save_to_db for {ts_code}: {e}")
                            raise e
                        finally:
                            db.close()
                
                # 在锁外休眠，给其他请求（如 UI 刷新）留出数据库处理空隙
                if total_stocks > 5:
                    time.sleep(0.02) 
            
            if (idx + 1) % 5 == 0:
                logger.info(f"Saving progress: {idx + 1}/{total_stocks} stocks indicators saved.")

    async def backfill_historical_indicators(self, ts_codes: list = None):
        """
        补全历史指标数据（最近5年）
        """
        logger.info("Starting historical indicator backfill...")
        
        if not ts_codes:
            def _get_all_codes():
                db = SessionLocal()
                try:
                    return [r[0] for r in db.query(Stock.ts_code).all()]
                finally:
                    db.close()
            ts_codes = await asyncio.get_event_loop().run_in_executor(self.executor, _get_all_codes)

        if not ts_codes:
            logger.warning("No codes found for backfill.")
            return

        target_date_str = await data_provider.get_last_trade_date()
        
        # 使用 Semaphore 限制并发
        semaphore = asyncio.Semaphore(10) # 补全历史压力大，并发设小一点
        
        async def _backfill_single(code):
            async with semaphore:
                try:
                    logger.info(f"Backfilling {code}...")
                    # return_full_history=True 会返回过去5年的所有指标
                    res = await self._calculate_single_stock(code, target_date_str, return_full_history=True)
                    if res:
                        await asyncio.get_event_loop().run_in_executor(self.executor, self._save_to_db, res)
                        return len(res)
                    return 0
                except Exception as e:
                    logger.error(f"Error backfilling {code}: {e}")
                    return 0

        total_records = 0
        for i in range(0, len(ts_codes), 20): # 每 20 个标的一组
            batch_codes = ts_codes[i:i+20]
            tasks = [_backfill_single(code) for code in batch_codes]
            batch_results = await asyncio.gather(*tasks)
            total_records += sum(batch_results)
            logger.info(f"Backfill Progress: {i + len(batch_codes)}/{len(ts_codes)} codes. Total records saved: {total_records}")

        logger.info(f"Historical indicator backfill complete. Total records: {total_records}")

indicator_service = IndicatorService()
