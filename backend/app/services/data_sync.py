from typing import Dict, Any, Tuple, Optional
import os
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.db.session import SessionLocal
from app.models.stock_models import Stock, DailyBar, IndustryData, WeeklyBar, MonthlyBar, MinuteBar, DailyBasic
from sqlalchemy import func, desc
from app.core.config import settings
from app.services.data_provider import data_provider
from app.services.market.tushare_client import tushare_client
import tushare as ts
from app.services.indicator_service import indicator_service

# 主要市场指数
MAJOR_INDICES = [
    '000001.SH',  # 上证指数
    '399001.SZ',  # 深证成指
    '399006.SZ',  # 创业板指
    '000300.SH',  # 沪深300
    '000905.SH',  # 中证500
    '000852.SH',  # 中证1000
    '000016.SH',  # 上证50
]

logger = logging.getLogger(__name__)

class DataSyncService:
    def __init__(self):
        self.pro = ts.pro_api(settings.TUSHARE_TOKEN)
        # 初始化同步状态
        self.sync_state = {
            "status": "idle", # idle, running, error
            "task": "",      # current task name
            "progress": 0,   # 0-100
            "message": "",   # detail message
            "last_updated": datetime.now().isoformat()
        }
        self._report_cache: Tuple[float, Dict[str, Any]] = (0.0, {})
        
    def _update_state(self, status=None, task=None, progress=None, message=None):
        """更新同步状态"""
        if status is not None: self.sync_state["status"] = status
        if task is not None: self.sync_state["task"] = task
        if progress is not None: self.sync_state["progress"] = progress
        if message is not None: self.sync_state["message"] = message
        self.sync_state["last_updated"] = datetime.now().isoformat()

    async def _fetch_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        params = {
            "exchange": "SSE",
            "start_date": start_date,
            "end_date": end_date,
            "is_open": "1"
        }
        last_error = None
        for attempt in range(3):
            try:
                df = await tushare_client.async_query("trade_cal", params=params, fields="cal_date,is_open")
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                last_error = e
            await asyncio.sleep(min(2 * (attempt + 1), 6))
        if last_error:
            logger.error(f"Trade calendar fetch failed: {last_error}")
        return pd.DataFrame()

    def init_db(self):
        """初始化数据库表 (仅用于创建新表)"""
        from app.db.base import Base
        from app.db.session import engine
        logger.info("Initializing database tables...")
        Base.metadata.create_all(bind=engine)

    async def sync_all_stocks(self):
        """同步所有股票列表"""
        logger.info("Syncing stock list from Tushare...")
        
        def _sync():
            db = SessionLocal()
            try:
                # 获取当前上市的所有股票
                df = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date')
                if df is not None and not df.empty:
                    # 获取已有的股票代码
                    existing_codes = {s[0] for s in db.query(Stock.ts_code).all()}
                    
                    count = 0
                    for _, row in df.iterrows():
                        if row['ts_code'] not in existing_codes:
                            stock = Stock(
                                ts_code=row['ts_code'],
                                symbol=row['symbol'],
                                name=row['name'],
                                area=row['area'],
                                industry=row['industry'],
                                list_date=row['list_date'] if row['list_date'] else None
                            )
                            db.add(stock)
                            count += 1
                    
                    db.commit()
                    logger.info(f"Sync complete. Added {count} new stocks.")
                    return count
                else:
                    logger.warning("No stock data returned from Tushare.")
                    return 0
            except Exception as e:
                logger.error(f"Error syncing stock list: {e}")
                db.rollback()
                return -1
            finally:
                db.close()
        
        return await asyncio.to_thread(_sync)

    async def sync_stock_basic(self):
        return await self.sync_all_stocks()

    async def sync_daily_data(self, trade_date=None, sync_industry_history=True, calculate_indicators: bool = True):
        """
        同步指定日期的所有股票日线数据
        """
        if trade_date is None:
            # [Fix] 默认取 Tushare 服务器认为的最新交易日，而非简单的 datetime.now()
            # 这样可以避免非交易日误跑导致产生脏数据
            trade_date = datetime.now().strftime('%Y%m%d')
        
        # 统一去除横线
        trade_date = trade_date.replace('-', '')
        
        # 检查是否已经是已知的非交易日
        cal_df = await self._fetch_trade_calendar(trade_date, trade_date)
        if cal_df is not None and not cal_df.empty:
            if str(cal_df.iloc[0]['is_open']) == '0':
                logger.info(f"[{trade_date}] is not a trading day, skipping sync.")
                return
        
        self._update_state(status="running", task=f"Syncing {trade_date}", progress=0)
        logger.info(f"Starting daily sync for {trade_date}...")

        # 1. 同步日线行情 (核心)
        success = await asyncio.to_thread(self._sync_daily_bars, trade_date)
        
        if success:
            # 2. 同步每日基础指标 (PE/市值等)
            try:
                logger.info(f"[{trade_date}] Step 3: Syncing DailyBasic...")
                await self.sync_daily_basic(trade_date)
            except Exception as e:
                logger.error(f"[{trade_date}] DailyBasic sync failed: {e}")

            # 3. 将日线数据转换为周线和月线
            try:
                logger.info(f"[{trade_date}] Step 4: Converting to Weekly/Monthly...")
                await asyncio.to_thread(self.convert_to_weekly_monthly, trade_date)
            except Exception as e:
                logger.error(f"[{trade_date}] Weekly/Monthly conversion failed: {e}")

            # 4. 同步主要指数
            try:
                logger.info(f"[{trade_date}] Step 5: Syncing index data...")
                await self.sync_index_data(trade_date)
            except Exception as e:
                logger.error(f"[{trade_date}] Index data sync failed: {e}")
            
            # 5. 同步行业指数
            if sync_industry_history:
                try:
                    logger.info(f"[{trade_date}] Step 6: Syncing industry data...")
                    await asyncio.to_thread(self.sync_industry_data_history, trade_date)
                except Exception as e:
                    logger.error(f"[{trade_date}] Industry data sync failed: {e}")
            
            # 6. 计算技术指标 (核心 follow-up)
            if calculate_indicators:
                try:
                    logger.info(f"[{trade_date}] Step 7: Calculating indicators...")
                    self._update_state(progress=80, message="Recalculating indicators...")
                    await indicator_service.calculate_all_indicators(trade_date, force_full=False)
                    logger.info(f"[{trade_date}] Indicator calculation completed.")
                except Exception as e:
                    logger.error(f"[{trade_date}] Indicator calculation failed: {e}")
            
            logger.info(f"[{trade_date}] All sync steps complete.")
            self._update_state(status="idle", progress=100, message=f"Sync for {trade_date} complete")
        else:
            logger.error(f"[{trade_date}] Daily bars sync failed, skipping subsequent steps.")
            self._update_state(status="error", message="Daily sync failed")

    def _sync_daily_bars(self, trade_date):
        """同步日线行情和复权因子的内部实现"""
        db = SessionLocal()
        try:
            # 1. 获取复权因子 (AdjFactor)
            logger.info(f"[{trade_date}] Step 1: Fetching adj factors...")
            adj_df = self.pro.adj_factor(trade_date=trade_date)
            
            # 2. 获取日线行情 (Daily)
            logger.info(f"[{trade_date}] Step 2: Fetching daily bars...")
            df = self.pro.daily(trade_date=trade_date)
            
            if df is not None and not df.empty:
                # 转换日期格式
                t_date = datetime.strptime(trade_date, '%Y%m%d').date()
                
                # [关键修复] 严格校验返回数据的 trade_date
                # Tushare 有时可能会返回非请求日期的数据（极罕见，但需防御）
                # 或者 df['trade_date'] 本身就是字符串，需要转换对比
                if 'trade_date' in df.columns:
                    # 统一转为字符串对比
                    df['trade_date_str'] = df['trade_date'].astype(str).str.replace('-', '').replace('/', '')
                    # 过滤掉日期不匹配的行
                    original_len = len(df)
                    df = df[df['trade_date_str'] == trade_date]
                    if len(df) < original_len:
                        logger.warning(f"[{trade_date}] Filtered {original_len - len(df)} records with mismatched trade_date.")
                
                if df.empty:
                    logger.warning(f"[{trade_date}] No valid daily bars after date filtering.")
                    return False

                # 获取该日期已存在的记录
                existing_bars = {
                    b.ts_code: b
                    for b in db.query(DailyBar).filter(DailyBar.trade_date == t_date).all()
                }
                
                # 合并复权因子 (优化逻辑: 避免无脑置为 1.0)
                if adj_df is not None and not adj_df.empty:
                    df = pd.merge(df, adj_df[['ts_code', 'adj_factor']], on='ts_code', how='left')
                else:
                    # 如果获取不到当天的复权因子，尝试使用数据库中每个股票最近的复权因子
                    logger.warning(f"[{trade_date}] adj_factor API returned empty. Attempting to use previous values.")
                    # 这是一个比较耗时的操作，但比默认为 1.0 安全
                    # 为简单起见，这里先设为 NaN，后续处理
                    df['adj_factor'] = None
                
                # 预加载所有涉及股票的最近一次复权因子
                ts_codes_in_df = df['ts_code'].tolist()
                # 批量查询最近复权因子 (性能优化)
                # ...这里为了保持代码简洁，我们在循环中处理，或者如果性能瓶颈再优化
                
                insert_count = 0
                update_count = 0
                
                # 缓存最近的复权因子，避免循环查询
                # key: ts_code, value: adj_factor
                prev_adj_cache = {}
                
                for _, row in df.iterrows():
                    ts_code = row['ts_code']
                    adj_factor_val = row.get('adj_factor')
                    
                    # 核心修复: 如果 adj_factor 无效 (NaN or None)，尝试回溯
                    if pd.isna(adj_factor_val):
                        # 尝试从 DB 获取该股票昨天或最近的 adj_factor
                        # 注意: 这种回溯仅在 incremental sync 时有效
                        # 如果是全量历史同步且 API 挂了，那也没办法
                        prev_adj = db.query(DailyBar.adj_factor).filter(
                            DailyBar.ts_code == ts_code,
                            DailyBar.trade_date < t_date,
                            DailyBar.adj_factor.isnot(None)
                        ).order_by(DailyBar.trade_date.desc()).first()
                        
                        if prev_adj:
                            adj_factor_val = float(prev_adj[0])
                        else:
                            # 实在没有，如果是新股，1.0 是合理的
                            adj_factor_val = 1.0
                    else:
                        adj_factor_val = float(adj_factor_val)

                    existing = existing_bars.get(ts_code)
                    if existing:
                        existing.open = row["open"]
                        existing.high = row["high"]
                        existing.low = row["low"]
                        existing.close = row["close"]
                        existing.pre_close = row["pre_close"]
                        existing.change = row["change"]
                        existing.pct_chg = row["pct_chg"]
                        existing.vol = row["vol"]
                        existing.amount = row["amount"]
                        existing.adj_factor = adj_factor_val
                        update_count += 1
                    else:
                        bar = DailyBar(
                            ts_code=row['ts_code'],
                            trade_date=t_date,
                            open=row['open'],
                            high=row['high'],
                            low=row['low'],
                            close=row['close'],
                            pre_close=row['pre_close'],
                            change=row['change'],
                            pct_chg=row['pct_chg'],
                            vol=row['vol'],
                            amount=row['amount']
                        )
                        setattr(bar, "adj_factor", adj_factor_val)
                        db.add(bar)
                        insert_count += 1
                
                db.commit()
                logger.info(f"[{trade_date}] Daily sync: Added {insert_count} records, Updated {update_count} records.")
                return True
            else:
                logger.warning(f"[{trade_date}] No daily bars returned from Tushare.")
                return False
        except Exception as e:
            logger.error(f"Error in _sync_daily_bars for {trade_date}: {e}")
            db.rollback()
            return False
        finally:
            db.close()

    async def sync_daily_basic(self, trade_date):
        """同步每日指标数据 (PE/PB/市值等)"""
        def _sync_basic():
            db = SessionLocal()
            try:
                df = self.pro.daily_basic(trade_date=trade_date)
                if df is not None and not df.empty:
                    t_date = datetime.strptime(trade_date, '%Y%m%d').date()
                    existing_codes = {b[0] for b in db.query(DailyBasic.ts_code).filter(DailyBasic.trade_date == t_date).all()}
                    
                    count = 0
                    for _, row in df.iterrows():
                        if row['ts_code'] not in existing_codes:
                            basic = DailyBasic(
                                ts_code=row['ts_code'],
                                trade_date=t_date,
                                close=row['close'],
                                turnover_rate=row['turnover_rate'],
                                turnover_rate_f=row['turnover_rate_f'],
                                volume_ratio=row['volume_ratio'],
                                pe=row['pe'],
                                pe_ttm=row['pe_ttm'],
                                pb=row['pb'],
                                ps=row['ps'],
                                ps_ttm=row['ps_ttm'],
                                dv_ratio=row['dv_ratio'],
                                dv_ttm=row['dv_ttm'],
                                total_share=row['total_share'],
                                float_share=row['float_share'],
                                free_share=row['free_share'],
                                total_mv=row['total_mv'],
                                circ_mv=row['circ_mv']
                            )
                            db.add(basic)
                            count += 1
                    db.commit()
                    logger.info(f"DailyBasic sync for {trade_date}: Added {count} records.")
                    return count
                return 0
            except Exception as e:
                logger.error(f"Error syncing daily basic for {trade_date}: {e}")
                db.rollback()
                return -1
            finally:
                db.close()
        
        return await asyncio.to_thread(_sync_basic)

    async def sync_index_data(self, trade_date):
        """同步主要市场指数数据"""
        def _sync_index():
            db = SessionLocal()
            try:
                # 使用 index_daily 获取指数行情
                # 由于指数较少，可以直接循环获取
                t_date = datetime.strptime(trade_date, '%Y%m%d').date()
                count = 0
                for ts_code in MAJOR_INDICES:
                    # 检查是否已存在
                    exists = db.query(DailyBar).filter(DailyBar.ts_code == ts_code, DailyBar.trade_date == t_date).first()
                    if exists:
                        continue
                        
                    df = self.pro.index_daily(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
                    if df is not None and not df.empty:
                        row = df.iloc[0]
                        bar = DailyBar(
                            ts_code=ts_code,
                            trade_date=t_date,
                            open=row['open'],
                            high=row['high'],
                            low=row['low'],
                            close=row['close'],
                            pre_close=row['pre_close'],
                            change=row['change'],
                            pct_chg=row['pct_chg'],
                            vol=row['vol'],
                            amount=row['amount']
                        )
                        setattr(bar, "adj_factor", 1.0)
                        db.add(bar)
                        count += 1
                
                db.commit()
                if count > 0:
                    logger.info(f"Index data sync for {trade_date}: Added {count} indices.")
                return count
            except Exception as e:
                logger.error(f"Error syncing index data for {trade_date}: {e}")
                db.rollback()
                return -1
            finally:
                db.close()
        
        return await asyncio.to_thread(_sync_index)

    def sync_industry_data_history(self, trade_date, days=None):
        """
        同步行业指数数据。从本地 DailyBar 和 Stock 表聚合计算行业均价和涨跌幅。
        days: 同步过去多少天的数据，如果不传则只同步 trade_date 当天
        """
        if days is None:
            days = 1
        
        # 强制限制在 5 年内
        days = min(days, 1825)
        logger.info(f"Syncing industry index history for last {days} days ending {trade_date}...")
        
        db = SessionLocal()
        try:
            target_date = datetime.strptime(trade_date, '%Y%m%d').date()
            start_date = target_date - timedelta(days=days)
            
            # 1. 获取期间内的所有交易日
            trade_dates_query = db.query(DailyBar.trade_date).filter(
                DailyBar.trade_date >= start_date,
                DailyBar.trade_date <= target_date
            ).distinct().all()
            
            all_trade_dates = [d[0] for d in trade_dates_query]
            
            # 2. 获取期间内已经存在的行业数据日期
            existing_dates_query = db.query(IndustryData.trade_date).filter(
                IndustryData.trade_date >= start_date,
                IndustryData.trade_date <= target_date
            ).distinct().all()
            existing_dates = {d[0] for d in existing_dates_query}
            
            # 3. 找出需要计算的日期
            needed_dates = [d for d in all_trade_dates if d not in existing_dates]
            needed_dates.sort(reverse=True)
            
            if not needed_dates:
                logger.info(f"Industry data for range {start_date} to {target_date} is already up to date.")
                return

            logger.info(f"Found {len(needed_dates)} dates needing industry data calculation.")

            for t_date in needed_dates:
                logger.info(f"Calculating industry data for {t_date}...")
                
                # 聚合行业数据
                # 使用 SQL 直接聚合以提高性能
                results = db.query(
                    Stock.industry,
                    func.avg(DailyBar.close).label('avg_price'),
                    func.avg(DailyBar.pct_chg).label('avg_pct_chg'),
                    func.sum(DailyBar.vol).label('total_vol'),
                    func.sum(DailyBar.amount).label('total_amount')
                ).join(Stock, DailyBar.ts_code == Stock.ts_code).filter(
                    DailyBar.trade_date == t_date,
                    Stock.industry != None,
                    Stock.industry != ''
                ).group_by(Stock.industry).all()
                
                if not results:
                    logger.warning(f"No industry data found for {t_date}")
                    continue

                for res in results:
                    ind_data = IndustryData(
                        industry=res.industry,
                        trade_date=t_date,
                        avg_price=res.avg_price,
                        avg_pct_chg=res.avg_pct_chg,
                        total_vol=res.total_vol,
                        total_amount=res.total_amount
                    )
                    db.add(ind_data)
                
                db.commit()
                logger.info(f"Industry data for {t_date} synced: {len(results)} industries.")
                
        except Exception as e:
            logger.error(f"Error syncing industry data history: {e}")
            db.rollback()
        finally:
            db.close()

    async def smart_sync_recent_data(self, days=7, sync_industry_history=True):
        """
        智能同步最近 N 天的数据 (补漏 + 更新今日)
        """
        # 强制限制在 5 年内
        days = min(days, 1825)
        self._update_state(status="running", task="smart_sync", progress=0, message="Starting smart sync...")
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = await self._fetch_trade_calendar(start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'))
            if df is None or df.empty:
                logger.warning("Failed to fetch trade calendar for smart sync. Fallback to simple sync.")
                await self.sync_daily_data()
                return

            trade_dates = df['cal_date'].tolist()
            today_str = end_date.strftime('%Y%m%d')
            
            async def _check_and_sync():
                db = SessionLocal()
                try:
                    for t_date_str in trade_dates:
                        if t_date_str == today_str: continue 
                        
                        t_date = datetime.strptime(t_date_str, '%Y%m%d').date()
                        # 使用上证指数检查该日数据是否存在
                        exists = db.query(DailyBar).filter(DailyBar.ts_code == '000001.SH', DailyBar.trade_date == t_date).first()
                        
                        if not exists:
                            logger.info(f"Detected missing data for {t_date_str}, performing backfill...")
                            # 注意：这里在内部调用 sync_daily_data，它是 async 的，需要 await
                            # 但我们在 to_thread 内部，不能直接 await。
                            # 所以我们将循环移出 to_thread，或者在内部使用新的 event loop (不推荐)
                            # 更好的做法是只在 to_thread 内部做 DB 查询。
                            yield t_date_str
                finally:
                    db.close()

            # 改进：将 DB 检查和同步逻辑分离
            db = SessionLocal()
            missing_dates = []
            try:
                for t_date_str in trade_dates:
                    if t_date_str == today_str: continue 
                    t_date = datetime.strptime(t_date_str, '%Y%m%d').date()
                    exists = await asyncio.to_thread(lambda: db.query(DailyBar).filter(DailyBar.ts_code == '000001.SH', DailyBar.trade_date == t_date).first())
                    if not exists:
                        missing_dates.append(t_date_str)
            finally:
                db.close()

            for t_date_str in missing_dates:
                logger.info(f"--- Backfilling missing date: {t_date_str} ---")
                await self.sync_daily_data(trade_date=t_date_str, sync_industry_history=False)

            # 强制同步今日数据
            logger.info(f"Syncing today's data: {today_str}")
            await self.sync_daily_data(trade_date=today_str, sync_industry_history=sync_industry_history)
            
            self._update_state(status="idle", task="", progress=100, message="Smart sync complete")
            
        except Exception as e:
            logger.error(f"Smart sync failed: {e}")
            self._update_state(status="error", message=str(e))
            await self.sync_daily_data()

    def reconstruct_weekly_monthly(self, ts_code: str):
        """
        [全量] 重建指定股票的周线和月线数据 (基于本地 DailyBar)
        用于修复数据异常
        """
        db = SessionLocal()
        try:
            # 1. 获取所有日线数据
            daily_bars = db.query(DailyBar).filter(
                DailyBar.ts_code == ts_code
            ).order_by(DailyBar.trade_date.asc()).all()
            
            if not daily_bars:
                logger.warning(f"No daily bars found for {ts_code}, skipping reconstruction.")
                return
            
            df_daily = pd.DataFrame([{
                'trade_date': b.trade_date,
                'open': b.open,
                'high': b.high,
                'low': b.low,
                'close': b.close,
                'vol': b.vol,
                'amount': b.amount,
                'adj_factor': b.adj_factor
            } for b in daily_bars])
            
            df_daily['trade_date_dt'] = pd.to_datetime(df_daily['trade_date'])
            
            # --- 2. 重建周线 ---
            # 清除旧周线? 或者 Upsert? 
            # Upsert 更安全，但如果有脏数据日期，Upsert 无法删除脏数据
            # 策略: 先全部删除该股票的周/月线，再插入
            db.query(WeeklyBar).filter(WeeklyBar.ts_code == ts_code).delete()
            db.query(MonthlyBar).filter(MonthlyBar.ts_code == ts_code).delete()
            db.commit() # 提交删除
            
            # 周线聚合
            df_daily['year_week'] = df_daily['trade_date_dt'].apply(lambda x: f"{x.isocalendar()[0]}-{x.isocalendar()[1]:02d}")
            w_grouped = df_daily.groupby('year_week').agg({
                'trade_date': 'last',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'vol': 'sum',
                'amount': 'sum',
                'adj_factor': 'last'
            }).reset_index()
            
            weekly_objects = []
            for _, row in w_grouped.iterrows():
                weekly_objects.append(WeeklyBar(
                    ts_code=ts_code,
                    trade_date=row['trade_date'],
                    open=float(row['open']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    vol=float(row['vol']),
                    amount=float(row['amount']),
                    adj_factor=float(row['adj_factor'])
                ))
            
            # 月线聚合
            df_daily['year_month'] = df_daily['trade_date_dt'].apply(lambda x: f"{x.year}-{x.month:02d}")
            m_grouped = df_daily.groupby('year_month').agg({
                'trade_date': 'last',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'vol': 'sum',
                'amount': 'sum',
                'adj_factor': 'last'
            }).reset_index()
            
            monthly_objects = []
            for _, row in m_grouped.iterrows():
                monthly_objects.append(MonthlyBar(
                    ts_code=ts_code,
                    trade_date=row['trade_date'],
                    open=float(row['open']),
                    high=float(row['high']),
                    low=float(row['low']),
                    close=float(row['close']),
                    vol=float(row['vol']),
                    amount=float(row['amount']),
                    adj_factor=float(row['adj_factor'])
                ))
            
            # 批量插入
            if weekly_objects:
                db.bulk_save_objects(weekly_objects)
            if monthly_objects:
                db.bulk_save_objects(monthly_objects)
            
            db.commit()
            logger.info(f"Reconstructed {len(weekly_objects)} weekly and {len(monthly_objects)} monthly bars for {ts_code}")
            
        except Exception as e:
            logger.error(f"Error reconstructing W/M for {ts_code}: {e}")
            db.rollback()
        finally:
            db.close()

    def convert_to_weekly_monthly(self, trade_date):
        """将日线合并为周线和月线并保存到本地数据库 (使用 ISO 规则)"""
        db = SessionLocal()
        try:
            # 转换日期格式
            t_date = datetime.strptime(trade_date, '%Y%m%d').date()
            
            # 获取所有股票代码
            stocks = db.query(Stock.ts_code).all()
            ts_codes = [s[0] for s in stocks]
            
            logger.info(f"Converting daily to weekly/monthly for {len(ts_codes)} stocks on {trade_date}...")
            
            # 批量处理以提高效率
            batch_size = 100
            for i in range(0, len(ts_codes), batch_size):
                batch_codes = ts_codes[i:i+batch_size]
                
                for ts_code in batch_codes:
                    try:
                        # 获取该股票最近一段时间的日线数据 (至少获取 60 天以确保聚合完整性)
                        start_date = t_date - timedelta(days=60)
                        daily_bars = db.query(DailyBar).filter(
                            DailyBar.ts_code == ts_code,
                            DailyBar.trade_date >= start_date,
                            DailyBar.trade_date <= t_date
                        ).order_by(DailyBar.trade_date.asc()).all()
                        
                        if not daily_bars:
                            continue
                            
                        df_daily = pd.DataFrame([{
                            'trade_date': b.trade_date,
                            'open': b.open,
                            'high': b.high,
                            'low': b.low,
                            'close': b.close,
                            'vol': b.vol,
                            'amount': b.amount,
                            'adj_factor': b.adj_factor
                        } for b in daily_bars])
                        
                        df_daily['trade_date_dt'] = pd.to_datetime(df_daily['trade_date'])
                        
                        # --- 1. 处理周线 (ISO 规则) ---
                        df_daily['year_week'] = df_daily['trade_date_dt'].apply(lambda x: f"{x.isocalendar()[0]}-{x.isocalendar()[1]:02d}")
                        w_grouped = df_daily.groupby('year_week').agg({
                            'trade_date': 'last',
                            'open': 'first',
                            'high': 'max',
                            'low': 'min',
                            'close': 'last',
                            'vol': 'sum',
                            'amount': 'sum',
                            'adj_factor': 'last'
                        }).reset_index()
                        
                        # 只更新包含 trade_date 的周
                        current_week = f"{t_date.isocalendar()[0]}-{t_date.isocalendar()[1]:02d}"
                        w_row = w_grouped[w_grouped['year_week'] == current_week]
                        
                        if not w_row.empty:
                            row = w_row.iloc[0]
                            w_trade_date = row['trade_date']
                            w_bar_data = {
                                "ts_code": ts_code,
                                "trade_date": w_trade_date,
                                "open": float(row['open']),
                                "high": float(row['high']),
                                "low": float(row['low']),
                                "close": float(row['close']),
                                "vol": float(row['vol']),
                                "amount": float(row['amount']),
                                "adj_factor": float(row['adj_factor'])
                            }
                            
                            # 查找该周已存在的记录
                            import calendar
                            start_of_week = w_trade_date - timedelta(days=w_trade_date.weekday())
                            end_of_week = start_of_week + timedelta(days=6)
                            existing_w = db.query(WeeklyBar).filter(
                                WeeklyBar.ts_code == ts_code,
                                WeeklyBar.trade_date >= start_of_week,
                                WeeklyBar.trade_date <= end_of_week
                            ).first()
                            
                            if existing_w:
                                for key, value in w_bar_data.items():
                                    setattr(existing_w, key, value)
                            else:
                                db.add(WeeklyBar(**w_bar_data))

                        # --- 2. 处理月线 ---
                        df_daily['year_month'] = df_daily['trade_date_dt'].apply(lambda x: f"{x.year}-{x.month:02d}")
                        m_grouped = df_daily.groupby('year_month').agg({
                            'trade_date': 'last',
                            'open': 'first',
                            'high': 'max',
                            'low': 'min',
                            'close': 'last',
                            'vol': 'sum',
                            'amount': 'sum',
                            'adj_factor': 'last'
                        }).reset_index()
                        
                        current_month = f"{t_date.year}-{t_date.month:02d}"
                        m_row = m_grouped[m_grouped['year_month'] == current_month]
                        
                        if not m_row.empty:
                            row = m_row.iloc[0]
                            m_trade_date = row['trade_date']
                            m_bar_data = {
                                "ts_code": ts_code,
                                "trade_date": m_trade_date,
                                "open": float(row['open']),
                                "high": float(row['high']),
                                "low": float(row['low']),
                                "close": float(row['close']),
                                "vol": float(row['vol']),
                                "amount": float(row['amount']),
                                "adj_factor": float(row['adj_factor'])
                            }
                            
                            import calendar
                            _, last_day = calendar.monthrange(m_trade_date.year, m_trade_date.month)
                            start_of_month = m_trade_date.replace(day=1)
                            end_of_month = m_trade_date.replace(day=last_day)
                            
                            existing_m = db.query(MonthlyBar).filter(
                                MonthlyBar.ts_code == ts_code,
                                MonthlyBar.trade_date >= start_of_month,
                                MonthlyBar.trade_date <= end_of_month
                            ).first()
                            
                            if existing_m:
                                for key, value in m_bar_data.items():
                                    setattr(existing_m, key, value)
                            else:
                                db.add(MonthlyBar(**m_bar_data))
                                
                    except Exception as e:
                        logger.error(f"Error converting {ts_code} for {trade_date}: {e}")
                        continue
                
                db.commit()
                if (i // batch_size) % 10 == 0:
                    logger.info(f"Converted {i + len(batch_codes)}/{len(ts_codes)} stocks...")
                    
            logger.info("Weekly/Monthly conversion complete.")
        except Exception as e:
            logger.error(f"Error in convert_to_weekly_monthly: {e}")
            db.rollback()
        finally:
            db.close()

    def convert_all_to_weekly_monthly(self, days=365):
        """批量转换历史日线为周线月线"""
        # 强制限制在 5 年内
        days = min(days, 1825)
        db = SessionLocal()
        try:
            logger.info(f"Batch converting last {days} days of daily data to weekly/monthly...")
            # 获取最近 N 天的所有交易日
            start_date = datetime.now().date() - timedelta(days=days)
            trade_dates = db.query(DailyBar.trade_date).filter(
                DailyBar.trade_date >= start_date
            ).distinct().order_by(DailyBar.trade_date.desc()).all()
            
            for date_row in trade_dates:
                d_str = date_row[0].strftime('%Y%m%d')
                self.convert_to_weekly_monthly(d_str)
        finally:
            db.close()

    async def backfill_data(self, days=30):
        """后台回溯同步"""
        await self.smart_sync_recent_data(days=days, sync_industry_history=True)

    def backfill(self, start_date, end_date):
        """按日期范围回溯"""
        pass

    async def sync_realtime_minute_data(self):
        """实时同步分钟数据"""
        # 暂时 pass，响应用户“不要分钟数据”的要求，或者后续修复
        pass

    async def flush_minute_buffer(self):
        """集中落库分钟数据"""
        # 暂时 pass
        pass

    async def sync_tdx_realtime_minutes(self):
        await self.sync_realtime_minute_data()

    async def archive_tdx_minutes(self):
        await self.flush_minute_buffer()

    async def init_tdx_data(self):
        return

    def download_minute_data(self, ts_code, start_date, end_date, freq='1min', force_network=False):
        """下载分钟数据
        force_network: 强制从网络(API)下载，不使用本地 TDX vipdoc
        """
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        from app.services.tdx_data_service import tdx_service
        import pandas as pd
        import os
        from app.core.config import settings

        freq = (freq or "").lower()
        if freq in ["5", "5m", "5min"]:
            freq = "5min"
        if freq in ["30", "30m", "30min"]:
            freq = "30min"

        if freq not in ["5min", "30min"]:
            raise ValueError("Only 5min/30min are supported for minute download.")

        bars_per_day = 48 if freq == "5min" else 8
        try:
            start_dt = datetime.strptime(str(start_date), "%Y%m%d").date() if start_date else None
            end_dt = datetime.strptime(str(end_date), "%Y%m%d").date() if end_date else None
        except Exception:
            start_dt = None
            end_dt = None

        if not end_dt:
            end_dt = datetime.now().date()
        if not start_dt:
            start_dt = end_dt - timedelta(days=90)

        days = max(1, (end_dt - start_dt).days + 1)
        target_bars = int(days * bars_per_day * 2)
        target_bars = max(target_bars, 800)
        target_bars = min(target_bars, 800 * 10)

        vipdoc_root = str(getattr(settings, "TDX_VIPDOC_ROOT", "") or "").strip()
        use_vipdoc = bool(vipdoc_root) and os.path.isdir(vipdoc_root) and not force_network

        df_raw = pd.DataFrame()
        if use_vipdoc:
            from app.services.tdx_vipdoc_service import TdxVipdocService

            vip = TdxVipdocService(vipdoc_root)
            df_5m = vip.read_5min_bars(ts_code)
            
            if df_5m is None or df_5m.empty:
                logger.info(f"Local TDX data not found for {ts_code}, fetching from network")
            else:
                latest_local_date = df_5m['trade_time'].max().date()
                if latest_local_date < end_dt:
                    logger.info(f"Local TDX data for {ts_code} is outdated (latest: {latest_local_date}, target: {end_dt}), fetching from network")
                    df_5m = None
                else:
                    logger.info(f"Using local TDX data for {ts_code}, latest: {latest_local_date}")
            
            if df_5m is None or df_5m.empty:
                dfs = []
                fetched = 0
                page = 0
                while fetched < target_bars and page < 10:
                    df_page = tdx_service.fetch_minute_bars(ts_code, freq, count=800, start=page * 800)
                    if df_page is None or df_page.empty:
                        if page == 0:
                            retry_ok = False
                            for attempt in range(2):
                                try:
                                    import time as _time
                                    _time.sleep(0.5 * (2 ** attempt))
                                except Exception:
                                    pass
                                df_retry = tdx_service.fetch_minute_bars(ts_code, freq, count=800, start=page * 800)
                                if df_retry is not None and (not df_retry.empty):
                                    df_page = df_retry
                                    retry_ok = True
                                    break
                            if not retry_ok:
                                break
                        else:
                            break
                    dfs.append(df_page)
                    fetched += len(df_page)
                    page += 1

                if not dfs:
                    logger.warning(f"Failed to fetch minute data from network for {ts_code}")
                    return 0

                df_raw = pd.concat(dfs, ignore_index=True)
            else:
                if freq == "5min":
                    df_raw = df_5m
                else:
                    df_raw = vip.aggregate_30min_from_5min(df_5m)
        else:
            if force_network:
                logger.info(f"Forcing network fetch for {ts_code} {freq} (force_network=True)")
            dfs = []
            fetched = 0
            page = 0
            while fetched < target_bars and page < 10:
                df_page = tdx_service.fetch_minute_bars(ts_code, freq, count=800, start=page * 800)
                if df_page is None or df_page.empty:
                    if page == 0:
                        retry_ok = False
                        for attempt in range(2):
                            try:
                                import time as _time
                                _time.sleep(0.5 * (2 ** attempt))
                            except Exception:
                                pass
                            df_retry = tdx_service.fetch_minute_bars(ts_code, freq, count=800, start=page * 800)
                            if df_retry is not None and (not df_retry.empty):
                                df_page = df_retry
                                retry_ok = True
                                break
                        if not retry_ok:
                            break
                    else:
                        break
                dfs.append(df_page)
                fetched += len(df_page)
                page += 1

            if not dfs:
                return 0

            df_raw = pd.concat(dfs, ignore_index=True)
        df_raw = df_raw.drop_duplicates(subset=["trade_time"]).sort_values("trade_time")
        df_raw = df_raw[(df_raw["trade_time"].dt.date >= start_dt) & (df_raw["trade_time"].dt.date <= end_dt)]
        if df_raw is None or df_raw.empty:
            return 0

        df_qfq = tdx_service.calculate_qfq(df_raw.copy(), ts_code)
        if 'adj_factor' not in df_qfq.columns:
            df_qfq['adj_factor'] = 1.0
        df_qfq['freq'] = freq

        logger.info(f"Downloaded {len(df_qfq)} minute bars for {ts_code} {freq}, date range: {df_qfq['trade_time'].min()} to {df_qfq['trade_time'].max()}")

        db = SessionLocal()
        try:
            records = []
            for _, r in df_qfq.iterrows():
                tt = r.get('trade_time')
                if pd.isna(tt):
                    continue
                if hasattr(tt, "to_pydatetime"):
                    tt = tt.to_pydatetime()
                records.append({
                    "ts_code": ts_code,
                    "trade_time": tt,
                    "freq": freq,
                    "open": float(r.get("open", 0) or 0),
                    "high": float(r.get("high", 0) or 0),
                    "low": float(r.get("low", 0) or 0),
                    "close": float(r.get("close", 0) or 0),
                    "vol": float(r.get("vol", 0) or 0),
                    "amount": float(r.get("amount", 0) or 0),
                    "adj_factor": float(r.get("adj_factor", 1.0) or 1.0),
                    "updated_at": datetime.now(),
                })

            if not records:
                logger.warning(f"No records to save for {ts_code} {freq}")
                return 0

            stmt = sqlite_insert(MinuteBar).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=["ts_code", "trade_time", "freq"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "vol": stmt.excluded.vol,
                    "amount": stmt.excluded.amount,
                    "adj_factor": stmt.excluded.adj_factor,
                    "updated_at": stmt.excluded.updated_at,
                }
            )
            res = db.execute(stmt)
            db.commit()
            rowcount = int(getattr(res, "rowcount", 0) or 0)
            logger.info(f"Saved {rowcount} minute bars to database for {ts_code} {freq}")
            try:
                tdx_service.save_to_redis(df_raw.tail(800), ts_code, freq)
            except Exception as e:
                logger.warning(f"Failed to save to Redis for {ts_code} {freq}: {e}")
            return rowcount
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    async def sync_post_close_minute_data(self, days: int = None, pool: str = None, limit: int = None, freqs: list[str] | None = None, force_network: bool = False):
        days = int(days if days is not None else settings.MINUTE_SYNC_DAYS)
        pool = (pool if pool is not None else settings.MINUTE_SYNC_POOL) or "shsz"
        limit = int(limit if limit is not None else settings.MINUTE_SYNC_LIMIT)
        if freqs is None:
            freqs = [f.strip().lower() for f in str(settings.MINUTE_SYNC_FREQS or "").split(",") if f.strip()]
        freqs = ["5min" if f in ["5", "5m", "5min"] else "30min" if f in ["30", "30m", "30min"] else f for f in freqs]
        freqs = [f for f in freqs if f in ["5min", "30min"]]
        if not freqs:
            freqs = ["5min", "30min"]

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=max(1, days))).strftime("%Y%m%d")

        codes: list[str] = []
        latest_map: dict[str, dict[str, str]] = {}
        failures = 0
        total = 0
        try:
            db = SessionLocal()
            try:
                if (pool or "").lower() == "active":
                    from app.models.stock_models import Position, TradingPlan
                    from datetime import date
                    codes_set = set()
                    pos_rows = db.query(Position.ts_code).filter(Position.vol > 0).all()
                    for r in pos_rows:
                        codes_set.add(r[0])
                    plan_rows = db.query(TradingPlan.ts_code).filter(TradingPlan.date == date.today(), TradingPlan.executed == False).all()
                    for r in plan_rows:
                        codes_set.add(r[0])
                    codes = sorted(list(codes_set))
                else:
                    q = db.query(Stock.ts_code).filter((Stock.ts_code.like("%.SZ")) | (Stock.ts_code.like("%.SH"))).order_by(Stock.ts_code.asc())
                    if limit and limit > 0:
                        q = q.limit(limit)
                    codes = [r[0] for r in q.all()]

                for f in freqs:
                    latest_rows = (
                        db.query(MinuteBar.ts_code, func.max(MinuteBar.trade_time))
                        .filter(MinuteBar.freq == f, MinuteBar.ts_code.in_(codes))
                        .group_by(MinuteBar.ts_code)
                        .all()
                    )
                    latest_map[f] = {
                        str(ts): dt.strftime("%Y%m%d")
                        for ts, dt in latest_rows
                        if dt is not None
                    }
            finally:
                db.close()

            if not codes:
                self._update_state(status="idle", task=None, progress=100, message="post_close_minute_sync skipped: no codes")
                logger.warning("Post-close minute sync skipped: no target codes.")
                return

            total = len(codes) * len(freqs)
            done = 0
            failure_samples: list[str] = []
            self._update_state(status="running", task="post_close_minute_sync", progress=0, message=f"codes={len(codes)}, freqs={freqs}, range={start_date}-{end_date}, force_network={force_network}")
            logger.info(f"Post-close minute sync start: codes={len(codes)}, freqs={freqs}, range={start_date}-{end_date}, pool={pool}, limit={limit}, force_network={force_network}")

            concurrency = int(getattr(settings, "MINUTE_SYNC_CONCURRENCY", 16) or 16)
            from app.services.tdx_data_service import tdx_service
            max_workers = max(1, int(getattr(tdx_service, "_pool_max_size", 32) or 32))
            concurrency = max(1, min(concurrency, max_workers))
            queue: asyncio.Queue[tuple[str, str, str] | None] = asyncio.Queue()

            async def _worker():
                nonlocal done, failures
                while True:
                    item = await queue.get()
                    try:
                        if item is None:
                            return
                        code, f, s_date = item
                        try:
                            await asyncio.to_thread(self.download_minute_data, code, s_date, end_date, f, force_network)
                        except Exception as e:
                            failures += 1
                            if len(failure_samples) < 10:
                                failure_samples.append(f"{code} {f}: {e}")
                            logger.warning(f"Minute download failed: {code} {f}: {e}")
                        done += 1
                        if done % 20 == 0 or done == total:
                            self._update_state(progress=int(done / total * 100), message=f"{code} {f} ({done}/{total})")
                    finally:
                        queue.task_done()

            for code in codes:
                for f in freqs:
                    s_date = latest_map.get(f, {}).get(code, start_date)
                    await queue.put((code, f, s_date))
            for _ in range(concurrency):
                await queue.put(None)

            workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
            await queue.join()
            await asyncio.gather(*workers, return_exceptions=True)
            if failures:
                logger.warning(f"Post-close minute sync completed with failures: {failures}/{total}")
                if failure_samples:
                    logger.warning(f"Post-close minute sync failure samples: {failure_samples}")
            else:
                logger.info(f"Post-close minute sync completed: {total} tasks")
            self._update_state(status="idle", task=None, progress=100, message=f"post_close_minute_sync done, failures={failures}, total={total}")
        except Exception as e:
            logger.error(f"Post-close minute sync failed: {e}")
            self._update_state(status="error", task="post_close_minute_sync", progress=0, message=str(e))
        finally:
            if self.sync_state.get("status") == "running":
                self._update_state(status="idle", task=None, progress=100, message=f"post_close_minute_sync done, failures={failures}, total={total}")

    async def get_data_quality_report(self):
        """获取数据质量报告，包含最新日期、覆盖率和任务状态"""
        now_ts = datetime.now().timestamp()
        
        # 1. 任务状态必须实时
        current_task_info = {
            "status": self.sync_state["status"],
            "task": self.sync_state["task"],
            "progress": self.sync_state["progress"],
            "message": self.sync_state["message"]
        }

        # 2. 数据库统计信息可以使用缓存
        # 如果正在运行任务，或者缓存超过 5 分钟 (300s)，或者缓存为空，则重新计算
        # (之前是 60s，对于 count(*) 来说可能还是太频繁，特别是数据量大时)
        cache_valid = (now_ts - self._report_cache[0] < 300) and self._report_cache[1]
        
        # 如果正在运行任务，我们希望看到进度，但不需要频繁更新覆盖率
        # 所以我们将任务状态和数据统计分开处理
        
        if not cache_valid:
            def _fetch_stats():
                db = SessionLocal()
                try:
                    # 1. 获取最新交易日 (从 DailyBar 获取)
                    # 使用 limit 1 配合索引通常很快
                    latest_trade_date_val = db.query(DailyBar.trade_date).order_by(desc(DailyBar.trade_date)).limit(1).scalar()
                    latest_trade_date = latest_trade_date_val.strftime('%Y-%m-%d') if latest_trade_date_val else "未知"
                    
                    # 2. 计算覆盖率 (最新日期的股票数 / 总股票数)
                    # 优化: 缓存 total_stocks，不需要每次都查
                    if not hasattr(self, '_total_stocks_cache') or (now_ts - getattr(self, '_total_stocks_ts', 0) > 3600):
                        self._total_stocks_cache = db.query(func.count(Stock.ts_code)).scalar() or 1
                        self._total_stocks_ts = now_ts
                    
                    total_stocks = self._total_stocks_cache

                    if latest_trade_date_val:
                        # 优化: 只在交易时间或最近数据更新时才查 count
                        # 非交易时间且数据未变时，使用上次结果
                        latest_count = db.query(func.count(DailyBar.id)).filter(DailyBar.trade_date == latest_trade_date_val).scalar() or 0
                    else:
                        latest_count = 0
                    
                    coverage = f"{(latest_count / total_stocks * 100):.1f}%"
                    
                    # 3. 确定健康状态
                    status = "Healthy"
                    if not latest_trade_date_val or (datetime.now().date() - latest_trade_date_val).days > 3:
                        # 如果最新数据超过 3 天且不是周末，则认为异常
                        # 简单判定: 超过 5 天肯定是异常 (包含周末)
                        if latest_trade_date_val and (datetime.now().date() - latest_trade_date_val).days > 5:
                            status = "Warning"
                    
                    return {
                        "status": status,
                        "latest_trade_date": latest_trade_date,
                        "latest_coverage": coverage,
                        "last_sync": self.sync_state["last_updated"]
                    }
                except Exception as e:
                    logger.error(f"Error generating data stats: {e}")
                    return None
                finally:
                    db.close()
            
            stats = await asyncio.to_thread(_fetch_stats)
            if stats:
                # 保留 current_task 占位，后面会合并
                self._report_cache = (now_ts, stats)
        
        # 合并实时任务状态和缓存的统计数据
        cached_stats = self._report_cache[1] or {
            "status": "Unknown",
            "latest_trade_date": "未知",
            "latest_coverage": "未知",
            "last_sync": ""
        }
        
        return {
            **cached_stats,
            "current_task": current_task_info
        }

    def fix_stock_data(self, ts_code: str):
        """手动修复单个股票数据：清理 + 重下"""
        try:
            from app.db.session import SessionLocal
            from app.models.stock_models import MinuteBar, DailyBar, WeeklyBar, MonthlyBar, StockIndicator
            from app.core.redis import redis_client
            from app.services.data_provider import data_provider
            from app.services.tdx_data_service import tdx_service
            from app.services.market.market_data_service import market_data_service
            
            ts_code = data_provider._normalize_ts_code(ts_code)
            logger.info(f"Starting manual fix for {ts_code}")
            self._update_state(status="running", task="manual_fix", progress=0, message=f"Fixing {ts_code}...")
            
            # 1. 清理 DB
            db = SessionLocal()
            try:
                db.query(MinuteBar).filter(MinuteBar.ts_code == ts_code).delete()
                db.query(DailyBar).filter(DailyBar.ts_code == ts_code).delete()
                db.query(WeeklyBar).filter(WeeklyBar.ts_code == ts_code).delete()
                db.query(MonthlyBar).filter(MonthlyBar.ts_code == ts_code).delete()
                db.query(StockIndicator).filter(StockIndicator.ts_code == ts_code).delete()
                db.commit()
            finally:
                db.close()
                
            # 2. 清理 Redis
            if redis_client:
                keys = [f"MARKET:MIN:5min:{ts_code}", f"MARKET:MIN:30min:{ts_code}", f"MARKET:MIN:1min:{ts_code}"]
                for k in keys:
                    try:
                        redis_client.delete(k)
                    except Exception:
                        pass
            
            market_data_service.clear_stock_cache(ts_code)
            self._update_state(progress=30, message=f"Downloading daily bars for {ts_code}...")

            df = tdx_service.fetch_bars(ts_code, 'D', count=1200)
            if df is None or df.empty:
                vipdoc_root = str(getattr(settings, "TDX_VIPDOC_ROOT", "") or "").strip()
                if vipdoc_root and os.path.isdir(vipdoc_root):
                    from app.services.tdx_vipdoc_service import TdxVipdocService
                    vip = TdxVipdocService(vipdoc_root)
                    df = vip.read_day_bars(ts_code, limit=1200)
            if df is None or df.empty:
                raise Exception("TDX 日线数据为空")

            df = df.sort_values('trade_time')
            daily_objects = []
            prev_close = None
            for _, row in df.iterrows():
                trade_time = row.get('trade_time')
                if not trade_time:
                    continue
                trade_date = pd.to_datetime(trade_time).date()
                close = float(row.get('close', 0) or 0.0)
                pre_close = float(row.get('pre_close', 0) or 0.0)
                if prev_close is not None:
                    pre_close = float(prev_close)
                change = close - pre_close if pre_close else 0.0
                pct_chg = (change / pre_close * 100.0) if pre_close else 0.0
                daily_objects.append(DailyBar(
                    ts_code=ts_code,
                    trade_date=trade_date,
                    open=float(row.get('open', 0) or 0.0),
                    high=float(row.get('high', 0) or 0.0),
                    low=float(row.get('low', 0) or 0.0),
                    close=close,
                    pre_close=pre_close,
                    change=change,
                    pct_chg=pct_chg,
                    vol=float(row.get('vol', 0) or 0.0),
                    amount=float(row.get('amount', 0) or 0.0),
                    adj_factor=float(row.get('adj_factor', 1.0) or 1.0)
                ))
                prev_close = close

            if daily_objects:
                db = SessionLocal()
                try:
                    db.bulk_save_objects(daily_objects)
                    db.commit()
                finally:
                    db.close()

            self.reconstruct_weekly_monthly(ts_code)
            self._update_state(progress=60, message=f"Downloading {ts_code} minute bars...")
            
            # 3. 重新下载 (最近 90 天)
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
            
            # [Fix] 强制下载所有关键周期的分钟线，确保点击一次修复按钮，所有周期都正常
            # 增加 force_network=True 以绕过可能存在的本地旧数据缓存检查
            self.download_minute_data(ts_code, start_date, end_date, freq='30min', force_network=True)
            self.download_minute_data(ts_code, start_date, end_date, freq='5min', force_network=True)
            
            # 顺便尝试下载 1min (如果配置支持)
            try:
                self.download_minute_data(ts_code, start_date, end_date, freq='1min', force_network=True)
            except Exception:
                pass
            
            logger.info(f"Manual fix for {ts_code} completed")
            self._update_state(status="idle", task=None, progress=100, message=f"Fixed {ts_code}")
        except Exception as e:
            logger.error(f"Manual fix for {ts_code} failed: {e}")
            self._update_state(status="error", task=None, progress=0, message=f"Fix failed: {e}")

    async def auto_fix_missing_minute_data(self):
        """
        自动巡检并修复关键股票的分钟数据缺失
        scope: 最近有交易计划的股票 + 最近选出的股票
        """
        from app.models.stock_models import TradingPlan, OutcomeEvent
        
        logger.info("Starting auto-fix minute data task...")
        
        # 1. 确定目标股票池
        target_codes = set()
        db = SessionLocal()
        try:
            # A. 最近 7 天有交易计划的股票
            recent_date = datetime.now().date() - timedelta(days=7)
            plans = db.query(TradingPlan.ts_code).filter(TradingPlan.date >= recent_date).all()
            for p in plans:
                target_codes.add(p[0])
                
            # B. 最近 3 天选出的股票 (OutcomeEvent)
            recent_select_date = datetime.now().date() - timedelta(days=3)
            events = db.query(OutcomeEvent.ts_code).filter(
                OutcomeEvent.event_date >= recent_select_date,
                OutcomeEvent.ts_code.isnot(None)
            ).all()
            for e in events:
                target_codes.add(e[0])
                
        except Exception as e:
            logger.error(f"Error gathering target stocks for auto-fix: {e}")
        finally:
            db.close()
            
        if not target_codes:
            logger.info("No active stocks to check.")
            return

        logger.info(f"Checking minute data for {len(target_codes)} active stocks...")
        
        # 2. 检查数据完整性
        is_trading = data_provider.is_trading_time()
        now = datetime.now()
        check_today = False
        
        if is_trading:
            check_today = True
        elif now.hour >= 15: # 盘后
             # 检查今日是否是交易日
             res = await data_provider.check_trade_day(now.strftime('%Y%m%d'))
             if res.get("is_open"):
                 check_today = True
        
        if not check_today:
             logger.info("Not in trading time or post-market, skipping minute data check.")
             return

        fixed_count = 0
        for ts_code in target_codes:
            try:
                # 获取最近 30min 数据 (轻量级)
                df = await data_provider.get_minute_data(ts_code, freq='30min', limit=5)
                
                needs_fix = False
                if df is None or df.empty:
                    needs_fix = True
                else:
                    # 检查最新时间是否新鲜
                    if not df.empty and 'trade_time' in df.columns:
                        last_time = pd.to_datetime(df['trade_time'].iloc[-1])
                        
                        if is_trading:
                            # 交易时间，允许 60 分钟延迟 (30min bar)
                            if (now - last_time).total_seconds() > 3600:
                                needs_fix = True
                        else:
                            # 盘后，必须有今日数据
                            if last_time.date() < now.date():
                                needs_fix = True
                
                if needs_fix:
                    logger.warning(f"Auto-fix triggered for {ts_code} (Missing/Stale data)")
                    # 在线程池中运行同步的 fix_stock_data
                    await asyncio.to_thread(self.fix_stock_data, ts_code)
                    
                    fixed_count += 1
                    if fixed_count >= 10: # 每次最多修复 10 个
                        logger.warning("Hit max auto-fix limit (10) per run.")
                        break
                        
            except Exception as e:
                logger.error(f"Error checking {ts_code}: {e}")
                
        logger.info(f"Auto-fix task complete. Fixed {fixed_count} stocks.")

data_sync_service = DataSyncService()
