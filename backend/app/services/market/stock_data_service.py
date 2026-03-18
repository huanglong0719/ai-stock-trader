from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, date
import asyncio
from sqlalchemy import desc, func
from app.db.session import SessionLocal
from app.models.stock_models import DailyBar, Stock, WeeklyBar, MonthlyBar, MinuteBar, DailyBasic, StockIndicator
from app.services.logger import logger

class StockDataService:
    def __init__(self):
        self._stock_basic_cache = None
        self._last_basic_time = 0.0
        self._float_share_cache = {}  # {ts_code: (float_share, timestamp)}
        self._cache_duration = 3600 * 24  # 股本数据缓存 24 小时
        self._latest_trade_date_cache = None
        self._latest_trade_date_time = 0.0

    def get_latest_trade_date(self, db=None) -> Optional[date]:
        """获取数据库中最新的交易日期 (带 1 分钟缓存)"""
        now = datetime.now().timestamp()
        if self._latest_trade_date_cache and (now - self._latest_trade_date_time < 60):
            return self._latest_trade_date_cache
            
        local_db = db if db else SessionLocal()
        try:
            res = local_db.query(func.max(DailyBar.trade_date)).scalar()
            if res:
                self._latest_trade_date_cache = res
                self._latest_trade_date_time = now
            return res
        finally:
            if not db:
                local_db.close()

    def get_stock_basic(self) -> List[Dict[str, Any]]:
        """
        获取本地股票列表 (带 1 小时缓存)
        """
        now = datetime.now().timestamp()
        if self._stock_basic_cache and (now - self._last_basic_time < 3600):
            return self._stock_basic_cache

        db = SessionLocal()
        try:
            stocks = db.query(Stock).all()
            result = []
            for s in stocks:
                result.append({
                    "ts_code": s.ts_code,
                    "symbol": s.symbol,
                    "name": s.name,
                    "area": s.area if hasattr(s, 'area') else '',
                    "industry": s.industry if hasattr(s, 'industry') else '',
                    "market": s.market if hasattr(s, 'market') else '',
                    "list_date": s.list_date if s.list_date else ''
                })
            
            self._stock_basic_cache = result
            self._last_basic_time = now
            return result
        except Exception as e:
            logger.error(f"读取本地 Stock Basic 失败: {e}")
            return []
        finally:
            db.close()

    async def async_get_stock_basic(self) -> List[Dict[str, Any]]:
        """
        获取本地股票列表 (异步版)
        """
        return await asyncio.to_thread(self.get_stock_basic)
    def get_float_share(self, ts_code: str) -> Optional[float]:
        """
        从本地数据库获取最新的流通股本 (万股) (带 24 小时缓存)
        """
        now = datetime.now().timestamp()
        if ts_code in self._float_share_cache:
            val, ts = self._float_share_cache[ts_code]
            if now - ts < self._cache_duration:
                return val

        db = SessionLocal()
        try:
            # 1. 尝试本地查询
            basic = db.query(DailyBasic.float_share).filter(
                DailyBasic.ts_code == ts_code
            ).order_by(desc(DailyBasic.trade_date)).first()
            
            if basic and basic[0]:
                val = float(basic[0])
                self._float_share_cache[ts_code] = (val, now)
                return val
            return None
        except Exception as e:
            logger.error(f"获取本地流通股本失败: {e}")
            return None
        finally:
            db.close()

    def get_all_float_shares(self) -> Dict[str, float]:
        """
        批量获取所有股票的最新流通股本 (用于高性能计算)
        """
        db = SessionLocal()
        try:
            # 获取最近的一个有数据的交易日
            latest_date = db.query(func.max(DailyBasic.trade_date)).scalar()
            if not latest_date:
                return {}
            
            results = db.query(DailyBasic.ts_code, DailyBasic.float_share).filter(
                DailyBasic.trade_date == latest_date,
                DailyBasic.float_share != None
            ).all()
            
            return {r.ts_code: float(r.float_share) for r in results}
        except Exception as e:
            logger.error(f"批量获取流通股本失败: {e}")
            return {}
        finally:
            db.close()

    def aggregate_period_stats(self, ts_code: str, period_start: str, curr_date: str) -> Optional[Dict[str, Optional[float]]]:
        """
        聚合指定周期内的日线数据
        """
        db = SessionLocal()
        try:
            # 1. 聚合基础统计
            stats = db.query(
                func.sum(DailyBar.vol),
                func.max(DailyBar.high),
                func.min(DailyBar.low)
            ).filter(
                DailyBar.ts_code == ts_code,
                DailyBar.trade_date >= datetime.strptime(period_start, '%Y%m%d').date(),
                DailyBar.trade_date < datetime.strptime(curr_date, '%Y%m%d').date()
            ).first()
            
            # 2. 获取周期开盘价 (第一天的 Open)
            first_day = db.query(DailyBar.open).filter(
                DailyBar.ts_code == ts_code,
                DailyBar.trade_date >= datetime.strptime(period_start, '%Y%m%d').date(),
                DailyBar.trade_date < datetime.strptime(curr_date, '%Y%m%d').date()
            ).order_by(DailyBar.trade_date.asc()).first()
            
            if not stats or stats[0] is None:
                return None
                
            return {
                "sum_vol": float(stats[0] or 0),
                "max_high": float(stats[1]) if stats[1] is not None else None,
                "min_low": float(stats[2]) if stats[2] is not None else None,
                "open": float(first_day[0]) if first_day else None
            }
        except Exception as e:
            logger.error(f"聚合历史统计数据失败: {e}")
            return None
        finally:
            db.close()

    def get_latest_indicators_batch(self, ts_codes: List[str], trade_date: Any = None) -> Dict[str, Dict[str, Any]]:
        """
        批量获取一组股票的最新日线数据及预计算指标
        """
        db = SessionLocal()
        try:
            from sqlalchemy import func
            
            # 如果没传日期，自动获取数据库最新日期
            if not trade_date:
                target_date = self.get_latest_trade_date(db)
            elif isinstance(trade_date, str):
                clean_date = trade_date.replace('-', '').replace('/', '')
                target_date = datetime.strptime(clean_date, '%Y%m%d').date()
            else:
                target_date = trade_date

            if not target_date:
                return {}

            # 2. 直接按日期联表查询 DailyBar 和 StockIndicator (不再使用子查询以提升性能)
            query = db.query(
                DailyBar.ts_code,
                DailyBar.trade_date,
                DailyBar.open,
                DailyBar.close,
                DailyBar.high,
                DailyBar.low,
                DailyBar.vol.label('volume'),
                DailyBar.pct_chg,
                DailyBar.adj_factor,
                StockIndicator.ma5,
                StockIndicator.ma10,
                StockIndicator.ma20,
                StockIndicator.ma60,
                StockIndicator.vol_ma5,
                StockIndicator.vol_ma10,
                StockIndicator.macd,
                StockIndicator.macd_diff,
                StockIndicator.macd_dea,
                StockIndicator.is_daily_bullish,
                StockIndicator.is_trend_recovering,
                StockIndicator.is_weekly_bullish,
                StockIndicator.is_monthly_bullish,
                StockIndicator.weekly_ma20_slope,
                StockIndicator.bias5,
                StockIndicator.bias10
            ).outerjoin(
                StockIndicator,
                (DailyBar.ts_code == StockIndicator.ts_code) & (DailyBar.trade_date == StockIndicator.trade_date)
            ).filter(
                DailyBar.ts_code.in_(ts_codes),
                DailyBar.trade_date == target_date
            )

            results = query.all()
            
            indicator_map = {}
            for row in results:
                indicator_map[row.ts_code] = {
                    "time": row.trade_date.strftime('%Y-%m-%d'),
                    "open": float(row.open or 0),
                    "close": float(row.close or 0),
                    "high": float(row.high or 0),
                    "low": float(row.low or 0),
                    "volume": float(row.volume or 0),
                    "pct_chg": float(row.pct_chg or 0),
                    "adj_factor": float(row.adj_factor or 1.0),
                    "ma5": float(row.ma5) if row.ma5 is not None else None,
                    "ma10": float(row.ma10) if row.ma10 is not None else None,
                    "ma20": float(row.ma20) if row.ma20 is not None else None,
                    "ma60": float(row.ma60) if row.ma60 is not None else None,
                    "vol_ma5": float(row.vol_ma5) if row.vol_ma5 is not None else None,
                    "vol_ma10": float(row.vol_ma10) if row.vol_ma10 is not None else None,
                    "macd": float(row.macd) if row.macd is not None else None,
                    "macd_diff": float(row.macd_diff) if row.macd_diff is not None else None,
                    "macd_dea": float(row.macd_dea) if row.macd_dea is not None else None,
                    "is_daily_bullish": bool(row.is_daily_bullish),
                    "is_trend_recovering": bool(row.is_trend_recovering),
                    "is_weekly_bullish": bool(row.is_weekly_bullish),
                    "is_monthly_bullish": bool(row.is_monthly_bullish),
                    "weekly_ma20_slope": float(row.weekly_ma20_slope or 0),
                    "bias5": float(row.bias5) if row.bias5 is not None else None,
                    "bias10": float(row.bias10) if row.bias10 is not None else None
                }
            return indicator_map
        except Exception as e:
            logger.error(f"批量获取指标失败: {e}")
            return {}
        finally:
            db.close()

    def get_local_quote(self, ts_code: str) -> Optional[Dict[str, Any]]:
        """
        从本地数据库获取最新的行情数据作为 Quote
        """
        db = SessionLocal()
        try:
            # 1. 获取最新的日线数据
            last_bar = db.query(DailyBar).filter(DailyBar.ts_code == ts_code).order_by(desc(DailyBar.trade_date)).first()
            if not last_bar:
                return None
            
            # 2. 获取股票名称 (增加指数名称映射支持)
            # 延迟导入以避免循环依赖
            from app.services.market.market_data_service import market_data_service
            if ts_code in market_data_service.INDEX_NAMES:
                name = market_data_service.INDEX_NAMES[ts_code]
            else:
                stock = db.query(Stock).filter(Stock.ts_code == ts_code).first()
                # 显式转换以消除 mypy 警告
                name = str(stock.name) if stock else ts_code

            # 3. 构造 Quote 格式
            trade_date = last_bar.trade_date or datetime.now().date()
            quote: Dict[str, Any] = {
                "name": name,
                "ts_code": ts_code,
                "symbol": ts_code,
                "price": last_bar.close,
                "pre_close": last_bar.pre_close,
                "change": last_bar.change,
                "pct_chg": last_bar.pct_chg,
                "open": last_bar.open,
                "high": last_bar.high,
                "low": last_bar.low,
                "vol": last_bar.vol,
                "amount": last_bar.amount,
                "time": trade_date.strftime('%Y-%m-%d') + " 15:00:00"
            }
            
            # 4. 补充换手率 (从 DailyBasic)
            basic = db.query(DailyBasic.turnover_rate).filter(
                DailyBasic.ts_code == ts_code,
                DailyBasic.trade_date == last_bar.trade_date
            ).first()
            
            if basic and basic[0] is not None:
                quote['turnover_rate'] = float(basic[0])
            
            return quote
        except Exception as e:
            logger.error(f"获取本地行情失败: {e}")
            return None
        finally:
            db.close()

    def get_local_kline(self, ts_code: str, freq: str = 'D', start_date: Optional[str | date] = None, end_date: Optional[str | date] = None, limit: int = 1000, include_indicators: bool = True) -> List[Dict[str, Any]]:
        """
        从本地数据库获取K线数据
        """
        # 0. 强制 5 年历史数据限制
        five_years_ago = (datetime.now() - timedelta(days=1825)).strftime('%Y%m%d')
        if not start_date:
            start_date = five_years_ago
        else:
            clean_start = str(start_date).replace('-', '').replace('/', '')
            if clean_start < five_years_ago:
                start_date = five_years_ago

        db = SessionLocal()
        try:
            model: Any = DailyBar
            if freq in ['W', 'M']:
                model = WeeklyBar if freq == 'W' else MonthlyBar
                
                # 联表查询获取预计算的周/月线指标
                if include_indicators:
                    if freq == 'W':
                        query = db.query(
                            model.trade_date.label('time'),
                            model.open,
                            model.close,
                            model.high,
                            model.low,
                            model.vol.label('volume'),
                            model.adj_factor,
                            StockIndicator.weekly_ma5.label('ma5'),
                            StockIndicator.weekly_ma10.label('ma10'),
                            StockIndicator.weekly_ma20.label('ma20'),
                            StockIndicator.weekly_ma60.label('ma60'),
                            StockIndicator.weekly_vol_ma5.label('vol_ma5'),
                            StockIndicator.weekly_vol_ma10.label('vol_ma10'),
                            StockIndicator.weekly_macd.label('macd'),
                            StockIndicator.weekly_macd_diff.label('macd_diff'),
                            StockIndicator.weekly_macd_dea.label('macd_dea'),
                            StockIndicator.is_weekly_bullish.label('is_bullish')
                        )
                    else: # Monthly
                        query = db.query(
                            model.trade_date.label('time'),
                            model.open,
                            model.close,
                            model.high,
                            model.low,
                            model.vol.label('volume'),
                            model.adj_factor,
                            StockIndicator.monthly_ma5.label('ma5'),
                            StockIndicator.monthly_ma10.label('ma10'),
                            StockIndicator.monthly_ma20.label('ma20'),
                            StockIndicator.monthly_ma60.label('ma60'),
                            StockIndicator.monthly_vol_ma5.label('vol_ma5'),
                            StockIndicator.monthly_vol_ma10.label('vol_ma10'),
                            StockIndicator.monthly_macd.label('macd'),
                            StockIndicator.monthly_macd_diff.label('macd_diff'),
                            StockIndicator.monthly_macd_dea.label('macd_dea'),
                            StockIndicator.is_monthly_bullish.label('is_bullish')
                        )
                    
                    query = query.outerjoin(
                        StockIndicator,
                        (model.ts_code == StockIndicator.ts_code) & (model.trade_date == StockIndicator.trade_date)
                    )
                else:
                    query = db.query(
                        model.trade_date.label('time'),
                        model.open,
                        model.close,
                        model.high,
                        model.low,
                        model.vol.label('volume'),
                        model.adj_factor
                    )
                query = query.filter(model.ts_code == ts_code)
            elif freq in ['5', '30'] or 'min' in freq:
                # 分钟线 (已禁用)
                logger.warning(f"Minute frequency {freq} is disabled in StockDataService.")
                return []
            else:
                # 日线 (默认) - 增加与 StockIndicator 的联表查询，减少重复计算
                model = DailyBar
                if include_indicators:
                    query = db.query(
                        DailyBar.trade_date.label('time'),
                        DailyBar.open,
                        DailyBar.close,
                        DailyBar.high,
                        DailyBar.low,
                        DailyBar.vol.label('volume'),
                        DailyBar.pct_chg,
                        DailyBar.adj_factor,
                        StockIndicator.ma5,
                        StockIndicator.ma10,
                        StockIndicator.ma20,
                        StockIndicator.ma60,
                        StockIndicator.vol_ma5,
                        StockIndicator.vol_ma10,
                        StockIndicator.macd,
                        StockIndicator.macd_diff,
                        StockIndicator.macd_dea,
                        StockIndicator.is_daily_bullish.label('is_bullish'),
                        StockIndicator.is_trend_recovering,
                        StockIndicator.adj_factor.label('ind_adj_factor')
                    ).outerjoin(
                        StockIndicator,
                        (DailyBar.ts_code == StockIndicator.ts_code) & (DailyBar.trade_date == StockIndicator.trade_date)
                    )
                else:
                    query = db.query(
                        DailyBar.trade_date.label('time'),
                        DailyBar.open,
                        DailyBar.close,
                        DailyBar.high,
                        DailyBar.low,
                        DailyBar.vol.label('volume'),
                        DailyBar.pct_chg,
                        DailyBar.adj_factor
                    )
                query = query.filter(DailyBar.ts_code == ts_code)

            if start_date:
                if isinstance(start_date, str):
                    clean_start = start_date.replace('-', '').replace('/', '')
                    if len(clean_start) == 8:
                        start_date = datetime.strptime(clean_start, '%Y%m%d').date()
                query = query.filter(model.trade_date >= start_date) if freq in ['D', 'W', 'M'] else query
            if end_date:
                if isinstance(end_date, str):
                    clean_end = end_date.replace('-', '').replace('/', '')
                    if len(clean_end) == 8:
                        end_date = datetime.strptime(clean_end, '%Y%m%d').date()
                query = query.filter(model.trade_date <= end_date) if freq in ['D', 'W', 'M'] else query

            # 默认取最近 limit 条
            data = query.order_by(model.trade_date.desc()).limit(limit).all() if freq in ['D', 'W', 'M'] else query.all()
            
            # 再次倒序，使其按时间正序
            data = data[::-1]
            
            result = []
            prev_close = None
            
            for i, row in enumerate(data):
                 # 统一转换逻辑 (已经通过 label 统一了字段名)
                 current_close = float(row.close)
                 
                 # 涨跌幅逻辑
                 if freq in ['W', 'M']:
                     # 动态计算周/月涨跌幅
                     pct_chg = 0.0
                     if prev_close and prev_close != 0:
                         pct_chg = (current_close - prev_close) / prev_close * 100
                     prev_close = current_close
                 else:
                     # 日线使用数据库自带的
                     pct_chg = float(row.pct_chg or 0.0)
                 
                 item = {
                    "time": row.time.strftime('%Y-%m-%d'),
                    "open": float(row.open),
                    "close": current_close,
                    "high": float(row.high),
                    "low": float(row.low),
                    "volume": float(row.volume),
                    "pct_chg": pct_chg,
                    "adj_factor": float(row.adj_factor or 1.0),
                    # 预计算指标
                    "ma5": float(row.ma5) if hasattr(row, 'ma5') and row.ma5 is not None else None,
                    "ma10": float(row.ma10) if hasattr(row, 'ma10') and row.ma10 is not None else None,
                    "ma20": float(row.ma20) if hasattr(row, 'ma20') and row.ma20 is not None else None,
                    "ma60": float(row.ma60) if hasattr(row, 'ma60') and row.ma60 is not None else None,
                    "vol_ma5": float(row.vol_ma5) if hasattr(row, 'vol_ma5') and row.vol_ma5 is not None else None,
                    "vol_ma10": float(row.vol_ma10) if hasattr(row, 'vol_ma10') and row.vol_ma10 is not None else None,
                    "macd": float(row.macd) if hasattr(row, 'macd') and row.macd is not None else None,
                    "macd_diff": float(row.macd_diff) if hasattr(row, 'macd_diff') and row.macd_diff is not None else None,
                    "macd_dea": float(row.macd_dea) if hasattr(row, 'macd_dea') and row.macd_dea is not None else None,
                    "is_bullish": int(row.is_bullish) if hasattr(row, 'is_bullish') and row.is_bullish is not None else None,
                }
                 
                 # 日线特有字段
                 if freq not in ['W', 'M']:
                     if hasattr(row, 'is_trend_recovering'):
                         item["is_trend_recovering"] = int(row.is_trend_recovering) if row.is_trend_recovering is not None else None
                     if hasattr(row, 'ind_adj_factor'):
                         item["ind_adj_factor"] = float(row.ind_adj_factor) if row.ind_adj_factor is not None else None
                         
                 result.append(item)
                 
            return result
        except Exception as e:
            logger.error(f"查询本地K线失败 ({ts_code}, {freq}): {e}")
            return []
        finally:
            db.close()

    def get_stock_concepts(self, ts_code: str) -> List[str]:
        """
        获取股票概念列表 (不调用 Tushare)
        """
        try:
            from app.services.market.tdx_formula_service import tdx_formula_service
            # 尝试从通达信 EXTERNSTR 获取本地资讯 (2=核心题材)
            val = tdx_formula_service.EXTERNSTR(2, ts_code)
            if val and val.strip():
                # 简单分割，通达信 EXTERNSTR 通常包含多个关键词
                return [c.strip() for c in val.split(' ') if c.strip()]
            return []
        except Exception as e:
            logger.info(f"获取本地股票概念失败 ({ts_code}): {e}")
            return []

    def get_top_turnover_local(self, trade_date: str, top_n: int = 200) -> List[Dict[str, Any]]:
        """
        从本地数据库获取成交额前 N 的股票
        """
        db = SessionLocal()
        try:
            from app.models.stock_models import DailyBar, Stock
            target_date = datetime.strptime(trade_date, '%Y%m%d').date()
            
            # 联合查询以排除指数(如果有的话)并获取名称
            query = db.query(DailyBar.ts_code, DailyBar.amount, Stock.name).join(
                Stock, DailyBar.ts_code == Stock.ts_code
            ).filter(
                DailyBar.trade_date == target_date
            ).order_by(DailyBar.amount.desc()).limit(top_n * 3) # 多取一些以便过滤
            
            bars = query.all()
            
            result = []
            for ts_code, amount, name in bars:
                # 过滤：排除科创、北交、指数等 (根据 ts_code 规则)
                if ts_code.startswith(('688', '8', '4')) or ts_code.endswith('.BJ'):
                    continue
                
                # [优化] 过滤 ST、退市
                if name and ('ST' in name or '退' in name):
                    continue
                
                # [优化] 过滤停牌股票 (成交额为 0)
                if not amount or amount <= 0:
                    continue
                
                result.append({
                    "ts_code": ts_code,
                    "turnover_amount": amount
                })
                
                if len(result) >= top_n:
                    break
                    
            return result
        except Exception as e:
            logger.error(f"读取本地成交额排名失败: {e}")
            return []
        finally:
            db.close()

    def get_daily_basic_local(self, trade_date: str, ts_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        从本地获取指定日期的 DailyBasic 数据 (联合 DailyBar 获取成交额)
        :param trade_date: 交易日期 %Y%m%d
        :param ts_codes: 可选，指定股票代码列表进行过滤
        """
        db = SessionLocal()
        try:
            from app.models.stock_models import DailyBasic, DailyBar
            target_date = datetime.strptime(trade_date, '%Y%m%d').date()
            
            # 联合查询以获取 amount
            query = db.query(DailyBasic, DailyBar.amount).join(
                DailyBar, 
                (DailyBasic.ts_code == DailyBar.ts_code) & (DailyBasic.trade_date == DailyBar.trade_date)
            ).filter(DailyBasic.trade_date == target_date)
            
            # [优化] 如果提供了 ts_codes，则增加过滤条件，避免加载全量数据
            if ts_codes:
                query = query.filter(DailyBasic.ts_code.in_(ts_codes))
            
            basics = query.all()
            
            result = []
            for b, amount in basics:
                result.append({
                    "ts_code": b.ts_code,
                    "trade_date": b.trade_date.strftime('%Y%m%d'),
                    "close": b.close,
                    "turnover_rate": b.turnover_rate,
                    "turnover_rate_f": b.turnover_rate_f,
                    "volume_ratio": b.volume_ratio,
                    "pe": b.pe,
                    "pe_ttm": b.pe_ttm,
                    "pb": b.pb,
                    "ps": b.ps,
                    "ps_ttm": b.ps_ttm,
                    "dv_ratio": b.dv_ratio,
                    "dv_ttm": b.dv_ttm,
                    "total_share": b.total_share,
                    "float_share": b.float_share,
                    "free_share": b.free_share,
                    "total_mv": b.total_mv,
                    "circ_mv": b.circ_mv,
                    "amount": amount
                })
            return result
        except Exception as e:
            logger.error(f"读取本地 DailyBasic 失败: {e}")
            return []
        finally:
            db.close()

    def get_latest_trade_date_local(self) -> str:
        """
        从本地获取最新交易日（使用DailyBasic，因为板块分析需要日线基础数据）
        """
        db = SessionLocal()
        try:
            from sqlalchemy import func
            from app.models.stock_models import DailyBasic
            latest_date = db.query(func.max(DailyBasic.trade_date)).scalar()
            if latest_date:
                return latest_date.strftime('%Y%m%d')
            return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        except Exception as e:
            logger.error(f"读取本地最新交易日失败: {e}")
            return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        finally:
            db.close()

    def get_market_counts_local(self, trade_date: str = None) -> Optional[tuple]:
        """
        从本地数据库获取市场涨跌统计
        """
        db = SessionLocal()
        try:
            from app.models.stock_models import MarketCloseCounts
            
            if not trade_date:
                # 获取最新日期的统计
                latest = db.query(MarketCloseCounts).order_by(desc(MarketCloseCounts.date)).first()
                if latest:
                    return (latest.up, latest.down, latest.limit_up, latest.limit_down, latest.flat, latest.amount)
                return None
            else:
                # 获取指定日期的统计
                target_date = datetime.strptime(trade_date, '%Y%m%d').date()
                counts = db.query(MarketCloseCounts).filter(MarketCloseCounts.date == target_date).first()
                if counts:
                    return (counts.up, counts.down, counts.limit_up, counts.limit_down, counts.flat, counts.amount)
                return None
        except Exception as e:
            logger.error(f"读取本地市场统计失败: {e}")
            return None
        finally:
            db.close()

    def get_industry_data_local(self, trade_date: str) -> List[Dict[str, Any]]:
        """
        从本地获取指定日期的 IndustryData 数据
        """
        db = SessionLocal()
        try:
            target_date = datetime.strptime(trade_date, '%Y%m%d').date()
            from app.models.stock_models import IndustryData
            industries = db.query(IndustryData).filter(IndustryData.trade_date == target_date).all()
            
            result = []
            for i in industries:
                item_date = i.trade_date or datetime.now().date()
                result.append({
                    "industry": i.industry,
                    "trade_date": item_date.strftime('%Y%m%d'),
                    "avg_price": i.avg_price,
                    "avg_pct_chg": i.avg_pct_chg,
                    "total_vol": i.total_vol,
                    "total_amount": i.total_amount
                })
            return result
        except Exception as e:
            logger.error(f"读取本地 IndustryData 失败: {e}")
            return []
        finally:
            db.close()

    def get_float_share_legacy(self, ts_code: str) -> Optional[float]:
        """从本地库读取流通股本"""
        # 已被上面的 get_float_share 替代，此处为了不破坏原有结构暂时保留
        return self.get_float_share(ts_code)


# Global Instance
stock_data_service = StockDataService()
