import logging
import json
import re
import asyncio
import math
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.core.config import settings
from app.services.data_provider import data_provider
from app.services.ai.ai_client import ai_client as ai_core_client
from app.services.ai_service import ai_service
from app.services.chat_service import chat_service
from app.services.trading_service import trading_service
from app.services.stock_selector import stock_selector
from app.services.search_service import search_service
from app.services.learning_service import learning_service
from app.db.session import SessionLocal
from app.models.stock_models import Stock, DailyBar, WeeklyBar, MonthlyBar, StockIndicator, TradingPlan, Account, Position, MarketSentiment, OutcomeEvent
from app.services.market.market_utils import get_limit_prices, is_after_market_close
from app.services.market.stock_data_service import stock_data_service

logger = logging.getLogger(__name__)

def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0

class MarketReviewService:
    def __init__(self):
        self.ai_semaphore = asyncio.Semaphore(4) # 限制并发 AI 请求数
        self._intraday_snapshot_cache: Dict[str, Any] = {"date": None, "ts": 0.0, "ladder": None, "turnover_top": None, "ladder_opps": None}

    def _should_keep_tracking(self, plan: TradingPlan) -> bool:
        name = str(plan.strategy_name or "")
        if name.startswith("选股监控-"):
            return True
        return name in {
            "收盘精选",
            "尾盘突击",
            "梯队联动",
            "成交额筛选",
            "首板挖掘",
            "低吸反包",
        }

    def _is_valid_stock(self, ts_code: str, name: str = "") -> bool:
        """
        通用选股过滤规则：
        1. 剔除科创板 (688)
        2. 剔除北交所 (8xx, 4xx, .BJ)
        3. 剔除 ST/退市
        """
        ts_code = str(ts_code or "")
        name = str(name or "")
        
        # 1. 剔除科创板 (688)
        if ts_code.startswith("688"):
            return False
            
        # 2. 剔除北交所 (8xx, 4xx, 92x, .BJ)
        if ts_code.startswith(("8", "4", "92")) or ts_code.endswith(".BJ"):
            return False
            
        # 3. 剔除 ST/退市
        if "ST" in name or "退" in name:
            return False
        if any(ts_code.endswith(s) for s in [".SI", ".HI", ".CSI", ".MSI"]):
            return False
        if ts_code.endswith(".SH") and (ts_code.startswith("000") or ts_code.startswith("880") or ts_code.startswith("930")):
            return False
        if ts_code.endswith(".SZ") and ts_code.startswith("399"):
            return False
        if ts_code.endswith(".BJ") and ts_code.startswith("899"):
            return False
            
        return True

    def _get_account_context(self, db: Session) -> Dict[str, Any]:
        """获取账户资金和持仓上下文"""
        account = db.query(Account).first()
        positions = db.query(Position).filter(Position.vol > 0).all()
        
        pos_list = []
        for p in positions:
            pos_list.append({
                "ts_code": p.ts_code,
                "name": p.name,
                "vol": p.vol,
                "available_vol": p.available_vol,
                "avg_price": p.avg_price,
                "current_price": p.current_price,
                "pnl_pct": p.pnl_pct
            })
        
        today = date.today()
        effective_plan_date = db.query(func.min(TradingPlan.date)).filter(TradingPlan.date >= today).scalar()
        if effective_plan_date is None:
            effective_plan_date = db.query(func.max(TradingPlan.date)).scalar()

        pending_plans = []
        if effective_plan_date:
            pending_plans = db.query(TradingPlan).filter(
                TradingPlan.date == effective_plan_date,
                TradingPlan.executed == False
            ).all()

        plan_list = []
        for pl in pending_plans:
            if (pl.track_status or "").upper() in {"CANCELLED", "FINISHED"}:
                continue
            action = trading_service._infer_plan_action(pl)
            plan_price = float(pl.limit_price or pl.buy_price_limit or 0.0)
            price_text = f"{plan_price:.2f}" if plan_price > 0 else "无"
            status = "监控中" if (pl.track_status or "").upper() == "TRACKING" else "待执行"
            if status != "监控中":
                if action == "BUY":
                    if float(pl.frozen_amount or 0.0) > 0:
                        status = "买入待成"
                    else:
                        status = "待确认"
                elif action == "SELL":
                    status = "卖出待成"
            order_type = (pl.order_type or "MARKET").upper()
            reason_text = str(pl.review_content or pl.reason or "")
            plan_list.append({
                "ts_code": pl.ts_code,
                "action": action,
                "order_type": order_type,
                "price": price_text,
                "status": status,
                "reason": reason_text,
            })

        return {
            "total_assets": account.total_assets if account else 1000000.0,
            "available_cash": account.available_cash if account else 1000000.0,
            "market_value": account.market_value if account else 0.0,
            "positions": pos_list,
            "pending_orders": plan_list
        }

    async def _build_selector_tracking(self, db_unused, review_date: date, intraday: bool, days: int = 7, max_items: int = 20) -> str:
        
        def _get_tracking_data():
            from app.db.session import SessionLocal
            local_db = SessionLocal()
            try:
                start_date = review_date - timedelta(days=max(1, days) - 1)
                event_types = ["selector_default", "selector_pullback"]

                events = (
                    local_db.query(OutcomeEvent)
                    .filter(
                        OutcomeEvent.event_type.in_(event_types),
                        OutcomeEvent.event_date >= start_date,
                        OutcomeEvent.event_date <= review_date,
                    )
                    .order_by(OutcomeEvent.event_date.desc(), OutcomeEvent.created_at.desc())
                    .all()
                )
                if not events:
                    return None, None

                items_by_strategy: Dict[str, Dict[str, Dict[str, Any]]] = {"default": {}, "pullback": {}}
                missing_name_codes = set()

                for e in events:
                    if not e.ts_code:
                        continue
                    strategy = "default"
                    if (e.event_type or "").endswith("_pullback"):
                        strategy = "pullback"

                    payload = {}
                    if e.payload_json:
                        try:
                            payload = json.loads(str(e.payload_json))
                        except Exception:
                            payload = {}

                    name = payload.get("name")
                    if not name:
                        missing_name_codes.add(str(e.ts_code))

                    ts_code_str = str(e.ts_code)
                    rec = items_by_strategy[strategy].get(ts_code_str)
                    if not rec:
                        rec = {
                            "ts_code": ts_code_str,
                            "name": name,
                            "first_date": e.event_date,
                            "last_date": e.event_date,
                            "count": 1,
                            "last_score": payload.get("score"),
                        }
                        items_by_strategy[strategy][ts_code_str] = rec
                    else:
                        rec["count"] += 1
                        if e.event_date < rec["first_date"]:
                            rec["first_date"] = e.event_date
                        if e.event_date > rec["last_date"]:
                            rec["last_date"] = e.event_date
                        if payload.get("score") is not None:
                            rec["last_score"] = payload.get("score")
                        if not rec.get("name") and name:
                            rec["name"] = name

                if missing_name_codes:
                    stocks = local_db.query(Stock).filter(Stock.ts_code.in_(list(missing_name_codes))).all()
                    name_map = {s.ts_code: s.name for s in stocks}
                    for strat in items_by_strategy.values():
                        for v in strat.values():
                            if not v.get("name"):
                                v["name"] = name_map.get(v["ts_code"])

                all_codes = list({*items_by_strategy["default"].keys(), *items_by_strategy["pullback"].keys()})
                if not all_codes:
                    return None, None

                min_first_date = min(
                    [x["first_date"] for strat in items_by_strategy.values() for x in strat.values() if x.get("first_date")]
                )
                bars = (
                    local_db.query(DailyBar)
                    .filter(
                        DailyBar.ts_code.in_(all_codes),
                        DailyBar.trade_date >= min_first_date,
                        DailyBar.trade_date <= review_date,
                    )
                    .order_by(DailyBar.ts_code.asc(), DailyBar.trade_date.asc())
                    .all()
                )
                bars_by_code: Dict[str, List[tuple[date, float]]] = {}
                for b in bars:
                    if not b.ts_code or not b.trade_date:
                        continue
                    bars_by_code.setdefault(str(b.ts_code), []).append((b.trade_date, float(b.close or 0.0)))
                
                return items_by_strategy, bars_by_code
            finally:
                local_db.close()

        items_by_strategy, bars_by_code = await asyncio.to_thread(_get_tracking_data)
        
        if not items_by_strategy:
            return ""

        all_codes = list({*items_by_strategy["default"].keys(), *items_by_strategy["pullback"].keys()})
        quotes = {}
        if intraday and all_codes:
            quotes = await data_provider.get_realtime_quotes(all_codes)

        def _calc_range_pct(ts_code: str, first_date: date):
            series = bars_by_code.get(ts_code, [])
            if not series:
                return None
            start_close = None
            end_close = None
            for d, c in series:
                if d >= first_date and c > 0:
                    start_close = c
                    break
            for d, c in reversed(series):
                if c > 0:
                    end_close = c
                    break
            if start_close is None or start_close <= 0:
                return None
            if intraday:
                q = quotes.get(ts_code) or {}
                q_price = float(q.get("price", 0) or 0)
                if q_price > 0:
                    end_close = q_price
            if end_close is None or end_close <= 0:
                return None
            return (end_close - start_close) / start_close * 100

        def _flatten():
            flat = []
            for strat_key, m in items_by_strategy.items():
                for v in m.values():
                    v = dict(v)
                    v["strategy"] = strat_key
                    v["range_pct"] = _calc_range_pct(v["ts_code"], v["first_date"])
                    flat.append(v)
            flat.sort(
                key=lambda x: (
                    x.get("strategy") != "default",
                    -(float(x.get("last_score") or 0)),
                    -(float(x.get("range_pct") or -9999)),
                )
            )
            return flat

        flat = _flatten()[: max(1, int(max_items))]
        lines = []
        for x in flat:
            name = x.get("name") or x["ts_code"]
            strat_cn = "多维综合" if x["strategy"] == "default" else "强势回调"
            range_txt = ""
            if x.get("range_pct") is not None:
                range_txt = f", 区间{float(x['range_pct']):+.2f}%"
            score_txt = ""
            if x.get("last_score") is not None:
                score_txt = f", 评分{float(x['last_score']):.0f}"
            lines.append(
                f"- {strat_cn} {x['ts_code']} {name}：首次{str(x['first_date'])}，最近{str(x['last_date'])}，入池{x['count']}次{score_txt}{range_txt}"
            )

        total_default = len(items_by_strategy["default"])
        total_pullback = len(items_by_strategy["pullback"])
        header = f"近{days}天选股池：多维综合{total_default}只，强势回调{total_pullback}只（展示前{len(lines)}只）"
        return header + "\n" + "\n".join(lines)

    async def perform_daily_review(self, review_date: date = None, watchlist: List[str] = None, preferred_provider: Optional[str] = "Xiaomi MiMo", api_key: Optional[str] = None):
        """
        [核心] 执行每日收盘复盘 (15:00+)
        对齐 API 和 Scheduler 调用
        """
        now = datetime.now()
        if not review_date:
            # 默认逻辑：如果现在还没收盘(15:30前)，默认复盘日期应为"上一个交易日"
            if now.hour < 15 or (now.hour == 15 and now.minute < 30):
                last_trade_date_str = await data_provider.market_data_service.get_last_trade_date(include_today=False)
                review_date = datetime.strptime(last_trade_date_str, '%Y%m%d').date()
            else:
                review_date = now.date()
        
        logger.info(f"开始执行 {review_date} 的每日复盘...")
        
        db = SessionLocal()
        sentiment_id = None
        try:
            def mark_sentiment_generating():
                existing = db.query(MarketSentiment).filter(MarketSentiment.date == review_date).order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc()).first()
                if existing:
                    existing.main_theme = "生成中"
                    existing.summary = "复盘任务已启动，后台生成中…"
                    existing.updated_at = datetime.now()
                    db.commit()
                    db.refresh(existing)
                    return existing
                sentiment = MarketSentiment(
                    date=review_date,
                    up_count=0,
                    down_count=0,
                    limit_up_count=0,
                    limit_down_count=0,
                    total_volume=0.0,
                    market_temperature=0.0,
                    highest_plate=0,
                    main_theme="生成中",
                    summary="复盘任务已启动，后台生成中…",
                    updated_at=datetime.now(),
                )
                db.add(sentiment)
                db.commit()
                db.refresh(sentiment)
                return sentiment

            sentiment_record = mark_sentiment_generating()
            sentiment_id = sentiment_record.id
            cached_stats = None
            cached_ladder_info = None
            cached_turnover_top = None
            cached_ladder_opps = None
            if sentiment_record:
                if any(
                    [
                        sentiment_record.up_count,
                        sentiment_record.down_count,
                        sentiment_record.limit_up_count,
                        sentiment_record.limit_down_count,
                        sentiment_record.total_volume,
                    ]
                ):
                    cached_stats = {
                        "up": int(sentiment_record.up_count or 0),
                        "down": int(sentiment_record.down_count or 0),
                        "limit_up": int(sentiment_record.limit_up_count or 0),
                        "limit_down": int(sentiment_record.limit_down_count or 0),
                        "total_volume": float(sentiment_record.total_volume or 0.0),
                    }
                try:
                    if sentiment_record.ladder_json:
                        cached_ladder_info = json.loads(str(sentiment_record.ladder_json))
                except Exception:
                    cached_ladder_info = None
                try:
                    if sentiment_record.turnover_top_json:
                        cached_turnover_top = json.loads(str(sentiment_record.turnover_top_json))
                except Exception:
                    cached_turnover_top = None
                try:
                    if sentiment_record.ladder_opportunities_json:
                        cached_ladder_opps = json.loads(str(sentiment_record.ladder_opportunities_json))
                except Exception:
                    cached_ladder_opps = None

            # 1. 获取全市场快照数据 (涨跌家数、涨跌停等)
            logger.info(f"[Review] Step 1: Fetching market stats for {review_date}...")
            stats = {"up": 0, "down": 0, "limit_up": 0, "limit_down": 0, "total_volume": 0.0}
            try:
                trade_date_str = review_date.strftime("%Y%m%d")
                if review_date == date.today():
                    local_counts = await asyncio.to_thread(stock_data_service.get_market_counts_local, trade_date_str)
                    if local_counts and len(local_counts) >= 6:
                        up, down, limit_up, limit_down, flat, amount = local_counts
                        stats = {
                            "up": int(up or 0),
                            "down": int(down or 0),
                            "limit_up": int(limit_up or 0),
                            "limit_down": int(limit_down or 0),
                            "total_volume": float(amount or 0.0),
                        }
                    else:
                        counts = await asyncio.wait_for(
                            data_provider.market_data_service._fetch_market_counts(force_tdx=True),
                            timeout=15.0,
                        )
                        if counts and len(counts) >= 6:
                            up, down, limit_up, limit_down, flat, amount_yi = counts
                            stats = {
                                "up": int(up or 0),
                                "down": int(down or 0),
                                "limit_up": int(limit_up or 0),
                                "limit_down": int(limit_down or 0),
                                "total_volume": float(amount_yi or 0.0),
                            }
                else:
                    local_counts = await asyncio.to_thread(stock_data_service.get_market_counts_local, trade_date_str)
                    if local_counts and len(local_counts) >= 6:
                        up, down, limit_up, limit_down, flat, amount = local_counts
                        stats = {
                            "up": int(up or 0),
                            "down": int(down or 0),
                            "limit_up": int(limit_up or 0),
                            "limit_down": int(limit_down or 0),
                            "total_volume": float(amount or 0.0),
                        }
            except asyncio.TimeoutError:
                logger.warning(f"[Review] Step 1 Timeout: Failed to fetch stats for {review_date}, using empty stats")
            except Exception as e:
                logger.warning(f"[Review] Step 1 Failed to fetch stats for {review_date}: {e}")
            if cached_stats and not any(
                [
                    stats.get("up"),
                    stats.get("down"),
                    stats.get("limit_up"),
                    stats.get("limit_down"),
                    stats.get("total_volume"),
                ]
            ):
                stats = cached_stats
            logger.info(f"[Review] Step 1 Done: up={stats.get('up')}, down={stats.get('down')}")
            
            # 2. 连板天梯与市场情绪分析
            logger.info("[Review] Step 2: Analyzing limit ladder...")
            try:
                # [优化] 增加超时时间到 60s
                ladder_info = await asyncio.wait_for(asyncio.to_thread(self._analyze_limit_ladder, None, review_date), timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning("[Review] Step 2 Timeout: analyze_limit_ladder timed out")
                ladder_info = cached_ladder_info or {"highest": 0, "tiers": {}, "stocks": []}
            
            # [优化] 如果是今天且数据库尚未同步完成(天梯为空)，或者是在交易时间内手动复盘，强制使用实时统计
            # 修改：只要是复盘“今天”，且没拿到数据(可能是数据库未同步或超时)，就尝试用实时接口补救
            # is_intraday = (review_date == date.today() and not is_after_market_close(datetime.now()))
            if review_date == date.today() and (not ladder_info or not ladder_info.get("stocks")):
                logger.info("[Review] Ladder info empty, using realtime limit-up codes for ladder analysis (fallback)")
                try:
                    # [优化] 增加超时时间，防止 TDX 全量获取超时
                    limit_up_codes = await asyncio.wait_for(
                        data_provider.market_data_service.get_realtime_limit_up_codes(),
                        timeout=60.0
                    )
                    if limit_up_codes:
                        # 找到上一个交易日作为计算高度的基准
                        last_trade_date_str = await data_provider.market_data_service.get_last_trade_date(include_today=False)
                        last_trade_date = datetime.strptime(last_trade_date_str, "%Y%m%d").date()
                        ladder_info = await asyncio.wait_for(asyncio.to_thread(self._analyze_limit_ladder_intraday, None, last_trade_date, limit_up_codes), timeout=30.0)
                except Exception as e:
                    logger.warning(f"[Review] Failed to fetch realtime ladder info: {e}")
            if (not ladder_info or not ladder_info.get("stocks")) and cached_ladder_info:
                ladder_info = cached_ladder_info

            temperature = self._calculate_market_temperature(stats, ladder_info)
            turnover_top: List[Dict[str, Any]] = []
            ladder_opps: List[Dict[str, Any]] = []
            
            logger.info("[Review] Step 3: Building turnover top...")
            try:
                turnover_top = await asyncio.wait_for(self._build_turnover_top(None, top_n=80, trade_date=review_date), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("[Review] Step 3 Timeout: build_turnover_top timed out")
                turnover_top = cached_turnover_top or []
            
            logger.info("[Review] Step 4: Filtering turnover top by trend...")
            # 优化：批量过滤或使用 to_thread
            def filter_turnover(top_list):
                return [x for x in (top_list or []) if isinstance(x, dict) and self._passes_trend_filter(None, str(x.get("ts_code") or ""), review_date)]
            
            try:
                turnover_top = await asyncio.wait_for(asyncio.to_thread(filter_turnover, turnover_top), timeout=20.0)
            except asyncio.TimeoutError:
                logger.warning("[Review] Step 4 Timeout: filter_turnover timed out")
            
            logger.info("[Review] Step 5: Building ladder opportunities...")
            try:
                ladder_opps = await asyncio.wait_for(
                    self._build_ladder_opportunities(None, ladder_info or {}, turnover_top),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                logger.warning("[Review] Step 5 Timeout: build_ladder_opportunities timed out")
                ladder_opps = cached_ladder_opps or []
            
            # 3. 搜索全网资讯 (宏观与板块)
            logger.info("[Review] Step 6: Searching market news...")
            try:
                market_news = await asyncio.wait_for(search_service.search_market_news(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("[Review] Search news timed out, skipping...")
                market_news = "搜索资讯超时，已跳过。"
            except Exception as e:
                logger.warning(f"[Review] Search news failed: {e}")
                market_news = "搜索资讯失败，已跳过。"

            # 4. AI 生成深度复盘总结 (注入多周期思维)
            logger.info("[Review] Step 7: Building selector tracking and generating AI summary...")
            selector_tracking = await self._build_selector_tracking(None, review_date, intraday=(review_date == date.today()))
            try:
                summary_data = await asyncio.wait_for(
                    self._generate_ai_market_summary(
                        review_date, stats, ladder_info, temperature, market_news, selector_tracking=selector_tracking,
                        preferred_provider=preferred_provider, api_key=api_key
                    ),
                    timeout=120.0
                )
            except asyncio.TimeoutError:
                logger.warning("[Review] AI summary generation timed out")
                summary_data = {"summary": "AI 生成总结超时", "main_theme": "超时"}
            except Exception as e:
                logger.error(f"[Review] AI summary generation failed: {e}")
                summary_data = {"summary": f"AI 生成总结失败: {str(e)}", "main_theme": "错误"}
            
            # 5. 获取账户上下文
            logger.info("[Review] Step 8: Getting account context...")
            def get_account_ctx():
                from app.db.session import SessionLocal
                local_db = SessionLocal()
                try:
                    return self._get_account_context(local_db)
                finally:
                    local_db.close()
            
            account_context = await asyncio.to_thread(get_account_ctx)

            # 7. 扫描次日机会并生成交易计划
            logger.info("[Review] Step 9: Scanning opportunities...")
            candidate_map: Dict[str, Dict[str, Any]] = {}

            def _add_candidate(ts_code: str, strategy: str, base_reason: str, priority: int, turnover_amount: float = 0.0):
                if not ts_code:
                    return
                ts_code_str = str(ts_code)
                if not self._is_valid_stock(ts_code_str):
                    return
                old = candidate_map.get(ts_code_str)
                priority_val = int(priority or 0)
                turnover_val = float(turnover_amount or 0.0)
                payload = {
                    "ts_code": ts_code_str,
                    "strategy": str(strategy or "收盘精选"),
                    "base_reason": str(base_reason or ""),
                    "priority": priority_val,
                    "turnover_amount": turnover_val,
                }
                if not old:
                    candidate_map[ts_code_str] = payload
                    return
                old_priority = int(old.get("priority") or 0)
                if priority_val < old_priority:
                    old["priority"] = payload["priority"]
                    old["strategy"] = payload["strategy"]
                    old["base_reason"] = payload["base_reason"]
                old_turnover = float(old.get("turnover_amount") or 0.0)
                old["turnover_amount"] = max(old_turnover, turnover_val)

            if watchlist:
                logger.info(f"[Review] Adding {len(watchlist)} watchlist stocks")
                for ts in watchlist:
                    _add_candidate(ts, "收盘精选", "自选股优先池", priority=0)

            opps = await stock_selector.scan_evening_opportunities(review_date.strftime('%Y%m%d'))
            logger.info(f"[Review] stock_selector found {len(opps.get('dragons', []))} dragons")
            
            # [Fix] 限制各选股条件筛选出来的股票数量不超过 3 只
            dragons = (opps.get("dragons") or [])
            if len(dragons) > 3:
                logger.info(f"[Review] Limiting dragons from {len(dragons)} to 3")
                dragons = dragons[:3]
                
            for s in dragons:
                if isinstance(s, dict):
                    ts_code = s.get("ts_code")
                    if ts_code:
                        _add_candidate(str(ts_code), "收盘精选", "强势龙头候选(涨停附近)", priority=2)

            logger.info(f"[Review] Adding {len(ladder_opps or [])} ladder opportunities")
            
            # [Fix] 限制梯队联动机会不超过 3 只
            ladder_candidates = (ladder_opps or [])
            if len(ladder_candidates) > 3:
                logger.info(f"[Review] Limiting ladder opps from {len(ladder_candidates)} to 3")
                ladder_candidates = ladder_candidates[:3]
                
            for x in ladder_candidates:
                if isinstance(x, dict):
                    ts_code = x.get("ts_code")
                    if ts_code:
                        _add_candidate(
                            str(ts_code),
                            "梯队联动",
                            x.get("reason") or "连板梯队板块联动",
                            priority=1,
                            turnover_amount=float(x.get("turnover_amount") or 0.0),
                        )

            logger.info(f"[Review] Adding {len(turnover_top or [])} turnover top stocks")
            
            # [Fix] 限制成交额筛选不超过 3 只
            turnover_candidates = (turnover_top or [])
            if len(turnover_candidates) > 3:
                logger.info(f"[Review] Limiting turnover top from {len(turnover_candidates)} to 3")
                turnover_candidates = turnover_candidates[:3]
                
            for idx, x in enumerate(turnover_candidates):
                if isinstance(x, dict):
                    ts_code = x.get("ts_code")
                    if ts_code:
                        ind = x.get("industry") or ""
                        _add_candidate(
                            str(ts_code),
                            "成交额筛选",
                            f"成交额Top池(排名{idx+1}) {ind}".strip(),
                            priority=3,
                            turnover_amount=float(x.get("turnover_amount") or 0.0),
                        )

            candidates = list(candidate_map.values())
            logger.info(f"[Review] Total unique candidates before filter: {len(candidates)}")
            candidates.sort(key=lambda x: (int(x.get("priority") or 0), -float(x.get("turnover_amount") or 0.0), x.get("ts_code") or ""))

            def filter_candidates(cands):
                filtered = []
                logger.info(f"[Review] Filtering {len(cands)} candidates...")
                for c in cands:
                    try:
                        ts_code = c.get("ts_code")
                        if ts_code and self._is_valid_stock(str(ts_code)) and self._passes_trend_filter(db, str(ts_code), review_date):
                            filtered.append(c)
                    except Exception as e:
                        logger.error(f"[Review] Filter error for {c.get('ts_code')}: {e}")
                        continue
                logger.info(f"[Review] {len(filtered)} candidates passed trend filter")
                return filtered

            prefiltered = []
            try:
                prefiltered = await asyncio.wait_for(
                    asyncio.to_thread(filter_candidates, candidates[:40]),
                    timeout=20.0
                )
                if prefiltered:
                    candidates = prefiltered
            except asyncio.TimeoutError:
                logger.warning("[Review] Step 9 Timeout: filter_candidates timed out")
                candidates = candidates[:40]
                prefiltered = candidates
            
            # 计算计划日期 (下一个交易日)
            trade_cal = await data_provider.check_trade_day(review_date.strftime('%Y%m%d'))
            next_trade_date_str = trade_cal.get('next_trade_date')
            if next_trade_date_str:
                plan_date = datetime.strptime(next_trade_date_str, '%Y%m%d').date()
            else:
                # 兜底逻辑
                plan_date = review_date + timedelta(days=1 if review_date.weekday() < 4 else 3)
            
            logger.info(f"复盘日期: {review_date}, 计划执行日期: {plan_date}")

            # 7.1 生成选股建议计划 (并发)
            min_confidence = 55  # [Active Exploration] 降低门槛以增加试错样本
            min_watch_confidence = 45
            max_ai_analyze = 20
            max_plans = 10
            
            logger.info(f"[Review] Start generating target plans. Prefiltered candidates: {len(prefiltered)}")

            async def generate_target_plan(cand: Dict[str, Any]):
                ts_code = str(cand.get("ts_code") or "")
                strategy = str(cand.get("strategy") or "收盘精选")
                base_reason = str(cand.get("base_reason") or "每日复盘自动生成")
                async with self.ai_semaphore:
                    stock_info = {"ts_code": ts_code, "price": 0}
                    plan_dict = await self._generate_ai_plan(
                        stock_info, strategy, base_reason, review_date, account_context,
                        preferred_provider=preferred_provider, api_key=api_key
                    )
                    if not plan_dict:
                        return None
                    action = str(plan_dict.get("action") or "BUY").upper()
                    score = float(plan_dict.get("score") or 0)
                    if action == "BUY" and score >= min_confidence:
                        accel_ok, accel_reason = await trading_service._is_buy_accel_allowed(ts_code)
                        if not accel_ok:
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            watch_reason = f"[{decision_time} 风控拦截] 仅允许进入/临近加速段（{accel_reason}）"
                            try:
                                plan = await trading_service.create_plan(
                                    ts_code=ts_code,
                                    strategy_name=plan_dict['strategy'],
                                    buy_price=0,
                                    stop_loss=0,
                                    take_profit=0,
                                    position_pct=plan_dict.get('position_pct', 0.1),
                                    reason=f"{plan_dict['reason']} | {watch_reason}",
                                    plan_date=plan_date,
                                    score=plan_dict.get('score', 0),
                                    source="system",
                                    ai_decision="WAIT"
                                )
                                if not plan:
                                    return None
                                plan_id = plan.id
                                if plan_id is not None:
                                    await trading_service.update_plan_review(
                                        plan_id,
                                        f"[{decision_time} AI观望] {plan_dict['reason']}",
                                        ai_decision="WAIT",
                                        decision_price=plan_dict.get("decision_price") or 0,
                                    )
                            except ValueError as e:
                                logger.warning(f"Skip creating watch plan for {ts_code}: {e}")
                            return None
                        try:
                            await trading_service.create_plan(
                                ts_code=ts_code,
                                strategy_name=plan_dict['strategy'],
                                buy_price=plan_dict['target_price'],
                                stop_loss=plan_dict['target_price'] * 0.92,
                                take_profit=plan_dict['target_price'] * 1.15,
                                position_pct=plan_dict.get('position_pct', 0.1),
                                reason=plan_dict['reason'],
                                plan_date=plan_date,
                                score=plan_dict.get('score', 0),
                                source="system",
                                ai_decision="BUY"
                            )
                        except ValueError as e:
                            logger.warning(f"Skip creating plan for {ts_code}: {e}")
                            return None
                        return plan_dict
                    if action == "WAIT" and score >= min_watch_confidence:
                        try:
                            plan = await trading_service.create_plan(
                                ts_code=ts_code,
                                strategy_name=plan_dict['strategy'],
                                buy_price=0,
                                stop_loss=0,
                                take_profit=0,
                                position_pct=plan_dict.get('position_pct', 0.1),
                                reason=plan_dict['reason'],
                                plan_date=plan_date,
                                score=plan_dict.get('score', 0),
                                source="system",
                                ai_decision="WAIT"
                            )
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            if not plan:
                                return None
                            plan_id = plan.id
                            if plan_id is not None:
                                await trading_service.update_plan_review(
                                    plan_id,
                                    f"[{decision_time} AI观望] {plan_dict['reason']}",
                                    ai_decision="WAIT",
                                    decision_price=plan_dict.get("decision_price") or 0,
                                )
                        except ValueError as e:
                            logger.warning(f"Skip creating plan for {ts_code}: {e}")
                        return None
                    return None

            target_tasks = [generate_target_plan(c) for c in candidates[:max_ai_analyze]]
            target_results = await asyncio.gather(*target_tasks)
            target_plans = [r for r in target_results if r]
            target_plans.sort(key=lambda x: float(x.get("score") or 0), reverse=True)
            target_plans = target_plans[:max_plans]

            # 8. 对当前持仓生成管理建议 (并发)
            async def generate_holding_plan(pos):
                async with self.ai_semaphore:
                    stock_info = {"ts_code": pos['ts_code'], "price": pos['current_price']}
                    plan_dict = await self._generate_ai_plan(
                        stock_info, "持仓管理", f"当前盈利: {pos['pnl_pct']}%", review_date, account_context,
                        preferred_provider=preferred_provider, api_key=api_key
                    )
                    if plan_dict:
                        action = plan_dict.get('action', 'HOLD')
                        strategy_name = "持仓卖出" if action == 'SELL' else "持仓持有"
                        await trading_service.create_plan(
                            ts_code=pos['ts_code'],
                            strategy_name=strategy_name,
                            buy_price=0,
                            stop_loss=0,
                            take_profit=0,
                            reason=plan_dict['reason'],
                            plan_date=plan_date,
                            score=plan_dict.get('score', 0),
                            source="system",
                            ai_decision=action
                        )
                    return plan_dict

            holding_tasks = [generate_holding_plan(pos) for pos in account_context.get('positions', [])]
            holding_results = await asyncio.gather(*holding_tasks)
            holding_plans = [r for r in holding_results if r]

            # [新增] 9. 晚间监控计划清洗 (Monitor Cleanup)
            # 对当前处于 TRACKING 状态的计划进行复核，如果不再符合监控条件（如破位、超时），则移除监控
            # 这通常在生成总结之前执行，以确保第二天早上监控列表是最新的
            try:
                logger.info("Step 9: Cleaning up tracking plans...")
                await self._cleanup_tracking_plans(review_date)
            except Exception as e:
                logger.error(f"Error cleaning up tracking plans: {e}")

            import json
            def finalize_sentiment():
                # 再次获取 record，确保 session 是最新的
                existing = db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()
                if not existing:
                    existing = MarketSentiment(date=review_date)
                    db.add(existing)
                
                # 核心修正：直接使用 get_market_snapshot 返回的 stats，确保与 880005 对齐
                existing.up_count = int(stats.get("up", 0))
                existing.down_count = int(stats.get("down", 0))
                existing.limit_up_count = int(stats.get("limit_up", 0))
                existing.limit_down_count = int(stats.get("limit_down", 0))
                # 注意：total_volume 在 snapshot 中已经是亿元单位
                existing.total_volume = float(stats.get("total_volume", 0.0))
                existing.market_temperature = float(temperature)
                existing.highest_plate = int((ladder_info or {}).get("highest") or 0)
                existing.main_theme = summary_data.get("main_theme", "暂无")
                existing.summary = summary_data.get("summary", "复盘生成失败")
                
                # 保存完整的分析结果
                existing.ladder_json = json.dumps(ladder_info, ensure_ascii=False) if ladder_info else None
                existing.turnover_top_json = json.dumps(turnover_top, ensure_ascii=False) if turnover_top else None
                existing.ladder_opportunities_json = json.dumps(ladder_opps, ensure_ascii=False) if ladder_opps else None
                
                existing.updated_at = datetime.now()
                db.commit()
                db.refresh(existing)
                return existing

            sentiment_final = finalize_sentiment()
            
            # [核心] 触发学习与反思闭环 (Phase 2 核心)
            # 在复盘数据落库后，立即启动学习任务，以便生成的交易计划能利用最新的反思记忆
            try:
                from app.services.learning_service import learning_service
                logger.info("[Review] Triggering learning & reflection loop...")
                # 使用 create_task 异步运行，不阻塞复盘主流程返回
                asyncio.create_task(learning_service.perform_daily_learning())
            except Exception as e:
                logger.error(f"[Review] Failed to trigger learning loop: {e}")

            # 返回 API 要求的格式
            return {
                "date": review_date.strftime('%Y-%m-%d'),
                "up": int(stats.get('up', 0)),
                "down": int(stats.get('down', 0)),
                "limit_up": int(stats.get('limit_up', 0)),
                "limit_down": int(stats.get('limit_down', 0)),
                "total_volume": float(stats.get('total_volume', 0.0)),
                "temp": float(temperature),
                "highest_plate": int((ladder_info or {}).get("highest") or 0),
                "ladder": ladder_info,
                "turnover_top": turnover_top,
                "ladder_opportunities": ladder_opps,
                "summary": summary_data.get('summary', ""),
                "main_theme": summary_data.get('main_theme', ""),
                "target_plan": target_plans[0] if target_plans else None,
                "target_plans": target_plans,
                "holding_plans": holding_plans,
                "created_at": sentiment_final.updated_at
            }
        except Exception as e:
            logger.error(f"每日复盘执行出错: {e}", exc_info=True)
            # 尝试在出错时更新数据库状态，避免前端卡死
            try:
                if sentiment_id:
                    err_existing = db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()
                    if err_existing:
                        err_existing.main_theme = "生成失败"
                        err_existing.summary = f"复盘任务执行出错: {str(e)}"
                        err_existing.updated_at = datetime.now()
                        db.commit()
            except Exception as db_err:
                logger.error(f"Failed to update error status to DB: {db_err}")
            return None
        finally:
            db.close()

    async def perform_noon_review(self, watchlist: List[str] = None, preferred_provider: Optional[str] = "Xiaomi MiMo", api_key: Optional[str] = None):
        """
        [核心] 午间复盘逻辑 (11:30 - 13:00)
        """
        logger.info("开始执行午间复盘...")
        sentiment_id = None
        db = SessionLocal()
        try:
            # 必须 await
            logger.info("[NoonReview] Step 1: Fetching market snapshot...")
            try:
                stats = await asyncio.wait_for(data_provider.get_market_snapshot(), timeout=25.0)
            except Exception as e:
                logger.warning(f"[NoonReview] Market snapshot failed: {e}")
                stats = {
                    "up": 0,
                    "down": 0,
                    "limit_up": 0,
                    "limit_down": 0,
                    "total_volume": 0.0,
                    "time": datetime.now().strftime('%H:%M:%S'),
                }
            if not stats.get("total_volume") and stats.get("total_amount_亿元") is not None:
                stats["total_volume"] = float(stats.get("total_amount_亿元") or 0.0)

            if not any([stats.get("up"), stats.get("down"), stats.get("limit_up"), stats.get("limit_down")]):
                try:
                    res_6 = await asyncio.wait_for(
                        data_provider.market_data_service._fetch_market_counts(force_tdx=True),
                        timeout=10.0,
                    )
                except Exception:
                    res_6 = None
                if res_6 and len(res_6) >= 6 and data_provider.market_data_service._is_counts_plausible(res_6[:5]):
                    up, down, limit_up, limit_down, flat, amount = res_6
                    stats = {
                        "up": int(up or 0),
                        "down": int(down or 0),
                        "limit_up": int(limit_up or 0),
                        "limit_down": int(limit_down or 0),
                        "total_volume": float(amount or 0.0),
                        "time": datetime.now().strftime('%H:%M:%S'),
                    }
            
            def mark_sentiment_generating():
                from app.db.session import SessionLocal
                local_db = SessionLocal()
                try:
                    today = date.today()
                    existing = local_db.query(MarketSentiment).filter(MarketSentiment.date == today).order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc()).first()
                    if existing:
                        existing.main_theme = "生成中"
                        existing.summary = "午间复盘任务已启动，后台生成中…"
                        existing.updated_at = datetime.now()
                        local_db.commit()
                        local_db.refresh(existing)
                        return existing.id
                    sentiment = MarketSentiment(
                        date=today,
                        up_count=int(stats.get('up', 0)),
                        down_count=int(stats.get('down', 0)),
                        limit_up_count=int(stats.get('limit_up', 0)),
                        limit_down_count=int(stats.get('limit_down', 0)),
                        total_volume=float(stats.get('total_volume', 0.0)),
                        market_temperature=0.0,
                        highest_plate=0,
                        main_theme="生成中",
                        summary="午间复盘任务已启动，后台生成中…",
                        updated_at=datetime.now(),
                    )
                    local_db.add(sentiment)
                    local_db.commit()
                    local_db.refresh(sentiment)
                    return sentiment.id
                finally:
                    local_db.close()

            sentiment_id = await asyncio.to_thread(mark_sentiment_generating)

            logger.info("[NoonReview] Step 2: Fetching realtime limit-up codes...")
            limit_up_codes: List[str] = []
            try:
                # [优化] 增加超时时间，防止 TDX 全量获取超时
                limit_up_codes = await asyncio.wait_for(
                    data_provider.market_data_service.get_realtime_limit_up_codes(),
                    timeout=60.0,
                )
            except Exception as e:
                logger.warning(f"[NoonReview] Failed to fetch realtime limit-up codes: {e}")
                limit_up_codes = []

            logger.info("[NoonReview] Step 3: Analyzing limit ladder...")
            end_date = date.today()
            try:
                last_trade_date_str = await data_provider.market_data_service.get_last_trade_date(include_today=False)
                if last_trade_date_str:
                    end_date = datetime.strptime(str(last_trade_date_str), "%Y%m%d").date()
            except Exception:
                end_date = date.today()

            try:
                ladder_info = await asyncio.wait_for(
                    asyncio.to_thread(self._analyze_limit_ladder_intraday, db, end_date, limit_up_codes),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning("[NoonReview] Step 3 Timeout: analyze_limit_ladder_intraday timed out")
                ladder_info = None
            if not ladder_info or not ladder_info.get("stocks"):
                cache = self._intraday_snapshot_cache or {}
                cached_ladder = cache.get("ladder")
                if cache.get("date") == date.today() and isinstance(cached_ladder, dict):
                    ladder_info = cached_ladder
                else:
                    ladder_info = {"highest": 0, "tiers": {}, "stocks": []}
            
            temperature = self._calculate_market_temperature(stats, ladder_info)
            
            # 1. 获取午间新闻
            logger.info("[NoonReview] Step 4: Searching market news...")
            try:
                news_str = await asyncio.wait_for(search_service.search_market_news(), timeout=30.0)
            except Exception as e:
                logger.warning(f"[NoonReview] Search news failed: {e}")
                news_str = "搜索资讯失败。"

            # 2. 生成 AI 复盘总结
            logger.info("[NoonReview] Step 5: Generating AI summary...")
            selector_tracking = await self._build_selector_tracking(db, date.today(), intraday=True)
            try:
                summary_data = await asyncio.wait_for(
                    self._generate_ai_market_summary(
                        date.today(), stats, ladder_info, temperature, news_str, selector_tracking=selector_tracking,
                        preferred_provider=preferred_provider, api_key=api_key
                    ),
                    timeout=120.0
                )
            except Exception as e:
                err_msg = str(e) or type(e).__name__
                logger.error(f"[NoonReview] AI summary failed: {err_msg}")
                summary_data = {"summary": f"AI 生成失败: {err_msg}", "main_theme": "错误"}

            logger.info("[NoonReview] Step 6: Building turnover top and opportunities...")
            try:
                turnover_top = await asyncio.wait_for(
                    self._build_turnover_top(db, top_n=80, trade_date=date.today()),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                logger.warning("[NoonReview] Step 6 Timeout: build_turnover_top timed out")
                turnover_top = []
            
            def filter_turnover(top_list):
                return [
                    x
                    for x in (top_list or [])
                    if isinstance(x, dict)
                    and x.get("ts_code")
                    and self._is_valid_stock(str(x.get("ts_code")), str(x.get("name", "")))
                    and self._passes_trend_filter(db, str(x.get("ts_code")), date.today())
                ]
            
            try:
                turnover_top = await asyncio.wait_for(
                    asyncio.to_thread(filter_turnover, turnover_top),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                logger.warning("[NoonReview] Step 6 Timeout: filter_turnover timed out")

            try:
                ladder_opps = await asyncio.wait_for(
                    self._build_ladder_opportunities(db, ladder_info or {}, turnover_top),
                    timeout=20.0
                )
            except asyncio.TimeoutError:
                logger.warning("[NoonReview] Step 6 Timeout: build_ladder_opportunities timed out")
                ladder_opps = []
            if not turnover_top or not ladder_opps:
                cache = self._intraday_snapshot_cache or {}
                if cache.get("date") == date.today():
                    if not turnover_top:
                        turnover_top = cache.get("turnover_top") or []
                    if not ladder_opps:
                        ladder_opps = cache.get("ladder_opps") or []

            # 3. 扫描午间机会
            logger.info("[NoonReview] Step 7: Scanning noon opportunities...")
            try:
                opps = await asyncio.wait_for(stock_selector.scan_noon_opportunities(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning("[NoonReview] Step 7 Timeout: scan_noon_opportunities timed out")
                opps = {}
            
            def get_noon_account_ctx():
                from app.db.session import SessionLocal
                local_db = SessionLocal()
                try:
                    return self._get_account_context(local_db)
                finally:
                    local_db.close()
            
            account_context = await asyncio.to_thread(get_noon_account_ctx)

            combined_codes = []
            if watchlist: combined_codes.extend(watchlist)
            dragons = opps.get('dragons', [])
            combined_codes.extend([str(s["ts_code"]) for s in dragons if isinstance(s, dict) and s.get("ts_code")])
            combined_codes.extend([str(x.get("ts_code")) for x in (ladder_opps or []) if isinstance(x, dict) and x.get("ts_code")])
            combined_codes.extend([str(x.get("ts_code")) for x in (turnover_top or [])[:20] if isinstance(x, dict) and x.get("ts_code")])
            
            unique_codes = list(dict.fromkeys(combined_codes))[:25]
            
            def filter_unique(codes):
                filtered = []
                for ts in codes:
                    try:
                        # 先检查是否有效股票（剔除ST/科创/北交）
                        # 注意：这里可能缺少 name 信息，只能先按代码过滤
                        # 如果需要 name 过滤，得在前面获取时就带上
                        if not self._is_valid_stock(str(ts)):
                            continue
                            
                        if ts and self._passes_trend_filter(db, str(ts), date.today()):
                            filtered.append(ts)
                    except Exception:
                        continue
                return filtered
            
            try:
                filtered_codes = await asyncio.wait_for(
                    asyncio.to_thread(filter_unique, unique_codes),
                    timeout=20.0
                )
                if filtered_codes:
                    unique_codes = filtered_codes[:15]
            except asyncio.TimeoutError:
                logger.warning("[NoonReview] Step 7 Timeout: filter_unique timed out")
                unique_codes = unique_codes[:10] # 降级处理
            
            plan_date = date.today()
            min_confidence = 60  # [Active Exploration] 降低午间门槛
            min_watch_confidence = 50
            max_plans = 8

            logger.info("[NoonReview] Step 8: Generating target plans...")
            target_plans = []
            quotes = {}
            if unique_codes:
                try:
                    quotes = await asyncio.wait_for(
                        data_provider.get_realtime_quotes(unique_codes, force_tdx=True),
                        timeout=20.0
                    )
                except Exception:
                    quotes = {}
            async def generate_target_plan(ts_code):
                async with self.ai_semaphore:
                    quote = quotes.get(ts_code) or {}
                    price = float(quote.get("price") or 0)
                    stock_info = {"ts_code": ts_code, "price": price}
                    plan_dict = await self._generate_ai_plan(
                        stock_info, "午间强势", "午间扫描机会", date.today(), account_context,
                        preferred_provider=preferred_provider, api_key=api_key
                    )
                    if not plan_dict:
                        return None
                    action = str(plan_dict.get("action") or "BUY").upper()
                    score = float(plan_dict.get("score") or 0)
                    if action == "BUY" and score >= min_confidence:
                        accel_ok, accel_reason = await trading_service._is_buy_accel_allowed(ts_code)
                        if not accel_ok:
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            watch_reason = f"[{decision_time} 风控拦截] 仅允许进入/临近加速段（{accel_reason}）"
                            try:
                                plan = await trading_service.create_plan(
                                    ts_code=ts_code,
                                    strategy_name="选股监控-午间强势",
                                    buy_price=0,
                                    stop_loss=0,
                                    take_profit=0,
                                    position_pct=plan_dict.get('position_pct', 0.1),
                                    reason=f"{plan_dict['reason']} | {watch_reason}",
                                    plan_date=plan_date,
                                    score=plan_dict.get('score', 0),
                                    source="system",
                                    ai_decision="WAIT"
                                )
                                decision_time = datetime.now().strftime("%H:%M:%S")
                                plan_id = plan.id if plan else None
                                if plan_id is not None:
                                    await trading_service.update_plan_review(
                                        plan_id,
                                        f"[{decision_time} AI观望] {plan_dict['reason']}",
                                        ai_decision="WAIT",
                                        decision_price=plan_dict.get("decision_price") or 0,
                                    )
                            except ValueError as e:
                                logger.warning(f"Skip creating watch plan for {ts_code}: {e}")
                            return None
                        try:
                            target_price = float(plan_dict.get("target_price") or 0)
                            if target_price <= 0 and price > 0:
                                target_price = price
                                plan_dict["target_price"] = target_price
                            if target_price <= 0:
                                plan = await trading_service.create_plan(
                                    ts_code=ts_code,
                                    strategy_name="选股监控-午间强势",
                                    buy_price=0,
                                    stop_loss=0,
                                    take_profit=0,
                                    position_pct=plan_dict.get('position_pct', 0.1),
                                    reason=f"{plan_dict['reason']} | 缺少参考价",
                                    plan_date=plan_date,
                                    score=plan_dict.get('score', 0),
                                    source="system",
                                    ai_decision="WAIT"
                                )
                                decision_time = datetime.now().strftime("%H:%M:%S")
                                plan_id = plan.id if plan else None
                                if plan_id is not None:
                                    await trading_service.update_plan_review(
                                        plan_id,
                                        f"[{decision_time} AI观望] {plan_dict['reason']}",
                                        ai_decision="WAIT",
                                        decision_price=price,
                                    )
                                return None
                            await trading_service.create_plan(
                                ts_code=ts_code,
                                strategy_name=plan_dict['strategy'],
                                buy_price=target_price,
                                stop_loss=target_price * 0.92,
                                take_profit=target_price * 1.15,
                                position_pct=plan_dict.get('position_pct', 0.1),
                                reason=plan_dict['reason'],
                                plan_date=plan_date,
                                score=plan_dict.get('score', 0),
                                source="system",
                                ai_decision="BUY"
                            )
                        except ValueError as e:
                            logger.warning(f"Skip creating plan for {ts_code}: {e}")
                            return None
                        return plan_dict
                    if action == "WAIT" and score >= min_watch_confidence:
                        try:
                            plan = await trading_service.create_plan(
                                ts_code=ts_code,
                                strategy_name="选股监控-午间强势",
                                buy_price=0,
                                stop_loss=0,
                                take_profit=0,
                                position_pct=plan_dict.get('position_pct', 0.1),
                                reason=plan_dict['reason'],
                                plan_date=plan_date,
                                score=plan_dict.get('score', 0),
                                source="system",
                                ai_decision="WAIT"
                            )
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            plan_id = plan.id if plan else None
                            if plan_id is not None:
                                await trading_service.update_plan_review(
                                    plan_id,
                                    f"[{decision_time} AI观望] {plan_dict['reason']}",
                                    ai_decision="WAIT",
                                    decision_price=plan_dict.get("decision_price") or 0,
                                )
                        except ValueError as e:
                            logger.warning(f"Skip creating watch plan for {ts_code}: {e}")
                        return None
                    return None

            target_tasks = [generate_target_plan(ts) for ts in unique_codes]
            target_results = await asyncio.gather(*target_tasks)
            target_plans = [r for r in target_results if r][:max_plans]

            # 4. 对持仓生成建议
            logger.info("[NoonReview] Step 9: Generating holding plans...")
            holding_plans = []
            async def generate_holding_plan(pos):
                async with self.ai_semaphore:
                    stock_info = {"ts_code": pos['ts_code'], "price": pos['current_price']}
                    plan_dict = await self._generate_ai_plan(
                        stock_info, "午间持仓管理", f"盈亏: {pos['pnl_pct']}%", date.today(), account_context,
                        preferred_provider=preferred_provider, api_key=api_key
                    )
                    if plan_dict:
                        action = plan_dict.get('action', 'HOLD')
                        strategy_name = "持仓卖出" if action == 'SELL' else "持仓持有"
                        
                        # [Fix] 增加 T+1 可用持仓校验
                        avail_vol = int(pos.get('available_vol', 0))
                        if action == 'SELL' and avail_vol <= 0:
                            logger.info(f"AI(午间)建议卖出 {pos['ts_code']}，但可用持仓为 0 (受 T+1 限制或已冻结)，自动拦截。")
                            # 改为持有，或直接忽略
                            action = 'HOLD'
                            strategy_name = "持仓持有"
                            plan_dict['action'] = 'HOLD'
                            plan_dict['reason'] = f"{plan_dict['reason']} | (T+1限制拦截卖出)"
                        
                        await trading_service.create_plan(
                            ts_code=pos['ts_code'],
                            strategy_name=strategy_name,
                            buy_price=0,
                            stop_loss=0,
                            take_profit=0,
                            reason=plan_dict['reason'],
                            plan_date=plan_date,
                            score=plan_dict.get('score', 0),
                            source="system",
                            ai_decision=action
                        )
                        return plan_dict
                    return None

            holding_tasks = [generate_holding_plan(pos) for pos in account_context.get('positions', [])]
            holding_results = await asyncio.gather(*holding_tasks)
            holding_plans = [r for r in holding_results if r]

            # 5. 保存午间复盘结果
            def finalize_noon_sentiment():
                from app.db.session import SessionLocal
                local_db = SessionLocal()
                try:
                    existing = local_db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()
                    if not existing:
                        existing = MarketSentiment(date=date.today())
                        local_db.add(existing)
                    
                    existing.up_count = int(stats.get('up', 0))
                    existing.down_count = int(stats.get('down', 0))
                    existing.limit_up_count = int(stats.get('limit_up', 0))
                    existing.limit_down_count = int(stats.get('limit_down', 0))
                    existing.total_volume = float(stats.get('total_volume', 0.0))
                    existing.market_temperature = float(temperature)
                    existing.highest_plate = int((ladder_info or {}).get("highest") or 0)
                    existing.main_theme = summary_data.get('main_theme', "午间热点")
                    existing.summary = summary_data.get('summary', "")
                    
                    existing.ladder_json = json.dumps(ladder_info, ensure_ascii=False) if ladder_info else None
                    existing.turnover_top_json = json.dumps(turnover_top, ensure_ascii=False) if turnover_top else None
                    existing.ladder_opportunities_json = json.dumps(ladder_opps, ensure_ascii=False) if ladder_opps else None
                    
                    existing.updated_at = datetime.now()
                    local_db.commit()
                    local_db.refresh(existing)
                    return existing.updated_at
                finally:
                    local_db.close()

            sentiment_final_updated_at = await asyncio.to_thread(finalize_noon_sentiment)

            return {
                "date": date.today().strftime('%Y-%m-%d'),
                "up": int(stats.get('up', 0)),
                "down": int(stats.get('down', 0)),
                "limit_up": int(stats.get('limit_up', 0)),
                "limit_down": int(stats.get('limit_down', 0)),
                "total_volume": float(stats.get('total_volume', 0.0)),
                "temp": float(temperature),
                "highest_plate": int((ladder_info or {}).get("highest") or 0),
                "ladder": ladder_info,
                "turnover_top": turnover_top,
                "ladder_opportunities": ladder_opps,
                "summary": summary_data.get('summary', ""),
                "main_theme": summary_data.get('main_theme', ""),
                "target_plans": target_plans,
                "holding_plans": holding_plans,
                "created_at": sentiment_final_updated_at
            }
        except Exception as e:
            logger.error(f"午间复盘执行出错: {e}", exc_info=True)
            try:
                if sentiment_id:
                    err_msg = str(e)
                    def update_error():
                        from app.db.session import SessionLocal
                        local_db = SessionLocal()
                        try:
                            err_existing = local_db.query(MarketSentiment).filter(MarketSentiment.id == sentiment_id).first()
                            if err_existing:
                                err_existing.main_theme = "生成失败"
                                err_existing.summary = f"午间复盘执行出错: {err_msg}"
                                err_existing.updated_at = datetime.now()
                                local_db.commit()
                        finally:
                            local_db.close()
                    await asyncio.to_thread(update_error)
            except Exception as db_err:
                logger.error(f"Failed to update noon error status to DB: {db_err}")
            return None
        finally:
            db.close()

    async def perform_ai_periodic_monitor(self):
        logger.info("执行 15分钟 AI 定期巡检(仅监控列表)")
        def get_account_ctx():
            db = SessionLocal()
            try:
                return self._get_account_context(db)
            finally:
                db.close()
        account_context = await asyncio.to_thread(get_account_ctx)
        rising_keywords = ["起涨", "上涨", "启动", "趋势初期", "主升", "突破", "放量", "弱转强", "二波"]
        top_keywords = ["顶部", "高位", "滞涨", "背离", "破位", "上影", "缩量", "放量阴", "钝化", "超买"]
        rising_memories = await learning_service.get_reflection_memories_by_keywords(
            ["盘中异动", "通用"],
            rising_keywords,
            limit=5,
            source_event_type="PATTERN_CASE"
        )
        top_memories = await learning_service.get_reflection_memories_by_keywords(
            ["盘中异动", "通用"],
            top_keywords,
            limit=5,
            source_event_type="PATTERN_CASE"
        )
        memory_injection = ""
        if rising_memories:
            memory_injection += "\n【AI反思-上涨特征】\n" + rising_memories
        if top_memories:
            memory_injection += "\n【AI反思-顶部风险】\n" + top_memories
        await self._run_ai_periodic_monitor(account_context, include_positions=False, include_tracking=True)

    async def perform_position_periodic_monitor(self):
        logger.info("执行 5分钟 持仓 AI 跟踪")
        def get_account_ctx():
            db = SessionLocal()
            try:
                return self._get_account_context(db)
            finally:
                db.close()
        account_context = await asyncio.to_thread(get_account_ctx)
        await self._run_ai_periodic_monitor(account_context, include_positions=True, include_tracking=False)

    async def _cleanup_tracking_plans(self, review_date: date):
        """
        [新增] 晚间清洗监控计划
        1. 移除超过 5 天未触发的监控
        2. 移除已经严重破位的监控 (如跌破止损位且 AI 建议放弃)
        """
        db = SessionLocal()
        try:
            # 1. 获取所有 TRACKING 状态的计划
            plans = db.query(TradingPlan).filter(
                TradingPlan.track_status == "TRACKING",
                TradingPlan.executed == False
            ).all()
            
            if not plans:
                return

            logger.info(f"Checking {len(plans)} tracking plans for cleanup...")
            
            if len(plans) > 30:
                logger.info(f"Monitor list size ({len(plans)}) > 30, triggering MA/Deviation cleanup...")
                pos_codes = {str(r[0]) for r in db.query(Position.ts_code).filter(Position.vol > 0).all() if r and r[0]}
                ts_codes = [p.ts_code for p in plans if p.ts_code]
                indicator_map = await asyncio.to_thread(stock_data_service.get_latest_indicators_batch, ts_codes, None)
                quotes = await data_provider.get_realtime_quotes(ts_codes)
                removable = []
                for p in plans:
                    ts_code = p.ts_code
                    if not ts_code or ts_code in pos_codes:
                        continue
                    ind = indicator_map.get(ts_code) or {}
                    ma5 = float(ind.get("ma5") or 0.0)
                    bias5 = float(ind.get("bias5") or 0.0)
                    prev_rows = db.query(StockIndicator.ma5).filter(
                        StockIndicator.ts_code == ts_code
                    ).order_by(StockIndicator.trade_date.desc()).limit(2).all()
                    ma5_prev = float(prev_rows[1][0] or 0.0) if len(prev_rows) > 1 else 0.0
                    q = quotes.get(ts_code) or {}
                    price = float(q.get("price") or 0.0)
                    if price <= 0:
                        price = float(ind.get("close") or 0.0)
                    ma_down = ma5 > 0 and ma5_prev > 0 and ma5 < ma5_prev
                    far_above = bias5 > 7.0
                    if ma_down or far_above:
                        removable.append((p, ma_down, bias5))

                excess_count = len(plans) - 30
                removable.sort(key=lambda x: (1 if x[1] else 0, x[2]), reverse=True)
                for p, _, _ in removable[:excess_count]:
                    logger.info(f"Removing excess tracking plan {p.ts_code} (MA/Deviation)")
                    p.track_status = "REMOVED_OVERFLOW"
                    p.review_content = f"[{review_date} AI清洗] 监控列表超限(>30)，均线/乖离过滤"

                plans = [p for p in plans if (p.track_status or "").upper() != "REMOVED_OVERFLOW"]
                if len(plans) > 30:
                    still_excess = len(plans) - 30
                    candidates = [p for p in plans if p.ts_code not in pos_codes]
                    candidates.sort(key=lambda x: (float(x.score or 0), x.date), reverse=False)
                    for p in candidates[:still_excess]:
                        logger.info(f"Removing excess tracking plan {p.ts_code} (Score: {p.score}, Date: {p.date})")
                        p.track_status = "REMOVED_OVERFLOW"
                        p.review_content = f"[{review_date} AI清洗] 监控列表超限(>30)，末位淘汰"
                    plans = [p for p in plans if (p.track_status or "").upper() != "REMOVED_OVERFLOW"]

            for plan in plans:
                # 规则 1: 超时移除 (默认 5 天)
                # track_days 可能会在盘中更新，这里再算一次天数
                days_since = (review_date - plan.date).days
                if days_since > 5:
                    logger.info(f"Removing tracking plan {plan.ts_code} (Expired: {days_since} days)")
                    plan.track_status = "EXPIRED"
                    plan.review_content = f"[{review_date} AI清洗] 监控超时({days_since}天)，自动移除"
                    continue
                
                # 规则 2: 严重破位 (跌幅超过 15% 或 跌破止损位 10% 以上)
                # 需要获取今日收盘价
                try:
                    quote = await data_provider.get_realtime_quote(plan.ts_code)
                    current_price = float(quote.get('price', 0))
                    
                    if current_price > 0:
                        # 检查止损位
                        stop_loss = plan.stop_loss_price or 0
                        if stop_loss > 0 and current_price < stop_loss * 0.9:
                            logger.info(f"Removing tracking plan {plan.ts_code} (Broken: {current_price} < {stop_loss} * 0.9)")
                            plan.track_status = "STOPPED"
                            plan.review_content = f"[{review_date} AI清洗] 严重破位，自动移除"
                            continue
                except Exception as e:
                    logger.warning(f"Failed to check price for {plan.ts_code}: {e}")
            
            db.commit()
        finally:
            db.close()

    async def _process_tracking_plans(self, account_context):
        """
        [新增] 处理处于监控状态(TRACKING)的计划
        让 AI 基于最新行情和“客观结构优先”风格，决定是否继续跟踪
        """
        db = SessionLocal()
        try:
            # 1. 获取全部监控中的计划（不限日期）
            plans = db.query(TradingPlan).filter(
                TradingPlan.executed == False,
                TradingPlan.track_status == "TRACKING"
            ).order_by(TradingPlan.date.desc()).all()
            
            if not plans:
                return

            logger.info(f"执行监控计划 AI 复核，共 {len(plans)} 个...")

            last_trade_date_str = await data_provider.get_last_trade_date(include_today=True)
            last_trade_date = datetime.strptime(last_trade_date_str, "%Y%m%d").date() if last_trade_date_str else date.today()
            today = date.today()
            batch_size = 30

            for i in range(0, len(plans), batch_size):
                batch = plans[i:i + batch_size]
                ts_codes = [p.ts_code for p in batch]
                quotes = await data_provider.get_realtime_quotes(ts_codes)

                for plan in batch:
                    ts_code = plan.ts_code
                    days_since = (today - plan.date).days
                    if days_since > 5:
                        logger.info(f"Removing tracking plan {ts_code} (Expired: {days_since} days)")
                        await trading_service.cancel_plan(plan.id, f"[{today} AI监控清理] 超期未触发({days_since}天)")
                        continue

                    quote = quotes.get(ts_code)
                    current_price = float(quote.get('price', 0)) if quote else 0.0
                    if current_price <= 0:
                        local_quote = await data_provider.get_local_quote(ts_code)
                        local_time = (local_quote or {}).get("time")
                        local_trade_date = None
                        if isinstance(local_time, str) and len(local_time) >= 10:
                            try:
                                local_trade_date = datetime.strptime(local_time[:10], "%Y-%m-%d").date()
                            except Exception:
                                local_trade_date = None
                        if local_trade_date and (last_trade_date - local_trade_date).days >= 3:
                            logger.info(f"Removing tracking plan {ts_code} (No quote since {local_trade_date})")
                            await trading_service.cancel_plan(plan.id, f"[{today} AI监控清理] 无行情/停牌超过3天")
                        else:
                            now_str = datetime.now().strftime("%H:%M:%S")
                            plan.review_content = f"[{now_str} 监控缺失] 无实时行情，保留观察"
                            db.commit()
                        continue
                
                    # 3. 获取 AI 上下文
                    raw_context = await chat_service.get_ai_trading_context(ts_code, cache_scope="trading")
                    
                    # 4. 调用 AI 决策 (使用 V3 实时接口)
                    # 提示词会自动包含“客观结构优先”的 System Prompt
                    async with self.ai_semaphore:
                        try:
                            decision = await ai_service.analyze_realtime_trade_signal_v3(
                                symbol=ts_code,
                                strategy=plan.strategy_name or "监控复核",
                                current_price=current_price,
                                buy_price=float(plan.buy_price_limit or current_price),
                                raw_trading_context=raw_context,
                                plan_reason=f"监控复核: {plan.reason}",
                                market_status="盘中监控",
                                search_info="",
                                account_info=account_context,
                                preferred_provider="NVIDIA NIM"
                            )
                        
                            action = str(decision.get('action', '')).upper()
                            confidence = float(decision.get('confidence', 0))
                            reason = decision.get('reason', '')
                            reason_text = reason or "AI复核完成"
                            plan.source = "ai_trader"
                            plan.ai_decision = action or plan.ai_decision
                            plan.ai_evaluation = f"{action or 'WAIT'}|{confidence:.0f}"
                            
                            # 5. 根据 AI 决策更新计划状态
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            
                            cancel_keywords = ["移出", "不再跟踪", "不再监控", "取消", "剔除", "清除"]
                            should_cancel = action in ["CANCEL", "ABANDON", "SELL", "REMOVE", "DROP"] or (action == "WAIT" and confidence < 30)
                            if not should_cancel and reason_text:
                                should_cancel = any(k in reason_text for k in cancel_keywords)
                            if should_cancel:
                                logger.info(f"AI 决定放弃跟踪 {ts_code}: {reason_text}")
                                await trading_service.cancel_plan(plan.id, f"[{decision_time} AI交易员取消] {reason_text}")
                                continue
                                
                            elif action == "BUY":
                                # AI 建议买入 -> 保持 TRACKING，更新价格和理由，等待 trade_monitor_job 执行
                                # 注意：这里不直接执行买入，只更新计划，由交易循环去执行
                                logger.info(f"AI 确认买入机会 {ts_code}: {reason_text}")
                                if float(decision.get('price', 0)) > 0:
                                    plan.buy_price_limit = float(decision.get('price', 0))
                                    plan.limit_price = plan.buy_price_limit
                                plan.ai_decision = "BUY"
                                plan.reason = f"{reason_text} (AI复核确认)"
                                plan.score = confidence
                                # 保持 TRACKING 状态，或者设为 None 让 check_and_execute_plans 接管？
                                # 根据 check_and_execute_plans 逻辑，它处理 track_status != TRACKING 的计划
                                # 或者 is_monitor_strategy 且 decision != BUY 时设为 TRACKING
                                # 如果这里确认为 BUY，应该把 track_status 清空，以便被执行
                                plan.track_status = None 
                                plan.review_content = f"[{decision_time} AI交易员激活] 监控转买入: {reason_text}"
                                
                            elif action == "WAIT":
                                # AI 建议继续观察 -> 保持 TRACKING
                                logger.info(f"AI 建议继续观察 {ts_code}: {reason_text}")
                                plan.reason = f"{reason_text} (AI复核等待)"
                                plan.score = confidence
                                plan.review_content = f"[{decision_time} AI交易员监控] {reason_text}"
                                # track_status 保持 "TRACKING"
                                
                            db.commit()
                            
                        except Exception as e:
                            logger.error(f"Error re-evaluating plan {plan.id} ({ts_code}): {e}")

        except Exception as e:
            logger.error(f"Error in _process_tracking_plans: {e}")
        finally:
            db.close()

    async def _run_ai_periodic_monitor(self, account_context, include_positions: bool = True, include_tracking: bool = True):
        try:
            # 1. 执行持仓股 AI 巡检
            positions = account_context.get('positions', [])
            if include_positions and positions:
                logger.info(f"执行持仓股 AI 巡检，共 {len(positions)} 只...")
                for pos in positions:
                    ts_code = pos['ts_code']
                    async with self.ai_semaphore:
                        try:
                            logger.info(f"AI 分析持仓: {ts_code}")
                            plan_res = await self._generate_ai_plan(
                                stock_info={'ts_code': ts_code, 'price': pos['current_price']},
                                strategy="持仓管理",
                                base_reason="15分钟定期持仓巡检",
                                review_date=date.today(),
                                account_info=account_context
                            )
                            if plan_res and plan_res.get('action') == 'SELL':
                                # [Fix] 增加 T+1 可用持仓校验
                                avail_vol = int(pos.get('available_vol', 0))
                                if avail_vol <= 0:
                                    logger.info(f"AI 建议卖出 {ts_code}，但可用持仓为 0 (受 T+1 限制或已冻结)，自动拦截。")
                                    continue

                                logger.info(f"AI 决定卖出持仓 {ts_code}: {plan_res.get('reason')}")
                                await trading_service.create_plan(
                                    ts_code=ts_code,
                                    strategy_name="AI巡检卖出",
                                    buy_price=0,
                                    stop_loss=0,
                                    take_profit=0,
                                    reason=f"AI定期巡检决定卖出: {plan_res.get('reason')}",
                                    plan_date=date.today(),
                                    score=plan_res.get('score', 50),
                                    is_sell=True
                                )
                        except Exception as e:
                            logger.warning(f"持仓巡检失败 {ts_code}: {e}")

            # 2. [新增] 执行监控计划 AI 复核
            if include_tracking:
                await self._process_tracking_plans(account_context)

            try:
                has_active_entrustments = await trading_service.has_active_entrustments()
                if has_active_entrustments:
                    await trading_service.monitor_entrustments()
            except Exception as e:
                logger.warning(f"委托挂单巡检失败: {e}")

        except Exception as e:
            logger.error(f"AI 深度巡检流程异常: {e}")

    async def perform_intraday_scan(self):
        """
        [核心] 盘中高频扫描 (每5-15分钟)
        扫描异动、涨停、炸板等机会，并自动创建交易计划
        """
        start_ts = datetime.now()
        logger.info("执行盘中高频扫描...")
        opps = await stock_selector.scan_noon_opportunities()
        dragons = opps.get('dragons', []) or []
        reversals = opps.get('reversals', []) or []
        pullbacks = opps.get('intraday_pullbacks', []) or []
        candidates = pullbacks + dragons + reversals
        fallback_count = 0
        speed_count = 0
        if not candidates:
            try:
                limit_up_codes = await asyncio.wait_for(
                    data_provider.market_data_service.get_realtime_limit_up_codes(),
                    timeout=20.0
                )
            except Exception as e:
                logger.warning(f"盘中扫描获取涨停榜失败: {e}")
                limit_up_codes = []
            if limit_up_codes:
                quotes = await data_provider.get_realtime_quotes(limit_up_codes[:10], force_tdx=True)
                fallback_candidates = []
                for ts_code in limit_up_codes[:10]:
                    q = quotes.get(ts_code) or {}
                    price = float(q.get("price") or 0)
                    if price > 0:
                        fallback_candidates.append({"ts_code": ts_code, "price": price})
                candidates = fallback_candidates
                fallback_count = len(fallback_candidates)

        speed_candidates: List[Dict[str, Any]] = []
        try:
            speed_items = await asyncio.wait_for(
                data_provider.get_realtime_speed_top(top_n=10),
                timeout=15.0,
            )
        except Exception as e:
            logger.warning(f"盘中扫描获取涨速榜失败: {e}")
            speed_items = []

        if speed_items:
            for item in speed_items:
                ts_code = str(item.get("ts_code") or "")
                name = str(item.get("name") or "")
                if not ts_code or not self._is_valid_stock(ts_code, name):
                    continue
                price = float(item.get("price") or 0)
                if price <= 0:
                    continue
                speed_candidates.append(
                    {
                        "ts_code": ts_code,
                        "name": name,
                        "price": price,
                        "pct_chg": float(item.get("pct_chg") or 0),
                        "speed": float(item.get("speed") or 0),
                        "type": "Speed",
                    }
                )
            speed_count = len(speed_candidates)

        if speed_candidates:
            if not candidates:
                candidates = speed_candidates
            else:
                seen = set()
                merged = []
                for item in candidates + speed_candidates:
                    code = str(item.get("ts_code") or "")
                    if not code or code in seen:
                        continue
                    seen.add(code)
                    merged.append(item)
                candidates = merged

        def get_account_ctx():
            db = SessionLocal()
            try:
                return self._get_account_context(db)
            finally:
                db.close()

        account_context = await asyncio.to_thread(get_account_ctx)
        rising_keywords = ["起涨", "上涨", "启动", "趋势初期", "主升", "突破", "放量", "弱转强", "二波"]
        top_keywords = ["顶部", "高位", "滞涨", "背离", "破位", "上影", "缩量", "放量阴", "钝化", "超买"]
        rising_memories = await learning_service.get_reflection_memories_by_keywords(
            ["盘中异动", "通用"],
            rising_keywords,
            limit=5,
            source_event_type="PATTERN_CASE"
        )
        top_memories = await learning_service.get_reflection_memories_by_keywords(
            ["盘中异动", "通用"],
            top_keywords,
            limit=5,
            source_event_type="PATTERN_CASE"
        )
        memory_injection = ""
        if rising_memories:
            memory_injection += "\n【AI反思-上涨特征】\n" + rising_memories
        if top_memories:
            memory_injection += "\n【AI反思-顶部风险】\n" + top_memories
        await self._run_ai_periodic_monitor(account_context, include_positions=False, include_tracking=True)

        if not candidates:
            logger.info(
                "盘中高频扫描结束: dragons=%d reversals=%d fallback=%d speed=%d ai=%d cost=%.1fs",
                len(dragons),
                len(reversals),
                fallback_count,
                speed_count,
                0,
                (datetime.now() - start_ts).total_seconds(),
            )
            return

        # 4. 对异动股进行 AI 快速决策
        ai_analyzed = 0
        max_ai = 4 if pullbacks else 2
        for stock in candidates[:max_ai]:
            ts_code = stock['ts_code']
            
            # 检查是否已持仓，如果已持仓则跳过买入分析
            is_held = any(p['ts_code'] == ts_code for p in account_context.get('positions', []))
            if is_held:
                continue

            raw_context = await chat_service.get_ai_trading_context(ts_code, cache_scope="review")
            if memory_injection:
                raw_context = f"{raw_context}\n{memory_injection}"
            
            # 使用 V3 实时决策接口
            plan_reason = "盘中高频扫描异动"
            if str(stock.get("type") or "") == "IntradayPullback":
                plan_reason = "盘中放量拉升后缩量回调，分时均价线支撑"

            decision = await ai_service.analyze_realtime_trade_signal_v3(
                symbol=ts_code,
                strategy="盘中异动",
                current_price=stock.get('price', 0),
                buy_price=stock.get('price', 0),
                raw_trading_context=raw_context,
                plan_reason=plan_reason,
                market_status="盘中交易",
                search_info="", # 盘中扫描暂不提供实时搜索，追求速度
                account_info=account_context,
                preferred_provider="NVIDIA NIM"
            )
            ai_analyzed += 1
            logger.info(
                "[IntradayScan] %s decision: action=%s confidence=%s reason=%s",
                ts_code,
                str((decision or {}).get("action") or ""),
                str((decision or {}).get("confidence") or ""),
                str((decision or {}).get("reason") or "")[:120],
            )

            action = str((decision or {}).get("action") or "").upper()
            confidence = float((decision or {}).get("confidence") or 0)
            if decision and action == 'BUY' and confidence > 60:  # [Active Exploration] 降低盘中门槛 (原75)
                logger.info(f"盘中扫描发现高价值机会: {ts_code}, 信心度: {decision.get('confidence')}")
                # 自动创建日内交易计划
                try:
                    accel_ok, accel_reason = await trading_service._is_buy_accel_allowed(ts_code)
                    if not accel_ok:
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        ai_reason = decision.get("reason", "盘中转强")
                        plan_reason = f"扫描原因: 盘中高频扫描异动\nAI确认: {ai_reason}"
                        watch_reason = f"[{decision_time} 风控拦截] 仅允许进入/临近加速段（{accel_reason}）"
                        plan = await trading_service.create_plan(
                            ts_code=ts_code,
                            strategy_name="盘中异动观察",
                            buy_price=stock.get('price', 0),  # [Fix] Set current price as plan price for visibility
                            stop_loss=0,
                            take_profit=0,
                            position_pct=0.1,
                            reason=f"{plan_reason} | {watch_reason}",
                            plan_date=date.today(),
                            score=confidence,
                            source="system",
                            ai_decision="WAIT"
                        )
                        plan_id = plan.id if plan else None
                        if plan_id is not None:
                            await trading_service.update_plan_review(
                                plan_id,
                                f"[{decision_time} AI观望] {ai_reason}",
                                ai_decision="WAIT",
                                decision_price=stock.get('price', 0),
                            )
                        continue
                    ai_reason = decision.get("reason", "盘中转强")
                    await trading_service.create_plan(
                        ts_code=ts_code,
                        strategy_name="盘中异动",
                        buy_price=stock.get('price', 0),
                        stop_loss=stock.get('price', 0) * 0.95,
                        take_profit=stock.get('price', 0) * 1.10,
                        reason=f"扫描原因: {plan_reason}\nAI确认: {ai_reason}",
                        plan_date=date.today(),
                        score=decision.get('confidence', 0)
                    )
                except ValueError as e:
                    logger.warning(f"Skip creating plan for {ts_code}: {e}")
            elif decision and action in ["WAIT", "HOLD"] and confidence >= 50:
                try:
                    ai_reason = decision.get("reason", "AI决定观望")
                    plan_reason = f"扫描原因: {plan_reason}\nAI确认: {ai_reason}"
                    plan = await trading_service.create_plan(
                        ts_code=ts_code,
                        strategy_name="盘中异动观察",
                        buy_price=stock.get('price', 0),  # [Fix] Set current price as plan price for visibility
                        stop_loss=0,
                        take_profit=0,
                        position_pct=0.1,
                        reason=plan_reason,
                        plan_date=date.today(),
                        score=confidence,
                        source="system",
                        ai_decision="WAIT"
                    )
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    if plan:
                        await trading_service.update_plan_review(
                            plan.id,
                            f"[{decision_time} AI观望] {plan_reason}",
                            ai_decision="WAIT",
                            decision_price=stock.get('price', 0),
                        )
                except ValueError as e:
                    logger.warning(f"Skip creating watch plan for {ts_code}: {e}")

        logger.info(
            "盘中高频扫描结束: dragons=%d reversals=%d fallback=%d speed=%d ai=%d cost=%.1fs",
            len(dragons),
            len(reversals),
            fallback_count,
            speed_count,
            ai_analyzed,
            (datetime.now() - start_ts).total_seconds(),
        )

    async def perform_open_confirm_monitor(self):
        start_ts = datetime.now()
        logger.info("开始执行 09:25 开盘确认巡检")
        def get_account_ctx():
            db = SessionLocal()
            try:
                return self._get_account_context(db)
            finally:
                db.close()

        account_context = await asyncio.to_thread(get_account_ctx)

        try:
            positions_analyzed = 0
            plans_analyzed = 0
            sell_suggested = 0
            cancel_suggested = 0
            buy_now_suggested = 0
            positions = account_context.get('positions', [])
            if positions:
                logger.info(f"执行持仓股 AI 开盘确认，共 {len(positions)} 只...")
                for pos in positions:
                    ts_code = pos['ts_code']
                    async with self.ai_semaphore:
                        try:
                            logger.info(f"AI 分析持仓(开盘确认): {ts_code}")
                            plan_res = await self._generate_ai_plan(
                                stock_info={'ts_code': ts_code, 'price': pos['current_price']},
                                strategy="持仓管理",
                                base_reason="09:25 开盘确认持仓",
                                review_date=date.today(),
                                account_info=account_context
                            )
                            positions_analyzed += 1
                            if plan_res and plan_res.get('action') == 'SELL':
                                # [Fix] 增加 T+1 可用持仓校验
                                avail_vol = int(pos.get('available_vol', 0))
                                if avail_vol <= 0:
                                    logger.info(f"AI 建议卖出 {ts_code}，但可用持仓为 0 (受 T+1 限制或已冻结)，自动拦截。")
                                    # 继续执行后续逻辑，但不创建卖出计划
                                else:
                                    sell_suggested += 1
                                    logger.info(f"AI 决定卖出持仓(开盘确认) {ts_code}: {plan_res.get('reason')}")
                                    await trading_service.create_plan(
                                        ts_code=ts_code,
                                        strategy_name="AI开盘确认卖出",
                                        buy_price=0,
                                        stop_loss=0,
                                        take_profit=0,
                                        reason=f"AI开盘确认决定卖出: {plan_res.get('reason')}",
                                        plan_date=date.today(),
                                        score=plan_res.get('score', 50),
                                        is_sell=True
                                    )
                        except Exception as e:
                            logger.warning(f"持仓开盘确认失败 {ts_code}: {e}")

            pending_plans = await trading_service.get_pending_plans(include_monitor_fallback=True)
            if pending_plans:
                logger.info(f"执行计划监控股 AI 开盘确认，共 {len(pending_plans)} 个计划...")
                for plan in pending_plans:
                    ts_code = plan.ts_code
                    async with self.ai_semaphore:
                        try:
                            q = await data_provider.get_realtime_quote(ts_code, cache_scope="review")
                            current_price = q.get('price') if q else 0
                            if not current_price:
                                continue
                            logger.info(f"AI 分析计划监控(开盘确认): {ts_code}")
                            decision = await ai_service.analyze_stock_for_plan(
                                context_str=f"09:25 开盘确认。股票 {ts_code} 当前价格 {current_price}。已有计划: 买入价 {plan.buy_price_limit}。请判断当前是否应该立即执行或取消计划。",
                                is_noon=False,
                                account_info=account_context,
                            )
                            plans_analyzed += 1
                            if decision and decision.get('action') == 'CANCEL':
                                if self._should_keep_tracking(plan):
                                    logger.info(f"AI 开盘确认保留跟踪 {ts_code}: {decision.get('reason')}")
                                    now_str = datetime.now().strftime("%H:%M:%S")
                                    await trading_service.update_plan_review(
                                        plan.id,
                                        f"[{now_str} AI开盘确认保留] 强势回撤视为机会，继续跟踪。{decision.get('reason')}",
                                        ai_decision="WAIT",
                                        decision_price=current_price
                                    )
                                else:
                                    cancel_suggested += 1
                                    logger.info(f"AI 决定取消计划(开盘确认) {ts_code}: {decision.get('reason')}")
                                    await trading_service.cancel_plan(plan.id, f"AI开盘确认决定取消: {decision.get('reason')}")
                            elif decision and decision.get('action') == 'BUY_NOW':
                                buy_now_suggested += 1
                                logger.info(f"AI 决定立即买入(开盘确认) {ts_code}: {decision.get('reason')}")
                                await trading_service.update_plan(
                                    plan.id,
                                    buy_price=current_price,
                                    limit_price=current_price,
                                    ai_decision="BUY",
                                    track_status=None,
                                )
                                now_str = datetime.now().strftime("%H:%M:%S")
                                await trading_service.update_plan_review(
                                    plan.id,
                                    f"[{now_str} AI开盘确认] 允许执行 @ {current_price}. {decision.get('reason')}",
                                    ai_decision="BUY",
                                    decision_price=current_price,
                                )
                        except Exception as e:
                            logger.warning(f"计划开盘确认失败 {ts_code}: {e}")
            logger.info(
                "开盘确认巡检完成: positions=%d, plans=%d, sell=%d, cancel=%d, buy_now=%d, cost=%.1fs",
                positions_analyzed,
                plans_analyzed,
                sell_suggested,
                cancel_suggested,
                buy_now_suggested,
                (datetime.now() - start_ts).total_seconds(),
            )
        except Exception as e:
            logger.error(f"AI 开盘确认巡检流程异常: {e}")

    async def _generate_ai_plan(self, stock_info, strategy, base_reason, review_date, account_info=None, preferred_provider: Optional[str] = "Xiaomi MiMo", api_key: Optional[str] = None):
        """
        统一的交易计划生成器 (注入 30/12/6 原始数据 + 持仓感知)
        """
        ts_code = stock_info['ts_code']
        from app.services.market.market_data_service import market_data_service
        # 1. 获取统一的多周期交易上下文
        try:
            with market_data_service.cache_scope("review"):
                context = await asyncio.wait_for(chat_service.get_ai_trading_context(ts_code, cache_scope="review"), timeout=45.0)
        except Exception as e:
            logger.warning(f"[Review] get_ai_trading_context failed for {ts_code}: {e}")
            context = ""
        
        # 2. 获取实时价格 - 已经异步化，直接 await
        try:
            with market_data_service.cache_scope("review"):
                realtime_quote = await asyncio.wait_for(data_provider.get_realtime_quote(ts_code, cache_scope="review"), timeout=10.0)
        except Exception:
            realtime_quote = None
        current_price = realtime_quote.get('price', 0) if (realtime_quote and realtime_quote.get('price')) else stock_info.get('price', 0)
        
        # 3. 构造账户上下文 (如果外部未传入)
        if not account_info:
            def get_ctx():
                db = SessionLocal()
                try:
                    return self._get_account_context(db)
                finally:
                    db.close()
            account_info = await asyncio.to_thread(get_ctx)

        # 3. 构造 AI 决策
        # 如果是持仓管理，策略提示词需要微调
        if strategy in ["持仓管理", "午间持仓管理"]:
            try:
                decision = await asyncio.wait_for(
                    ai_service.analyze_realtime_trade_signal_v3(
                        symbol=ts_code,
                        strategy=strategy,
                        current_price=current_price,
                        buy_price=stock_info.get('price', 0),
                        raw_trading_context=context,
                        plan_reason=f"当前持仓状态: {base_reason}",
                        market_status="复盘管理",
                        search_info="",
                        account_info=account_info,
                        preferred_provider=preferred_provider,
                        api_key=api_key,
                    ),
                    timeout=45.0,
                )
            except Exception as e:
                logger.warning(f"[Review] AI decision failed for {ts_code}: {e}")
                decision = {"action": "WAIT", "confidence": 50, "reason": "AI 暂不可用，默认继续持有"}
            # 这里的决策可能返回 WAIT (继续持有) 或 CANCEL (卖出)
            # 我们将其转换为统一格式
            return {
                "ts_code": ts_code,
                "strategy": strategy, # 统一使用 strategy 字段
                "target_price": 0, # 持仓股不设置买入价
                "reason": f"扫描原因: {base_reason}\nAI确认: {decision.get('reason', '继续持有')}",
                "action": "SELL" if decision.get('action') == 'CANCEL' else "HOLD",
                "score": decision.get('confidence', 50),
                "position_pct": 0.1 # 默认仓位
            }

        # 正常买入计划逻辑
        try:
            decision = await asyncio.wait_for(
                ai_service.analyze_realtime_trade_signal_v3(
                    symbol=ts_code,
                    strategy=strategy,
                    current_price=current_price,
                    buy_price=current_price,
                    raw_trading_context=context,
                    plan_reason=base_reason,
                    market_status="复盘分析",
                    search_info="",
                    account_info=account_info,
                    preferred_provider=preferred_provider,
                    api_key=api_key,
                ),
                timeout=45.0,
            )
        except Exception as e:
            logger.warning(f"[Review] AI decision failed for {ts_code}: {e}")
            decision = None
        
        act = str((decision or {}).get('action') or 'WAIT').upper()
        conf = int((decision or {}).get('confidence') or 0)
        logger.info(f"[Review] AI Decision for {ts_code}: action={act}, confidence={conf}, reason={str((decision or {}).get('reason') or '')[:50]}...")
        reference_price = float(current_price or 0.0)
        plan_price = float((decision or {}).get("plan_price") or (decision or {}).get("price") or current_price or 0.0)
        if plan_price <= 0 and reference_price > 0:
            plan_price = reference_price

        if decision and act == 'BUY':
            return {
                "ts_code": ts_code,
                "strategy": strategy,
                "target_price": plan_price,
                "reference_price": reference_price,
                "plan_price": plan_price,
                "reason": f"扫描原因: {base_reason}\nAI确认: {decision.get('reason', base_reason)}\n参考价: {reference_price:.2f} 计划价: {plan_price:.2f}",
                "score": decision.get('confidence', 60),
                "position_pct": 0.1,
                "action": "BUY",
            }

        if decision and act in ["WAIT", "HOLD"]:
            return {
                "ts_code": ts_code,
                "strategy": strategy,
                "target_price": 0,
                "reference_price": reference_price,
                "plan_price": 0,
                "reason": f"扫描原因: {base_reason}\nAI确认: {decision.get('reason', base_reason)}\n参考价: {reference_price:.2f}",
                "score": decision.get('confidence', 0),
                "position_pct": 0.1,
                "action": "WAIT",
                "decision_price": current_price,
            }

        return None

    def _build_market_summary_fallback(self, review_date, stats, ladder, temperature, selector_tracking: str = "") -> Dict[str, str]:
        up = int(stats.get("up") or 0)
        down = int(stats.get("down") or 0)
        limit_up = int(stats.get("limit_up") or 0)
        limit_down = int(stats.get("limit_down") or 0)
        total_volume = float(stats.get("total_volume") or 0.0)
        highest = int((ladder or {}).get("highest") or 0)
        tiers = (ladder or {}).get("tiers") or {}
        tier_desc = []
        try:
            for k in sorted(tiers.keys(), key=lambda x: int(x), reverse=True):
                cnt = tiers.get(k)
                if cnt:
                    tier_desc.append(f"{k}板{int(cnt)}只")
        except Exception:
            tier_desc = []
        temp_desc = "震荡"
        if temperature >= 70:
            temp_desc = "偏热"
        elif temperature < 30:
            temp_desc = "偏冷"
        tier_str = "，".join(tier_desc[:5]) if tier_desc else "无明显梯队"
        summary = (
            f"上涨{up}家，下跌{down}家，涨停{limit_up}家，跌停{limit_down}家，成交额{total_volume:.1f}亿。"
            f"连板高度{highest}板，{tier_str}。市场温度{temperature}，情绪{temp_desc}。"
        )
        if selector_tracking:
            summary = f"{summary} 选股池跟踪：{selector_tracking[:120]}..."
        main_theme = f"连板高度{highest}板"
        return {"main_theme": main_theme, "summary": summary}

    async def _generate_ai_market_summary(self, review_date, stats, ladder, temperature, news, selector_tracking: str = "", preferred_provider: Optional[str] = "Xiaomi MiMo", api_key: Optional[str] = None):
        """
        AI 生成市场复盘总结
        """
        selector_block = ""
        if selector_tracking:
            selector_block = f"\n\n近7日选股池跟踪：\n{selector_tracking}\n"
        memory_context = await learning_service.get_reflection_memories("通用", temperature)
        if not memory_context:
            memory_context = "【策略反思与长期记忆 (基于历史成败提炼)】\n- 暂无匹配记忆"
        prompt = f"""
        你是一个资深的 A 股游资策略分析师，擅长“龙头战法”和“情绪周期”分析。请根据以下市场数据，生成今日深度市场复盘总结。
        
        日期: {review_date}
        市场快照: 上涨{stats.get('up')}家, 下跌{stats.get('down')}家, 涨停{stats.get('limit_up')}家, 成交额{stats.get('total_volume')}亿元
        市场温度: {temperature} (注: 0-30 冰点, 30-70 震荡, 70-100 火热)
        连板天梯: {ladder}
        市场资讯: {news[:1500]}
        {selector_block}
        {memory_context}
        
        任务:
        1. **核心主线 (main_theme)**: 用一句话概括当前最强板块或题材（如“低空经济爆发，高标龙头引领”）。
        2. **情绪博弈 (summary)**: 
           - **宏观与消息面**: 简要点评国际国内大事、政策变动对市场的影响（如有）。
           - **情绪与预期**: 分析今日情绪状态（分歧/一致/冰点/过热）及明日预期（修复/退潮/加速）。
           - **龙头点评**: 指出当前的连板最高标及其带动力。
           - **选股池结论**: 重点总结选股池（{selector_block[:50]}...）中核心标的的表现（谁在领涨、谁在回撤、谁具备弱转强潜力）。
        
        注意：必须严格以 JSON 格式输出，不要包含任何额外文字，格式如下：
        {{"main_theme": "...", "summary": "..."}}
        """
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    ai_core_client.call_ai_best_effort,
                    prompt,
                    preferred_provider=preferred_provider,
                    api_key=api_key
                ),
                timeout=60.0
            )
            
            if not response:
                return self._build_market_summary_fallback(review_date, stats, ladder, temperature, selector_tracking)
            
            logger.info(f"AI Market Summary Response: {response}")
            
            # 改进的 JSON 提取逻辑
            json_content = response
            if "```json" in response:
                json_content = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                json_content = response.split("```")[1].split("```")[0].strip()
            else:
                match = re.search(r'\{.*\}', response, re.DOTALL)
                if match:
                    json_content = match.group(0)

            try:
                data = json.loads(json_content, strict=False)
                return {
                    "main_theme": data.get("main_theme", "未知主线"),
                    "summary": data.get("summary", response)
                }
            except json.JSONDecodeError:
                # 兜底：如果 JSON 解析失败，尝试从文本中提取
                logger.warning(f"AI JSON decode failed, using raw response: {response}")
                return {"main_theme": "解析失败", "summary": response}
                
        except asyncio.TimeoutError:
            logger.warning("AI market summary timed out, using fallback summary")
            return self._build_market_summary_fallback(review_date, stats, ladder, temperature, selector_tracking)
        except Exception as e:
            logger.error(f"Error in _generate_ai_market_summary: {e}", exc_info=True)
            return self._build_market_summary_fallback(review_date, stats, ladder, temperature, selector_tracking)

    async def get_review_result(self, target_date: date):
        """
        从数据库获取复盘结果 (供 API 调用)
        """
        db = SessionLocal()
        try:
            sentiment = (
                db.query(MarketSentiment)
                .filter(MarketSentiment.date == target_date)
                .order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc())
                .first()
            )
            if not sentiment:
                return None

            is_generating = (sentiment.main_theme == "生成中") or ("后台生成中" in str(sentiment.summary or ""))

            updated_at = sentiment.updated_at or datetime.now()
            sentiment_date = sentiment.date or target_date
            is_evening_review = (updated_at.hour < 11 or updated_at.hour >= 15) or (updated_at.date() > sentiment_date)
            plan_dates: List[date] = []
            if is_evening_review:
                plan_dates = [target_date + timedelta(days=i) for i in range(1, 6)]
            else:
                plan_dates = [target_date]

            plans = db.query(TradingPlan).filter(TradingPlan.date.in_(plan_dates)).all()

            target_plan = None
            target_plans: List[Dict[str, Any]] = []
            holding_plans: List[Dict[str, Any]] = []

            for p in plans:
                action = "HOLD"
                if "卖出" in (p.strategy_name or ""):
                    action = "SELL"
                elif "买入" in (p.strategy_name or "") or p.strategy_name in ["午间强势", "首板挖掘", "低吸反包", "收盘精选", "尾盘突击", "梯队联动", "成交额筛选", "盘中异动"]:
                    action = "BUY"

                plan_dict = {
                    "ts_code": p.ts_code,
                    "strategy": p.strategy_name,
                    "reason": p.reason or "无决策",
                    "position_pct": p.position_pct or 0.1,
                    "score": p.score,
                    "target_price": p.buy_price_limit,
                    "action": action,
                }

                if p.strategy_name in ["午间强势", "首板挖掘", "低吸反包", "收盘精选", "尾盘突击", "梯队联动", "成交额筛选", "盘中异动"]:
                    if not self._is_valid_stock(str(p.ts_code)):
                        continue
                    target_plans.append(plan_dict)
                    if not target_plan:
                        target_plan = plan_dict
                    elif float(p.score or 0) > _to_float(target_plan.get("score")):
                        target_plan = plan_dict
                elif p.strategy_name in ["持仓管理", "持仓卖出", "持仓减仓", "持仓做T", "持仓持有"]:
                    holding_plans.append(plan_dict)

            # 1. 尝试从数据库读取已有的分析结果 (JSON 字段)
            import json
            ladder_info = None
            turnover_top = None
            ladder_opps = None
            realtime_stats = None
            
            # [核心] 如果是今天且在交易时间内，尝试获取实时统计数据以确保页面显示正确
            if target_date == date.today() and data_provider.is_trading_time():
                try:
                    realtime_stats = await data_provider.get_market_snapshot()
                except Exception as e:
                    logger.warning(f"Failed to fetch realtime stats for get_review_result: {e}")

            try:
                if sentiment.ladder_json:
                    ladder_info = json.loads(str(sentiment.ladder_json))
                if sentiment.turnover_top_json:
                    turnover_top = json.loads(str(sentiment.turnover_top_json))
                if sentiment.ladder_opportunities_json:
                    ladder_opps = json.loads(str(sentiment.ladder_opportunities_json))
            except Exception as e:
                logger.warning(f"Failed to parse saved JSON results for {target_date}: {e}")

            # 2. 如果数据库没有存储，则进行动态计算 (向后兼容)
            if not is_generating:
                if not ladder_info:
                    limit_up_codes: List[str] = []
                    if target_date == date.today():
                        if not data_provider.is_trading_time():
                            ladder_info = self._analyze_limit_ladder(db, target_date)
                            turnover_top = await self._build_turnover_top(db, top_n=80, trade_date=target_date)
                            turnover_top = [
                                x
                                for x in (turnover_top or [])
                                if isinstance(x, dict)
                                and x.get("ts_code")
                                and self._is_valid_stock(str(x.get("ts_code")), str(x.get("name", "")))
                                and self._passes_trend_filter(db, str(x.get("ts_code")), target_date)
                            ]
                            ladder_opps = await self._build_ladder_opportunities(db, ladder_info or {}, turnover_top)
                        else:
                            now_ts = datetime.now().timestamp()
                            cache = self._intraday_snapshot_cache or {}
                            if cache.get("date") == target_date and (now_ts - float(cache.get("ts") or 0.0)) < 20.0:
                                ladder_info = cache.get("ladder")
                                turnover_top = cache.get("turnover_top") or []
                                ladder_opps = cache.get("ladder_opps") or []
                            else:
                                try:
                                    logger.info("Fetching realtime limit-up codes for ladder analysis...")
                                    limit_up_codes = await asyncio.wait_for(
                                        data_provider.market_data_service.get_realtime_limit_up_codes(),
                                        timeout=30.0,
                                    )
                                    logger.info(f"Fetched {len(limit_up_codes)} codes for ladder")
                                except asyncio.TimeoutError:
                                    logger.warning("Timeout fetching limit-up codes for ladder analysis (30s)")
                                    limit_up_codes = []
                                except Exception as e:
                                    logger.error(f"Error fetching limit-up codes for ladder: {e}")
                                    limit_up_codes = []

                                end_date = date.today()
                                try:
                                    last_trade_date_str = await data_provider.market_data_service.get_last_trade_date(include_today=False)
                                    if last_trade_date_str:
                                        end_date = datetime.strptime(str(last_trade_date_str), "%Y%m%d").date()
                                except Exception:
                                    end_date = date.today()

                                ladder_info = self._analyze_limit_ladder_intraday(db, end_date, limit_up_codes)
                                turnover_top = await self._build_turnover_top(db, top_n=80, trade_date=target_date)
                                turnover_top = [
                                    x
                                    for x in (turnover_top or [])
                                    if isinstance(x, dict)
                                    and x.get("ts_code")
                                    and self._is_valid_stock(str(x.get("ts_code")), str(x.get("name", "")))
                                    and self._passes_trend_filter(db, str(x.get("ts_code")), end_date)
                                ]
                                ladder_opps = await self._build_ladder_opportunities(db, ladder_info or {}, turnover_top)
                                self._intraday_snapshot_cache = {
                                    "date": target_date,
                                    "ts": now_ts,
                                    "ladder": ladder_info,
                                    "turnover_top": turnover_top,
                                    "ladder_opps": ladder_opps,
                                }
                    else:
                        ladder_info = self._analyze_limit_ladder(db, target_date)
                        turnover_top = await self._build_turnover_top(db, top_n=80, trade_date=target_date)
                        turnover_top = [
                            x
                            for x in (turnover_top or [])
                            if isinstance(x, dict)
                            and x.get("ts_code")
                            and self._is_valid_stock(str(x.get("ts_code")), str(x.get("name", "")))
                            and self._passes_trend_filter(db, str(x.get("ts_code")), target_date)
                        ]
                        ladder_opps = await self._build_ladder_opportunities(db, ladder_info or {}, turnover_top)

                if turnover_top is None:
                    turnover_top = await self._build_turnover_top(db, top_n=80, trade_date=target_date)
                    turnover_top = [
                        x
                        for x in (turnover_top or [])
                        if isinstance(x, dict)
                        and x.get("ts_code")
                        and self._is_valid_stock(str(x.get("ts_code")), str(x.get("name", "")))
                        and self._passes_trend_filter(db, str(x.get("ts_code")), target_date)
                    ]
                
                if ladder_opps is None:
                    ladder_opps = await self._build_ladder_opportunities(db, ladder_info or {}, turnover_top)

            highest_plate = int(sentiment.highest_plate or 0)
            if ladder_info and int((ladder_info or {}).get("highest") or 0) > 0:
                highest_plate = int((ladder_info or {}).get("highest") or 0)

            # 优先使用实时统计数据，如果没有则使用数据库存储的数据
            res_up = int(realtime_stats.get('up', 0)) if realtime_stats else sentiment.up_count
            res_down = int(realtime_stats.get('down', 0)) if realtime_stats else sentiment.down_count
            res_limit_up = int(realtime_stats.get('limit_up', 0)) if realtime_stats else sentiment.limit_up_count
            res_limit_down = int(realtime_stats.get('limit_down', 0)) if realtime_stats else (sentiment.limit_down_count or 0)
            res_volume = float(realtime_stats.get('total_volume', 0.0)) if realtime_stats else (sentiment.total_volume or 0.0)
            res_temp = sentiment.market_temperature
            if realtime_stats:
                # 重新计算温度以匹配实时数据
                res_temp = self._calculate_market_temperature(realtime_stats, ladder_info or {"highest": highest_plate})

            return {
                "date": sentiment_date.strftime("%Y-%m-%d"),
                "up": res_up,
                "down": res_down,
                "limit_up": res_limit_up,
                "limit_down": res_limit_down,
                "total_volume": res_volume,
                "temp": res_temp,
                "highest_plate": highest_plate,
                "ladder": ladder_info,
                "turnover_top": turnover_top,
                "ladder_opportunities": ladder_opps,
                "summary": sentiment.summary,
                "main_theme": sentiment.main_theme,
                "created_at": sentiment.updated_at,
                "target_plan": target_plan,
                "target_plans": target_plans,
                "holding_plans": holding_plans,
            }
        finally:
            db.close()

    def _passes_trend_filter(self, db_unused, ts_code: str, asof_date: date) -> bool:
        ts_code = str(ts_code or "")
        if not ts_code:
            return False
        
        # 调试日志
        logger.debug(f"[Filter] Querying {ts_code} for date <= {asof_date}")
        
        from app.db.session import SessionLocal
        local_db = SessionLocal()
        try:
            rows = (
                local_db.query(DailyBar.trade_date, DailyBar.close)
                .filter(DailyBar.ts_code == ts_code)
                .filter(DailyBar.trade_date <= asof_date)
                .order_by(DailyBar.trade_date.desc())
                .limit(60)
                .all()
            )
            
            # 如果 rows 为空，进一步检查
            if not rows or len(rows) == 0:
                 total_count = local_db.query(DailyBar).filter(DailyBar.ts_code == ts_code).count()
                 if total_count > 0:
                     logger.info(f"[Filter] {ts_code} rejected: data points 0 < 15. Total records in DB: {total_count}")
                 return False

            closes_desc = [float(r[1] or 0.0) for r in (rows or []) if r and float(r[1] or 0.0) > 0]
            
            # 放宽要求：从 20 天降到 15 天
            if len(closes_desc) < 15:
                logger.info(f"[Filter] {ts_code} rejected: data points {len(closes_desc)} < 15")
                return False
                
            closes = list(reversed(closes_desc))
            last = float(closes[-1] or 0.0)
            if last <= 0:
                return False
            
            actual_ma_days = min(len(closes), 20)
            ma_val = sum(closes[-actual_ma_days:]) / float(actual_ma_days)
            
            if ma_val <= 0:
                return False
            
            if last < ma_val:
                logger.info(f"[Filter] {ts_code} rejected: price {last} < MA{actual_ma_days} {ma_val:.2f}")
                return False
                
            if last / ma_val > 1.35: 
                logger.info(f"[Filter] {ts_code} rejected: bias {last/ma_val:.2f} > 1.35")
                return False

            ind = (
                local_db.query(StockIndicator)
                .filter(StockIndicator.ts_code == ts_code)
                .filter(StockIndicator.trade_date <= asof_date)
                .order_by(StockIndicator.trade_date.desc())
                .first()
            )
            bias5_threshold, bias10_threshold = trading_service._get_dynamic_bias_threshold(local_db)
            bias_checked = False
            if ind and ind.bias5 is not None:
                if float(ind.bias5) > bias5_threshold:
                    logger.info(f"[Filter] {ts_code} rejected: bias5 {float(ind.bias5):.2f} > {bias5_threshold}")
                    return False
                bias_checked = True
            if not bias_checked and ind and ind.bias10 is not None:
                if float(ind.bias10) > bias10_threshold:
                    logger.info(f"[Filter] {ts_code} rejected: bias10 {float(ind.bias10):.2f} > {bias10_threshold}")
                    return False
                bias_checked = True
            if not bias_checked:
                if len(closes) >= 5:
                    ma5 = sum(closes[-5:]) / 5.0
                    if ma5 > 0:
                        bias5 = (last - ma5) / ma5 * 100.0
                        if bias5 > bias5_threshold:
                            logger.info(f"[Filter] {ts_code} rejected: bias5 {bias5:.2f} > {bias5_threshold}")
                            return False

            monthly_rows = (
                local_db.query(MonthlyBar.trade_date, MonthlyBar.close)
                .filter(MonthlyBar.ts_code == ts_code)
                .filter(MonthlyBar.trade_date <= asof_date)
                .order_by(MonthlyBar.trade_date.asc())
                .limit(36)
                .all()
            )
            monthly_closes = [float(r[1] or 0.0) for r in (monthly_rows or []) if r and float(r[1] or 0.0) > 0]
            if len(monthly_closes) < 18:
                return True
            monthly_ma5 = []
            for i in range(len(monthly_closes)):
                if i >= 4:
                    window = monthly_closes[i - 4 : i + 1]
                    monthly_ma5.append(sum(window) / 5.0 if window else 0.0)
                else:
                    monthly_ma5.append(0.0)
            valid_ma5 = [v for v in monthly_ma5 if v > 0]
            if len(valid_ma5) < 18:
                return True

            def _angle_from_ma5(ma_list: List[float], start_idx: int, end_idx: int) -> float:
                if end_idx <= start_idx:
                    return 0.0
                sv = ma_list[start_idx]
                ev = ma_list[end_idx]
                if sv <= 0 or ev <= 0:
                    return 0.0
                months = end_idx - start_idx
                rate = (ev / sv - 1.0) / months
                return math.degrees(math.atan(rate))

            def _rising_count(ma_list: List[float], start_idx: int, end_idx: int) -> int:
                cnt = 0
                for i in range(start_idx + 1, end_idx + 1):
                    if ma_list[i] > 0 and ma_list[i - 1] > 0 and ma_list[i] >= ma_list[i - 1]:
                        cnt += 1
                return cnt

            end_idx = len(monthly_ma5) - 1
            recent_start = max(4, end_idx - 8)
            angle_m = _angle_from_ma5(monthly_ma5, recent_start, end_idx)
            rising_m = _rising_count(monthly_ma5, recent_start, end_idx)
            if angle_m >= 18.0 and rising_m >= 9:
                logger.info(f"[Filter] {ts_code} rejected: monthly overheated angle {angle_m:.1f}, rising {rising_m}")
                return False

            return True
        finally:
            local_db.close()

    async def _build_turnover_top(self, db, top_n: int = 80, trade_date: Optional[date] = None) -> List[Dict[str, Any]]:
        top_n = max(1, min(int(top_n or 80), 300))
        if trade_date and (trade_date != date.today() or not data_provider.is_trading_time()):
            def _query_history():
                from app.db.session import SessionLocal
                local_db = SessionLocal()
                try:
                    return (
                        local_db.query(DailyBar.ts_code, DailyBar.amount, Stock.name, Stock.industry)
                        .join(Stock, Stock.ts_code == DailyBar.ts_code, isouter=True)
                        .filter(DailyBar.trade_date == trade_date)
                        .order_by(DailyBar.amount.desc())
                        .limit(top_n)
                        .all()
                    )
                finally:
                    local_db.close()
            rows = await asyncio.to_thread(_query_history)
            out: List[Dict[str, Any]] = []
            for ts_code, amt, name, industry in rows:
                if not ts_code:
                    continue
                # DB 中的 amount 通常是千元，转换为亿元
                amt_yi = round(float(amt or 0.0) / 100000.0, 2)
                ts_str = str(ts_code)
                name_str = str(name or "")
                if not self._is_valid_stock(ts_str, name_str):
                    continue
                out.append(
                    {
                        "ts_code": ts_str,
                        "name": name_str,
                        "industry": str(industry or ""),
                        "turnover_amount": amt_yi,
                    }
                )
            return out

        try:
            # 实时接口通常已经返回亿元或万元，需统一
            items = await data_provider.market_data_service.get_market_turnover_top(top_n=200)
        except Exception:
            items = []
        codes = [i.get("ts_code") for i in (items or []) if isinstance(i, dict) and i.get("ts_code")]
        if not codes:
            return []

        def _query_meta():
            from app.db.session import SessionLocal
            local_db = SessionLocal()
            try:
                return local_db.query(Stock.ts_code, Stock.name, Stock.industry).filter(Stock.ts_code.in_(codes)).all()
            finally:
                local_db.close()
        
        rows = await asyncio.to_thread(_query_meta)
        meta = {str(ts): {"name": str(n or ""), "industry": str(ind or "")} for ts, n, ind in (rows or [])}

        out_items: List[Dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            ts_code = it.get("ts_code")
            if not ts_code:
                continue
            m = meta.get(str(ts_code), {})
            
            # 统一为亿元
            raw_amt = float(it.get("turnover_amount") or 0.0)
            if raw_amt > 100000: # 可能是千元
                amt_yi = round(raw_amt / 100000.0, 2)
            elif raw_amt > 10000: # 可能是万元
                amt_yi = round(raw_amt / 10000.0, 2)
            else:
                amt_yi = round(raw_amt, 2)

            name_str = m.get("name", "") or it.get("name", "")
            if not self._is_valid_stock(str(ts_code), str(name_str or "")):
                continue
            out_items.append(
                {
                    "ts_code": ts_code,
                    "name": name_str,
                    "industry": m.get("industry", ""),
                    "turnover_amount": amt_yi,
                }
            )
        out_items.sort(key=lambda x: float(x.get("turnover_amount") or 0.0), reverse=True)
        return out_items[:top_n]

    async def _build_ladder_opportunities(self, db, ladder_info: Dict[str, Any], turnover_top: List[Dict[str, Any]], max_total: int = 8, per_industry: int = 3, per_concept: int = 2) -> List[Dict[str, Any]]:
        max_total = max(1, min(int(max_total or 8), 8))
        per_industry = max(1, min(int(per_industry or 3), 10))
        per_concept = max(1, min(int(per_concept or 2), 10))

        ladder_stocks = (ladder_info or {}).get("stocks") or []
        ladder_codes = [s.get("ts_code") for s in (ladder_stocks or []) if isinstance(s, dict) and s.get("ts_code")]
        ladder_set = set(ladder_codes)
        if not ladder_codes or not turnover_top:
            return []

        def _query_ladder_meta():
            from app.db.session import SessionLocal
            local_db = SessionLocal()
            try:
                return local_db.query(Stock.ts_code, Stock.name, Stock.industry).filter(Stock.ts_code.in_(ladder_codes)).all()
            finally:
                local_db.close()

        rows = await asyncio.to_thread(_query_ladder_meta)
        ladder_meta = {str(ts): {"name": str(n or ""), "industry": str(ind or "")} for ts, n, ind in (rows or [])}
        ladder_codes_unique = [str(c) for c in ladder_codes if c]
        turnover_codes = [str(it.get("ts_code")) for it in (turnover_top or []) if isinstance(it, dict) and it.get("ts_code")]

        ladder_concepts_map: Dict[str, List[str]] = {}
        turnover_concepts_map: Dict[str, List[str]] = {}
        if ladder_codes_unique:
            ladder_results = await asyncio.gather(*[data_provider.get_stock_concepts(c) for c in ladder_codes_unique], return_exceptions=True)
            for code, res in zip(ladder_codes_unique, ladder_results):
                if isinstance(res, BaseException):
                    continue
                concepts_raw = res if isinstance(res, list) else []
                concepts = [x for x in concepts_raw if isinstance(x, str) and x.strip()]
                if concepts:
                    ladder_concepts_map[code] = list(dict.fromkeys(concepts))

        if turnover_codes:
            turnover_results = await asyncio.gather(*[data_provider.get_stock_concepts(c) for c in turnover_codes], return_exceptions=True)
            for code, res in zip(turnover_codes, turnover_results):
                if isinstance(res, BaseException):
                    continue
                concepts_raw = res if isinstance(res, list) else []
                concepts = [x for x in concepts_raw if isinstance(x, str) and x.strip()]
                if concepts:
                    turnover_concepts_map[code] = list(dict.fromkeys(concepts))

        concept_to_turnover: Dict[str, List[str]] = {}
        for c, concepts in turnover_concepts_map.items():
            for concept in concepts:
                concept_to_turnover.setdefault(concept, []).append(c)

        out: List[Dict[str, Any]] = []
        used = set()
        industry_count: Dict[str, int] = {}
        concept_count: Dict[str, int] = {}

        ladder_sorted = sorted(
            [s for s in ladder_stocks if isinstance(s, dict)],
            key=lambda x: int(x.get("height") or 0),
            reverse=True,
        )

        for s in ladder_sorted:
            src_code = s.get("ts_code")
            if not src_code:
                continue
            src_name = s.get("name") or ladder_meta.get(str(src_code), {}).get("name") or ""
            src_height = int(s.get("height") or 0)
            industry = ladder_meta.get(str(src_code), {}).get("industry") or ""
            src_concepts = ladder_concepts_map.get(str(src_code), [])

            if src_concepts:
                for concept in src_concepts:
                    cnt = concept_count.get(concept, 0)
                    if cnt >= per_concept:
                        continue
                    for c in concept_to_turnover.get(concept, []):
                        if not c or c in ladder_set or c in used:
                            continue
                        it = next((x for x in turnover_top if isinstance(x, dict) and str(x.get("ts_code")) == str(c)), None)
                        if not it:
                            continue
                        used.add(c)
                        concept_count[concept] = cnt + 1
                        out.append(
                            {
                                "ts_code": c,
                                "name": it.get("name", ""),
                                "industry": it.get("industry", ""),
                                "turnover_amount": float(it.get("turnover_amount") or 0.0),
                                "source_ladder_stock": src_code,
                                "source_ladder_name": src_name,
                                "source_ladder_height": src_height,
                                "reason": f"同题材(概念:{concept})联动，来自{src_code}{f'({src_name})' if src_name else ''}{src_height}板梯队",
                            }
                        )
                        if len(out) >= max_total:
                            return out

            if industry:
                for it in turnover_top:
                    if not isinstance(it, dict):
                        continue
                    candidate_code = str(it.get("ts_code") or "")
                    if not candidate_code or candidate_code in ladder_set or candidate_code in used:
                        continue
                    if str(it.get("industry") or "") != industry:
                        continue

                    cnt = industry_count.get(industry, 0)
                    if cnt >= per_industry:
                        break

                    used.add(candidate_code)
                    industry_count[industry] = cnt + 1
                    out.append(
                        {
                            "ts_code": candidate_code,
                            "name": it.get("name", ""),
                            "industry": industry,
                            "turnover_amount": float(it.get("turnover_amount") or 0.0),
                            "source_ladder_stock": src_code,
                            "source_ladder_name": src_name,
                            "source_ladder_height": src_height,
                            "reason": f"同板块(行业:{industry})联动，来自{src_code}{f'({src_name})' if src_name else ''}{src_height}板梯队",
                        }
                    )
                    if len(out) >= max_total:
                        return out

        return out

    def _analyze_limit_ladder(self, db_unused, review_date):
        from app.db.session import SessionLocal
        local_db = SessionLocal()
        try:
            return self._analyze_limit_ladder_daily(local_db, review_date)
        finally:
            local_db.close()

    def _is_limit_up_day(self, ts_code: str, name: str, close: float, pre_close: float, pct_chg: float) -> bool:
        try:
            if pre_close and pre_close > 0 and close and close > 0:
                lu, _ = get_limit_prices(ts_code, pre_close, name=name)
                if lu > 0 and round(float(close), 2) == round(float(lu), 2):
                    return True
        except Exception:
            pass

        ratio = 0.1
        if name and "ST" in name:
            ratio = 0.05
        elif ts_code.startswith("688") or ts_code.startswith("30"):
            ratio = 0.2
        elif ts_code.startswith("8") or ts_code.startswith("4") or ts_code.startswith("43") or ts_code.startswith("83") or ts_code.startswith("87") or ts_code.startswith("92"):
            ratio = 0.3
        return float(pct_chg or 0) >= (ratio * 100 - 0.2)

    def _analyze_limit_ladder_by_codes(self, db, end_date: date, codes: List[str], add_one_for_today: bool) -> Dict[str, Any]:
        end_date = end_date if isinstance(end_date, date) else datetime.now().date()
        codes = [str(c) for c in (codes or []) if c]
        if not codes:
            return {"highest": 0, "tiers": {}, "stocks": []}

        lookback_days = 60
        start_date = end_date - timedelta(days=lookback_days)

        bars = (
            db.query(DailyBar.ts_code, DailyBar.trade_date, DailyBar.close, DailyBar.pre_close, DailyBar.pct_chg)
            .filter(DailyBar.ts_code.in_(codes))
            .filter(DailyBar.trade_date >= start_date, DailyBar.trade_date <= end_date)
            .all()
        )

        name_map: Dict[str, str] = {}
        stocks = db.query(Stock.ts_code, Stock.name).filter(Stock.ts_code.in_(codes)).all()
        for ts_code, name in stocks:
            name_map[str(ts_code)] = str(name or "")

        by_code: Dict[str, list] = {c: [] for c in codes}
        actual_end_date = None
        for ts_code, trade_date, close, pre_close, pct_chg in bars:
            if not ts_code or not trade_date:
                continue
            dt = trade_date
            if actual_end_date is None or dt > actual_end_date:
                actual_end_date = dt
            by_code.setdefault(str(ts_code), []).append(
                (dt, float(close or 0), float(pre_close or 0), float(pct_chg or 0))
            )

        # 如果 bars 为空，尝试使用传入的 end_date
        if actual_end_date is None:
            actual_end_date = end_date

        logger.info(f"Analyzing ladder for {len(codes)} codes, actual_end_date in DB: {actual_end_date}")

        streaks: Dict[str, int] = {}
        tiers: Dict[int, int] = {}
        stocks_out: list[dict] = []

        for ts_code in codes:
            rows = by_code.get(ts_code) or []
            if rows:
                rows.sort(key=lambda x: x[0], reverse=True)

            name = name_map.get(ts_code, "")
            streak = 0
            # 如果数据库中最新的日期就是实际截止日期，则从数据库开始计算连板
            if rows and rows[0][0] == actual_end_date:
                for d, close, pre_close, pct_chg in rows:
                    if self._is_limit_up_day(ts_code, name, close, pre_close, pct_chg):
                        streak += 1
                    else:
                        break
            
            # 如果是盘中分析 (add_one_for_today=True)，则在数据库连板基础上 +1
            # 注意：如果该股今天才涨停(首板)，且数据库里最后一天没涨停，streak 也会从 0 变成 1
            if add_one_for_today:
                streak += 1

            if streak <= 0:
                continue

            streaks[ts_code] = streak
            tiers[streak] = tiers.get(streak, 0) + 1
            stocks_out.append({"ts_code": ts_code, "name": name, "height": streak})

        highest = max(streaks.values()) if streaks else 0
        stocks_out.sort(key=lambda x: (int(x.get("height") or 0), x.get("ts_code") or ""), reverse=True)
        tiers_out = {str(k): int(v) for k, v in sorted(tiers.items(), key=lambda kv: kv[0], reverse=True)}
        return {"highest": int(highest), "tiers": tiers_out, "stocks": stocks_out[:50]}

    def _analyze_limit_ladder_intraday(self, db_unused, end_date: date, limit_up_codes: List[str]) -> Dict[str, Any]:
        from app.db.session import SessionLocal
        local_db = SessionLocal()
        try:
            return self._analyze_limit_ladder_by_codes(local_db, end_date=end_date, codes=limit_up_codes, add_one_for_today=True)
        finally:
            local_db.close()

    def _analyze_limit_ladder_daily(self, db, review_date: date) -> Dict[str, Any]:
        review_date = review_date if isinstance(review_date, date) else datetime.now().date()

        stock_filter = or_(
            DailyBar.ts_code.like("60%.SH"),
            DailyBar.ts_code.like("688%.SH"),
            DailyBar.ts_code.like("00%.SZ"),
            DailyBar.ts_code.like("30%.SZ"),
            DailyBar.ts_code.like("4%.BJ"),
            DailyBar.ts_code.like("8%.BJ"),
            DailyBar.ts_code.like("43%.BJ"),
            DailyBar.ts_code.like("83%.BJ"),
            DailyBar.ts_code.like("87%.BJ"),
            DailyBar.ts_code.like("92%.BJ"),
        )

        rows = (
            db.query(DailyBar.ts_code, DailyBar.close, DailyBar.pre_close, DailyBar.pct_chg)
            .filter(DailyBar.trade_date == review_date)
            .filter(stock_filter)
            .all()
        )
        if not rows and review_date == date.today():
            latest_date = db.query(func.max(DailyBar.trade_date)).scalar()
            if latest_date and latest_date != review_date:
                review_date = latest_date
                rows = (
                    db.query(DailyBar.ts_code, DailyBar.close, DailyBar.pre_close, DailyBar.pct_chg)
                    .filter(DailyBar.trade_date == review_date)
                    .filter(stock_filter)
                    .all()
                )
        if not rows:
            return {"highest": 0, "tiers": {}, "stocks": []}

        codes = [r[0] for r in rows if r and r[0]]
        name_map: Dict[str, str] = {}
        stocks = db.query(Stock.ts_code, Stock.name).filter(Stock.ts_code.in_(codes)).all()
        for ts_code, name in stocks:
            name_map[str(ts_code)] = str(name or "")

        limit_up_codes: List[str] = []
        for ts_code, close, pre_close, pct_chg in rows:
            if not ts_code:
                continue
            name = name_map.get(str(ts_code), "")
            if self._is_limit_up_day(str(ts_code), name, float(close or 0), float(pre_close or 0), float(pct_chg or 0)):
                limit_up_codes.append(str(ts_code))

        return self._analyze_limit_ladder_by_codes(db, end_date=review_date, codes=limit_up_codes, add_one_for_today=False)

    def _calculate_market_temperature(self, stats, ladder):
        """计算市场温度 (0-100)"""
        up = int(stats.get('up', 0) or stats.get('up_count', 0))
        down = int(stats.get('down', 0) or stats.get('down_count', 0))
        limit_up = int(stats.get('limit_up', 0) or stats.get('limit_up_count', 0))
        limit_down = int(stats.get('limit_down', 0) or stats.get('limit_down_count', 0))
        
        total = up + down
        if total == 0: return 50
        
        # 1. 基础分：上涨家数占比 (权重 40%)
        up_ratio = up / total
        base_score = up_ratio * 40
        
        # 2. 情绪分：涨跌停对比 (权重 40%)
        limit_total = limit_up + limit_down
        if limit_total > 0:
            limit_score = (limit_up / limit_total) * 40
        else:
            limit_score = 20 # 中性
            
        # 3. 赚钱效应：连板高度 (权重 20%)
        highest = ladder.get('highest', 1)
        # 连板高度 7+ 为极强，1-2 为弱
        ladder_score = min(20, highest * 2.5) 
        
        temp = base_score + limit_score + ladder_score
        
        # 极端行情修正：如果跌停家数 > 涨停家数，温度上限压制在 50 以下
        if limit_down > limit_up and limit_down > 5:
            temp = min(49, temp)
            
        return round(min(100, max(0, temp)), 1)

review_service = MarketReviewService()
