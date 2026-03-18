from __future__ import annotations
import asyncio
from sqlalchemy.orm import Session
from datetime import datetime, date, timedelta, time
from typing import Any, Dict, List, Optional, Tuple, Set, TypedDict
import math
import json
from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, MarketSentiment, Account, Position, TradeRecord, Stock, PatternCase, StockIndicator
from app.services.logger import logger
from app.core.config import settings
from app.core.redis import redis_client

from app.services.data_provider import data_provider
from app.services.ai_service import ai_service
from app.services.indicators.technical_indicators import technical_indicators
from app.services.market.market_utils import get_limit_prices
from app.utils.concurrency import trading_lock_manager
from app.services import entrustment_signal

ANALYSIS_HISTORY_KEY_PREFIX = "ai_trader:analysis_history:"

class TradingService:
    """
    交易执行服务
    核心准则：AI 最高决策权 (Chief Trading Officer)
    - AI 决策优先级高于用户手动计划，支持自动覆盖与撤单。
    - 盈利保护为第一优先级，严禁因形态约束错过卖点。
    """
    # 类变量：存储股票分析历史，防止短时间内重复分析
    # 结构: {ts_code: {"last_time": datetime, "last_price": float}}
    class _AnalysisHistoryItem(TypedDict):
        last_time: datetime
        last_price: float

    _analysis_history: Dict[str, _AnalysisHistoryItem] = {}
    
    # 类变量：存储正在处理中的计划 ID，防止并发执行
    _processing_plan_ids: Set[int] = set()

    # 风控配置常量
    MAX_SINGLE_STOCK_POSITION_RATIO = 0.5  # 单只股票仓位上限50%
    MAX_POSITION_COUNT = 10  # 持仓数量上限10只
    MAX_TOTAL_POSITION_RATIO = 0.9  # 总仓位上限90%

    def __init__(self):
        self._monitor_plan_concurrency = 3
        self._monitor_position_concurrency = 3
        self._monitor_context_concurrency = 5
        self._intraday_search_cache = {}  # {ts_code: (timestamp, content)}

    def _get_min_buy_volume(self, ts_code: str) -> int:
        if not ts_code:
            return 100
        if ts_code.startswith("688"):
            return 200
        return 100

    def _get_buy_volume_step(self, ts_code: str) -> int:
        return 100

    def _normalize_buy_volume(self, ts_code: str, volume: int) -> int:
        if volume is None:
            return 0
        v = int(volume)
        if v <= 0:
            return 0
        step = self._get_buy_volume_step(ts_code)
        if step <= 1:
            return v
        return (v // step) * step

    def _normalize_sell_volume(self, ts_code: str, volume: int) -> int:
        if volume is None:
            return 0
        v = int(volume)
        if v <= 0:
            return 0
        return v

    def _ceil_to_hand(self, volume: int) -> int:
        v = int(volume or 0)
        if v <= 0:
            return 0
        step = 100
        return ((v + step - 1) // step) * step

    def _is_closing_auction_time(self, now_time: time) -> bool:
        return time(14, 57) <= now_time < time(15, 0)

    def _is_opening_auction_time(self, now_time: time) -> bool:
        return time(9, 15) <= now_time < time(9, 30)

    def _calc_reduce_volume(self, available_vol: int) -> int:
        v = int(available_vol or 0)
        if v <= 0:
            return 0
        half = (v + 1) // 2
        reduced = self._ceil_to_hand(half)
        return min(v, reduced)

    def _shares_to_hands(self, shares: int) -> float:
        v = float(shares or 0)
        if v <= 0:
            return 0.0
        return float(v / 100.0)

    def _is_suspended_quote(self, quote: Dict) -> bool:
        if not quote:
            return True
        vol = float(quote.get("vol", 0) or 0)
        if vol > 0:
            return False
        open_p = float(quote.get("open", 0) or 0)
        high_p = float(quote.get("high", 0) or 0)
        low_p = float(quote.get("low", 0) or 0)
        bid_ask = quote.get("bid_ask") or {}
        b1_p = float(bid_ask.get("b1_p", 0) or 0)
        s1_p = float(bid_ask.get("s1_p", 0) or 0)
        if open_p == 0 and high_p == 0 and low_p == 0 and b1_p == 0 and s1_p == 0:
            return True
        return False

    def _calc_buy_fee(self, amount: float) -> float:
        amt = float(amount or 0.0)
        if amt <= 0:
            return 0.0
        return float(max(5.0, amt * 0.00025))

    def _calc_transfer_fee(self, ts_code: str, amount: float) -> float:
        amt = float(amount or 0.0)
        if amt <= 0:
            return 0.0
        if ts_code and ts_code.endswith(".SH"):
            return float(amt * 0.00001)
        return 0.0

    def _calc_sell_fees(self, ts_code: str, amount: float) -> float:
        amt = float(amount or 0.0)
        if amt <= 0:
            return 0.0
        commission = float(max(5.0, amt * 0.00025))
        stamp_tax = float(amt * 0.0005)
        transfer_fee = self._calc_transfer_fee(ts_code, amt)
        return commission + stamp_tax + transfer_fee

    def _should_trigger_deep_analysis(self, ts_code: str, current_price: float, cooling_minutes: int = 10, price_delta_pct: float = 0.5) -> bool:
        """
        判定是否应该触发深度分析 (冷却时间 + 价格波动双重校验)
        使用 Redis 持久化存储，确保服务重启后状态不丢失
        """
        now = datetime.now()
        
        # 优先从 Redis 获取
        redis_key = f"{ANALYSIS_HISTORY_KEY_PREFIX}{ts_code}"
        history_data = None
        
        if redis_client:
            try:
                history_data = redis_client.hgetall(redis_key)
            except Exception as e:
                logger.warning(f"Failed to read analysis history from Redis for {ts_code}: {e}")
        
        if history_data and "last_time" in history_data and "last_price" in history_data:
            try:
                last_time = datetime.fromisoformat(history_data["last_time"])
                last_price = float(history_data["last_price"])
                
                # 1. 检查冷却时间
                if now - last_time < timedelta(minutes=cooling_minutes):
                    # 2. 即使在冷却时间内，如果价格波动剧烈 (超过 0.5%)，也允许重新分析
                    if last_price > 0:
                        price_change = abs(current_price - last_price) / last_price * 100
                        if price_change < price_delta_pct:
                            return False
                return True
            except Exception as e:
                logger.warning(f"Failed to parse analysis history for {ts_code}: {e}")
        
        # 回退到内存缓存（仅当 Redis 不可用时）
        history = self._analysis_history.get(ts_code)
        
        if not history:
            return True
            
        last_time = history["last_time"]
        last_price = history["last_price"]
        
        # 1. 检查冷却时间
        if now - last_time < timedelta(minutes=cooling_minutes):
            # 2. 即使在冷却时间内，如果价格波动剧烈 (超过 0.5%)，也允许重新分析
            price_change = abs(current_price - last_price) / last_price * 100
            if price_change < price_delta_pct:
                return False
        
        return True

    def _get_dynamic_bias_threshold(self, db: Session) -> Tuple[float, float]:
        """
        根据市场情绪动态调整乖离率阈值
        :return: (bias5_threshold, bias10_threshold)
        """
        try:
            # 获取最新的市场情绪数据
            latest_sentiment = db.query(MarketSentiment).order_by(MarketSentiment.date.desc()).first()
            
            if latest_sentiment and latest_sentiment.market_temperature is not None:
                temperature = float(latest_sentiment.market_temperature)
                
                # 根据市场温度动态调整阈值
                # 温度越高（市场越强势），阈值越宽松
                # 温度越低（市场越弱势），阈值越严格
                if temperature >= 70:
                    # 强势市场：放宽阈值
                    bias5_threshold = 8.0
                    bias10_threshold = 12.0
                    logger.info(f"Strong market (temp: {temperature:.1f}), bias thresholds relaxed to BIAS5={bias5_threshold}%, BIAS10={bias10_threshold}%")
                elif temperature >= 50:
                    # 中性市场：标准阈值
                    bias5_threshold = 6.0
                    bias10_threshold = 10.0
                    logger.info(f"Neutral market (temp: {temperature:.1f}), bias thresholds standard at BIAS5={bias5_threshold}%, BIAS10={bias10_threshold}%")
                else:
                    # 弱势市场：收紧阈值
                    bias5_threshold = 4.0
                    bias10_threshold = 8.0
                    logger.info(f"Weak market (temp: {temperature:.1f}), bias thresholds tightened to BIAS5={bias5_threshold}%, BIAS10={bias10_threshold}%")
                
                return bias5_threshold, bias10_threshold
        except Exception as e:
            logger.error(f"Error getting dynamic bias threshold: {e}")
        
        # 默认阈值
        return 6.0, 10.0

    async def _get_monthly_ma5_metrics(self, ts_code: str) -> tuple[float, int]:
        try:
            from app.services.market.market_data_service import market_data_service
            monthly_k = await market_data_service.get_kline(
                ts_code,
                freq="M",
                limit=36,
                local_only=True,
                include_indicators=False,
                adj="qfq",
                is_ui_request=False,
                cache_scope="validation",
            )
            if not monthly_k or len(monthly_k) < 18:
                return 0.0, 0

            def _bar_pct(bar: dict) -> float:
                try:
                    v = bar.get("pct_chg")
                    if v is not None:
                        return float(v)
                except Exception:
                    pass
                try:
                    o = float(bar.get("open") or 0.0)
                    c = float(bar.get("close") or 0.0)
                    if o > 0:
                        return (c / o - 1.0) * 100.0
                except Exception:
                    pass
                return 0.0

            closes = []
            for bar in monthly_k:
                try:
                    closes.append(float(bar.get("close") or 0.0))
                except Exception:
                    closes.append(0.0)

            ma20_list: list[float] = []
            for i in range(len(closes)):
                if i >= 19:
                    window = closes[i - 19 : i + 1]
                    v = sum(window) / 20.0 if window else 0.0
                    ma20_list.append(v)
                else:
                    ma20_list.append(0.0)

            ma5_list: list[float] = []
            for i in range(len(closes)):
                if i >= 4:
                    window = closes[i - 4 : i + 1]
                    v = sum(window) / 5.0 if window else 0.0
                    ma5_list.append(v)
                else:
                    ma5_list.append(0.0)

            valid_ma5 = [v for v in ma5_list if v > 0]
            if len(valid_ma5) < 18:
                return 0.0, 0

            def _angle(start_idx: int, end_idx: int) -> float:
                if end_idx <= start_idx:
                    return 0.0
                start_v = ma5_list[start_idx]
                end_v = ma5_list[end_idx]
                if start_v <= 0 or end_v <= 0:
                    return 0.0
                months = end_idx - start_idx
                monthly_rate = (end_v / start_v - 1.0) / months
                return math.degrees(math.atan(monthly_rate))

            def _rising_count(start_idx: int, end_idx: int) -> int:
                cnt = 0
                for i in range(start_idx + 1, end_idx + 1):
                    if ma5_list[i] > 0 and ma5_list[i - 1] > 0 and ma5_list[i] >= ma5_list[i - 1]:
                        cnt += 1
                return cnt

            end = len(ma5_list) - 1
            rising_start = max(4, end - 8)
            angle_start = max(4, end - 1)

            angle_recent = _angle(angle_start, end)
            rising_recent = _rising_count(rising_start, end)

            return angle_recent, rising_recent
        except Exception:
            return 0.0, 0

    async def _is_monthly_overheated(self, ts_code: str, current_price: float) -> tuple[bool, str]:
        angle_recent, rising_recent = await self._get_monthly_ma5_metrics(ts_code)
        if angle_recent >= 18.0 and rising_recent >= 9:
            return True, f"MA5角度{angle_recent:.1f}°, 连续上行{rising_recent}月"
        return False, ""

    async def _is_monthly_accel_start(self, ts_code: str) -> tuple[bool, str]:
        angle_recent, rising_recent = await self._get_monthly_ma5_metrics(ts_code)
        if angle_recent >= 18.0 and 7 <= rising_recent < 9:
            return True, f"MA5角度{angle_recent:.1f}°, 连续上行{rising_recent}月"
        return False, ""

    async def _calc_ma5_angle(self, ts_code: str, freq: str, length: int) -> float:
        try:
            from app.services.market.market_data_service import market_data_service
            k = await market_data_service.get_kline(
                ts_code,
                freq=freq,
                limit=max(30, length + 10),
                local_only=True,
                include_indicators=False,
                adj="qfq",
                is_ui_request=False,
                cache_scope="validation",
            )
            if not k or len(k) < length + 5:
                return 0.0
            closes: list[float] = []
            for bar in k:
                try:
                    closes.append(float(bar.get("close") or 0.0))
                except Exception:
                    closes.append(0.0)
            ma5: list[float] = []
            for i in range(len(closes)):
                if i >= 4:
                    w = closes[i - 4 : i + 1]
                    ma5.append(sum(w) / 5.0 if w else 0.0)
                else:
                    ma5.append(0.0)
            end = len(ma5) - 1
            start = max(4, end - (length - 1))
            sv = ma5[start]
            ev = ma5[end]
            if sv <= 0 or ev <= 0 or end <= start:
                return 0.0
            periods = end - start
            rate = (ev / sv - 1.0) / periods
            return math.degrees(math.atan(rate))
        except Exception:
            return 0.0

    async def _is_buy_accel_allowed(self, ts_code: str) -> tuple[bool, str]:
        angle_m, rising_m = await self._get_monthly_ma5_metrics(ts_code)
        entering = angle_m >= 18.0 and 7 <= rising_m
        pre = (angle_m >= 18.0 and rising_m >= 5)
        angle_w = await self._calc_ma5_angle(ts_code, "W", 9)
        angle_d = await self._calc_ma5_angle(ts_code, "D", 20)
        mtf_ok = (angle_w >= 5.0 and angle_d >= 3.0)
        if entering and mtf_ok:
            return True, f"进入加速段(月角{angle_m:.1f}°,周角{angle_w:.1f}°,日角{angle_d:.1f}°)"
        if pre and mtf_ok:
            return True, f"即将进入加速(月角{angle_m:.1f}°,周角{angle_w:.1f}°,日角{angle_d:.1f}°)"
        return False, f"月角{angle_m:.1f}°,周角{angle_w:.1f}°,日角{angle_d:.1f}°"

    async def _validate_buy_decision(self, db: Session, plan: TradingPlan, decision: Dict, current_price: float) -> tuple[bool, str]:
        """
        买入决策二次验证（双模认证）
        验证AI决策是否符合风控规则
        :return: True表示通过验证，False表示拒绝
        """
        try:
            action = decision.get('action', '').upper()
            if action != 'BUY':
                return True, ""
            
            # 1. 检查单只股票仓位限制
            account = await self._get_or_create_account(db)
            position = db.query(Position).filter(Position.ts_code == plan.ts_code).first()
            
            # 计算当前持仓市值
            current_mv = position.market_value if position else 0.0
            
            # 计算本次买入后的预估市值
            buy_volume = plan.frozen_vol or 0
            if buy_volume == 0:
                ref_price = plan.buy_price_limit or plan.limit_price or current_price
                position_pct = float(plan.position_pct or 0.0)
                target_amount = account.total_assets * position_pct
                step = self._get_buy_volume_step(plan.ts_code)
                if step <= 1:
                    buy_volume = int(target_amount / ref_price)
                else:
                    buy_volume = int(target_amount / ref_price / step) * step
            
            estimated_mv = current_mv + buy_volume * current_price
            single_stock_ratio = estimated_mv / account.total_assets if account.total_assets > 0 else 0
            
            if single_stock_ratio > self.MAX_SINGLE_STOCK_POSITION_RATIO:
                logger.warning(f"Buy decision validation FAILED: Single stock position {single_stock_ratio:.2%} > {self.MAX_SINGLE_STOCK_POSITION_RATIO:.0%} for {plan.ts_code}")
                return False, "单票仓位超限"
            
            # 2. 检查持仓数量限制
            position_count = db.query(Position).filter(Position.vol > 0).count()
            if position_count >= self.MAX_POSITION_COUNT:
                logger.warning(f"Buy decision validation FAILED: Too many positions {position_count} >= {self.MAX_POSITION_COUNT} for {plan.ts_code}")
                return False, "持仓数量超限"
            
            # 3. 检查价格合理性
            ai_price = float(decision.get('price', 0))
            plan_limit = float(plan.buy_price_limit or 0)
            
            if plan_limit > 0 and ai_price > plan_limit * 1.05:
                logger.warning(f"Buy decision validation FAILED: AI price {ai_price:.2f} exceeds plan limit {plan_limit:.2f} by >5% for {plan.ts_code}")
                return False, "价格超限"
            
            # 4. 检查决策置信度
            confidence = decision.get('confidence', 0)
            if confidence < 50:
                logger.warning(f"Buy decision validation FAILED: Low confidence {confidence} for {plan.ts_code}")
                return False, "置信度不足"
            
            overheat, reason = await self._is_monthly_overheated(plan.ts_code, current_price)
            if overheat:
                logger.warning(f"Buy decision validation FAILED: Monthly MA5 acceleration {reason} for {plan.ts_code}")
                return False, f"月线加速过热（{reason}）"

            accel_ok, accel_reason = await self._is_buy_accel_allowed(plan.ts_code)
            if not accel_ok:
                logger.warning(f"Buy decision validation FAILED: Not in/near acceleration {accel_reason} for {plan.ts_code}")
                return False, f"未进入/临近加速段（{accel_reason}）"
            
            logger.info(f"Buy decision validation PASSED for {plan.ts_code}: action={action}, confidence={confidence}, single_stock_ratio={single_stock_ratio:.2%}")
            return True, ""
            
        except Exception as e:
            logger.error(f"Error validating buy decision: {e}")
            return False, "校验异常"

    def _update_analysis_history(self, ts_code: str, current_price: float):
        """记录分析历史，同时持久化到 Redis 和内存"""
        now = datetime.now()
        
        # 1. 更新 Redis（持久化）
        redis_key = f"{ANALYSIS_HISTORY_KEY_PREFIX}{ts_code}"
        if redis_client:
            try:
                redis_client.hset(redis_key, mapping={
                    "last_time": now.isoformat(),
                    "last_price": str(current_price)
                })
                # 设置过期时间 24 小时，避免 Redis 无限膨胀
                redis_client.expire(redis_key, 86400)
            except Exception as e:
                logger.warning(f"Failed to update analysis history in Redis for {ts_code}: {e}")
        
        # 2. 更新内存缓存（作为回退）
        self._analysis_history[ts_code] = {
            "last_time": now,
            "last_price": current_price
        }

    async def _get_or_create_account(self, db: Session) -> Account:
        """获取或创建默认交易账户"""
        account = db.query(Account).first()
        if not account:
            account = Account(
                total_assets=settings.INITIAL_CAPITAL,
                available_cash=settings.INITIAL_CAPITAL,
                frozen_cash=0.0,
                market_value=0.0,
                total_pnl=0.0
            )
            db.add(account)
            db.commit()
            db.refresh(account)
            logger.info(f"Initialized new trading account with {settings.INITIAL_CAPITAL:,.0f} capital")
        return account

    async def _calc_expected_total_cash(self, db: Session) -> float:
        from sqlalchemy import func

        buy_cost = await asyncio.to_thread(
            lambda: float(
                db.query(
                    func.coalesce(
                        func.sum(
                            func.coalesce(TradeRecord.amount, 0.0)
                            + func.coalesce(TradeRecord.fee, 0.0)
                        ),
                        0.0,
                    )
                )
                .filter(TradeRecord.trade_type == "BUY")
                .scalar()
                or 0.0
            )
        )
        sell_income = await asyncio.to_thread(
            lambda: float(
                db.query(func.coalesce(func.sum(func.coalesce(TradeRecord.amount, 0.0)), 0.0))
                .filter(TradeRecord.trade_type == "SELL")
                .scalar()
                or 0.0
            )
        )
        initial_capital = settings.INITIAL_CAPITAL
        return float(initial_capital - buy_cost + sell_income)

    async def _calc_expected_frozen_cash(self, db: Session) -> float:
        from sqlalchemy import func

        frozen_sum = await asyncio.to_thread(
            lambda: float(
                db.query(func.coalesce(func.sum(func.coalesce(TradingPlan.frozen_amount, 0.0)), 0.0))
                .filter(TradingPlan.frozen_amount > 0)
                .scalar()
                or 0.0
            )
        )
        return float(max(0.0, frozen_sum))

    async def reconcile_account_cash(self) -> Dict[str, float]:
        db = SessionLocal()
        try:
            async with trading_lock_manager.lock("trade:account"):
                account = await self._get_or_create_account(db)
                expected_total_cash = await self._calc_expected_total_cash(db)
                expected_frozen = await self._calc_expected_frozen_cash(db)
                expected_available = float(expected_total_cash - expected_frozen)

                before_available = float(account.available_cash or 0.0)
                before_frozen = float(account.frozen_cash or 0.0)
                before_total_assets = float(account.total_assets or 0.0)

                account.available_cash = expected_available
                account.frozen_cash = expected_frozen
                account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
                self._recalc_account_pnl(account)

                await asyncio.to_thread(db.commit)
                await asyncio.to_thread(db.refresh, account)

                return {
                    "before_available_cash": before_available,
                    "after_available_cash": float(account.available_cash or 0.0),
                    "before_frozen_cash": before_frozen,
                    "after_frozen_cash": float(account.frozen_cash or 0.0),
                    "before_total_assets": before_total_assets,
                    "after_total_assets": float(account.total_assets or 0.0),
                }
        finally:
            db.close()

    def _validate_trade_time(self, ts_code: str, record_skip_func, strict: bool = True) -> bool:
        """
        [New] 统一交易时间检查逻辑 (DRY)
        :param ts_code: 股票代码
        :param record_skip_func: 记录跳过原因的回调函数
        :param strict: 严格模式，如果为 True 则检查完整的交易时段（包括集合竞价拦截）；如果为 False 则仅检查是否在交易日内
        """
        # 1. 基础交易日检查
        if not data_provider.is_trading_time():
            logger.warning(f"Skip execution for {ts_code}: Not trading time.")
            if record_skip_func: record_skip_func("非交易时间")
            return False

        if not strict:
            return True

        now_time = datetime.now().time()

        # 2. 开盘集合竞价 (9:15-9:25) - 严禁挂单
        if time(9, 15) <= now_time < time(9, 25):
            logger.info(f"Skip execution for {ts_code}: 9:15-9:25 strictly forbidden.")
            if record_skip_func: record_skip_func("集合竞价期间不可成交")
            return False

        # 3. 午休期间 (11:30-13:00)
        if time(11, 30) <= now_time < time(13, 0):
            logger.info(f"Skip execution for {ts_code}: Noon break (wait for 13:00).")
            if record_skip_func: record_skip_func("午休不可成交")
            return False

        # 4. 收盘集合竞价阶段 (14:57-15:00) - 真实交易在 15:00 一次性撮合
        if self._is_closing_auction_time(now_time):
            logger.info(f"Skip execution for {ts_code}: Closing call auction (wait for 15:00 match).")
            if record_skip_func: record_skip_func("收盘集合竞价期间不可成交")
            return False

        # 5. 收盘后不允许成交
        if now_time >= time(15, 0):
            logger.info(f"Skip execution for {ts_code}: After market close.")
            if record_skip_func: record_skip_func("收盘后不可成交")
            return False

        return True

    async def execute_buy(self, db: Session, plan: TradingPlan, suggested_price: float, volume: int = None) -> bool:
        """
        执行买入操作
        :param suggested_price: AI 建议的买入价
        :param volume: 指定买入数量，如果为None则根据 plan.position_pct 自动计算
        """
        from app.services.reward_punish_service import reward_punish_service
        if reward_punish_service.is_trading_paused():
            now_str = datetime.now().strftime("%H:%M:%S")
            plan.review_content = f"[{now_str}] 风控暂停交易：奖惩系统处于暂停状态"
            try:
                db.commit()
            except Exception:
                db.rollback()
            return False
        def _record_skip(reason: str):
            try:
                now_str = datetime.now().strftime("%H:%M:%S")
                plan.review_content = f"[{now_str} 风控拦截] {reason}"
                db.commit()
            except Exception:
                pass

        # [新增] 统一交易时间检查 (DRY优化)
        if not self._validate_trade_time(plan.ts_code, _record_skip):
            return False

        try:
            current_price = float(suggested_price or 0.0)
            if current_price <= 0:
                q = await data_provider.get_realtime_quote(plan.ts_code)
                current_price = float((q or {}).get("price") or 0.0)
                if current_price <= 0:
                    logger.warning(f"Failed to get valid price for {plan.ts_code} in execute_buy pre-check.")
                    return False
        except Exception as e:
            logger.error(f"Error getting price for {plan.ts_code}: {e}")
            return False

        async with trading_lock_manager.lock("trade:account"):
            try:
                db.refresh(plan)
                if plan.executed:
                    logger.warning(f"Plan {plan.id} for {plan.ts_code} already executed, skipping duplicate execution.")
                    return False

                account = await self._get_or_create_account(db)
                stock_info = db.query(Stock).filter(Stock.ts_code == plan.ts_code).first()
                stock_name = str((stock_info.name if stock_info else plan.ts_code) or plan.ts_code or "")

                q = await data_provider.get_realtime_quote(plan.ts_code, cache_scope="trading")
                if self._is_suspended_quote(q):
                    logger.info(f"Skip execution for {plan.ts_code}: Suspended quote.")
                    return False
                current_price = float(q['price']) if q else suggested_price
                if current_price <= 0:
                    logger.error(f"Cannot get valid price for {plan.ts_code}")
                    return False

                ask_price = float((q.get("bid_ask") or {}).get("s1_p", 0) or 0) if q else 0.0
                trigger_price = current_price if current_price > 0 else ask_price

                order_type = plan.order_type or "MARKET"
                plan_buy_limit = float(plan.buy_price_limit or 0.0)
                limit_p = float(plan.limit_price or plan_buy_limit or suggested_price or 0.0)
                pre_close = float((q or {}).get("pre_close", 0) or 0.0)
                if pre_close > 0:
                    limit_up, _ = get_limit_prices(str(plan.ts_code), pre_close)
                    if limit_up and float(limit_up) > 0:
                        limit_up = float(limit_up)
                        if plan_buy_limit > 0 and plan_buy_limit > limit_up:
                            plan_buy_limit = limit_up
                            plan.buy_price_limit = limit_up
                        if limit_p > 0 and limit_p > limit_up:
                            limit_p = limit_up
                            plan.limit_price = limit_up

                if limit_p > 0:
                    if trigger_price > limit_p:
                        logger.info(
                            f"Limit price violation: {plan.ts_code} trigger {trigger_price} > limit {limit_p}, waiting..."
                        )
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        plan.review_content = f"[{decision_time} 风控拦截] 触发价{trigger_price:.2f}高于限价{limit_p:.2f}"
                        db.commit()
                        return False

                exec_price = trigger_price
                if pre_close > 0:
                    limit_up, _ = get_limit_prices(str(plan.ts_code), pre_close)
                    if limit_up and float(limit_up) > 0 and exec_price > float(limit_up):
                        exec_price = float(limit_up)
                if order_type == "LIMIT" and plan_buy_limit > 0 and exec_price > plan_buy_limit:
                    logger.info(f"Capping execution price {exec_price} to plan limit {plan_buy_limit} for {plan.ts_code}")
                    exec_price = plan_buy_limit

                if volume is None:
                    if plan.frozen_vol and plan.frozen_vol > 0:
                        volume = plan.frozen_vol
                        logger.info(f"Using frozen volume {volume} for {plan.ts_code}")
                    else:
                        ref_price = plan_buy_limit or exec_price
                        if ref_price <= 0:
                            ref_price = exec_price

                        position_pct = float(plan.position_pct or 0.0)
                        target_amount = account.total_assets * position_pct
                        step = self._get_buy_volume_step(plan.ts_code)
                        if step <= 1:
                            volume = int(target_amount / ref_price)
                        else:
                            volume = int(target_amount / ref_price / step) * step
                        logger.info(f"Calculated volume {volume} based on position {plan.position_pct} and ref_price {ref_price} for {plan.ts_code}")
                        plan.frozen_vol = volume
                else:
                    logger.info(f"Using provided volume {volume} for {plan.ts_code}")

                volume = self._normalize_buy_volume(plan.ts_code, volume)
                min_buy_vol = self._get_min_buy_volume(plan.ts_code)
                if volume < min_buy_vol:
                    logger.warning(f"Calculated volume {volume} < {min_buy_vol} for {plan.ts_code}, skipping buy.")
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{decision_time} 风控拦截] 买入数量不足，需≥{min_buy_vol}股"
                    db.commit()
                    return False

                need_cash = volume * exec_price
                fee = self._calc_buy_fee(need_cash) + self._calc_transfer_fee(plan.ts_code, need_cash)
                total_cost = need_cash + fee

                can_trade = await self._check_limit_order_queue(db, plan, q, side="BUY", order_vol=volume)
                if not can_trade:
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{decision_time} 风控拦截] 涨停排队或盘口无法成交"
                    db.commit()
                    return False

                # [新增] 单只股票仓位限制检查
                position = db.query(Position).filter(Position.ts_code == plan.ts_code).first()
                current_mv = position.market_value if position else 0.0
                estimated_mv = current_mv + volume * exec_price
                single_stock_ratio = estimated_mv / account.total_assets if account.total_assets > 0 else 0
                
                if single_stock_ratio > self.MAX_SINGLE_STOCK_POSITION_RATIO:
                    logger.warning(f"Single stock position limit: {plan.ts_code} estimated ratio {single_stock_ratio:.2%} > {self.MAX_SINGLE_STOCK_POSITION_RATIO:.0%}, skipping buy.")
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{decision_time} 风控拦截] 单票仓位超限"
                    db.commit()
                    return False

                # [新增] 持仓数量限制检查
                position_count = db.query(Position).filter(Position.vol > 0).count()
                if position_count >= self.MAX_POSITION_COUNT:
                    logger.warning(f"Position count limit: Too many positions {position_count} >= {self.MAX_POSITION_COUNT}, skipping buy for {plan.ts_code}.")
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{decision_time} 风控拦截] 持仓数量超限"
                    db.commit()
                    return False

                # [原有] 总仓位限制检查
                position_ratio = account.market_value / account.total_assets if account.total_assets > 0 else 0
                if position_ratio > self.MAX_TOTAL_POSITION_RATIO:
                    logger.warning(f"Total position limit: Current position ratio {position_ratio:.2%} > {self.MAX_TOTAL_POSITION_RATIO:.0%}, skipping buy for {plan.ts_code}.")
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{decision_time} 风控拦截] 总仓位超限"
                    db.commit()
                    return False

                if plan.frozen_amount and plan.frozen_amount > 0:
                    diff = total_cost - plan.frozen_amount
                    if account.available_cash < diff:
                        logger.warning(f"Insufficient available cash for extra cost {diff} (Total: {total_cost}, Frozen: {plan.frozen_amount})")
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        plan.review_content = f"[{decision_time} 风控拦截] 资金不足，需{total_cost:.2f}，可用{account.available_cash:.2f}"
                        db.commit()
                        return False

                    account.frozen_cash = max(0, account.frozen_cash - plan.frozen_amount)
                    account.available_cash -= diff
                    plan.frozen_amount = 0.0
                    plan.frozen_vol = 0
                else:
                    if account.available_cash < total_cost:
                        logger.warning(f"Insufficient funds for {plan.ts_code}: Need {total_cost}, Have {account.available_cash}")
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        plan.review_content = f"[{decision_time} 风控拦截] 资金不足，需{total_cost:.2f}，可用{account.available_cash:.2f}"
                        db.commit()
                        return False
                    account.available_cash -= total_cost

                position = db.query(Position).filter(Position.ts_code == plan.ts_code).first()
                if not position:
                    position = Position(
                        ts_code=plan.ts_code,
                        symbol=plan.ts_code.split('.')[0],
                        name=stock_name,
                        vol=0,
                        available_vol=0,
                        avg_price=0.0
                    )
                    db.add(position)

                old_cost = position.vol * position.avg_price
                new_cost = old_cost + total_cost
                position.vol += volume
                position.avg_price = new_cost / position.vol
                position.current_price = exec_price
                position.market_value = position.vol * current_price
                position.float_pnl = position.market_value - new_cost
                position.pnl_pct = (position.float_pnl / new_cost * 100) if new_cost > 0 else 0

                record = TradeRecord(
                    ts_code=plan.ts_code,
                    name=stock_name,
                    trade_type='BUY',
                    price=exec_price,
                    vol=volume,
                    amount=need_cash,
                    fee=fee,
                    plan_id=plan.id
                )
                db.add(record)

                plan.executed = True
                plan.entry_price = exec_price
                decision_time = datetime.now().strftime("%H:%M:%S")
                plan.review_content = f"[{decision_time} AI成交] {order_type} @ {exec_price:.2f}. 成交{volume}股"
                if hasattr(plan, 'ai_reason') and plan.ai_reason:
                    plan.review_content += f"。理由: {plan.ai_reason}"

                all_positions = db.query(Position).filter(Position.vol > 0).all()
                total_mv = sum(p.market_value for p in all_positions)
                account.market_value = total_mv
                account.total_assets = account.available_cash + account.frozen_cash + account.market_value
                
                initial_capital = settings.INITIAL_CAPITAL
                account.total_pnl = account.total_assets - initial_capital
                account.total_pnl_pct = (account.total_pnl / initial_capital * 100)

                db.commit()
                try:
                    from app.services.plan_event_service import plan_event_service
                    await plan_event_service.publish({
                        "type": "plan_removed",
                        "plan_id": int(plan.id or 0),
                        "ts_code": str(plan.ts_code),
                        "reason": "executed"
                    })
                except Exception:
                    pass
                try:
                    dup_plans = db.query(TradingPlan).filter(
                        TradingPlan.date == plan.date,
                        TradingPlan.ts_code == plan.ts_code,
                        TradingPlan.executed == False
                    ).all()
                    if dup_plans:
                        async with trading_lock_manager.lock("trade:account"):
                            for dp in dup_plans:
                                if dp.id == plan.id:
                                    continue
                                if self._infer_plan_action(dp) != "BUY":
                                    continue
                                await self._unfreeze_funds(db, dp)
                                dp.track_status = "CANCELLED"
                                dp.executed = True
                                dp.review_content = f"[{datetime.now().strftime('%H:%M:%S')}] 已成交后自动撤销重复委托"
                            db.commit()
                except Exception as e:
                    db.rollback()
                    logger.error(f"Error cancelling duplicate BUY entrustments for {plan.ts_code}: {e}")
                logger.info(f"🚀 Executed BUY for {plan.ts_code}: {volume} shares at {exec_price} ({order_type}). Account assets updated.")


                from app.services.audit_service import audit_service
                await audit_service.run_realtime_audit(str(plan.ts_code))

                return True

            except Exception as e:
                db.rollback()
                logger.error(f"Error executing buy for {plan.ts_code}: {e}")
                return False

    async def _collect_pattern_if_successful(self, db: Session, plan: TradingPlan, pnl_pct: float):
        """
        收集交易案例到 PatternCase (数据闭环)
        记录所有交易案例（包括盈利和亏损），用于AI学习
        """
        try:
            # 1. 获取买入日的市场快照
            market_snapshot = plan.market_snapshot_json
            if not market_snapshot:
                # 尝试补救：如果是当天的，可以重新获取；否则只能留空
                if plan.date == date.today():
                    snapshot = await data_provider.get_market_snapshot()
                    market_snapshot = json.dumps(snapshot, ensure_ascii=False)
                else:
                    market_snapshot = "{}"

            # 2. 获取买入前的 K 线形态 (前 10 天)
            kline_data = "[]"
            try:
                end_date_str = plan.date.strftime('%Y%m%d')
                start_date = (plan.date - timedelta(days=20)).strftime('%Y%m%d')
                kline_list = await data_provider.get_kline(str(plan.ts_code), start_date=start_date, end_date=end_date_str)
                if kline_list:
                    # 取最后 10 行
                    tail_list = kline_list[-10:]
                    kline_data = json.dumps(tail_list)
            except Exception as e:
                logger.warning(f"Failed to fetch kline for pattern: {e}")

            # 3. 创建 PatternCase 记录（包含成功和失败案例）
            is_successful = pnl_pct >= 0.5  # 盈利>=0.5%标记为成功，否则为失败
            pattern = PatternCase(
                ts_code=plan.ts_code,
                trade_date=plan.date,
                pattern_type=plan.strategy_name,
                market_environment=market_snapshot,
                kline_pattern=kline_data,
                profit_pct=pnl_pct,
                hold_days=(date.today() - plan.date).days,
                is_successful=is_successful
            )
            await asyncio.to_thread(db.add, pattern)
            result_type = "成功" if is_successful else "失败"
            logger.info(f"✨ Collected {result_type} pattern for {plan.ts_code} ({pnl_pct:+.2f}%)")
        except Exception as e:
            logger.error(f"Error collecting pattern: {e}")

    async def _check_limit_order_queue(self, db: Session, plan: TradingPlan, quote: Dict, side: str = "BUY", order_vol: Optional[int] = None) -> bool:
        """
        模拟交易所排队制度 (涨跌停排队)
        :param side: BUY 或 SELL
        :return: True 可以成交, False 需要排队
        """
        try:
            if not quote:
                return True
            current_price = float(quote.get('price', 0) or 0)
            pre_close = float(quote.get('pre_close', 0))
            if pre_close <= 0:
                return True
            
            # 获取涨跌停价
            limit_up, limit_down = get_limit_prices(str(plan.ts_code), pre_close)
            
            # 获取买卖盘信息
            bid_ask = quote.get('bid_ask', {})
            b1_v = float(bid_ask.get('b1_v', 0))
            s1_v = float(bid_ask.get('s1_v', 0))
            total_vol = float(quote.get('vol', 0)) # 当前总成交量 (手)
            order_hands = self._shares_to_hands(order_vol) if order_vol else 0

            if side == "SELL" and current_price >= limit_up:
                if plan.market_snapshot_json:
                    try:
                        snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                    except:
                        snapshot = {}
                    if snapshot.get("queue_info"):
                        snapshot.pop("queue_info", None)
                        plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                        await asyncio.to_thread(db.commit)
                if order_hands <= 0:
                    return True
                if b1_v > 0 and b1_v >= order_hands:
                    return True
                return False

            if side == "BUY" and current_price <= limit_down:
                if plan.market_snapshot_json:
                    try:
                        snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                    except:
                        snapshot = {}
                    if snapshot.get("queue_info"):
                        snapshot.pop("queue_info", None)
                        plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                        await asyncio.to_thread(db.commit)
                if order_hands <= 0:
                    return True
                if s1_v > 0 and s1_v >= order_hands:
                    return True
                return False

            # 1. 涨停买入排队逻辑
            if side == "BUY" and current_price >= limit_up:
                # [Fix] 如果虽然价格在涨停板，但仍有卖单 (未封死)，允许买入
                # 模拟交易中，只要卖一量足够覆盖委托量，即可成交
                if order_hands > 0 and s1_v > 0 and s1_v >= order_hands:
                    if plan.market_snapshot_json:
                        try:
                            snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                        except Exception:
                            snapshot = {}
                        if snapshot.get("queue_info"):
                            snapshot.pop("queue_info", None)
                            plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                            await asyncio.to_thread(db.commit)
                    return True

                snapshot = {}
                if plan.market_snapshot_json:
                    try:
                        snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                    except:
                        snapshot = {}

                queue_info = snapshot.get('queue_info')
                if not queue_info or queue_info.get("queue_type") != "LIMIT_UP_BUY":
                    queue_info = {
                        "queue_type": "LIMIT_UP_BUY",
                        "start_vol": total_vol,
                        "ahead_hands": b1_v,
                        "order_hands": order_hands,
                        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    }
                    snapshot['queue_info'] = queue_info
                    plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                    plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')}] 涨停封死，开始排队。当前封单: {b1_v:.0f}手"
                    await asyncio.to_thread(db.commit)
                    return False
                return False

            # 2. 跌停卖出排队逻辑
            if side == "SELL" and current_price <= limit_down:
                if order_hands > 0 and b1_v > 0 and b1_v >= order_hands:
                    if plan.market_snapshot_json:
                        try:
                            snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                        except Exception:
                            snapshot = {}
                        if snapshot.get("queue_info"):
                            snapshot.pop("queue_info", None)
                            plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                            await asyncio.to_thread(db.commit)
                    return True

                snapshot = {}
                if plan.market_snapshot_json:
                    try:
                        snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                    except:
                        snapshot = {}

                queue_info = snapshot.get('queue_info')
                if not queue_info or queue_info.get("queue_type") != "LIMIT_DOWN_SELL":
                    queue_info = {
                        "queue_type": "LIMIT_DOWN_SELL",
                        "start_vol": total_vol,
                        "ahead_hands": s1_v,
                        "order_hands": order_hands,
                        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    snapshot['queue_info'] = queue_info
                    plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                    plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')}] 跌停封死，等待卖出排队。当前封单: {s1_v:.0f}手"
                    await asyncio.to_thread(db.commit)
                    return False
                return False

            if plan.market_snapshot_json:
                try:
                    snapshot = json.loads(str(plan.market_snapshot_json)) or {}
                except:
                    snapshot = {}
                if snapshot.get("queue_info"):
                    snapshot.pop("queue_info", None)
                    plan.market_snapshot_json = json.dumps(snapshot, ensure_ascii=False)
                    await asyncio.to_thread(db.commit)

            return True # 非涨跌停状态，直接成交
        except Exception as e:
            logger.error(f"Error checking limit order queue: {e}")
            return True # 出错时默认允许成交，避免系统卡死

    async def execute_sell(self, db: Session, ts_code: str, suggested_price: float, volume: int = None, reason: str = "Manual/AI Sell", order_type: str = "MARKET", plan_id: int = None) -> bool:
        """
        执行卖出操作 (支持 0.5% 价格误差控制)
        :param suggested_price: AI 建议的卖出价格
        :param order_type: MARKET (市价单，带0.5%保护) 或 LIMIT (限价单)
        :param plan_id: 关联的交易计划ID (可选)
        """
        # [新增] 交易时间检查 (DRY优化)
        if not self._validate_trade_time(ts_code, None):
            return False

        try:
            # 0. 价格误差控制
            quote = await data_provider.get_realtime_quote(ts_code, cache_scope="trading")
            if not quote:
                logger.error(f"Cannot get realtime quote for {ts_code}, aborting sell.")
                return False
            if self._is_suspended_quote(quote):
                logger.info(f"Skip sell execution for {ts_code}: Suspended quote.")
                return False
            
            current_price = float(quote['price'])
            
            order_plan = None
            if plan_id:
                candidate = await asyncio.to_thread(lambda: db.query(TradingPlan).get(plan_id))
                if candidate and candidate.executed:
                    return False
                if candidate and self._infer_plan_action(candidate) == "SELL":
                    order_plan = candidate

            open_plan = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.ts_code == ts_code,
                    TradingPlan.executed == True,
                    TradingPlan.exit_price == None,
                ).order_by(TradingPlan.date.desc()).first()
            )
            if not open_plan:
                open_plan = await asyncio.to_thread(
                    lambda: db.query(TradingPlan).filter(
                        TradingPlan.ts_code == ts_code,
                        TradingPlan.executed == True,
                        TradingPlan.exit_price == None,
                    ).order_by(TradingPlan.date.desc()).first()
                )

            if not order_plan:
                inferred = "持仓卖出"
                reason_txt = reason or ""
                if "[REDUCE]" in reason_txt or "减仓" in reason_txt or "减持" in reason_txt:
                    inferred = "持仓减仓"
                if "清仓" in reason_txt:
                    inferred = "清仓卖出"

                incoming_decision = "SELL"
                if inferred in ["持仓减仓"] or "[REDUCE]" in reason_txt or "减仓" in reason_txt or "减持" in reason_txt:
                    incoming_decision = "REDUCE"

                lock_key = f"plan:{date.today().isoformat()}:{ts_code}:SELL"
                async with trading_lock_manager.lock(lock_key):
                    candidate_plans = await asyncio.to_thread(
                        lambda: db.query(TradingPlan).filter(
                            TradingPlan.date == date.today(),
                            TradingPlan.ts_code == ts_code,
                            TradingPlan.executed == False,
                        ).all()
                    )
                    existing_plan = None
                    for cp in candidate_plans:
                        if self._infer_plan_action(cp) == "SELL":
                            existing_plan = cp
                            break

                    if existing_plan:
                        if (existing_plan.source or "").lower() != "user":
                            was_tracking = (existing_plan.track_status or "").upper() == "TRACKING"
                            existing_plan.strategy_name = inferred
                            existing_plan.buy_price_limit = float(suggested_price or current_price or 0.0)
                            existing_plan.reason = reason_txt
                            existing_plan.order_type = (order_type or "MARKET")
                            existing_plan.limit_price = float(suggested_price or current_price or 0.0)
                            existing_plan.ai_decision = incoming_decision
                            existing_plan.updated_at = datetime.now()
                            if was_tracking:
                                existing_plan.track_status = None
                                now_str = datetime.now().strftime("%H:%M:%S")
                                existing_plan.review_content = f"[{now_str}] 计划更新，重新进入待执行"
                        order_plan = existing_plan
                    else:
                        sell_plan = TradingPlan()
                        sell_plan.date = date.today()
                        sell_plan.ts_code = ts_code
                        sell_plan.strategy_name = inferred
                        sell_plan.buy_price_limit = float(suggested_price or current_price or 0.0)
                        sell_plan.stop_loss_price = 0.0
                        sell_plan.take_profit_price = 0.0
                        sell_plan.position_pct = 0.0
                        sell_plan.reason = reason_txt
                        sell_plan.order_type = (order_type or "MARKET")
                        sell_plan.limit_price = float(suggested_price or current_price or 0.0)
                        sell_plan.executed = False
                        sell_plan.score = 0.0
                        sell_plan.source = "system"
                        sell_plan.ai_decision = incoming_decision
                        sell_plan.created_at = datetime.now()
                        await asyncio.to_thread(db.add, sell_plan)
                        await asyncio.to_thread(db.flush)
                        order_plan = sell_plan

                    await asyncio.to_thread(db.commit)
                    if order_plan:
                        try:
                            await asyncio.to_thread(db.refresh, order_plan)
                        except Exception:
                            pass

            order_type_val = (order_type or (order_plan.order_type if order_plan else "MARKET") or "MARKET").upper()
            bid_price = float((quote.get("bid_ask") or {}).get("b1_p", 0) or 0)
            limit_p = float(suggested_price or 0.0)
            if limit_p <= 0 and order_plan:
                limit_p = float(order_plan.limit_price or order_plan.buy_price_limit or 0.0)
            if order_type_val == "LIMIT" and limit_p <= 0:
                logger.warning(f"Invalid LIMIT sell price for {ts_code}: {suggested_price} (plan_id={plan_id})")
                return False
            if limit_p > 0:
                trigger_price = current_price if current_price > 0 else bid_price
                if trigger_price < limit_p:
                    logger.info(
                        f"Price error control: {ts_code} trigger {trigger_price} < limit {limit_p}, waiting..."
                    )
                    return False
            
            # 使用当前成交价执行
            price = bid_price if bid_price > 0 else current_price
            logger.info(f"Executing SELL for {ts_code}: Suggest {suggested_price}, Actual {price}, Vol {volume}, Reason {reason}")

            async with trading_lock_manager.lock("trade:account"):
                if order_plan:
                    try:
                        db.refresh(order_plan)
                    except Exception:
                        pass
                    if order_plan.executed:
                        return False

                account = await self._get_or_create_account(db)
                stock_info = await asyncio.to_thread(lambda: db.query(Stock).filter(Stock.ts_code == ts_code).first())
                stock_name = str((stock_info.name if stock_info else ts_code) or ts_code or "")

                position = await asyncio.to_thread(lambda: db.query(Position).filter(Position.ts_code == ts_code).first())
                if not position or position.vol <= 0:
                    logger.warning(f"No position found for {ts_code}, cannot sell.")
                    return False

                if volume is None:
                    volume = int(position.vol or 0)
                volume = self._normalize_sell_volume(ts_code, int(volume))
                if volume <= 0:
                    logger.warning(f"Sell volume normalized to 0 for {ts_code}, skipping sell.")
                    return False

                available_vol = self._normalize_sell_volume(ts_code, int(position.available_vol or 0))
                if volume > available_vol:
                    logger.warning(f"Insufficient available volume for {ts_code}: Have {available_vol}, Want to sell {volume} (T+1)")
                    return False

                if order_plan:
                    can_trade = await self._check_limit_order_queue(db, order_plan, quote, side="SELL", order_vol=volume)
                    if not can_trade:
                        return False

                amount = volume * price
                total_fee = self._calc_sell_fees(ts_code, amount)
                net_amount = amount - total_fee

                account.available_cash += net_amount

                sold_cost = volume * float(position.avg_price or 0.0)
                sold_pnl = net_amount - sold_cost
                sold_pnl_pct = (sold_pnl / sold_cost * 100) if sold_cost > 0 else 0

                position.vol -= volume
                if position.available_vol > 0:
                    position.available_vol = max(0, int(position.available_vol or 0) - volume)

                if position.vol <= 0:
                    await asyncio.to_thread(db.delete, position)
                    logger.info(f"Position for {ts_code} cleared and removed from database.")
                else:
                    position.market_value = float(position.vol or 0) * price
                    cost = float(position.vol or 0) * float(position.avg_price or 0.0)
                    position.float_pnl = float(position.market_value or 0.0) - cost
                    position.pnl_pct = (float(position.float_pnl or 0.0) / cost * 100) if cost > 0 else 0.0

                record = TradeRecord()
                record.ts_code = ts_code
                record.name = stock_name
                record.trade_type = "SELL"
                record.price = price
                record.vol = volume
                record.amount = net_amount
                record.fee = total_fee
                record.trade_time = datetime.now()
                record.plan_id = int(order_plan.id) if order_plan and order_plan.id is not None else None
                record.pnl_pct = sold_pnl_pct
                await asyncio.to_thread(db.add, record)

                if order_plan:
                    order_plan.exit_price = price
                    order_plan.pnl_pct = sold_pnl_pct
                    order_plan.close_reason = reason
                    order_plan.executed = True

                if open_plan:
                    open_plan.exit_price = price
                    open_plan.real_pnl = float(sold_pnl)
                    open_plan.pnl_pct = sold_pnl_pct
                    open_plan.real_pnl_pct = sold_pnl_pct
                    open_plan.close_reason = reason
                    open_plan.executed = True
                    if open_plan.frozen_amount and open_plan.frozen_amount > 0:
                        await self._unfreeze_funds(db, open_plan)
                    await self._collect_pattern_if_successful(db, open_plan, float(sold_pnl_pct))

                all_positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
                ts_codes = [str(p.ts_code) for p in all_positions if p.ts_code]
                quotes = await data_provider.get_realtime_quotes(ts_codes) if ts_codes else {}
                total_mv, _ = await self._refresh_positions_with_quotes(all_positions, quotes)
                account.market_value = float(total_mv or 0.0)
                account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
                self._recalc_account_pnl(account)

                await asyncio.to_thread(db.commit)
                try:
                    from app.services.plan_event_service import plan_event_service
                    if order_plan and order_plan.id:
                        await plan_event_service.publish({
                            "type": "plan_removed",
                            "plan_id": int(order_plan.id),
                            "ts_code": str(ts_code),
                            "reason": "sell_executed"
                        })
                except Exception:
                    pass

                logger.info(f"🚀 Executed {order_type} SELL for {ts_code}: {volume} shares at {price} (PnL: {sold_pnl_pct:.2f}%, Suggested: {suggested_price}). Account assets updated.")

                from app.services.audit_service import audit_service
                await audit_service.run_realtime_audit(ts_code)

                return True
            
        except Exception as e:
            await asyncio.to_thread(db.rollback)
            logger.error(f"Error executing sell for {ts_code}: {e}")
            return False

    async def execute_pending_sell_plans(self) -> int:
        """
        处理【今日】未成交的卖出委托（包含用户手动挂单与系统卖出挂单）
        规则：
        - LIMIT：当前价 >= 委托价时触发成交
        - MARKET：直接触发成交（由 execute_sell 内部做交易时间拦截）
        """
        db = SessionLocal()
        executed_count = 0
        try:
            from sqlalchemy import or_

            plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == date.today(),
                    TradingPlan.executed == False,
                    or_(TradingPlan.track_status == None, TradingPlan.track_status != "TRACKING")
                ).all()
            )
            sell_plans = [p for p in plans if self._infer_plan_action(p) == "SELL"]
            if not sell_plans:
                return 0
            dedup = {}
            for p in sorted(sell_plans, key=lambda x: (x.created_at or datetime.min, x.id or 0), reverse=True):
                key = str(p.ts_code)
                if key in dedup:
                    continue
                dedup[key] = p
            if len(dedup) != len(sell_plans):
                logger.warning(f"Duplicate SELL entrustments detected, deduped {len(sell_plans)} -> {len(dedup)}")
            sell_plans = list(dedup.values())

            ts_codes = [str(p.ts_code) for p in sell_plans if p.ts_code]
            quotes = await data_provider.get_realtime_quotes(ts_codes, cache_scope="trading")

            for p in sell_plans:
                ts_code = p.ts_code
                if not ts_code:
                    continue

                quote = quotes.get(str(ts_code))
                current_price = float(quote.get("price", 0)) if quote else 0.0
                if current_price <= 0:
                    continue

                pos = await asyncio.to_thread(lambda: db.query(Position).filter(Position.ts_code == ts_code).first())
                if not pos or int(pos.vol or 0) <= 0:
                    p.track_status = "TRACKING"
                    p.review_content = f"[{datetime.now().strftime('%H:%M:%S')}] 卖出委托停止：当前无持仓"
                    await asyncio.to_thread(db.commit)
                    continue

                available_vol = int(pos.available_vol or 0)
                if available_vol <= 0:
                    continue

                order_type_val = (p.order_type or "MARKET").upper()
                limit_price = float(p.limit_price or p.buy_price_limit or 0.0)
                suggested_price = limit_price if limit_price > 0 else (float(p.buy_price_limit or 0.0) or current_price)

                if order_type_val == "LIMIT" and limit_price <= 0:
                    if not (p.review_content or "").strip():
                        p.review_content = f"[{datetime.now().strftime('%H:%M:%S')}] 限价卖出委托价无效，等待手工修正/撤单"
                        await asyncio.to_thread(db.commit)
                    continue
                if limit_price > 0 and current_price < limit_price:
                    continue

                strategy_text = f"{p.strategy_name or ''} {p.reason or ''}"
                if any(k in strategy_text for k in ["减仓", "减持", "[REDUCE]"]):
                    volume = self._calc_reduce_volume(available_vol)
                else:
                    volume = available_vol

                if volume <= 0:
                    continue

                ok = await self.execute_sell(
                    db,
                    ts_code=str(ts_code),
                    suggested_price=suggested_price,
                    volume=int(volume),
                    reason=str(p.reason or p.strategy_name or "Pending Sell"),
                    order_type=order_type_val,
                    plan_id=int(p.id or 0)
                )
                if ok:
                    executed_count += 1

            return executed_count
        finally:
            db.close()

    async def execute_pending_buy_entrustments(self) -> int:
        # 使用统一验证，确保在非交易时段正确处理
        if not self._validate_trade_time("__system__", lambda x: None):
            return 0
        from app.services.reward_punish_service import reward_punish_service
        if reward_punish_service.is_trading_paused():
            return 0

        db = SessionLocal()
        executed_count = 0
        try:
            from sqlalchemy import or_

            plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == date.today(),
                    TradingPlan.executed == False,
                    TradingPlan.frozen_amount > 0,
                    or_(TradingPlan.track_status == None, TradingPlan.track_status != "TRACKING")
                ).all()
            )
            if not plans:
                return 0

            plans = [p for p in plans if self._infer_plan_action(p) == "BUY"]
            if not plans:
                return 0
            dedup = {}
            for p in sorted(plans, key=lambda x: (x.created_at or datetime.min, x.id or 0), reverse=True):
                key = str(p.ts_code)
                if key in dedup:
                    continue
                dedup[key] = p
            if len(dedup) != len(plans):
                logger.warning(f"Duplicate BUY entrustments detected, deduped {len(plans)} -> {len(dedup)}")
            plans = list(dedup.values())

            ts_codes = [str(p.ts_code) for p in plans if p.ts_code]
            if not ts_codes:
                return 0

            quotes = await data_provider.get_realtime_quotes(ts_codes)
            for p in plans:
                if not p.ts_code:
                    continue

                quote = quotes.get(str(p.ts_code)) or {}
                if self._is_suspended_quote(quote):
                    continue

                current_price = float(quote.get("price", 0) or 0)
                if current_price <= 0:
                    continue

                order_type_val = (p.order_type or "MARKET").upper()
                ask_price = float((quote.get("bid_ask") or {}).get("s1_p", 0) or 0)
                trigger_price = ask_price if ask_price > 0 else current_price

                limit_p = float(p.limit_price or p.buy_price_limit or 0.0)
                if order_type_val == "LIMIT" and limit_p <= 0:
                    continue
                if limit_p > 0:
                    if trigger_price <= 0 or trigger_price > limit_p:
                        continue

                ok = await self.execute_buy(
                    db,
                    p,
                    suggested_price=float(p.limit_price or p.buy_price_limit or current_price),
                )
                if ok:
                    executed_count += 1

            return executed_count
        finally:
            db.close()

    async def execute_pending_entrustments(self) -> dict:
        # 使用统一验证，确保在非交易时段正确处理
        if not self._validate_trade_time("__system__", lambda x: None):
            return {"buy_executed": 0, "sell_executed": 0}

        buy_executed = await self.execute_pending_buy_entrustments()
        sell_executed = await self.execute_pending_sell_plans()
        return {
            "buy_executed": int(buy_executed or 0),
            "sell_executed": int(sell_executed or 0),
        }

    async def sync_account_assets(self, quotes_override: Optional[Dict[str, Dict]] = None):
        """
        同步账户资产：根据最新行情更新持仓市值和总资产
        """
        db = SessionLocal()
        try:
            logger.info("Sync Assets: Starting account asset synchronization...")
            account = await self._get_or_create_account(db)
            positions: List[Position] = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
            logger.info(f"Sync Assets: Found {len(positions)} active positions.")

            async with trading_lock_manager.lock("trade:account"):
                logger.info("Sync Assets: Locked account for cash reconciliation.")
                expected_total_cash = await self._calc_expected_total_cash(db)
                expected_frozen = await self._calc_expected_frozen_cash(db)
                
                if expected_frozen > expected_total_cash + 0.01:
                    logger.warning(f"Sync Assets: Expected frozen ({expected_frozen:.2f}) > total cash ({expected_total_cash:.2f}). Capping.")
                    expected_frozen = expected_total_cash
                    
                expected_available = float(expected_total_cash - expected_frozen)
                
                logger.info(f"Sync Assets: Reconciliation - Total: {expected_total_cash:.2f}, Frozen: {expected_frozen:.2f}, Available: {expected_available:.2f}")
                
                # 更新账户现金
                changed = False
                if abs(float(account.available_cash or 0.0) - expected_available) > 0.01:
                    logger.info(f"Sync Assets: Updating available cash {account.available_cash:.2f} -> {expected_available:.2f}")
                    account.available_cash = expected_available
                    changed = True
                    
                if abs(float(account.frozen_cash or 0.0) - expected_frozen) > 0.01:
                    logger.info(f"Sync Assets: Updating frozen cash {account.frozen_cash:.2f} -> {expected_frozen:.2f}")
                    account.frozen_cash = expected_frozen
                    changed = True
                
                if changed:
                    await asyncio.to_thread(db.commit)
                    await asyncio.to_thread(db.refresh, account)
            
            if not positions:
                logger.info("Sync Assets: No positions to update. Recalculating totals.")
                async with trading_lock_manager.lock("trade:account"):
                    account.market_value = 0
                    account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0)
                    self._recalc_account_pnl(account)
                    await asyncio.to_thread(db.commit)
                return

            ts_codes = [p.ts_code for p in positions]
            norm_codes = [data_provider._normalize_ts_code(c) for c in ts_codes]
            if quotes_override is not None:
                quotes = quotes_override
            else:
                logger.info(f"Sync Assets: Fetching realtime quotes for {norm_codes}")
                try:
                    quotes = await asyncio.wait_for(data_provider.get_realtime_quotes(norm_codes), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.error("Sync Assets: Timeout fetching realtime quotes.")
                    quotes = {}
                except Exception as qe:
                    logger.error(f"Sync Assets: Error fetching realtime quotes: {qe}")
                    quotes = {}
            
            total_market_value = 0.0
            update_count = 0
            
            for pos in positions:
                norm_code = data_provider._normalize_ts_code(pos.ts_code)
                quote = quotes.get(norm_code) or quotes.get(pos.ts_code)
                if quote:
                    current_price = float(quote['price'])
                    if current_price > 0:
                        pos.current_price = current_price
                        pos.market_value = pos.vol * current_price
                        update_count += 1
                
                # 如果获取不到行情，保留原有的 current_price 和 market_value
                # 这样可以防止行情服务波动时市值瞬间归零
                total_market_value += (pos.market_value or 0.0)
                
                # 更新盈亏 (基于最新的或保留的 current_price)
                cost = pos.vol * pos.avg_price
                pos.float_pnl = (pos.market_value or 0.0) - cost
                pos.pnl_pct = (pos.float_pnl / cost * 100) if cost > 0 else 0
            
            if update_count < len(positions):
                logger.warning(f"Sync Assets: Only updated {update_count}/{len(positions)} positions. Retaining old prices for the rest.")

            async with trading_lock_manager.lock("trade:account"):
                await asyncio.to_thread(db.refresh, account)
                account.market_value = total_market_value
                account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
                self._recalc_account_pnl(account)
                await asyncio.to_thread(db.commit)
                
            logger.info(f"Sync Assets: Completed. Total Assets: {account.total_assets:.2f}, Market Value: {account.market_value:.2f}, Update Count: {update_count}")
            
        except Exception as e:
            logger.error(f"Sync Assets: Fatal error: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            db.close()

    async def _refresh_positions_with_quotes(self, positions: List[Position], quotes: Dict[str, Dict]) -> Tuple[float, int]:
        total_market_value = 0.0
        updated_count = 0
        for pos in positions:
            norm_code = data_provider._normalize_ts_code(pos.ts_code)
            quote = quotes.get(norm_code) if quotes else None
            current_price = float(quote.get("price", 0) or 0.0) if quote else 0.0
            if current_price > 0:
                pos.current_price = current_price
                pos.market_value = float(pos.vol or 0) * current_price
                updated_count += 1

            mv = float(pos.market_value or 0.0)
            total_market_value += mv

            cost = float(pos.vol or 0) * float(pos.avg_price or 0.0)
            pos.float_pnl = mv - cost
            pos.pnl_pct = (pos.float_pnl / cost * 100) if cost > 0 else 0.0

        return total_market_value, updated_count

    def _recalc_account_pnl(self, account: Account) -> None:
        initial_capital = settings.INITIAL_CAPITAL
        account.total_pnl = float(account.total_assets or 0.0) - initial_capital
        account.total_pnl_pct = (account.total_pnl / initial_capital * 100) if initial_capital != 0 else 0.0

    async def settle_positions(self):
        """
        每日盘后/盘前结算：将所有持仓的 available_vol 重置为 vol (T+1 规则)
        核心逻辑：可用持仓 = 总持仓 - 今日买入持仓
        """
        db = SessionLocal()
        try:
            from sqlalchemy import func
            from datetime import date, datetime, time
            
            # 1. 获取今日所有买入成交记录的汇总
            today_start = datetime.combine(date.today(), time.min)
            buy_records = await asyncio.to_thread(lambda: db.query(
                TradeRecord.ts_code, 
                func.sum(TradeRecord.vol).label('total_buy_vol')
            ).filter(
                TradeRecord.trade_type == 'BUY',
                TradeRecord.trade_time >= today_start
            ).group_by(TradeRecord.ts_code).all())
            
            today_buy_map = {r.ts_code: r.total_buy_vol for r in buy_records}
            
            # 2. 更新持仓可用数量并清理 0 仓位
            all_positions = await asyncio.to_thread(lambda: db.query(Position).all())
            updated_count = 0
            deleted_count = 0
            for pos in all_positions:
                if pos.vol <= 0:
                    await asyncio.to_thread(db.delete, pos)
                    deleted_count += 1
                    continue
                    
                today_buy_vol = today_buy_map.get(pos.ts_code, 0)
                # 可用数量 = 当前总持仓 - 今日买入数量
                raw_available = max(0, pos.vol - today_buy_vol)
                new_available = self._normalize_sell_volume(pos.ts_code, raw_available)
                
                if pos.available_vol != new_available:
                    pos.available_vol = new_available
                    updated_count += 1
            
            if updated_count > 0 or deleted_count > 0:
                await asyncio.to_thread(db.commit)
                logger.info(f"Settled positions: Updated {updated_count}, Deleted {deleted_count} empty positions.")
            else:
                logger.info("Settled positions: No updates needed.")
                
        except Exception as e:
            logger.error(f"Error settling positions: {e}")
            await asyncio.to_thread(db.rollback)
        finally:
            db.close()

    async def _get_unified_trading_context(self, db, ts_code: str):
        """
        [系统级加固] 统一获取 AI 交易所需要的全部上下文数据。
        确保全系统（买入监控、卖出监控、尾盘决策）使用相同的数据契约。
        """
        from app.services.chat_service import chat_service
        from app.services.search_service import search_service
        from app.services.market.market_data_service import market_data_service
        
        # 1. 基础信息
        stock_info = await asyncio.to_thread(lambda: db.query(Stock).filter(Stock.ts_code == ts_code).first())
        stock_name = str((stock_info.name if stock_info else ts_code) or ts_code or "")
        
        # 2. 原始多周期 K 线 (30日/12周/6月)
        raw_context = ""
        try:
            with market_data_service.cache_scope("trading"):
                raw_context = await asyncio.wait_for(chat_service.get_ai_trading_context(ts_code, cache_scope="trading"), timeout=30.0)
        except Exception as e:
            logger.warning(f"Context Provider: Raw context failed for {ts_code}: {e}")
        
        # 3. 市场与搜索资讯 (包含实时盘口、行业、搜索结果)
        search_info = ""
        try:
            now_ts = datetime.now().timestamp()
            cached = self._intraday_search_cache.get(ts_code)
            if cached and now_ts - cached[0] < 900:
                search_info = cached[1]
            else:
                search_info = await asyncio.wait_for(search_service.search_stock_info(ts_code, stock_name), timeout=8.0)
                if len(self._intraday_search_cache) > 1000:
                    self._intraday_search_cache.clear()
                self._intraday_search_cache[ts_code] = (now_ts, search_info)
        except Exception as e:
            logger.error(f"Context Provider: Search failed for {ts_code}: {e}")

        # 4. 获取实时盘口
        quote = None
        try:
            with market_data_service.cache_scope("trading"):
                quote = await asyncio.wait_for(data_provider.get_realtime_quote(ts_code, cache_scope="trading"), timeout=3.0)
        except Exception:
            quote = None
        handicap_str = ""
        if quote and 'bid_ask' in quote:
            bid_ask = quote['bid_ask']
            handicap_str = f"""
            【实时五档盘口】
            卖五: {bid_ask.get('s5_p', 0)} ({bid_ask.get('s5_v', 0):.0f})
            卖四: {bid_ask.get('s4_p', 0)} ({bid_ask.get('s4_v', 0):.0f})
            卖三: {bid_ask.get('s3_p', 0)} ({bid_ask.get('s3_v', 0):.0f})
            卖二: {bid_ask.get('s2_p', 0)} ({bid_ask.get('s2_v', 0):.0f})
            卖一: {bid_ask.get('s1_p', 0)} ({bid_ask.get('s1_v', 0):.0f})
            -----------------------
            买一: {bid_ask.get('b1_p', 0)} ({bid_ask.get('b1_v', 0):.0f})
            买二: {bid_ask.get('b2_p', 0)} ({bid_ask.get('b2_v', 0):.0f})
            买三: {bid_ask.get('b3_p', 0)} ({bid_ask.get('b3_v', 0):.0f})
            买四: {bid_ask.get('b4_p', 0)} ({bid_ask.get('b4_v', 0):.0f})
            买五: {bid_ask.get('b5_p', 0)} ({bid_ask.get('b5_v', 0):.0f})
            """
            search_info = handicap_str + "\n\n" + search_info

        # 5. 板块联动
        sector_context = {}
        try:
            with market_data_service.cache_scope("trading"):
                sector_context = await asyncio.wait_for(data_provider.get_sector_context(ts_code), timeout=3.0)
        except Exception:
            sector_context = {}
        if sector_context.get('industry') != "未知":
            leaders_list = sector_context.get('leaders', [])
            leaders_str = ', '.join(leaders_list) if leaders_list else "无"
            sector_str = f"\n【板块联动】\n- 行业: {sector_context.get('industry')}\n- 龙头涨幅: {sector_context.get('avg_pct', 0):+.2f}%\n- 领涨: {leaders_str}\n"
            search_info = sector_str + search_info
            logger.info(f"Context Provider: Integrated sector data for {ts_code}: {sector_context.get('industry')} {sector_context.get('avg_pct')}%")

        # 6. 账户信息与挂单状态
        account = await asyncio.to_thread(lambda: db.query(Account).first())
        
        # 获取当前持仓明细
        positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
        pos_list = []
        for p in positions:
            pos_list.append(f"{p.ts_code}({p.name}): {p.vol}股, 盈亏{p.pnl_pct:.2f}%")
        
        from sqlalchemy import func
        today = date.today()
        effective_plan_date = await asyncio.to_thread(
            lambda: db.query(func.min(TradingPlan.date)).filter(TradingPlan.date >= today).scalar()
        )
        if effective_plan_date is None:
            effective_plan_date = await asyncio.to_thread(lambda: db.query(func.max(TradingPlan.date)).scalar())

        pending_plans = []
        if effective_plan_date is not None:
            pending_plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == effective_plan_date,
                    TradingPlan.executed == False
                ).all()
            )
        plan_list = []
        for pl in pending_plans:
            action = self._infer_plan_action(pl)
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
            plan_list.append(f"{pl.ts_code}: {action} {order_type} @ {price_text}, 状态: {status}")

        account_info = {
            "total_assets": account.total_assets if account else 0,
            "available_cash": account.available_cash if account else 0,
            "market_value": account.market_value if account else 0,
            "total_pnl_pct": account.total_pnl_pct if account else 0,
            "current_positions": "; ".join(pos_list) if pos_list else "空仓",
            "pending_orders": "; ".join(plan_list) if plan_list else "无"
        }

        return {
            "raw_context": raw_context,
            "search_info": search_info,
            "account_info": account_info,
            "stock_name": stock_name,
            "handicap_info": handicap_str
        }

    async def monitor_trades(self, force_open_confirm: bool = False):
        # [Fix] 严格限制交易时间，避免非交易时段（尤其是夜间）无意义运行与日志输出
        # 交易时间: 9:15-11:35, 13:00-15:01
        if not self._validate_trade_time("__monitor__", lambda x: None):
            return

        if force_open_confirm:
            logger.info("进入 09:25 开盘确认交易监控")
        try:
            await asyncio.wait_for(self._ensure_ai_confirm_for_no_price_plans(), timeout=20.0)
        except Exception:
            pass
        start = datetime.now()
        results = await asyncio.gather(
            asyncio.wait_for(self.check_and_execute_plans(force_open_confirm=force_open_confirm), timeout=90.0),
            asyncio.wait_for(self.execute_pending_sell_plans(), timeout=60.0),
            asyncio.wait_for(self.check_positions_and_sell(), timeout=90.0),
            return_exceptions=True
        )

        for idx, r in enumerate(results):
            if isinstance(r, Exception):
                logger.error(f"Trade monitor subtask {idx} failed: {type(r).__name__}: {r}")

        try:
            await asyncio.wait_for(self.sync_account_assets(), timeout=30.0)
        except Exception:
            pass
        logger.info(f"Trade monitor finished in {(datetime.now() - start).total_seconds():.1f}s")

    async def _ensure_ai_confirm_for_no_price_plans(self) -> int:
        db = SessionLocal()
        try:
            from sqlalchemy import or_

            plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == date.today(),
                    TradingPlan.executed == False,
                    or_(TradingPlan.track_status == None, TradingPlan.track_status != "TRACKING"),
                ).all()
            )
            if not plans:
                return 0

            targets = []
            for p in plans:
                if self._infer_plan_action(p) != "BUY":
                    continue
                if float(p.buy_price_limit or 0.0) <= 0 and float(p.limit_price or 0.0) <= 0:
                    targets.append(p)

            if not targets:
                return 0

            need_unfreeze = [p for p in targets if float(p.frozen_amount or 0.0) > 0]
            if need_unfreeze:
                async with trading_lock_manager.lock("trade:account"):
                    for p in need_unfreeze:
                        await self._unfreeze_funds(db, p)
                    db.commit()

            now_str = datetime.now().strftime("%H:%M:%S")
            updated = 0
            for p in targets:
                p.review_content = f"[{now_str} 等待AI确认] 无有效参考价，暂不挂单，等待行情更新后再确认"
                if p.ai_decision:
                    p.ai_decision = None
                updated += 1

            if updated > 0:
                db.commit()
                logger.info(f"无参考价计划已转入AI确认等待: {updated} 条")

            return updated
        except Exception as e:
            db.rollback()
            logger.error(f"无参考价计划AI确认清理失败: {e}")
            return 0
        finally:
            db.close()

    async def monitor_entrustments(self):
        if not data_provider.is_trading_time():
            return

        try:
            result = await asyncio.wait_for(self.execute_pending_entrustments(), timeout=30.0)
        except Exception:
            return
        buy_executed = int(result.get("buy_executed", 0) or 0)
        sell_executed = int(result.get("sell_executed", 0) or 0)
        if buy_executed + sell_executed <= 0:
            return

        from app.services.monitor_service import monitor_service

        log_id = await monitor_service.log_job_start("entrustment_monitor")
        await monitor_service.log_job_end(
            log_id,
            "SUCCESS",
            f"buy_executed={buy_executed}, sell_executed={sell_executed}"
        )

    async def has_active_entrustments(self) -> bool:
        db = SessionLocal()
        try:
            from sqlalchemy import or_

            plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == date.today(),
                    TradingPlan.executed == False,
                    or_(TradingPlan.track_status == None, TradingPlan.track_status != "TRACKING"),
                ).all()
            )
            if not plans:
                return False
            positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
            pos_map = {str(p.ts_code): int(p.available_vol or 0) for p in positions if p.ts_code}

            for p in plans:
                action = self._infer_plan_action(p)
                if action == "SELL":
                    avail = pos_map.get(str(p.ts_code), 0)
                    if avail > 0:
                        return True
                if action == "BUY":
                    if float(p.frozen_amount or 0.0) > 0 and int(p.frozen_vol or 0) >= self._get_min_buy_volume(str(p.ts_code)):
                        return True
            return False
        finally:
            db.close()

    async def check_and_execute_plans(self, force_open_confirm: bool = False):
        """
        盘中监控：检查并执行交易计划 (V2: 引入市场环境感知 + 真实账户逻辑)
        """
        if not settings.ENABLE_AUTO_TRADE:
            return

        # 1. 获取今日未执行的计划
        db = SessionLocal()
        try:
            from sqlalchemy import or_
            plans = db.query(TradingPlan).filter(
                TradingPlan.date == date.today(),
                TradingPlan.executed == False,
                or_(TradingPlan.track_status == None, TradingPlan.track_status != "TRACKING")
            ).all()
            
            if not plans:
                if force_open_confirm:
                    logger.info("开盘确认阶段：今日未找到待执行计划")
                return
            
            if force_open_confirm:
                logger.info(f"开盘确认阶段：检测到待执行计划 {len(plans)} 个")
            
            # 3. 预加载基础数据
            normalized_map = {p.id: data_provider._normalize_ts_code(p.ts_code) for p in plans}
            ts_codes = list(dict.fromkeys(normalized_map.values()))
            quotes = await data_provider.get_realtime_quotes(ts_codes)
            market_snapshot = await data_provider.get_market_snapshot()
            market_status = await ai_service.analyze_market_snapshot(market_snapshot) 
            
            # 3. 并行处理所有计划
            queue: asyncio.Queue[tuple[int, dict] | None] = asyncio.Queue()
            total = 0
            concurrency = max(1, min(int(self._monitor_plan_concurrency or 3), 10))
            for plan in plans:
                # 1.1 跳过持仓管理类计划 (由 check_positions_and_sell 负责)
                if plan.strategy_name in ['持仓卖出', '持仓减仓', '持仓做T', '持仓持有', '持仓管理']:
                    continue
                strategy_name = str(plan.strategy_name or "")
                if strategy_name.startswith("选股监控-"):
                    if (str(getattr(plan, "ai_decision", "") or "")).strip().upper() != "BUY":
                        continue

                decision_key = (str(getattr(plan, "ai_decision", "") or "")).strip().upper()
                if decision_key in ["BUY", "SELL", "REDUCE"]:
                    review_txt = (plan.review_content or "")
                    if "AI挂单" in review_txt and decision_key != "BUY":
                        continue
                
                norm_code = normalized_map.get(plan.id) or str(plan.ts_code)
                quote = quotes.get(norm_code) or quotes.get(str(plan.ts_code))
                if not quote:
                    continue
                
                await queue.put((plan.id, quote))
                total += 1
            
            if total > 0:
                for _ in range(concurrency):
                    await queue.put(None)

                async def _worker():
                    while True:
                        item = await queue.get()
                        try:
                            if item is None:
                                return
                            pid, q = item
                            try:
                                await asyncio.wait_for(
                                    self._process_single_plan(
                                        pid,
                                        q,
                                        market_status,
                                        market_snapshot,
                                        force_open_confirm=force_open_confirm,
                                    ),
                                    timeout=70.0,
                                )
                            except asyncio.TimeoutError:
                                logger.error(f"Processing plan {pid} timed out after 70s")
                            except Exception as e:
                                logger.error(f"Error processing plan {pid}: {e}", exc_info=True)
                        finally:
                            queue.task_done()

                workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
                await queue.join()
                await asyncio.gather(*workers, return_exceptions=True)
                
        finally:
            db.close()

    async def _process_single_plan(
        self,
        plan_id: int,
        quote: dict,
        market_status: str,
        market_snapshot: dict,
        force_open_confirm: bool = False,
    ):
        """处理单个交易计划的深度分析与执行逻辑"""
        # [并发加固] 内存级锁：检查该计划是否正在被分析中
        if plan_id in self._processing_plan_ids:
            logger.info(f"Plan {plan_id} is already being processed, skipping.")
            return

        self._processing_plan_ids.add(plan_id)
        db = SessionLocal()
        try:
            plan = db.query(TradingPlan).get(plan_id)
            if not plan or plan.executed:
                return
            if (plan.track_status or "").upper() == "TRACKING":
                return
            if str(plan.strategy_name or "").startswith("选股监控-"):
                if (str(getattr(plan, "ai_decision", "") or "")).strip().upper() != "BUY":
                    return

            decision_key = (str(getattr(plan, "ai_decision", "") or "")).strip().upper()
            # [权限提升] 即使已经处于“AI挂单”状态，如果满足重新分析条件（如价格大幅变动或过了冷却期），依然允许 AI 重新评估，以便在行情转弱时及时“撤单”
            # if decision_key in ["BUY", "SELL", "REDUCE"] and "AI挂单" in (plan.review_content or ""):
            #     return

            current_price = float(quote['price'])
            if current_price <= 0: return
            review_txt = (plan.review_content or "")
            if decision_key == "BUY" and "AI挂单" in review_txt and float(plan.frozen_amount or 0.0) > 0:
                now_time = datetime.now().time()
                if time(11, 30) <= now_time < time(13, 0):
                    decision_time = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{decision_time} AI挂单] 午休等待13:00后自动成交"
                    db.commit()
                    return
                order_type = (plan.order_type or "LIMIT").upper()
                ask_price = float((quote.get("bid_ask") or {}).get("s1_p", 0) or 0)
                trigger_price = current_price if current_price > 0 else ask_price
                exec_price = trigger_price
                should_execute = False
                if order_type == "MARKET":
                    should_execute = True
                elif order_type == "LIMIT":
                    limit_price = float(plan.limit_price or 0.0)
                    if limit_price > 0 and trigger_price <= limit_price:
                        should_execute = True
                if should_execute:
                    try:
                        plan.market_snapshot_json = json.dumps(market_snapshot, ensure_ascii=False)
                    except Exception:
                        pass
                    success = await self.execute_buy(db, plan, plan.limit_price or exec_price)
                    if success:
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        plan.review_content = f"[{decision_time} AI成交] {order_type} @ {exec_price}. 理由: {plan.ai_reason or 'AI确认挂单'}"
                        db.commit()
                        self._update_analysis_history(plan.ts_code, current_price)
                return

            now_time = datetime.now().time()
            created_time = plan.created_at.time() if plan.created_at else None
            
            # [新增] 区分计划来源：昨选计划 vs 盘中筛选计划
            # 昨选计划：创建时间早于今日开盘 (9:15)
            # 盘中筛选计划：创建时间晚于今日开盘 (9:15)
            is_intraday_plan = False
            if plan.created_at and plan.created_at.date() == date.today():
                if plan.created_at.time() >= time(9, 15):
                    is_intraday_plan = True
            
            # 午间强势策略属于盘中筛选计划的一种特殊情况，但这里已经包含了
            force_first_analysis = (
                (plan.strategy_name == "午间强势")
                and (plan.ai_decision is None or str(plan.ai_decision).strip() == "")
                and created_time is not None
                and time(11, 30) <= created_time < time(13, 0)
                and now_time >= time(13, 0)
            )
            
            # 4. 价格预警
            plan_price = float(plan.buy_price_limit or plan.limit_price or 0.0)
            
            if data_provider.is_trading_time() and self._is_suspended_quote(quote):
                return

            # [逻辑优化] 
            # 1. 盘中筛选计划：一旦生成，AI决定买入就直接执行 (前提是满足触发条件)
            # 2. 昨选计划：必须在 9:25 开盘价生成后进行二次确认
            
            # 触发深度分析的条件
            should_analyze = False
            is_open_confirm_window = force_open_confirm and time(9, 25) <= now_time < time(9, 30)
            if is_open_confirm_window:
                should_analyze = True
            elif force_first_analysis:
                should_analyze = True
            else:
                if plan_price <= 0:
                    return
                # 昨选计划在 9:15-9:25 之间拦截分析，等待 9:25 开盘后再分析
                if not is_intraday_plan and time(9, 15) <= now_time < time(9, 25):
                    # 记录拦截状态
                    if not plan.review_content:
                        plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')} 等待开盘] 昨选计划拦截，等待 09:25 集合竞价开盘后再执行二次确认"
                        db.commit()
                    return
                within_plan_range = False
                if plan_price > 0 and current_price > 0:
                    within_plan_range = abs(current_price - plan_price) / plan_price <= 0.01
                if not within_plan_range:
                    return
                if not self._should_trigger_deep_analysis(plan.ts_code, current_price, cooling_minutes=20, price_delta_pct=1.0):
                    return
                should_analyze = True

            if should_analyze:
                # [新增] 增强 9:25 开盘确认上下文
                effective_market_status = market_status
                if not is_intraday_plan and is_open_confirm_window:
                    effective_market_status = f"【09:25 开盘二次确认】当前为开盘集合竞价结束时刻，开盘价已出。请结合最新开盘情况，对昨选计划进行最终买入确认。\n{market_status}"
                
                logger.info(f"🎯 {'Intraday' if is_intraday_plan else 'Previous-day'} Price trigger for {plan.ts_code}: Current {current_price} near Plan {plan_price} (±1%). Starting AI Deep Analysis.")
                
                # 5. 获取统一的交易上下文
                ctx = await self._get_unified_trading_context(db, plan.ts_code)
                
                # 7. 调用 AI 决策
                decision = await ai_service.analyze_realtime_trade_signal_v3(
                    symbol=plan.ts_code,
                    strategy=plan.strategy_name,
                    current_price=current_price,
                    buy_price=plan_price,
                    raw_trading_context=ctx['raw_context'],
                    plan_reason=plan.reason,
                    market_status=effective_market_status,
                    search_info=ctx['search_info'],
                    account_info=ctx['account_info'],
                    is_intraday_plan=is_intraday_plan
                )

                if not decision or 'action' not in decision:
                    logger.error(f"AI 决策返回格式非法: {decision}")
                    plan.ai_decision = "WAIT"
                    plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')} AI异常] 决策无效，暂缓执行"
                    db.commit()
                    return

                logger.info(f"AI Decision for {plan.ts_code}: {decision}")
                
                # 8. 执行 AI 决策
                action = decision.get('action')
                order_type = decision.get('order_type', 'MARKET')
                ai_suggested_price = decision.get('price', 0)
                explicit_price = bool(decision.get("_explicit_price")) or float(ai_suggested_price or 0) > 0
                explicit_signal = bool(decision.get("_explicit_signal"))
                
                if not action:
                    return
                if str(action).upper() == "BUY":
                    reason_text = str(decision.get("reason") or "")
                    if not explicit_signal or not explicit_price or not reason_text.startswith("买"):
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        plan.ai_decision = "WAIT"
                        plan.review_content = f"[{decision_time} AI拒绝挂单] 买入信号不明确，必须以“买/价格”开头后再给理由"
                        db.commit()
                        return
                
                # 记录 AI 决策，但限价单必须尊重计划设定的最高买入价
                plan.ai_decision = action
                plan.order_type = order_type
                plan.ai_reason = decision.get('reason', 'AI决策') # 暂存 AI 理由用于 execute_buy
                
                # 如果原计划有限价，AI 建议的价格不能超过原限价 (严格遵守限价制度)
                if plan.buy_price_limit > 0 and ai_suggested_price > plan.buy_price_limit:
                    logger.warning(f"AI suggested price {ai_suggested_price} exceeds plan limit {plan.buy_price_limit}. Capping to plan limit.")
                    ai_suggested_price = plan.buy_price_limit
                
                plan.limit_price = ai_suggested_price or plan.buy_price_limit
                if (order_type or "").upper() == "MARKET":
                    max_slip_price = float(plan.buy_price_limit or 0.0)
                    if max_slip_price > 0:
                        plan.limit_price = min(float(max_slip_price), max(float(plan.limit_price or 0.0), float(current_price) * 1.002))
                    else:
                        plan.limit_price = max(float(plan.limit_price or 0.0), float(current_price) * 1.002)
                pre_close = float((quote or {}).get("pre_close", 0) or 0.0)
                if pre_close > 0:
                    limit_up, _ = get_limit_prices(str(plan.ts_code), pre_close)
                    if limit_up and float(limit_up) > 0:
                        limit_up = float(limit_up)
                        if plan.buy_price_limit and float(plan.buy_price_limit) > limit_up:
                            plan.buy_price_limit = limit_up
                        if plan.limit_price and float(plan.limit_price) > limit_up:
                            plan.limit_price = limit_up
                        if ai_suggested_price and float(ai_suggested_price) > limit_up:
                            ai_suggested_price = limit_up
                plan.ai_reason = decision.get('reason', 'AI决策')
                plan.decision_price = current_price
                db.commit()
                decision_time = datetime.now().strftime("%H:%M:%S")
                if action == 'BUY':
                    display_price = plan.limit_price if order_type == 'LIMIT' else current_price
                    plan.review_content = f"[{decision_time} AI确认] {order_type} @ {display_price}. {decision.get('reason', '')}"
                    db.commit()
                
                if action == 'BUY':
                    async with trading_lock_manager.lock("trade:account"):
                        await self._freeze_funds(db, plan, strict=False)
                        db.commit()
                        db.refresh(plan)
                        if float(plan.frozen_amount or 0.0) <= 0:
                            await self.reconcile_account_cash()
                            await self._freeze_funds(db, plan, strict=False)
                            db.commit()
                            db.refresh(plan)

                # [优化] 减少延迟：若 AI 确认买入且已冻结资金，立即尝试执行
                # 无需 return 等待下一轮循环
                if action == 'BUY' and float(plan.frozen_amount or 0.0) > 0:
                    logger.info(f"AI approved BUY for {plan.ts_code}, executing immediately...")
                    if time(11, 30) <= now_time < time(13, 0):
                        decision_time = datetime.now().strftime("%H:%M:%S")
                        plan.review_content = f"[{decision_time} AI挂单] 午休确认买入，13:00开盘自动成交"
                        db.commit()
                        return
                    
                    # [安全检查] 再次获取最新价格，防止 quote 过期
                    current_price_check = current_price
                    try:
                        latest_quote = await data_provider.get_realtime_quote(plan.ts_code)
                        if latest_quote:
                            q_price = float(latest_quote.get('price') or 0.0)
                            if q_price > 0:
                                current_price_check = q_price
                                # 更新计划中的参考价
                                plan.decision_price = q_price
                    except Exception as e:
                        logger.warning(f"Failed to refresh quote for immediate execution: {e}")

                    # 尝试执行 (复用上文的 AI挂单执行逻辑)
                    order_type = (plan.order_type or "LIMIT").upper()
                    exec_price = plan.limit_price or current_price_check
                    
                    # 再次检查是否满足执行条件
                    should_execute = False
                    if order_type == "MARKET":
                        should_execute = True
                    elif order_type == "LIMIT":
                        limit_price = float(plan.limit_price or 0.0)
                        if limit_price > 0 and current_price_check <= limit_price:
                            should_execute = True
                    
                    if should_execute:
                        try:
                            plan.market_snapshot_json = json.dumps(market_snapshot, ensure_ascii=False)
                        except Exception:
                            pass
                        success = await self.execute_buy(db, plan, exec_price)
                        if success:
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            plan.review_content = f"[{decision_time} AI成交(直通)] {order_type} @ {exec_price}. 理由: {plan.ai_reason or 'AI确认挂单'}"
                            db.commit()
                            self._update_analysis_history(plan.ts_code, current_price_check)
                    else:
                        logger.info(f"Immediate execution skipped: Price {current_price_check} > Limit {plan.limit_price}")

            from app.services.reward_punish_service import reward_punish_service
            if action == 'BUY' and reward_punish_service.is_trading_paused():
                decision_time = datetime.now().strftime("%H:%M:%S")
                plan.review_content = f"[{decision_time}] 风控暂停交易：奖惩系统处于暂停状态"
                db.commit()
                action = 'WAIT'

            # 8. 执行 AI 决策 (SELL 分支)
            if action == 'SELL':
                # ... (原有卖出逻辑保持不变，或者也可以类似优化)
                pass
                
            elif action == 'CANCEL':
                decision_time = datetime.now().strftime("%H:%M:%S")
                reason_txt = decision.get('reason', '无理由')
                await self.cancel_plan(plan.id, f"[{decision_time} AI取消] {reason_txt}")
                self._update_analysis_history(plan.ts_code, current_price)

            elif action == 'WAIT':
                decision_time = datetime.now().strftime("%H:%M:%S")
                ai_reason = decision.get('reason', 'AI保持观望')
                cancel_keywords = ["清除", "移出", "不再跟踪", "不再监控", "取消", "放弃跟踪", "剔除", "清理"]
                if any(k in str(ai_reason or "") for k in cancel_keywords):
                    await self.cancel_plan(plan.id, f"[{decision_time} AI取消] {ai_reason}")
                    self._update_analysis_history(plan.ts_code, current_price)
                    return
                async with trading_lock_manager.lock("trade:account"):
                    try:
                        db.refresh(plan)
                    except Exception:
                        pass
                        
                    if plan.frozen_amount and plan.frozen_amount > 0:
                        await self._unfreeze_funds(db, plan)
                        plan.review_content = f"[{decision_time} AI观望解冻] {order_type} @ {plan.limit_price if order_type=='LIMIT' else '现价'}. {ai_reason}"
                    else:
                        plan.review_content = f"[{decision_time} AI观望] {order_type} @ {plan.limit_price if order_type=='LIMIT' else '现价'}. {ai_reason}"
                    
                    # [Optimization] AI 决定观望，将计划转入慢速监控通道 (TRACKING)
                    # 避免每分钟重复高频请求 AI，交由 review_service 每 15 分钟复核一次
                    if (plan.track_status or "").upper() != "TRACKING":
                        plan.track_status = "TRACKING"
                        logger.info(f"Plan {plan.ts_code} moved to TRACKING (Slow Loop) due to WAIT decision.")
                        
                    db.commit()
                self._update_analysis_history(plan.ts_code, current_price)

        except Exception as e:
            logger.error(f"Error processing single plan {plan_id}: {e}", exc_info=True)
        finally:
            # [并发加固] 释放内存锁
            if plan_id in self._processing_plan_ids:
                self._processing_plan_ids.remove(plan_id)
            db.close()

    async def check_late_session_opportunity(self):
        """
        尾盘选股逻辑 (14:45): 
        将【今日选股策略筛选出的标的】、【当前监控列表中未成交的标的】与【当前持仓】合并，
        统一提交给 AI 进行终极决策 (AI是首席交易官，拥有最高决策权)。
        """
        from app.services.stock_selector import stock_selector
        
        logger.info("Starting Late Session Selection (Full Portfolio Decision Mode)...")
        
        db = SessionLocal()
        try:
            # [新增] 检查今日成交情况 (确保每日至少一交易)
            from app.models.stock_models import OutcomeEvent
            executed_count = db.query(OutcomeEvent).filter(
                OutcomeEvent.event_date == date.today()
            ).count()
            
            force_trade = False
            if executed_count == 0:
                logger.info("Today has no trades yet. Enabling FORCE TRADE mode for late session.")
                force_trade = True

            # --- 1. 准备候选买入池 (Candidates) ---
            candidate_pool = []
            seen_codes = set()

            # 1.1 获取选股策略候选 (Top 3)
            # [修改] 集成“10日内唯一倍量柱”策略 (vol_doubling) 到尾盘选股
            # 移除旧的 "default" (龙头+趋势) 策略，改用新策略
            try:
                # 1. 倍量柱策略 (vol_doubling)
                vol_doubling_candidates = await stock_selector.select_stocks(strategy="vol_doubling", top_n=3)
                for c in vol_doubling_candidates:
                    if c['ts_code'] not in seen_codes:
                        candidate_pool.append({
                            "ts_code": c['ts_code'],
                            "name": c['name'], 
                            "reason": f"[倍量柱选股] {c.get('analysis', '')[:50]}...",
                            "source": "倍量柱策略",
                            "score": c.get('score', 0)
                        })
                        seen_codes.add(c['ts_code'])
                        
                # 2. 回踩策略 (pullback) - 保留作为补充
                pullback_candidates = await stock_selector.select_stocks(strategy="pullback", top_n=2)
                for c in pullback_candidates:
                    if c['ts_code'] not in seen_codes:
                        candidate_pool.append({
                            "ts_code": c['ts_code'],
                            "name": c['name'],
                            "reason": f"[回踩选股] {c.get('analysis', '')[:50]}...",
                            "source": "回踩策略",
                            "score": c.get('score', 0)
                        })
                        seen_codes.add(c['ts_code'])
                        
            except Exception as e:
                logger.error(f"Error getting selector candidates: {e}")

            # 1.2 获取监控列表 (今日未执行的买入计划)
            try:
                plans = await asyncio.to_thread(lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == date.today(),
                    TradingPlan.executed == False
                ).all())
                
                for plan_item in plans:
                    # 排除持仓管理类的计划
                    if plan_item.strategy_name in ['持仓卖出', '持仓减仓', '持仓做T', '持仓持有', '持仓管理']:
                        continue
                        
                    # 强势盘后/尾盘/选股监控类计划，始终纳入盘中监控池
                    strong_keep = False
                    name = str(plan_item.strategy_name or "")
                    if name.startswith("选股监控-") or name in ["收盘精选", "尾盘突击", "梯队联动", "成交额筛选", "首板挖掘", "低吸反包"]:
                        strong_keep = True
                    
                    if plan_item.ts_code not in seen_codes:
                        stock_info = await asyncio.to_thread(lambda: db.query(Stock).filter(Stock.ts_code == plan_item.ts_code).first())
                        name = str(stock_info.name) if (stock_info and stock_info.name) else str(plan_item.ts_code or "")
                        
                        reason_text = str(plan_item.reason or "")
                        strategy_text = str(plan_item.strategy_name or "")
                        candidate_pool.append({
                            "ts_code": plan_item.ts_code,
                            "name": name,
                            "reason": f"[监控计划] {strategy_text}{' (强跟踪)' if strong_keep else ''}: {reason_text[:50]}...",
                            "source": "监控列表",
                            "score": 0 
                        })
                        seen_codes.add(plan_item.ts_code)
            except Exception as e:
                logger.error(f"Error getting monitoring plans: {e}")

            # 1.3 补充实时数据和技术摘要
            
            # [新增] 如果强制交易且候选池为空，尝试从今日市场热点中获取
            if force_trade and not candidate_pool:
                logger.info("Force trade active but no candidates. Fetching hot stocks as backup...")
                try:
                    from app.models.stock_models import MarketSentiment
                    sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == date.today()).first()
                    if sentiment and sentiment.turnover_top_json:
                        import json
                        top_stocks = json.loads(sentiment.turnover_top_json)
                        # top_stocks 应该是一个列表，取前3个
                        for s in top_stocks[:3]:
                            ts_code = s.get('ts_code') or s.get('code')
                            if ts_code and ts_code not in seen_codes:
                                candidate_pool.append({
                                    "ts_code": ts_code,
                                    "name": s.get('name', ts_code),
                                    "reason": "[强制交易] 市场成交额前排热股，作为保底标的",
                                    "source": "市场热点",
                                    "score": 60
                                })
                                seen_codes.add(ts_code)
                except Exception as e:
                    logger.error(f"Error fetching backup candidates: {e}")
            
            final_candidates = []
            if candidate_pool:
                cand_codes = [c['ts_code'] for c in candidate_pool]
                quotes = await data_provider.get_realtime_quotes(cand_codes, cache_scope="trading")
                
                # 优化：并行获取所有上下文
                from app.services.chat_service import chat_service
                context_tasks = [chat_service.get_ai_trading_context(c['ts_code'], cache_scope="trading") for c in candidate_pool]
                all_contexts = await asyncio.gather(*context_tasks)
                
                for i, c in enumerate(candidate_pool):
                    ts_code = c['ts_code']
                    quote = quotes.get(ts_code)
                    if not quote: continue
                    
                    current_price = float(quote['price'])
                    if current_price <= 0: continue
                    
                    c['price'] = current_price
                    c['raw_trading_context'] = all_contexts[i]
                    final_candidates.append(c)

            # --- 2. 准备持仓池 (Positions) ---
            positions_data = []
            positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
            if positions:
                pos_codes = [p.ts_code for p in positions]
                pos_quotes = await data_provider.get_realtime_quotes(pos_codes, cache_scope="trading")
                
                # 优化：并行获取所有持仓的上下文
                from app.services.chat_service import chat_service
                pos_context_tasks = [chat_service.get_ai_trading_context(p.ts_code, cache_scope="trading") for p in positions]
                all_pos_contexts = await asyncio.gather(*pos_context_tasks)
                
                for i, p in enumerate(positions):
                    q = pos_quotes.get(p.ts_code)
                    curr_p = float(q['price']) if q else p.current_price
                    if curr_p <= 0: continue
                    
                    mv = p.vol * curr_p
                    cost = p.vol * p.avg_price
                    pnl = mv - cost
                    pnl_pct = (pnl / cost * 100) if cost > 0 else 0
                    
                    raw_context = all_pos_contexts[i]
                    
                    positions_data.append({
                        "ts_code": p.ts_code,
                        "name": p.name,
                        "vol": p.vol,
                        "available_vol": p.available_vol,
                        "can_sell": p.available_vol > 0,
                        "current_price": curr_p,
                        "avg_price": p.avg_price,
                        "pnl_pct": pnl_pct,
                        "raw_trading_context": raw_context # 注入原始多周期数据
                    })

            if not final_candidates and not positions_data:
                logger.info("Nothing to do (No candidates and No positions).")
                return

            # --- 3. 获取市场状态与账户信息 ---
            market_snapshot = await data_provider.get_market_snapshot()
            market_status = await ai_service.analyze_market_snapshot(market_snapshot)
            
            account = await self._get_or_create_account(db)
            account_info = {
                "total_assets": account.total_assets if account else 0,
                "available_cash": account.available_cash if account else 0,
                "market_value": account.market_value if account else 0,
                "total_pnl_pct": account.total_pnl_pct if account else 0
            }
            
            # --- 4. 提交给 AI 进行终极决策 ---
            logger.info(f"AI Decision Context: {len(final_candidates)} candidates, {len(positions_data)} positions. Force Trade: {force_trade}")
            decision = await ai_service.decide_late_session_strategy(
                final_candidates, 
                positions_data, 
                market_status, 
                account_info=account_info,
                force_trade=force_trade
            )
            
            logger.info(f"AI Final Decision Result: {json.dumps(decision, ensure_ascii=False)}")
            
            # --- 5. 执行决策 ---
            
            # 5.1 执行持仓决策 (Priority 1)
            pos_decisions = decision.get('position_decisions', [])
            for pd in pos_decisions:
                p_code = pd.get('ts_code')
                p_action = pd.get('action')
                p_reason = pd.get('reason', 'AI决策')
                
                if p_action in ['SELL', 'REDUCE']:
                    # 查找对应的持仓信息以获取价格
                    pos_info = next((x for x in positions_data if x['ts_code'] == p_code), None)
                    if not pos_info: continue
                    if not pos_info['can_sell']:
                        logger.warning(f"AI wants to SELL {p_code} but no available volume (T+0).")
                        continue
                        
                    logger.info(f"Executing AI Position Decision: {p_action} {p_code} ({p_reason})")
                    
                    p_order_type = str(pd.get('order_type') or 'MARKET')
                    p_price_raw = pd.get('price')
                    p_price_val = float(p_price_raw) if isinstance(p_price_raw, (int, float, str)) else 0.0
                    current_price_raw = pos_info.get("current_price")
                    current_price_val = float(current_price_raw) if isinstance(current_price_raw, (int, float, str)) else 0.0
                    p_price = p_price_val or current_price_val

                    # 记录计划 (为了留痕)
                    sell_plan = await self.create_plan( 
                        ts_code=p_code,
                        strategy_name="持仓卖出" if p_action == 'SELL' else "持仓减仓",
                        buy_price=0,
                        stop_loss=0,
                        take_profit=0,
                        reason=f"[AI尾盘决策] {p_reason}",
                        source="system",
                        order_type=p_order_type,
                        limit_price=p_price,
                        ai_decision=p_action
                    )
                    
                    # 立即执行卖出
                    available_vol_raw = pos_info.get("available_vol")
                    available_vol = int(available_vol_raw) if isinstance(available_vol_raw, (int, float, str)) else 0
                    vol_to_sell = available_vol if p_action == 'SELL' else self._calc_reduce_volume(available_vol)
                    if int(vol_to_sell) > 0:
                        # 对于卖出，误差控制：如果现价低于 AI 建议价 0.5% 以上，且是限价单，则不执行或按限价挂单
                        # 这里统一交给 execute_sell 处理逻辑
                        await self.execute_sell(
                            db,
                            p_code,
                            float(p_price),
                            volume=int(vol_to_sell),
                            reason=f"[AI尾盘] {p_reason}",
                            order_type=p_order_type,
                            plan_id=sell_plan.id if sell_plan else None
                        )

            # 5.2 执行新开仓决策 (Priority 2)
            buy_dec = decision.get('new_buy_decision', {})
            b_action = buy_dec.get('action')
            b_target = buy_dec.get('target_code')
            b_price = float(buy_dec.get('price', 0))
            b_order_type = buy_dec.get('order_type', 'MARKET')
            b_reason = buy_dec.get('reason', 'AI无理由')
            
            if b_action == 'BUY' and b_target and b_price > 0:
                # 再次确认资金是否充足 (虽然 AI 可能不知道确切金额，但 execute_buy 会检查)
                # 找到对应的候选信息
                target_info = next((c for c in final_candidates if c['ts_code'] == b_target), None)
                if not target_info:
                    logger.error(f"AI selected {b_target} but not in candidate pool!")
                else:
                    logger.info(f"Executing AI Buy Decision: Buy {b_target} ({target_info['name']}) at {b_price} ({b_order_type})")
                    
                    plan: Optional[TradingPlan] = None
                    try:
                        plan = await self.create_plan( 
                            ts_code=b_target,
                            strategy_name="尾盘突击",
                            buy_price=b_price,
                            stop_loss=b_price * 0.95,
                            take_profit=b_price * 1.10,
                            position_pct=0.2,
                            reason=f"[AI尾盘决策] {b_reason}",
                            source="system",
                            order_type=b_order_type,
                            limit_price=b_price
                        )
                    except ValueError as e:
                        logger.warning(f"Skip AI buy plan for {b_target}: {e}")
                        plan = None
                    
                    # 立即执行
                    # 获取最新价格进行成交
                    if plan:
                        q = await data_provider.get_realtime_quote(b_target, cache_scope="trading")
                        curr_p = float(q['price']) if q else b_price
                        await self.execute_buy(db, plan, b_price)
            else:
                logger.info("AI decided to WAIT for new positions.")

        except Exception as e:
            logger.error(f"Error in late session check: {e}", exc_info=True)
        finally:
            db.close()


    async def _process_single_position(self, pos_id: int, quote: dict, market_status: str, account_info: dict, HARD_STOP_LOSS_PCT: float):
        """处理单个持仓的监控与卖出逻辑"""
        db = SessionLocal()
        try:
            pos = await asyncio.to_thread(lambda: db.query(Position).get(pos_id))
            if not pos or pos.vol <= 0 or pos.available_vol <= 0:
                return

            current_price = float(quote['price'])
            if current_price <= 0: return
            
            # 更新持仓市值信息
            pos.current_price = current_price
            new_mv = pos.vol * current_price
            cost = pos.vol * pos.avg_price
            pnl = new_mv - cost
            pnl_pct = (pnl / cost * 100) if cost > 0 else 0
            
            # --- 动态止盈数据更新 (Systemic Optimization) ---
            # 更新持仓期间最高价与最高盈亏比
            if current_price > (pos.high_price or 0.0):
                pos.high_price = current_price
            
            # 计算基于当前价格的理论最高盈亏比 (假设成本不变)
            current_high_pnl_pct = ((pos.high_price - pos.avg_price) / pos.avg_price * 100) if pos.avg_price > 0 else 0
            if current_high_pnl_pct > (pos.high_pnl_pct or 0.0):
                pos.high_pnl_pct = current_high_pnl_pct
            # -----------------------------------------------

            # 计算持仓天数
            hold_days = 1
            first_trade = await asyncio.to_thread(lambda: db.query(TradeRecord).filter(
                TradeRecord.ts_code == pos.ts_code,
                TradeRecord.trade_type == 'BUY'
            ).order_by(TradeRecord.trade_time.asc()).first())
            
            if first_trade:
                delta = date.today() - first_trade.trade_time.date()
                hold_days = delta.days
            if hold_days <= 0:
                return
            
            # 判定是否需要进行 AI 深度评估
            should_call_ai = False
            trigger_reason = ""
            
            plan = await asyncio.to_thread(lambda: db.query(TradingPlan).filter(
                TradingPlan.ts_code == pos.ts_code,
                TradingPlan.executed == True
            ).order_by(TradingPlan.created_at.desc()).first())

            if plan:
                # [User Request] Remove system-imposed limits. Let AI decide based on market data.
                pass
                # stop_loss = plan.stop_loss_price or (plan.entry_price * 0.92 if plan.entry_price else 0)
                # take_profit = plan.take_profit_price or (plan.entry_price * 1.15 if plan.entry_price else 0)
                # if current_price <= stop_loss:
                #     should_call_ai = True
                #     trigger_reason = f"触及计划止损线 ({stop_loss})"
                # elif current_price >= take_profit:
                #     should_call_ai = True
                #     trigger_reason = f"触及计划止盈线 ({take_profit})"
            
            # if pnl_pct <= HARD_STOP_LOSS_PCT:
            #     should_call_ai = True
            #     trigger_reason = f"触发全局硬止损 ({HARD_STOP_LOSS_PCT}%)"
            
            daily_pct_chg = quote.get('pct_chg', 0)
            # if daily_pct_chg <= -3.0:
            #     should_call_ai = True
            #     trigger_reason = f"当日跌幅过大 ({daily_pct_chg}%)"

            # --- 动态止盈与保本机制 (Systemic Optimization - Tightened for Profit Protection) ---
            # [User Request] Remove system-imposed limits. Let AI decide based on market data.
            # 1. 保本触发
            # if (pos.high_pnl_pct or 0) >= 2.0 and pnl_pct <= 0.3:
            #     should_call_ai = True
            #     trigger_reason = f"触发保本保护机制 (最高盈利 {pos.high_pnl_pct:.2f}%)"
            
            # 2. 阶梯止盈 (Trailing Stop)
            # if (pos.high_pnl_pct or 0) >= 15.0:
            #     if pnl_pct <= (pos.high_pnl_pct - 4.0):
            #         should_call_ai = True
            #         trigger_reason = f"触发阶梯止盈 [15%档] (回撤自最高点 {pos.high_pnl_pct:.2f}%)"
            # elif (pos.high_pnl_pct or 0) >= 8.0:
            #     if pnl_pct <= (pos.high_pnl_pct - 2.0):
            #         should_call_ai = True
            #         trigger_reason = f"触发阶梯止盈 [8%档] (回撤自最高点 {pos.high_pnl_pct:.2f}%)"
            # elif (pos.high_pnl_pct or 0) >= 4.0:
            #     if pnl_pct <= (pos.high_pnl_pct - 1.2):
            #         should_call_ai = True
            #         trigger_reason = f"触发阶梯止盈 [4%档] (回撤自最高点 {pos.high_pnl_pct:.2f}%)"
            
            # 3. 极速回撤保护 (Flash Drawdown)
            # if daily_high > 0 and current_price > 0:
            #     daily_drawdown = (daily_high - current_price) / daily_high * 100
            #     if daily_drawdown >= 3.0 and pnl_pct > 0:
            #         should_call_ai = True
            #         trigger_reason = f"触发当日极速回撤保护 (日内回撤 {daily_drawdown:.2f}%)"
            
            # 3. 30分钟技术走弱预警 (Real-time Detection)
            # 检查包括顶背离、量价异常、破位等技术走弱信号
            if not should_call_ai and pnl_pct > -2.0: 
                try:
                    # 获取最近 30 分钟 K 线
                    kline_30m = await data_provider.get_kline(pos.ts_code, freq='30', limit=40, local_only=False)
                    if kline_30m:
                        df_30m = technical_indicators.calculate(kline_30m)
                        
                        # 检查是否有顶背离
                        has_divergence = technical_indicators.detect_top_divergence(df_30m)
                        # 检查是否破位 (例如跌破 20 周期均线)
                        is_breaking_down = False
                        if len(df_30m) >= 2:
                            last_row = df_30m.iloc[-1]
                            if last_row['close'] < last_row['ma20'] and df_30m.iloc[-2]['close'] >= df_30m.iloc[-2]['ma20']:
                                is_breaking_down = True
                        
                        if has_divergence:
                            should_call_ai = True
                            trigger_reason = "检测到 30 分钟级别顶背离风险"
                        elif is_breaking_down:
                            should_call_ai = True
                            trigger_reason = "检测到 30 分钟级别均线破位 (跌破 MA20)"
                except Exception as e:
                    logger.warning(f"Error checking 30min technical weakness for {pos.ts_code}: {e}")
            # -----------------------------------------------

            # [User Request] Increase periodic check frequency to let AI decide more often
            if datetime.now().minute % 5 == 0: # Check every 5 minutes (0, 5, 10, ..., 55)
                should_call_ai = True
                trigger_reason = "定期状态巡检"

            if should_call_ai:
                if not self._should_trigger_deep_analysis(pos.ts_code, current_price, cooling_minutes=15):
                    return
                
                logger.info(f"🚀 AI Evaluation triggered for {pos.ts_code}. Reason: {trigger_reason}")
                ctx = await self._get_unified_trading_context(db, pos.ts_code)
                
                decision = await ai_service.analyze_selling_opportunity(
                    symbol=pos.ts_code,
                    current_price=current_price,
                    avg_price=pos.avg_price,
                    pnl_pct=pnl_pct,
                    hold_days=hold_days,
                    market_status=market_status,
                    account_info=account_info,
                    handicap_info=ctx['handicap_info'],
                    search_info=ctx['search_info'],
                    raw_trading_context=ctx['raw_context'],
                    vol=pos.vol,
                    available_vol=pos.available_vol,
                    trigger_reason=trigger_reason,
                    high_price=pos.high_price,
                    high_pnl_pct=pos.high_pnl_pct
                )
                
                if not decision: return
                
                action = (decision.get('action') or "").upper()
                reason = decision.get('reason', 'AI决策')
                
                if action in ['SELL', 'REDUCE']:
                    self._update_analysis_history(pos.ts_code, current_price)
                    if action == 'SELL':
                        vol_to_sell = pos.available_vol
                    else:
                        vol_to_sell = self._calc_reduce_volume(pos.available_vol)
                    
                    if vol_to_sell >= 100:
                        order_type = decision.get('order_type', 'MARKET')
                        target_price = decision.get('price', current_price) or current_price
                        sell_plan = None
                        try:
                            sell_plan = await self.create_plan(
                                ts_code=pos.ts_code,
                                strategy_name="持仓卖出" if action == 'SELL' else "持仓减仓",
                                buy_price=0,
                                stop_loss=0,
                                take_profit=0,
                                position_pct=0,
                                reason=f"[AI盘中] {reason}",
                                plan_date=date.today(),
                                source="system",
                                order_type=order_type,
                                limit_price=target_price,
                                ai_decision=action
                            )
                        except Exception as e:
                            logger.error(f"Error creating sell plan for {pos.ts_code}: {e}")
                        
                        success = await self.execute_sell(
                            db, str(pos.ts_code), target_price, 
                            volume=int(vol_to_sell or 0), 
                            reason=f"[{action}] {reason}",
                            order_type=order_type,
                            plan_id=int(sell_plan.id) if sell_plan and sell_plan.id is not None else (int(plan.id) if plan and plan.id is not None else None)
                        )
                        if success:
                            target_plan = sell_plan or plan
                            if target_plan:
                                target_plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')} AI成交] {action} @ {target_price}. 理由: {reason}"
                                await asyncio.to_thread(db.commit)
                elif action in ['T', 'T0', 'T-0', 'TRADE_T']:
                    self._update_analysis_history(pos.ts_code, current_price)
                    raw_vol = decision.get('volume') or decision.get('t_vol') or decision.get('trade_vol') or decision.get('sell_vol')
                    t_vol = int(raw_vol) if raw_vol else 0
                    if t_vol <= 0:
                        t_vol = self._calc_reduce_volume(pos.available_vol)
                    
                    # [T0增强] 允许加仓逻辑 (如果 available_vol 很小，可能是刚买入的底仓，允许按策略加仓)
                    # 只有当策略明确为 "四信号共振" 且 AI 建议加仓时，才允许突破 available_vol 限制
                    is_add_position = False
                    if str(decision.get('strategy_name', '')).startswith('选股监控-四信号') or str(pos.strategy_name or '').startswith('选股监控-四信号'):
                         if action in ['T', 'T0'] and decision.get('buy_price'):
                             # 四信号策略 T0 加仓模式：允许买入
                             is_add_position = True
                    
                    if not is_add_position:
                        t_vol = min(int(pos.available_vol or 0), int(t_vol))
                    
                    t_vol = self._normalize_sell_volume(pos.ts_code, t_vol)
                    min_buy_vol = self._get_min_buy_volume(pos.ts_code)
                    if t_vol < min_buy_vol:
                        return

                    sell_order_type = str(decision.get('sell_order_type') or decision.get('order_type') or "MARKET").upper()
                    buy_order_type = str(decision.get('buy_order_type') or decision.get('order_type') or "MARKET").upper()
                    sell_price = float(decision.get('sell_price') or decision.get('price') or current_price or 0.0)
                    buy_price = float(decision.get('buy_price') or current_price or 0.0)

                    # [T0增强] 如果是回调衰竭加仓模式
                    if is_add_position and buy_price > 0 and buy_price < current_price * 1.01: # 价格合理
                         logger.info(f"执行 T0 加仓逻辑 (四信号策略): {pos.ts_code}, 目标价 {buy_price}")
                         # 创建买入计划 (补仓)
                         t0_buy_plan = await self.create_plan(
                            ts_code=pos.ts_code,
                            strategy_name="持仓做T-补仓",
                            buy_price=buy_price,
                            stop_loss=buy_price * 0.98, # 补仓部分止损极窄 (-2%)
                            take_profit=buy_price * 1.05,
                            position_pct=0.1, # 补仓 10%
                            reason=f"[T0补仓] 回调衰竭，降低成本。{reason}",
                            plan_date=date.today(),
                            source="system",
                            order_type=buy_order_type,
                            limit_price=buy_price,
                            ai_decision=f"BUY_T0_{t_vol}"
                         )
                         if not t0_buy_plan:
                             return
                         
                         # 同时创建对应的防守卖出计划 (如果补仓失败，卖出底仓)
                         # 这个计划是条件单，当价格跌破补仓价 2% 时触发卖出昨日持仓
                         if pos.available_vol >= t_vol:
                             sell_trigger_price = buy_price * 0.98
                             t0_sell_plan = await self.create_plan(
                                ts_code=pos.ts_code,
                                strategy_name="持仓做T-止损",
                                buy_price=0, # 卖出单
                                stop_loss=0,
                                take_profit=0,
                                position_pct=0,
                                reason=f"[T0风控] 补仓失败止损，卖出底仓。触发价: {sell_trigger_price}",
                                plan_date=date.today(),
                                source="system",
                                order_type=sell_order_type,
                                limit_price=sell_trigger_price, # 触发价
                                is_sell=True,
                                ai_decision=f"SELL_T0_{t_vol}" # 标记关联
                             )
                             # 标记该卖出计划为条件单 (需配合 execute_plan 修改)
                             if t0_sell_plan:
                                 t0_sell_plan.limit_price = sell_trigger_price
                                 t0_sell_plan.review_content = f"条件单: 价格跌破 {sell_trigger_price} 时卖出 {t_vol} 股"
                                 await asyncio.to_thread(db.commit)
                             
                         # 立即执行买入
                         buy_success = await self.execute_buy(db, t0_buy_plan, buy_price, volume=int(t_vol))
                         if buy_success:
                             logger.info(f"T0 补仓成功: {pos.ts_code} {t_vol}股")
                         return

                    t_sell_plan: Optional[TradingPlan] = None
                    try:
                        t_sell_plan = await self.create_plan(
                            ts_code=pos.ts_code,
                            strategy_name="持仓做T",
                            buy_price=0,
                            stop_loss=0,
                            take_profit=0,
                            position_pct=0,
                            reason=f"[AI盘中做T] {reason}",
                            plan_date=date.today(),
                            source="system",
                            order_type=sell_order_type,
                            limit_price=sell_price,
                            ai_decision="SELL",
                            is_sell=True
                        )
                    except Exception as e:
                        logger.error(f"Error creating T sell plan for {pos.ts_code}: {e}")

                    sell_success = await self.execute_sell(
                        db, str(pos.ts_code), sell_price,
                        volume=int(t_vol or 0),
                        reason=f"[T-SELL] {reason}",
                        order_type=sell_order_type,
                        plan_id=int(t_sell_plan.id) if t_sell_plan and t_sell_plan.id is not None else (int(plan.id) if plan and plan.id is not None else None)
                    )

                    if sell_success and t_sell_plan:
                        t_sell_plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')} AI成交] T卖出 @ {sell_price}. 理由: {reason}"
                        await asyncio.to_thread(db.commit)

                    if sell_success:
                        t_buy_plan: Optional[TradingPlan] = None
                        try:
                            t_buy_plan = await self.create_plan(
                                ts_code=str(pos.ts_code),
                                strategy_name="持仓做T",
                                buy_price=buy_price,
                                stop_loss=0,
                                take_profit=0,
                                position_pct=0,
                                reason=f"[AI盘中做T回补] {reason}",
                                plan_date=date.today(),
                                source="system",
                                order_type=buy_order_type,
                                limit_price=buy_price,
                                ai_decision="BUY"
                            )
                        except Exception as e:
                            logger.error(f"Error creating T buy plan for {pos.ts_code}: {e}")

                        if t_buy_plan:
                            if buy_order_type == "LIMIT":
                                account = await self._get_or_create_account(db)
                                total_assets = float(account.total_assets or 0.0)
                                if total_assets > 0 and buy_price > 0:
                                    t_buy_plan.position_pct = float((t_vol * buy_price) / total_assets)
                                async with trading_lock_manager.lock("trade:account"):
                                    await self._freeze_funds(db, t_buy_plan, strict=False)
                                    await asyncio.to_thread(db.commit)

                            buy_success = await self.execute_buy(db, t_buy_plan, buy_price, volume=int(t_vol))
                            decision_time = datetime.now().strftime("%H:%M:%S")
                            if buy_success:
                                t_buy_plan.review_content = f"[{decision_time} AI成交] T回补 @ {buy_price}. 理由: {reason}"
                                await asyncio.to_thread(db.commit)
                            else:
                                t_buy_plan.review_content = f"[{decision_time} 等待回补] T回补挂单 @ {buy_price}. 理由: {reason}"
                                await asyncio.to_thread(db.commit)
                elif action in ['WAIT', 'HOLD']:
                    self._update_analysis_history(str(pos.ts_code), current_price)
                    target_plan = plan
                    if not target_plan or self._infer_plan_action(target_plan) == "SELL" or target_plan.executed:
                        try:
                            target_plan = await self.create_plan(
                                ts_code=str(pos.ts_code),
                                strategy_name="持仓持有",
                                buy_price=0,
                                stop_loss=0,
                                take_profit=0,
                                position_pct=0,
                                reason=f"[AI盘中] {reason}",
                                plan_date=date.today(),
                                source="system",
                                order_type="MARKET",
                                limit_price=0,
                                ai_decision="HOLD"
                            )
                        except Exception as e:
                            logger.error(f"Error creating hold plan for {pos.ts_code}: {e}")
                    if target_plan:
                        target_plan.review_content = f"[{datetime.now().strftime('%H:%M:%S')} AI观望] {reason}"
                        await asyncio.to_thread(db.commit)

        except Exception as e:
            await asyncio.to_thread(db.rollback)
            logger.error(f"Error processing single position {pos_id}: {e}")
        finally:
            db.close()

    async def check_positions_and_sell(self):
        """
        盘中持仓监控：AI 自动跟踪卖点 (止盈/止损/趋势坏)
        """
        if not settings.ENABLE_AUTO_TRADE:
            return

        db = SessionLocal()
        try:
            # 1. 获取所有持仓
            positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
            if not positions:
                return
                
            ts_codes = [p.ts_code for p in positions]
            
            # 2. 获取实时行情
            quotes = await data_provider.get_realtime_quotes(ts_codes)
            
            # 3. 获取市场快照 (用于辅助决策) - 必须 await
            market_snapshot = await data_provider.get_market_snapshot()
            market_status = await ai_service.analyze_market_snapshot(market_snapshot)
            
            # 获取账户信息，以便在 AI 决策时提供上下文
            account = await self._get_or_create_account(db)
            account_info = {
                "total_assets": account.total_assets if account else 0,
                "available_cash": account.available_cash if account else 0,
                "market_value": account.market_value if account else 0,
                "total_pnl_pct": account.total_pnl_pct if account else 0
            }
            
            # 计算总仓位占比，用于动态调整风控阈值
            position_ratio = account_info['market_value'] / account_info['total_assets'] if account_info['total_assets'] > 0 else 0
            
            # 动态调整全局硬止损阈值：仓位越高，止损越严
            # 默认 -10%，若仓位 > 80%，收紧至 -8%；若仓位 > 90%，收紧至 -6%
            HARD_STOP_LOSS_PCT = -10.0
            if position_ratio > 0.9:
                HARD_STOP_LOSS_PCT = -6.0
                logger.warning(f"Extreme high position ({position_ratio:.2f}). Tightening Hard Stop Loss to {HARD_STOP_LOSS_PCT}%")
            elif position_ratio > 0.8:
                HARD_STOP_LOSS_PCT = -8.0
                logger.info(f"High position ({position_ratio:.2f}). Tightening Hard Stop Loss to {HARD_STOP_LOSS_PCT}%")

            # --- AI 动态调仓 (Portfolio Level Check) ---
            # 降低频率：每 15 分钟分析一次整体仓位风险，而不是每分钟
            if datetime.now().minute in [0, 15, 30, 45]:
                # 优化：并行获取所有上下文
                from app.services.chat_service import chat_service
                
                # 过滤出有效的持仓股票
                valid_positions = []
                for p in positions:
                    q = quotes.get(p.ts_code)
                    curr_p = float(q['price']) if q else p.current_price
                    if curr_p > 0:
                        valid_positions.append((p, curr_p))
                
                if valid_positions:
                    queue: asyncio.Queue[tuple[int, str] | None] = asyncio.Queue()
                    concurrency = max(1, min(int(self._monitor_context_concurrency or 5), 10))
                    all_contexts = [""] * len(valid_positions)
                    for i, (p, _) in enumerate(valid_positions):
                        await queue.put((i, p.ts_code))
                    for _ in range(concurrency):
                        await queue.put(None)

                    async def _ctx_worker():
                        while True:
                            item = await queue.get()
                            try:
                                if item is None:
                                    return
                                idx, code = item
                                try:
                                    all_contexts[idx] = await asyncio.wait_for(chat_service.get_ai_trading_context(code, cache_scope="trading"), timeout=25.0)
                                except Exception:
                                    all_contexts[idx] = ""
                            finally:
                                queue.task_done()

                    workers = [asyncio.create_task(_ctx_worker()) for _ in range(concurrency)]
                    await queue.join()
                    await asyncio.gather(*workers, return_exceptions=True)
                    
                    positions_data = []
                    for i, (p, curr_p) in enumerate(valid_positions):
                        raw_context = all_contexts[i]
                        positions_data.append({
                            "ts_code": p.ts_code,
                            "name": p.name,
                            "vol": p.vol,
                            "current_price": curr_p,
                            "pnl_pct": ((p.vol * curr_p - p.vol * p.avg_price) / (p.vol * p.avg_price) * 100) if p.avg_price > 0 else 0,
                            "raw_trading_context": raw_context
                        })

                    if positions_data:
                        try:
                            # 确保有市场快照 - 必须 await
                            if market_snapshot is None:
                                market_snapshot = await data_provider.get_market_snapshot()
                                market_status = await ai_service.analyze_market_snapshot(market_snapshot)
                                
                            adjustments = await ai_service.analyze_portfolio_adjustment(market_status, positions_data, account_info)
                            # ... (后续处理逻辑保持不变)
                        except Exception as e:
                            logger.error(f"Error in dynamic portfolio adjustment: {e}")
            
            # --- End Portfolio Check ---
            
            # --- Individual Position Monitoring ---
            monitor_queue: asyncio.Queue[tuple[int, dict] | None] = asyncio.Queue()
            total = 0
            concurrency = max(1, min(int(self._monitor_position_concurrency or 3), 10))
            for pos in positions:
                # 优化：如果是 T+0 当天买入的股票 (可用持仓为 0)，则无需执行盘中跟踪
                if pos.available_vol <= 0:
                    continue

                quote = quotes.get(pos.ts_code)
                if not quote:
                    continue

                await monitor_queue.put((pos.id, quote))
                total += 1

            if total > 0:
                for _ in range(concurrency):
                    await monitor_queue.put(None)

                async def _worker():
                    while True:
                        item = await monitor_queue.get()
                        try:
                            if item is None:
                                return
                            pos_id, q = item
                            try:
                                await asyncio.wait_for(
                                    self._process_single_position(pos_id, q, market_status, account_info, HARD_STOP_LOSS_PCT),
                                    timeout=70.0,
                                )
                            except Exception:
                                pass
                        finally:
                            monitor_queue.task_done()

                workers = [asyncio.create_task(_worker()) for _ in range(concurrency)]
                await monitor_queue.join()
                await asyncio.gather(*workers, return_exceptions=True)

        except Exception as e:
            logger.error(f"Error in position monitoring: {e}")
        finally:
            db.close()

    async def _freeze_funds(self, db: Session, plan: TradingPlan, strict: bool = False):
        """冻结资金"""
        if plan.executed:
            return
        if float(plan.position_pct or 0.0) <= 0:
            return
        if self._infer_plan_action(plan) != "BUY":
            return

        # 如果已经有冻结资金，不再重复冻结
        if plan.frozen_amount and plan.frozen_amount > 0:
            return

        account = await self._get_or_create_account(db)
        
        # 计算需要冻结的金额: 总资产 * 仓位比例
        # 为了保险起见，冻结金额包含预估手续费
        base_assets = float(account.total_assets or 0.0)
        if base_assets <= 0:
            base_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
        available_cash = float(account.available_cash or 0.0)
        if available_cash <= 0:
            derived_available = float(base_assets) - float(account.frozen_cash or 0.0) - float(account.market_value or 0.0)
            if derived_available > available_cash:
                available_cash = max(0.0, derived_available)
        raw_budget = float(base_assets) * float(plan.position_pct or 0.0)
        budget = max(0.0, min(raw_budget, available_cash))
        if budget <= 0:
            if strict:
                raise ValueError(f"资金不足：可用资金 {float(account.available_cash or 0.0):.2f}，无法冻结委托资金")
            return

        ref_price = float(plan.buy_price_limit or plan.limit_price or plan.decision_price or 0.0)
        if ref_price <= 0:
            quote = await data_provider.get_realtime_quote(str(plan.ts_code), cache_scope="trading")
            ref_price = float(quote.get("price", 0)) if quote else 0.0
        if ref_price <= 0:
            from app.models.stock_models import DailyBar
            latest_bar = await asyncio.to_thread(lambda: db.query(DailyBar).filter(DailyBar.ts_code == plan.ts_code).order_by(DailyBar.trade_date.desc()).first())
            if latest_bar and float(latest_bar.close or 0.0) > 0:
                ref_price = float(latest_bar.close or 0.0)

        if ref_price <= 0:
            if strict:
                raise ValueError("无法获取有效参考价，无法冻结委托资金")
            return

        if float(plan.buy_price_limit or 0.0) <= 0:
            plan.buy_price_limit = float(ref_price)
        if float(plan.limit_price or 0.0) <= 0:
            plan.limit_price = float(ref_price)

        step = self._get_buy_volume_step(str(plan.ts_code))
        if step <= 1:
            vol = int(budget / ref_price)
        else:
            vol = int(budget / ref_price / step) * step
        min_buy_vol = self._get_min_buy_volume(str(plan.ts_code))
        if vol < min_buy_vol:
            if strict:
                raise ValueError(f"资金不足：按参考价 {float(ref_price):.2f}，预算 {float(budget):.2f} 无法满足最小 {min_buy_vol} 股")
            return

        def _calc_total_cost(v: int) -> float:
            need_cash = v * ref_price
            fee = self._calc_buy_fee(need_cash) + self._calc_transfer_fee(str(plan.ts_code), need_cash)
            return need_cash + fee

        frozen_amount = _calc_total_cost(vol)
        while vol >= min_buy_vol and frozen_amount > budget:
            vol -= step
            if vol < min_buy_vol:
                if strict:
                    raise ValueError(f"资金不足：预算 {float(budget):.2f} 无法覆盖最小 {min_buy_vol} 股含费成本")
                return
            frozen_amount = _calc_total_cost(vol)

        account.available_cash -= frozen_amount
        account.frozen_cash += frozen_amount
        plan.frozen_amount = frozen_amount
        plan.frozen_vol = vol
        account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)

        logger.info(f"Frozen {frozen_amount:.2f} cash for plan {plan.id} ({plan.ts_code})")

    def _infer_plan_action(self, plan: TradingPlan) -> str:
        decision = (getattr(plan, "ai_decision", "") or "").strip().upper()
        if decision in ["CANCEL", "WAIT", "HOLD"]:
            return "HOLD"
        if decision in ["SELL", "REDUCE"]:
            return "SELL"
        if decision == "BUY":
            return "BUY"
        strategy_name = (plan.strategy_name or "").strip()
        if any(k in strategy_name for k in ["卖出", "减仓", "减持", "清仓", "止盈", "止损", "抛售"]):
            return "SELL"
        if any(k in strategy_name for k in ["持有", "观望", "待定"]):
            return "HOLD"
        return "BUY"

    async def _unfreeze_funds(self, db: Session, plan: TradingPlan):
        """解冻资金"""
        if not plan.frozen_amount or plan.frozen_amount <= 0:
            return

        account = await self._get_or_create_account(db)
        account.available_cash += plan.frozen_amount
        account.frozen_cash = max(0.0, float(account.frozen_cash or 0.0) - float(plan.frozen_amount or 0.0))
        account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
        
        unfrozen = plan.frozen_amount
        plan.frozen_amount = 0.0
        plan.frozen_vol = 0
        
        logger.info(f"Unfrozen {unfrozen:.2f} cash for plan {plan.id} ({plan.ts_code})")

    async def _realign_frozen_funds(self, db: Session, plan: TradingPlan, ref_price: float):
        if plan.executed:
            return
        if not plan.frozen_amount or plan.frozen_amount <= 0:
            return
        if float(plan.position_pct or 0.0) <= 0:
            return
        if self._infer_plan_action(plan) != "BUY":
            return
        min_buy_vol = self._get_min_buy_volume(str(plan.ts_code))
        if not plan.frozen_vol or int(plan.frozen_vol) < min_buy_vol:
            return
        if not ref_price or ref_price <= 0:
            return

        account = await self._get_or_create_account(db)
        old_frozen_amount = float(plan.frozen_amount or 0.0)
        old_vol = int(plan.frozen_vol or 0)
        budget = old_frozen_amount + float(account.available_cash or 0.0)

        def _calc_total_cost(v: int) -> float:
            need_cash = v * ref_price
            fee = self._calc_buy_fee(need_cash) + self._calc_transfer_fee(str(plan.ts_code), need_cash)
            return need_cash + fee

        vol = old_vol
        target_cost = _calc_total_cost(vol)
        if target_cost > budget:
            step = self._get_buy_volume_step(str(plan.ts_code))
            if step <= 1:
                vol = int(budget / ref_price)
            else:
                vol = int(budget / ref_price / step) * step
            if vol > old_vol:
                vol = old_vol
            if vol < min_buy_vol:
                await self._unfreeze_funds(db, plan)
                return

            target_cost = _calc_total_cost(vol)
            while vol >= min_buy_vol and target_cost > budget:
                vol -= step
                if vol < min_buy_vol:
                    await self._unfreeze_funds(db, plan)
                    return
                target_cost = _calc_total_cost(vol)

        diff = target_cost - old_frozen_amount
        account.available_cash = max(0.0, float(account.available_cash or 0.0) - diff)
        account.frozen_cash = max(0.0, float(account.frozen_cash or 0.0) + diff)
        plan.frozen_amount = float(target_cost)
        plan.frozen_vol = int(vol)

    async def create_plan(self, ts_code: str, strategy_name: str, 
                    buy_price: float, stop_loss: float = 0.0, take_profit: float = 0.0, 
                    position_pct: float = 0.1, reason: str = "", plan_date: date = None, score: float = 0.0,
                    source: str = "system", order_type: str = "MARKET", limit_price: float = 0.0,
                    ai_decision: str = None, is_sell: bool = False) -> Optional[TradingPlan]:
        """
        创建新的交易计划 (支持自动去重：同日同代码仅保留一份最新计划，无论策略名是否相同)
        source: 'system' (AI生成) 或 'user' (用户手动)
        order_type: 'MARKET' (市价) 或 'LIMIT' (限价)
        limit_price: 当 order_type 为 LIMIT 时使用的价格
        is_sell: 是否为卖出计划 (强制标记为 SELL)
        """
        db = SessionLocal()
        try:
            ts_code = data_provider._normalize_ts_code(ts_code)
            if not ts_code or len(ts_code) < 6:
                raise ValueError("股票代码格式错误")
            target_date = plan_date or date.today()
            incoming_strategy = (strategy_name or "").strip()
            is_monitor_strategy = incoming_strategy.startswith("选股监控-")
            should_freeze = True
            if source == "system" and is_monitor_strategy:
                should_freeze = False
            incoming_action = "BUY"
            if is_sell:
                incoming_action = "SELL"

            decision_key = (ai_decision or "").strip().upper()
            if decision_key in ["CANCEL", "WAIT", "HOLD"]:
                incoming_action = "HOLD"
            elif decision_key in ["SELL", "REDUCE"]:
                incoming_action = "SELL"
            elif decision_key == "BUY":
                incoming_action = "BUY"
            elif any(k in incoming_strategy for k in ["卖出", "减仓", "减持", "清仓", "止盈", "止损", "抛售"]):
                incoming_action = "SELL"
            elif any(k in incoming_strategy for k in ["持有", "观望", "待定"]):
                incoming_action = "HOLD"

            normalized_reason = (reason or "").strip()
            if is_monitor_strategy and decision_key != "BUY":
                if not normalized_reason:
                    normalized_reason = "AI监控入池"
            
            # [全局风控] 买入限制：买入价格不得超过 5日均线 (MA5) 的 5%
            # 仅针对买入计划生效 (BUY)
            # 排除 T0 补仓 (持仓做T-补仓) - 补仓通常在低位，但也需要防止追高，不过这里主要是限制开新仓追高
            # 如果是监控类计划（buy_price 为当前价），也需要检查
            if incoming_action == "BUY" and buy_price > 0 and "补仓" not in incoming_strategy:
                try:
                    # 获取最新 MA5
                    from app.services.market.stock_data_service import stock_data_service
                    # 获取最近几天的日线以计算 MA5 (包含今日)
                    # 如果今日已收盘，limit=5即为最近5天
                    # 如果是盘中，limit=5可能不包含今日实时数据，这里简化处理取最近已完成的日线 + 实时价格估算
                    # 更稳妥的是取最近 4 天收盘 + 当前 buy_price 作为第 5 天
                    klines = await asyncio.to_thread(stock_data_service.get_local_kline, ts_code, 'D', limit=5)
                    
                    ma5 = 0.0
                    if klines and len(klines) >= 4:
                        closes = [float(k['close']) for k in klines]
                        # 假设 buy_price 是今天的收盘价来计算动态 MA5
                        current_closes = closes[-4:] + [buy_price]
                        ma5 = sum(current_closes) / 5
                        
                        # 检查乖离率
                        if ma5 > 0:
                            bias_ma5 = (buy_price - ma5) / ma5 * 100
                            if bias_ma5 > 5.0:
                                logger.warning(f"Plan creation BLOCKED for {ts_code}: Buy price {buy_price} > MA5 {ma5:.2f} * 1.05 (BIAS: {bias_ma5:.2f}%)")
                                # 如果是系统生成的，直接拦截
                                if source == "system":
                                    logger.info(f"System plan rejected due to MA5 limit: {ts_code}")
                                    return None 
                                else:
                                    normalized_reason = f"[高风险警告] 价格偏离 MA5 {bias_ma5:.1f}% > 5%。{normalized_reason}"
                except Exception as e:
                    logger.error(f"Error checking MA5 limit for {ts_code}: {e}")

            if target_date == date.today() and data_provider.is_trading_time() and incoming_action in ["BUY", "SELL"]:
                q = await data_provider.get_realtime_quote(ts_code, cache_scope="trading")
                if self._is_suspended_quote(q):
                    if source == "user":
                        if normalized_reason:
                            normalized_reason = f"停牌或无成交，计划已创建等待恢复。{normalized_reason}"
                        else:
                            normalized_reason = "停牌或无成交，计划已创建等待恢复。"
                    else:
                        raise ValueError(f"{ts_code} 当前停牌，已拦截创建交易计划")
            lock_key = f"plan:{target_date.isoformat()}:{ts_code}:{incoming_action}"
            
            async with trading_lock_manager.lock(lock_key):
                candidate_plans = db.query(TradingPlan).filter(
                    TradingPlan.date == target_date,
                    TradingPlan.ts_code == ts_code,
                    TradingPlan.executed == False
                ).all()
                existing_plan = None
                for cp in candidate_plans:
                    if self._infer_plan_action(cp) == incoming_action:
                        existing_plan = cp
                        break
                
                buy_price = float(buy_price or 0.0)
                limit_price = float(limit_price or 0.0)
                order_type = (order_type or "MARKET").upper()
                need_ai_confirm = incoming_action == "BUY" and buy_price <= 0 and limit_price <= 0

                if source == "system" and incoming_action == "BUY" and buy_price <= 0 and limit_price <= 0:
                    # [Fix] 允许 AI 创建观望计划 (WAIT) 时价格为 0，仅在实际买入时强制要求价格
                    if decision_key != "WAIT":
                        raise ValueError(f"{ts_code} 交易计划缺少参考价格，已拦截创建/更新")

                if existing_plan:
                    # AI 拥有最高决策权，不再无条件保护用户手动创建的计划 (source='user')
                    # 如果现有计划是用户手动的，但当前是系统生成的更高优先级的交易决策，则允许覆盖更新
                    # 仅在现有计划已成交的情况下才禁止更新
                    if existing_plan.executed:
                        logger.info(f"Skipping update for already executed plan {ts_code}")
                        return existing_plan
                    
                    if existing_plan.source == 'user' and source == 'system':
                        logger.info(f"AI Highest Authority: System is overriding user-defined plan for {ts_code}")
                        # 允许继续向下执行更新逻辑

                    # 更新现有计划
                    if (existing_plan.track_status or "").upper() == "TRACKING":
                        if (not is_monitor_strategy) or decision_key == "BUY":
                            existing_plan.track_status = None
                    existing_plan.strategy_name = strategy_name
                    existing_plan.buy_price_limit = buy_price
                    existing_plan.stop_loss_price = stop_loss
                    existing_plan.take_profit_price = take_profit
                    existing_plan.position_pct = position_pct
                    existing_plan.reason = normalized_reason
                    existing_plan.order_type = order_type
                    existing_plan.limit_price = limit_price or buy_price
                    # [Fix] 如果 limit_price 为 0 且有 buy_price，则强制使用 buy_price
                    if (not existing_plan.limit_price or existing_plan.limit_price <= 0) and buy_price > 0:
                        existing_plan.limit_price = buy_price
                    existing_plan.updated_at = datetime.now()
                    existing_plan.source = source
                    if ai_decision:
                        existing_plan.ai_decision = ai_decision
                    if score > 0:
                        existing_plan.score = score
                    if is_monitor_strategy and decision_key != "BUY":
                        existing_plan.track_status = "TRACKING"
                    
                    db.commit()
                    db.refresh(existing_plan)
                    if existing_plan.date == date.today():
                        async with trading_lock_manager.lock("trade:account"):
                            if need_ai_confirm and float(existing_plan.frozen_amount or 0.0) > 0:
                                await self._unfreeze_funds(db, existing_plan)
                            if should_freeze and not need_ai_confirm:
                                ref_price = float(existing_plan.limit_price or existing_plan.buy_price_limit or 0.0)
                                if ref_price > 0 and float(existing_plan.frozen_amount or 0.0) > 0:
                                    await self._realign_frozen_funds(db, existing_plan, ref_price=ref_price)
                                else:
                                    await self._freeze_funds(
                                        db,
                                        existing_plan,
                                        strict=bool(source == "system" and str(existing_plan.strategy_name) == "午间强势" and existing_plan.date == date.today()),
                                    )
                            else:
                                await self._unfreeze_funds(db, existing_plan)
                            db.commit()
                            db.refresh(existing_plan)
                    if need_ai_confirm and not (existing_plan.review_content or "").strip():
                        now_str = datetime.now().strftime("%H:%M:%S")
                        existing_plan.review_content = f"[{now_str} 等待AI确认] 无有效参考价，暂不挂单，等待行情更新后再确认"
                        db.commit()
                        db.refresh(existing_plan)
                    if source == "system" and existing_plan.date == date.today() and incoming_action in ["BUY", "SELL"]:
                        entrustment_signal.notify()
                    try:
                        from app.services.plan_event_service import plan_event_service
                        await plan_event_service.publish({"type": "plan_refresh"})
                    except Exception:
                        pass
                    logger.info(f"Updated existing trading plan for {ts_code} (New Strategy: {strategy_name}, Type: {order_type})")
                    return existing_plan

                # 2. 不存在则创建新计划
                plan = TradingPlan()
                plan.date = target_date
                plan.ts_code = ts_code
                plan.strategy_name = strategy_name
                plan.buy_price_limit = buy_price
                plan.stop_loss_price = stop_loss
                plan.take_profit_price = take_profit
                plan.position_pct = position_pct
                plan.reason = normalized_reason
                plan.order_type = order_type
                plan.limit_price = limit_price or buy_price
                # [Fix] 如果 limit_price 为 0 且有 buy_price，则强制使用 buy_price
                if (not plan.limit_price or plan.limit_price <= 0) and buy_price > 0:
                    plan.limit_price = buy_price
                plan.executed = False
                plan.score = score
                plan.source = source
                plan.ai_decision = ai_decision
                if is_monitor_strategy and decision_key != "BUY":
                    plan.track_status = "TRACKING"
                db.add(plan)
                db.flush()
                db.commit()
                db.refresh(plan)
                if plan.date == date.today() and should_freeze and not need_ai_confirm:
                    async with trading_lock_manager.lock("trade:account"):
                        await self._freeze_funds(
                            db,
                            plan,
                            strict=bool(source == "system" and str(plan.strategy_name) == "午间强势"),
                        )
                        db.commit()
                        db.refresh(plan)
                    if not plan.review_content:
                        now_time = datetime.now().time()
                        wait_hint = None
                        if time(9, 15) <= now_time < time(9, 25):
                            wait_hint = "集合竞价期间生成，等待 09:25 后确认开盘价再执行"
                        elif time(11, 30) <= now_time < time(13, 0):
                            wait_hint = "午休时间生成，等待 13:00 后继续监控执行"
                        elif not data_provider.is_trading_time():
                            wait_hint = "非交易时段生成，等待下次交易时段再执行"
                        if wait_hint:
                            now_str = datetime.now().strftime("%H:%M:%S")
                            plan.review_content = f"[{now_str} 等待执行] {wait_hint}"
                            db.commit()
                            db.refresh(plan)
                if need_ai_confirm and not (plan.review_content or "").strip():
                    now_str = datetime.now().strftime("%H:%M:%S")
                    plan.review_content = f"[{now_str} 等待AI确认] 无有效参考价，暂不挂单，等待行情更新后再确认"
                    db.commit()
                    db.refresh(plan)
                if source == "system" and plan.date == date.today() and incoming_action in ["BUY", "SELL"]:
                    entrustment_signal.notify()
                try:
                    from app.services.plan_event_service import plan_event_service
                    await plan_event_service.publish({"type": "plan_refresh"})
                except Exception:
                    pass
                logger.info(f"Created trading plan for {ts_code} ({strategy_name}, Type: {order_type})")
                return plan
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating/updating trading plan: {e}")
            raise
        finally:
            db.close()

    async def update_plan(self, plan_id: int, **kwargs) -> TradingPlan:
        """更新交易计划"""
        db = SessionLocal()
        try:
            plan = db.query(TradingPlan).filter(TradingPlan.id == plan_id).first()
            if not plan:
                raise ValueError("Plan not found")
            
            if plan.executed:
                raise ValueError("Cannot update executed plan")

            if plan.frozen_amount and plan.frozen_amount > 0:
                async with trading_lock_manager.lock("trade:account"):
                    await self._unfreeze_funds(db, plan)
                    await asyncio.to_thread(db.commit)

            # 更新字段
            for key, value in kwargs.items():
                if hasattr(plan, key) and value is not None:
                    setattr(plan, key, value)
                elif key == 'buy_price': # 兼容 PlanUpdateRequest
                    plan.buy_price_limit = value
                elif key == 'stop_loss':
                    plan.stop_loss_price = value
                elif key == 'take_profit':
                    plan.take_profit_price = value

            plan.updated_at = datetime.now()

            if plan.date == date.today():
                async with trading_lock_manager.lock("trade:account"):
                    await self._freeze_funds(db, plan)
                    await asyncio.to_thread(db.commit)
            else:
                await asyncio.to_thread(db.commit)
            await asyncio.to_thread(db.refresh, plan)
            return plan
        except Exception as e:
            await asyncio.to_thread(db.rollback)
            logger.error(f"Error updating plan {plan_id}: {e}")
            raise
        finally:
            db.close()

    async def delete_plan(self, plan_id: int):
        """删除交易计划并解冻资金"""
        db = SessionLocal()
        try:
            plan = db.query(TradingPlan).filter(TradingPlan.id == plan_id).first()
            if not plan:
                raise ValueError("Plan not found")
            
            if plan.executed:
                raise ValueError("Cannot delete executed plan")
            
            # 解冻资金
            if plan.frozen_amount and plan.frozen_amount > 0:
                async with trading_lock_manager.lock("trade:account"):
                    await self._unfreeze_funds(db, plan)
            
            db.delete(plan)
            db.commit()
            logger.info(f"Deleted plan {plan_id} and unfrozen funds")
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting plan {plan_id}: {e}")
            raise
        finally:
            db.close()

    async def unfreeze_daily_funds(self):
        await self.unfreeze_all_expired_plans()

    async def unfreeze_all_expired_plans(self):
        """
        盘后处理：解冻所有未成交计划的资金，以及处理异常遗留的冻结资金
        """
        db = SessionLocal()
        try:
            plans = await asyncio.to_thread(lambda: db.query(TradingPlan).filter(TradingPlan.frozen_amount > 0).all())

            unfrozen_total = 0.0
            count = 0
            if not plans:
                logger.info("No plans with frozen funds found.")
            else:
                today = date.today()
                allow_unfreeze_today = data_provider.is_after_market_close()
                async with trading_lock_manager.lock("trade:account"):
                    for plan in plans:
                        # [Fix Race Condition] Refresh plan to ensure we have the latest state (executed, frozen_amount)
                        try:
                            db.refresh(plan)
                        except Exception:
                            continue

                        amount = float(plan.frozen_amount or 0.0)
                        if amount <= 0:
                            continue

                        if plan.date == today and not allow_unfreeze_today:
                            continue

                        if (not plan.executed) and (not plan.review_content):
                            now_str = datetime.now().strftime("%H:%M")
                            if plan.date < today or allow_unfreeze_today:
                                plan.review_content = f"收盘未成交，资金已于 {now_str} 自动解冻"
                            else:
                                plan.review_content = f"系统检测到异常冻结资金，已于 {now_str} 自动释放"

                        await self._unfreeze_funds(db, plan)
                        unfrozen_total += amount
                        count += 1
            
            if unfrozen_total > 0:
                await asyncio.to_thread(db.commit)
                logger.info(f"Auto-unfrozen {unfrozen_total:.2f} cash for {count} plans.")
            else:
                logger.info("No plans met unfreeze criteria (either it's trading time or no frozen amounts).")

            if not data_provider.is_trading_time():
                from datetime import timedelta
                from app.models.stock_models import TradeRecord, Position

                today = date.today()
                start_date = today - timedelta(days=30)
                candidates = await asyncio.to_thread(lambda: db.query(TradingPlan).filter(
                    TradingPlan.executed == False,
                    TradingPlan.frozen_amount == 0,
                    TradingPlan.date < today,
                    TradingPlan.date >= start_date
                ).order_by(TradingPlan.date.desc(), TradingPlan.id.desc()).all())

                expired_count = 0
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                for plan in candidates:
                    strategy_name = plan.strategy_name or ""
                    if not any(k in strategy_name for k in ["卖出", "减仓", "清仓", "止盈", "止损"]):
                        continue
                    has_trade = await asyncio.to_thread(lambda: db.query(TradeRecord.id).filter(TradeRecord.plan_id == plan.id).first())
                    if has_trade:
                        continue
                    has_pos = await asyncio.to_thread(lambda: db.query(Position.id).filter(Position.ts_code == plan.ts_code, Position.vol > 0).first())
                    if has_pos:
                        continue
                    if (plan.track_status or "").upper() != "FINISHED":
                        plan.track_status = "FINISHED"
                    if not plan.review_content:
                        plan.review_content = f"卖出计划未执行且已过期，系统已自动作废 ({now_str})"
                    expired_count += 1

                if expired_count > 0:
                    await asyncio.to_thread(db.commit)
                    logger.info(f"Auto-expired {expired_count} stale SELL plans.")
        finally:
            db.close()

    async def freeze_pending_plans_today(self) -> int:
        db = SessionLocal()
        try:
            today = date.today()
            plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan).filter(
                    TradingPlan.date == today,
                    TradingPlan.executed == False,
                    TradingPlan.frozen_amount == 0,
                ).all()
            )
            if not plans:
                return 0

            async with trading_lock_manager.lock("trade:account"):
                before_frozen = 0.0
                account = await self._get_or_create_account(db)
                before_frozen = float(account.frozen_cash or 0.0)

                for plan in plans:
                    await self._freeze_funds(db, plan)

                await asyncio.to_thread(db.commit)
                await asyncio.to_thread(db.refresh, account)
                after_frozen = float(account.frozen_cash or 0.0)

            frozen_count = sum(1 for p in plans if float(p.frozen_amount or 0.0) > 0)
            logger.info(f"Freeze pending plans today: {frozen_count} plans, frozen_cash {before_frozen:.2f} -> {after_frozen:.2f}")
            return frozen_count
        finally:
            db.close()

    async def repair_duplicate_plans_today(self) -> Dict[str, int]:
        db = SessionLocal()
        try:
            today = date.today()
            plans = await asyncio.to_thread(
                lambda: db.query(TradingPlan)
                .filter(TradingPlan.date == today, TradingPlan.executed == False)
                .order_by(TradingPlan.created_at.desc(), TradingPlan.id.desc())
                .all()
            )
            if not plans:
                return {"dedup_groups": 0, "deleted_plans": 0}

            groups: Dict[Tuple[str, str], List[TradingPlan]] = {}
            for p in plans:
                ts_code = (p.ts_code or "").strip()
                if not ts_code:
                    continue
                action = self._infer_plan_action(p)
                groups.setdefault((ts_code, action), []).append(p)

            dedup_groups = 0
            deleted = 0

            def _query_trade_exists(plan_id: int) -> Any:
                return db.query(TradeRecord.id).filter(TradeRecord.plan_id == plan_id).first()

            async with trading_lock_manager.lock("trade:account"):
                for (ts_code, action), items in groups.items():
                    if len(items) <= 1:
                        continue

                    dedup_groups += 1

                    user_items = [x for x in items if (x.source or "").lower() == "user"]
                    if user_items:
                        keep = user_items[0]
                    else:
                        frozen_items = [x for x in items if float(x.frozen_amount or 0.0) > 0]
                        keep = frozen_items[0] if frozen_items else items[0]

                    try:
                        db.refresh(keep)
                    except Exception:
                        pass

                    for p in items:
                        if p.id == keep.id:
                            continue
                        try:
                            db.refresh(p)
                        except Exception:
                            continue

                        plan_id = int(p.id or 0)
                        has_trade = await asyncio.to_thread(_query_trade_exists, plan_id)
                        if has_trade:
                            continue

                        if float(p.frozen_amount or 0.0) > 0:
                            await self._unfreeze_funds(db, p)

                        await asyncio.to_thread(db.delete, p)
                        deleted += 1

                    if action == "BUY":
                        should_freeze = True
                        strategy = (keep.strategy_name or "").strip()
                        if (keep.source or "").lower() == "system" and strategy.startswith("选股监控-"):
                            should_freeze = False
                        if should_freeze:
                            ref_price = float(keep.limit_price or keep.buy_price_limit or 0.0)
                            if ref_price > 0 and float(keep.frozen_amount or 0.0) > 0:
                                await self._realign_frozen_funds(db, keep, ref_price=ref_price)
                            elif float(keep.frozen_amount or 0.0) <= 0:
                                await self._freeze_funds(db, keep, strict=False)

                await asyncio.to_thread(db.commit)

            return {"dedup_groups": int(dedup_groups), "deleted_plans": int(deleted)}
        finally:
            db.close()

    async def repair_orphan_trade_records(self, days: int = 7) -> int:
        db = SessionLocal()
        try:
            start_dt = datetime.combine(date.today() - timedelta(days=days), datetime.min.time())
            records = await asyncio.to_thread(
                lambda: db.query(TradeRecord).filter(
                    TradeRecord.trade_time >= start_dt,
                    TradeRecord.plan_id == None,
                ).order_by(TradeRecord.trade_time.asc()).all()
            )
            if not records:
                records = []

            async with trading_lock_manager.lock("trade:repair"):
                repaired = 0
                for r in records:
                    trade_type = (r.trade_type or "").upper()
                    action_label = "买入" if trade_type == "BUY" else "卖出"
                    px = float(r.price or 0.0)
                    plan = TradingPlan()
                    plan.date = r.trade_time.date() if r.trade_time else date.today()
                    plan.ts_code = r.ts_code
                    plan.strategy_name = f"补单-{action_label}"
                    plan.buy_price_limit = px
                    plan.stop_loss_price = 0.0
                    plan.take_profit_price = 0.0
                    plan.position_pct = 0.0
                    plan.reason = ""
                    plan.order_type = "MARKET"
                    plan.limit_price = px
                    plan.executed = True
                    plan.score = 0.0
                    plan.source = "system"
                    plan.created_at = r.trade_time or datetime.now()
                    if trade_type == "BUY":
                        plan.entry_price = px
                    else:
                        plan.exit_price = px
                        plan.close_reason = "补单"

                    await asyncio.to_thread(db.add, plan)
                    await asyncio.to_thread(db.flush)
                    r.plan_id = plan.id
                    repaired += 1

                mislinked = await asyncio.to_thread(
                    lambda: db.query(TradeRecord).filter(
                        TradeRecord.trade_time >= start_dt,
                        TradeRecord.plan_id != None,
                    ).order_by(TradeRecord.trade_time.asc()).all()
                )
                for r in mislinked:
                    if (r.trade_type or "").upper() != "SELL":
                        continue
                    
                    def _query_plan_by_id(pid: int):
                        return db.query(TradingPlan).filter(TradingPlan.id == pid).first()
                    
                    plan = await asyncio.to_thread(_query_plan_by_id, int(r.plan_id or 0))
                    if not plan:
                        continue
                    if self._infer_plan_action(plan) != "BUY":
                        continue

                    base_reason = plan.close_reason or ""
                    strategy_name = "持仓卖出"
                    if "[REDUCE]" in base_reason or "减仓" in base_reason or "减持" in base_reason:
                        strategy_name = "持仓减仓"
                    if "清仓" in base_reason:
                        strategy_name = "清仓卖出"

                    t = r.trade_time or datetime.now()
                    px = float(r.price or 0.0)
                    existing = await asyncio.to_thread(
                        lambda: db.query(TradingPlan).filter(
                            TradingPlan.ts_code == r.ts_code,
                            TradingPlan.executed == True,
                            TradingPlan.created_at >= t - timedelta(minutes=10),
                            TradingPlan.created_at <= t + timedelta(minutes=10),
                        ).order_by(TradingPlan.created_at.desc()).all()
                    )
                    sell_plan = None
                    for ep in existing:
                        if self._infer_plan_action(ep) != "SELL":
                            continue
                        if ep.exit_price is None:
                            continue
                        if abs(float(ep.exit_price or 0.0) - px) <= max(0.01, px * 0.001):
                            sell_plan = ep
                            break

                    if not sell_plan:
                        sell_plan = TradingPlan()
                        sell_plan.date = t.date()
                        sell_plan.ts_code = r.ts_code
                        sell_plan.strategy_name = strategy_name
                        sell_plan.buy_price_limit = px
                        sell_plan.stop_loss_price = 0.0
                        sell_plan.take_profit_price = 0.0
                        sell_plan.position_pct = 0.0
                        sell_plan.reason = base_reason
                        sell_plan.order_type = plan.order_type or "MARKET"
                        sell_plan.limit_price = px
                        sell_plan.executed = True
                        sell_plan.score = 0.0
                        sell_plan.source = "system"
                        sell_plan.created_at = t
                        sell_plan.exit_price = px
                        sell_plan.pnl_pct = r.pnl_pct
                        sell_plan.close_reason = base_reason
                        await asyncio.to_thread(db.add, sell_plan)
                        await asyncio.to_thread(db.flush)

                    r.plan_id = sell_plan.id
                    repaired += 1

                await asyncio.to_thread(db.commit)
                return repaired
        finally:
            db.close()

    async def get_todays_plans(self) -> List[TradingPlan]:
        """
        获取当前有效交易日的交易计划
        """
        db = SessionLocal()
        try:
            def _load() -> List[TradingPlan]:
                from sqlalchemy import func

                today = date.today()
                next_plan_date = db.query(func.min(TradingPlan.date)).filter(TradingPlan.date >= today).scalar()
                if next_plan_date is None:
                    next_plan_date = db.query(func.max(TradingPlan.date)).scalar()
                if next_plan_date is None:
                    return []
                return db.query(TradingPlan).filter(
                    TradingPlan.date == next_plan_date,
                    TradingPlan.executed == False
                ).all()

            return await asyncio.to_thread(_load)
        finally:
            db.close()

    async def record_daily_performance(self, target_date: Optional[date] = None):
        """记录每日盈亏与资金快照"""
        from app.models.stock_models import DailyPerformance, Account
        
        if not target_date:
            target_date = date.today()
            
        db = SessionLocal()
        try:
            account = await asyncio.to_thread(lambda: db.query(Account).first())
            if not account:
                logger.error("No account found to record performance")
                return

            async with trading_lock_manager.lock("trade:account"):
                expected_total_cash = await self._calc_expected_total_cash(db)
                expected_frozen = await self._calc_expected_frozen_cash(db)
                if expected_frozen > expected_total_cash + 0.01:
                    expected_frozen = expected_total_cash
                expected_available = float(expected_total_cash - expected_frozen)

                if abs(float(account.available_cash or 0.0) - expected_available) > 0.01:
                    account.available_cash = expected_available
                if abs(float(account.frozen_cash or 0.0) - expected_frozen) > 0.01:
                    account.frozen_cash = expected_frozen

                account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
                self._recalc_account_pnl(account)

                await asyncio.to_thread(db.commit)
                await asyncio.to_thread(db.refresh, account)
            
            # 2. 获取累计盈亏
            initial_assets = settings.INITIAL_CAPITAL
            total_pnl = account.total_assets - initial_assets
            total_pnl_pct = (total_pnl / initial_assets) * 100 if initial_assets != 0 else 0
            
            # 3. 计算当日盈亏 (相比于前一个记录)
            prev_perf = await asyncio.to_thread(lambda: db.query(DailyPerformance)
                .filter(DailyPerformance.date < target_date)
                .order_by(DailyPerformance.date.desc())
                .first())
            prev_assets = prev_perf.total_assets if prev_perf else initial_assets
            prev_available = prev_perf.available_cash if prev_perf else account.available_cash
            prev_frozen = prev_perf.frozen_cash if prev_perf else account.frozen_cash
            prev_market_value = prev_perf.market_value if prev_perf else account.market_value
            prev_total_pnl = prev_perf.total_pnl if prev_perf else (prev_assets - initial_assets)
            prev_total_pnl_pct = prev_perf.total_pnl_pct if prev_perf else ((prev_total_pnl / initial_assets) * 100 if initial_assets != 0 else 0)

            if prev_perf and prev_perf.date and prev_perf.date < target_date - timedelta(days=1):
                fill_date = prev_perf.date + timedelta(days=1)
                while fill_date < target_date:
                    try:
                        trade_day_info = await data_provider.check_trade_day(fill_date.strftime("%Y%m%d"))
                        is_open = bool(trade_day_info.get("is_open", False))
                    except Exception:
                        is_open = fill_date.weekday() < 5
                    if is_open:
                        missing = await asyncio.to_thread(
                            lambda: db.query(DailyPerformance).filter(DailyPerformance.date == fill_date).first()
                        )
                        if not missing:
                            missing = DailyPerformance()
                            missing.date = fill_date
                            await asyncio.to_thread(db.add, missing)
                        missing.total_assets = prev_assets
                        missing.available_cash = prev_available
                        missing.frozen_cash = prev_frozen
                        missing.market_value = prev_market_value
                        missing.daily_pnl = 0.0
                        missing.daily_pnl_pct = 0.0
                        missing.total_pnl = prev_total_pnl
                        missing.total_pnl_pct = prev_total_pnl_pct
                        missing.updated_at = datetime.now()
                    fill_date += timedelta(days=1)
                await asyncio.to_thread(db.commit)
            
            if prev_perf:
                daily_pnl = account.total_assets - prev_assets
                daily_pnl_pct = (daily_pnl / prev_assets) * 100 if prev_assets != 0 else 0
            else:
                # 第一条记录，当日盈亏即总盈亏
                daily_pnl = total_pnl
                daily_pnl_pct = total_pnl_pct
            
            # 4. 保存记录 (支持覆盖同一天的记录)
            perf = await asyncio.to_thread(lambda: db.query(DailyPerformance).filter(DailyPerformance.date == target_date).first())
            if not perf:
                perf = DailyPerformance()
                perf.date = target_date
                await asyncio.to_thread(db.add, perf)
            
            perf.total_assets = account.total_assets
            perf.available_cash = account.available_cash
            perf.frozen_cash = account.frozen_cash
            perf.market_value = account.market_value
            perf.daily_pnl = daily_pnl
            perf.daily_pnl_pct = daily_pnl_pct
            perf.total_pnl = total_pnl
            perf.total_pnl_pct = total_pnl_pct
            perf.updated_at = datetime.now()
            
            await asyncio.to_thread(db.commit)
            logger.info(f"Recorded daily performance for {target_date}: Assets={account.total_assets:.2f}, Daily PnL={daily_pnl:.2f}")
            
        except Exception as e:
            logger.error(f"Error recording daily performance: {e}")
            await asyncio.to_thread(db.rollback)
        finally:
            db.close()

    async def update_execution_status(
        self,
        plan_id: int,
        entry_price: float,
        volume: int,
        executed: bool = True,
        fee: Optional[float] = None,
    ):
        db = SessionLocal()
        try:
            plan = await asyncio.to_thread(lambda: db.query(TradingPlan).filter(TradingPlan.id == plan_id).first())
            if not plan:
                raise ValueError("Plan not found")

            if not executed:
                plan.executed = False
                plan.entry_price = None
                await asyncio.to_thread(db.commit)
                logger.info(f"Updated execution status for plan {plan_id}")
                return

            if not entry_price or entry_price <= 0:
                raise ValueError("Invalid entry_price")
            min_buy_vol = self._get_min_buy_volume(str(plan.ts_code))
            if not volume or int(volume) < min_buy_vol:
                raise ValueError("Invalid volume")

            async with trading_lock_manager.lock("trade:account"):
                account = await self._get_or_create_account(db)
                stock_info = await asyncio.to_thread(lambda: db.query(Stock).filter(Stock.ts_code == plan.ts_code).first())
                stock_name = str((stock_info.name if stock_info else plan.ts_code) or plan.ts_code or "")

                need_cash = float(volume) * float(entry_price)
                calc_fee = self._calc_buy_fee(need_cash) + self._calc_transfer_fee(str(plan.ts_code), need_cash)
                fee_val = float(fee) if fee is not None else float(calc_fee)
                total_cost = need_cash + fee_val

                if plan.frozen_amount and plan.frozen_amount > 0:
                    diff = total_cost - float(plan.frozen_amount)
                    if float(account.available_cash or 0.0) < diff:
                        raise ValueError("Insufficient available cash for execution diff")

                    account.frozen_cash = max(0.0, float(account.frozen_cash or 0.0) - float(plan.frozen_amount))
                    account.available_cash = float(account.available_cash or 0.0) - diff
                    plan.frozen_amount = 0.0
                    plan.frozen_vol = 0
                else:
                    if float(account.available_cash or 0.0) < total_cost:
                        raise ValueError("Insufficient available cash for execution")
                    account.available_cash = float(account.available_cash or 0.0) - total_cost

                position = await asyncio.to_thread(lambda: db.query(Position).filter(Position.ts_code == plan.ts_code).first())
                if not position:
                    position = Position()
                    position.ts_code = str(plan.ts_code)
                    position.symbol = str(plan.ts_code).split('.')[0]
                    position.name = stock_name
                    position.vol = 0
                    position.available_vol = 0
                    position.avg_price = 0.0
                    await asyncio.to_thread(db.add, position)

                old_cost = float(position.vol or 0) * float(position.avg_price or 0.0)
                new_cost = old_cost + total_cost
                position.vol = int(position.vol or 0) + int(volume)
                position.avg_price = new_cost / float(position.vol)
                position.current_price = float(entry_price)
                position.market_value = float(position.vol) * float(entry_price)
                position.float_pnl = float(position.market_value) - new_cost
                position.pnl_pct = (float(position.float_pnl) / new_cost * 100) if new_cost > 0 else 0.0

                record = TradeRecord()
                record.ts_code = str(plan.ts_code)
                record.name = stock_name
                record.trade_type = "BUY"
                record.price = float(entry_price)
                record.vol = int(volume)
                record.amount = float(need_cash)
                record.fee = float(fee_val)
                record.plan_id = plan.id
                await asyncio.to_thread(db.add, record)

                plan.executed = True
                plan.entry_price = float(entry_price)

                all_positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
                ts_codes = [str(p.ts_code) for p in all_positions if p.ts_code]
                quotes = await data_provider.get_realtime_quotes(ts_codes) if ts_codes else {}
                total_mv, _ = await self._refresh_positions_with_quotes(all_positions, quotes)
                account.market_value = float(total_mv or 0.0)
                account.total_assets = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
                self._recalc_account_pnl(account)

                await asyncio.to_thread(db.commit)
                logger.info(f"Updated execution status for plan {plan_id}")
        finally:
            db.close()

    async def save_market_sentiment(self, up_count: int, down_count: int, 
                              limit_up: int, limit_down: int, 
                              temperature: float, summary: str, sentiment_date: date = None):
        """
        保存每日市场情绪快照
        """
        db = SessionLocal()
        try:
            target_date = sentiment_date or date.today()
            
            # 检查是否已存在同日期的记录
            existing = await asyncio.to_thread(lambda: db.query(MarketSentiment).filter(MarketSentiment.date == target_date).first())
            
            if existing:
                # 更新现有记录
                existing.up_count = up_count
                existing.down_count = down_count
                existing.limit_up_count = limit_up
                existing.limit_down_count = limit_down
                existing.market_temperature = temperature
                existing.summary = summary
                existing.updated_at = datetime.now()
                logger.info(f"Updated market sentiment for {target_date} (Temp: {temperature})")
            else:
                # 创建新记录
                sentiment = MarketSentiment()
                sentiment.date = target_date
                sentiment.up_count = up_count
                sentiment.down_count = down_count
                sentiment.limit_up_count = limit_up
                sentiment.limit_down_count = limit_down
                sentiment.market_temperature = temperature
                sentiment.summary = summary
                await asyncio.to_thread(db.add, sentiment)
                logger.info(f"Created market sentiment for {target_date} (Temp: {temperature})")
                
            await asyncio.to_thread(db.commit)
        except Exception as e:
            await asyncio.to_thread(db.rollback)
            logger.error(f"Error saving market sentiment: {e}")
        finally:
            db.close()

    async def get_pending_plans(self, include_monitor_fallback: bool = False) -> List[TradingPlan]:
        """获取今日未执行的计划"""
        def _get():
            db = SessionLocal()
            try:
                today = date.today()
                # 过滤出未执行且未被取消的计划
                plans = db.query(TradingPlan).filter(
                    TradingPlan.date == today,
                    TradingPlan.executed == False,
                ).all()
                if include_monitor_fallback and not plans:
                    from sqlalchemy import func
                    max_date = db.query(func.max(TradingPlan.date)).filter(
                        TradingPlan.executed == False,
                        TradingPlan.strategy_name.like("选股监控-%"),
                    ).scalar()
                    if max_date:
                        plans = db.query(TradingPlan).filter(
                            TradingPlan.date == max_date,
                            TradingPlan.executed == False,
                            TradingPlan.strategy_name.like("选股监控-%"),
                        ).all()
                return plans
            finally:
                db.close()
        # 再次过滤掉状态为 CANCELLED 的（如果有的话，虽然 TradingPlan 没有 status 字段，但可能有 track_status）
        plans = await asyncio.to_thread(_get)
        return [p for p in plans if (p.track_status or "").upper() not in {"CANCELLED", "FINISHED"}]

    async def cancel_plan(self, plan_id: int, reason: str = "Cancelled"):
        """取消计划 (解冻资金并标记为已取消)"""
        db = SessionLocal()
        try:
            plan = await asyncio.to_thread(lambda: db.query(TradingPlan).get(plan_id))
            if not plan:
                return
            
            if plan.executed:
                return

            # 解冻资金
            if plan.frozen_amount and plan.frozen_amount > 0:
                async with trading_lock_manager.lock("trade:account"):
                    await self._unfreeze_funds(db, plan)
                    await asyncio.to_thread(db.commit)
            
            # 标记为已取消 (借用 track_status)
            plan.track_status = 'CANCELLED'
            plan.review_content = reason
            plan.executed = True # 标记为已执行以从 pending 列表移除，防止重复处理
            plan.updated_at = datetime.now()
            
            await asyncio.to_thread(db.commit)
            try:
                from app.services.plan_event_service import plan_event_service
                await plan_event_service.publish({
                    "type": "plan_removed",
                    "plan_id": int(plan.id or 0),
                    "ts_code": str(plan.ts_code),
                    "reason": str(reason or "")
                })
            except Exception:
                pass
            logger.info(f"Cancelled plan {plan_id}: {reason}")
        except Exception as e:
            await asyncio.to_thread(db.rollback)
            logger.error(f"Error cancelling plan {plan_id}: {e}")
        finally:
            db.close()

    async def update_plan_price(self, plan_id: int, price: float, reason: str = None):
        """更新计划价格 (通常用于追涨或修改限价)"""
        await self.update_plan(plan_id, buy_price=price, limit_price=price, reason=reason)
        logger.info(f"Updated plan {plan_id} price to {price}: {reason}")

    async def update_plan_review(self, plan_id: int, review_content: str, ai_decision: str = None, decision_price: float = None):
        db = SessionLocal()
        try:
            plan = db.query(TradingPlan).filter(TradingPlan.id == plan_id).first()
            if not plan:
                return
            if review_content is not None:
                plan.review_content = review_content
            if ai_decision is not None:
                plan.ai_decision = ai_decision
            if decision_price is not None:
                plan.decision_price = decision_price
            plan.updated_at = datetime.now()
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating plan review {plan_id}: {e}")
        finally:
            db.close()

trading_service = TradingService()
