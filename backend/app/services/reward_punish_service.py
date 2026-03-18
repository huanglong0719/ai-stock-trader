import datetime
import threading
import time
import asyncio
import json
from dataclasses import dataclass
from typing import Dict, List, Optional

from sqlalchemy import func, case

from app.db.session import SessionLocal
from app.models.stock_models import (
    RewardPunishRule,
    RewardPunishEvent,
    RewardPunishState,
    RewardPunishAppeal,
    DailyPerformance,
    TradeRecord,
    Account,
    DailyBar,
    TradingPlan,
    OutcomeEvent,
)
from app.services.logger import logger


@dataclass
class RewardPunishMetrics:
    total_return: float
    max_drawdown: float
    sharpe: float
    win_rate: float
    consecutive_loss_pct: float
    daily_buy_count: int


class RewardPunishService:
    def __init__(self):
        self._summary_cache: Optional[Dict[str, object]] = None
        self._summary_cache_ts: float = 0.0
        self._summary_cache_ttl: float = 30.0
        self._summary_lock = threading.Lock()

    def _refresh_summary(self) -> None:
        db = SessionLocal()
        try:
            state = self._get_or_create_state(db)
            metrics = self._calc_metrics(db)
            events = (
                db.query(RewardPunishEvent)
                .order_by(RewardPunishEvent.created_at.desc())
                .limit(20)
                .all()
            )
            summary = {
                "metrics": metrics.__dict__,
                "trading_paused": state.trading_paused,
                "pause_reason": state.pause_reason,
                "recent_events": [
                    {
                        "id": e.id,
                        "rule_name": e.rule_name,
                        "metric": e.metric,
                        "value": e.metric_value,
                        "action": e.action,
                        "level": e.level,
                        "status": e.status,
                        "date": e.event_date.isoformat(),
                    }
                    for e in events
                ],
            }
            with self._summary_lock:
                self._summary_cache = summary
                self._summary_cache_ts = time.time()
        finally:
            db.close()

    def _get_or_create_state(self, db) -> RewardPunishState:
        state = db.query(RewardPunishState).first()
        if not state:
            state = RewardPunishState(trading_paused=False, pause_reason="")
            db.add(state)
            db.commit()
            db.refresh(state)
        return state

    def _get_or_init_rules(self, db) -> List[RewardPunishRule]:
        rules = db.query(RewardPunishRule).all()
        if rules:
            return rules
        defaults = [
            RewardPunishRule(
                name="reward_return_20",
                category="reward",
                metric="total_return",
                comparator=">=",
                threshold=0.20,
                action="BONUS",
                level="LEVEL1",
            ),
            RewardPunishRule(
                name="reward_return_30",
                category="reward",
                metric="total_return",
                comparator=">=",
                threshold=0.30,
                action="BONUS_EXTRA",
                level="LEVEL2",
            ),
            RewardPunishRule(
                name="reward_sharpe_1_5",
                category="reward",
                metric="sharpe",
                comparator=">=",
                threshold=1.5,
                action="BONUS_SHARPE",
                level="LEVEL1",
            ),
            RewardPunishRule(
                name="reward_win_rate_60",
                category="reward",
                metric="win_rate",
                comparator=">=",
                threshold=0.60,
                action="BONUS_WINRATE",
                level="LEVEL1",
            ),
            RewardPunishRule(
                name="punish_drawdown_15",
                category="punish",
                metric="max_drawdown",
                comparator=">=",
                threshold=0.15,
                action="PAUSE_TRADING",
                level="HIGH",
            ),
            RewardPunishRule(
                name="punish_consecutive_loss_10",
                category="punish",
                metric="consecutive_loss_pct",
                comparator=">=",
                threshold=0.10,
                action="PAUSE_TRADING",
                level="HIGH",
            ),
            RewardPunishRule(
                name="punish_no_daily_buy",
                category="punish",
                metric="daily_buy_count",
                comparator="<",
                threshold=1,
                action="STRONG_PENALTY",
                level="CRITICAL",
            ),
        ]
        for rule in defaults:
            db.add(rule)
        db.commit()
        return db.query(RewardPunishRule).all()

    def _compare(self, metric_value: float, rule: RewardPunishRule) -> bool:
        if rule.comparator == ">=":
            return metric_value >= rule.threshold
        if rule.comparator == "<=":
            return metric_value <= rule.threshold
        if rule.comparator == ">":
            return metric_value > rule.threshold
        if rule.comparator == "<":
            return metric_value < rule.threshold
        return False

    def _pick_force_buy_candidate(self, db) -> Optional[str]:
        latest_date = db.query(func.max(DailyBar.trade_date)).scalar()
        if latest_date:
            event_type = "selector_four_signals"
            events = (
                db.query(OutcomeEvent.payload_json)
                .filter(
                    OutcomeEvent.event_type == event_type,
                    OutcomeEvent.event_date == latest_date,
                )
                .all()
            )
            candidates = []
            for e in events:
                try:
                    payload = json.loads(e[0] or "{}")
                except Exception:
                    payload = {}
                ts_code = payload.get("ts_code")
                if ts_code:
                    candidates.append(str(ts_code))
            if candidates:
                bars = (
                    db.query(DailyBar.ts_code, DailyBar.close, DailyBar.amount)
                    .filter(DailyBar.trade_date == latest_date, DailyBar.ts_code.in_(candidates))
                    .order_by(DailyBar.amount.desc())
                    .all()
                )
                for row in bars:
                    ts_code = str(row[0])
                    closes = (
                        db.query(DailyBar.close)
                        .filter(DailyBar.ts_code == ts_code, DailyBar.trade_date <= latest_date)
                        .order_by(DailyBar.trade_date.desc())
                        .limit(10)
                        .all()
                    )
                    close_vals = [float(x[0]) for x in closes if x and x[0]]
                    if len(close_vals) < 10:
                        continue
                    ma5_val = sum(close_vals[:5]) / 5
                    ma10_val = sum(close_vals[:10]) / 10
                    price = float(row[1] or 0.0)
                    if price <= 0:
                        continue
                    near_ma5 = price <= ma5_val * 1.05
                    above_ma10 = price > ma10_val
                    if near_ma5 and above_ma10:
                        return ts_code
        return None

    def _ensure_force_buy_plan(self, db, as_of_date: datetime.date) -> Optional[str]:
        exists = (
            db.query(TradingPlan)
            .filter(
                TradingPlan.date == as_of_date,
                TradingPlan.executed == False,
                TradingPlan.strategy_name == "强制买入",
            )
            .first()
        )
        if exists:
            return str(exists.ts_code or "")
        ts_code = self._pick_force_buy_candidate(db)
        if not ts_code:
            return None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        from app.services.trading_service import trading_service
        coro = trading_service.create_plan(
            ts_code=ts_code,
            strategy_name="强制买入",
            buy_price=0.0,
            stop_loss=0.0,
            take_profit=0.0,
            position_pct=0.1,
            reason="每日必须买入约束触发",
            plan_date=as_of_date,
            source="system",
            order_type="MARKET",
            limit_price=0.0,
            ai_decision="BUY",
        )
        if loop and loop.is_running():
            logger.warning("Force buy plan skipped due to running event loop")
            return None
        asyncio.run(coro)
        return ts_code

    def _calc_metrics(self, db, as_of_date: Optional[datetime.date] = None) -> RewardPunishMetrics:
        daily = db.query(DailyPerformance).order_by(DailyPerformance.date.asc()).all()
        total_return = 0.0
        max_drawdown = 0.0
        sharpe = 0.0
        consecutive_loss_pct = 0.0
        if daily:
            first_assets = daily[0].total_assets or 0
            last_assets = daily[-1].total_assets or 0
            if first_assets > 0:
                total_return = last_assets / first_assets - 1.0
            peak = daily[0].total_assets or 0
            drawdowns = []
            returns = []
            for d in daily:
                if d.total_assets > peak:
                    peak = d.total_assets
                if peak > 0:
                    drawdowns.append((peak - d.total_assets) / peak)
                returns.append((d.daily_pnl_pct or 0) / 100.0)
            max_drawdown = max(drawdowns) if drawdowns else 0.0
            if len(returns) >= 2:
                avg_ret = sum(returns) / len(returns)
                var = sum((x - avg_ret) ** 2 for x in returns) / (len(returns) - 1)
                std = var ** 0.5
                if std > 0:
                    sharpe = (avg_ret / std) * (252 ** 0.5)
            streak_loss = 0.0
            for d in reversed(daily):
                if (d.daily_pnl_pct or 0) < 0:
                    streak_loss += abs(d.daily_pnl_pct or 0) / 100.0
                else:
                    break
            consecutive_loss_pct = streak_loss

        trade_stats = db.query(
            func.count(TradeRecord.id),
            func.sum(case((TradeRecord.pnl_pct > 0, 1), else_=0))
        ).filter(TradeRecord.pnl_pct != None).first()
        trade_count = int(trade_stats[0] or 0) if trade_stats else 0
        win_count = int(trade_stats[1] or 0) if trade_stats else 0
        win_rate = (win_count / trade_count) if trade_count > 0 else 0.0

        if as_of_date is None:
            as_of_date = datetime.date.today()
        start_dt = datetime.datetime.combine(as_of_date, datetime.time.min)
        end_dt = datetime.datetime.combine(as_of_date, datetime.time.max)
        daily_buy_count = (
            db.query(TradeRecord)
            .filter(
                TradeRecord.trade_type == "BUY",
                TradeRecord.trade_time >= start_dt,
                TradeRecord.trade_time <= end_dt,
            )
            .count()
        )

        return RewardPunishMetrics(
            total_return=total_return,
            max_drawdown=max_drawdown,
            sharpe=sharpe,
            win_rate=win_rate,
            consecutive_loss_pct=consecutive_loss_pct,
            daily_buy_count=daily_buy_count,
        )

    def evaluate_daily(self, as_of_date: Optional[datetime.date] = None) -> Dict[str, float]:
        db = SessionLocal()
        try:
            state = self._get_or_create_state(db)
            rules = self._get_or_init_rules(db)
            metrics = self._calc_metrics(db, as_of_date)
            metric_map = {
                "total_return": metrics.total_return,
                "max_drawdown": metrics.max_drawdown,
                "sharpe": metrics.sharpe,
                "win_rate": metrics.win_rate,
                "consecutive_loss_pct": metrics.consecutive_loss_pct,
                "daily_buy_count": float(metrics.daily_buy_count),
            }

            if as_of_date is None:
                as_of_date = datetime.date.today()

            if metrics.daily_buy_count < 1:
                forced = self._ensure_force_buy_plan(db, as_of_date)
                if forced:
                    logger.info(f"Force buy plan created for {forced}")

            for rule in rules:
                if not rule.enabled:
                    continue
                metric_value = metric_map.get(rule.metric, 0.0)
                triggered = self._compare(metric_value, rule)
                if not triggered:
                    continue
                exists = (
                    db.query(RewardPunishEvent)
                    .filter(
                        RewardPunishEvent.rule_id == rule.id,
                        RewardPunishEvent.event_date == as_of_date,
                    )
                    .first()
                )
                if exists:
                    continue
                event_action = rule.action
                if event_action in ["PAUSE_TRADING", "STRONG_PENALTY"]:
                    event_action = "ALERT"
                event = RewardPunishEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    event_date=as_of_date,
                    metric=rule.metric,
                    metric_value=float(metric_value),
                    action=event_action,
                    level=rule.level,
                    detail=f"{rule.metric}={metric_value:.4f}",
                )
                db.add(event)
                if state.trading_paused:
                    state.trading_paused = False
                    state.pause_reason = ""
                db.commit()

            return metric_map
        finally:
            db.close()

    def evaluate_intraday(self) -> Dict[str, float]:
        db = SessionLocal()
        try:
            state = self._get_or_create_state(db)
            account = db.query(Account).first()
            if not account:
                return {"intraday_drawdown": 0.0}
            assets = float(account.total_assets or 0.0)
            if state.intraday_peak_assets <= 0:
                state.intraday_peak_assets = assets
            if assets > state.intraday_peak_assets:
                state.intraday_peak_assets = assets
            drawdown = 0.0
            if state.intraday_peak_assets > 0:
                drawdown = (state.intraday_peak_assets - assets) / state.intraday_peak_assets
            state.intraday_drawdown = drawdown
            if drawdown >= 0.15:
                event = RewardPunishEvent(
                    rule_id=0,
                    rule_name="intraday_drawdown_15",
                    event_date=datetime.date.today(),
                    metric="max_drawdown",
                    metric_value=drawdown,
                    action="ALERT",
                    level="HIGH",
                    detail=f"intraday_drawdown={drawdown:.4f}",
                )
                db.add(event)
                if state.trading_paused:
                    state.trading_paused = False
                    state.pause_reason = ""
            db.commit()
            return {"intraday_drawdown": drawdown}
        finally:
            db.close()

    def is_trading_paused(self) -> bool:
        db = SessionLocal()
        try:
            state = db.query(RewardPunishState).first()
            if state and state.trading_paused:
                state.trading_paused = False
                state.pause_reason = ""
                db.commit()
            return False
        finally:
            db.close()

    def get_summary(self) -> Dict[str, object]:
        now_ts = time.time()
        with self._summary_lock:
            cached = dict(self._summary_cache) if self._summary_cache else None
            cached_ts = self._summary_cache_ts
        if cached and (now_ts - cached_ts) < self._summary_cache_ttl:
            cached["trading_paused"] = False
            cached["pause_reason"] = ""
            return dict(cached)
        if cached and (now_ts - cached_ts) < self._summary_cache_ttl * 6:
            threading.Thread(target=self._refresh_summary, daemon=True).start()
            cached["trading_paused"] = False
            cached["pause_reason"] = ""
            return dict(cached)
        db = SessionLocal()
        try:
            state = self._get_or_create_state(db)
            metrics = self._calc_metrics(db)
            events = (
                db.query(RewardPunishEvent)
                .order_by(RewardPunishEvent.created_at.desc())
                .limit(20)
                .all()
            )
            summary = {
                "metrics": metrics.__dict__,
                "trading_paused": False,
                "pause_reason": "",
                "recent_events": [
                    {
                        "id": e.id,
                        "rule_name": e.rule_name,
                        "metric": e.metric,
                        "value": e.metric_value,
                        "action": e.action,
                        "level": e.level,
                        "status": e.status,
                        "date": e.event_date.isoformat(),
                    }
                    for e in events
                ],
            }
            with self._summary_lock:
                self._summary_cache = summary
                self._summary_cache_ts = now_ts
            return dict(summary)
        finally:
            db.close()

    def submit_appeal(self, event_id: int, reason: str) -> int:
        db = SessionLocal()
        try:
            appeal = RewardPunishAppeal(event_id=event_id, reason=reason)
            db.add(appeal)
            db.commit()
            db.refresh(appeal)
            return appeal.id
        finally:
            db.close()

    def review_appeal(self, appeal_id: int, approve: bool, reviewer: str, note: str) -> bool:
        db = SessionLocal()
        try:
            appeal = db.query(RewardPunishAppeal).filter(RewardPunishAppeal.id == appeal_id).first()
            if not appeal:
                return False
            appeal.status = "APPROVED" if approve else "REJECTED"
            appeal.reviewer = reviewer
            appeal.review_note = note
            appeal.reviewed_at = datetime.datetime.now()
            if approve:
                event = db.query(RewardPunishEvent).filter(RewardPunishEvent.id == appeal.event_id).first()
                if event and event.action in ["PAUSE_TRADING", "STRONG_PENALTY"]:
                    state = self._get_or_create_state(db)
                    state.trading_paused = False
                    state.pause_reason = ""
                    event.status = "RESOLVED"
                    event.resolved_at = datetime.datetime.now()
            db.commit()
            return True
        finally:
            db.close()


reward_punish_service = RewardPunishService()
