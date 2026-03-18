import asyncio
import time
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from typing import List, Optional, Dict, Any, cast
from datetime import date, datetime, timedelta
from pydantic import BaseModel

from app.services.trading_service import trading_service
from app.services.plan_event_service import plan_event_service
from app.services.reward_punish_service import reward_punish_service
from app.services.review_service import review_service
from app.services.logger import logger, selector_logger
from app.models.stock_models import Account, Position, TradeRecord, MarketSentiment, TradingPlan, Stock
from app.repositories.trading_repository import TradingRepository
from app.core.config import settings
from app.db.session import SessionLocal
import json

router = APIRouter()

def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

# --- Pydantic Models ---

class PlanCreateRequest(BaseModel):
    ts_code: str
    strategy_name: str
    buy_price: float
    stop_loss: float
    take_profit: float
    position_pct: float = 0.1
    reason: str = ""
    score: Optional[float] = None
    source: str = "user"

class PlanUpdateRequest(BaseModel):
    strategy_name: Optional[str] = None
    buy_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    position_pct: Optional[float] = None
    reason: Optional[str] = None
    score: Optional[float] = None

class PlanCancelRequest(BaseModel):
    reason: Optional[str] = None

class PlanResponse(BaseModel):
    id: int
    date: date
    ts_code: str
    strategy_name: str
    executed: bool
    track_status: Optional[str] = None
    score: Optional[float]
    entry_price: Optional[float] = None
    review_content: Optional[str] = None
    decision_price: Optional[float] = None
    buy_price_limit: Optional[float] = None
    plan_price: Optional[float] = None
    created_at: Optional[datetime] = None
    reason: Optional[str] = None
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    source: Optional[str] = "system"

def _plan_to_response(plan: TradingPlan, review_content: Optional[str] = None) -> PlanResponse:
    plan_id = plan.id
    if plan_id is None:
        raise HTTPException(status_code=500, detail="计划缺少ID")
    plan_date = plan.date
    if plan_date is None:
        raise HTTPException(status_code=500, detail="计划缺少日期")
    ts_code = plan.ts_code
    if not ts_code:
        raise HTTPException(status_code=500, detail="计划缺少股票代码")
    strategy_name = plan.strategy_name
    if not strategy_name:
        raise HTTPException(status_code=500, detail="计划缺少策略名称")
    executed = plan.executed
    if executed is None:
        raise HTTPException(status_code=500, detail="计划缺少执行状态")
    plan_price = plan.limit_price or plan.buy_price_limit
    
    # [Fix] For display purposes, if plan_price is 0 (e.g. watch-only plans), 
    # fallback to decision_price so the UI doesn't show "-"
    if (not plan_price or plan_price <= 0) and plan.decision_price and plan.decision_price > 0:
        plan_price = plan.decision_price
        
    return PlanResponse(
        id=plan_id,
        date=plan_date,
        ts_code=ts_code,
        strategy_name=strategy_name,
        executed=executed,
        track_status=plan.track_status,
        score=_to_float(plan.score),
        entry_price=_to_float(plan.entry_price),
        review_content=review_content if review_content is not None else plan.review_content,
        decision_price=_to_float(plan.decision_price),
        buy_price_limit=_to_float(plan.buy_price_limit),
        plan_price=_to_float(plan_price),
        created_at=plan.created_at,
        reason=plan.reason,
        stop_loss_price=_to_float(plan.stop_loss_price),
        take_profit_price=_to_float(plan.take_profit_price),
        source=plan.source,
    )

class ReviewRequest(BaseModel):
    review_date: Optional[str] = None
    watchlist: Optional[List[str]] = []
    async_mode: bool = True
    preferred_provider: Optional[str] = None
    api_key: Optional[str] = None

class TargetPlan(BaseModel):
    ts_code: str
    strategy: str
    reason: str
    position_pct: Optional[float] = None
    score: Optional[float] = None
    target_price: Optional[float] = None
    action: Optional[str] = None

class SentimentResponse(BaseModel):
    date: date
    up_count: int
    down_count: int
    limit_up_count: int
    limit_down_count: int = 0
    total_volume: float = 0.0
    market_temperature: float
    highest_plate: int = 0
    ladder: Optional[Dict[str, Any]] = None
    turnover_top: Optional[List[Dict[str, Any]]] = None
    ladder_opportunities: Optional[List[Dict[str, Any]]] = None
    summary: str
    main_theme: Optional[str] = None
    target_plan: Optional[TargetPlan] = None
    target_plans: Optional[List[TargetPlan]] = None
    holding_plans: Optional[List[TargetPlan]] = None
    created_at: Optional[datetime] = None

class AccountInfo(BaseModel):
    total_assets: float
    available_cash: float
    frozen_cash: float
    market_value: float
    total_pnl: float
    total_pnl_pct: float
    updated_at: datetime

class PositionInfo(BaseModel):
    ts_code: str
    name: str
    vol: int
    available_vol: int
    avg_price: float
    current_price: float
    market_value: float
    float_pnl: float
    pnl_pct: float

class TradeRecordInfo(BaseModel):
    ts_code: str
    name: str
    trade_type: str
    price: float
    vol: int
    amount: float
    fee: float
    trade_time: datetime
    pnl_pct: Optional[float] = None

class EntrustmentInfo(BaseModel):
    id: int
    ts_code: str
    name: str
    strategy_name: str
    order_type: str
    action: str # BUY, SELL, HOLD
    limit_price: float
    target_vol: Optional[int] = None
    target_price: Optional[float] = None
    executed_vol: Optional[int] = None
    executed_price: Optional[float] = None
    current_price: Optional[float] = 0.0
    frozen_amount: Optional[float] = 0.0
    frozen_vol: Optional[int] = 0
    required_amount: Optional[float] = 0.0
    required_vol: Optional[int] = 0
    status: str
    created_at: datetime
    review_content: Optional[str] = None
    warning: Optional[str] = None

class TradingDashboardResponse(BaseModel):
    account: AccountInfo
    positions: List[PositionInfo]
    records: List[TradeRecordInfo]
    entrustments: List[EntrustmentInfo]

# --- Endpoints ---

class DailyPerformanceResponse(BaseModel):
    date: date
    total_assets: float
    daily_pnl: float
    daily_pnl_pct: float
    total_pnl: float
    total_pnl_pct: float

@router.get("/equity-curve", response_model=List[DailyPerformanceResponse])
async def get_equity_curve():
    """获取资金曲线数据"""
    db = SessionLocal()
    try:
        from app.models.stock_models import DailyPerformance
        performances = db.query(DailyPerformance).order_by(DailyPerformance.date.asc()).all()
        return performances
    finally:
        db.close()

@router.get("/account", response_model=AccountInfo)
async def get_account_info():
    """获取账户资产信息"""
    db = SessionLocal()
    try:
        # 确保账户存在
        account = await trading_service._get_or_create_account(db)
        if not account:
             raise HTTPException(status_code=404, detail="Account not found")
        
        return AccountInfo(
            total_assets=account.total_assets,
            available_cash=account.available_cash,
            frozen_cash=account.frozen_cash,
            market_value=account.market_value,
            total_pnl=account.total_pnl,
            total_pnl_pct=account.total_pnl_pct,
            updated_at=account.updated_at or datetime.now()
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching account info: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@router.get("/positions", response_model=List[PositionInfo])
async def get_positions():
    """获取当前持仓列表"""
    def _do_query():
        db = SessionLocal()
        try:
            positions = db.query(Position).filter(Position.vol > 0).all()
            return [
                PositionInfo(
                    ts_code=p.ts_code,
                    name=p.name,
                    vol=p.vol,
                    available_vol=p.available_vol,
                    avg_price=p.avg_price,
                    current_price=p.current_price,
                    market_value=p.market_value,
                    float_pnl=p.float_pnl,
                    pnl_pct=p.pnl_pct
                ) for p in positions
            ]
        finally:
            db.close()
            
    return await asyncio.to_thread(_do_query)

@router.get("/records", response_model=List[TradeRecordInfo])
async def get_trade_records(limit: int = 50):
    """获取最近交易记录"""
    def _do_query():
        db = SessionLocal()
        try:
            records = db.query(TradeRecord).order_by(TradeRecord.trade_time.desc()).limit(limit).all()
            return [
                TradeRecordInfo(
                    ts_code=r.ts_code,
                    name=r.name,
                    trade_type=r.trade_type,
                    price=r.price,
                    vol=r.vol,
                    amount=r.amount,
                    fee=r.fee,
                    trade_time=r.trade_time,
                    pnl_pct=r.pnl_pct
                ) for r in records
            ]
        finally:
            db.close()
            
    return await asyncio.to_thread(_do_query)

@router.get("/entrustments", response_model=List[EntrustmentInfo])
async def get_entrustments(days: int = 7):
    """获取今日及历史委托计划并进行三方一致性审计"""
    await trading_service.repair_duplicate_plans_today()
    await trading_service.repair_orphan_trade_records(days)
    # 1. 预先获取实时行情，用于显示当前价
    db = SessionLocal()
    try:
        today = date.today()
        start_date = today - timedelta(days=days)
        plans = db.query(TradingPlan).filter(TradingPlan.date >= start_date).all()
        records = db.query(TradeRecord).filter(TradeRecord.trade_time >= datetime.combine(start_date, datetime.min.time())).all()
        ts_codes = list(
            set([p.ts_code for p in plans if p.ts_code] + [r.ts_code for r in records if r.ts_code])
        )
        norm_codes = [data_provider._normalize_ts_code(c) for c in ts_codes]
        try:
            quotes = await asyncio.wait_for(
                data_provider.get_realtime_quotes(norm_codes),
                timeout=8.0
            )
        except asyncio.TimeoutError:
            logger.warning("Trading dashboard quote fetch timed out, fallback to local quotes.")
            quotes = await data_provider.get_realtime_quotes(norm_codes, local_only=True)
        except Exception as e:
            logger.warning(f"Trading dashboard quote fetch failed: {e}")
            quotes = await data_provider.get_realtime_quotes(norm_codes, local_only=True)
        is_trading = data_provider.is_trading_time()
        is_after_close = data_provider.is_after_market_close()
    finally:
        db.close()

    def _do_query(quotes_data, trading_now, after_close):
        db = SessionLocal()
        try:
            today = date.today()
            start_date = today - timedelta(days=days)
            # 0. 获取账户资产 (用于计算预计委托量)
            account = db.query(Account).first()
            total_assets = account.total_assets if account else settings.INITIAL_CAPITAL
            available_cash = float(account.available_cash) if account else settings.INITIAL_CAPITAL

            # 1. 获取指定天数内的所有计划 (委托)
            plans = db.query(TradingPlan).filter(TradingPlan.date >= start_date).order_by(TradingPlan.date.desc(), TradingPlan.created_at.desc()).all()
            
            # 2. 获取当前持仓状态 (持仓)
            positions = db.query(Position).filter(Position.vol > 0).all()
            pos_dict = {p.ts_code: p for p in positions}
            
            # 3. 获取相关日期的成交记录 (成交)
            records = db.query(TradeRecord).filter(TradeRecord.trade_time >= datetime.combine(start_date, datetime.min.time())).all()
            
            # 建立 record 索引：plan_id -> records 和 ts_code -> records
            records_by_plan: Dict[int, List[TradeRecord]] = {}
            for r in records:
                if r.plan_id:
                    records_by_plan.setdefault(r.plan_id, []).append(r)
            records_by_code: Dict[str, List[TradeRecord]] = {}
            for r in records:
                if r.ts_code not in records_by_code:
                    records_by_code[r.ts_code] = []
                records_by_code[r.ts_code].append(r)
            
            def _calc_total_cost(v: int, price: float) -> float:
                need_cash = float(v) * float(price)
                fee = max(5.0, need_cash * 0.00025)
                return need_cash + fee

            buy_entrustments: Dict[tuple[date, str], int] = {}
            results = []
            plan_ids = set(p.id for p in plans)
            
            for p in plans:
                if not p.ts_code:
                    logger.warning(f"Skip entrustment plan {p.id}: missing ts_code")
                    continue
                if (p.track_status or "").upper() in {"CANCELLED", "FINISHED"}:
                    continue
                stock = db.query(Stock).filter(Stock.ts_code == p.ts_code).first()
                name = (stock.name or p.ts_code) if stock else p.ts_code
                
                # 获取实时价格
                norm_code = data_provider._normalize_ts_code(p.ts_code)
                quote = quotes_data.get(norm_code) or quotes_data.get(p.ts_code, {})
                current_price = float(quote.get('price', 0))

                action = TradingRepository._infer_plan_action(p)

                plan_records = records_by_plan.get(p.id, [])
                record = None
                if plan_records:
                    if action == "SELL":
                        record = next((x for x in reversed(plan_records) if (x.trade_type or "").upper() == "SELL"), None)
                    elif action == "BUY":
                        record = next((x for x in reversed(plan_records) if (x.trade_type or "").upper() == "BUY"), None)
                    if record is None:
                        record = plan_records[-1]

                # --- 基础状态判定 ---
                status = "待成交"
                # 获取相关记录用于状态判定
                executed_vol = record.vol if record else None
                executed_price = record.price if record else None
                
                frozen_amount_val = float(p.frozen_amount or 0.0)
                frozen_vol_val = int(p.frozen_vol or 0)

                is_cancelled = (p.track_status or "").upper() == "CANCELLED"
                if is_cancelled:
                    status = "已撤销"
                elif (p.track_status or "").upper() == "TRACKING":
                    status = "观察中"
                elif p.executed:
                    status = "已成交" if record else "成交异常"
                else:
                    # 只有委托里面才有废单，逻辑如下：
                    # 1. 计划日期是以前的，且未成交，且资金已解冻
                    # 2. 计划日期是今天的，但已收盘，且未成交，且资金已解冻
                    is_past_date = p.date < today
                    is_after_market = p.date == today and after_close
                    
                    if (is_past_date or is_after_market) and frozen_amount_val == 0:
                        status = "废单"
                    elif frozen_amount_val > 0:
                        # 资金已冻结，检查是否排队
                        status = "已报待成"
                        # 检查涨停排队：如果是买入，且当前价 >= 涨停价(近似)，或者有明确排队信息
                        if p.market_snapshot_json:
                            try:
                                snapshot = json.loads(p.market_snapshot_json)
                                if 'queue_info' in snapshot:
                                    status = "排队中"
                            except: pass
                        
                        # 简单判断涨停: 如果当前价 > 0 且 涨幅 > 9.5% 且未成交
                        if current_price > 0 and quote.get('pct_chg', 0) > 9.5:
                             status = "涨停排队"
                    else:
                        # 资金未冻结
                        status = "未冻结"

                if action == "SELL" and (not p.executed) and status == "废单" and (record is None) and (p.ts_code not in pos_dict):
                    continue

                # --- 三方一致性审计 (委托 vs 成交 vs 持仓) ---
                warnings = []
                
                # A. 委托与成交的一致性
                record = record
                if p.executed and (not record) and (not is_cancelled):
                    candidates = []
                    if p.created_at:
                        candidates = [
                            r for r in records_by_code.get(p.ts_code, [])
                            if abs((r.trade_time - p.created_at).total_seconds()) < 3600
                        ]
                    if len(candidates) == 1:
                        record = candidates[0]
                        warnings.append("成交记录缺少plan_id：已按时间近似匹配，建议补齐回写以消除歧义。")
                    else:
                        warnings.append("状态矛盾：计划标记为[已成交]，但数据库中缺失对应的[成交记录]。")
                
                if not p.executed and record:
                    warnings.append(f"数据错位：计划标记为[未成交]，但检测到关联的[成交记录] (ID: {record.id})。")

                frozen_amount_val = float(p.frozen_amount or 0.0)
                frozen_vol_val = int(p.frozen_vol or 0)
                if p.executed and frozen_amount_val > 0:
                    warnings.append("资金异常：计划已成交但仍存在冻结资金，占用资金未释放。")
                elif (not p.executed) and frozen_amount_val > 0:
                    ref_price = p.buy_price_limit or p.limit_price or 0.0
                    if ref_price and ref_price > 0:
                        if frozen_vol_val < 100:
                            warnings.append("冻结异常：已冻结资金但冻结数量不足100股，无法形成有效委托。")
                        else:
                            need_cash = frozen_vol_val * float(ref_price)
                            fee = max(5.0, need_cash * 0.00025)
                            expected_total = need_cash + fee
                            if abs(frozen_amount_val - expected_total) > max(1.0, expected_total * 0.002):
                                warnings.append(f"冻结不吻合：冻结{frozen_amount_val:.2f}，按{frozen_vol_val}股@{ref_price:.2f}含费应为{expected_total:.2f}。")
                    else:
                        warnings.append("冻结异常：已冻结资金但缺少有效委托参考价，无法核验。")

                # B. 时间轴逻辑审计
                if p.executed and record and p.created_at and record.trade_time:
                    # 允许 1 秒以内的微小误差（数据库存储精度可能导致）
                    if record.trade_time < p.created_at - timedelta(seconds=1):
                        warnings.append(f"时间序异常：成交时间 ({record.trade_time.strftime('%H:%M:%S')}) 早于 委托时间 ({p.created_at.strftime('%H:%M:%S')})，这在逻辑上是不可能的。")

                order_type_val = (p.order_type or "MARKET").upper()
                # 计算委托数量与价格
                target_price = p.buy_price_limit or p.limit_price
                executed_vol = record.vol if record else None
                executed_price = record.price if record else None
                
                planned_vol = None
                position_pct_val = float(p.position_pct or 0.0)
                if target_price and target_price > 0 and position_pct_val > 0 and total_assets > 0:
                    pv = int((total_assets * position_pct_val) / float(target_price) / 100) * 100
                    planned_vol = pv if pv >= 100 else None

                frozen_vol_val = int(p.frozen_vol or 0)
                frozen_amount_val = float(p.frozen_amount or 0.0)

                required_amount = 0.0
                required_vol = 0
                if action == "BUY":
                    required_vol = frozen_vol_val if frozen_vol_val >= 100 else (planned_vol or 0)
                    required_amount = _calc_total_cost(required_vol, float(target_price)) if (required_vol and required_vol >= 100 and target_price and target_price > 0) else 0.0

                # 优先展示冻结量，否则展示计划量
                target_vol = frozen_vol_val if frozen_vol_val >= 100 else planned_vol
                
                if action == "BUY":
                    if p.executed and record:
                        target_vol = record.vol
                        if order_type_val != "LIMIT":
                            target_price = record.price
                    if not p.executed:
                        # 检查是否在同一交易日内出现“计划生成滞后”
                        same_day_records = [r for r in records_by_code.get(p.ts_code, []) if r.trade_time.date() == p.date]
                        earlier_records = [r for r in same_day_records if (p.created_at and r.trade_time < p.created_at)]
                        
                        if earlier_records:
                            warnings.append(f"逻辑滞后：该计划生成前已有成交 ({earlier_records[0].trade_time.strftime('%H:%M:%S')})，请核实是否重复生成。")
                        
                        if p.ts_code in pos_dict and "做T" not in (p.strategy_name or ""):
                            warnings.append(f"持仓冲突：当前已持有该股 ({pos_dict[p.ts_code].vol}股)，重复买入将增加风险。")

                        has_frozen = frozen_amount_val > 0 and frozen_vol_val >= 100
                        key = (p.date, p.ts_code)
                        if has_frozen:
                            if key in buy_entrustments:
                                warnings.append(f"逻辑冲突：同日存在多个买入委托 (ID: {buy_entrustments[key]})。")
                            else:
                                buy_entrustments[key] = p.id

                        if (not has_frozen) and required_amount > 0:
                            warnings.append(f"未冻结：预计需 {required_amount:.2f}，当前可用 {available_cash:.2f}。")
                
                elif action == "SELL":
                    if p.executed and record:
                        target_vol = record.vol
                    elif p.ts_code in pos_dict:
                        target_vol = pos_dict[p.ts_code].vol
                    
                    if not p.executed:
                        if p.ts_code not in pos_dict:
                            warnings.append("超卖冲突：当前账户未持有该股，无法执行卖出委托。")
                        else:
                            pos = pos_dict[p.ts_code]
                            if pos.available_vol <= 0:
                                warnings.append(f"T+1限制：当前可用数量为 0 (总持仓 {pos.vol}股 为今日买入)，无法卖出。")
                
                elif action == "HOLD":
                    continue # 不显示“观察中”的计划，仅显示买入/卖出委托

                if action == "BUY" and (not p.executed) and float(p.frozen_amount or 0.0) <= 0:
                    continue
                
                # if action == "BUY" and (not p.executed) and status in ["待成交", "排队中"] and frozen_amount_val <= 0:
                #    continue

                # D. 持仓量与成交记录的汇总校验 (仅对今日变动的股票)
                if p.ts_code in pos_dict or p.ts_code in records_by_code:
                    # 获取该股票的所有历史成交记录（用于计算最终应有持仓）
                    all_records = db.query(TradeRecord).filter(TradeRecord.ts_code == p.ts_code).all()
                    calc_vol = 0
                    for r in all_records:
                        trade_type = (r.trade_type or "").upper()
                        if trade_type == "BUY":
                            calc_vol += int(r.vol or 0)
                        elif trade_type == "SELL":
                            calc_vol -= int(r.vol or 0)
                    current_vol = pos_dict[p.ts_code].vol if p.ts_code in pos_dict else 0
                    
                    if calc_vol != current_vol:
                        warnings.append(f"账实不符：成交记录累计持仓 ({calc_vol}股) 与 当前持仓表 ({current_vol}股) 不一致。")

                display_review = p.review_content
                if (p.track_status or "").upper() == "TRACKING":
                    display_review = display_review or (p.reason or "") or "AI监控入池"
                results.append(EntrustmentInfo(
                    id=p.id,
                    ts_code=p.ts_code,
                    name=name,
                    strategy_name=p.strategy_name or "",
                    order_type=order_type_val,
                    action=action,
                    limit_price=(p.limit_price or p.buy_price_limit or 0.0) if order_type_val == "LIMIT" else (p.buy_price_limit or p.limit_price or 0.0),
                    target_vol=target_vol,
                    target_price=target_price,
                    executed_vol=executed_vol,
                    executed_price=executed_price,
                    current_price=current_price,
                    frozen_amount=p.frozen_amount or 0.0,
                    frozen_vol=p.frozen_vol or 0,
                    required_amount=required_amount,
                    required_vol=required_vol,
                    status=status,
                    created_at=p.created_at,
                    review_content=display_review,
                    warning=" | ".join(warnings) if warnings else None
                ))

            referenced_plan_ids = {r.plan_id for r in records if r.plan_id}
            existing_plan_ids = set()
            if referenced_plan_ids:
                existing_plan_ids = set(
                    pid for (pid,) in db.query(TradingPlan.id).filter(TradingPlan.id.in_(list(referenced_plan_ids))).all()
                )

            orphan_records = [r for r in records if (not r.plan_id) or (r.plan_id not in existing_plan_ids)]
            for r in orphan_records:
                norm_code = data_provider._normalize_ts_code(r.ts_code)
                quote = quotes_data.get(norm_code) or quotes_data.get(r.ts_code, {})
                current_price = float(quote.get('price', 0))
                action = "BUY" if (r.trade_type or "").upper() == "BUY" else "SELL"
                results.append(EntrustmentInfo(
                    id=-int(r.id),
                    ts_code=r.ts_code,
                    name=r.name or r.ts_code,
                    strategy_name="(无委托)成交",
                    order_type="MARKET",
                    action=action,
                    limit_price=0.0,
                    target_vol=r.vol,
                    target_price=r.price,
                    executed_vol=r.vol,
                    executed_price=r.price,
                    current_price=current_price,
                    frozen_amount=0.0,
                    frozen_vol=0,
                    required_amount=0.0,
                    required_vol=0,
                    status="已成交",
                    created_at=r.trade_time,
                    review_content=None,
                    warning="成交记录缺少plan_id"
                ))
            return results
        finally:
            db.close()
            
    return await asyncio.to_thread(_do_query, quotes, is_trading, is_after_close)

@router.post("/sync_assets")
async def sync_assets():
    """手动触发资产同步"""
    await trading_service.unfreeze_all_expired_plans()
    await trading_service.sync_account_assets()
    return {"status": "ok"}

@router.post("/refresh", response_model=TradingDashboardResponse)
async def refresh_trading_dashboard(days: int = 7):
    await trading_service.unfreeze_all_expired_plans()
    db = SessionLocal()
    try:
        account = await trading_service._get_or_create_account(db)
        today = date.today()
        start_date = today - timedelta(days=days)
        positions: List[Position] = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())
        plans: List[TradingPlan] = await asyncio.to_thread(lambda: db.query(TradingPlan).filter(TradingPlan.date >= start_date).all())
        records: List[TradeRecord] = await asyncio.to_thread(lambda: db.query(TradeRecord).filter(TradeRecord.trade_time >= datetime.combine(start_date, datetime.min.time())).order_by(TradeRecord.trade_time.desc()).limit(50).all())
        
        union_codes = list(set([pos.ts_code for pos in positions if pos.ts_code] + [r.ts_code for r in records if r.ts_code] + [plan.ts_code for plan in plans if plan.ts_code]))
        norm_codes = [data_provider._normalize_ts_code(c) for c in union_codes]
        quotes = await data_provider.get_realtime_quotes(norm_codes)
        
        await trading_service.sync_account_assets(quotes_override=quotes)
        await asyncio.to_thread(db.refresh, account)
        positions = await asyncio.to_thread(lambda: db.query(Position).filter(Position.vol > 0).all())

        positions_out: List[PositionInfo] = []
        total_market_value = 0.0
        for pos in positions:
            current_price = float(pos.current_price or 0.0)
            mv = float(pos.market_value or 0.0)
            if mv <= 0 and current_price > 0:
                mv = float(pos.vol or 0) * current_price
            cost = float(pos.vol or 0) * float(pos.avg_price or 0.0)
            float_pnl = float(pos.float_pnl or (mv - cost))
            pnl_pct = float(pos.pnl_pct or ((float_pnl / cost * 100) if cost > 0 else 0.0))
            total_market_value += mv
            positions_out.append(PositionInfo(
                ts_code=pos.ts_code,
                name=pos.name,
                vol=pos.vol,
                available_vol=pos.available_vol,
                avg_price=pos.avg_price,
                current_price=current_price,
                market_value=mv,
                float_pnl=float_pnl,
                pnl_pct=pnl_pct
            ))
        
        def _infer_action(plan: TradingPlan) -> str:
            return TradingRepository._infer_plan_action(plan)
        
        records_by_plan: Dict[int, List[TradeRecord]] = {}
        records_by_code: Dict[str, List[TradeRecord]] = {}
        for r in records:
            if r.plan_id:
                records_by_plan.setdefault(r.plan_id, []).append(r)
            if r.ts_code:
                records_by_code.setdefault(r.ts_code, []).append(r)

        def _calc_total_cost(v: int, price: float) -> float:
            need_cash = float(v) * float(price)
            fee = max(5.0, need_cash * 0.00025)
            return need_cash + fee

        total_assets_val = float(account.total_assets or settings.INITIAL_CAPITAL)

        entrustments_out: List[EntrustmentInfo] = []
        for plan in plans:
            if not plan.ts_code:
                logger.warning(f"Skip dashboard plan {plan.id}: missing ts_code")
                continue
            if (plan.track_status or "").upper() == "CANCELLED":
                continue
            stock = await asyncio.to_thread(lambda: db.query(Stock).filter(Stock.ts_code == plan.ts_code).first())
            name = (stock.name or plan.ts_code) if stock else plan.ts_code
            norm_code = data_provider._normalize_ts_code(plan.ts_code)
            quote = quotes.get(norm_code) or {}
            current_price = float(quote.get("price", 0) or 0.0)
            action = _infer_action(plan)
            plan_records = records_by_plan.get(plan.id, [])
            record = None
            if plan_records:
                if action == "SELL":
                    record = next((x for x in reversed(plan_records) if (x.trade_type or "").upper() == "SELL"), None)
                elif action == "BUY":
                    record = next((x for x in reversed(plan_records) if (x.trade_type or "").upper() == "BUY"), None)
                if record is None:
                    record = plan_records[-1]

            status = "待成交"
            if (plan.track_status or "").upper() == "TRACKING":
                status = "观察中"
            elif plan.executed:
                status = "已成交" if record else "成交异常"

            order_type_val = (plan.order_type or "MARKET").upper()
            target_price = plan.buy_price_limit or plan.limit_price
            executed_vol = record.vol if record else None
            executed_price = record.price if record else None

            planned_vol = None
            position_pct_val = float(plan.position_pct or 0.0)
            if target_price and target_price > 0 and position_pct_val > 0 and total_assets_val > 0:
                pv = int((total_assets_val * position_pct_val) / float(target_price) / 100) * 100
                planned_vol = pv if pv >= 100 else None

            frozen_vol_val = int(plan.frozen_vol or 0)
            frozen_amount_val = float(plan.frozen_amount or 0.0)

            required_amount = 0.0
            required_vol = 0
            if action == "BUY":
                required_vol = frozen_vol_val if frozen_vol_val >= 100 else (planned_vol or 0)
                required_amount = _calc_total_cost(required_vol, float(target_price)) if (required_vol and required_vol >= 100 and target_price and target_price > 0) else 0.0

            target_vol = frozen_vol_val if frozen_vol_val >= 100 else planned_vol
            if action == "BUY":
                if plan.executed and record:
                    target_vol = record.vol
                    if order_type_val != "LIMIT":
                        target_price = record.price
            elif action == "SELL":
                if plan.executed and record:
                    target_vol = record.vol
            elif action == "HOLD":
                continue

            if action == "BUY" and (not plan.executed) and frozen_amount_val <= 0:
                continue

            entrustments_out.append(EntrustmentInfo(
                id=plan.id,
                ts_code=plan.ts_code,
                name=name,
                strategy_name=plan.strategy_name or "",
                order_type=order_type_val,
                action=action,
                limit_price=(plan.limit_price or plan.buy_price_limit or 0.0) if order_type_val == "LIMIT" else (plan.buy_price_limit or plan.limit_price or 0.0),
                target_vol=target_vol,
                target_price=target_price,
                executed_vol=executed_vol,
                executed_price=executed_price,
                current_price=current_price,
                frozen_amount=frozen_amount_val,
                frozen_vol=frozen_vol_val,
                required_amount=required_amount,
                required_vol=required_vol,
                status=status,
                created_at=plan.created_at,
                review_content=plan.review_content,
                warning=None
            ))
        
        account_out = AccountInfo(
            total_assets=float(account.total_assets or 0.0),
            available_cash=float(account.available_cash or 0.0),
            frozen_cash=float(account.frozen_cash or 0.0),
            market_value=float(account.market_value or total_market_value),
            total_pnl=float(account.total_pnl or 0.0),
            total_pnl_pct=float(account.total_pnl_pct or 0.0),
            updated_at=account.updated_at or datetime.now()
        )
        
        records_out = [
            TradeRecordInfo(
                ts_code=r.ts_code,
                name=r.name,
                trade_type=r.trade_type,
                price=r.price,
                vol=r.vol,
                amount=r.amount,
                fee=r.fee,
                trade_time=r.trade_time,
                pnl_pct=r.pnl_pct
            ) for r in records
        ]
        
        return TradingDashboardResponse(
            account=account_out,
            positions=positions_out,
            records=records_out,
            entrustments=entrustments_out
        )
    finally:
        db.close()

@router.post("/settle")
async def settle_positions():
    """手动触发持仓结算 (T+1可用性更新)"""
    await trading_service.settle_positions()
    return {"status": "ok", "message": "Positions settled (available_vol updated)"}


from app.services.data_provider import data_provider

@router.get("/snapshot")
async def get_market_snapshot():
    # 必须 await
    return await data_provider.get_market_snapshot()

@router.post("/review/daily", response_model=SentimentResponse)
async def trigger_daily_review(background_tasks: BackgroundTasks, request: ReviewRequest = ReviewRequest()):
    """
    手动触发今日复盘 (计算情绪 + 生成总结 + 评估自选股)
    """
    review_date = request.review_date
    watchlist = request.watchlist
    
    target_date = None
    if review_date:
        try:
            target_date = datetime.strptime(review_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    else:
        # 默认使用最近一个有数据的交易日
        latest_str = await data_provider.get_last_trade_date(include_today=True)
        if latest_str:
            target_date = datetime.strptime(latest_str, "%Y%m%d").date()
        else:
            target_date = date.today()
    
    # 智能判断：如果是今天且在午间时间段（11:00-15:00），尝试午间复盘逻辑
    current_time = datetime.now()
    is_noon_time = 11 <= current_time.hour < 15
    
    # 如果未指定日期，且是午间，强制尝试今天
    if not review_date and is_noon_time:
        target_date = date.today()
    
    is_today = target_date == date.today()
    
    async def _save_placeholder(stats_dict: dict, review_dt: date) -> dict:
        temperature = float(review_service._calculate_market_temperature(stats_dict or {}, {"highest": 0}) or 50.0)

        def _upsert():
            db = SessionLocal()
            try:
                existing = db.query(MarketSentiment).filter(MarketSentiment.date == review_dt).order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc()).first()
                now_ts = datetime.now()
                up_count = int((stats_dict or {}).get("up", 0) or 0)
                down_count = int((stats_dict or {}).get("down", 0) or 0)
                limit_up_count = int((stats_dict or {}).get("limit_up", 0) or 0)
                limit_down_count = int((stats_dict or {}).get("limit_down", 0) or 0)
                total_volume = float((stats_dict or {}).get("total_volume", 0.0) or 0.0)

                if existing:
                    existing.up_count = up_count
                    existing.down_count = down_count
                    existing.limit_up_count = limit_up_count
                    existing.limit_down_count = limit_down_count
                    existing.total_volume = total_volume
                    existing.market_temperature = temperature
                    existing.main_theme = "生成中"
                    existing.summary = "复盘任务已启动，后台生成中…"
                    existing.updated_at = now_ts
                    db.commit()
                    db.refresh(existing)
                    created_at = existing.updated_at
                else:
                    sentiment = MarketSentiment(
                        date=review_dt,
                        up_count=up_count,
                        down_count=down_count,
                        limit_up_count=limit_up_count,
                        limit_down_count=limit_down_count,
                        total_volume=total_volume,
                        market_temperature=temperature,
                        highest_plate=0,
                        main_theme="生成中",
                        summary="复盘任务已启动，后台生成中…",
                        updated_at=now_ts,
                    )
                    db.add(sentiment)
                    db.commit()
                    db.refresh(sentiment)
                    created_at = sentiment.updated_at
                return {
                    "date": review_dt.strftime("%Y-%m-%d"),
                    "temp": temperature,
                    "created_at": created_at,
                    "highest_plate": 0,
                    "summary": "复盘任务已启动，后台生成中…",
                    "main_theme": "生成中",
                    "target_plan": None,
                    "target_plans": [],
                    "holding_plans": [],
                    "up": up_count,
                    "down": down_count,
                    "limit_up": limit_up_count,
                    "limit_down": limit_down_count,
                    "total_volume": total_volume,
                }
            finally:
                db.close()

        return await asyncio.to_thread(_upsert)

    async def _run_full_review_async():
        try:
            if is_today and is_noon_time:
                await review_service.perform_noon_review(watchlist, preferred_provider=request.preferred_provider, api_key=request.api_key)
            else:
                await review_service.perform_daily_review(target_date, watchlist, preferred_provider=request.preferred_provider, api_key=request.api_key)
        except Exception as e:
            logger.error(f"Background review failed: {e}", exc_info=True)

    result = None
    if bool(getattr(request, "async_mode", True)):
        logger.info(f"Enqueueing review task for {target_date} (noon={is_today and is_noon_time})")
        stats_for_placeholder = await data_provider.get_market_snapshot(target_date)
        result = await _save_placeholder(stats_for_placeholder, target_date)
        background_tasks.add_task(_run_full_review_async)
    else:
        if is_today and is_noon_time:
            logger.info(f"Triggering intraday (noon) review for {target_date}...")
            result = await review_service.perform_noon_review(watchlist, preferred_provider=request.preferred_provider, api_key=request.api_key)

        if not result:
            logger.info(f"Triggering daily review for {target_date}...")
            result = await review_service.perform_daily_review(target_date, watchlist, preferred_provider=request.preferred_provider, api_key=request.api_key)

        if not result:
            if is_today and not result:
                result = await review_service.perform_noon_review(watchlist, preferred_provider=request.preferred_provider, api_key=request.api_key)

        if not result:
            raise HTTPException(status_code=400, detail="Review failed or no data available")

    # 适配并清理数据，防止 NaN/Inf 导致序列化失败
    import math
    def clean_val(obj):
        if isinstance(obj, dict):
            return {k: clean_val(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_val(i) for i in obj]
        elif isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj): return 0.0
            return round(obj, 2)
        return obj

    cleaned_result = clean_val(result)

    return SentimentResponse(
        date=cleaned_result['date'],
        up_count=cleaned_result.get('up', 0),
        down_count=cleaned_result.get('down', 0),
        limit_up_count=cleaned_result.get('limit_up', 0),
        limit_down_count=cleaned_result.get('limit_down', 0),
        total_volume=cleaned_result.get('total_volume', 0.0),
        market_temperature=cleaned_result.get('temp', 50.0),
        highest_plate=int(cleaned_result.get('highest_plate', 0) or 0),
        ladder=cleaned_result.get('ladder'),
        turnover_top=cleaned_result.get('turnover_top'),
        ladder_opportunities=cleaned_result.get('ladder_opportunities'),
        summary=cleaned_result.get('summary', ""),
        main_theme=cleaned_result.get('main_theme', ""),
        target_plan=cleaned_result.get('target_plan'),
        target_plans=cleaned_result.get('target_plans') or [],
        holding_plans=cleaned_result.get('holding_plans') or [],
        created_at=cleaned_result.get('created_at') or datetime.now()
    )

@router.get("/review/latest", response_model=SentimentResponse)
async def get_latest_review(review_date: Optional[str] = None):
    """
    获取最新或指定日期的复盘结果
    如果未指定日期，则返回数据库中最新生成的一条复盘记录（基于 ID 倒序）
    """
    target_date = None
    if review_date:
        try:
            target_date = datetime.strptime(review_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # 修改逻辑：如果有指定日期，按日期查；否则查最新的记录
    if target_date:
        result = await review_service.get_review_result(target_date)
    else:
        # 获取最新的一条记录，直接从数据库取，不依赖日期过滤(防止同一天多条记录返回旧的)
        def _get_latest_sentiment():
            db = SessionLocal()
            try:
                return db.query(MarketSentiment).order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc()).first()
            finally:
                db.close()
        
        latest_sentiment = await asyncio.to_thread(_get_latest_sentiment)
        if latest_sentiment:
            # 既然已经拿到了最新的 sentiment 对象，我们只需要补全它的 plan 数据
            # 这里直接复用 get_review_result 逻辑更稳妥，因为它现在已经支持按 ID 倒序了
            result = await review_service.get_review_result(latest_sentiment.date)
        else:
            result = None
    
    if not result:
        raise HTTPException(status_code=404, detail="No review data found")

    # 适配并清理数据，防止 NaN/Inf 导致序列化失败
    import math
    def clean_val(obj):
        if isinstance(obj, dict):
            return {k: clean_val(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_val(i) for i in obj]
        elif isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj): return 0.0
            return round(obj, 2)
        return obj

    cleaned_result = clean_val(result)

    return SentimentResponse(
        date=cleaned_result['date'],
        up_count=cleaned_result['up'],
        down_count=cleaned_result['down'],
        limit_up_count=cleaned_result['limit_up'],
        limit_down_count=cleaned_result.get('limit_down', 0),
        total_volume=cleaned_result.get('total_volume', 0.0),
        market_temperature=cleaned_result['temp'],
        highest_plate=int(cleaned_result.get('highest_plate', 0) or 0),
        ladder=cleaned_result.get('ladder'),
        turnover_top=cleaned_result.get('turnover_top'),
        ladder_opportunities=cleaned_result.get('ladder_opportunities'),
        summary=cleaned_result['summary'],
        main_theme=cleaned_result.get('main_theme', ""),
        target_plan=cleaned_result.get('target_plan'),
        target_plans=cleaned_result.get('target_plans') or [],
        holding_plans=cleaned_result.get('holding_plans'),
        created_at=cleaned_result.get('created_at') or datetime.now()
    )


@router.get("/review/stream")
async def stream_review(request: Request, review_date: Optional[str] = None, watchlist: Optional[str] = None, preferred_provider: Optional[str] = None, api_key: Optional[str] = None):
    def _to_iso(v):
        if isinstance(v, (datetime, date)):
            try:
                return v.isoformat()
            except Exception:
                return str(v)
        return v

    def _clean(obj):
        if isinstance(obj, dict):
            return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_clean(x) for x in obj]
        return _to_iso(obj)

    def _sse(event: str, payload: Any) -> str:
        return f"event: {event}\ndata: {json.dumps(_clean(payload), ensure_ascii=False)}\n\n"

    target_date = None
    if review_date:
        try:
            target_date = datetime.strptime(review_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    else:
        latest_str = await data_provider.get_last_trade_date(include_today=True)
        if latest_str:
            target_date = datetime.strptime(latest_str, "%Y%m%d").date()
        else:
            target_date = date.today()

    current_time = datetime.now()
    is_noon_time = 11 <= current_time.hour < 15
    if not review_date and is_noon_time:
        target_date = date.today()

    watchlist_codes: List[str] = []
    if watchlist:
        watchlist_codes = [c.strip() for c in str(watchlist).split(",") if c and c.strip()]

    import uuid
    review_log_channel = f"review_stream_{uuid.uuid4().hex}"
    selector_logger.clear(review_log_channel)

    stats_for_placeholder = await data_provider.get_market_snapshot(target_date)

    async def _save_placeholder(stats_dict: dict, review_dt: date) -> dict:
        temperature = float(review_service._calculate_market_temperature(stats_dict or {}, {"highest": 0}) or 50.0)

        def _upsert():
            db = SessionLocal()
            try:
                existing = (
                    db.query(MarketSentiment)
                    .filter(MarketSentiment.date == review_dt)
                    .order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc())
                    .first()
                )
                now_ts = datetime.now()
                up_count = int((stats_dict or {}).get("up", 0) or 0)
                down_count = int((stats_dict or {}).get("down", 0) or 0)
                limit_up_count = int((stats_dict or {}).get("limit_up", 0) or 0)
                limit_down_count = int((stats_dict or {}).get("limit_down", 0) or 0)
                total_volume = float((stats_dict or {}).get("total_volume", 0.0) or 0.0)

                if existing:
                    existing.up_count = up_count
                    existing.down_count = down_count
                    existing.limit_up_count = limit_up_count
                    existing.limit_down_count = limit_down_count
                    existing.total_volume = total_volume
                    existing.market_temperature = temperature
                    existing.main_theme = "生成中"
                    existing.summary = "复盘任务已启动，后台生成中…"
                    existing.updated_at = now_ts
                    db.commit()
                    db.refresh(existing)
                    created_at = existing.updated_at
                else:
                    sentiment = MarketSentiment(
                        date=review_dt,
                        up_count=up_count,
                        down_count=down_count,
                        limit_up_count=limit_up_count,
                        limit_down_count=limit_down_count,
                        total_volume=total_volume,
                        market_temperature=temperature,
                        highest_plate=0,
                        main_theme="生成中",
                        summary="复盘任务已启动，后台生成中…",
                        updated_at=now_ts,
                    )
                    db.add(sentiment)
                    db.commit()
                    db.refresh(sentiment)
                    created_at = sentiment.updated_at
                return {
                    "date": review_dt.strftime("%Y-%m-%d"),
                    "temp": temperature,
                    "created_at": created_at,
                    "highest_plate": 0,
                    "summary": "复盘任务已启动，后台生成中…",
                    "main_theme": "生成中",
                    "target_plan": None,
                    "target_plans": [],
                    "holding_plans": [],
                    "up": up_count,
                    "down": down_count,
                    "limit_up": limit_up_count,
                    "limit_down": limit_down_count,
                    "total_volume": total_volume,
                }
            finally:
                db.close()

        return await asyncio.to_thread(_upsert)

    placeholder = await _save_placeholder(stats_for_placeholder, target_date)
    placeholder_created_at = placeholder.get("created_at")

    async def _run_full_review_async():
        if target_date == date.today() and is_noon_time:
            await review_service.perform_noon_review(watchlist_codes, preferred_provider=preferred_provider, api_key=api_key)
        else:
            await review_service.perform_daily_review(target_date, watchlist_codes, preferred_provider=preferred_provider, api_key=api_key)

    with selector_logger.bind(review_log_channel):
        review_task = asyncio.create_task(_run_full_review_async())

    async def event_generator():
        last_logs_len = 0
        last_ping = time.monotonic()
        try:
            yield _sse("placeholder", placeholder)

            while True:
                if await request.is_disconnected():
                    try:
                        review_task.cancel()
                    except Exception:
                        pass
                    break

                logs = selector_logger.get_logs(review_log_channel)
                if last_logs_len < len(logs):
                    for line in logs[last_logs_len:]:
                        yield _sse("log", {"line": line})
                    last_logs_len = len(logs)

                done = False
                try:
                    def _get_sentiment():
                        db = SessionLocal()
                        try:
                            return (
                                db.query(MarketSentiment)
                                .filter(MarketSentiment.date == target_date)
                                .order_by(MarketSentiment.updated_at.desc(), MarketSentiment.id.desc())
                                .first()
                            )
                        finally:
                            db.close()

                    sentiment = await asyncio.to_thread(_get_sentiment)
                    if sentiment:
                        main_theme = str(sentiment.main_theme or "")
                        summary = str(sentiment.summary or "")
                        is_generating = (main_theme == "生成中") or ("后台生成中" in summary) or ("复盘任务已启动" in summary)
                        updated_at = sentiment.updated_at
                        if placeholder_created_at and updated_at and updated_at <= placeholder_created_at:
                            is_generating = True
                        if not is_generating:
                            result = await review_service.get_review_result(target_date)
                            yield _sse("final", result or {})
                            done = True
                except Exception as e:
                    yield _sse("error", {"message": str(e)})
                    done = True

                if done:
                    break

                if review_task.done():
                    exc = review_task.exception()
                    if exc:
                        yield _sse("error", {"message": str(exc)})
                        break

                now = time.monotonic()
                if now - last_ping >= 15.0:
                    yield "event: ping\ndata: {}\n\n"
                    last_ping = now

                await asyncio.sleep(0.8)
        finally:
            selector_logger.clear(review_log_channel)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/plan", response_model=PlanResponse)
async def create_trading_plan(plan: PlanCreateRequest):
    """
    创建新的交易计划
    """
    try:
        new_plan = await trading_service.create_plan(
            ts_code=plan.ts_code,
            strategy_name=plan.strategy_name,
            buy_price=plan.buy_price,
            stop_loss=plan.stop_loss,
            take_profit=plan.take_profit,
            position_pct=plan.position_pct,
            reason=plan.reason,
            score=plan.score or 0.0,
            source=plan.source
        )
        if not new_plan:
            raise HTTPException(status_code=409, detail="Plan creation blocked")
        return _plan_to_response(new_plan)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/plan/{plan_id}", response_model=PlanResponse)
async def update_trading_plan(plan_id: int, plan_update: PlanUpdateRequest):
    """
    更新现有交易计划
    """
    try:
        plan = await trading_service.update_plan(
            plan_id=plan_id,
            strategy_name=plan_update.strategy_name,
            buy_price=plan_update.buy_price,
            stop_loss=plan_update.stop_loss,
            take_profit=plan_update.take_profit,
            position_pct=plan_update.position_pct,
            reason=plan_update.reason,
            score=plan_update.score
        )
        
        return _plan_to_response(plan)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/plan/{plan_id}/cancel", response_model=PlanResponse)
async def cancel_trading_plan(plan_id: int, payload: PlanCancelRequest):
    try:
        reason = (payload.reason or "").strip() or "手动撤单"
        await trading_service.cancel_plan(plan_id, reason)
        db = SessionLocal()
        try:
            plan = db.query(TradingPlan).get(plan_id)
            if not plan:
                raise HTTPException(status_code=404, detail="Plan not found")
            return _plan_to_response(plan)
        finally:
            db.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling plan: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/plan/{plan_id}")
async def delete_trading_plan(plan_id: int):
    """
    删除交易计划
    """
    try:
        await trading_service.delete_plan(plan_id)
        return {"status": "success", "message": f"Plan {plan_id} deleted"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/plans/history", response_model=List[PlanResponse])
async def get_plan_history(
    limit: int = 50, 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
):
    """
    获取历史交易计划
    """
    db = SessionLocal()
    try:
        query = db.query(TradingPlan)
        
        if start_date:
            try:
                s_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                query = query.filter(TradingPlan.date >= s_date)
            except ValueError:
                pass
                
        if end_date:
            try:
                e_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                query = query.filter(TradingPlan.date <= e_date)
            except ValueError:
                pass
                
        plans = query.order_by(TradingPlan.date.desc(), TradingPlan.id.desc()).limit(limit).all()
        
        result = []
        for p in plans:
            try:
                plan_resp = _plan_to_response(p, review_content=p.review_content or p.reason)
                result.append(plan_resp)
            except Exception:
                continue
        return result
    finally:
        db.close()

@router.get("/plans/today", response_model=List[PlanResponse])
async def get_todays_plans():
    """
    获取“最新”的交易计划
    逻辑：
    1. 优先获取今日(系统日期)生成的计划 (适应午间复盘产生的今日计划)
    2. 如果今日无计划，则尝试获取基于最新复盘日期的次日计划
    3. 兜底显示未来所有未执行计划
    """
    db = SessionLocal()
    try:
        today = date.today()
        target_plans = []
        
        # 1. 优先查今日计划 (适用于午间复盘产生的当日计划)
        # target_plans = db.query(TradingPlan).filter(TradingPlan.date == today).all()
        # 优化：不再仅查计划日期为今天的，而是查 created_at 在今天之后的，或者计划日期 >= 今天的
        # 这样能确保无论是午间复盘（计划日期=今天）还是晚间复盘（计划日期=明天）都能被查到
        
        # 策略调整：
        # 我们想要展示的是“当前最相关的计划”。
        # 如果是盘中（<15:00），我们想看今天的计划（午间复盘生成，或昨晚生成）
        # 如果是盘后（>15:00），我们想看明天的计划（刚刚晚间复盘生成）
        
        # 但用户可能在盘后复盘，生成了明天的计划，此时他点“计划”列表，应该看到明天的计划。
        # 同时，他也可能想看今天的执行情况。
        
        # 综合方案：获取所有 date >= today 的计划
        target_plans = db.query(TradingPlan).filter(TradingPlan.date >= today).order_by(TradingPlan.date.asc(), TradingPlan.id.desc()).all()
        
        # 去重逻辑：保留每个股票的最新计划 (忽略日期差异，只看最新的一个动作)
        # 用户希望能看到最新的状态，而不是历史执行记录 + 未来计划并存
        seen_codes = set()
        unique_plans = []
        
        # 先按 ID 倒序排列 (最新的在前)
        target_plans.sort(key=lambda x: x.id, reverse=True)
        
        for p in target_plans:
            # [Fix] 过滤掉状态为 EXPIRED, STOPPED, REMOVED_OVERFLOW, CANCELLED 的计划
            # 确保前端列表只显示活跃的监控计划
            status = (p.track_status or "").upper()
            if status in {"CANCELLED", "EXPIRED", "STOPPED", "REMOVED_OVERFLOW", "FINISHED"}:
                continue
                
            if p.ts_code not in seen_codes:
                if p.executed:
                    continue
                    
                seen_codes.add(p.ts_code)
                unique_plans.append(p)
        
        target_plans = unique_plans
        # 恢复按日期排序，方便查看
        target_plans.sort(key=lambda x: (x.date, x.id))
        
        # 如果没有 >= today 的计划（比如周末复盘了周五的？不对，复盘只会生成未来计划）
        # 如果真的没有，尝试查最近一次复盘生成的计划
        if not target_plans:
            latest_sentiment = db.query(MarketSentiment).order_by(MarketSentiment.id.desc()).first()
            if latest_sentiment:
                review_date = latest_sentiment.date
                # 查找 date > review_date 的计划
                target_plans = db.query(TradingPlan).filter(TradingPlan.date > review_date).order_by(TradingPlan.date.asc()).all()
                
                # 如果还是没有，查 review_date 当天的（可能是午间复盘）
                if not target_plans:
                     target_plans = db.query(TradingPlan).filter(TradingPlan.date == review_date).order_by(TradingPlan.date.asc()).all()

        logger.info(f"Found {len(target_plans)} plans")
        
        result = []
        for p in target_plans:
            try:
                if not p.ts_code:
                    logger.warning(f"Skip plan {p.id}: missing ts_code")
                    continue

                # Filter out excluded stocks (BJ 920, etc)
                # This is a safety filter for display
                if p.ts_code.startswith('920') or p.ts_code.endswith('.BJ') or p.ts_code.startswith('8') or p.ts_code.startswith('4'):
                    continue

                plan_resp = _plan_to_response(p, review_content=p.review_content or p.reason)
                result.append(plan_resp)
            except Exception as e:
                logger.error(f"Error converting plan {p.id}: {e}")
                continue
        return result
    finally:
        db.close()
                
@router.get("/audit/reports")
async def get_audit_reports(limit: int = 10):
    """获取审计报告列表"""
    db = SessionLocal()
    try:
        from app.models.stock_models import AuditReport
        reports = db.query(AuditReport).order_by(AuditReport.audit_date.desc()).limit(limit).all()
        return reports
    finally:
        db.close()

@router.get("/audit/report/{report_id}")
async def get_audit_report_detail(report_id: int):
    """获取审计报告详情"""
    db = SessionLocal()
    try:
        from app.models.stock_models import AuditReport, AuditDetail
        report = db.query(AuditReport).get(report_id)
        if not report:
            raise HTTPException(status_code=404, detail="Report not found")
        
        details = db.query(AuditDetail).filter(AuditDetail.report_id == report_id).all()
        return {
            "report": report,
            "details": details
        }
    finally:
        db.close()

@router.post("/audit/trigger")
async def trigger_audit():
    """手动触发全面审计"""
    from app.services.audit_service import audit_service
    report = await audit_service.run_daily_audit(force=True)
    return {"status": "ok", "report_id": report.id if report else None}

class AppealRequest(BaseModel):
    event_id: int
    reason: str

class AppealReviewRequest(BaseModel):
    appeal_id: int
    approve: bool
    reviewer: str
    review_note: str = ""

@router.get("/reward-punish/summary")
async def get_reward_punish_summary():
    return await asyncio.to_thread(reward_punish_service.get_summary)

@router.get("/plans/stream")
async def stream_plan_events(request: Request):
    queue = await plan_event_service.subscribe()

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"event: plan\ndata: {json.dumps(evt, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield "event: ping\ndata: {}\n\n"
        finally:
            plan_event_service.unsubscribe(queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@router.post("/reward-punish/appeal")
async def submit_reward_punish_appeal(payload: AppealRequest):
    appeal_id = reward_punish_service.submit_appeal(payload.event_id, payload.reason)
    return {"status": "ok", "appeal_id": appeal_id}

@router.post("/reward-punish/review")
async def review_reward_punish_appeal(payload: AppealReviewRequest):
    ok = reward_punish_service.review_appeal(payload.appeal_id, payload.approve, payload.reviewer, payload.review_note)
    if not ok:
        raise HTTPException(status_code=404, detail="Appeal not found")
    return {"status": "ok", "approved": payload.approve}
