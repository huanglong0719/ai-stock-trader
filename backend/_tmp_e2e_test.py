import asyncio
import json
import os
import time
from datetime import date

from app.db.session import SessionLocal
from app.models.stock_models import Account, DailyBar, Position, TradeRecord, TradingPlan
from app.services.ai.analysis_service import analysis_service
from app.services.chat_service import chat_service
from app.services.data_provider import data_provider
from app.services.market.market_data_service import market_data_service
from app.services.market.market_utils import get_limit_prices
from app.services.stock_selector import stock_selector
from app.services.trading_service import trading_service


async def _pick_ts_code() -> str:
    if os.getenv("RUN_PRESSURE_TEST", "0") == "1":
        turnover_codes = await data_provider.get_market_turnover_top_codes(top_n=200)
        for code in turnover_codes or []:
            code_str = str(code)
            if code_str:
                return code_str
        raise RuntimeError("压力测试候选为空")

    opps = await stock_selector.scan_noon_opportunities()
    candidates = (opps.get("dragons") or []) + (opps.get("reversals") or [])
    if candidates:
        ts_code = candidates[0].get("ts_code")
        if ts_code:
            return ts_code

    try:
        limit_up_codes = await market_data_service.get_realtime_limit_up_codes()
    except Exception:
        limit_up_codes = []
    if limit_up_codes:
        return str(limit_up_codes[0])

    try:
        turnover_codes = await data_provider.get_market_turnover_top_codes(top_n=200)
    except Exception:
        turnover_codes = []
    if turnover_codes:
        return str(turnover_codes[0])

    raise RuntimeError("盘中扫描未获取到候选股票")


async def _pick_buy_candidate(exclude_codes: set[str]) -> tuple[str, dict] | None:
    try:
        turnover_codes = await data_provider.get_market_turnover_top_codes(top_n=200)
    except Exception:
        turnover_codes = []
    if not turnover_codes:
        return None
    quotes = await data_provider.get_realtime_quotes(turnover_codes[:200])
    for code in turnover_codes:
        if code in exclude_codes:
            continue
        q = quotes.get(code) or {}
        price = float(q.get("price") or 0)
        pct = float(q.get("pct_chg") or 0)
        if price > 0 and pct < 9.5:
            return str(code), q
    return None


async def _pick_limit_up_candidate(exclude_codes: set[str]) -> tuple[str, dict] | None:
    try:
        limit_up_codes = await market_data_service.get_realtime_limit_up_codes()
    except Exception:
        limit_up_codes = []
    for code in limit_up_codes or []:
        code_str = str(code)
        if code_str in exclude_codes:
            continue
        quote = await data_provider.get_realtime_quote(code_str)
        if quote and float(quote.get("price") or 0) > 0:
            return code_str, quote
    return None


async def _run_trade_execution_tests(base_ts_code: str) -> None:
    check_results = []

    def _add_check(name: str, ok: bool, detail: str) -> None:
        check_results.append({"name": name, "ok": ok, "detail": detail})

    db = SessionLocal()
    try:
        today = date.today()
        existing_positions = db.query(Position).filter(Position.vol > 0).all()
        pos_codes = {p.ts_code for p in existing_positions if p.ts_code}
        existing_plans = db.query(TradingPlan.ts_code).filter(
            TradingPlan.date == today,
            TradingPlan.executed == False
        ).all()
        plan_codes = {p.ts_code for p in existing_plans if p.ts_code}
    finally:
        db.close()

    test_plan_ids: list[int] = []
    test_codes: set[str] = set()
    ai_result = "SKIP"
    cancel_result = "SKIP"
    buy_result = "SKIP"
    sell_result = "SKIP"
    limit_result = "SKIP"

    is_trading_time = bool(data_provider.is_trading_time())
    ai_candidate = None
    if base_ts_code and base_ts_code not in plan_codes:
        base_quote = await data_provider.get_realtime_quote(base_ts_code)
        if base_quote and float(base_quote.get("price") or 0) > 0:
            ai_candidate = (base_ts_code, base_quote)
    if not ai_candidate:
        ai_candidate = await _pick_buy_candidate(pos_codes | plan_codes)

    if ai_candidate:
        ai_code, ai_quote = ai_candidate
        ai_price = float(ai_quote.get("price") or 0)
        ai_plan = await trading_service.create_plan(
            ts_code=ai_code,
            strategy_name="AI计划测试",
            buy_price=ai_price,
            position_pct=0.01,
            source="user",
            order_type="MARKET",
            limit_price=ai_price,
        )
        ai_plan_id = ai_plan.id if ai_plan else None
        if ai_plan_id is not None:
            test_plan_ids.append(ai_plan_id)
        test_codes.add(ai_code)
        market_snapshot = await data_provider.get_market_snapshot()
        market_status = f"测试快照时间 {market_snapshot.get('time', '')}"
        if ai_plan_id is not None:
            await trading_service._process_single_plan(
                ai_plan_id,
                ai_quote,
                market_status,
                market_snapshot,
                force_open_confirm=False,
            )
        db = SessionLocal()
        try:
            plan_db = None
            record = None
            if ai_plan_id is not None:
                plan_db = db.query(TradingPlan).filter(TradingPlan.id == ai_plan_id).first()
                record = db.query(TradeRecord).filter(TradeRecord.plan_id == ai_plan_id).first()
            ai_result = json.dumps(
                {
                    "ai_decision": plan_db.ai_decision if plan_db else None,
                    "executed": bool(plan_db.executed) if plan_db else False,
                    "frozen_amount": float(plan_db.frozen_amount or 0) if plan_db else 0.0,
                    "review": plan_db.review_content if plan_db else None,
                    "track_status": plan_db.track_status if plan_db else None,
                    "record": bool(record),
                },
                ensure_ascii=False,
            )
        finally:
            db.close()
        ai_ok = False
        if ai_plan_id is not None:
            if ai_result:
                try:
                    ai_json = json.loads(ai_result)
                except Exception:
                    ai_json = {}
                ai_decision = (ai_json.get("ai_decision") or "").upper()
                ai_executed = bool(ai_json.get("executed"))
                ai_record = bool(ai_json.get("record"))
                ai_frozen = float(ai_json.get("frozen_amount") or 0.0)
                ai_review = str(ai_json.get("review") or "")
                ai_track_status = str(ai_json.get("track_status") or "")
                if ai_decision == "BUY":
                    blocked = (
                        ("风控拦截" in ai_review)
                        or ("月线过热" in ai_review)
                        or ("月线MA5加速" in ai_review)
                        or ("乖离率过大" in ai_review)
                        or ("震荡不足" in ai_review)
                        or ("MA5加速" in ai_review)
                        or ("重点关注" in ai_review)
                    )
                    if ai_track_status == "CANCELLED" and ai_executed and (not ai_record):
                        ai_ok = True
                    elif blocked:
                        ai_ok = not ai_record
                    elif data_provider.is_trading_time():
                        ai_ok = (ai_executed and ai_record) or ((not ai_executed) and (not ai_record))
                    else:
                        ai_ok = (not ai_executed) and (not ai_record) and ai_frozen > 0
                elif ai_decision in {"WAIT", "HOLD"}:
                    ai_ok = (not ai_record) and (not ai_executed) and ai_frozen == 0.0
                elif ai_decision in {"CANCEL", "SELL", "REDUCE"}:
                    if ai_decision == "CANCEL" and ai_executed and ai_track_status == "CANCELLED":
                        ai_ok = True
                    else:
                        ai_ok = (not ai_record) and (not ai_executed) and ai_frozen == 0.0
                else:
                    ai_ok = False
        _add_check("AI计划处理", ai_ok, ai_result or "")

    cancel_candidate = await _pick_buy_candidate(pos_codes | plan_codes | test_codes)
    if not cancel_candidate and base_ts_code:
        base_quote = await data_provider.get_realtime_quote(base_ts_code)
        base_price = float(base_quote.get("price") or 0) if base_quote else 0.0
        if base_price <= 0:
            db = SessionLocal()
            try:
                last_bar = db.query(DailyBar).filter(DailyBar.ts_code == base_ts_code).order_by(DailyBar.trade_date.desc()).first()
                base_price = float(last_bar.close or 0.0) if last_bar else 0.0
            finally:
                db.close()
        if base_price <= 0:
            base_price = 1.0
        if base_price > 0:
            cancel_candidate = (base_ts_code, {"price": base_price})
    if cancel_candidate:
        cancel_code, cancel_quote = cancel_candidate
        cancel_price = float(cancel_quote.get("price") or 0)
        cancel_plan = await trading_service.create_plan(
            ts_code=cancel_code,
            strategy_name="测试撤单",
            buy_price=cancel_price,
            position_pct=0.01,
            source="user",
            order_type="MARKET",
            limit_price=cancel_price,
        )
        cancel_plan_id = cancel_plan.id if cancel_plan else None
        if cancel_plan_id is not None:
            test_plan_ids.append(cancel_plan_id)
        test_codes.add(cancel_code)
        db = SessionLocal()
        try:
            account = db.query(Account).first()
            frozen_before = float(account.frozen_cash or 0.0) if account else 0.0
        finally:
            db.close()
        if cancel_plan_id is not None:
            await trading_service.cancel_plan(cancel_plan_id, "测试撤单")
        db = SessionLocal()
        try:
            plan_db = None
            if cancel_plan_id is not None:
                plan_db = db.query(TradingPlan).filter(TradingPlan.id == cancel_plan_id).first()
            account = db.query(Account).first()
            frozen_after = float(account.frozen_cash or 0.0) if account else 0.0
            cancel_result = json.dumps(
                {
                    "executed": bool(plan_db.executed) if plan_db else False,
                    "frozen_before": frozen_before,
                    "frozen_after": frozen_after,
                    "plan_frozen": float(plan_db.frozen_amount or 0) if plan_db else 0.0,
                },
                ensure_ascii=False,
            )
        finally:
            db.close()
        cancel_ok = False
        if cancel_result and cancel_result != "SKIP":
            try:
                cancel_json = json.loads(cancel_result)
            except Exception:
                cancel_json = {}
            cancel_ok = (
                bool(cancel_json.get("executed"))
                and float(cancel_json.get("plan_frozen") or 0.0) == 0.0
                and float(cancel_json.get("frozen_after") or 0.0) <= float(cancel_json.get("frozen_before") or 0.0) + 1e-6
            )
        _add_check("撤单解冻", cancel_ok, cancel_result or "")

    buy_candidate = await _pick_buy_candidate(pos_codes | plan_codes)
    buy_ok = False
    buy_code = None
    buy_price = 0.0
    buy_volume = 100
    if buy_candidate:
        buy_code, buy_quote = buy_candidate
        buy_price = float(buy_quote.get("price") or 0)
        plan = await trading_service.create_plan(
            ts_code=buy_code,
            strategy_name="测试买入",
            buy_price=buy_price,
            position_pct=0.01,
            source="user",
            order_type="MARKET",
            limit_price=buy_price,
        )
        plan_id = plan.id if plan else None
        if plan_id is not None:
            test_plan_ids.append(plan_id)
        test_codes.add(buy_code)
        db = SessionLocal()
        try:
            plan_db = None
            record = None
            account_before = db.query(Account).first()
            available_before = float(account_before.available_cash or 0.0) if account_before else 0.0
            if plan_id is not None:
                plan_db = db.query(TradingPlan).filter(TradingPlan.id == plan_id).first()
            buy_success = False
            if plan_db:
                buy_success = await trading_service.execute_buy(db, plan_db, suggested_price=buy_price, volume=buy_volume)
                db.refresh(plan_db)
            if plan_id is not None:
                record = db.query(TradeRecord).filter(TradeRecord.plan_id == plan_id).first()
            position = db.query(Position).filter(Position.ts_code == buy_code).first() if buy_code else None
            account_after = db.query(Account).first()
            available_after = float(account_after.available_cash or 0.0) if account_after else 0.0
            buy_result = json.dumps(
                {
                    "is_trading_time": is_trading_time,
                    "buy_success": bool(buy_success),
                    "plan_executed": bool(plan_db.executed) if plan_db else False,
                    "record": bool(record),
                    "position_vol": int(position.vol) if position else 0,
                    "available_vol": int(position.available_vol) if position else 0,
                    "available_cash_before": available_before,
                    "available_cash_after": available_after,
                    "review": str(plan_db.review_content or "") if plan_db else "",
                },
                ensure_ascii=False,
            )
            if is_trading_time:
                blocked = False
                try:
                    rj = json.loads(buy_result)
                    blocked = (
                        ("风控拦截" in str(rj.get("review") or ""))
                        or ("月线高位" in str(rj.get("review") or ""))
                        or ("月线过热" in str(rj.get("review") or ""))
                        or ("乖离率过大" in str(rj.get("review") or ""))
                        or ("震荡不足" in str(rj.get("review") or ""))
                        or ("MA5加速" in str(rj.get("review") or ""))
                        or ("进入加速" in str(rj.get("review") or ""))
                        or ("临近加速" in str(rj.get("review") or ""))
                    )
                except Exception:
                    blocked = False
                buy_ok = (bool(buy_success) and bool(record) and bool(plan_db and plan_db.executed)) or blocked
            else:
                buy_ok = (not buy_success) and (not record) and bool(plan_db and (not plan_db.executed))
        finally:
            db.close()
        _add_check("买入执行", buy_ok, buy_result or "")

        sell_code = buy_code
        sell_ok = False
        if sell_code:
            sell_quote = await data_provider.get_realtime_quote(sell_code)
            if sell_quote:
                sell_price = float(sell_quote.get("price") or buy_price)
                sell_plan = await trading_service.create_plan(
                    ts_code=sell_code,
                    strategy_name="测试卖出",
                    buy_price=sell_price,
                    position_pct=0.0,
                    source="system",
                    order_type="MARKET",
                    limit_price=sell_price,
                    is_sell=True,
                )
                sell_plan_id = sell_plan.id if sell_plan else None
                if sell_plan_id is not None:
                    test_plan_ids.append(sell_plan_id)
                test_codes.add(sell_code)
                db = SessionLocal()
                try:
                    sell_record = None
                    plan_db = None
                    position = db.query(Position).filter(Position.ts_code == sell_code).first()
                    available_vol = int(position.available_vol or 0) if position else 0
                    if sell_plan_id is not None:
                        plan_db = db.query(TradingPlan).filter(TradingPlan.id == sell_plan_id).first()
                    sell_success = await trading_service.execute_sell(
                        db,
                        ts_code=str(sell_code),
                        suggested_price=float(sell_price),
                        volume=buy_volume,
                        reason="测试卖出",
                        order_type="MARKET",
                        plan_id=int(sell_plan_id or 0) if sell_plan_id is not None else None,
                    )
                    if sell_plan_id is not None:
                        sell_record = db.query(TradeRecord).filter(TradeRecord.plan_id == sell_plan_id).first()
                    sell_result = json.dumps(
                        {
                            "is_trading_time": is_trading_time,
                            "sell_success": bool(sell_success),
                            "plan_executed": bool(plan_db.executed) if plan_db else False,
                            "record": bool(sell_record),
                            "available_vol": available_vol,
                        },
                        ensure_ascii=False,
                    )
                    if not is_trading_time:
                        sell_ok = (not sell_success) and (not sell_record)
                    else:
                        if buy_ok:
                            sell_ok = (not sell_success) and (not sell_record) and available_vol <= 0
                        else:
                            sell_ok = (not sell_success) and (not sell_record)
                finally:
                    db.close()
                _add_check("卖出执行", sell_ok, sell_result or "")

    limit_candidate = await _pick_limit_up_candidate(test_codes | pos_codes | plan_codes)
    if not limit_candidate and base_ts_code:
        base_quote = await data_provider.get_realtime_quote(base_ts_code)
        if base_quote:
            limit_candidate = (base_ts_code, base_quote)
    if limit_candidate:
        limit_code, limit_quote = limit_candidate
        pre_close = float(limit_quote.get("pre_close") or 0.0)
        if pre_close <= 0:
            db = SessionLocal()
            try:
                last_bar = db.query(DailyBar).filter(DailyBar.ts_code == limit_code).order_by(DailyBar.trade_date.desc()).first()
                pre_close = float(last_bar.close or 0.0) if last_bar else 0.0
            finally:
                db.close()
        limit_up, _ = get_limit_prices(str(limit_code), float(pre_close or 0.0))
        limit_price = float(limit_up or 0.0)
        limit_ok = False
        if limit_price > 0:
            limit_plan = await trading_service.create_plan(
                ts_code=limit_code,
                strategy_name="测试涨停排队",
                buy_price=limit_price,
                position_pct=0.01,
                source="user",
                order_type="MARKET",
                limit_price=limit_price,
            )
            limit_plan_id = limit_plan.id if limit_plan else None
            if limit_plan_id is not None:
                test_plan_ids.append(limit_plan_id)
            test_codes.add(limit_code)
            db = SessionLocal()
            try:
                plan_db = None
                if limit_plan_id is not None:
                    plan_db = db.query(TradingPlan).filter(TradingPlan.id == limit_plan_id).first()
                if plan_db:
                    plan_db.market_snapshot_json = "{}"
                    db.commit()
                queue_quote = {
                    "price": limit_price,
                    "pre_close": pre_close,
                    "bid_ask": {"b1_v": 200000, "s1_v": 0},
                    "vol": 1000000,
                }
                can_trade = True
                if plan_db:
                    can_trade = await trading_service._check_limit_order_queue(
                        db,
                        plan_db,
                        queue_quote,
                        side="BUY",
                        order_vol=1000,
                    )
                    db.refresh(plan_db)
                queued = False
                if plan_db and plan_db.market_snapshot_json:
                    try:
                        snapshot = json.loads(str(plan_db.market_snapshot_json)) or {}
                        queued = bool(snapshot.get("queue_info"))
                    except Exception:
                        queued = False
                limit_result = json.dumps(
                    {
                        "can_trade": bool(can_trade),
                        "queued": queued,
                        "limit_price": limit_price,
                        "pre_close": pre_close,
                    },
                    ensure_ascii=False,
                )
                limit_ok = (not can_trade) and queued
            finally:
                db.close()
        _add_check("涨停排队", limit_ok, limit_result if limit_price > 0 else "无法获取有效前收/涨停价")

    if test_plan_ids:
        db = SessionLocal()
        try:
            db.query(TradeRecord).filter(TradeRecord.plan_id.in_(test_plan_ids)).delete(synchronize_session=False)
            db.query(TradingPlan).filter(TradingPlan.id.in_(test_plan_ids)).delete(synchronize_session=False)
            for code in test_codes:
                if code not in pos_codes:
                    db.query(Position).filter(Position.ts_code == code).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

        await trading_service.reconcile_account_cash()
        await trading_service.sync_account_assets()

    print("trade_test_buy", buy_result)
    print("trade_test_sell", sell_result)
    print("trade_test_limit_up", limit_result)
    print("trade_test_ai_plan", ai_result)
    print("trade_test_cancel", cancel_result)
    print("trade_test_checks", json.dumps(check_results, ensure_ascii=False))
    failed = [c for c in check_results if not c.get("ok")]
    if failed:
        raise RuntimeError(json.dumps(failed, ensure_ascii=False))


async def _run_pressure_test() -> None:
    codes = await data_provider.get_market_turnover_top_codes(top_n=200)
    codes = [str(c) for c in (codes or []) if c]
    if not codes:
        raise RuntimeError("压力测试未获取到候选股票")

    start = time.monotonic()
    errors: list[str] = []
    rounds_env = os.getenv("PRESSURE_ROUNDS") or "2"
    timeout_env = os.getenv("PRESSURE_TIMEOUT_SEC") or "20"
    sem_env = os.getenv("PRESSURE_CONCURRENCY") or "10"
    rounds = max(1, int(rounds_env))
    timeout_sec = max(5.0, float(timeout_env))
    sem = asyncio.Semaphore(max(1, int(sem_env)))
    batches = [codes[i:i + 80] for i in range(0, len(codes), 80)]

    async def _guarded(coro, tag: str, timeout_override: float | None = None):
        async with sem:
            task = asyncio.create_task(coro)
            try:
                timeout_use = timeout_override if timeout_override is not None else timeout_sec
                return await asyncio.wait_for(task, timeout=timeout_use)
            except asyncio.TimeoutError:
                errors.append(f"{tag}: TimeoutError")
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                return None
            except asyncio.CancelledError:
                errors.append(f"{tag}: CancelledError")
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                return None
            except Exception as e:
                errors.append(f"{tag}: {type(e).__name__} {e}")
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                return None

    tasks = []
    for _ in range(rounds):
        for batch in batches:
            tasks.append(_guarded(data_provider.get_realtime_quotes(batch), f"quotes_{len(batch)}"))

    context_count = max(0, int(os.getenv("PRESSURE_CONTEXT_COUNT") or "5"))
    context_timeout = max(5.0, float(os.getenv("PRESSURE_CONTEXT_TIMEOUT_SEC") or "30"))
    for ts_code in codes[:context_count]:
        tasks.append(_guarded(chat_service.get_ai_trading_context(ts_code), f"context_{ts_code}", timeout_override=context_timeout))

    tasks.append(_guarded(trading_service.reconcile_account_cash(), "reconcile_cash"))
    tasks.append(_guarded(trading_service.sync_account_assets(), "sync_assets"))

    with market_data_service.cache_scope("pressure"):
        await asyncio.gather(*tasks)

    duration = time.monotonic() - start
    print(
        "pressure_test",
        json.dumps(
            {
                "codes": len(codes),
                "tasks": len(tasks),
                "seconds": round(duration, 2),
                "errors": errors[:5],
                "error_count": len(errors),
            },
            ensure_ascii=False,
        ),
    )
    if errors:
        raise RuntimeError(json.dumps(errors[:10], ensure_ascii=False))


async def main() -> None:
    pressure_mode = os.getenv("RUN_PRESSURE_TEST", "0") == "1"
    e2e_timeout_env = os.getenv("E2E_TIMEOUT_SEC")
    if e2e_timeout_env:
        e2e_timeout = float(e2e_timeout_env or 0)
    else:
        e2e_timeout = 120.0 if pressure_mode else 0.0

    async def _with_timeout(coro, label: str):
        if e2e_timeout and e2e_timeout > 0:
            return await asyncio.wait_for(coro, timeout=e2e_timeout)
        return await coro

    ts_code = await _pick_ts_code()

    quote = await market_data_service.get_realtime_quote(ts_code)
    print(
        "realtime_quote",
        json.dumps(
            {k: (quote or {}).get(k) for k in ["ts_code", "price", "pct_chg", "time", "amount"]},
            ensure_ascii=False,
        ),
    )

    data = await market_data_service.get_ai_context_data(ts_code)
    print(
        "kline_lens",
        json.dumps(
            {
                "D": len(data.get("kline_d") or []),
                "W": len(data.get("weekly_k") or []),
                "M": len(data.get("monthly_k") or []),
                "30m": len(data.get("kline_30m") or []),
                "5m": len(data.get("kline_5m") or []),
            },
            ensure_ascii=False,
        ),
    )

    stats = data.get("stats") or {}
    print(
        "stats_5y",
        json.dumps(
            {k: stats.get(k) for k in ["h_5y", "l_5y", "h_date", "l_date"]},
            ensure_ascii=False,
        ),
    )

    raw_ctx = await _with_timeout(chat_service.get_ai_trading_context(ts_code), "ai_context")
    has_pankou = ("盘口" in raw_ctx) or ("买一" in raw_ctx) or ("卖一" in raw_ctx)
    has_history = (
        ("【周K线明细" in raw_ctx)
        or ("【月K线明细" in raw_ctx)
        or ("【30分钟K线明细" in raw_ctx)
        or ("【5分钟K线明细" in raw_ctx)
        or ("【周线最近" in raw_ctx)
        or ("【月线最近" in raw_ctx)
        or ("【30分钟K线最近" in raw_ctx)
        or ("【5分钟K线最近" in raw_ctx)
    )
    print(
        "context_meta",
        json.dumps(
            {
                "chars": len(raw_ctx),
                "compressed": "【上下文已分段压缩】" in raw_ctx,
                "has_5y": "【历史统计概览 - 5年全景】" in raw_ctx,
                "has_key_data": "【关键数据】" in raw_ctx,
                "has_pankou": has_pankou,
                "has_history": has_history,
            },
            ensure_ascii=False,
        ),
    )
    if not has_history:
        raise RuntimeError(json.dumps({"name": "上下文历史数据缺失", "ts_code": ts_code}, ensure_ascii=False))
    if not has_pankou:
        raise RuntimeError(json.dumps({"name": "上下文盘口缺失", "ts_code": ts_code}, ensure_ascii=False))

    result = await _with_timeout(
        analysis_service.analyze_stock(
            ts_code,
            kline_data=data.get("kline_d"),
            weekly_kline=data.get("weekly_k"),
            monthly_kline=data.get("monthly_k"),
            kline_5m=data.get("kline_5m"),
            kline_30m=data.get("kline_30m"),
            realtime_quote=data.get("quote"),
            basic_info={"name": data.get("name", ts_code)},
            raw_trading_context=raw_ctx,
            preferred_provider="Xiaomi MiMo",
        ),
        "ai_analyze",
    )

    if not result:
        print("ai_result_meta", json.dumps(None, ensure_ascii=False))
        return

    print(
        "ai_result_meta",
        json.dumps(
            {
                "symbol": result.get("symbol"),
                "score": result.get("score"),
                "source": result.get("source"),
                "is_worth_trading": result.get("is_worth_trading"),
            },
            ensure_ascii=False,
        ),
    )
    analysis = str(result.get("analysis") or "")
    analysis_head = analysis[:400].replace("\n", " ")
    print("ai_analysis_head", analysis_head)

    await _with_timeout(_run_trade_execution_tests(ts_code), "trade_execution_tests")
    if os.getenv("RUN_PRESSURE_TEST", "0") == "1":
        await _run_pressure_test()


if __name__ == "__main__":
    asyncio.run(main())
