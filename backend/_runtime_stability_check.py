import os
import sys
import json
import time
import asyncio
from typing import List, Tuple, Any

from fastapi.testclient import TestClient

sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.main import app
from app.core.config import settings
from app.services.data_provider import data_provider
from app.services.scheduler import scheduler_manager
from app.services.trading_service import trading_service


def _check(resp, name: str, allow_empty: bool = False) -> Tuple[bool, str]:
    if resp.status_code != 200:
        return False, f"{name} status {resp.status_code}"
    try:
        data = resp.json()
    except Exception:
        return False, f"{name} invalid json"
    if data is None:
        return False, f"{name} empty"
    if not allow_empty and isinstance(data, list) and len(data) == 0:
        return False, f"{name} empty list"
    return True, ""


def _pick_symbol(stocks):
    if not isinstance(stocks, list):
        return "000001.SH"
    for item in stocks:
        if isinstance(item, dict):
            code = item.get("ts_code") or item.get("code") or ""
            if isinstance(code, str) and (code.endswith(".SH") or code.endswith(".SZ")):
                return code
    return "000001.SH"


def main():
    client = TestClient(app)
    failures: List[str] = []
    try:
        client.__enter__()
        stocks_resp = client.get("/api/market/stocks")
        ok, err = _check(stocks_resp, "market_stocks", allow_empty=False)
        if not ok:
            failures.append(err)
            symbol = "000001.SH"
            stocks = []
        else:
            stocks = stocks_resp.json()
            symbol = _pick_symbol(stocks)

        checks = [
            ("root", client.get("/")),
            ("market_overview", client.get("/api/market/overview")),
            ("sync_status", client.get("/api/sync/status")),
            ("ai_providers", client.get("/api/ai/providers")),
            ("trading_account", client.get("/api/trading/account")),
            ("trading_positions", client.get("/api/trading/positions")),
            ("trading_records", client.get("/api/trading/records")),
            ("trading_entrustments", client.get("/api/trading/entrustments")),
            ("memory_export", client.get("/api/memory/export")),
            (f"quote_{symbol}", client.get(f"/api/market/quote/{symbol}")),
            (f"quotes_{symbol}", client.post("/api/market/quotes", json=[symbol])),
            (f"kline_D_{symbol}", client.get(f"/api/market/kline/{symbol}?freq=D&limit=200")),
            (f"kline_W_{symbol}", client.get(f"/api/market/kline/{symbol}?freq=W&limit=200")),
            (f"kline_M_{symbol}", client.get(f"/api/market/kline/{symbol}?freq=M&limit=200")),
            (f"kline_5m_{symbol}", client.get(f"/api/market/kline/{symbol}?freq=5min&limit=200")),
            (f"kline_30m_{symbol}", client.get(f"/api/market/kline/{symbol}?freq=30min&limit=200")),
        ]

        for name, resp in checks:
            allow_empty = "kline_5m" in name or "kline_30m" in name
            ok, err = _check(resp, name, allow_empty=allow_empty)
            if not ok:
                failures.append(err)

        quote_resp = client.get(f"/api/market/quote/{symbol}")
        price = 0.0
        if quote_resp.status_code == 200:
            try:
                payload: Any = quote_resp.json()
                price = float((payload or {}).get("price", 0) or 0)
            except Exception:
                price = 0.0
        buy_price = max(price, 1.0)

        plan_resp = client.post(
            "/api/trading/plan",
            json={
                "ts_code": symbol,
                "strategy_name": "稳定性检查",
                "buy_price": buy_price,
                "stop_loss": 0.0,
                "take_profit": 0.0,
                "position_pct": 0.01,
                "reason": "stability_check",
                "score": 0.0,
                "source": "user"
            }
        )
        ok, err = _check(plan_resp, "trading_plan_create", allow_empty=False)
        plan_id = None
        if ok:
            try:
                plan_id = plan_resp.json().get("id")
            except Exception:
                plan_id = None
            if not plan_id:
                failures.append("trading_plan_create missing id")
                ok = False
        else:
            failures.append(err)

        if plan_id:
            update_resp = client.put(
                f"/api/trading/plan/{plan_id}",
                json={"reason": "stability_check_update"}
            )
            ok, err = _check(update_resp, "trading_plan_update", allow_empty=False)
            if not ok:
                failures.append(err)

            cancel_resp = client.post(
                f"/api/trading/plan/{plan_id}/cancel",
                json={"reason": "stability_check_cancel"}
            )
            ok, err = _check(cancel_resp, "trading_plan_cancel", allow_empty=False)
            if not ok:
                failures.append(err)

        try:
            has_active = asyncio.run(trading_service.has_active_entrustments())
            if not has_active:
                asyncio.run(trading_service.execute_pending_entrustments())
                asyncio.run(trading_service.monitor_entrustments())
        except Exception as e:
            failures.append(f"entrustment_monitor failed: {e}")

        try:
            if not bool(getattr(settings, "ENABLE_AUTO_TRADE", False)):
                asyncio.run(trading_service.check_and_execute_plans(force_open_confirm=False))
        except Exception as e:
            failures.append(f"plan_execute failed: {e}")

        review_resp = client.post("/api/trading/review/daily", json={"async_mode": True, "watchlist": []})
        ok, err = _check(review_resp, "review_daily", allow_empty=False)
        if not ok:
            failures.append(err)
        latest_review_resp = client.get("/api/trading/review/latest")
        if latest_review_resp.status_code not in (200, 404):
            failures.append(f"review_latest status {latest_review_resp.status_code}")

        try:
            job_ids = set()
            for _ in range(3):
                jobs = scheduler_manager.scheduler.get_jobs()
                job_ids = {j.id for j in jobs}
                if job_ids:
                    break
                time.sleep(0.5)
            required = {"daily_sync", "trade_monitor", "daily_review"}
            if not job_ids:
                failures.append("scheduler jobs empty")
            if not required.issubset(job_ids):
                failures.append(f"scheduler jobs missing {sorted(list(required - job_ids))}")
        except Exception as e:
            failures.append(f"scheduler check failed: {e}")
    finally:
        client.__exit__(None, None, None)

    if failures:
        print(json.dumps({"ok": False, "errors": failures}, ensure_ascii=False, indent=2))
        raise SystemExit(1)
    print(json.dumps({"ok": True, "errors": []}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
