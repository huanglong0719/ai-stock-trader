from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from app.services.data_sync import data_sync_service
from datetime import datetime

router = APIRouter()

class SyncRequest(BaseModel):
    days: int = 3

class FixStockRequest(BaseModel):
    ts_code: str

@router.get("/sync/status")
async def get_sync_status():
    """获取当前同步状态"""
    state = dict(data_sync_service.sync_state or {})
    status = state.get("status", "unknown")
    progress = state.get("progress", 0) or 0
    last_updated = state.get("last_updated", "")
    age_sec = None
    try:
        age_sec = (datetime.now() - datetime.fromisoformat(last_updated)).total_seconds()
    except Exception:
        age_sec = None
    if status == "running":
        if progress >= 100:
            status = "idle"
            state["status"] = "idle"
            state["task"] = ""
            state["message"] = state.get("message") or "auto reset: completed"
        elif age_sec is not None and age_sec > 1800:
            status = "idle"
            state["status"] = "idle"
            state["task"] = ""
            state["message"] = "auto reset: stale status"
    
    # 构造前端兼容的响应结构
    return {
        "status": status,
        "data_quality": {
            "status": "Healthy" if status == "idle" else ("Error" if status == "error" else "Syncing"),
            "latest_trade_date": datetime.now().strftime("%Y-%m-%d"), # 暂用当前日期
            "latest_coverage": "全市场", # 暂用占位符
            "current_task": state
        }
    }

@router.post("/sync/backfill")
async def sync_backfill(request: SyncRequest, background_tasks: BackgroundTasks):
    """
    手动触发每日增量同步 (包含日线和分钟线)
    """
    # 1. 启动日线同步
    background_tasks.add_task(data_sync_service.backfill_data, days=request.days)
    
    # 2. [新增] 启动分钟线同步 (针对活跃池)
    # 前端用户反馈手动同步只下载日线，这里补上分钟线下载逻辑
    # 限制 days 为 2 天，避免全量下载耗时过长
    minute_days = min(request.days, 3) 
    background_tasks.add_task(data_sync_service.sync_post_close_minute_data, days=minute_days, pool="active")
    
    return {"message": f"Started backfill (Daily + Minute) for last {request.days} days"}

@router.post("/sync/fix_stock")
async def fix_stock_data(request: FixStockRequest, background_tasks: BackgroundTasks):
    """手动修复指定股票数据"""
    background_tasks.add_task(data_sync_service.fix_stock_data, request.ts_code)
    return {"message": f"已触发 {request.ts_code} 数据修复任务"}
