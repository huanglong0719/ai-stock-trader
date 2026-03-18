from fastapi import APIRouter, HTTPException, Query
from app.services.data_provider import data_provider
from app.services.ai_service import ai_service
from app.services.sector_analysis import sector_analysis
from pydantic import BaseModel
import asyncio
from datetime import datetime, timedelta

router = APIRouter()

class ReportEvaluateRequest(BaseModel):
    days: int = 30
    horizon_days: int = 5
    max_reports: int = 200
    only_unrated: bool = True

class AnalysisRequest(BaseModel):
    symbol: str
    preferred_provider: str | None = None
    api_key: str | None = None

class SelfReviewRequest(BaseModel):
    days: int = 30

@router.get("/ai/providers")
async def get_ai_providers():
    from app.services.ai.ai_client import ai_client
    return ai_client.get_available_providers()

@router.post("/analysis/kline")
async def analyze_kline(request: AnalysisRequest):
    # 1. 启动各项并行任务
    # [核心改进] 使用统一的防幻觉数据上下文，确保全系统分析结论一致
    from app.services.chat_service import chat_service
    context_task = chat_service.get_ai_trading_context(request.symbol, cache_scope="api")
    
    # 板块分析
    sector_task = asyncio.create_task(sector_analysis.analyze_sector(request.symbol))
    
    # 2. 等待基础数据
    # data_provider 的 get_realtime_quote 和 get_stock_basic_info 现在都是异步的，直接调用
    raw_trading_context, realtime_quote, basic_info = await asyncio.gather(
        context_task, 
        data_provider.get_realtime_quote(request.symbol, cache_scope="api"), 
        data_provider.get_stock_basic_info(request.symbol)
    )
    
    # 3. 获取最近 K 线 (在有 raw_trading_context 时，传 None 以确保 AI 视野统一)
    # 之前这里传了 kline_data 导致 AI 过度关注短期波动（如上影线），产生结论分歧
    kline_data = None
    
    # 4. 调用 AI 分析 (异步调用)
    result = await ai_service.analyze_stock(
        request.symbol, 
        kline_data, 
        basic_info, 
        realtime_quote, 
        raw_trading_context=raw_trading_context,
        sector_task=sector_task,
        preferred_provider=request.preferred_provider,
        api_key=request.api_key
    )
    
    return result

@router.get("/analysis/reports")
async def list_ai_reports(
    days: int = Query(30, ge=1, le=90),
    ts_code: str = Query("", max_length=20),
    analysis_type: str = Query("", max_length=50),
    evaluation_label: str = Query("", max_length=30),
    limit: int = Query(100, ge=1, le=500),
):
    from app.services.ai_report_service import ai_report_service

    ts = ts_code.strip() or None
    at = analysis_type.strip() or None
    el = evaluation_label.strip() or None
    return await ai_report_service.list_reports(days=days, ts_code=ts, analysis_type=at, evaluation_label=el, limit=limit)

@router.post("/analysis/reports/evaluate")
async def evaluate_ai_reports(request: ReportEvaluateRequest):
    from app.services.ai_report_service import ai_report_service
    return await ai_report_service.evaluate_recent_reports(
        days=max(1, min(request.days, 90)),
        horizon_days=max(1, min(request.horizon_days, 20)),
        max_reports=max(1, min(request.max_reports, 500)),
        only_unrated=bool(request.only_unrated),
    )

@router.get("/analysis/reports/self_review")
async def get_self_review_summary(days: int = Query(30, ge=1, le=90)):
    from app.services.ai_report_service import ai_report_service
    summary = await ai_report_service.get_latest_self_review_summary(days=days)
    if summary is None:
        return {"days": days, "total_evaluated": 0, "by_type": {}, "generated_at": None}
    return summary

@router.post("/analysis/reports/self_review/refresh")
async def refresh_self_review_summary(request: SelfReviewRequest):
    from app.services.ai_report_service import ai_report_service
    await ai_report_service.save_self_review_summary(days=max(1, min(request.days, 90)))
    summary = await ai_report_service.get_latest_self_review_summary(days=max(1, min(request.days, 90)))
    return summary or {"days": request.days, "total_evaluated": 0, "by_type": {}, "generated_at": None}
