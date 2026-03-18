import logging
import asyncio
from fastapi import APIRouter, HTTPException, Query, Depends
from app.services.data_provider import data_provider
from app.services.stock_selector import stock_selector
from app.services.logger import selector_logger
from app.db.session import get_db
from app.models.stock_models import IndustryData
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.services.ai_service import ai_service
from typing import List, Optional
import pandas as pd
import math

import numpy as np

router = APIRouter()
logger = logging.getLogger(__name__)

# 递归清理所有 NaN/Inf 的工具函数，确保 JSON 序列化正常
def clean_data(obj):
    if obj is None:
        return None
    if isinstance(obj, list):
        if len(obj) > 50: # 对大数据量采用非递归方式
            return [clean_data(i) for i in obj]
        return [clean_data(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: clean_data(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return round(obj, 2)
    return obj

def fast_clean_df(df: pd.DataFrame) -> list:
    """快速清理 DataFrame 中的异常值并转换为 dict 列表"""
    if df.empty:
        return []
    
    # 创建副本以避免副作用
    df_clean = df.copy()
        
    # 1. 对数值列进行四舍五入
    float_cols = df_clean.select_dtypes(include=['float64', 'float32']).columns
    round_3_cols = ['ma5', 'ma10', 'ma20', 'ma60', 'vol_ma5', 'vol_ma10', 'macd', 'macd_dea', 'macd_signal', 'macd_diff', 'weekly_ma20', 'monthly_ma20']
    
    cols_3 = [c for c in float_cols if c in round_3_cols]
    cols_2 = [c for c in float_cols if c not in round_3_cols]
    
    if cols_3:
        df_clean[cols_3] = df_clean[cols_3].round(3)
    if cols_2:
        df_clean[cols_2] = df_clean[cols_2].round(2)
    
    # 2. 处理 Inf 和 NaN
    # 先处理 Inf: 仅在数值列中查找并替换为 NaN
    # 注意：直接 replace([np.inf], np.nan) 在某些版本或混合类型下可能不生效
    if len(float_cols) > 0:
        # 使用 numpy 的 isinf 检测（最快且准确）
        # 需要先提取数值部分，处理后再赋值回去
        vals = df_clean[float_cols].values
        vals[np.isinf(vals)] = np.nan
        df_clean[float_cols] = vals

    # 3. 将 NaN 替换为 None (JSON 兼容)
    # 必须先转为 object 类型，否则 float 列中的 None 会被自动转回 NaN
    df_clean = df_clean.astype(object)
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    return df_clean.to_dict(orient='records')

@router.get("/market/kline/{symbol}")
async def get_kline(symbol: str, freq: str = 'D', start: str = None, end: str = None, limit: Optional[int] = None):
    # data_provider.get_kline 现在是异步的，直接 await
    effective_limit = limit
    if effective_limit is None and freq == 'D':
        effective_limit = 200

    data = await data_provider.get_kline(symbol, freq, start, end, limit=effective_limit, is_ui_request=True)
    if not data:
        if freq in ["5min", "30min"]:
            return []
        raise HTTPException(status_code=404, detail="Data not found or error fetching data")
    
    # 自动计算常用指标 (MA, MACD, KDJ, RSI)
    try:
        # 检查是否已经包含预计算指标 (通过 get_local_kline 联表查询获取)
        # 不仅要检查 key 存在，还要检查值不全为 None (至少第一个和最后一个要有值，或者采样检查)
        has_indicators = False
        if data and len(data) > 0:
            # 采样检查：不仅检查最后一条数据，还要检查中间数据
            # 如果只有最后一条有指标，说明数据库只存了最新状态，历史数据仍需计算
            last_item = data[-1]
            
            # 根据周期选择检查的指标
            if freq == 'D':
                check_keys = ['ma5', 'ma10', 'ma20', 'vol_ma10', 'macd', 'macd_dea']
            else:
                # 周线/月线现在也支持 ma5, ma10, ma20, ma60, vol_ma5, vol_ma10, macd 等
                check_keys = ['ma5', 'ma10', 'ma20', 'vol_ma10', 'is_bullish']
            
            # 检查最后一条
            if all(k in last_item and last_item[k] is not None for k in check_keys):
                # 如果数据量较多，额外检查中间一条和开始一条
                if len(data) > 20:
                    mid_item = data[len(data) // 2]
                    first_item = data[0]
                    if all(k in mid_item and mid_item[k] is not None for k in check_keys) and \
                       all(k in first_item and first_item[k] is not None for k in check_keys):
                        has_indicators = True
                else:
                    has_indicators = True
        
        if has_indicators:
            # 如果已经有预计算指标，且数据量足够大，则直接返回
            df = pd.DataFrame(data)
            return fast_clean_df(df)

        last_date = data[-1]['time'] if data else 'none'
        cache_key = f"{symbol}_{freq}_{last_date}_{len(data)}"
        
        # 计算指标是 CPU 密集型任务，但现在方法内部已经处理了线程切换
        df = await ai_service.calculate_technical_indicators(data, cache_key)
        if df is not None and not df.empty:
            # 使用快速清理函数
            return fast_clean_df(df)
    except Exception as e:
        logger.error(f"Error calculating indicators in endpoint: {e}")
        
    return clean_data(data)

@router.get("/market/quote/{symbol}")
async def get_quote(symbol: str):
    # data_provider.get_realtime_quote 现在是异步的
    data = await data_provider.get_realtime_quote(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Quote not found")
    return clean_data(data)

@router.post("/market/quotes")
async def get_quotes(symbols: List[str]):
    """批量获取实时行情"""
    # data_provider.get_realtime_quotes 现在是异步的
    data = await data_provider.get_realtime_quotes(symbols)
    if isinstance(data, dict):
        return clean_data(list(data.values()))
    return clean_data(data)

@router.get("/market/industry/ranking")
async def get_industry_ranking(
    date: Optional[str] = None, 
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """获取行业涨跌幅排名"""
    def _do_query():
        query = db.query(IndustryData)
        if date:
            query = query.filter(IndustryData.trade_date == date)
        else:
            # 获取最新日期
            latest_date = db.query(IndustryData.trade_date).order_by(desc(IndustryData.trade_date)).first()
            if latest_date:
                query = query.filter(IndustryData.trade_date == latest_date[0])
        
        return query.order_by(desc(IndustryData.avg_pct_chg)).limit(limit).all()
    
    results = await asyncio.to_thread(_do_query)
    return clean_data(results)

@router.get("/market/stocks")
async def get_stocks():
    # 现在是异步方法，直接 await
    data = await data_provider.get_stock_basic()
    return clean_data(data) # 返回所有股票，供前端搜索

@router.get("/market/overview")
async def get_market_overview():
    """获取大盘指数概览"""
    # 现在是异步方法，直接 await
    data = await data_provider.get_market_overview()
    return clean_data(data)

@router.get("/strategy/selector")
async def select_stocks(strategy: str = "default", limit: int = 10):
    """
    执行选股策略
    - strategy: default (多维度综合), pullback (强势回调)
    """
    try:
        results = await stock_selector.select_stocks(strategy=strategy, top_n=limit)
        return clean_data(results)
    except Exception as e:
        logger.error(f"Error in select_stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/strategy/logs")
async def get_selector_logs():
    """获取选股执行日志"""
    return selector_logger.get_logs()
