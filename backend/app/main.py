import sys
import io
import os

# 强制设置标准输出/错误为 UTF-8，防止 Windows 下 emoji 输出报错
# 必须在最开始执行
if sys.platform.startswith('win'):
    if hasattr(sys.stdout, 'buffer'):
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    if hasattr(sys.stderr, 'buffer'):
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from app.api.endpoints import router as api_router
from app.api.ai_endpoints import router as ai_router
from app.api.sync_endpoints import router as sync_router
from app.api.trading_endpoints import router as trading_router
from app.api.chat_endpoints import router as chat_router
from app.api.memory_endpoints import router as memory_router
from app.services.data_provider import data_provider
from app.services.data_sync import data_sync_service
from app.services.scheduler import scheduler_manager
from app.services.trading_service import trading_service
from app.services.redis_server_service import redis_server_service
import asyncio
import json
from datetime import datetime
from typing import Dict, Set
from app.services.logger import logger

app = FastAPI(title="AI Trader API")

# 启用 GZip 压缩，显著提升大 JSON（如股票列表）的传输速度
app.add_middleware(GZipMiddleware, minimum_size=1000)

# WebSocket 连接管理器
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, symbol: str):
        await websocket.accept()
        if symbol not in self.active_connections:
            self.active_connections[symbol] = set()
        self.active_connections[symbol].add(websocket)
        logger.info(f"WebSocket 连接建立: {symbol}，当前连接数: {len(self.active_connections[symbol])}")

    def disconnect(self, websocket: WebSocket, symbol: str):
        if symbol in self.active_connections:
            self.active_connections[symbol].remove(websocket)
            if not self.active_connections[symbol]:
                del self.active_connections[symbol]
        logger.info(f"WebSocket 连接断开: {symbol}")

manager = ConnectionManager()

# 配置 CORS
# NOTE: 生产环境应指定具体前端域名，如 ["http://localhost:5173", "https://your-domain.com"]
allowed_origins = ["null"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")
app.include_router(ai_router, prefix="/api")
app.include_router(sync_router, prefix="/api")
app.include_router(chat_router, prefix="/api")
app.include_router(memory_router, prefix="/api")
app.include_router(trading_router, prefix="/api/trading")

@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(redis_server_service.ensure_started)
    # 1. 初始化数据库
    data_sync_service.init_db()
    # 2. 启动定时任务
    await scheduler_manager.start()
    # 3. 执行每日持仓结算 (T+1)
    logger.info("Initializing position settlement (T+1)...")
    try:
        await trading_service.settle_positions()
    except Exception as e:
        logger.error(f"Failed to settle positions at startup: {e}")
    # 4. 初始同步股票列表与最近数据（异步后台执行）
    # 用户反馈：不要在启动时自动同步数据，避免开发调试时频繁触发
    # 数据同步应由定时任务或手动触发完成
    
    # 始终同步股票基本列表 (耗时短且必要) - 暂时保留，如果用户也觉得慢可以禁用
    # asyncio.create_task(data_sync_service.sync_all_stocks())
    
    # 禁用启动时的自动回溯同步，避免重启后长时间占用 CPU 和网络
    # if not is_trading_time():
    #     logger.info("Non-trading hours detected. Starting automatic data backfill...")
    #     asyncio.create_task(data_sync_service.backfill_data(days=3))
    # else:
    #     logger.info("Trading hours detected. Skipping automatic data backfill to save resources.")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler_manager.shutdown()
    await asyncio.to_thread(redis_server_service.stop)
    try:
        from app.services.market.tushare_client import tushare_client
        await tushare_client.close()
    except Exception as e:
        logger.warning(f"Shutdown: failed to close tushare_client: {e}")

@app.get("/")
async def root():
    return {"message": "AI Trader System is running"}

@app.websocket("/ws/quote/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await manager.connect(websocket, symbol)
    try:
        while True:
            # data_provider.get_realtime_quote 现在是异步的，直接 await
            quote = await data_provider.get_realtime_quote(symbol)
            if quote:
                # 增加心跳/时间戳
                quote['server_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                await websocket.send_json(quote)
            else:
                # 如果没有行情数据，发送心跳包保持连接
                await websocket.send_json({"type": "heartbeat", "time": datetime.now().isoformat()})
            
            # 每隔 3 秒更新一次
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        manager.disconnect(websocket, symbol)
    except Exception as e:
        import traceback
        logger.error(f"WebSocket 错误 ({symbol}): {e}")
        traceback.print_exc()
        manager.disconnect(websocket, symbol)
        try:
            await websocket.close()
        except Exception:
            # 忽略关闭时的异常，连接可能已断开
            pass
