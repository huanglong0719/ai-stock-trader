
import asyncio
import os
import sys

# 将后端目录添加到路径中
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.data_sync import DataSyncService
from app.db.session import SessionLocal

async def backfill_missing_data():
    print("Starting backfill for 2026-01-09 data...")
    sync_service = DataSyncService()
    target_date = "20260109"
    
    try:
        # 同步日线数据和每日基础指标
        print(f"Syncing daily bars and daily basics for {target_date}...")
        await sync_service.sync_daily_data(trade_date=target_date, sync_industry_history=True)
        print("Backfill completed successfully.")
    except Exception as e:
        print(f"Error during backfill: {e}")

if __name__ == "__main__":
    asyncio.run(backfill_missing_data())
