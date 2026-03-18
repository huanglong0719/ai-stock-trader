
import asyncio
import os
import sys

# 设置项目根目录
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 确保可以导入 backend.app
if root_dir not in sys.path:
    sys.path.append(root_dir)
# 也可以尝试直接添加 backend 目录
backend_dir = os.path.join(root_dir, "backend")
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.services.indicator_service import indicator_service
from app.services.logger import logger

async def main():
    logger.info("Starting global indicator backfill for all stocks...")
    try:
        # 不传 ts_codes 则默认处理所有股票
        await indicator_service.backfill_historical_indicators()
        logger.info("Global backfill completed successfully.")
    except Exception as e:
        logger.error(f"Global backfill failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(main())
