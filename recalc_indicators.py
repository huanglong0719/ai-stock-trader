import asyncio
import sys
import os

# 将当前目录添加到路径中以导入 app
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicator_service import IndicatorService
from app.services.logger import logger

async def main():
    service = IndicatorService()
    # force_full=True 会重算所有股票的所有历史指标
    # 这将确保指标与重建后的周/月线数据一致
    logger.info("开始全面重新计算所有股票指标...")
    await service.calculate_all_indicators(force_full=True)
    logger.info("所有股票指标重新计算完成。")

if __name__ == "__main__":
    asyncio.run(main())
