import asyncio
import os
import sys
import logging

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.indicator_service import indicator_service

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting full indicator recalculation...")
    try:
        # force_full=True will recalculate everything for all stocks
        await indicator_service.calculate_all_indicators(force_full=True)
        logger.info("Indicator recalculation complete!")
    except Exception as e:
        logger.error(f"Critical error during indicator recalculation: {e}")

if __name__ == "__main__":
    asyncio.run(main())
