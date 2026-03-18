from app.services.data_sync import data_sync_service
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_full_backfill():
    # Backfill for 3 years (approx 1095 days) to ensure solid history for weekly/monthly analysis
    # This will also fix adj_factor for weekly/monthly bars
    days = 1095 
    logger.info(f"Starting FORCE full backfill for last {days} days...")
    
    try:
        await data_sync_service.backfill_data(days=days)
        logger.info("Full backfill execution finished successfully.")
    except Exception as e:
        logger.error(f"Backfill failed: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_full_backfill())
