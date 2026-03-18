import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.indicator_service import indicator_service
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import asyncio
import argparse

async def run_recalc():
    parser = argparse.ArgumentParser(description='Recalculate indicators')
    parser.add_argument('--full', action='store_true', help='Force full recalculation')
    parser.add_argument('--codes', type=str, help='Comma separated list of ts_codes to recalculate')
    args = parser.parse_args()

    logger.info(f"Starting manual indicator recalculation ({'full' if args.full else 'incremental'})...")
    try:
        if args.codes:
            codes = [c.strip() for c in args.codes.split(',')]
            await indicator_service.calculate_for_codes(codes, force_full=args.full)
        else:
            await indicator_service.calculate_all_indicators(force_full=args.full)
        logger.info("Indicator recalculation complete.")
    except Exception as e:
        logger.error(f"Recalculation failed: {e}")

if __name__ == "__main__":
    asyncio.run(run_recalc())
