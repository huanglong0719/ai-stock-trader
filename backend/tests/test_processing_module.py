import os
import sys
import unittest
import asyncio

backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.services.indicator_service import IndicatorService
from app.services.data_provider import data_provider


async def _pick_ts_code() -> str:
    try:
        top_codes = await data_provider.get_market_turnover_top_codes(top_n=50)
        if isinstance(top_codes, list) and top_codes:
            return top_codes[0]
    except Exception:
        pass
    return "000001.SZ"


class TestProcessingModule(unittest.TestCase):
    def test_indicator_calc_in_memory(self):
        async def _run():
            ts_code = await _pick_ts_code()
            svc = IndicatorService()
            res = await svc._calculate_single_stock(ts_code, target_date="", force_no_cache=True, return_full_history=False, local_only=True)
            self.assertIsInstance(res, list)
            if not res:
                self.fail("empty indicator result")
                return
            row = res[-1]
            self.assertEqual(row.get("ts_code"), ts_code)
            self.assertTrue("ma5" in row)
            self.assertTrue("macd" in row)
        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
