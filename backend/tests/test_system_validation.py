import os
import sys
import unittest
import asyncio

backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.services.data_provider import data_provider
from app.services.market.market_data_service import market_data_service
from app.services.indicator_service import indicator_service
from app.services.chat_service import chat_service
from app.services.trading_service import trading_service
from app.services.stock_selector import stock_selector


async def _pick_ts_code() -> str:
    try:
        top_codes = await data_provider.get_market_turnover_top_codes(top_n=50)
        if isinstance(top_codes, list) and top_codes:
            return top_codes[0]
    except Exception:
        pass
    return "000001.SZ"


class TestSystemValidation(unittest.TestCase):
    def test_full_module_flow(self):
        async def _run():
            ts_code = await _pick_ts_code()
            with market_data_service.cache_scope("system_validation"):
                ctx = await market_data_service.get_ai_context_data(ts_code, no_side_effect=True, cache_scope="system_validation")
            self.assertIsInstance(ctx, dict)
            ind = await indicator_service._calculate_single_stock(ts_code, target_date="", force_no_cache=True, return_full_history=False, local_only=True)
            self.assertIsInstance(ind, list)
            ai_ctx = await chat_service.get_ai_trading_context(ts_code)
            self.assertIsInstance(ai_ctx, str)
            res = await trading_service.reconcile_account_cash()
            self.assertIsInstance(res, dict)
            scan = await stock_selector.scan_noon_opportunities()
            self.assertIsInstance(scan, dict)
        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
