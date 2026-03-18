import os
import sys
import unittest
import asyncio

backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.services.trading_service import trading_service


class TestTradingModule(unittest.TestCase):
    def test_reconcile_account_cash(self):
        async def _run():
            res = await trading_service.reconcile_account_cash()
            self.assertIsInstance(res, dict)
            self.assertTrue("after_total_assets" in res)
            self.assertTrue("after_available_cash" in res)
        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
