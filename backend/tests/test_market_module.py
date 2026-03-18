import os
import sys
import unittest
import asyncio

backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.services.data_provider import data_provider
from app.services.market.market_data_service import market_data_service


async def _pick_ts_code() -> str:
    try:
        top_codes = await data_provider.get_market_turnover_top_codes(top_n=50)
        if isinstance(top_codes, list) and top_codes:
            return top_codes[0]
    except Exception:
        pass
    return "000001.SZ"


class TestMarketModule(unittest.TestCase):
    def test_kline_and_stats_integrity(self):
        async def _run():
            ts_code = await _pick_ts_code()
            ctx = await market_data_service.get_ai_context_data(ts_code, no_side_effect=True)
            self.assertIsInstance(ctx, dict)
            d = ctx.get("kline_d") or []
            w = ctx.get("weekly_k") or []
            m = ctx.get("monthly_k") or []
            k5 = ctx.get("kline_5m") or []
            k30 = ctx.get("kline_30m") or []
            self.assertGreaterEqual(len(d), 30)
            self.assertGreaterEqual(len(w), 12)
            self.assertGreaterEqual(len(m), 6)
            self.assertIsInstance(k5, list)
            self.assertIsInstance(k30, list)
            stats = ctx.get("stats") or {}
            self.assertTrue(stats.get("h_5y") is not None)
            self.assertTrue(stats.get("l_5y") is not None)
        asyncio.run(_run())

    def test_realtime_quote_basic(self):
        async def _run():
            ts_code = await _pick_ts_code()
            quote = await market_data_service.get_realtime_quote(ts_code)
            self.assertIsInstance(quote, dict)
            if not quote:
                self.fail("empty quote")
                return
            self.assertEqual(quote.get("ts_code"), ts_code)
        asyncio.run(_run())

    def test_cache_scope_isolation(self):
        async def _run():
            ts_code = await _pick_ts_code()
            with market_data_service.cache_scope("market_module_a"):
                await market_data_service.get_kline(ts_code, freq="D", limit=5, local_only=True)
            with market_data_service.cache_scope("market_module_b"):
                await market_data_service.get_kline(ts_code, freq="D", limit=5, local_only=True)
            self.assertIn("market_module_a", market_data_service._scoped_kline_cache)
            self.assertIn("market_module_b", market_data_service._scoped_kline_cache)
            self.assertIsNot(
                market_data_service._scoped_kline_cache.get("market_module_a"),
                market_data_service._scoped_kline_cache.get("market_module_b"),
            )
        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
