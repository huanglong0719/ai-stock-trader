import os
import sys
import unittest
import asyncio

backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if backend_dir not in sys.path:
    sys.path.append(backend_dir)

from app.services.data_provider import data_provider
from app.services.chat_service import chat_service


async def _pick_ts_code() -> str:
    try:
        top_codes = await data_provider.get_market_turnover_top_codes(top_n=50)
        if isinstance(top_codes, list) and top_codes:
            return top_codes[0]
    except Exception:
        pass
    return "000001.SZ"


class TestAiModule(unittest.TestCase):
    def test_ai_context_basic(self):
        async def _run():
            ts_code = await _pick_ts_code()
            ctx = await chat_service.get_ai_trading_context(ts_code)
            self.assertIsInstance(ctx, str)
            self.assertGreater(len(ctx), 50)
        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()
