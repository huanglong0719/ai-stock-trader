import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import sys
import os
import json
import asyncio
from datetime import date, datetime

# Add project root to path
sys.path.insert(0, r"D:\木偶说\backend")

# Mock the database setup before importing services
mock_db_session_module = MagicMock()
class DummyBase:
    metadata = MagicMock() # SQLAlchemy models often access Base.metadata
    pass
mock_db_session_module.Base = DummyBase
mock_db_session_module.SessionLocal = MagicMock()
sys.modules['app.db.session'] = mock_db_session_module

# We need to mock app.core.config.settings carefully
# The MagicMock needs to have string attributes for URLs to satisfy httpx validation
mock_settings = MagicMock()
mock_settings.ENABLE_AUTO_TRADE = True
mock_settings.MIMO_MODEL = "test-model"
mock_settings.MIMO_API_KEY = "sk-test"
mock_settings.MIMO_BASE_URL = "https://api.test.com/v1"
mock_settings.DEEPSEEK_API_KEY = "sk-test"
mock_settings.DEEPSEEK_BASE_URL = "https://api.test.com/v1"

# Patch settings in sys.modules
# We need to patch the module 'app.core.config' so that 'from app.core.config import settings' works
mock_config_module = MagicMock()
mock_config_module.settings = mock_settings
sys.modules['app.core.config'] = mock_config_module

# Also patch OpenAI to avoid actual network calls during init
sys.modules['openai'] = MagicMock()

from app.models.stock_models import TradingPlan, Position, Account, Stock

class TestSystemIntegrity(unittest.TestCase):

    def setUp(self):
        # Setup common mocks
        self.mock_db = MagicMock()
        self.mock_db_session = MagicMock()
        self.mock_db.return_value = self.mock_db_session
        
        # Patching imports in trading_service
        self.patcher_db = patch('app.services.trading_service.SessionLocal', side_effect=self.mock_db)
        self.patcher_dp = patch('app.services.trading_service.data_provider')
        # We need to patch the ai_service INSTANCE in trading_service, not the class
        self.patcher_ai = patch('app.services.trading_service.ai_service')
        self.patcher_search = patch('app.services.search_service.search_service')
        self.patcher_chat = patch('app.services.chat_service.chat_service')
        
        self.mock_session_cls = self.patcher_db.start()
        self.mock_data_provider = self.patcher_dp.start()
        self.mock_ai_service = self.patcher_ai.start()
        self.mock_search_service = self.patcher_search.start()
        self.mock_chat_service = self.patcher_chat.start()

        self.mock_data_provider.get_realtime_quotes = AsyncMock(return_value={})
        self.mock_data_provider.get_market_snapshot = AsyncMock(return_value={})
        self.mock_data_provider.get_realtime_quote = AsyncMock(return_value={})
        self.mock_data_provider.get_sector_context = AsyncMock(return_value={"industry": "未知"})

        self.mock_ai_service.analyze_market_snapshot = AsyncMock(return_value="Market OK")
        self.mock_ai_service.analyze_realtime_trade_signal_v3 = AsyncMock(return_value={"action": "WAIT", "reason": "Testing"})
        self.mock_ai_service.analyze_selling_opportunity = AsyncMock(return_value={"action": "HOLD"})
        self.mock_ai_service.analyze_portfolio_adjustment = AsyncMock(return_value=[])

        self.mock_search_service.search_stock_info = AsyncMock(return_value="")
        self.mock_chat_service.get_ai_trading_context = AsyncMock(return_value="CTX: mock trading context")
        
        # Import TradingService after patching
        from app.services.trading_service import TradingService
        self.trading_service = TradingService()

    def tearDown(self):
        self.patcher_db.stop()
        self.patcher_dp.stop()
        self.patcher_ai.stop()
        self.patcher_search.stop()
        self.patcher_chat.stop()

    def test_buy_flow_data_integrity(self):
        """
        验证买入流程中：
        1. 账户信息(account_info)是否正确获取并传递
        2. 板块联动(sector_context)是否正确注入
        3. 盘口数据(bid_ask)是否正确注入
        """
        print("\n[Test] Checking Buy Flow Data Integrity...")
        
        # 1. Mock DB Data
        mock_plan = MagicMock(spec=TradingPlan)
        mock_plan.id = 1
        mock_plan.ts_code = "000001.SZ"
        mock_plan.strategy_name = "TestStrategy"
        mock_plan.buy_price_limit = 10.0
        mock_plan.reason = "Test Reason"
        mock_plan.executed = False
        mock_plan.track_status = None
        mock_plan.frozen_amount = 0.0
        mock_plan.limit_price = 0.0
        mock_plan.order_type = "MARKET"
        
        mock_unified = MagicMock()
        mock_unified.name = "TestStock"
        mock_unified.total_assets = 100000.0
        mock_unified.available_cash = 50000.0
        mock_unified.market_value = 50000.0
        mock_unified.total_pnl_pct = 5.0
        
        # Configure DB queries
        self.mock_db_session.query.return_value.filter.return_value.all.return_value = [mock_plan]
        # For .first() calls (Stock and Account)
        self.mock_db_session.query.return_value.filter.return_value.first.return_value = mock_unified
        self.mock_db_session.query.return_value.first.return_value = mock_unified # Handle non-filtered query
        self.mock_db_session.query.return_value.get.return_value = mock_plan

        # 2. Mock Data Provider
        self.mock_data_provider.get_realtime_quotes.return_value = {
            "000001.SZ": {
                "price": 10.0,
                "vol": 1000, 
                "bid_ask": {
                    "s1_p": 10.01, "s1_v": 500,
                    "b1_p": 9.99, "b1_v": 300
                }
            }
        }
        self.mock_data_provider.get_realtime_quote.return_value = self.mock_data_provider.get_realtime_quotes.return_value["000001.SZ"]
        self.mock_data_provider.get_sector_context.return_value = {
            "industry": "Banking",
            "avg_pct": 2.5,
            "leaders": ["BankA: +3%", "BankB: +2%"]
        }
        self.mock_data_provider.is_trading_time.return_value = True
        
        # 3. Mock AI & Search
        self.mock_ai_service.analyze_market_snapshot.return_value = "Market OK"
        self.mock_ai_service.analyze_realtime_trade_signal_v3.return_value = {"action": "WAIT", "reason": "Testing"}
        self.mock_search_service.search_stock_info.return_value = "News: Good stuff"

        # 4. Run Execution
        asyncio.run(self.trading_service.check_and_execute_plans())
        
        # 5. Verify AI Call Arguments
        args, kwargs = self.mock_ai_service.analyze_realtime_trade_signal_v3.call_args
        
        passed_account_info = kwargs.get('account_info')
        self.assertIsNotNone(passed_account_info, "❌ account_info was NOT passed to AI!")
        self.assertEqual(passed_account_info['total_assets'], 100000.0)
        print("✅ Account Info passed correctly.")
        
        passed_search_info = kwargs.get('search_info')
        self.assertIn("【板块联动】", passed_search_info, "❌ Sector info missing in search_info")
        self.assertIn("Banking", passed_search_info)
        self.assertIn("【实时五档盘口】", passed_search_info, "❌ Handicap info missing in search_info")
        print("✅ Sector & Handicap Info passed correctly.")

    def test_sell_flow_data_integrity(self):
        """
        验证卖出流程中：
        1. 账户信息是否传递
        2. 盘口/板块信息是否注入 market_status
        """
        print("\n[Test] Checking Sell Flow Data Integrity...")
        
        # 1. Mock DB Data
        mock_pos = MagicMock(spec=Position)
        mock_pos.id = 1
        mock_pos.ts_code = "000002.SZ"
        mock_pos.vol = 100
        mock_pos.available_vol = 100
        mock_pos.avg_price = 10.0
        mock_pos.current_price = 10.0
        
        mock_account = MagicMock(spec=Account)
        mock_account.total_assets = 200000.0
        mock_account.available_cash = 100000.0
        mock_account.market_value = 100000.0
        mock_account.total_pnl_pct = 10.0
        
        # Reset mocks
        self.mock_db_session.reset_mock()
        
        # Setup Query returns
        # .all() returns positions
        self.mock_db_session.query.return_value.filter.return_value.all.return_value = [mock_pos]
        # .first() returns account
        self.mock_db_session.query.return_value.first.return_value = mock_account
        self.mock_db_session.query.return_value.filter.return_value.first.return_value = None # No trade plan/record
        self.mock_db_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
        self.mock_db_session.query.return_value.get.return_value = mock_pos

        # 2. Mock Data Provider
        self.mock_data_provider.get_realtime_quotes.return_value = {
            "000002.SZ": {
                "price": 11.0,
                "vol": 2000,
                "pct_chg": -3.5,
                "bid_ask": {"s1_p": 11.01, "s1_v": 100}
            }
        }
        self.mock_data_provider.get_realtime_quote.return_value = self.mock_data_provider.get_realtime_quotes.return_value["000002.SZ"]
        self.mock_data_provider.get_sector_context.return_value = {
            "industry": "RealEstate",
            "avg_pct": -1.0
        }
        self.mock_data_provider.is_trading_time.return_value = True
        
        # 3. Mock AI
        self.mock_ai_service.analyze_market_snapshot.return_value = "Market Weak"
        self.mock_ai_service.analyze_selling_opportunity.return_value = {"action": "HOLD"}

        # 4. Run Execution
        asyncio.run(self.trading_service.check_positions_and_sell())
        
        # 5. Verify
        self.assertTrue(self.mock_ai_service.analyze_selling_opportunity.called, "❌ AI analyze_selling_opportunity NOT called!")

        args, kwargs = self.mock_ai_service.analyze_selling_opportunity.call_args
        
        passed_account = kwargs.get('account_info')
        self.assertIsNotNone(passed_account, "❌ account_info NOT passed in sell flow")
        self.assertEqual(passed_account['total_assets'], 200000.0)
        print("✅ Account Info passed in Sell Flow.")
        
        passed_handicap_info = kwargs.get('handicap_info', "")
        self.assertIn("【实时五档盘口】", passed_handicap_info, "❌ Handicap missing in handicap_info")

        passed_search_info = kwargs.get('search_info', "")
        self.assertIn("【板块联动】", passed_search_info, "❌ Sector info missing in search_info")
        print("✅ Sector & Handicap Info passed correctly (Sell Flow).")

    def test_suspension_filtering(self):
        """
        验证停牌股票 (Vol=0) 是否被正确过滤
        """
        print("\n[Test] Checking Suspension Filtering...")
        
        # Setup Plan for Suspended Stock
        mock_plan = MagicMock(spec=TradingPlan)
        mock_plan.ts_code = "000003.SZ" 
        
        self.mock_db_session.query.return_value.filter.return_value.all.return_value = [mock_plan]
        # Make sure account query works so it doesn't crash before checking volume
        mock_account = MagicMock()
        self.mock_db_session.query.return_value.first.return_value = mock_account
        
        # Mock Quote with Vol=0
        self.mock_data_provider.get_realtime_quotes.return_value = {
            "000003.SZ": {
                "price": 10.0,
                "vol": 0, # SUSPENDED!
                "bid_ask": {}
            }
        }
        self.mock_data_provider.is_trading_time.return_value = True
        
        # Run
        asyncio.run(self.trading_service.check_and_execute_plans())
        
        # Verify AI NOT called
        self.mock_ai_service.analyze_realtime_trade_signal_v3.assert_not_called()
        print("✅ Suspended stock correctly skipped (Buy Flow).")

class TestSchedulerEntrustmentMonitor(unittest.TestCase):
    def setUp(self):
        self.patcher_dp = patch('app.services.scheduler.data_provider')
        self.patcher_ts = patch('app.services.scheduler.trading_service')
        self.patcher_monitor = patch('app.services.scheduler.monitor_service')

        self.mock_data_provider = self.patcher_dp.start()
        self.mock_trading_service = self.patcher_ts.start()
        self.mock_monitor_service = self.patcher_monitor.start()

        self.mock_data_provider.is_trading_time.return_value = True
        self.mock_trading_service.execute_pending_entrustments = AsyncMock(
            return_value={"buy_executed": 0, "sell_executed": 0}
        )
        self.mock_monitor_service.log_job_start = AsyncMock(return_value=1)
        self.mock_monitor_service.log_job_end = AsyncMock(return_value=None)

        from app.services.scheduler import SchedulerManager
        self.scheduler_manager = SchedulerManager()

    def tearDown(self):
        self.patcher_dp.stop()
        self.patcher_ts.stop()
        self.patcher_monitor.stop()

    def test_entrustment_monitor_skips_when_not_trading(self):
        self.mock_data_provider.is_trading_time.return_value = False
        asyncio.run(self.scheduler_manager.entrustment_monitor_job())
        self.mock_trading_service.execute_pending_entrustments.assert_not_called()

    def test_entrustment_monitor_calls_pending_without_logging_when_zero(self):
        asyncio.run(self.scheduler_manager.entrustment_monitor_job())
        self.mock_trading_service.execute_pending_entrustments.assert_called_once()
        self.mock_monitor_service.log_job_start.assert_not_called()

    def test_entrustment_monitor_logs_when_executed(self):
        self.mock_trading_service.execute_pending_entrustments.return_value = {"buy_executed": 1, "sell_executed": 2}
        asyncio.run(self.scheduler_manager.entrustment_monitor_job())
        self.mock_trading_service.execute_pending_entrustments.assert_called_once()
        self.mock_monitor_service.log_job_start.assert_called_once()
        self.mock_monitor_service.log_job_end.assert_called_once()

    def test_entrustment_monitor_skips_when_trade_monitor_locked(self):
        async def _run():
            await self.scheduler_manager._trade_monitor_lock.acquire()
            try:
                await self.scheduler_manager.entrustment_monitor_job()
            finally:
                self.scheduler_manager._trade_monitor_lock.release()

        asyncio.run(_run())
        self.mock_trading_service.execute_pending_entrustments.assert_not_called()

if __name__ == '__main__':
    unittest.main()
