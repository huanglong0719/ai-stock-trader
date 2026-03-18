import asyncio
from app.services.ai.ai_client import ai_client
from app.services.ai.analysis_service import analysis_service

class AIService:
    """
    Facade class for backward compatibility.
    Delegates calls to specialized services:
    - ai_client: for API clients and low-level calls
    - analysis_service: for high-level business logic
    """

    @property
    def mimo_client(self):
        return ai_client.mimo_client

    @property
    def ds_client(self):
        return ai_client.ds_client

    async def calculate_technical_indicators(self, *args, **kwargs):
        return await asyncio.to_thread(analysis_service.calculate_technical_indicators, *args, **kwargs)

    async def get_trading_status(self, *args, **kwargs):
        return await analysis_service.get_trading_status(*args, **kwargs)

    async def analyze_stock(self, *args, **kwargs):
        return await analysis_service.analyze_stock(*args, **kwargs)

    async def analyze_realtime_trade_signal_v3(self, *args, **kwargs):
        return await analysis_service.analyze_realtime_trade_signal_v3(*args, **kwargs)

    async def analyze_selling_opportunity(self, *args, **kwargs):
        return await analysis_service.analyze_selling_opportunity(*args, **kwargs)

    async def analyze_sell_signal(self, *args, **kwargs):
        return await analysis_service.analyze_sell_signal(*args, **kwargs)

    async def analyze_rebalance_signal(self, *args, **kwargs):
        return await analysis_service.analyze_rebalance_signal(*args, **kwargs)

    async def analyze_holding_strategy(self, *args, **kwargs):
        return await analysis_service.analyze_holding_strategy(*args, **kwargs)

    async def analyze_stock_for_plan(self, *args, **kwargs):
        return await analysis_service.analyze_stock_for_plan(*args, **kwargs)

    async def analyze_market_snapshot(self, *args, **kwargs):
        return await analysis_service.analyze_market_snapshot(*args, **kwargs)

    async def analyze_portfolio_adjustment(self, *args, **kwargs):
        return await analysis_service.analyze_portfolio_adjustment(*args, **kwargs)

    async def analyze_late_session_opportunity(self, *args, **kwargs):
        return await analysis_service.analyze_late_session_opportunity(*args, **kwargs)

    async def decide_late_session_strategy(self, *args, **kwargs):
        return await analysis_service.decide_late_session_strategy(*args, **kwargs)

    async def calculate_smart_trailing_stop(self, *args, **kwargs):
        return await asyncio.to_thread(analysis_service.calculate_smart_trailing_stop, *args, **kwargs)

    async def _call_ai_api(self, *args, **kwargs):
        return await asyncio.to_thread(ai_client.call_ai_best_effort, *args, **kwargs)

# Global instance
ai_service = AIService()
