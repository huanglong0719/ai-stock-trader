import asyncio
import pandas as pd
from app.services.chat_service import ChatService
from app.services.data_provider import data_provider

async def test():
    chat_service = ChatService()
    ts_code = "600699.SH"
    
    print(f"Fetching context for {ts_code}...")
    context = await chat_service.get_ai_trading_context(ts_code)
    print("\n--- AI Context Snippet ---")
    print(context)

if __name__ == "__main__":
    asyncio.run(test())
