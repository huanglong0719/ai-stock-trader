
import asyncio
import pandas as pd
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import market_data_service
from app.services.chat_service import chat_service

async def check_data_distortion(ts_code):
    print(f"--- Checking data for {ts_code} ---")
    
    # 1. Check AI Context
    print("\n[1] AI Trading Context Snippet:")
    context = await chat_service.get_ai_trading_context(ts_code)
    # Print the "【日K线明细" section
    if "【日K线明细" in context:
        start_idx = context.find("【日K线明细")
        end_idx = context.find("【", start_idx + 1)
        if end_idx == -1: end_idx = len(context)
        print(context[start_idx:end_idx])
    else:
        print("AI Context section missing!")

    # 2. Check Raw Data from market_data_service
    print("\n[2] Raw data from market_data_service.get_ai_context_data:")
    data = await market_data_service.get_ai_context_data(ts_code)
    kline_d = data.get('kline_d', [])
    if kline_d:
        df = pd.DataFrame(kline_d)
        cols = ['time', 'close', 'volume', 'ma5', 'ma20', 'macd']
        available_cols = [c for c in cols if c in df.columns]
        print(df[available_cols].tail(5))
    else:
        print("Raw K-line data missing!")

    # 3. Check Quote
    print("\n[3] Real-time Quote:")
    quote = data.get('quote', {})
    print(quote)

if __name__ == "__main__":
    ts_code = "605358.SH"
    asyncio.run(check_data_distortion(ts_code))
