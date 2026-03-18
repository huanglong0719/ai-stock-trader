
import asyncio
import os
import sys
import json
import pandas as pd

# 确保能找到 app 模块
sys.path.append(os.getcwd())

from app.services.ai_service import ai_service
from app.services.data_provider import data_provider

async def get_jerry_data():
    symbol = '002353.SZ'
    klines = await data_provider.get_kline(symbol, freq='D', start_date='20250101')
    quote = await data_provider.get_realtime_quote(symbol)
    
    df = ai_service.calculate_technical_indicators(klines)
    
    if not df.empty:
        latest = df.iloc[-1]
        print(json.dumps({
            "price": quote.get('price'),
            "pct_chg": quote.get('pct_chg'),
            "ma5": latest.get('ma5'),
            "ma10": latest.get('ma10'),
            "ma20": latest.get('ma20'),
            "ma60": latest.get('ma60'),
            "vwap": quote.get('vwap'),
            "vol": quote.get('vol'),
            "amount": quote.get('amount')
        }, indent=2))
    else:
        print("No data found")

if __name__ == "__main__":
    asyncio.run(get_jerry_data())
