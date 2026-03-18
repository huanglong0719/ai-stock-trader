
import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.stock_data_service import stock_data_service

def test_bars():
    ts_code = "000001.SZ"
    
    print(f"\n--- Testing Weekly Bars for {ts_code} ---")
    w_bars = stock_data_service.get_local_kline(ts_code, freq='W', limit=5, include_indicators=True)
    for b in w_bars:
        print(f"Date: {b['time']}, Close: {b['close']}, MA5: {b.get('ma5')}, MACD: {b.get('macd')}")

    print(f"\n--- Testing Monthly Bars for {ts_code} ---")
    m_bars = stock_data_service.get_local_kline(ts_code, freq='M', limit=5, include_indicators=True)
    for b in m_bars:
        print(f"Date: {b['time']}, Close: {b['close']}, MA5: {b.get('ma5')}, MACD: {b.get('macd')}")

if __name__ == "__main__":
    test_bars()
