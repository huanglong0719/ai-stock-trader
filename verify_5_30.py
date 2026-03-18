
import asyncio
import os
import sys
import pandas as pd
from datetime import datetime

# 将 backend 路径加入 sys.path
backend_path = os.path.join(os.getcwd(), 'backend')
if backend_path not in sys.path:
    sys.path.append(backend_path)

from app.services.market.market_data_service import market_data_service

async def test_kline(ts_code, freq):
    print(f"\n--- Testing {ts_code} {freq} ---")
    try:
        # 获取 K 线数据，强制触发实时合并
        klines = await market_data_service.get_kline(ts_code, freq=freq, limit=10)
        
        if not klines:
            print(f"FAILED: No klines returned for {freq}")
            return
        
        last_bar = klines[-1]
        print(f"Last Bar Time: {last_bar.get('time')}")
        print(f"Last Bar Close: {last_bar.get('close')}")
        print(f"Last Bar High: {last_bar.get('high')}")
        print(f"Last Bar Low: {last_bar.get('low')}")
        print(f"Last Bar PctChg: {last_bar.get('pct_chg')}%")
        print(f"Last Bar Volume: {last_bar.get('volume')}")
        
        # 验证 pct_chg 是否存在
        has_pct_chg = all('pct_chg' in k for k in klines)
        print(f"All bars have pct_chg: {has_pct_chg}")
        
        # 验证实时价格是否已经合并（通过检查时间是否是当天的最新时间段）
        # 比如 30min 线在 14:00 之后应该显示 14:00 或 14:30 的柱子
        # 5min 线应该显示最近一个 5 分钟的柱子
        print(f"Total bars returned: {len(klines)}")
        
        # 验证历史柱子涨跌幅
        if len(klines) > 1:
            curr = klines[-1]
            prev = klines[-2]
            calc_pct = round((curr['close'] - prev['close']) / prev['close'] * 100, 2)
            print(f"Verification: Manual calc pct_chg = {calc_pct}%, Bar pct_chg = {curr['pct_chg']}%")
            if abs(calc_pct - curr['pct_chg']) < 0.01:
                print(f"SUCCESS: {freq} pct_chg calculation verified.")
            else:
                print(f"FAILED: {freq} pct_chg mismatch!")

    except Exception as e:
        print(f"Error testing {freq}: {e}")
        import traceback
        traceback.print_exc()

async def main():
    # 测试一只活跃股
    ts_code = "002970.SZ" 
    await test_kline(ts_code, "5")
    await test_kline(ts_code, "30")

if __name__ == "__main__":
    asyncio.run(main())
