
import asyncio
import sys
import os
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.append(os.path.join(os.getcwd(), 'backend'))

from app.services.market.market_data_service import market_data_service
from app.services.market.tushare_client import tushare_client

def _err(msg: str):
    sys.stderr.write(f"{msg}\n")


async def _check_minute_kline(ts_code: str, freq: str):
    kline = await market_data_service.get_kline(ts_code, freq=freq, limit=200)
    if not kline:
        _err(f"{freq} 无数据")
        return
    df = pd.DataFrame(kline)
    if df.empty:
        _err(f"{freq} 结果为空")
        return
    df["time"] = df["time"].astype(str)
    today_str = datetime.now().strftime('%Y-%m-%d')
    today_df = df[df["time"].str.startswith(today_str)]
    times = today_df["time"].tolist() if not today_df.empty else df["time"].tolist()
    has_1130 = any(t.endswith("11:30:00") or t.endswith("11:30") for t in times)
    has_1300 = any(t.endswith("13:00:00") or t.endswith("13:00") for t in times)
    has_1305 = any(t.endswith("13:05:00") or t.endswith("13:05") for t in times)
    has_1330 = any(t.endswith("13:30:00") or t.endswith("13:30") for t in times)
    last_time = times[-1] if times else ""
    _err(f"{freq} bars={len(times)} 11:30={has_1130} 13:00={has_1300} 13:05={has_1305} 13:30={has_1330} last={last_time}")


async def test_minute_debug():
    ts_code = "000001.SZ"
    _err(f"=== Debugging minute K-line for {ts_code} ===")
    try:
        from app.services.market.market_utils import is_trading_time
        trading = is_trading_time()
        _err(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        _err(f"is_trading_time(): {trading}")
        latest_trade_date = await market_data_service.get_last_trade_date(include_today=True)
        _err(f"Latest trade date: {latest_trade_date}")
        await _check_minute_kline(ts_code, "5min")
        await _check_minute_kline(ts_code, "30min")
    finally:
        await tushare_client.close()

if __name__ == "__main__":
    asyncio.run(test_minute_debug())
