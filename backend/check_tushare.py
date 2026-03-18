import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.market.tushare_client import tushare_client

async def check_tushare_fields():
    ts_code = "600519.SH"
    print(f"检查 Tushare fina_indicator 字段: {ts_code}")
    df = await asyncio.to_thread(tushare_client.get_fina_indicator, ts_code=ts_code)
    if not df.empty:
        print("返回的列名:")
        print(df.columns.tolist())
        print("\n第一行数据:")
        print(df.iloc[0].to_dict())
    else:
        print("未获取到数据")

if __name__ == "__main__":
    asyncio.run(check_tushare_fields())
