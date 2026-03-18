import os
import sys
import pandas as pd
from datetime import datetime

# 设置环境变量，确保能找到 app 模块
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

from app.services.tdx_data_service import tdx_service

def test_tick_data(ts_code='000001.SZ'):
    print(f"--- Testing Tick Data Fetching for {ts_code} ---")
    
    if not tdx_service.connect():
        print("Error: Could not connect to TDX")
        return

    market, code = tdx_service._get_market_code(ts_code)
    
    try:
        # 1. 测试获取当日分笔数据 (get_transaction_data)
        # 参数: market, code, start, count
        print("\n1. Testing get_transaction_data (Today's Ticks):")
        ticks = tdx_service.api.get_transaction_data(market, code, 0, 100)
        if ticks:
            df_ticks = pd.DataFrame(ticks)
            print(f"Successfully fetched {len(df_ticks)} ticks via API.")
            
        # 2. 测试封装后的 fetch_ticks (带 Redis 缓存)
        print("\n2. Testing fetch_ticks (with Redis cache):")
        df_cached = tdx_service.fetch_ticks(ts_code, 200)
        print(f"Fetched {len(df_cached)} ticks (cached/new).")
        print("Last 10 ticks:")
        print(df_cached.tail(10).to_string())
        
        # 再次调用，观察是否从缓存读取
        print("\n3. Testing second call to fetch_ticks:")
        df_cached_2 = tdx_service.fetch_ticks(ts_code, 50)
        print(f"Fetched {len(df_cached_2)} ticks.")
        
    except Exception as e:
        print(f"Error during testing: {e}")

        # 2. 测试获取历史分笔数据 (get_history_transaction_data)
        # 参数: market, code, start, count, date (YYYYMMDD)
        yesterday = "20260129" # 根据当前环境日期 2026-01-30，尝试获取昨天
        print(f"\n2. Testing get_history_transaction_data (Date: {yesterday}):")
        h_ticks = tdx_service.api.get_history_transaction_data(market, code, 0, 100, int(yesterday))
        if h_ticks:
            df_h_ticks = pd.DataFrame(h_ticks)
            print(f"Successfully fetched {len(df_h_ticks)} history ticks.")
            print(df_h_ticks[['time', 'price', 'vol', 'buyorsell']].head())
        else:
            print(f"No history ticks returned for {yesterday}.")

    except Exception as e:
        print(f"Error during tick fetching: {e}")
    finally:
        tdx_service.api.disconnect()

if __name__ == "__main__":
    # 可以通过命令行参数指定股票代码
    target_code = sys.argv[1] if len(sys.argv) > 1 else '000001.SZ'
    test_tick_data(target_code)
