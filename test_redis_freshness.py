
import pandas as pd
from datetime import datetime, time, timedelta
import sys
import os

# Mocking is_trading_time
def is_trading_time(dt=None):
    if dt is None:
        dt = datetime.now()
    if dt.weekday() >= 5:
        return False
    curr_time = dt.time()
    if (time(9, 15) <= curr_time <= time(11, 35)) or (time(13, 0) <= curr_time <= time(15, 1)):
        return True
    return False

def check_redis_freshness(last_redis_time, latest_trade_date, now_dt=None):
    if now_dt is None:
        now_dt = datetime.now()
        
    last_redis_date = last_redis_time.strftime('%Y%m%d')
    
    is_redis_fresh = False
    if last_redis_date >= latest_trade_date:
        if not is_trading_time(now_dt):
            is_redis_fresh = True
        else:
            # 交易时间内，检查时间差 (2分钟内认为新鲜)
            time_diff = (now_dt - last_redis_time).total_seconds()
            if 0 <= time_diff < 120:
                is_redis_fresh = True
                print(f"Fresh: diff {time_diff:.1f}s")
            elif time_diff < 0:
                print(f"Future data: diff {time_diff:.1f}s")
            else:
                print(f"Stale: diff {time_diff:.1f}s")
    else:
        print(f"Old date: last_redis={last_redis_date}, latest={latest_trade_date}")
        
    return is_redis_fresh

def test_freshness_scenarios():
    latest_trade_date = "20260129"
    
    # Scenario 1: Non-trading time, Redis date is today
    now_dt = datetime(2026, 1, 29, 18, 0, 0) # 6 PM
    last_redis = datetime(2026, 1, 29, 15, 0, 0)
    print(f"Scenario 1 (After close): {check_redis_freshness(last_redis, latest_trade_date, now_dt)}")
    
    # Scenario 2: Trading time, Redis is 1 minute old
    now_dt = datetime(2026, 1, 29, 10, 0, 0) # 10 AM
    last_redis = datetime(2026, 1, 29, 9, 59, 0)
    print(f"Scenario 2 (Trading, 1m old): {check_redis_freshness(last_redis, latest_trade_date, now_dt)}")
    
    # Scenario 3: Trading time, Redis is 5 minutes old
    now_dt = datetime(2026, 1, 29, 10, 0, 0) # 10 AM
    last_redis = datetime(2026, 1, 29, 9, 55, 0)
    print(f"Scenario 3 (Trading, 5m old): {check_redis_freshness(last_redis, latest_trade_date, now_dt)}")
    
    # Scenario 4: Trading time, Redis is from yesterday
    now_dt = datetime(2026, 1, 29, 10, 0, 0) # 10 AM
    last_redis = datetime(2026, 1, 28, 15, 0, 0)
    print(f"Scenario 4 (Trading, yesterday): {check_redis_freshness(last_redis, latest_trade_date, now_dt)}")

if __name__ == "__main__":
    test_freshness_scenarios()
