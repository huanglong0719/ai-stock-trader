import sys
import os
import requests
import json
from datetime import datetime

# Add backend to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.core.config import settings
import tushare as ts

def test_tushare_limit_list():
    print("Testing Tushare limit_list...")
    try:
        pro = ts.pro_api(settings.TUSHARE_TOKEN)
        today = datetime.now().strftime('%Y%m%d')
        # limit_list usually updates at 16:00, but let's check
        df = pro.limit_list(trade_date=today)
        if df.empty:
            print("Tushare limit_list is empty (likely only updates after close).")
        else:
            print(f"Tushare limit_list found {len(df)} records.")
            print(df.head())
    except Exception as e:
        print(f"Tushare error: {e}")

def test_eastmoney_sentiment():
    print("\nTesting EastMoney Sentiment API...")
    # This is a common endpoint for market counts (Shanghai + Shenzhen)
    # 000001.SH (1.000001) + 399001.SZ (0.399001) ? 
    # Actually EastMoney has a specific "Market Overview" api.
    
    url = "http://push2.eastmoney.com/api/qt/ulist.get"
    # Parameters to get all stocks sorted by change pct? No, too big.
    
    # Try a known "Market Breadth" endpoint used by open source projects
    # https://github.com/akfamily/akshare/blob/main/akshare/stock_feature/stock_a_indicator.py
    
    # Trying a snapshot endpoint
    try:
        # Requesting "Shanghai A" (m:1+t:2) and "Shenzhen A" (m:0+t:6) count?
        # Let's try to fetch a summary.
        pass
    except Exception as e:
        print(e)

    # Let's try to get ALL stocks (pagination) - just first page to check latency
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1, "pz": 20, "po": 1, "np": 1, 
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23", # SHA, SZA
        "fields": "f1,f2,f3,f4,f12,f13,f14"
    }
    try:
        r = requests.get(url, params=params, timeout=5)
        data = r.json()
        total = data.get('data', {}).get('total', 0)
        print(f"EastMoney Total Stocks: {total}") 
        # If we have total, maybe we can deduce up/down?
        # No, we need the counts.
    except Exception as e:
        print(f"EastMoney request failed: {e}")

if __name__ == "__main__":
    test_tushare_limit_list()
    test_eastmoney_sentiment()
