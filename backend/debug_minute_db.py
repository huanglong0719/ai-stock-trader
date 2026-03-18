import sys
import os
from datetime import datetime
import requests

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.session import SessionLocal
from app.models.stock_models import MinuteBar

def check_db_and_api():
    ts_code = '000001.SZ'
    today_int = datetime.now().strftime('%Y%m%d') # 20260105
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Checking data for {ts_code} on {today_str}...\n")

    # 1. Check DB
    db = SessionLocal()
    try:
        bars = db.query(MinuteBar).filter(
            MinuteBar.ts_code == ts_code,
            MinuteBar.trade_time >= datetime.strptime(today_int, '%Y%m%d'),
            MinuteBar.freq == '1min'
        ).order_by(MinuteBar.trade_time.asc()).limit(5).all()
        
        print("--- Database Records (First 5) ---")
        if not bars:
            print("No records found in DB!")
        for bar in bars:
            print(f"Time: {bar.trade_time} | Open: {bar.open} | High: {bar.high} | Low: {bar.low} | Close: {bar.close} | Vol: {bar.vol}")
            
    finally:
        db.close()

    # 2. Check Sina API Raw Response
    print("\n--- Sina API Raw Response (First 2 items) ---")
    try:
        sina_code = "sz000001"
        url = f"http://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData?symbol={sina_code}&scale=1&ma=no&datalen=10"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        for item in data[:2]:
            print(item)
    except Exception as e:
        print(f"Error fetching API: {e}")

if __name__ == "__main__":
    check_db_and_api()
