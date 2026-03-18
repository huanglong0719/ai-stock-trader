import sys
import os
import time
from datetime import datetime
import concurrent.futures

# Add backend directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.data_sync import data_sync_service
from app.db.session import SessionLocal
from app.models.stock_models import Stock

def download_all_minutes_sina():
    """
    全量同步所有股票的近期分钟数据 (基于新浪接口)
    """
    print(f"Starting FULL sync of minute data via Sina at {datetime.now()}...")
    
    db = SessionLocal()
    try:
        # 1. 获取所有股票代码
        stocks = db.query(Stock.ts_code).all()
        ts_codes = [s[0] for s in stocks]
        print(f"Found {len(ts_codes)} stocks in DB.")
        
        if not ts_codes:
            print("No stocks found. Please sync basic info first.")
            return

        # 2. 设定同步日期范围 (例如今天)
        today = datetime.now().strftime('%Y%m%d')
        start_date = today
        end_date = today
        
        print(f"Syncing data for range: {start_date} - {end_date}")
        
        # 3. 并发下载
        # 新浪接口限制较宽，可以开较高并发，但为了安全起见，控制在 10-20
        max_workers = 20
        total = len(ts_codes)
        completed = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务
            future_to_code = {
                executor.submit(data_sync_service.download_minute_data, code, start_date, end_date, freq='1min'): code 
                for code in ts_codes
            }
            
            print(f"Tasks submitted. Processing with {max_workers} threads...")
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error syncing {code}: {e}")
                
                completed += 1
                if completed % 100 == 0:
                    print(f"Progress: {completed}/{total} ({(completed/total)*100:.1f}%)")
                    
        print(f"Sync complete at {datetime.now()}.")
        
    except Exception as e:
        print(f"Global error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    download_all_minutes_sina()
