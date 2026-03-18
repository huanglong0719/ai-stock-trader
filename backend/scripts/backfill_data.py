import os
import sys
from datetime import datetime, timedelta

# 添加项目根目录到 python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.data_sync import data_sync_service
from app.db.session import SessionLocal

def main():
    print("开始更新最近一年的 A 股数据...")
    
    # 1. 计算日期范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    print(f"同步范围: {start_date} 至 {end_date}")
    
    # 2. 首先确保股票列表是最新的
    print("正在更新股票列表...")
    data_sync_service.sync_all_stocks()
    
    # 3. 执行回溯同步
    # 注意：由于 Tushare 接口限制和数据量，这可能需要一段时间
    # backfill 内部有重试和间隔逻辑
    print("正在开始历史数据回溯同步 (日线和行业数据)...")
    data_sync_service.backfill(start_date, end_date)
    
    print("数据更新任务完成！")

if __name__ == "__main__":
    main()
