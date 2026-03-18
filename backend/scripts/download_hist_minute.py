import argparse
import logging
import time
from app.services.data_sync import data_sync_service
from app.services.data_provider import data_provider

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Download historical minute data from Tushare")
    parser.add_argument('--date', type=str, required=True, help='Date to download (YYYYMMDD)')
    parser.add_argument('--stock', type=str, help='Stock code (e.g., 000001.SH) or "watchlist"')
    
    args = parser.parse_args()
    
    date_str = args.date
    # 格式化日期：YYYYMMDD -> YYYY-MM-DD (Tushare 接口其实接受 YYYY-MM-DD HH:MM:SS)
    # stk_mins start_date/end_date 格式：'2023-10-27 09:30:00'
    start_time = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} 09:30:00"
    end_time = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} 15:00:00"
    
    ts_codes = []
    
    if args.stock:
        if args.stock == 'watchlist':
            # 获取自选股列表 (假设存在一个简单的方式获取，这里简化处理)
            # 由于没有直接的 get_watchlist，我们暂时从 Stock 表获取 limit 10 测试，或者让用户手动输入
            print("目前不支持直接指定 watchlist 参数，请指定具体股票代码或使用逗号分隔")
            return
        else:
            ts_codes = args.stock.split(',')
    else:
        # 如果没有指定股票，为了保护积分，默认只下载主要指数
        ts_codes = ['000001.SH', '399001.SZ', '399006.SZ', '000300.SH']
        print("未指定股票，默认下载主要指数数据...")

    print(f"准备下载 {len(ts_codes)} 只标的在 {date_str} 的分钟线数据...")
    print("注意：由于 Tushare 积分限制 (每分钟2次)，每只股票下载后将暂停 31 秒...")
    
    for i, code in enumerate(ts_codes):
        code = code.strip()
        data_sync_service.download_minute_data(code, start_time, end_time, freq='1min')
        
        # 为了避免触发流控 (每分钟2次)，除了最后一个，每次下载后暂停
        if i < len(ts_codes) - 1:
            print("等待 31 秒以遵守流控限制...")
            time.sleep(31)
        
    print("下载任务完成。")

if __name__ == "__main__":
    main()
