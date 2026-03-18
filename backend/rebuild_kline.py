from app.services.data_sync import DataSyncService
import logging

logging.basicConfig(level=logging.INFO)

def main():
    sync_service = DataSyncService()
    print("Initializing database tables...")
    sync_service.init_db()
    
    print("Converting historical daily data to weekly and monthly...")
    # 这里我们处理最近 2 年的数据，确保周线月线完整
    sync_service.convert_all_to_weekly_monthly(days=730)
    print("Conversion complete.")

if __name__ == "__main__":
    main()
