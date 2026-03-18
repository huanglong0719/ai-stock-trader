from app.services.data_sync import DataSyncService
import logging

logging.basicConfig(level=logging.INFO)

service = DataSyncService()
service.init_db()
print("DB initialized.")
