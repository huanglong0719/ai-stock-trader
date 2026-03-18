from app.services.learning_service import learning_service
from app.services.logger import logger
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if __name__ == "__main__":
    print("Running offline learning task...")
    try:
        learning_service.perform_daily_learning()
        print("Task completed successfully. Check logs/ for details.")
    except Exception as e:
        print(f"Task failed: {e}")
