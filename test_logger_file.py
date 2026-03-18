
import os
import sys

# Add backend to path
sys.path.append(os.path.join(os.getcwd(), "backend"))

from app.services.logger import selector_logger
import logging

def test_log():
    print("Testing selector_logger...")
    import app.services.logger
    print(f"logger file: {app.services.logger.__file__}")
    print(f"selector_logger type: {type(selector_logger)}")
    print(f"selector_logger std_logger handlers: {selector_logger.std_logger.handlers}")
    selector_logger.info("This is a test INFO log from selector_logger")
    selector_logger.error("This is a test ERROR log from selector_logger")
    
    log_file = os.path.join(os.getcwd(), "backend", "logs", "selector.log")
    print(f"Checking for log file at: {log_file}")
    if os.path.exists(log_file):
        print("✅ Log file exists!")
        with open(log_file, 'r', encoding='utf-8') as f:
            print("File content:")
            print(f.read())
    else:
        print("❌ Log file does NOT exist!")

if __name__ == "__main__":
    test_log()
