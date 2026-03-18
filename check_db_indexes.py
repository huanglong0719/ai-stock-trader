
from app.db.session import engine
from sqlalchemy import inspect

def check_indexes():
    inspector = inspect(engine)
    for table_name in ["daily_bars", "stock_indicators"]:
        print(f"\nTable: {table_name}")
        indexes = inspector.get_indexes(table_name)
        for index in indexes:
            print(f"  Index: {index['name']}, Columns: {index['column_names']}, Unique: {index['unique']}")

if __name__ == "__main__":
    check_indexes()
