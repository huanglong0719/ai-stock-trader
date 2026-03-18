import sys
import os
from app.db.session import engine, Base
from app.models.stock_models import StockIndicator

def init_tables():
    print("Creating new tables...")
    Base.metadata.create_all(bind=engine)
    print("Tables created.")

if __name__ == "__main__":
    init_tables()
