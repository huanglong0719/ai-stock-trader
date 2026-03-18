
import os
import sys
from datetime import date

# 设置项目根目录
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from backend.app.db.session import SessionLocal
from backend.app.models.stock_models import Position

def check_positions():
    db = SessionLocal()
    try:
        positions = db.query(Position).filter(Position.vol > 0).all()
        print(f"{'-' * 80}")
        print(f"{'TS Code':<12} {'Vol':<10} {'Price':<10} {'Calc MV':<15} {'Stored MV':<15} {'Diff':<10}")
        print(f"{'-' * 80}")
        
        total_calc_mv = 0.0
        total_stored_mv = 0.0
        
        for p in positions:
            calc_mv = p.vol * p.current_price
            diff = calc_mv - p.market_value
            print(f"{p.ts_code:<12} {p.vol:<10} {p.current_price:<10.2f} {calc_mv:<15.2f} {p.market_value:<15.2f} {diff:<10.2f}")
            total_calc_mv += calc_mv
            total_stored_mv += p.market_value
            
        print(f"{'-' * 80}")
        print(f"{'TOTAL':<12} {'':<10} {'':<10} {total_calc_mv:<15.2f} {total_stored_mv:<15.2f} {total_calc_mv - total_stored_mv:<10.2f}")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_positions()
