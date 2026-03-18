
from app.db.session import SessionLocal
from app.models.stock_models import MarketSentiment
from datetime import date
import json

def inspect_sentiment():
    db = SessionLocal()
    try:
        today = date.today()
        sentiments = db.query(MarketSentiment).order_by(MarketSentiment.date.desc()).limit(5).all()
        print(f"Checking last 5 sentiments:")
        for s in sentiments:
            print(f"Date: {s.date}, Updated At: {s.updated_at}")
            print(f"  Up: {s.up_count}, Down: {s.down_count}, LU: {s.limit_up_count}, LD: {s.limit_down_count}")
            print(f"  Highest Plate: {s.highest_plate}")
            print(f"  Main Theme: {s.main_theme}")
            print(f"  Ladder JSON Length: {len(s.ladder_json) if s.ladder_json else 'None'}")
            if s.ladder_json:
                try:
                    ladder = json.loads(s.ladder_json)
                    print(f"  Ladder Stocks Count: {len(ladder.get('stocks', []))}")
                    print(f"  Ladder Tiers: {ladder.get('tiers')}")
                except:
                    print("  Failed to parse Ladder JSON")
            print("-" * 20)
    finally:
        db.close()

if __name__ == "__main__":
    inspect_sentiment()
