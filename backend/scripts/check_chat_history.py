import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.models.chat_models import ChatMessage


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--contains", type=str, default="")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        q = db.query(ChatMessage).order_by(ChatMessage.created_at.desc())
        if args.contains:
            q = q.filter(ChatMessage.content.contains(args.contains))
        rows = q.limit(args.limit).all()
        for r in reversed(rows):
            created_at = r.created_at.strftime("%Y-%m-%d %H:%M:%S") if r.created_at else ""
            print(f"[{created_at}] {r.role}: {r.content}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
