
from app.db.session import SessionLocal
from app.models.stock_models import TradingPlan, MarketSentiment, DailyBar
from sqlalchemy import desc, and_
import json

def fetch_trade_history():
    db = SessionLocal()
    try:
        # 1. 盈利变亏损交易 (Profit-to-Loss)
        # 定义：最终 real_pnl_pct < 0，但在持仓期间曾达到过 high_pct > 3%
        potential_p2l = db.query(TradingPlan).filter(
            TradingPlan.executed == True,
            TradingPlan.real_pnl_pct < -1.0  # 最终亏损超过 1%
        ).all()

        p2l_trades = []
        for p in potential_p2l:
            # 获取持仓期间的 K 线
            bars = db.query(DailyBar).filter(
                DailyBar.ts_code == p.ts_code,
                DailyBar.trade_date >= p.date
            ).order_by(DailyBar.trade_date.asc()).limit(10).all()
            
            entry_price = p.entry_price or p.buy_price_limit
            if not entry_price: continue
            
            max_gain = 0
            for bar in bars:
                gain = (bar.high - entry_price) / entry_price * 100
                if gain > max_gain:
                    max_gain = gain
                if p.exit_price and bar.trade_date > p.date: # 粗略判断卖出日
                     # 如果有记录显示在卖出前曾经大涨
                     pass
            
            if max_gain > 3.0: # 曾经盈利超过 3%
                p2l_trades.append({
                    "plan": p,
                    "max_gain": max_gain
                })

        # 2. 获取盈利最大的 5 笔交易
        wins = db.query(TradingPlan).filter(
            TradingPlan.executed == True,
            TradingPlan.real_pnl_pct > 0
        ).order_by(desc(TradingPlan.real_pnl_pct)).limit(5).all()

        # 3. 获取亏损最大的 5 笔交易
        losses = db.query(TradingPlan).filter(
            TradingPlan.executed == True,
            TradingPlan.real_pnl_pct < 0
        ).order_by(TradingPlan.real_pnl_pct).limit(5).all()

        def format_plan(p, extra=None):
            sentiment = db.query(MarketSentiment).filter(MarketSentiment.date == p.date).first()
            data = {
                "id": p.id,
                "date": str(p.date),
                "ts_code": p.ts_code,
                "strategy": p.strategy_name,
                "pnl_pct": p.real_pnl_pct or p.pnl_pct,
                "reason": p.reason,
                "ai_decision": p.ai_decision,
                "review": p.review_content,
                "temp": sentiment.market_temperature if sentiment else "未知",
            }
            if extra:
                data.update(extra)
            return data

        print("--- PROFIT-TO-LOSS TRADES (盈利转亏损) ---")
        for item in p2l_trades:
            print(json.dumps(format_plan(item["plan"], {"max_gain_during_hold": round(item["max_gain"], 2)}), ensure_ascii=False, indent=2))

        print("\n--- TOP WINS ---")
        for p in wins:
            print(json.dumps(format_plan(p), ensure_ascii=False, indent=2))
        
        print("\n--- TOP LOSSES ---")
        for p in losses:
            print(json.dumps(format_plan(p), ensure_ascii=False, indent=2))

    finally:
        db.close()

if __name__ == "__main__":
    fetch_trade_history()
