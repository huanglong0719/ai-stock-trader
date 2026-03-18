
import os
import sys
from datetime import date

# 设置项目根目录
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backend'))

from backend.app.db.session import SessionLocal
from backend.app.models.stock_models import Account, TradeRecord, Position, TradingPlan

def repair_data():
    db = SessionLocal()
    try:
        print("=== Starting Account Data Repair ===")

        print("\n=== Step 0: Reconcile Plan Execution Consistency ===")
        all_plans = db.query(TradingPlan).all()
        trade_plan_ids = set(r[0] for r in db.query(TradeRecord.plan_id).filter(TradeRecord.plan_id != None).all())

        reverted_exec = 0
        fixed_exec = 0
        cleared_frozen_for_exec = 0
        for p in all_plans:
            has_record = p.id in trade_plan_ids
            if bool(p.executed) and not has_record:
                p.executed = False
                p.entry_price = None
                p.exit_price = None
                p.pnl_pct = None
                p.real_pnl = None
                p.real_pnl_pct = None
                p.close_reason = None
                reverted_exec += 1
            elif (not bool(p.executed)) and has_record:
                p.executed = True
                first_buy = db.query(TradeRecord).filter(TradeRecord.plan_id == p.id, TradeRecord.trade_type == "BUY").order_by(TradeRecord.trade_time.asc()).first()
                if first_buy and first_buy.price:
                    p.entry_price = float(first_buy.price)
                fixed_exec += 1

            if bool(p.executed) and (p.frozen_amount or 0) > 0:
                p.frozen_amount = 0.0
                p.frozen_vol = 0
                cleared_frozen_for_exec += 1

        db.commit()
        print(f"Reverted executed->False (missing trade record): {reverted_exec}")
        print(f"Fixed executed->True (has trade record): {fixed_exec}")
        print(f"Cleared frozen funds for executed plans: {cleared_frozen_for_exec}")

        print("\n=== Step 0.5: Realign Frozen Funds to Entrustment Price ===")
        adjusted_count = 0
        cleared_count = 0
        total_delta = 0.0

        def _calc_total(v: int, price: float) -> float:
            need_cash = float(v) * float(price)
            fee = max(5.0, need_cash * 0.00025)
            return need_cash + fee

        frozen_unexecuted = db.query(TradingPlan).filter(
            TradingPlan.executed == False,
            TradingPlan.frozen_amount > 0
        ).all()

        for p in frozen_unexecuted:
            ref_price = float((p.buy_price_limit or 0.0) or (p.limit_price or 0.0) or 0.0)
            old_amount = float(p.frozen_amount or 0.0)
            old_vol = int(p.frozen_vol or 0)
            if ref_price <= 0 or old_amount <= 0 or old_vol < 100:
                continue

            budget = old_amount
            vol = old_vol
            expected = _calc_total(vol, ref_price)
            if expected > budget:
                vol = int(budget / ref_price / 100) * 100
                if vol > old_vol:
                    vol = old_vol
                if vol < 100:
                    p.frozen_amount = 0.0
                    p.frozen_vol = 0
                    cleared_count += 1
                    total_delta -= old_amount
                    continue
                expected = _calc_total(vol, ref_price)
                while vol >= 100 and expected > budget:
                    vol -= 100
                    if vol < 100:
                        p.frozen_amount = 0.0
                        p.frozen_vol = 0
                        cleared_count += 1
                        total_delta -= old_amount
                        break
                    expected = _calc_total(vol, ref_price)
                if vol < 100:
                    continue

            if abs(expected - old_amount) > max(1e-6, expected * 1e-6) or vol != old_vol:
                p.frozen_amount = float(expected)
                p.frozen_vol = int(vol)
                adjusted_count += 1
                total_delta += float(expected - old_amount)

        db.commit()
        print(f"Adjusted frozen plans: {adjusted_count}")
        print(f"Cleared frozen plans (insufficient budget): {cleared_count}")
        print(f"Total frozen delta: {total_delta:,.2f}")
        
        # 1. 获取初始资金 (系统固定为 100万)
        initial_cash = 1000000.0
        
        # 2. 计算所有交易产生的资金变动
        trades = db.query(TradeRecord).order_by(TradeRecord.trade_time.asc()).all()
        
        calculated_total_cash = initial_cash
        print(f"Initial Cash: {initial_cash:,.2f}")
        
        for t in trades:
            if t.trade_type == 'BUY':
                # BUY: 支出 = amount (成交额) + fee (手续费)
                cost = t.amount + (t.fee or 0)
                calculated_total_cash -= cost
                print(f"[{t.trade_time}] BUY  {t.ts_code}: -{cost:,.2f} (Price: {t.price}, Vol: {t.vol}, Fee: {t.fee})")
            elif t.trade_type == 'SELL':
                # SELL: 收入 = amount (已经是扣除手续费后的净额)
                income = t.amount
                calculated_total_cash += income
                print(f"[{t.trade_time}] SELL {t.ts_code}: +{income:,.2f} (Price: {t.price}, Vol: {t.vol}, Fee: {t.fee})")
        
        print(f"Calculated Total Cash (Available + Frozen): {calculated_total_cash:,.2f}")
        
        # 3. 计算当前的冻结资金
        # 冻结资金来源于未执行且 frozen_amount > 0 的计划
        frozen_plans = db.query(TradingPlan).filter(
            TradingPlan.executed == False,
            TradingPlan.frozen_amount > 0
        ).all()
        
        total_frozen_cash = sum(float(p.frozen_amount or 0.0) for p in frozen_plans)
        print(f"Total Frozen Cash from Plans: {total_frozen_cash:,.2f}")
        for p in frozen_plans:
            print(f"  - Plan ID {p.id} ({p.ts_code}): {p.frozen_amount:,.2f}")
            
        # 4. 计算可用资金
        available_cash = calculated_total_cash - total_frozen_cash
        print(f"Calculated Available Cash: {available_cash:,.2f}")
        
        # 5. 计算持仓市值
        positions = db.query(Position).filter(Position.vol > 0).all()
        total_market_value = sum(p.market_value for p in positions)
        print(f"Total Market Value: {total_market_value:,.2f}")
        for p in positions:
            print(f"  - {p.ts_code}: {p.vol} shares * {p.current_price} = {p.market_value:,.2f}")
            
        # 6. 计算总资产
        total_assets = calculated_total_cash + total_market_value
        print(f"Calculated Total Assets: {total_assets:,.2f}")
        
        # 7. 更新账户表
        account = db.query(Account).first()
        if not account:
            print("No Account record found. Creating one...")
            account = Account()
            db.add(account)
        
        old_available = account.available_cash
        old_total = account.total_assets
        
        account.available_cash = available_cash
        account.frozen_cash = total_frozen_cash
        account.market_value = total_market_value
        account.total_assets = total_assets
        account.total_pnl = total_assets - initial_cash
        account.total_pnl_pct = (account.total_pnl / initial_cash * 100)
        
        print("\n--- Update Summary ---")
        print(f"Available Cash: {old_available:,.2f} -> {account.available_cash:,.2f} (Diff: {account.available_cash - old_available:,.2f})")
        print(f"Total Assets:   {old_total:,.2f} -> {account.total_assets:,.2f} (Diff: {account.total_assets - old_total:,.2f})")
        
        db.commit()
        print("\n=== Database Updated Successfully ===")
        
    except Exception as e:
        db.rollback()
        print(f"Error during repair: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    repair_data()
