import asyncio
import sys
import os
from sqlalchemy import func

# Add backend path to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from app.db.session import SessionLocal
from app.models.stock_models import TradeRecord, Account, TradingPlan, Position
from app.services.data_provider import data_provider

async def fix_assets():
    print("=== 开始执行资产修正 ===")
    db = SessionLocal()
    try:
        # 1. 初始资金
        initial_cash = 1000000.0
        current_cash = initial_cash
        print(f"初始资金: {initial_cash}")

        # 2. 回溯历史交易
        records = db.query(TradeRecord).order_by(TradeRecord.trade_time.asc(), TradeRecord.id.asc()).all()
        print(f"找到 {len(records)} 条交易记录")

        for record in records:
            if record.trade_type == 'BUY':
                # 买入：扣除 (成交金额 + 手续费)
                # TradeRecord 中 amount 是成交金额(need_cash), fee 是手续费
                cost = float(record.amount or 0.0) + float(record.fee or 0.0)
                current_cash -= cost
                # print(f"[-] 买入 {record.ts_code}: -{cost:.2f} -> 余额 {current_cash:.2f}")
            elif record.trade_type == 'SELL':
                # 卖出：增加 (净得金额)
                # TradeRecord 中 amount 是净得金额(net_amount)
                income = float(record.amount or 0.0)
                current_cash += income
                # print(f"[+] 卖出 {record.ts_code}: +{income:.2f} -> 余额 {current_cash:.2f}")
        
        print(f"回溯计算后现金余额 (含冻结): {current_cash:.2f}")

        # 3. 计算当前冻结资金
        # 查询所有未成交且有冻结金额的计划
        frozen_plans = db.query(TradingPlan).filter(
            TradingPlan.executed == False,
            TradingPlan.frozen_amount > 0
        ).all()
        
        frozen_cash = 0.0
        for plan in frozen_plans:
            frozen_cash += float(plan.frozen_amount or 0.0)
        
        print(f"当前冻结资金 (来自 {len(frozen_plans)} 个计划): {frozen_cash:.2f}")

        # 4. 计算可用资金
        available_cash = current_cash - frozen_cash
        if available_cash < 0:
            print(f"警告：可用资金为负 ({available_cash:.2f})！可能存在数据异常或超额冻结。")
            # 这种情况下，可能需要修正冻结金额或者接受负值（不应该发生）
            # 暂时按逻辑继续
        
        # 5. 计算持仓市值
        positions = db.query(Position).filter(Position.vol > 0).all()
        ts_codes = [p.ts_code for p in positions]
        
        total_market_value = 0.0
        if ts_codes:
            quotes = await data_provider.get_realtime_quotes(ts_codes)
            for pos in positions:
                quote = quotes.get(pos.ts_code)
                price = float(quote['price']) if quote else float(pos.current_price or 0.0)
                if price <= 0:
                    price = pos.current_price # Fallback
                
                mv = pos.vol * price
                total_market_value += mv
                
                # 更新持仓市值字段 (Optional but good)
                pos.current_price = price
                pos.market_value = mv
                cost = pos.vol * pos.avg_price
                pos.float_pnl = mv - cost
                pos.pnl_pct = (pos.float_pnl / cost * 100) if cost > 0 else 0
        
        print(f"当前持仓市值: {total_market_value:.2f}")

        # 6. 汇总总资产
        total_assets = available_cash + frozen_cash + total_market_value
        total_pnl = total_assets - initial_cash
        total_pnl_pct = (total_pnl / initial_cash * 100)

        # 7. 更新账户
        account = db.query(Account).first()
        if not account:
            account = Account()
            db.add(account)
        
        print("\n=== 修正前账户状态 ===")
        print(f"可用资金: {account.available_cash}")
        print(f"冻结资金: {account.frozen_cash}")
        print(f"持仓市值: {account.market_value}")
        print(f"总资产:   {account.total_assets}")
        print(f"总收益率: {account.total_pnl_pct}%")

        print("\n=== 修正后账户状态 (拟提交) ===")
        print(f"可用资金: {available_cash:.2f}")
        print(f"冻结资金: {frozen_cash:.2f}")
        print(f"持仓市值: {total_market_value:.2f}")
        print(f"总资产:   {total_assets:.2f}")
        print(f"总收益率: {total_pnl_pct:.2f}%")

        # 提交更改
        account.available_cash = available_cash
        account.frozen_cash = frozen_cash
        account.market_value = total_market_value
        account.total_assets = total_assets
        account.total_pnl = total_pnl
        account.total_pnl_pct = total_pnl_pct
        
        db.commit()
        print("\n✅ 资产修正已提交到数据库！")

    except Exception as e:
        print(f"❌ 修正失败: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(fix_assets())
