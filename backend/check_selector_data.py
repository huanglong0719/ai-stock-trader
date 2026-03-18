"""
检查选股所需的数据是否完整
"""
import asyncio
from datetime import datetime
from app.services.data_provider import data_provider
from app.db.session import SessionLocal
from app.models.stock_models import StockIndicator, DailyBar
from sqlalchemy import func

async def check_data():
    print("=" * 60)
    print("检查选股所需数据")
    print("=" * 60)
    
    # 1. 检查最新交易日
    trade_date = await data_provider.get_last_trade_date()
    print(f"\n1. 最新交易日: {trade_date}")
    
    # 2. 检查 daily_basic 数据
    try:
        df_basic = await data_provider.get_daily_basic(trade_date=trade_date)
        print(f"\n2. daily_basic 数据:")
        print(f"   - 记录数: {len(df_basic)}")
        if not df_basic.empty:
            print(f"   - 列: {list(df_basic.columns)}")
            print(f"   - 示例 (前3条):")
            print(df_basic.head(3))
    except Exception as e:
        print(f"   ❌ 获取失败: {e}")
    
    # 3. 检查 DailyBar 数据
    db = SessionLocal()
    try:
        bar_count = db.query(func.count(DailyBar.id)).filter(
            DailyBar.trade_date == trade_date
        ).scalar()
        print(f"\n3. DailyBar 数据 ({trade_date}):")
        print(f"   - 记录数: {bar_count}")
        
        if bar_count > 0:
            bar_samples = db.query(DailyBar).filter(
                DailyBar.trade_date == trade_date
            ).limit(3).all()
            print(f"   - 示例:")
            for bar in bar_samples:
                print(f"     {bar.ts_code}: close={bar.close}, pct_chg={bar.pct_chg}")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    
    # 4. 检查 StockIndicator 数据
    try:
        ind_count = db.query(func.count(StockIndicator.id)).scalar()
        print(f"\n4. StockIndicator 预计算指标:")
        print(f"   - 总记录数: {ind_count}")
        
        if ind_count > 0:
            ind_samples = db.query(StockIndicator).limit(3).all()
            print(f"   - 示例:")
            for ind in ind_samples:
                print(f"     {ind.ts_code}:")
                print(f"       ma5={ind.ma5}, ma20={ind.ma20}")
                print(f"       is_daily_bullish={ind.is_daily_bullish}")
                print(f"       is_weekly_bullish={ind.is_weekly_bullish}")
                print(f"       is_monthly_bullish={ind.is_monthly_bullish}")
        else:
            print(f"   ⚠️ 没有预计算指标数据！这可能是选股失败的原因")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    finally:
        db.close()
    
    # 5. 检查资金流数据
    try:
        df_mf = await data_provider.get_moneyflow(trade_date=trade_date, silent=True)
        print(f"\n5. 资金流数据:")
        print(f"   - 记录数: {len(df_mf)}")
        if not df_mf.empty:
            print(f"   - 列: {list(df_mf.columns)}")
    except Exception as e:
        print(f"   ❌ 获取失败: {e}")
    
    # 6. 检查股票基本信息
    try:
        stocks = await data_provider.get_stock_basic()
        print(f"\n6. 股票基本信息:")
        print(f"   - 总股票数: {len(stocks)}")
    except Exception as e:
        print(f"   ❌ 获取失败: {e}")
    
    print("\n" + "=" * 60)
    print("数据检查完成")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(check_data())
