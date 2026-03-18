"""
检查选股问题的根本原因
"""
import asyncio
from datetime import datetime, timedelta
from app.services.data_provider import data_provider
from app.db.session import SessionLocal
from app.models.stock_models import StockIndicator, DailyBar
from sqlalchemy import func, desc

async def check_issue():
    print("=" * 60)
    print("诊断选股功能问题")
    print("=" * 60)
    
    # 1. 检查最新交易日
    trade_date = await data_provider.get_last_trade_date()
    print(f"\n1. 最新交易日: {trade_date}")
    
    db = SessionLocal()
    try:
        # 2. 检查 DailyBar 最新数据日期
        latest_bar_date = db.query(DailyBar.trade_date).order_by(
            desc(DailyBar.trade_date)
        ).first()
        
        if latest_bar_date:
            print(f"\n2. DailyBar 最新数据日期: {latest_bar_date[0]}")
            
            bar_count = db.query(func.count(DailyBar.ts_code)).filter(
                DailyBar.trade_date == latest_bar_date[0]
            ).scalar()
            print(f"   该日期记录数: {bar_count}")
            
            if latest_bar_date[0] != trade_date:
                print(f"   ⚠️ 警告: DailyBar 数据日期({latest_bar_date[0]})与最新交易日({trade_date})不一致")
                print(f"   这会导致选股使用旧数据或无数据")
        else:
            print(f"\n2. ❌ DailyBar 表中没有任何数据！")
        
        # 3. 检查 StockIndicator 数据
        ind_count = db.query(func.count(StockIndicator.ts_code)).scalar()
        print(f"\n3. StockIndicator 预计算指标:")
        print(f"   - 总记录数: {ind_count}")
        
        if ind_count == 0:
            print(f"   ❌ 没有预计算指标！这是选股失败的主要原因")
            print(f"   解决方案: 需要运行指标计算任务")
        else:
            # 检查指标数据的日期
            sample = db.query(StockIndicator).limit(5).all()
            print(f"   - 示例数据:")
            for ind in sample:
                print(f"     {ind.ts_code}: trade_date={ind.trade_date}")
                print(f"       is_daily_bullish={ind.is_daily_bullish}, is_weekly_bullish={ind.is_weekly_bullish}")
        
        # 4. 测试初选逻辑的关键条件
        print(f"\n4. 测试初选条件:")
        
        # 获取 daily_basic 数据
        df_basic = await data_provider.get_daily_basic(trade_date=trade_date)
        print(f"   - daily_basic 记录数: {len(df_basic)}")
        
        if not df_basic.empty:
            # 应用基础过滤
            df = df_basic[
                ~df_basic['ts_code'].str.startswith('688') & 
                ~df_basic['ts_code'].str.startswith('8') & 
                ~df_basic['ts_code'].str.startswith('4') &
                ~df_basic['ts_code'].str.endswith('.BJ')
            ].copy()
            print(f"   - 排除科创板/北交所后: {len(df)}")
            
            # 估值与市值过滤
            mv_col = 'circ_mv' if 'circ_mv' in df.columns else 'total_mv'
            mask = (df['pe'] > 0) & (df['pe'] < 80) & (df[mv_col] > 200000)
            df = df[mask]
            print(f"   - 估值与市值过滤后: {len(df)}")
            
            # 检查有多少股票有预计算指标
            ts_codes = df['ts_code'].tolist()[:100]  # 取前100个测试
            indicators = db.query(StockIndicator).filter(
                StockIndicator.ts_code.in_(ts_codes)
            ).all()
            indicators_map = {ind.ts_code: ind for ind in indicators}
            
            print(f"   - 前100只股票中有预计算指标的: {len(indicators_map)}")
            
            # 检查有多少符合技术面条件
            valid_count = 0
            for code in ts_codes:
                ind_opt = indicators_map.get(code)
                if not ind_opt:
                    continue
                ind = ind_opt
                
                # 技术面硬性条件 (来自 _filter_candidates)
                if not ind.is_monthly_bullish:
                    continue
                if not ind.is_weekly_bullish:
                    continue
                weekly_slope = float(ind.weekly_ma20_slope or 0.0)
                if weekly_slope < -5:
                    continue
                if not (ind.is_daily_bullish or ind.is_trend_recovering):
                    continue
                
                valid_count += 1
            
            print(f"   - 符合技术面条件的: {valid_count}")
            
            if valid_count == 0:
                print(f"\n   ❌ 问题诊断: 所有股票都被技术面条件过滤掉了")
                print(f"   可能原因:")
                print(f"     1. 预计算指标数据过时或不准确")
                print(f"     2. 技术面条件过于严格")
                print(f"     3. 当前市场环境下符合条件的股票确实很少")
                
                # 显示一些指标样本
                print(f"\n   指标样本 (前5只):")
                for i, (code, ind_item) in enumerate(list(indicators_map.items())[:5]):
                    print(f"     {code}:")
                    print(f"       is_monthly_bullish={ind_item.is_monthly_bullish}")
                    print(f"       is_weekly_bullish={ind_item.is_weekly_bullish}")
                    print(f"       weekly_ma20_slope={ind_item.weekly_ma20_slope}")
                    print(f"       is_daily_bullish={ind_item.is_daily_bullish}")
                    print(f"       is_trend_recovering={ind_item.is_trend_recovering}")
    
    finally:
        db.close()
    
    print("\n" + "=" * 60)
    print("诊断完成")
    print("=" * 60)
    print("\n建议:")
    print("1. 如果 DailyBar 数据缺失，运行: python trigger_5year_sync.py")
    print("2. 如果 StockIndicator 数据缺失，需要实现指标计算任务")
    print("3. 如果技术面条件过严，考虑放宽选股条件")

if __name__ == "__main__":
    asyncio.run(check_issue())
