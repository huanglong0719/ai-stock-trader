
import asyncio
from app.services.stock_selector import StockSelectorService
from app.services.logger import selector_logger

async def test_selection():
    selector = StockSelectorService()
    trade_date = "20260112"
    print(f"--- 测试 {trade_date} 选股 ---")
    
    try:
        # 使用多维综合策略
        results = await selector.select_stocks(strategy="default", trade_date=trade_date)
        
        print(f"选股结果数量: {len(results)}")
        for i, res in enumerate(results):
            print(f"{i+1}. {res['ts_code']} ({res['name']}) - 评分: {res['score']}, 是否值得交易: {res['is_worth_trading']}")
            
        if not results:
            print("警告: 依然没有选出股票。")
            # 打印 selector_logger 的最后几条日志
            print("选股日志:")
            for log in selector_logger.get_logs()[-10:]:
                print(f"  {log}")
    except Exception as e:
        print(f"测试过程出错: {e}")
    finally:
        await selector.close()

if __name__ == "__main__":
    asyncio.run(test_selection())
