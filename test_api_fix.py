#!/usr/bin/env python
"""
测试 API 修复
"""
import sys
import asyncio

async def test_market_data_service():
    """测试 market_data_service 的异步调用"""
    print("🔍 测试 MarketDataService...")
    
    try:
        # 需要在 backend 目录下运行
        sys.path.insert(0, 'backend')
        
        from app.services.market.market_data_service import market_data_service
        
        # 测试 _get_latest_adj_factor
        print("  测试 _get_latest_adj_factor...")
        adj_factor = await market_data_service._get_latest_adj_factor('000001.SZ')
        print(f"  ✅ 复权因子获取成功: {adj_factor}")
        
        # 测试 get_kline
        print("  测试 get_kline...")
        kline = await market_data_service.get_kline('000001.SZ', freq='D', limit=10)
        if kline:
            print(f"  ✅ K线数据获取成功: {len(kline)} 条")
        else:
            print("  ⚠️  K线数据为空")
        
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_data_provider():
    """测试 data_provider 的异步调用"""
    print("\n🔍 测试 DataProvider...")
    
    try:
        sys.path.insert(0, 'backend')
        
        from app.services.data_provider import data_provider
        
        # 测试 get_kline
        print("  测试 get_kline...")
        kline = await data_provider.get_kline('000001.SZ', freq='D', limit=10)
        if kline:
            print(f"  ✅ K线数据获取成功: {len(kline)} 条")
            if kline:
                print(f"  最新数据: {kline[-1]}")
        else:
            print("  ⚠️  K线数据为空")
        
        return True
        
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    print("=" * 60)
    print("🔧 API 修复验证")
    print("=" * 60)
    
    results = []
    
    # 测试 MarketDataService
    results.append(("MarketDataService", await test_market_data_service()))
    
    # 测试 DataProvider
    results.append(("DataProvider", await test_data_provider()))
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("📊 测试结果")
    print("=" * 60)
    
    for name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{status} - {name}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    print(f"\n总计: {passed}/{total} 项测试通过")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
