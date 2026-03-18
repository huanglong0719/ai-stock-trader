"""
触发指标计算任务
"""
import asyncio
import logging
from app.services.indicator_service import indicator_service

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    print("=" * 60)
    print("开始计算所有股票的技术指标")
    print("=" * 60)
    print("\n这可能需要几分钟时间，请耐心等待...\n")
    
    try:
        await indicator_service.calculate_all_indicators()
        print("\n" + "=" * 60)
        print("✅ 指标计算完成！")
        print("=" * 60)
        print("\n现在可以使用选股功能了")
    except Exception as e:
        print(f"\n❌ 指标计算失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
