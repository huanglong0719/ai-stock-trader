"""
直接同步股票基本信息
"""
import asyncio
import logging
from app.services.data_sync import data_sync_service

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    print("=" * 60)
    print("开始同步股票基本信息")
    print("=" * 60)
    
    try:
        count = await data_sync_service.sync_all_stocks()
        print(f"\n✅ 同步完成！新增 {count} 只股票")
    except Exception as e:
        print(f"\n❌ 同步失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
