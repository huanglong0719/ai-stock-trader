"""
并发控制工具
防止竞态条件和资源冲突
"""
import asyncio
from typing import Dict
from contextlib import asynccontextmanager


class AsyncLockManager:
    """异步锁管理器 - 用于防止并发冲突"""
    
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._master_lock = asyncio.Lock()
    
    async def get_lock(self, key: str) -> asyncio.Lock:
        """
        获取指定键的锁
        
        Args:
            key: 锁的唯一标识（如股票代码）
        
        Returns:
            异步锁对象
        """
        async with self._master_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]
    
    @asynccontextmanager
    async def lock(self, key: str):
        """
        上下文管理器形式的锁
        
        使用示例:
            async with lock_manager.lock("000001.SZ"):
                # 执行需要互斥的操作
                await execute_trade()
        """
        lock = await self.get_lock(key)
        async with lock:
            yield
    
    async def cleanup(self, key: str):
        """清理不再使用的锁"""
        async with self._master_lock:
            if key in self._locks:
                del self._locks[key]


# 全局锁管理器实例
trading_lock_manager = AsyncLockManager()
data_lock_manager = AsyncLockManager()


class Semaphore:
    """信号量 - 限制并发数量"""
    
    def __init__(self, max_concurrent: int = 5):
        """
        Args:
            max_concurrent: 最大并发数
        """
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    @asynccontextmanager
    async def acquire(self):
        """
        获取信号量
        
        使用示例:
            semaphore = Semaphore(max_concurrent=3)
            async with semaphore.acquire():
                await api_call()
        """
        async with self._semaphore:
            yield


# 全局信号量实例
api_semaphore = Semaphore(max_concurrent=5)  # API 调用限制
db_semaphore = Semaphore(max_concurrent=10)  # 数据库操作限制
