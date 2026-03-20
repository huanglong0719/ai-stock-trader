import asyncio
import time
import psutil
import logging
from datetime import datetime
from typing import Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class SystemLoad(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class SystemMetrics:
    cpu_percent: float
    memory_percent: float
    network_latency_ms: float
    active_requests: int
    load_level: SystemLoad

class AdaptiveRateLimiter:
    def __init__(
        self,
        name: str,
        base_concurrency: int = 5,
        max_concurrency: int = 20,
        min_concurrency: int = 2,
        check_interval: float = 5.0
    ):
        self.name = name
        self.base_concurrency = base_concurrency
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.check_interval = check_interval
        
        self._semaphore = asyncio.Semaphore(base_concurrency)
        self._active_requests = 0
        self._lock = asyncio.Lock()
        
        self._last_check_time = 0.0
        self._current_concurrency = base_concurrency
        self._metrics_history: list[SystemMetrics] = []
        self._max_history = 10
        
        self._cpu_threshold_high = 80.0
        self._cpu_threshold_critical = 95.0
        self._memory_threshold_high = 80.0
        self._memory_threshold_critical = 90.0
        self._latency_threshold_high = 500.0
        self._latency_threshold_critical = 1000.0
        
    async def get_system_metrics(self) -> SystemMetrics:
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            network_latency = await self._measure_network_latency()
            
            async with self._lock:
                active_requests = self._active_requests
            
            load_level = self._calculate_load_level(
                cpu_percent, memory_percent, network_latency, active_requests
            )
            
            return SystemMetrics(
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                network_latency_ms=network_latency,
                active_requests=active_requests,
                load_level=load_level
            )
        except Exception as e:
            logger.warning(f"Failed to get system metrics: {e}")
            return SystemMetrics(
                cpu_percent=50.0,
                memory_percent=50.0,
                network_latency_ms=100.0,
                active_requests=0,
                load_level=SystemLoad.MEDIUM
            )
    
    async def _measure_network_latency(self) -> float:
        try:
            start = time.time()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('8.8.8.8', 53),
                timeout=1.0
            )
            writer.close()
            await writer.wait_closed()
            return (time.time() - start) * 1000
        except Exception:
            return 999.0
    
    def _calculate_load_level(
        self,
        cpu: float,
        memory: float,
        latency: float,
        active_requests: int
    ) -> SystemLoad:
        if cpu >= self._cpu_threshold_critical or memory >= self._memory_threshold_critical:
            return SystemLoad.CRITICAL
        
        if latency >= self._latency_threshold_critical:
            return SystemLoad.CRITICAL
        
        if cpu >= self._cpu_threshold_high or memory >= self._memory_threshold_high:
            return SystemLoad.HIGH
        
        if latency >= self._latency_threshold_high:
            return SystemLoad.HIGH
        
        if cpu >= 60 or memory >= 60 or latency >= 200:
            return SystemLoad.MEDIUM
        
        return SystemLoad.LOW
    
    async def adjust_concurrency(self):
        now = time.time()
        if now - self._last_check_time < self.check_interval:
            return
        
        self._last_check_time = now
        metrics = await self.get_system_metrics()
        
        self._metrics_history.append(metrics)
        if len(self._metrics_history) > self._max_history:
            self._metrics_history.pop(0)
        
        new_concurrency = self._calculate_new_concurrency(metrics)
        
        if new_concurrency != self._current_concurrency:
            old_concurrency = self._current_concurrency
            self._current_concurrency = new_concurrency
            
            old_semaphore = self._semaphore
            self._semaphore = asyncio.Semaphore(new_concurrency)
            
            logger.info(
                f"[{self.name}] Concurrency adjusted: {old_concurrency} -> {new_concurrency} "
                f"(CPU: {metrics.cpu_percent:.1f}%, Mem: {metrics.memory_percent:.1f}%, "
                f"Latency: {metrics.network_latency_ms:.1f}ms, Load: {metrics.load_level.value})"
            )
    
    def _calculate_new_concurrency(self, metrics: SystemMetrics) -> int:
        if metrics.load_level == SystemLoad.CRITICAL:
            return self.min_concurrency
        
        if metrics.load_level == SystemLoad.HIGH:
            return max(self.min_concurrency, self.base_concurrency - 2)
        
        if metrics.load_level == SystemLoad.MEDIUM:
            return self.base_concurrency
        
        if len(self._metrics_history) >= 3:
            recent = self._metrics_history[-3:]
            avg_cpu = sum(m.cpu_percent for m in recent) / len(recent)
            avg_mem = sum(m.memory_percent for m in recent) / len(recent)
            
            if avg_cpu < 50 and avg_mem < 50:
                return min(self.max_concurrency, self.base_concurrency + 3)
        
        return self.base_concurrency
    
    async def acquire(self):
        await self.adjust_concurrency()
        await self._semaphore.acquire()
        async with self._lock:
            self._active_requests += 1
    
    def release(self):
        async def _release():
            async with self._lock:
                self._active_requests = max(0, self._active_requests - 1)
            self._semaphore.release()
        
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_release())
        except RuntimeError:
            self._semaphore.release()
    
    async def __aenter__(self):
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
    
    def get_current_stats(self) -> dict:
        return {
            "name": self.name,
            "current_concurrency": self._current_concurrency,
            "active_requests": self._active_requests,
            "base_concurrency": self.base_concurrency,
            "max_concurrency": self.max_concurrency,
            "min_concurrency": self.min_concurrency
        }


class RequestPriority(Enum):
    USER_INTERACTIVE = 0
    USER_QUERY = 1
    CRITICAL = 2
    HIGH = 3
    NORMAL = 4
    LOW = 5
    BACKGROUND = 6

@dataclass
class QueuedRequest:
    priority: RequestPriority
    callback: Callable
    args: tuple
    kwargs: dict
    future: asyncio.Future
    enqueue_time: float

class PriorityRequestQueue:
    def __init__(self, rate_limiter: AdaptiveRateLimiter):
        self.rate_limiter = rate_limiter
        self._queues: dict[RequestPriority, list[QueuedRequest]] = {
            RequestPriority.USER_INTERACTIVE: [],
            RequestPriority.USER_QUERY: [],
            RequestPriority.CRITICAL: [],
            RequestPriority.HIGH: [],
            RequestPriority.NORMAL: [],
            RequestPriority.LOW: [],
            RequestPriority.BACKGROUND: []
        }
        self._lock = asyncio.Lock()
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
    
    async def start(self):
        if self._running:
            return
        self._running = True
        self._processor_task = asyncio.create_task(self._process_queue())
    
    async def stop(self):
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
    
    async def enqueue(
        self,
        callback: Callable,
        *args,
        priority: RequestPriority = RequestPriority.NORMAL,
        **kwargs
    ) -> Any:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        
        request = QueuedRequest(
            priority=priority,
            callback=callback,
            args=args,
            kwargs=kwargs,
            future=future,
            enqueue_time=time.time()
        )
        
        async with self._lock:
            self._queues[priority].append(request)
        
        return await future
    
    async def _process_queue(self):
        while self._running:
            try:
                request = await self._get_next_request()
                if request is None:
                    await asyncio.sleep(0.1)
                    continue
                
                wait_time = time.time() - request.enqueue_time
                if wait_time > 30.0:
                    request.future.set_exception(
                        TimeoutError(f"Request timed out in queue ({wait_time:.1f}s)")
                    )
                    continue
                
                async with self.rate_limiter:
                    try:
                        if asyncio.iscoroutinefunction(request.callback):
                            result = await request.callback(*request.args, **request.kwargs)
                        else:
                            result = await asyncio.to_thread(
                                request.callback, *request.args, **request.kwargs
                            )
                        request.future.set_result(result)
                    except Exception as e:
                        request.future.set_exception(e)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing request queue: {e}")
                await asyncio.sleep(0.5)
    
    async def _get_next_request(self) -> Optional[QueuedRequest]:
        async with self._lock:
            for priority in RequestPriority:
                if self._queues[priority]:
                    return self._queues[priority].pop(0)
        return None
    
    def get_queue_stats(self) -> dict:
        return {
            "queues": {
                priority.name: len(queue)
                for priority, queue in self._queues.items()
            },
            "rate_limiter": self.rate_limiter.get_current_stats()
        }


tdx_rate_limiter = AdaptiveRateLimiter(
    name="tdx_quotes",
    base_concurrency=5,
    max_concurrency=15,
    min_concurrency=2,
    check_interval=5.0
)

sina_rate_limiter = AdaptiveRateLimiter(
    name="sina_quotes",
    base_concurrency=8,
    max_concurrency=20,
    min_concurrency=3,
    check_interval=5.0
)

ai_rate_limiter = AdaptiveRateLimiter(
    name="ai_requests",
    base_concurrency=3,
    max_concurrency=8,
    min_concurrency=1,
    check_interval=10.0
)

db_rate_limiter = AdaptiveRateLimiter(
    name="db_queries",
    base_concurrency=10,
    max_concurrency=30,
    min_concurrency=5,
    check_interval=5.0
)
