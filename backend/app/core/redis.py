import redis
from typing import Optional
from app.core.config import settings
from app.services.logger import logger

redis_client: Optional[redis.Redis]
try:
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        db=settings.REDIS_DB,
        password=settings.REDIS_PASSWORD,
        decode_responses=True
    )
    # 测试连接
    # redis_client.ping()
except Exception as e:
    logger.error(f"Failed to initialize Redis client: {e}")
    redis_client = None
