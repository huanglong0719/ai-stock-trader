import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# 显式加载 backend/.env 文件
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(env_path)

class Settings(BaseSettings):
    TUSHARE_TOKEN: str = ""
    DEEPSEEK_API_KEY: str = ""
    DEEPSEEK_BASE_URL: str = "https://api.deepseek.com/v1"

    ENABLE_THS_STATS: bool = False
    THS_INDEXFLASH_COOKIE: str = ""
    THS_INDEXFLASH_UA: str = ""

    # 小米 MiMo API 配置
    MIMO_API_KEY: str = ""
    MIMO_BASE_URL: str = "https://api.xiaomimimo.com/v1"
    MIMO_MODEL: str = "mimo-v2-flash"
    
    NVIDIA_NIM_API_KEY: str = ""
    NVIDIA_NIM_BASE_URL: str = "https://integrate.api.nvidia.com/v1"
    NVIDIA_NIM_MODEL: str = "deepseek-ai/deepseek-v3.1-terminus"

    # 搜索 API 配置 (可选)
    SEARCH_API_KEY: str = "" 
    SEARCH_ENGINE: str = "serper"
    
    # 自动交易开关 (默认开启)
    ENABLE_AUTO_TRADE: bool = True

    # Redis 配置
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str | None = None
    REDIS_AUTO_START: bool = True
    REDIS_SERVER_PATH: str | None = None

    MINUTE_AUTO_SYNC_AFTER_CLOSE: bool = True
    MINUTE_SYNC_POOL: str = "shsz"
    MINUTE_SYNC_DAYS: int = 30
    MINUTE_SYNC_FREQS: str = "5min,30min"
    MINUTE_SYNC_LIMIT: int = 6000
    MINUTE_SYNC_CONCURRENCY: int = 32
    MINUTE_SYNC_AFTER_CLOSE_HOUR: int = 16
    MINUTE_SYNC_AFTER_CLOSE_MINUTE: int = 10

    TDX_REALTIME_SYNC_ENABLED: bool = False
    TDX_DAILY_ARCHIVE_ENABLED: bool = False

    AI_CONTEXT_SUMMARY_ENABLED: bool = True
    AI_CONTEXT_SUMMARY_TRIGGER_CHARS: int = 12000
    AI_CONTEXT_SUMMARY_CHUNK_CHARS: int = 6000
    AI_CONTEXT_SUMMARY_MAX_MERGE_CHARS: int = 8000

    TDX_VIPDOC_ROOT: str = r"D:\tdxkxgzhb"
    
    # Tushare 频率限制 (每分钟最多调用次数)
    TUSHARE_MAX_CALLS_PER_MINUTE: int = 10
    
    class Config:
        env_file = ".env"

settings = Settings()
