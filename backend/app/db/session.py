"""
数据库会话管理模块
配置连接池和会话工厂
"""
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os

# 数据库文件路径 (指向 backend/aitrader.db)
DATABASE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'aitrader.db')
DATABASE_URL = f"sqlite:///{DATABASE_PATH}"

# 创建引擎，配置连接池
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # SQLite 特殊配置
    poolclass=QueuePool,
    pool_size=50,           # 增加连接池大小，从 10 增加到 50
    max_overflow=100,       # 增加最大溢出连接数，从 20 增加到 100
    pool_timeout=60,        # 增加连接超时时间，从 30 增加到 60 秒
    pool_recycle=1800,      # 减少连接回收时间，从 3600 减少到 1800 秒，及时释放长期不用的连接
    pool_pre_ping=True,     # 连接前检查可用性
    echo=False              # 不打印 SQL 语句（生产环境）
)

# 增加 SQLite 并发优化监听器
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    # 开启 WAL 模式，极大提升并发读写性能
    cursor.execute("PRAGMA journal_mode=WAL")
    # 设置繁忙等待超时，避免 database is locked 报错
    cursor.execute("PRAGMA busy_timeout=60000")
    # 降低同步级别，提升写入速度（有掉电丢失少量数据风险，但对行情系统可接受）
    cursor.execute("PRAGMA synchronous=NORMAL")
    # 设置缓存大小，提升读取速度
    cursor.execute("PRAGMA cache_size=-10000") # 约 10MB
    cursor.close()

# 创建会话工厂
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# 声明基类
Base = declarative_base()


def get_db():
    """
    获取数据库会话的依赖注入函数
    用于 FastAPI 路由中自动管理数据库连接
    
    使用示例:
        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
