"""
统一的日志配置模块
规范日志级别使用
"""
import logging
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


class LoggerConfig:
    """日志配置管理器"""
    
    @staticmethod
    def setup_logger(
        name: str,
        log_file: str = None,
        level: int = logging.INFO,
        backup_count: int = 7
    ) -> logging.Logger:
        """
        配置并返回日志记录器
        
        Args:
            name: 日志记录器名称
            log_file: 日志文件路径（可选）
            level: 日志级别
            backup_count: 保留的日志文件天数
        
        Returns:
            配置好的日志记录器
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # 避免重复添加处理器
        if logger.handlers:
            return logger
        
        # 格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 文件处理器（如果指定了日志文件）
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 按天轮转，保留 7 天
            file_handler = TimedRotatingFileHandler(
                log_file,
                when="midnight",
                interval=1,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger


# 日志级别使用指南
"""
日志级别使用规范:

1. DEBUG (10):
   - 详细的调试信息
   - 变量值、函数参数
   - 仅在开发环境使用
   示例: logger.debug(f"Processing stock {ts_code} with price {price}")

2. INFO (20):
   - 正常的业务流程信息
   - 系统启动、关闭
   - 重要操作的确认
   示例: logger.info(f"Trade executed: BUY {ts_code} at {price}")

3. WARNING (30):
   - 警告信息，不影响系统运行
   - 可恢复的错误
   - 配置缺失但有默认值
   示例: logger.warning(f"API rate limit approaching: {calls}/100")

4. ERROR (40):
   - 错误信息，影响功能但不致命
   - 需要人工介入
   - 应包含堆栈信息
   示例: logger.error(f"Failed to fetch data for {ts_code}", exc_info=True)

5. CRITICAL (50):
   - 严重错误，系统无法继续运行
   - 数据损坏、服务崩溃
   示例: logger.critical("Database connection lost", exc_info=True)
"""


# 创建默认日志记录器
def get_logger(name: str) -> logging.Logger:
    """获取日志记录器的便捷函数"""
    # 确保日志路径是相对于项目根目录的绝对路径
    project_root = Path(__file__).parent.parent.parent
    log_dir = project_root / "logs"
    
    return LoggerConfig.setup_logger(
        name=name,
        log_file=str(log_dir / f"{name}.log"),
        level=logging.INFO
    )
