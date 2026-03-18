from datetime import datetime
import threading
import logging
import os
from logging.handlers import TimedRotatingFileHandler
import contextvars
from contextlib import contextmanager


class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    def doRollover(self):
        try:
            super().doRollover()
        except PermissionError:
            return

# Configure standard logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class SelectorLogger:
    def __init__(self):
        self._logs_by_channel: dict[str, list[str]] = {"default": []}
        self._lock = threading.Lock()
        self._max_logs = 2000
        self._channel_var: contextvars.ContextVar[str] = contextvars.ContextVar("selector_log_channel", default="default")
        self.std_logger = logging.getLogger("app")
        self.std_logger.setLevel(logging.INFO)
        self.std_logger.propagate = False
        
        # Ensure it has handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Clear existing handlers to avoid duplicates
        self.std_logger.handlers = []
        
        # 1. Terminal output
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self.std_logger.addHandler(stream_handler)
            
        # 2. File output
        try:
            # Get backend directory
            current_file = os.path.abspath(__file__)
            # backend/app/services/logger.py -> backend
            backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
            log_dir = os.path.join(backend_dir, "logs")
            
            if not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
                
            log_path = os.path.join(log_dir, "selector.log")
            # 按天轮转，保留 7 天
            file_handler = SafeTimedRotatingFileHandler(
                log_path, 
                when="midnight", 
                interval=1, 
                backupCount=7, 
                encoding='utf-8',
                delay=True
            )
            file_handler.setFormatter(formatter)
            self.std_logger.addHandler(file_handler)
        except Exception as e:
            # Fallback to a simpler path if needed
            print(f"Failed to setup file logging: {e}")

    def info(self, message: str, *args, **kwargs):
        self.log(message, "INFO", *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        self.log(message, "ERROR", *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        self.log(message, "WARNING", *args, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        self.log(message, "DEBUG", *args, **kwargs)

    def get_channel(self) -> str:
        try:
            return str(self._channel_var.get() or "default")
        except Exception:
            return "default"

    @contextmanager
    def bind(self, channel: str):
        token = self._channel_var.set(str(channel or "default"))
        try:
            yield
        finally:
            try:
                self._channel_var.reset(token)
            except Exception:
                pass

    def log(self, message: str, level: str = "INFO", *args, **kwargs):
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"[{timestamp}] [{level}] {message}"
        
        with self._lock:
            channel = self.get_channel()
            if channel not in self._logs_by_channel:
                self._logs_by_channel[channel] = []
            logs = self._logs_by_channel[channel]
            logs.append(log_entry)
            if len(logs) > self._max_logs:
                del logs[: max(1, len(logs) - self._max_logs)]
        
        # Using standard logger for output instead of print
        # try:
        #     print(log_entry, flush=True)
        # except Exception:
        #     pass
            
        # Also log via standard logger for file handlers etc.
        if level == "ERROR":
            self.std_logger.error(message, *args, **kwargs)
        elif level == "WARNING":
            self.std_logger.warning(message, *args, **kwargs)
        elif level == "DEBUG":
            self.std_logger.debug(message, *args, **kwargs)
        else:
            self.std_logger.info(message, *args, **kwargs)

    def get_logs(self, channel: str | None = None):
        with self._lock:
            ch = str(channel or self.get_channel() or "default")
            return list(self._logs_by_channel.get(ch, []))

    def clear(self, channel: str | None = None):
        with self._lock:
            ch = str(channel or self.get_channel() or "default")
            if ch == "default":
                self._logs_by_channel[ch] = []
            else:
                self._logs_by_channel.pop(ch, None)

selector_logger = SelectorLogger()
# Alias for broader usage
logger = selector_logger
