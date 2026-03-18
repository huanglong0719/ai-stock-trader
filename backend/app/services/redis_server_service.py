import os
import socket
import subprocess
import shutil
from typing import Optional
from app.core.config import settings
from app.services.logger import logger


class RedisServerService:
    def __init__(self):
        self._proc: Optional[subprocess.Popen] = None
        self._started_by_us: bool = False
        self._last_warn_ts: float = 0.0

    def _tcp_ping(self, host: str, port: int, timeout: float = 0.4) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def _resolve_redis_server_path(self) -> Optional[str]:
        if settings.REDIS_SERVER_PATH and os.path.exists(settings.REDIS_SERVER_PATH):
            return settings.REDIS_SERVER_PATH

        which_path = shutil.which("redis-server")
        if which_path:
            return which_path

        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        candidates = [
            os.path.join(base_dir, "tools", "redis", "redis-server.exe"),
            r"C:\Program Files\Redis\redis-server.exe",
            r"C:\redis\redis-server.exe",
            r"D:\redis\redis-server.exe",
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        return None

    def ensure_started(self) -> bool:
        if not settings.REDIS_AUTO_START:
            return False

        host = settings.REDIS_HOST
        port = int(settings.REDIS_PORT)
        if self._tcp_ping(host, port, timeout=0.2):
            return True

        if self._started_by_us and self._proc and self._proc.poll() is None:
            for _ in range(10):
                if self._tcp_ping(host, port, timeout=0.2):
                    return True
            return False

        server_path = self._resolve_redis_server_path()
        if not server_path:
            self._warn_once("Redis 未检测到可执行文件，跳过自动启动。可在 .env 设置 REDIS_SERVER_PATH 指向 redis-server.exe")
            return False

        args = [
            server_path,
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
        ]
        if settings.REDIS_PASSWORD:
            args += ["--requirepass", settings.REDIS_PASSWORD]

        try:
            creationflags = 0
            if os.name == "nt":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            self._proc = subprocess.Popen(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
            self._started_by_us = True
        except Exception as e:
            self._warn_once(f"Redis 自动启动失败: {e}")
            return False

        for _ in range(20):
            if self._tcp_ping(host, port, timeout=0.2):
                logger.info("Redis auto-started.")
                return True
        self._warn_once("Redis 自动启动后仍不可用。")
        return False

    def stop(self):
        if not self._started_by_us:
            return
        if not self._proc:
            return
        try:
            if self._proc.poll() is None:
                self._proc.terminate()
        except Exception:
            pass

    def _warn_once(self, msg: str):
        now_ts = __import__("time").time()
        if now_ts - self._last_warn_ts < 300:
            return
        self._last_warn_ts = now_ts
        logger.warning(msg)


redis_server_service = RedisServerService()

