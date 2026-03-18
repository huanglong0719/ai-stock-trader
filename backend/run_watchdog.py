
import subprocess
import time
import os
import sys
import signal
import sqlite3
from datetime import datetime, timedelta

# 配置
CHECK_INTERVAL = 60         # 每 60 秒检查一次
HEARTBEAT_TIMEOUT = 300     # 如果 5 分钟日志没动，视为假死 (仅当 DB 心跳也失效时)
DB_HEARTBEAT_TIMEOUT = 900  # 如果 15 分钟数据库心跳没更新，视为调度器挂了
FAIL_THRESHOLD_DB_STALE = 3
FAIL_THRESHOLD_DB_ERROR = 5
MAIN_SCRIPT = "app.main"

def get_log_file():
    # 监控核心业务日志 selector.log
    return os.path.join(os.path.dirname(__file__), "logs", "selector.log")

def get_db_file():
    return os.path.join(os.path.dirname(__file__), "aitrader.db")

def get_file_mtime(filepath):
    try:
        return os.path.getmtime(filepath)
    except FileNotFoundError:
        return 0

def check_db_heartbeat():
    """
    检查数据库中的调度器心跳
    Returns:
        True:  Fresh (正常)
        False: Stale (过期)
        None:  Error/Unknown (查询失败或不存在)
    """
    db_path = get_db_file()
    if not os.path.exists(db_path):
        return None

    try:
        # 使用只读模式查询，避免锁库
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        cursor = conn.cursor()
        
        # 查询 scheduler 组件的最后心跳
        cursor.execute("SELECT last_beat FROM system_heartbeats WHERE component='scheduler'")
        row = cursor.fetchone()
        conn.close()

        if row and row[0]:
            last_beat_str = row[0]
            try:
                if "." in last_beat_str:
                    last_beat = datetime.strptime(last_beat_str, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    last_beat = datetime.strptime(last_beat_str, "%Y-%m-%d %H:%M:%S")
                
                delta = (datetime.now() - last_beat).total_seconds()
                if delta > DB_HEARTBEAT_TIMEOUT:
                    print(f"[{datetime.now()}] DB Heartbeat Stale! Scheduler last beat: {last_beat} ({int(delta)}s ago)")
                    return False # Stale
                return True # Fresh
            except Exception as e:
                print(f"[{datetime.now()}] Error parsing heartbeat time: {e}")
                return None
        
        return None # No record
    except Exception as e:
        print(f"[{datetime.now()}] Error checking DB heartbeat: {e}")
        return None # Error

def kill_process_tree(pid):
    try:
        # Windows taskkill
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"[{datetime.now()}] Killed process tree {pid}")
    except Exception as e:
        print(f"Error killing process: {e}")

def run_system():
    print(f"[{datetime.now()}] Starting AI Trader System...")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    
    # 假设我们在 backend 目录下运行
    process = subprocess.Popen(
        [sys.executable, "-u", "-m", "app.main"],
        cwd=os.path.dirname(__file__),
        env=env,
        shell=False
    )
    return process

def main():
    print(f"Watchdog config: Log timeout={HEARTBEAT_TIMEOUT}s, DB timeout={DB_HEARTBEAT_TIMEOUT}s")
    print(f"Monitoring log: {get_log_file()}")
    
    process = run_system()
    
    # 给系统一点启动时间
    time.sleep(30)
    last_healthy_ts = time.time()
    consecutive_fail = 0
    consecutive_db_error = 0
    
    try:
        while True:
            time.sleep(CHECK_INTERVAL)
            
            # 1. 检查进程是否存活
            if process.poll() is not None:
                print(f"[{datetime.now()}] System process exited unexpectedly (code {process.returncode}). Restarting...")
                process = run_system()
                time.sleep(30)
                continue
            
            # 2. 优先检查数据库心跳
            # 如果 DB 心跳新鲜 (True)，说明调度器活着且能写库，此时忽略日志是否更新 (支持静默模式)
            db_status = check_db_heartbeat()
            if db_status is True:
                # System is healthy
                last_healthy_ts = time.time()
                consecutive_fail = 0
                consecutive_db_error = 0
                continue
                
            # 3. 如果 DB 心跳过期 (False) 或 查询失败 (None)，则回退到日志检查
            # 这种情况包括: 调度器挂了、DB锁死、或者系统刚启动还没写心跳
            log_file = get_log_file()
            last_mtime = get_file_mtime(log_file)
            now = time.time()
            
            log_stale = now - last_mtime > HEARTBEAT_TIMEOUT
            if db_status is None:
                consecutive_db_error += 1
            else:
                consecutive_db_error = 0

            if log_stale and (db_status is False or db_status is None):
                consecutive_fail += 1
            else:
                consecutive_fail = 0

            grace_seconds = DB_HEARTBEAT_TIMEOUT + HEARTBEAT_TIMEOUT
            if now - last_healthy_ts < grace_seconds:
                continue

            threshold = FAIL_THRESHOLD_DB_STALE if db_status is False else FAIL_THRESHOLD_DB_ERROR
            if log_stale and consecutive_fail >= threshold:
                # DB 不正常 且 日志也不动 -> 判定死亡
                reason = "DB Stale" if db_status is False else "DB Error/Missing"
                print(f"[{datetime.now()}] SYSTEM HANG DETECTED! ({reason} AND Log Stale)")
                print(f"[{datetime.now()}] Last log update: {datetime.fromtimestamp(last_mtime)}")
                print(f"[{datetime.now()}] Force restarting system...")
                
                kill_process_tree(process.pid)
                process = run_system()
                time.sleep(30)
                continue

    except KeyboardInterrupt:
        print("\nStopping Watchdog...")
        kill_process_tree(process.pid)

if __name__ == "__main__":
    print("=== AI Trader Watchdog Started ===")
    main()
