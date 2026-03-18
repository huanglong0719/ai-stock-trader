import asyncio
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from app.db.session import SessionLocal
from app.models.system_models import SystemJobLog, SystemHeartbeat
from app.services.logger import logger
import traceback
import time

def _is_db_locked_error(exc: Exception) -> bool:
    return "database is locked" in str(exc).lower()

def _commit_with_retry(db: Session, max_attempts: int = 6) -> bool:
    for attempt in range(max_attempts):
        try:
            db.commit()
            return True
        except OperationalError as e:
            db.rollback()
            if _is_db_locked_error(e) and attempt < max_attempts - 1:
                time.sleep(min(1.0, 0.05 * (2 ** attempt)))
                continue
            raise
    return False

class MonitorService:
    def __init__(self):
        pass

    async def run_intraday_scan(self):
        import uuid
        from app.services.logger import selector_logger
        from app.services.review_service import review_service

        channel = f"intraday_scan_{uuid.uuid4().hex}"
        selector_logger.clear(channel)
        with selector_logger.bind(channel):
            try:
                # [优化] 增加超时控制，防止任务卡死阻塞系统
                # 注意：perform_intraday_scan 是 async 函数，直接 await 即可
                # 如果内部有 heavy CPU 操作，建议在内部使用 to_thread 优化
                await asyncio.wait_for(review_service.perform_intraday_scan(), timeout=600.0)
            except asyncio.TimeoutError:
                logger.error(f"Intraday scan timed out (channel={channel})")
            except asyncio.CancelledError:
                logger.info(f"Intraday scan cancelled (shutdown/reload) (channel={channel})")
            except Exception as e:
                logger.error(f"Intraday scan failed (channel={channel}): {e}")

    async def run_late_session_check(self):
        import uuid
        from app.services.logger import selector_logger
        from app.services.trading_service import trading_service

        channel = f"late_session_check_{uuid.uuid4().hex}"
        selector_logger.clear(channel)
        with selector_logger.bind(channel):
            try:
                await asyncio.wait_for(trading_service.check_late_session_opportunity(), timeout=600.0)
            except asyncio.TimeoutError:
                logger.error(f"Late session check timed out (channel={channel})")
            except Exception as e:
                logger.error(f"Late session check failed (channel={channel}): {e}")

    async def run_noon_review(self):
        import uuid
        from app.services.logger import selector_logger
        from app.services.review_service import review_service

        channel = f"noon_review_{uuid.uuid4().hex}"
        selector_logger.clear(channel)
        with selector_logger.bind(channel):
            try:
                await asyncio.wait_for(review_service.perform_noon_review(), timeout=900.0)
            except asyncio.TimeoutError:
                logger.error(f"Noon review timed out (channel={channel})")
            except Exception as e:
                logger.error(f"Noon review failed (channel={channel}): {e}")

    async def log_job_start(self, job_name: str) -> int:
        """
        记录任务开始
        Returns: log_id
        """
        def _sync_log_start():
            db = SessionLocal()
            try:
                log = SystemJobLog(
                    job_name=job_name,
                    status="RUNNING",
                    start_time=datetime.now()
                )
                db.add(log)
                _commit_with_retry(db)
                db.refresh(log)
                return log.id
            except Exception as e:
                logger.error(f"Failed to log job start: {e}")
                return -1
            finally:
                db.close()
        return await asyncio.to_thread(_sync_log_start)

    async def log_job_end(self, log_id: int, status: str = "SUCCESS", message: str = None):
        """
        记录任务结束
        """
        if log_id == -1: 
            logger.warning(f"log_job_end called with log_id -1. status={status}")
            return

        def _sync_log_end():
            db = SessionLocal()
            try:
                log = db.query(SystemJobLog).filter(SystemJobLog.id == log_id).first()
                if log:
                    log.end_time = datetime.now()
                    log.status = status
                    log.message = message
                    # Calculate duration
                    if log.start_time:
                        delta = log.end_time - log.start_time
                        log.duration_seconds = float(delta.total_seconds())
                    
                    _commit_with_retry(db)
                    logger.info(f"Job {log.job_name} (id={log_id}) finished with status {status}")
                else:
                    logger.error(f"Could not find job log with id {log_id}")
            except Exception as e:
                logger.error(f"Failed to log job end: {e}")
                db.rollback()
            finally:
                db.close()
        await asyncio.to_thread(_sync_log_end)

    async def cleanup_stale_jobs(self, is_startup: bool = True):
        """
        清理长期处于 RUNNING 状态的过时任务 (通常是系统重启或崩溃导致)
        is_startup: 是否是系统启动时调用，如果是，则清理所有 RUNNING 状态的任务
        """
        def _sync_cleanup():
            db = SessionLocal()
            try:
                if is_startup:
                    # 系统启动时，将所有之前 RUNNING 状态的任务标记为 INTERRUPTED
                    stale_jobs = db.query(SystemJobLog).filter(SystemJobLog.status == "RUNNING").all()
                    msg = "System restarted or task crashed"
                    status = "INTERRUPTED"
                else:
                    # 非启动时，只清理超过 4 小时的任务
                    from datetime import timedelta
                    stale_threshold = datetime.now() - timedelta(hours=4)
                    stale_jobs = db.query(SystemJobLog).filter(
                        SystemJobLog.status == "RUNNING",
                        SystemJobLog.start_time < stale_threshold
                    ).all()
                    msg = "Job marked as stale (timed out > 4h)"
                    status = "STALE"

                if not stale_jobs:
                    return 0
                
                count = 0
                for job in stale_jobs:
                    job.status = status
                    job.message = msg
                    job.end_time = datetime.now()
                    if job.start_time:
                        delta = job.end_time - job.start_time
                        job.duration_seconds = float(delta.total_seconds())
                    count += 1
                
                _commit_with_retry(db)
                logger.info(f"Cleaned up {count} stale jobs from database (startup={is_startup}).")
                return count
            except Exception as e:
                logger.error(f"Failed to cleanup stale jobs: {e}")
                db.rollback()
                return 0
            finally:
                db.close()
        return await asyncio.to_thread(_sync_cleanup)

    async def update_heartbeat(self, component: str, status: str = "OK", details: str = ""):
        """
        更新组件心跳
        """
        def _sync_update():
            max_attempts = 6
            for attempt in range(max_attempts):
                db = SessionLocal()
                try:
                    hb = db.query(SystemHeartbeat).filter(SystemHeartbeat.component == component).first()
                    if not hb:
                        hb = SystemHeartbeat(component=component)
                        db.add(hb)
                    
                    hb.last_beat = datetime.now()
                    hb.status = status
                    hb.details = details
                    _commit_with_retry(db)
                    return
                except OperationalError as e:
                    db.rollback()
                    if _is_db_locked_error(e) and attempt < max_attempts - 1:
                        time.sleep(min(1.0, 0.05 * (2 ** attempt)))
                        continue
                    logger.error(f"Failed to update heartbeat: {e}")
                    return
                except Exception as e:
                    logger.error(f"Failed to update heartbeat: {e}")
                    return
                finally:
                    db.close()
        await asyncio.to_thread(_sync_update)

    async def get_system_status(self):
        """
        获取系统当前状态报告
        """
        def _sync_get_status():
            db = SessionLocal()
            try:
                # 1. Check recent jobs (raw logs)
                recent_logs = db.query(SystemJobLog).order_by(SystemJobLog.start_time.desc()).limit(20).all()
                
                # 2. Check heartbeats
                heartbeats = db.query(SystemHeartbeat).all()
                
                # 3. Analyze Job Health (Group by Job Name)
                # Define expected jobs and frequencies (in minutes)
                # This logic helps detect "Missing" or "Stale" jobs
                expected_jobs = {
                    "trade_monitor": 15,         # 盘中交易监控 (每分钟)
                    "market_analysis": 30,      # 盘中市场分析 (30min)
                    "intraday_scan": 30,        # 高频交易扫描 (30min)
                    "intraday_calc": 30,        # 盘中指标计算 (30min)
                    "ai_periodic_monitor": 15,  # 监控列表巡检 (15min)
                    "position_periodic_monitor": 5, # 持仓跟踪 (5min)
                    "noon_review": 1440,        # 午间复盘 (每日)
                    "daily_sync": 1440,         # 每日同步 (每日)
                    "post_close_minute_sync": 1440, # 盘后分钟同步 (每日)
                    "late_session_check": 1440, # 尾盘选股 (每日)
                    "daily_learning": 1440,     # 每日学习 (每日)
                    "position_settlement": 1440, # 持仓结算 (每日)
                    "stock_basic_sync": 10080,  # 股票基本信息同步 (每周)
                }
                
                job_health = {}
                for job_name in expected_jobs.keys():
                    last_run = db.query(SystemJobLog).filter(
                        SystemJobLog.job_name == job_name
                    ).order_by(SystemJobLog.start_time.desc()).first()
                    
                    status = "UNKNOWN"
                    last_time = None
                    duration = None
                    msg = ""
                    
                    if last_run:
                        last_time = last_run.start_time
                        status = last_run.status
                        duration = last_run.duration_seconds
                        msg = last_run.message or ""
                        
                        # Check for Stale RUNNING (e.g., > 1 hour)
                        if status == "RUNNING" and last_run.start_time:
                            delta = datetime.now() - last_run.start_time
                            if delta.total_seconds() > 3600:
                                status = "STALE (Crashed?)"
                        
                        # Check for Missing/Late (simple heuristic)
                        freq_mins = expected_jobs.get(job_name)
                        if freq_mins and last_time:
                             delta_since_last = (datetime.now() - last_time).total_seconds() / 60
                             
                             # [Fix] 交易时间感知：非交易时间不报 LATE
                             from app.services.market.market_utils import is_trading_time
                             is_trading = is_trading_time()
                             
                             # 盘中高频任务列表
                             intraday_jobs = ["trade_monitor", "market_analysis", "intraday_scan", "intraday_calc"]
                             
                             # 如果是非交易时间，且任务是盘中任务，则放宽检查
                             if not is_trading and job_name in intraday_jobs:
                                 # 如果是非交易时间，只要今天或者昨天收盘前运行过，就不算 LATE
                                 # 简单处理：非交易时间直接显示 SLEEPING 而不是 LATE
                                 if "LATE" not in status and "STALE" not in status:
                                     # 检查是否真的过久（比如超过 24小时）
                                     if delta_since_last > 1440 + 60: 
                                         status = f"LATE? ({int(delta_since_last/60)}h ago)"
                                     else:
                                         # 正常休眠
                                         pass 
                             else:
                                 # 正常交易时间或非盘中任务，保持原有逻辑
                                 # 3x frequency buffer to avoid false positives during breaks/weekends
                                 if delta_since_last > freq_mins * 5 and delta_since_last > 120: 
                                     if "LATE" not in status and "STALE" not in status:
                                         status = f"LATE? ({int(delta_since_last)}m ago)"
                                
                    job_health[job_name] = {
                        "last_run": last_time,
                        "status": status,
                        "duration": duration,
                        "message": msg
                    }

                return {
                    "heartbeats": [{
                        "component": hb.component,
                        "last_beat": hb.last_beat,
                        "status": hb.status,
                        "details": hb.details
                    } for hb in heartbeats],
                    "recent_jobs": [{
                        "job": l.job_name,
                        "start": l.start_time,
                        "end": l.end_time,
                        "status": l.status,
                        "duration": l.duration_seconds,
                        "message": l.message
                    } for l in recent_logs],
                    "job_health": job_health
                }
            finally:
                db.close()
        return await asyncio.to_thread(_sync_get_status)


monitor_service = MonitorService()
