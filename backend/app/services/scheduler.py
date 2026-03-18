from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.services.data_sync import data_sync_service
from app.services.data_provider import data_provider
from app.services.indicator_service import indicator_service
from app.services.monitor_service import monitor_service
import logging
import asyncio
from datetime import datetime, timedelta
from app.core.config import settings

from app.services.trading_service import trading_service
from app.services.ai_service import ai_service
from app.services.learning_service import learning_service
from app.services.audit_service import audit_service
from app.services.review_service import review_service

from app.services.evolution_service import evolution_service
from app.services.tdx_data_service import tdx_service
from app.services import entrustment_signal
from app.services.reward_punish_service import reward_punish_service

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._trade_monitor_lock = asyncio.Lock()
        self._entrustment_monitor_lock = asyncio.Lock()
        self._entrustment_job_id = "entrustment_monitor"

    async def start(self):
        logging.getLogger("apscheduler").setLevel(logging.WARNING)
        entrustment_signal.set_notifier(self._resume_entrustment_job)

        # 0. 启动时清理过期的任务状态
        await monitor_service.cleanup_stale_jobs()

        # 1. 每天 17:30 更新日线数据
        self.scheduler.add_job(
            self.sync_job, 
            'cron', 
            day_of_week='mon-fri',
            hour=17, 
            minute=30,
            id='daily_sync',
            replace_existing=True
        )

        # 1.1 每天盘后增量同步分钟数据 (5min/30min)
        if getattr(settings, "MINUTE_AUTO_SYNC_AFTER_CLOSE", True):
            # [Active Exploration] 15:50 开始下载，确保收盘数据已就绪
            hour = 15
            minute = 50
            self.scheduler.add_job(
                self.post_close_minute_sync_job,
                'cron',
                day_of_week='mon-fri',
                hour=hour,
                minute=minute,
                id='post_close_minute_sync',
                replace_existing=True
            )
        
        # 2. 每周一凌晨 1:00 更新股票基本信息列表
        self.scheduler.add_job(
            self.stock_basic_sync_job,
            'cron',
            day_of_week='mon',
            hour=1,
            minute=0,
            id='stock_basic_sync',
            replace_existing=True
        )

        # 3. 盘中指标计算 (每30分钟)
        self.scheduler.add_job(
            self.intraday_calc_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute='5,35', # 错开 0,30 的市场分析
            id='intraday_calc',
            replace_existing=True
        )

        # 4. 实时分钟数据采集 (每分钟采集到内存，15:00 统一落库)
        # User Request: "分钟数据还是不对，实在不行我们就不要分钟数据了" -> Disabled
        # self.scheduler.add_job(
        #     self.minute_sync_job,
        #     'cron',
        #     day_of_week='mon-fri',
        #     hour='9-15',
        #     minute='*',
        #     id='minute_sync',
        #     replace_existing=True
        # )
        
        # 4.1 分钟数据集中落库 (每天 15:05)
        # User Request: Disabled
        # self.scheduler.add_job(
        #     self.flush_minute_data_job,
        #     'cron',
        #     day_of_week='mon-fri',
        #     hour=15,
        #     minute=5,
        #     id='minute_flush',
        #     replace_existing=True
        # )

        # 4.2 盘后全量下载分钟数据 (每天 15:35)
        # User Request: Disabled
        # self.scheduler.add_job(
        #     self.download_daily_minutes_job,
        #     'cron',
        #     day_of_week='mon-fri',
        #     hour=15,
        #     minute=35,
        #     id='download_daily_minutes',
        #     replace_existing=True
        # )

        # 5. 盘中交易监控 (每分钟) - 确保及时执行 AI 计划
        self.scheduler.add_job(
            self.trade_monitor_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute='*', # 改为每分钟执行一次
            id='trade_monitor',
            replace_existing=True
        )

        # 5.1 [新增] 09:25 开盘集合竞价确认 (确保昨选计划及时确认)
        self.scheduler.add_job(
            self.confirm_open_job,
            'cron',
            day_of_week='mon-fri',
            hour=9,
            minute=25,
            second=5, # 稍微延迟几秒确保行情到位
            id='confirm_open',
            replace_existing=True
        )

        # 5.2 [新增] AI 监控列表巡检 (每15分钟)
        self.scheduler.add_job(
            self.ai_periodic_monitor_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-14', # 9:30, 9:45, ..., 14:45
            minute='0,15,30,45',
            id='ai_periodic_monitor',
            replace_existing=True
        )

        # 5.2.1 [新增] 持仓 AI 跟踪 (每5分钟)
        self.scheduler.add_job(
            self.position_periodic_monitor_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute='*/5',
            id='position_periodic_monitor',
            replace_existing=True
        )

        self.scheduler.add_job(
            self.reward_punish_intraday_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute='2,7,12,17,22,27,32,37,42,47,52,57',
            id='reward_punish_intraday',
            replace_existing=True
        )

        # 5.3 [新增] 自动数据巡检与修复 (每小时，错开整点)
        # 09:20-15:20: 盘中实时修复
        # 19:20: 盘后全量数据就绪后的终极兜底修复 (确保15:50和17:30的任务都已完成)
        self.scheduler.add_job(
            self.auto_fix_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15,19',
            minute='20', # 9:20, 10:20... 15:20, 19:20
            id='auto_fix_minute_data',
            replace_existing=True
        )

        # [Active Exploration] 仅有挂单时唤醒，监控频率 10 秒
        # 仅当有活跃挂单时才唤醒 (entrustment_signal)
        entrustment_job = self.scheduler.add_job(
            self.entrustment_monitor_job,
            'interval',
            seconds=10,
            id=self._entrustment_job_id,
            replace_existing=True
        )
        try:
            entrustment_job.pause()
        except Exception:
            pass

        # 6. 盘中市场分析 (每30分钟)
        self.scheduler.add_job(
            self.market_analysis_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-15',
            minute='0,30',
            id='market_analysis',
            replace_existing=True
        )

        # 6.1 高频交易扫描 (每30分钟) - 新增
        self.scheduler.add_job(
            self.intraday_scan_job,
            'cron',
            day_of_week='mon-fri',
            hour='9-14', # 9:45, 10:15, 10:45, 11:15, 13:15, 13:45, 14:15, 14:45
            minute='15,45', # 错开市场分析的 0,30
            id='intraday_scan',
            replace_existing=True
        )

        # 7. 尾盘选股 (每天 14:45)
        # [优化] 增加 misfire_grace_time=900 (15分钟)，防止因盘中服务重启错过关键选股
        self.scheduler.add_job(
            self.late_session_job,
            'cron',
            day_of_week='mon-fri',
            hour=14,
            minute=45,
            misfire_grace_time=900,
            id='late_session_check',
            replace_existing=True
        )

        # 8. 午间自动复盘 (每天 11:40)
        self.scheduler.add_job(
            self.noon_review_job,
            'cron',
            day_of_week='mon-fri',
            hour=11,
            minute=40,
            id='noon_review',
            replace_existing=True
        )

        # 9. 每日离线学习 (每天 20:00)
        self.scheduler.add_job(
            self.learning_job,
            'cron',
            day_of_week='mon-fri',
            hour=20,
            minute=0,
            id='daily_learning',
            replace_existing=True
        )

        # 10. 系统心跳监控 (每5分钟)
        self.scheduler.add_job(
            self.system_heartbeat_job,
            'interval',
            minutes=5,
            id='system_heartbeat',
            replace_existing=True
        )

        # 6. 每日持仓结算 (T+1)
        self.scheduler.add_job(
            self.position_settlement_job,
            'cron',
            day_of_week='mon-fri',
            hour=9,
            minute=10,
            id='position_settlement',
            replace_existing=True
        )

        # 7. 每日收盘对账审计 (15:30)
        self.scheduler.add_job(
            self.audit_job,
            'cron',
            day_of_week='mon-fri',
            hour=15,
            minute=30,
            id='daily_audit',
            replace_existing=True
        )

        self.scheduler.add_job(
            self.close_counts_snapshot_job,
            'cron',
            day_of_week='mon-fri',
            hour=15,
            minute=5,
            id='close_counts_snapshot',
            replace_existing=True
        )

        # 7.1 盘后资金解冻 (每天 15:05)
        self.scheduler.add_job(
            self.unfreeze_funds_job,
            'cron',
            day_of_week='mon-fri',
            hour=15,
            minute=5,
            id='post_trade_unfreeze',
            replace_existing=True
        )

        # 7.2 盘后缓存清理 (每天 16:20)
        self.scheduler.add_job(
            self.purge_cache_job,
            'cron',
            day_of_week='mon-fri',
            hour=16,
            minute=20,
            id='purge_inactive_cache',
            replace_existing=True
        )

        # 7.2 记录每日盈亏与资金曲线 (每天 15:35, 审计之后)
        self.scheduler.add_job(
            self.daily_performance_job,
            'cron',
            day_of_week='mon-fri',
            hour=15,
            minute=35,
            id='daily_performance',
            replace_existing=True
        )

        self.scheduler.add_job(
            self.reward_punish_daily_job,
            'cron',
            day_of_week='mon-fri',
            hour=15,
            minute=40,
            id='reward_punish_daily',
            replace_existing=True
        )

        self.scheduler.add_job(
            self.daily_review_job,
            'cron',
            day_of_week='mon-fri',
            hour=20,
            minute=30,
            id='daily_review',
            replace_existing=True
        )

        # 8. 每月最后一天进行全面复核
        self.scheduler.add_job(
            self.audit_job,
            'cron',
            day='last',
            hour=23,
            minute=0,
            id='monthly_audit',
            replace_existing=True
        )

        # 启动时立即执行一次审计，确保系统一致性 (仅在交易日执行)
        self.scheduler.add_job(self.audit_job, id='initial_audit')

        # 11. 周级参数进化 (每周五 19:00)
        self.scheduler.add_job(
            self.evolution_job,
            'cron',
            day_of_week='fri',
            hour=19,
            minute=0,
            id='weekly_evolution',
            replace_existing=True
        )

        # 12. 每日“四信号共振”选股 (18:40)
        self.scheduler.add_job(
            self.four_signals_job,
            'cron',
            day_of_week='mon-fri',
            hour=18,
            minute=40,
            id='daily_four_signals',
            replace_existing=True
        )

        if getattr(settings, "TDX_REALTIME_SYNC_ENABLED", False):
            self.scheduler.add_job(
                self.tdx_realtime_job,
                'cron',
                day_of_week='mon-fri',
                hour='9-15',
                minute='*',
                id='tdx_realtime_sync',
                replace_existing=True
            )

        if getattr(settings, "TDX_DAILY_ARCHIVE_ENABLED", False):
            self.scheduler.add_job(
                self.tdx_archive_job,
                'cron',
                day_of_week='mon-fri',
                hour=17,
                minute=30,
                id='tdx_daily_archive',
                replace_existing=True
            )

        # 14. [新增] TDX 初始化数据下载 (启动后 1 分钟执行)
        self.scheduler.add_job(
            self.tdx_init_job,
            'date',
            run_date=datetime.now() + timedelta(minutes=1),
            id='tdx_init_download',
            replace_existing=True
        )

        self.scheduler.start()
        logger.info("Scheduler started.")

    async def close_counts_snapshot_job(self):
        try:
            # 交易日检查
            is_trade_day_res = await data_provider.check_trade_day()
            if not is_trade_day_res.get("is_open", False):
                return

            trade_date_str = await data_provider.get_last_trade_date(include_today=True)
            # 强制仅使用通达信数据源
            counts = await data_provider.market_data_service._fetch_market_counts(force_tdx=True)
            if counts and trade_date_str:
                # 检查是否获得了有效数据 (前5位不全为0)
                if any(c > 0 for c in counts[:5]):
                    await data_provider.market_data_service._save_close_counts(trade_date_str, counts, "TDX_RULES")
                    logger.info(f"Successfully saved close counts for {trade_date_str} using TDX-ONLY source")
                else:
                    logger.warning(f"Fetched zero counts for {trade_date_str} from TDX, skipping save")
        except Exception as e:
            logger.warning(f"close_counts_snapshot_job (TDX-ONLY) failed: {e}")

    async def tdx_realtime_job(self):
        await self._check_and_run(data_sync_service.sync_tdx_realtime_minutes, "tdx_realtime")

    async def tdx_archive_job(self):
        await self._check_and_run(data_sync_service.archive_tdx_minutes, "tdx_archive")

    async def tdx_init_job(self):
        # 初始化数据通常只需要执行一次，不管是不是交易日
        # 但为了保持安静，可以加上简单的日志控制
        try:
            # 只有交易日才进行完整的初始化同步，非交易日跳过以保持日志整洁
            is_trade_day_res = await data_provider.check_trade_day()
            if not is_trade_day_res.get("is_open", False):
                return
            await data_sync_service.init_tdx_data()
        except Exception as e:
            logger.error(f"Error in tdx_init_job: {e}")

    async def _check_and_run(self, job_func, job_name, allow_post_close: bool = False, timeout: float = 600.0):
        """通用包装器：检查是否是交易日再运行任务"""
        log_id = -1
        try:
            # 只有交易日才运行 (或者特定的非交易日任务)
            is_trade_day_res = await data_provider.check_trade_day()
            if not is_trade_day_res.get("is_open", False):
                if not allow_post_close or not data_provider.is_after_market_close():
                    return

            log_id = await monitor_service.log_job_start(job_name)
            try:
                await asyncio.wait_for(job_func(), timeout=timeout)
                await monitor_service.log_job_end(log_id, "SUCCESS")
            except asyncio.TimeoutError:
                await monitor_service.log_job_end(log_id, "FAILED", "timeout")
        except Exception as e:
            if log_id != -1:
                try:
                    await monitor_service.log_job_end(log_id, "FAILED", str(e))
                except Exception:
                    pass
            logger.error(f"Error in scheduled job {job_name}: {e}", exc_info=True)

    async def purge_cache_job(self):
        await self._check_and_run(data_provider.purge_inactive_cache, "purge_cache")

    async def sync_job(self):
        try:
            is_trade_day_res = await data_provider.check_trade_day()
            if is_trade_day_res.get("is_open", False):
                async def _run_daily_and_indicators():
                    await data_sync_service.sync_daily_data(calculate_indicators=False)
                    await indicator_service.calculate_all_indicators()
                await self._check_and_run(_run_daily_and_indicators, "daily_sync", allow_post_close=True, timeout=3600.0)
                return
            prev_trade_date = is_trade_day_res.get("prev_trade_date")
            if not prev_trade_date:
                return
            async def _run_prev():
                await data_sync_service.sync_daily_data(prev_trade_date, calculate_indicators=False)
                await indicator_service.calculate_all_indicators(trade_date=prev_trade_date)
            await self._check_and_run(_run_prev, "daily_sync", allow_post_close=True, timeout=3600.0)
        except Exception as e:
            logger.error(f"Error in sync_job: {e}")

    async def post_close_minute_sync_job(self):
        await self._check_and_run(data_sync_service.sync_post_close_minute_data, "post_close_minute_sync")

    async def intraday_calc_job(self):
        await self._check_and_run(indicator_service.calculate_intraday_indicators, "intraday_calc")

    async def minute_sync_job(self):
        """分钟级数据同步任务 (只采集到内存)"""
        # is_trading_time 内部已包含时间检查
        if not data_provider.is_trading_time():
            return
            
        try:
            # 现在 sync_realtime_minute_data 应该是异步的
            await data_sync_service.sync_realtime_minute_data()
        except Exception as e:
            logger.error(f"Minute sync failed: {e}")

    async def flush_minute_data_job(self):
        """分钟数据集中落库任务 (15:05)"""
        job_name = "flush_minute_data"
        log_id = await monitor_service.log_job_start(job_name)
        logger.info(f"Starting minute data flush at {datetime.now()}...")
        try:
            # 已经改为异步
            await data_sync_service.flush_minute_buffer()
            logger.info("Minute data flush finished.")
            await monitor_service.log_job_end(log_id, "SUCCESS")
        except Exception as e:
            error_msg = f"Minute flush failed: {e}"
            logger.error(error_msg)
            await monitor_service.log_job_end(log_id, "FAILED", error_msg)

    async def download_daily_minutes_job(self):
        """
        盘后全量下载分钟数据 (15:35)
        从新浪下载所有股票当天的 1min 数据，并自动聚合为 5/30min
        """
        job_name = "download_daily_minutes"
        log_id = await monitor_service.log_job_start(job_name)
        logger.info(f"Starting daily minute data download task at {datetime.now()}...")
        try:
            # 1. 获取所有股票列表
            from app.db.session import SessionLocal
            from app.models.stock_models import Stock
            
            db = SessionLocal()
            try:
                stocks = await asyncio.to_thread(lambda: db.query(Stock.ts_code).all())
                ts_codes = [s[0] for s in stocks]
            finally:
                db.close()
                
            if not ts_codes:
                logger.warning("No stocks found for minute data download.")
                await monitor_service.log_job_end(log_id, "SKIPPED", "No stocks found")
                return

            today = datetime.now().strftime('%Y%m%d')
            logger.info(f"Target date: {today}, Stocks count: {len(ts_codes)}")

            # 2. 并发下载 (控制并发度)
            # 使用 ThreadPoolExecutor 在后台运行
            import concurrent.futures
            
            def _sync_one(code):
                try:
                    data_sync_service.download_minute_data(code, today, today, freq='1min')
                except Exception as e:
                    logger.error(f"Error syncing minutes for {code}: {e}")

            # 放到独立线程中运行整个批量任务，避免阻塞事件循环太久
            def _run_batch():
                max_workers = 10
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(_sync_one, code) for code in ts_codes]
                    # 等待所有完成
                    concurrent.futures.wait(futures)
                logger.info("Daily minute data download task completed.")

            await asyncio.to_thread(_run_batch)
            await monitor_service.log_job_end(log_id, "SUCCESS", f"Synced {len(ts_codes)} stocks")
            
        except Exception as e:
            error_msg = f"Daily minute data download failed: {e}"
            logger.error(error_msg)
            await monitor_service.log_job_end(log_id, "FAILED", error_msg)

    async def trade_monitor_job(self):
        async with self._trade_monitor_lock:
            await self._check_and_run(trading_service.monitor_trades, "trade_monitor", timeout=120.0)

    async def confirm_open_job(self):
        """09:25 开盘集合竞价确认"""
        await self._check_and_run(review_service.perform_open_confirm_monitor, "confirm_open", timeout=60.0)

    async def ai_periodic_monitor_job(self):
        """AI 15分钟定期巡检"""
        await self._check_and_run(review_service.perform_ai_periodic_monitor, "ai_periodic_monitor", timeout=120.0)

    async def position_periodic_monitor_job(self):
        """持仓 5分钟 AI 跟踪"""
        await self._check_and_run(review_service.perform_position_periodic_monitor, "position_periodic_monitor", timeout=120.0)

    async def auto_fix_job(self):
        """自动修复缺失的分钟数据"""
        await self._check_and_run(data_sync_service.auto_fix_missing_minute_data, "auto_fix_minute_data", timeout=300.0)

    async def entrustment_monitor_job(self):
        async with self._entrustment_monitor_lock:
            try:
                from app.services.market.market_utils import is_trading_time
                if not is_trading_time():
                    self._pause_entrustment_job()
                    return
            except Exception:
                pass
            try:
                has_active = await trading_service.has_active_entrustments()
                if not has_active:
                    self._pause_entrustment_job()
                    return
            except Exception:
                pass
            await self._check_and_run(trading_service.monitor_entrustments, "entrustment_monitor", timeout=30.0)
            try:
                has_active = await trading_service.has_active_entrustments()
                if not has_active:
                    self._pause_entrustment_job()
            except Exception:
                pass

    async def entrustment_wakeup_job(self):
        """
        唤醒委托监控任务：
        1. 检查当前是否在交易时间
        2. 检查是否有活跃委托
        3. 如果满足条件且任务已停止，则启动
        """
        try:
            # 交易时间检查
            from app.services.market.market_utils import is_trading_time
            if not is_trading_time():
                return

            # 交易日检查
            is_trade_day_res = await data_provider.check_trade_day()
            if not is_trade_day_res.get("is_open", False):
                return

            # 检查活跃委托
            has_active = await trading_service.has_active_entrustments()
            job = self.scheduler.get_job(self._entrustment_job_id)
            
            if has_active and job:
                if job.next_run_time is None: # 任务当前被暂停
                    self.scheduler.resume_job(self._entrustment_job_id)
                    logger.info("Resumed entrustment monitor job due to active entrustments.")
            elif not has_active and job:
                if job.next_run_time is not None: # 任务当前正在运行
                    self.scheduler.pause_job(self._entrustment_job_id)
                    logger.info("Paused entrustment monitor job as no active entrustments.")
        except Exception as e:
            logger.error(f"Error in entrustment wakeup job: {e}")

    def _pause_entrustment_job(self):
        try:
            job = self.scheduler.get_job(self._entrustment_job_id)
            if job:
                job.pause()
        except Exception:
            pass

    def _resume_entrustment_job(self):
        try:
            job = self.scheduler.get_job(self._entrustment_job_id)
            if job:
                job.resume()
                try:
                    job.modify(next_run_time=datetime.now())
                except Exception:
                    pass
        except Exception:
            pass

    def _has_system_entrustments(self) -> bool:
        try:
            from sqlalchemy import or_
            from app.db.session import SessionLocal
            from app.models.stock_models import TradingPlan

            db = SessionLocal()
            try:
                # 优化查询：只查询 ID 且使用 exists()，减少数据传输
                # 同时确保查询尽可能简单
                today = datetime.now().date()
                q = (
                    db.query(TradingPlan.id)
                    .filter(TradingPlan.date == today)
                    .filter(TradingPlan.executed == False)
                    .filter(TradingPlan.source == "system")
                )
                
                # 预先过滤掉明显的非交易计划，减少后续 or_ 判断的压力
                # 如果 source 是 system 且未执行，通常就是我们需要关注的
                return db.query(q.exists()).scalar() is True
            finally:
                db.close()
        except Exception as e:
            logger.error(f"Error checking system entrustments: {e}")
            return False

    async def _run_trade_monitor(self):
        """实际执行监控逻辑"""
        start = datetime.now()
        results = await asyncio.gather(
            trading_service.check_and_execute_plans(),
            trading_service.execute_pending_sell_plans(),
            trading_service.check_positions_and_sell(),
            return_exceptions=True
        )

        for idx, r in enumerate(results):
            if isinstance(r, Exception):
                logger.error(f"Trade monitor subtask {idx} failed: {type(r).__name__}: {r}")

        await trading_service.sync_account_assets()
        logger.info(f"Trade monitor finished in {(datetime.now() - start).total_seconds():.1f}s")

    async def market_analysis_job(self):
        await self._check_and_run(self._run_market_analysis, "market_analysis", timeout=300.0)

    async def _run_market_analysis(self):
        now = datetime.now()
        if now.hour == 9 and now.minute == 0:
            return
        try:
            snapshot = await asyncio.wait_for(data_provider.get_market_snapshot(), timeout=20.0)
        except Exception as e:
            logger.warning(f"Market analysis snapshot failed: {e}")
            snapshot = {
                "up_count": 0,
                "down_count": 0,
                "limit_up_count": 0,
                "limit_down_count": 0,
                "time": datetime.now().strftime('%H:%M:%S')
            }
        analysis_result = ""
        try:
            analysis_result = await asyncio.wait_for(
                ai_service.analyze_market_snapshot(snapshot, force_refresh=True),
                timeout=25.0
            )
        except Exception as e:
            logger.warning(f"Market analysis AI failed: {e}")
        if analysis_result:
            logger.info(f"Market analysis AI result: {analysis_result}")
        
        # [新增] 响应用户需求：在市场分析的同时，立即执行个股异动扫描，确保及时发现交易机会
        # 这样 00/30 分时点既有大盘研判，也有个股机会扫描
        logger.info("Starting intraday stock scan as part of market analysis...")
        await monitor_service.run_intraday_scan()
        
        logger.info("Market analysis finished.")

    async def intraday_scan_job(self):
        await self._check_and_run(monitor_service.run_intraday_scan, "intraday_scan", timeout=600.0)

    async def late_session_job(self):
        await self._check_and_run(monitor_service.run_late_session_check, "late_session_check", timeout=600.0)

    async def noon_review_job(self):
        await self._check_and_run(monitor_service.run_noon_review, "noon_review", timeout=900.0)

    async def daily_review_job(self):
        await self._check_and_run(review_service.perform_daily_review, "daily_review", timeout=1200.0)

    async def learning_job(self):
        await self._check_and_run(learning_service.perform_daily_learning, "daily_learning", timeout=900.0)

    async def system_heartbeat_job(self):
        """系统心跳任务 (每5分钟)"""
        # 直接更新数据库心跳，不通过 _check_and_run (避免写 logs/selector.log)
        # 这样既能维持 Watchdog 的 DB 监控，又不会在盘后产生大量日志
        # 无论是否交易日都执行，确保看门狗能检测到系统存活
        try:
            await monitor_service.update_heartbeat("scheduler", "OK", "Running")
        except Exception:
            pass

    async def stock_basic_sync_job(self):
        # 股票列表更新每周一次，通常不需要严格检查交易日，但为了保持安静可以加上
        await self._check_and_run(data_sync_service.sync_stock_basic, "stock_basic_sync")

    async def position_settlement_job(self):
        await self._check_and_run(trading_service.settle_positions, "position_settlement")

    async def audit_job(self):
        await self._check_and_run(audit_service.run_daily_audit, "audit")

    async def daily_performance_job(self):
        """记录每日盈亏与资金曲线"""
        job_name = "daily_performance"
        log_id = await monitor_service.log_job_start(job_name)
        logger.info("Starting daily performance recording job...")
        try:
            await trading_service.record_daily_performance()
            await monitor_service.log_job_end(log_id, "SUCCESS")
        except Exception as e:
            logger.error(f"Daily performance job failed: {e}")
            await monitor_service.log_job_end(log_id, "FAILED", str(e))

    async def reward_punish_daily_job(self):
        job_name = "reward_punish_daily"
        log_id = await monitor_service.log_job_start(job_name)
        logger.info("Starting reward/punish daily evaluation...")
        try:
            metrics = await asyncio.to_thread(reward_punish_service.evaluate_daily)
            await monitor_service.log_job_end(log_id, "SUCCESS", f"metrics={metrics}")
        except Exception as e:
            logger.error(f"Reward/punish daily job failed: {e}")
            await monitor_service.log_job_end(log_id, "FAILED", str(e))

    async def reward_punish_intraday_job(self):
        job_name = "reward_punish_intraday"
        log_id = await monitor_service.log_job_start(job_name)
        try:
            metrics = await asyncio.to_thread(reward_punish_service.evaluate_intraday)
            await monitor_service.log_job_end(log_id, "SUCCESS", f"metrics={metrics}")
        except Exception as e:
            logger.error(f"Reward/punish intraday job failed: {e}")
            await monitor_service.log_job_end(log_id, "FAILED", str(e))

    async def unfreeze_funds_job(self):
        await self._check_and_run(trading_service.unfreeze_daily_funds, "unfreeze_funds")

    async def evolution_job(self):
        await self._check_and_run(evolution_service.evolve_parameters, "evolution")

    async def four_signals_job(self):
        """
        [新任务] 四信号共振选股 (18:40)
        """
        logger.info("Executing scheduled Four Signals strategy...")
        try:
            from app.services.stock_selector import stock_selector
            # 自动执行四信号策略
            await stock_selector.select_stocks(strategy="four_signals", top_n=5)
        except Exception as e:
            logger.error(f"Error in four_signals_job: {e}")

    def shutdown(self):
        try:
            entrustment_signal.set_notifier(None)
        except Exception:
            pass
        self.scheduler.shutdown()
        logger.info("Scheduler shutdown.")

scheduler_manager = SchedulerManager()
