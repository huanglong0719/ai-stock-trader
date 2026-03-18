import sys
import os
import time
import asyncio
from datetime import datetime, timedelta
import glob

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.monitor_service import monitor_service

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'monitor')

def ensure_log_dir():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def clean_old_logs(days=7):
    """Clean logs older than specified days"""
    try:
        ensure_log_dir()
        cutoff_time = time.time() - (days * 86400)
        
        # Pattern for monitor logs
        log_pattern = os.path.join(LOG_DIR, "monitor_*.log")
        
        for file_path in glob.glob(log_pattern):
            if os.path.getmtime(file_path) < cutoff_time:
                try:
                    os.remove(file_path)
                    print(f"[System] Cleaned old log: {os.path.basename(file_path)}")
                except Exception as e:
                    print(f"[System] Failed to delete {file_path}: {e}")
    except Exception as e:
        print(f"[System] Error in log cleanup: {e}")

def save_log_to_file(content):
    try:
        ensure_log_dir()
        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = f"monitor_{date_str}.log"
        file_path = os.path.join(LOG_DIR, filename)
        
        # Append mode
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(content + "\n")
    except Exception as e:
        print(f"Error saving log: {e}")

def format_status_report(status):
    output = []
    output.append(f"=== AI Trader System Monitor ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===")
    
    # Check for global staleness (Scheduler heartbeat)
    scheduler_hb = next((hb for hb in status['heartbeats'] if hb['component'] == 'scheduler'), None)
    if scheduler_hb and scheduler_hb['last_beat']:
        delta = (datetime.now() - scheduler_hb['last_beat']).total_seconds()
        if delta > 600: # 10 mins
            output.append("\n" + "!" * 80)
            output.append(f"WARNING: SYSTEM MAY BE DOWN! Scheduler heartbeat stale ({int(delta)}s ago).")
            output.append("Please restart the system using 'python run_watchdog.py'.")
            output.append("!" * 80 + "\n")

    # 1. Component Heartbeats
    output.append("\n[1. 组件运行状态 (Component Heartbeats)]")
    output.append(f"{'组件名称':<20} {'最后心跳':<25} {'状态':<10} {'详情'}")
    output.append("-" * 80)
    for hb in status['heartbeats']:
        output.append(f"{hb['component']:<20} {str(hb['last_beat']):<25} {hb['status']:<10} {hb['details'] or ''}")
        
    # 2. Expected Jobs Health
    output.append("\n[2. 自动任务健康度 (Auto-Job Health Summary)]")
    output.append(f"{'任务名称':<25} {'最后执行时间':<25} {'执行状态':<15} {'耗时(s)':<10} {'消息'}")
    output.append("-" * 100)
    # Sort by last run time descending
    sorted_jobs = sorted(status.get('job_health', {}).items(), 
                        key=lambda x: x[1]['last_run'] if x[1]['last_run'] else datetime.min, 
                        reverse=True)
    
    for job_name, info in sorted_jobs:
        last_run_str = info['last_run'].strftime('%Y-%m-%d %H:%M:%S') if info['last_run'] else "NEVER"
        status_str = info['status']
        dur = f"{info['duration']:.1f}" if info['duration'] is not None else "-"
        msg = (info['message'] or '')[:40]
        output.append(f"{job_name:<25} {last_run_str:<25} {status_str:<15} {dur:<10} {msg}")

    # 3. Recent Execution Logs
    output.append("\n[3. 最近执行流水 (Recent Job Execution Logs)]")
    output.append(f"{'任务名称':<25} {'开始时间':<20} {'耗时(s)':<10} {'状态':<10} {'执行结果'}")
    output.append("-" * 100)
    for job in status['recent_jobs']:
        start_str = job['start'].strftime('%H:%M:%S') if job['start'] else '-'
        duration = f"{job['duration']:.1f}" if job['duration'] else '-'
        msg = (job.get('message') or '')[:40]
        # In recent_jobs, status is usually SUCCESS/ERROR/RUNNING
        output.append(f"{job['job']:<25} {start_str:<20} {duration:<10} {job['status']:<10} {msg}")

    output.append("\n" + "=" * 100)
    return "\n".join(output)

async def print_status():
    try:
        status = await monitor_service.get_system_status()
        report = format_status_report(status)
        
        os.system('cls' if os.name == 'nt' else 'clear')
        print(report)
        
        # Save to file
        save_log_to_file(report)
    except Exception as e:
        print(f"Error in print_status: {e}")
        import traceback
        traceback.print_exc()

async def main():
    # Clean logs on startup
    clean_old_logs(7)
    
    last_clean_time = time.time()
    
    while True:
        try:
            await print_status()
            
            # Daily cleanup check
            if time.time() - last_clean_time > 86400: # 24 hours
                clean_old_logs(7)
                last_clean_time = time.time()
                
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            print("\nMonitor stopped.")
            break
        except Exception as e:
            print(f"Main Loop Error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
