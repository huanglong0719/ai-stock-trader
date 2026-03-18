import sqlite3

def check_recent_any(limit=20):
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    print(f"--- Recent {limit} logs ---")
    cursor.execute('''
        SELECT job_name, status, start_time, end_time, message 
        FROM system_job_logs 
        ORDER BY start_time DESC 
        LIMIT ?
    ''', (limit,))
    for row in cursor.fetchall():
        print(row)
    conn.close()

def check_running_jobs():
    conn = sqlite3.connect('backend/aitrader.db')
    cursor = conn.cursor()
    print("--- Currently RUNNING jobs ---")
    cursor.execute('''
        SELECT id, job_name, status, start_time, message 
        FROM system_job_logs 
        WHERE status = 'RUNNING'
        ORDER BY start_time DESC
    ''')
    rows = cursor.fetchall()
    if not rows:
        print("No running jobs found.")
    for row in rows:
        print(row)
    conn.close()

if __name__ == "__main__":
    check_running_jobs()
    print("\n")
    check_recent_any(10)
