"""
数据库迁移脚本：添加财务筛选结果缓存表

新增表：
1. fina_screening_results - 财务筛选结果缓存表
"""
import sqlite3
import os

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(__file__), 'aitrader.db')

def migrate():
    """执行数据库迁移"""
    print(f"正在迁移数据库: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='fina_screening_results'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            print("✓ 创建 fina_screening_results 表...")
            cursor.execute("""
                CREATE TABLE fina_screening_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_code VARCHAR NOT NULL,
                    end_date DATE NOT NULL,
                    screening_json TEXT,
                    total_score FLOAT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            cursor.execute("""
                CREATE INDEX idx_fina_screening_ts_code_date 
                ON fina_screening_results(ts_code, end_date)
            """)
            
            print("  - 表创建成功")
            print("  - 索引创建成功")
        else:
            print("✓ fina_screening_results 表已存在，跳过")
        
        # 提交更改
        conn.commit()
        print("\n✅ 数据库迁移完成！")
        
        # 验证迁移结果
        print("\n验证迁移结果：")
        cursor.execute("SELECT COUNT(*) FROM fina_screening_results")
        count = cursor.fetchone()[0]
        print(f"  - fina_screening_results 表记录数: {count}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ 迁移失败: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
