"""
数据库迁移脚本：添加失败案例学习相关字段

新增字段：
1. PatternCase.is_successful - 标记案例是否成功
2. ReflectionMemory.source_event_type - 区分来源类型
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
        # 1. 检查 PatternCase 表是否存在 is_successful 字段
        cursor.execute("PRAGMA table_info(pattern_cases)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'is_successful' not in columns:
            print("✓ 添加 PatternCase.is_successful 字段...")
            cursor.execute("ALTER TABLE pattern_cases ADD COLUMN is_successful BOOLEAN DEFAULT 1")
            print("  - 字段已添加，默认值为 True（向后兼容）")
        else:
            print("✓ PatternCase.is_successful 字段已存在，跳过")
        
        # 2. 检查 ReflectionMemory 表是否存在 source_event_type 字段
        cursor.execute("PRAGMA table_info(reflection_memories)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'source_event_type' not in columns:
            print("✓ 添加 ReflectionMemory.source_event_type 字段...")
            cursor.execute("ALTER TABLE reflection_memories ADD COLUMN source_event_type VARCHAR")
            print("  - 字段已添加，默认值为 NULL")
        else:
            print("✓ ReflectionMemory.source_event_type 字段已存在，跳过")
        
        # 3. 提交更改
        conn.commit()
        print("\n✅ 数据库迁移完成！")
        
        # 4. 验证迁移结果
        print("\n验证迁移结果：")
        cursor.execute("PRAGMA table_info(pattern_cases)")
        pattern_columns = [row[1] for row in cursor.fetchall()]
        print(f"  - PatternCase 表字段数: {len(pattern_columns)}")
        print(f"  - 包含 is_successful: {'is_successful' in pattern_columns}")
        
        cursor.execute("PRAGMA table_info(reflection_memories)")
        memory_columns = [row[1] for row in cursor.fetchall()]
        print(f"  - ReflectionMemory 表字段数: {len(memory_columns)}")
        print(f"  - 包含 source_event_type: {'source_event_type' in memory_columns}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ 迁移失败: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
