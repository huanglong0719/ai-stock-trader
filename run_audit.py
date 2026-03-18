
import asyncio
import os
import sys

# 设置项目根目录，确保 app 模块可以被正确导入
root_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(root_dir, 'backend')
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app.services.audit_service import AuditService
from app.db.session import SessionLocal
# 从 AuditService 获取关联的模型，避免直接从 stock_models 导入可能出现的问题
from app.models.stock_models import AuditDetail, AuditReport

async def main():
    audit_service = AuditService()
    db = SessionLocal()
    try:
        print("Running full audit...")
        # 强制运行一个新的每日审计
        report = await audit_service.run_daily_audit(force=True)
        
        print(f"Audit Report ID: {report.id}")
        print(f"Audit Status: {report.status}")
        print(f"Audit Summary: {report.summary}")
        
        # 重新获取 report 详情，确保是从数据库加载的最新的
        details = db.query(AuditDetail).filter(AuditDetail.report_id == report.id).all()
        if details:
            print("\n--- Audit Details ---")
            for d in details:
                print(f"[{d.diff_type}] {d.ts_code or 'ACCOUNT'}: Expected {d.expected_value}, Actual {d.actual_value}, Diff {d.diff_amount}")
                print(f"  Desc: {d.description}")
        else:
            print("\nNo issues found in this audit.")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(main())
