import asyncio
import logging
import json
from datetime import datetime, date, time
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from app.models.stock_models import TradeRecord, Position, Account, AuditReport, AuditDetail, StockIndicator
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

class AuditService:
    """
    审计与对账服务：负责交易记录与持仓、账户资金的一致性核查
    """

    async def run_daily_audit(self, audit_date: Optional[date] = None, force: bool = False):
        """
        执行每日全面审计
        """
        if audit_date is None:
            audit_date = date.today()

        if not force:
            db = SessionLocal()
            try:
                existing = db.query(AuditReport).filter(
                    AuditReport.audit_date == audit_date,
                    AuditReport.status != "REALTIME"
                ).order_by(AuditReport.created_at.desc()).first()
                if existing:
                    logger.info(f"Audit for {audit_date} already exists (status={existing.status}), skipping.")
                    return existing
            finally:
                db.close()

        try:
            from app.services.trading_service import trading_service
            await trading_service.sync_account_assets()
        except Exception as e:
            logger.warning(f"Pre-audit asset sync failed (ignoring): {e}")
            
        db = SessionLocal()
        try:
            logger.info(f"Starting comprehensive audit for {audit_date}")
            
            # 1. 创建审计报告记录
            report = AuditReport(
                audit_date=audit_date,
                status="IN_PROGRESS",
                summary="{}"
            )
            db.add(report)
            db.commit()
            db.refresh(report)
            
            details = []
            summary = {
                "total_stocks_checked": 0,
                "issues_found": 0,
                "total_diff_amount": 0.0
            }
            
            # 2. 获取所有有交易记录或持仓的股票代码
            trade_codes = db.query(TradeRecord.ts_code).distinct().all()
            pos_codes = db.query(Position.ts_code).distinct().all()
            all_codes = sorted(list(set([c[0] for c in trade_codes] + [c[0] for c in pos_codes])))
            
            summary["total_stocks_checked"] = len(all_codes)
            
            # 3. 逐笔核查每个股票
            for ts_code in all_codes:
                stock_issues = await self._audit_stock(db, ts_code, report.id)
                if stock_issues:
                    details.extend(stock_issues)
                    summary["issues_found"] += len(stock_issues)
                    summary["total_diff_amount"] += sum(abs(d.diff_amount) for d in stock_issues)
            
            # 4. 核查账户总额一致性
            account_issues = await self._audit_account_balance(db, report.id)
            if account_issues:
                details.extend(account_issues)
                summary["issues_found"] += len(account_issues)
            
            # 5. 更新报告状态
            report.status = "SUCCESS" if summary["issues_found"] == 0 else "WARNING"
            if any(d.diff_type in ['QTY_MISMATCH', 'BALANCE_MISMATCH'] for d in details):
                report.status = "ERROR"
                
            report.summary = json.dumps(summary)
            db.add_all(details)
            db.commit()
            
            logger.info(f"Audit completed: {report.status}, Issues: {summary['issues_found']}")
            return report
            
        except Exception as e:
            logger.error(f"Error during audit: {e}", exc_info=True)
            if 'report' in locals():
                report.status = "ERROR"
                report.summary = json.dumps({"error": str(e)})
                db.commit()
        finally:
            db.close()

    async def run_realtime_audit(self, ts_code: str):
        """
        重大交易后实时触发针对特定股票的核查
        """
        db = SessionLocal()
        try:
            logger.info(f"Triggering real-time audit for {ts_code}")
            
            # 创建一个临时的实时审计报告
            report = AuditReport(
                audit_date=date.today(),
                status="REALTIME",
                summary=json.dumps({"ts_code": ts_code})
            )
            db.add(report)
            db.commit()
            db.refresh(report)
            
            issues = await self._audit_stock(db, ts_code, report.id)
            if issues:
                db.add_all(issues)
                report.status = "ERROR"
                logger.warning(f"Real-time audit found issues for {ts_code}: {len(issues)} issues")
            else:
                report.status = "SUCCESS"
                
            db.commit()
            return report
        except Exception as e:
            logger.error(f"Error during real-time audit for {ts_code}: {e}")
        finally:
            db.close()

    async def _audit_stock(self, db: Session, ts_code: str, report_id: int) -> List[AuditDetail]:
        """
        对单个股票进行交易回放审计
        """
        issues = []
        
        # 1. 获取所有交易记录 (按时间升序)
        trades = db.query(TradeRecord).filter(TradeRecord.ts_code == ts_code).order_by(TradeRecord.trade_time.asc()).all()
        
        # 2. 回放交易
        expected_vol = 0
        total_cost = 0.0
        expected_avg_price = 0.0
        total_fee = 0.0
        
        for t in trades:
            total_fee += (t.fee or 0.0)
            if t.trade_type == 'BUY':
                expected_vol += t.vol
                total_cost += (t.vol * t.price) + (t.fee or 0.0)
            elif t.trade_type == 'SELL':
                # 简单移动平均成本法
                if expected_vol > 0:
                    unit_cost = total_cost / expected_vol
                    # 卖出时不改变单位成本，只减少总量和总额
                    expected_vol -= t.vol
                    total_cost -= (t.vol * unit_cost)
                    # 卖出费用不计入持仓成本，直接作为交易损耗
                else:
                    # 异常：无持仓卖出
                    issues.append(AuditDetail(
                        report_id=report_id,
                        ts_code=ts_code,
                        diff_type="QTY_MISMATCH",
                        expected_value=0,
                        actual_value=-t.vol,
                        diff_amount=t.vol * t.price,
                        description=f"异常：在无持仓情况下执行了卖出操作 (TradeID: {t.id})",
                        adjustment_suggestion="核查交易记录是否完整，或是否存在手工录入错误"
                    ))
                    expected_vol -= t.vol
            
            if expected_vol > 0:
                expected_avg_price = total_cost / expected_vol
            else:
                expected_avg_price = 0.0
                total_cost = 0.0 # 仓位清空，成本归零

        # 3. 检查公司行为 (送配股/除权除息)
        # 通过 adj_factor 变化检测
        # 注意：这里简化处理，实际需要更复杂的复权回溯逻辑
        
        # 4. 与当前持仓比对
        pos = db.query(Position).filter(Position.ts_code == ts_code).first()
        actual_vol = pos.vol if pos else 0
        actual_avg_price = pos.avg_price if pos else 0.0
        
        # 数量比对
        if actual_vol != expected_vol:
            issues.append(AuditDetail(
                report_id=report_id,
                ts_code=ts_code,
                diff_type="QTY_MISMATCH",
                expected_value=float(expected_vol),
                actual_value=float(actual_vol),
                diff_amount=float(abs(actual_vol - expected_vol) * (pos.current_price if pos else 0)),
                description=f"持仓数量不一致：预期 {expected_vol}, 实际 {actual_vol}",
                adjustment_suggestion="同步交易记录并重新计算持仓数量"
            ))
            
        # 成本比对 (允许 0.01 的精度误差)
        if abs(actual_avg_price - expected_avg_price) > 0.01 and actual_vol > 0:
            issues.append(AuditDetail(
                report_id=report_id,
                ts_code=ts_code,
                diff_type="COST_MISMATCH",
                expected_value=float(expected_avg_price),
                actual_value=float(actual_avg_price),
                diff_amount=float(abs(actual_avg_price - expected_avg_price) * actual_vol),
                description=f"持仓成本价不一致：预期 {expected_avg_price:.4f}, 实际 {actual_avg_price:.4f}",
                adjustment_suggestion="按历史成交记录重新加权计算成本价"
            ))
            
        return issues

    async def _audit_account_balance(self, db: Session, report_id: int) -> List[AuditDetail]:
        """
        核查账户总额一致性
        """
        issues = []
        account = db.query(Account).first()
        if not account:
            return []
            
        # 1. 计算所有持仓的当前市值总和
        positions = db.query(Position).filter(Position.vol > 0).all()
        calculated_market_value = sum((p.vol or 0) * float(p.current_price or 0.0) for p in positions)
        
        # 2. 验证 Account.market_value
        if abs(float(account.market_value or 0.0) - calculated_market_value) > 1.0:
            issues.append(AuditDetail(
                report_id=report_id,
                ts_code="SYSTEM",
                diff_type="BALANCE_MISMATCH",
                expected_value=float(calculated_market_value),
                actual_value=float(account.market_value or 0.0),
                diff_amount=float(abs(float(account.market_value or 0.0) - calculated_market_value)),
                description=f"账户市值不一致：持仓汇总市值 {calculated_market_value:.2f}, 账户记录市值 {float(account.market_value or 0.0):.2f}",
                adjustment_suggestion="执行 sync_account_assets 同步最新市值"
            ))
            
        # 3. 验证总资产公式：Total = Cash + Frozen + MarketValue
        expected_total = float(account.available_cash or 0.0) + float(account.frozen_cash or 0.0) + float(account.market_value or 0.0)
        if abs(float(account.total_assets or 0.0) - expected_total) > 1.0:
            issues.append(AuditDetail(
                report_id=report_id,
                ts_code="SYSTEM",
                diff_type="BALANCE_MISMATCH",
                expected_value=float(expected_total),
                actual_value=float(account.total_assets or 0.0),
                diff_amount=float(abs(float(account.total_assets or 0.0) - expected_total)),
                description=f"总资产平衡公式失效：现金+市值={expected_total:.2f}, 账户总资产={float(account.total_assets or 0.0):.2f}",
                adjustment_suggestion="重新计算并更新账户总资产"
            ))
            
        return issues

audit_service = AuditService()
