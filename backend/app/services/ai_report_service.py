import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, Optional, List, Tuple

from sqlalchemy import and_

from app.db.session import SessionLocal
from app.models.stock_models import AIAnalysisReport
from app.services.data_provider import data_provider
from app.services.market.market_utils import get_limit_prices

logger = logging.getLogger(__name__)


def _safe_json_dumps(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"_repr_": str(data)}, ensure_ascii=False)


def _truncate_text(text: Optional[str], max_len: int) -> Optional[str]:
    if text is None:
        return None
    if len(text) <= max_len:
        return text
    return text[:max_len]


def _shrink_payload(obj: Any, max_str_len: int = 12000, max_list_len: int = 200, max_depth: int = 4) -> Any:
    if max_depth <= 0:
        return obj

    if isinstance(obj, str):
        return _truncate_text(obj, max_str_len)

    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj

    if isinstance(obj, list):
        trimmed = obj[:max_list_len]
        return [_shrink_payload(x, max_str_len=max_str_len, max_list_len=max_list_len, max_depth=max_depth - 1) for x in trimmed]

    if isinstance(obj, tuple):
        trimmed = list(obj)[:max_list_len]
        return [_shrink_payload(x, max_str_len=max_str_len, max_list_len=max_list_len, max_depth=max_depth - 1) for x in trimmed]

    if isinstance(obj, dict):
        out: Dict[str, Any] = {}
        for k, v in list(obj.items())[: max_list_len]:
            key = str(k)
            out[key] = _shrink_payload(v, max_str_len=max_str_len, max_list_len=max_list_len, max_depth=max_depth - 1)
        return out

    return _truncate_text(str(obj), max_str_len)


def _normalize_trade_date_str(trade_date_str: Optional[str]) -> Optional[str]:
    if not trade_date_str:
        return None
    s = str(trade_date_str).replace("-", "")
    if len(s) != 8:
        return None
    return s


def _parse_date_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


@dataclass(frozen=True)
class EvaluationResult:
    label: str
    detail: Dict[str, Any]


class AIReportService:
    async def save_report(
        self,
        analysis_type: str,
        ts_code: Optional[str] = None,
        strategy_name: Optional[str] = None,
        request_payload: Optional[Dict[str, Any]] = None,
        response_payload: Optional[Any] = None,
        trade_date: Optional[date] = None,
        max_json_len: int = 60000,
    ) -> Optional[int]:
        try:
            if not analysis_type:
                return None

            if trade_date is None:
                trade_date_str = await data_provider.get_last_trade_date(include_today=True)
                trade_date_str = _normalize_trade_date_str(trade_date_str)
                trade_date = _parse_date_yyyymmdd(trade_date_str) if trade_date_str else datetime.now().date()

            request_payload_small = _shrink_payload(request_payload) if request_payload else None
            response_payload_small = _shrink_payload(response_payload) if response_payload is not None else None

            request_json = _safe_json_dumps(request_payload_small) if request_payload_small else None
            response_json = _safe_json_dumps(response_payload_small) if response_payload_small is not None else None

            if request_json and len(request_json) > max_json_len:
                request_json = _safe_json_dumps({"_truncated": True, "size": len(request_json)})
            if response_json and len(response_json) > max_json_len:
                response_json = _safe_json_dumps({"_truncated": True, "size": len(response_json)})

            def _save() -> int:
                db = SessionLocal()
                try:
                    row = AIAnalysisReport(
                        trade_date=trade_date,
                        ts_code=ts_code,
                        analysis_type=analysis_type,
                        strategy_name=strategy_name,
                        request_json=request_json,
                        response_json=response_json,
                        created_at=datetime.now(),
                    )
                    db.add(row)
                    db.commit()
                    db.refresh(row)
                    return int(row.id or 0)
                finally:
                    db.close()

            return await asyncio.to_thread(_save)
        except Exception as e:
            logger.error(f"AIReport save_report failed: {e}", exc_info=True)
            return None

    async def prune_reports(self, keep_days: int = 30) -> Dict[str, Any]:
        cutoff = datetime.now() - timedelta(days=keep_days)

        def _prune() -> int:
            db = SessionLocal()
            try:
                q = db.query(AIAnalysisReport).filter(AIAnalysisReport.created_at < cutoff)
                deleted = q.delete(synchronize_session=False)
                db.commit()
                return int(deleted or 0)
            finally:
                db.close()

        try:
            deleted = await asyncio.to_thread(_prune)
            return {"deleted": deleted, "cutoff": cutoff.isoformat()}
        except Exception as e:
            logger.error(f"AIReport prune_reports failed: {e}", exc_info=True)
            return {"deleted": 0, "cutoff": cutoff.isoformat(), "error": str(e)}

    async def list_reports(
        self,
        days: int = 30,
        ts_code: Optional[str] = None,
        analysis_type: Optional[str] = None,
        evaluation_label: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        since_dt = datetime.now() - timedelta(days=days)

        def _query() -> List[AIAnalysisReport]:
            db = SessionLocal()
            try:
                q = db.query(AIAnalysisReport).filter(AIAnalysisReport.created_at >= since_dt)
                if ts_code:
                    q = q.filter(AIAnalysisReport.ts_code == ts_code)
                if analysis_type:
                    q = q.filter(AIAnalysisReport.analysis_type == analysis_type)
                if evaluation_label:
                    q = q.filter(AIAnalysisReport.evaluation_label == evaluation_label)
                q = q.order_by(AIAnalysisReport.created_at.desc()).limit(max(1, min(limit, 500)))
                return list(q.all())
            finally:
                db.close()

        rows = await asyncio.to_thread(_query)
        result: List[Dict[str, Any]] = []
        for r in rows:
            result.append(
                {
                    "id": r.id,
                    "trade_date": r.trade_date.isoformat() if r.trade_date else None,
                    "ts_code": r.ts_code,
                    "analysis_type": r.analysis_type,
                    "strategy_name": r.strategy_name,
                    "request_json": r.request_json,
                    "response_json": r.response_json,
                    "evaluation_label": r.evaluation_label,
                    "evaluation_json": r.evaluation_json,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
            )
        return result

    async def evaluate_recent_reports(
        self,
        days: int = 30,
        horizon_days: int = 5,
        max_reports: int = 200,
        only_unrated: bool = True,
    ) -> Dict[str, Any]:
        since_dt = datetime.now() - timedelta(days=days)

        def _fetch() -> List[Tuple[int, str, Optional[str], Optional[str], date, Optional[str]]]:
            db = SessionLocal()
            try:
                q = db.query(
                    AIAnalysisReport.id,
                    AIAnalysisReport.analysis_type,
                    AIAnalysisReport.ts_code,
                    AIAnalysisReport.strategy_name,
                    AIAnalysisReport.trade_date,
                    AIAnalysisReport.response_json,
                ).filter(AIAnalysisReport.created_at >= since_dt)

                q = q.filter(AIAnalysisReport.analysis_type.in_(["realtime_trade_signal_v3", "selling_opportunity"]))
                q = q.filter(AIAnalysisReport.ts_code != None)
                if only_unrated:
                    q = q.filter(AIAnalysisReport.evaluation_label == None)
                q = q.filter(AIAnalysisReport.trade_date != None)
                q = q.order_by(AIAnalysisReport.created_at.asc()).limit(max(1, min(max_reports, 500)))
                rows_raw = q.all()
                rows: List[Tuple[int, str, Optional[str], Optional[str], date, Optional[str]]] = []
                for r in rows_raw:
                    report_id = int(r[0] or 0)
                    analysis_type = str(r[1] or "")
                    trade_date_value = r[4]
                    if not report_id or not analysis_type or not trade_date_value:
                        continue
                    rows.append((report_id, analysis_type, r[2], r[3], trade_date_value, r[5]))
                return rows
            finally:
                db.close()

        rows = await asyncio.to_thread(_fetch)
        if not rows:
            return {"evaluated": 0, "labels": {}, "since": since_dt.isoformat()}

        semaphore = asyncio.Semaphore(10)
        labels_count: Dict[str, int] = {}

        async def _evaluate_one(row: Tuple[int, str, Optional[str], Optional[str], date, Optional[str]]) -> Optional[EvaluationResult]:
            report_id, analysis_type, ts_code, _strategy_name, trade_date_value, response_json = row
            if not ts_code or not trade_date_value:
                return None

            try:
                if not response_json:
                    return EvaluationResult(label="NO_RESPONSE", detail={"analysis_type": analysis_type})

                try:
                    response = json.loads(response_json)
                except Exception:
                    response = {"raw": _truncate_text(response_json, 2000)}

                action = str(response.get("action") or "").upper()
                if not action:
                    return EvaluationResult(label="NO_ACTION", detail={"analysis_type": analysis_type})

                end_date_value = trade_date_value + timedelta(days=horizon_days * 3 + 10)
                start_date_value = trade_date_value - timedelta(days=80)
                start_date_str = start_date_value.strftime("%Y%m%d")
                end_date_str = end_date_value.strftime("%Y%m%d")
                kline = await data_provider.get_kline(
                    ts_code,
                    freq="D",
                    start_date=start_date_str,
                    end_date=end_date_str,
                    local_only=False,
                    limit=None,
                    include_indicators=False,
                    adj="qfq",
                )
                if not kline or len(kline) < 2:
                    return EvaluationResult(label="NO_KLINE", detail={"analysis_type": analysis_type, "ts_code": ts_code})

                def _parse_kline_date(v: Any) -> Optional[date]:
                    if not v:
                        return None
                    s = str(v).strip()
                    if len(s) >= 10 and s[4] in ["-", "/"] and s[7] in ["-", "/"]:
                        s = s[:10].replace("/", "-")
                        try:
                            return datetime.strptime(s, "%Y-%m-%d").date()
                        except Exception:
                            return None
                    s2 = s.replace("-", "").replace("/", "")
                    if len(s2) == 8 and s2.isdigit():
                        try:
                            return datetime.strptime(s2, "%Y%m%d").date()
                        except Exception:
                            return None
                    return None

                def _to_float(v: Any) -> float:
                    try:
                        if v is None:
                            return 0.0
                        return float(v)
                    except Exception:
                        return 0.0

                candles: List[Dict[str, Any]] = []
                for item in kline:
                    d = _parse_kline_date(item.get("time") or item.get("trade_date") or item.get("date"))
                    if not d:
                        continue
                    candles.append(
                        {
                            "date": d,
                            "open": _to_float(item.get("open")),
                            "high": _to_float(item.get("high")),
                            "low": _to_float(item.get("low")),
                            "close": _to_float(item.get("close")),
                            "volume": _to_float(item.get("volume") if item.get("volume") is not None else item.get("vol")),
                        }
                    )
                candles.sort(key=lambda x: x["date"])

                base_idx: Optional[int] = None
                for i, bar in enumerate(candles):
                    d = bar["date"]
                    c = bar["close"]
                    if d >= trade_date_value and c > 0:
                        base_idx = i
                        break
                if base_idx is None:
                    for i in range(len(candles) - 1, -1, -1):
                        d = candles[i]["date"]
                        c = candles[i]["close"]
                        if d <= trade_date_value and c > 0:
                            base_idx = i
                            break
                if base_idx is None:
                    return EvaluationResult(label="BAD_BASE", detail={"analysis_type": analysis_type, "ts_code": ts_code})

                base_date = candles[base_idx]["date"]
                base_close = candles[base_idx]["close"]
                if not base_close or base_close <= 0:
                    return EvaluationResult(label="BAD_BASE", detail={"analysis_type": analysis_type, "ts_code": ts_code})

                lookback_start = max(0, base_idx - 19)
                lookback = candles[lookback_start : base_idx + 1]

                limit_up_in_20d = False
                limit_up_dates: List[str] = []
                for i in range(lookback_start, base_idx + 1):
                    if i <= 0:
                        continue
                    pre_close = candles[i - 1]["close"]
                    if pre_close <= 0:
                        continue
                    limit_up_price, _ = get_limit_prices(ts_code, pre_close)
                    if limit_up_price <= 0:
                        continue
                    today_high = candles[i]["high"]
                    today_close = candles[i]["close"]
                    if today_high >= (limit_up_price - 0.01) and today_close >= (limit_up_price - 0.01):
                        limit_up_in_20d = True
                        limit_up_dates.append(candles[i]["date"].isoformat())

                bull_streak_end = 0
                for i in range(base_idx, -1, -1):
                    if candles[i]["close"] > candles[i]["open"] > 0:
                        bull_streak_end += 1
                        continue
                    break

                max_bull_streak_20d = 0
                cur = 0
                for bar in lookback:
                    if bar["close"] > bar["open"] > 0:
                        cur += 1
                        if cur > max_bull_streak_20d:
                            max_bull_streak_20d = cur
                    else:
                        cur = 0

                gap_up_strict_in_20d = False
                gap_up_dates: List[str] = []
                for i in range(lookback_start, base_idx + 1):
                    if i <= 0:
                        continue
                    prev_high = candles[i - 1]["high"]
                    today_low = candles[i]["low"]
                    if prev_high > 0 and today_low > prev_high:
                        gap_up_strict_in_20d = True
                        gap_up_dates.append(candles[i]["date"].isoformat())

                volume_surge_in_20d = False
                volume_surge_days = 0
                max_volume_ratio: float = 0.0
                latest_volume_ratio: float = 0.0
                for i in range(lookback_start, base_idx + 1):
                    if i < 5:
                        continue
                    vols = [candles[j]["volume"] for j in range(i - 5, i) if candles[j]["volume"] > 0]
                    if not vols:
                        continue
                    avg_vol = sum(vols) / len(vols)
                    if avg_vol <= 0:
                        continue
                    ratio = candles[i]["volume"] / avg_vol if candles[i]["volume"] > 0 else 0.0
                    if i == base_idx:
                        latest_volume_ratio = round(ratio, 3)
                    if ratio > max_volume_ratio:
                        max_volume_ratio = ratio
                    if ratio >= 2.0:
                        volume_surge_in_20d = True
                        volume_surge_days += 1

                video_4signals_all = bool(
                    limit_up_in_20d and max_bull_streak_20d >= 4 and gap_up_strict_in_20d and volume_surge_in_20d
                )

                future: List[float] = []
                for bar in candles[base_idx + 1 :]:
                    c = bar["close"]
                    if c > 0:
                        future.append(float(c))
                    if len(future) >= horizon_days:
                        break

                if not future:
                    return EvaluationResult(
                        label="NO_FUTURE",
                        detail={
                            "analysis_type": analysis_type,
                            "ts_code": ts_code,
                            "base_close": base_close,
                            "base_date": base_date.isoformat(),
                            "video_signals": {
                                "lookback_days": 20,
                                "limit_up_in_20d": limit_up_in_20d,
                                "limit_up_dates": limit_up_dates[:10],
                                "bull_streak_end": bull_streak_end,
                                "max_bull_streak_20d": max_bull_streak_20d,
                                "gap_up_strict_in_20d": gap_up_strict_in_20d,
                                "gap_up_dates": gap_up_dates[:10],
                                "volume_surge_in_20d": volume_surge_in_20d,
                                "volume_surge_days": volume_surge_days,
                                "max_volume_ratio_5d": round(max_volume_ratio, 3) if max_volume_ratio else 0.0,
                                "latest_volume_ratio_5d": latest_volume_ratio,
                                "all_4_signals": video_4signals_all,
                            },
                        },
                    )

                returns = [round((c - base_close) / base_close * 100, 2) for c in future]
                max_ret = max(returns)
                min_ret = min(returns)

                up_th = 3.0
                down_th = -3.0

                if action == "BUY":
                    if max_ret >= up_th:
                        label = "CORRECT"
                    elif min_ret <= down_th:
                        label = "WRONG"
                    else:
                        label = "NEUTRAL"
                elif action in ["WAIT", "HOLD"]:
                    if max_ret >= up_th:
                        label = "MISS"
                    elif min_ret <= down_th:
                        label = "CORRECT"
                    else:
                        label = "NEUTRAL"
                elif action in ["SELL", "REDUCE", "CANCEL"]:
                    if min_ret <= down_th:
                        label = "CORRECT"
                    elif max_ret >= up_th:
                        label = "WRONG"
                    else:
                        label = "NEUTRAL"
                else:
                    label = "UNKNOWN_ACTION"

                return EvaluationResult(
                    label=label,
                    detail={
                        "analysis_type": analysis_type,
                        "action": action,
                        "horizon_days": horizon_days,
                        "base_close": base_close,
                        "base_date": base_date.isoformat(),
                        "max_return_pct": max_ret,
                        "min_return_pct": min_ret,
                        "returns": returns[: min(len(returns), 10)],
                        "video_signals": {
                            "lookback_days": 20,
                            "limit_up_in_20d": limit_up_in_20d,
                            "limit_up_dates": limit_up_dates[:10],
                            "bull_streak_end": bull_streak_end,
                            "max_bull_streak_20d": max_bull_streak_20d,
                            "gap_up_strict_in_20d": gap_up_strict_in_20d,
                            "gap_up_dates": gap_up_dates[:10],
                            "volume_surge_in_20d": volume_surge_in_20d,
                            "volume_surge_days": volume_surge_days,
                            "max_volume_ratio_5d": round(max_volume_ratio, 3) if max_volume_ratio else 0.0,
                            "latest_volume_ratio_5d": latest_volume_ratio,
                            "all_4_signals": video_4signals_all,
                        },
                    },
                )
            except Exception as e:
                return EvaluationResult(label="EVAL_ERROR", detail={"error": str(e), "analysis_type": analysis_type})

        async def _run_and_save(row: Tuple[int, str, Optional[str], Optional[str], date, Optional[str]]) -> Optional[str]:
            async with semaphore:
                report_id = row[0]
                res = await _evaluate_one(row)
                if not res:
                    return None

                def _update():
                    db = SessionLocal()
                    try:
                        db.query(AIAnalysisReport).filter(AIAnalysisReport.id == report_id).update(
                            {
                                AIAnalysisReport.evaluation_label: res.label,
                                AIAnalysisReport.evaluation_json: _safe_json_dumps(res.detail),
                                AIAnalysisReport.updated_at: datetime.now(),
                            }
                        )
                        db.commit()
                    finally:
                        db.close()

                await asyncio.to_thread(_update)
                return res.label

        labels = await asyncio.gather(*[_run_and_save(r) for r in rows])
        for lab in labels:
            if not lab:
                continue
            labels_count[lab] = labels_count.get(lab, 0) + 1

        return {"evaluated": len([x for x in labels if x]), "labels": labels_count, "since": since_dt.isoformat()}

    async def build_self_review_summary(self, days: int = 30) -> Dict[str, Any]:
        since_dt = datetime.now() - timedelta(days=days)

        def _query() -> List[Tuple[int, str, Optional[str], Optional[str], Optional[date], str, Optional[str]]]:
            db = SessionLocal()
            try:
                q = db.query(
                    AIAnalysisReport.id,
                    AIAnalysisReport.analysis_type,
                    AIAnalysisReport.ts_code,
                    AIAnalysisReport.strategy_name,
                    AIAnalysisReport.trade_date,
                    AIAnalysisReport.evaluation_label,
                    AIAnalysisReport.evaluation_json,
                ).filter(AIAnalysisReport.created_at >= since_dt)
                q = q.filter(AIAnalysisReport.evaluation_label != None)
                q = q.filter(AIAnalysisReport.analysis_type.in_(["realtime_trade_signal_v3", "selling_opportunity"]))
                rows_raw = q.all()
                rows: List[Tuple[int, str, Optional[str], Optional[str], Optional[date], str, Optional[str]]] = []
                for r in rows_raw:
                    report_id = int(r[0] or 0)
                    analysis_type = str(r[1] or "")
                    label = str(r[5] or "")
                    if not report_id or not analysis_type or not label:
                        continue
                    rows.append((report_id, analysis_type, r[2], r[3], r[4], label, r[6]))
                return rows
            finally:
                db.close()

        rows = await asyncio.to_thread(_query)
        by_type: Dict[str, Dict[str, int]] = {}
        by_strategy: Dict[str, Dict[str, int]] = {}
        examples: Dict[str, List[Dict[str, Any]]] = {}

        def _push_example(label: str, item: Dict[str, Any], max_items: int = 5):
            lst = examples.setdefault(label, [])
            if len(lst) >= max_items:
                return
            lst.append(item)

        for report_id, analysis_type, ts_code, strategy_name, trade_date_value, label, evaluation_json in rows:
            if not label:
                continue
            d = by_type.setdefault(str(analysis_type), {})
            d[label] = d.get(label, 0) + 1

            s_name = (strategy_name or "").strip() or "UNKNOWN"
            sd = by_strategy.setdefault(s_name, {})
            sd[label] = sd.get(label, 0) + 1

            if label in ["WRONG", "MISS", "EVAL_ERROR"]:
                eval_detail = {}
                if evaluation_json:
                    try:
                        eval_detail = json.loads(evaluation_json)
                    except Exception:
                        eval_detail = {"raw": _truncate_text(evaluation_json, 1000)}
                _push_example(
                    label,
                    {
                        "id": report_id,
                        "analysis_type": analysis_type,
                        "ts_code": ts_code,
                        "strategy_name": strategy_name,
                        "trade_date": trade_date_value.isoformat() if trade_date_value else None,
                        "evaluation": eval_detail,
                    },
                )

        total = sum(sum(v.values()) for v in by_type.values())
        return {
            "days": days,
            "total_evaluated": total,
            "by_type": by_type,
            "by_strategy": by_strategy,
            "examples": examples,
            "generated_at": datetime.now().isoformat(),
        }

    async def save_self_review_summary(self, days: int = 30) -> Optional[int]:
        summary = await self.build_self_review_summary(days=days)
        return await self.save_report(
            analysis_type="self_review_summary",
            ts_code=None,
            strategy_name=None,
            request_payload={"days": days},
            response_payload=summary,
            trade_date=datetime.now().date(),
        )

    async def get_latest_self_review_summary(self, days: int = 30) -> Optional[Dict[str, Any]]:
        since_dt = datetime.now() - timedelta(days=days)

        def _query() -> Optional[AIAnalysisReport]:
            db = SessionLocal()
            try:
                q = db.query(AIAnalysisReport).filter(
                    and_(
                        AIAnalysisReport.analysis_type == "self_review_summary",
                        AIAnalysisReport.created_at >= since_dt,
                    )
                )
                q = q.order_by(AIAnalysisReport.created_at.desc()).limit(1)
                return q.first()
            finally:
                db.close()

        row = await asyncio.to_thread(_query)
        if not row or not row.response_json:
            return None
        try:
            return json.loads(row.response_json)
        except Exception:
            return {"raw": row.response_json, "created_at": row.created_at.isoformat() if row.created_at else None}


ai_report_service = AIReportService()
