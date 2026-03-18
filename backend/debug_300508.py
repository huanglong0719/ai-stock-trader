import asyncio
from app.services.data_provider import data_provider
import json
from datetime import datetime

from app.db.session import SessionLocal
from app.models.stock_models import AIAnalysisReport
from app.services.ai_report_service import ai_report_service

async def debug_weihong():
    symbol = '300508.SZ'
    
    for freq in ['D', 'W', 'M']:
        print(f"\nFetching {freq} K-line for {symbol}...")
        kline = await data_provider.get_kline(symbol, freq=freq)
        print(f"{freq} K-line count: {len(kline) if kline else 0}")
        if kline:
            print(f"Latest 2 bars:")
            for bar in kline[-2:]:
                print(json.dumps(bar, ensure_ascii=False))

    quote = await data_provider.get_realtime_quote(symbol)
    print(f"\nRealtime quote: {json.dumps(quote, ensure_ascii=False)}")

    kline = await data_provider.get_kline(symbol, freq="D", limit=80, include_indicators=False, adj="qfq")
    kline = [x for x in (kline or []) if (x.get("time") or x.get("trade_date") or x.get("date"))]
    kline = sorted(kline, key=lambda x: str(x.get("time") or x.get("trade_date") or x.get("date"))[:10])
    if len(kline) < 35:
        print("\nNot enough daily bars for evaluation")
        return

    pick = kline[-25]
    pick_date_str = str(pick.get("time") or pick.get("trade_date") or pick.get("date"))[:10].replace("/", "-")
    trade_date = datetime.strptime(pick_date_str.replace("-", ""), "%Y%m%d").date()
    report_id = await ai_report_service.save_report(
        analysis_type="realtime_trade_signal_v3",
        ts_code=symbol,
        strategy_name="video_signals_smoke",
        request_payload={"note": "smoke"},
        response_payload={"action": "BUY"},
        trade_date=trade_date,
    )
    print(f"\nCreated report_id={report_id}, trade_date={trade_date.isoformat()}")

    res = await ai_report_service.evaluate_recent_reports(days=2, horizon_days=5, max_reports=200, only_unrated=True)
    print(f"evaluate_recent_reports: {json.dumps(res, ensure_ascii=False)}")

    db = SessionLocal()
    try:
        row = db.query(AIAnalysisReport).filter(AIAnalysisReport.id == report_id).first()
        print(f"evaluation_label: {getattr(row, 'evaluation_label', None)}")
        if row and row.evaluation_json:
            detail = json.loads(row.evaluation_json)
            print(f"video_signals: {json.dumps(detail.get('video_signals'), ensure_ascii=False)}")
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(debug_weihong())
