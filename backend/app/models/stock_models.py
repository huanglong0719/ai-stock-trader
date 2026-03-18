from sqlalchemy import Column, String, Float, DateTime, Integer, Date, Index, Boolean, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.db.session import Base
import datetime
from typing import Optional, Any

class TradingPlan(Base):
    __tablename__ = "trading_plans"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date, index=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    
    # 计划内容
    strategy_name: Mapped[str] = mapped_column(String)
    buy_price_limit: Mapped[Optional[float]] = mapped_column(Float)
    stop_loss_price: Mapped[Optional[float]] = mapped_column(Float)
    take_profit_price: Mapped[Optional[float]] = mapped_column(Float)
    position_pct: Mapped[Optional[float]] = mapped_column(Float)
    reason: Mapped[Optional[str]] = mapped_column(String)
    
    # 执行结果 (盘后更新)
    executed: Mapped[bool] = mapped_column(Boolean, default=False)
    entry_price: Mapped[Optional[float]] = mapped_column(Float)
    exit_price: Mapped[Optional[float]] = mapped_column(Float)
    pnl_pct: Mapped[Optional[float]] = mapped_column(Float)
    real_pnl: Mapped[Optional[float]] = mapped_column(Float)
    real_pnl_pct: Mapped[Optional[float]] = mapped_column(Float)
    close_reason: Mapped[Optional[str]] = mapped_column(String)
    market_snapshot_json: Mapped[Optional[str]] = mapped_column(Text)
    
    # 复盘总结
    review_content: Mapped[Optional[str]] = mapped_column(String)
    score: Mapped[Optional[float]] = mapped_column(Float)
    
    # 跟踪统计 (用于 AI 自我评估)
    track_status: Mapped[Optional[str]] = mapped_column(String, default="NONE")
    ai_decision: Mapped[Optional[str]] = mapped_column(String)
    decision_price: Mapped[Optional[float]] = mapped_column(Float)
    track_days: Mapped[int] = mapped_column(Integer, default=0)
    track_data: Mapped[Optional[str]] = mapped_column(Text)
    ai_evaluation: Mapped[Optional[str]] = mapped_column(String)
    
    # AI 挂单指令
    order_type: Mapped[Optional[str]] = mapped_column(String, default="MARKET")
    limit_price: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    
    source: Mapped[Optional[str]] = mapped_column(String, default="system")

    # 资金占用
    frozen_amount: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    frozen_vol: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, onupdate=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class PolicyConfig(Base):
    """
    策略参数配置 (可进化)
    """
    __tablename__ = "policy_configs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, index=True) # 策略名称
    market_temperature_bucket = Column(String) # 温度分桶 (LOW, MID, HIGH, ALL)
    
    # 参数集合 (JSON)
    # e.g. {"max_position_pct": 0.2, "stop_loss_pct": 0.05, "take_profit_pct": 0.10, "min_score": 75}
    parameters = Column(Text) 
    
    version = Column(Integer) # 版本号
    status = Column(String)   # ACTIVE (正式生效), SHADOW (影子模式), ARCHIVED (归档)
    
    # 进化元数据
    parent_id = Column(Integer, nullable=True) # 上一版本ID
    evolution_reason = Column(String) # 进化原因 (e.g. "Win rate > 60%, increasing position")
    
    # 表现统计 (用于影子模式评估或回滚判定)
    start_date = Column(Date) # 生效/开始日期
    end_date = Column(Date, nullable=True) # 结束/归档日期
    performance_metrics = Column(Text) # JSON: {"win_rate": 0.65, "drawdown": 0.02}
    
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)
    ai_reason = Column(String)                # AI 决策理由 (新添加)

class AIAnalysisReport(Base):
    __tablename__ = "ai_analysis_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    trade_date = Column(Date, index=True)
    ts_code = Column(String, index=True, nullable=True)
    analysis_type = Column(String, index=True)
    strategy_name = Column(String, index=True, nullable=True)

    request_json = Column(Text, nullable=True)
    response_json = Column(Text, nullable=True)

    evaluation_label = Column(String, index=True, nullable=True)
    evaluation_json = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.now, index=True)
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)

    __table_args__ = (
        Index('idx_ai_report_ts_type_date', 'ts_code', 'analysis_type', 'trade_date'),
        Index('idx_ai_report_type_date', 'analysis_type', 'trade_date'),
    )

class OutcomeEvent(Base):
    __tablename__ = "outcome_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String, index=True)
    event_type = Column(String, index=True)
    event_date = Column(Date, index=True)

    payload_json = Column(Text, nullable=True)

    evaluation_label = Column(String, index=True, nullable=True)
    evaluation_json = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.now, index=True)
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)

    __table_args__ = (
        Index('idx_outcome_event_unique', 'ts_code', 'event_type', 'event_date', unique=True),
        Index('idx_outcome_event_type_date', 'event_type', 'event_date'),
    )

class PatternCase(Base):
    """
    优质交易模式案例 (用于 AI 学习)
    记录成功的交易案例，包含当时的市场环境和 K 线形态
    """
    __tablename__ = "pattern_cases"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String, index=True)
    trade_date = Column(Date, index=True) # 成功交易的日期
    
    # 模式分类
    pattern_type = Column(String) # e.g. "首板挖掘", "弱转强"
    
    # 市场快照 (存储为 JSON 字符串)
    market_environment = Column(Text) # 当时的市场情绪、板块热度等
    kline_pattern = Column(Text)      # 交易前 N 天的 K 线形态数据
    
    # 表现
    profit_pct = Column(Float) # 实际获利比例
    hold_days = Column(Integer) # 持仓天数
    is_successful = Column(Boolean, default=True) # 是否成功案例（True=盈利，False=亏损）
    
    created_at = Column(DateTime, default=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class PatternLibrary(Base):
    """
    通用交易模式库 (存储挖掘出的高胜率模式)
    """
    __tablename__ = "pattern_library"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)   # 模式名称
    features_json = Column(Text)         # 特征描述 (JSON)
    
    success_rate = Column(Float)         # 胜率
    sample_count = Column(Integer)       # 样本数
    
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)

class StrategyStats(Base):
    """
    策略表现统计 (用于 AI 调整策略权重)
    """
    __tablename__ = "strategy_stats"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    strategy_name = Column(String, unique=True)
    
    total_trades = Column(Integer, default=0)
    win_trades = Column(Integer, default=0)
    total_pnl_pct = Column(Float, default=0.0)
    
    win_rate = Column(Float, default=0.0) # 胜率
    avg_pnl_pct = Column(Float, default=0.0) # 平均盈亏
    max_drawdown = Column(Float, default=0.0) # 最大回撤
    
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

class MarketSentiment(Base):
    __tablename__ = "market_sentiments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date, index=True)
    
    up_count: Mapped[int] = mapped_column(Integer)
    down_count: Mapped[int] = mapped_column(Integer)
    limit_up_count: Mapped[int] = mapped_column(Integer)
    limit_down_count: Mapped[int] = mapped_column(Integer)
    total_volume: Mapped[float] = mapped_column(Float)
    
    market_temperature: Mapped[float] = mapped_column(Float)
    highest_plate: Mapped[int] = mapped_column(Integer)
    main_theme: Mapped[str] = mapped_column(String)
    
    summary: Mapped[str] = mapped_column(String)
    
    ladder_json: Mapped[Optional[str]] = mapped_column(Text)
    turnover_top_json: Mapped[Optional[str]] = mapped_column(Text)
    ladder_opportunities_json: Mapped[Optional[str]] = mapped_column(Text)

    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class MarketCloseCounts(Base):
    __tablename__ = "market_close_counts"

    date = Column(Date, primary_key=True)
    up = Column(Integer)
    down = Column(Integer)
    flat = Column(Integer)
    limit_up = Column(Integer)
    limit_down = Column(Integer)
    amount = Column(Float) # 总成交额 (亿元)
    source = Column(String)
    saved_at = Column(DateTime, default=datetime.datetime.now)

class Stock(Base):
    __tablename__ = "stocks"

    ts_code = Column(String, primary_key=True)
    symbol = Column(String, index=True)
    name = Column(String)
    area = Column(String)
    industry = Column(String, index=True)
    list_date = Column(String)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

class DailyBar(Base):
    __tablename__ = "daily_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    trade_date: Mapped[datetime.date] = mapped_column(Date, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    pre_close: Mapped[float] = mapped_column(Float)
    change: Mapped[float] = mapped_column(Float)
    pct_chg: Mapped[float] = mapped_column(Float)
    vol: Mapped[float] = mapped_column(Float)
    amount: Mapped[float] = mapped_column(Float)
    adj_factor: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_ts_code_date', 'ts_code', 'trade_date', unique=True),
        Index('idx_daily_pct_chg', 'pct_chg'), # 用于快速筛选大涨股票
    )

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class IndustryData(Base):
    __tablename__ = "industry_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    industry = Column(String, index=True)
    trade_date = Column(Date, index=True)
    avg_price = Column(Float)
    avg_pct_chg = Column(Float)
    total_vol = Column(Float)
    total_amount = Column(Float)
    updated_at = Column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_industry_date', 'industry', 'trade_date', unique=True),
    )

class DailyBasic(Base):
    __tablename__ = "daily_basics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String, index=True)
    trade_date = Column(Date, index=True)
    close = Column(Float)
    turnover_rate = Column(Float)
    turnover_rate_f = Column(Float)
    volume_ratio = Column(Float)
    pe = Column(Float)
    pe_ttm = Column(Float)
    pb = Column(Float)
    ps = Column(Float)
    ps_ttm = Column(Float)
    dv_ratio = Column(Float)
    dv_ttm = Column(Float)
    total_share = Column(Float)
    float_share = Column(Float)
    free_share = Column(Float)
    total_mv = Column(Float)
    circ_mv = Column(Float)
    updated_at = Column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_daily_ts_code_date', 'ts_code', 'trade_date', unique=True),
    )

class WeeklyBar(Base):
    __tablename__ = "weekly_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    trade_date: Mapped[datetime.date] = mapped_column(Date, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    vol: Mapped[float] = mapped_column(Float)
    amount: Mapped[float] = mapped_column(Float)
    adj_factor: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_weekly_ts_code_date', 'ts_code', 'trade_date', unique=True),
    )

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class MonthlyBar(Base):
    __tablename__ = "monthly_bars"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    trade_date: Mapped[datetime.date] = mapped_column(Date, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    vol: Mapped[float] = mapped_column(Float)
    amount: Mapped[float] = mapped_column(Float)
    adj_factor: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_monthly_ts_code_date', 'ts_code', 'trade_date', unique=True),
    )

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class MinuteBar(Base):
    __tablename__ = "minute_bars"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String, index=True)
    trade_time = Column(DateTime, index=True) # 具体时间 YYYY-MM-DD HH:MM:SS
    freq = Column(String, index=True) # 5min, 30min
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    vol = Column(Float)
    amount = Column(Float)
    adj_factor = Column(Float, default=1.0) # 复权因子
    updated_at = Column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_minute_ts_code_time_freq', 'ts_code', 'trade_time', 'freq', unique=True),
    )

class StockIndicator(Base):
    __tablename__ = "stock_indicators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String, index=True)
    trade_date = Column(Date, index=True) # 计算指标的基准日期
    
    # 技术指标
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    ma60 = Column(Float)
    vol_ma5 = Column(Float)
    vol_ma10 = Column(Float)
    macd = Column(Float)      # DIF
    macd_dea = Column(Float)  # DEA
    macd_diff = Column(Float) # Histogram (DIF-DEA)*2
    
    # 乖离率 (Bias)
    bias5 = Column(Float)
    bias10 = Column(Float)
    bias20 = Column(Float)
    
    # 周线指标
    weekly_ma5 = Column(Float)
    weekly_ma10 = Column(Float)
    weekly_ma20 = Column(Float)
    weekly_ma60 = Column(Float)
    weekly_vol_ma5 = Column(Float)
    weekly_vol_ma10 = Column(Float)
    weekly_macd = Column(Float)
    weekly_macd_dea = Column(Float)
    weekly_macd_diff = Column(Float)
    weekly_ma20_slope = Column(Float) 
    is_weekly_bullish = Column(Integer) # 0/1 (SQLite no Boolean)
    
    # 月线指标
    monthly_ma5 = Column(Float)
    monthly_ma10 = Column(Float)
    monthly_ma20 = Column(Float)
    monthly_ma60 = Column(Float)
    monthly_vol_ma5 = Column(Float)
    monthly_vol_ma10 = Column(Float)
    monthly_macd = Column(Float)
    monthly_macd_dea = Column(Float)
    monthly_macd_diff = Column(Float)
    is_monthly_bullish = Column(Integer)
    
    # 综合判定
    is_daily_bullish = Column(Integer) # 日线多头排列
    is_trend_recovering = Column(Integer) # 趋势修复
    adj_factor = Column(Float, default=1.0) # 计算时使用的复权因子
    
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    __table_args__ = (
        Index('idx_indicator_ts_code_date', 'ts_code', 'trade_date', unique=True),
    )

class FinaIndicator(Base):
    """
    财务指标表 (季度/年度报告)
    用于基本面筛选
    """
    __tablename__ = "fina_indicators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ts_code = Column(String, index=True)
    end_date = Column(Date, index=True)      # 报告期 (如 20231231)

    # 核心指标
    roe = Column(Float)                      # 净资产收益率 (%)
    netprofit_margin = Column(Float)         # 销售净利率 (%)
    grossprofit_margin = Column(Float)       # 销售毛利率 (%)
    yoy_net_profit = Column(Float)           # 净利润同比 (%)
    yoy_revenue = Column(Float)              # 营业收入同比 (%)
    debt_to_assets = Column(Float)           # 资产负债率 (%)
    op_cashflow = Column(Float)              # 经营性现金流净额 (亿元)
    
    updated_at = Column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        Index('idx_fina_ts_code_date', 'ts_code', 'end_date', unique=True),
    )

class Account(Base):
    __tablename__ = "accounts"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    total_assets: Mapped[float] = mapped_column(Float, default=1000000.0)
    available_cash: Mapped[float] = mapped_column(Float, default=1000000.0)
    frozen_cash: Mapped[float] = mapped_column(Float, default=0.0)
    market_value: Mapped[float] = mapped_column(Float, default=0.0)
    total_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    total_pnl_pct: Mapped[float] = mapped_column(Float, default=0.0)
    
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class Position(Base):
    __tablename__ = "positions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    symbol: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    
    vol: Mapped[int] = mapped_column(Integer, default=0)
    available_vol: Mapped[int] = mapped_column(Integer, default=0)
    avg_price: Mapped[float] = mapped_column(Float, default=0.0)
    current_price: Mapped[float] = mapped_column(Float, default=0.0)
    market_value: Mapped[float] = mapped_column(Float, default=0.0)
    
    float_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    pnl_pct: Mapped[float] = mapped_column(Float, default=0.0)
    
    high_price: Mapped[float] = mapped_column(Float, default=0.0)
    high_pnl_pct: Mapped[float] = mapped_column(Float, default=0.0)
    
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    
    __table_args__ = (
        Index('idx_position_ts_code', 'ts_code', unique=True),
    )

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class TradeRecord(Base):
    __tablename__ = "trade_records"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    name: Mapped[str] = mapped_column(String)
    
    trade_type: Mapped[str] = mapped_column(String)
    price: Mapped[float] = mapped_column(Float)
    vol: Mapped[int] = mapped_column(Integer)
    amount: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float, default=0.0)
    
    trade_time: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now, index=True)
    plan_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    settlement_status: Mapped[str] = mapped_column(String, default="SETTLED")
    pnl_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class DailyPerformance(Base):
    """每日盈亏与资金曲线数据"""
    __tablename__ = "daily_performance"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date, index=True, unique=True)
    total_assets: Mapped[float] = mapped_column(Float)
    available_cash: Mapped[float] = mapped_column(Float)
    frozen_cash: Mapped[float] = mapped_column(Float, default=0.0)
    market_value: Mapped[float] = mapped_column(Float)
    daily_pnl: Mapped[float] = mapped_column(Float)
    daily_pnl_pct: Mapped[float] = mapped_column(Float)
    total_pnl: Mapped[float] = mapped_column(Float)
    total_pnl_pct: Mapped[float] = mapped_column(Float)
    
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)

class AuditReport(Base):
    """审计报告主表"""
    __tablename__ = "audit_reports"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    audit_date: Mapped[datetime.date] = mapped_column(Date, index=True)
    status: Mapped[str] = mapped_column(String)
    summary: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class AuditDetail(Base):
    """审计报告明细表"""
    __tablename__ = "audit_details"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[int] = mapped_column(Integer, index=True)
    ts_code: Mapped[str] = mapped_column(String)
    diff_type: Mapped[str] = mapped_column(String)
    expected_value: Mapped[float] = mapped_column(Float)
    actual_value: Mapped[float] = mapped_column(Float)
    diff_amount: Mapped[float] = mapped_column(Float)
    description: Mapped[str] = mapped_column(String)
    adjustment_suggestion: Mapped[str] = mapped_column(String)
    is_resolved: Mapped[bool] = mapped_column(Boolean, default=False)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class ReflectionMemory(Base):
    """
    反思记忆：可复用规则片段
    """
    __tablename__ = "reflection_memories"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # 规则三要素
    condition: Mapped[str] = mapped_column(String)
    action: Mapped[str] = mapped_column(String)
    reason: Mapped[str] = mapped_column(String)
    
    # 元数据
    strategy_name: Mapped[str] = mapped_column(String, index=True)
    market_temperature_bucket: Mapped[str] = mapped_column(String)
    
    weight: Mapped[float] = mapped_column(Float, default=1.0)
    source_event_id: Mapped[int] = mapped_column(Integer)
    source_event_type: Mapped[str] = mapped_column(String)
    
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, onupdate=datetime.datetime.now)

class TempMemory(Base):
    __tablename__ = "temp_memories"

    id = Column(Integer, primary_key=True, autoincrement=True)
    memory_date = Column(Date, index=True)
    category = Column(String, index=True)
    content = Column(Text)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, onupdate=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class FinaScreeningResult(Base):
    """
    财务筛选结果缓存表
    用于存储财务筛选结果，避免每次选股都从Tushare获取数据
    """
    __tablename__ = "fina_screening_results"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_code: Mapped[str] = mapped_column(String, index=True)
    end_date: Mapped[datetime.date] = mapped_column(Date, index=True)
    
    # 筛选结果
    screening_json: Mapped[str] = mapped_column(Text)
    total_score: Mapped[float] = mapped_column(Float)
    
    # 元数据
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    
    __table_args__ = (
        Index('idx_fina_screening_ts_code_date', 'ts_code', 'end_date', unique=True),
    )

class RewardPunishRule(Base):
    __tablename__ = "reward_punish_rules"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    category: Mapped[str] = mapped_column(String)
    metric: Mapped[str] = mapped_column(String)
    comparator: Mapped[str] = mapped_column(String)
    threshold: Mapped[float] = mapped_column(Float)
    action: Mapped[str] = mapped_column(String)
    level: Mapped[str] = mapped_column(String)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class RewardPunishEvent(Base):
    __tablename__ = "reward_punish_events"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(Integer, index=True)
    rule_name: Mapped[str] = mapped_column(String, index=True)
    event_date: Mapped[datetime.date] = mapped_column(Date, index=True)
    metric: Mapped[str] = mapped_column(String)
    metric_value: Mapped[float] = mapped_column(Float)
    action: Mapped[str] = mapped_column(String)
    level: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="TRIGGERED")
    detail: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)
    resolved_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, nullable=True)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class RewardPunishState(Base):
    __tablename__ = "reward_punish_state"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trading_paused: Mapped[bool] = mapped_column(Boolean, default=False)
    pause_reason: Mapped[str] = mapped_column(String, default="")
    intraday_peak_assets: Mapped[float] = mapped_column(Float, default=0.0)
    intraday_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)

class RewardPunishAppeal(Base):
    __tablename__ = "reward_punish_appeals"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[int] = mapped_column(Integer, index=True)
    status: Mapped[str] = mapped_column(String, default="PENDING")
    reason: Mapped[str] = mapped_column(Text)
    reviewer: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    review_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, default=datetime.datetime.now)
    reviewed_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime, nullable=True)

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            setattr(self, key, value)
