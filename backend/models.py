from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.database import Base


class Ticker(Base):
    __tablename__ = "tickers"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, unique=True, index=True)
    exchange: Mapped[str | None] = mapped_column(String, nullable=True)
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    sector: Mapped[str | None] = mapped_column(String, nullable=True)
    created_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    created_by_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String, nullable=True)
    is_sample: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    metrics: Mapped[list["Metrics"]] = relationship(back_populates="ticker_ref", cascade="all, delete-orphan")
    financials_history: Mapped[list["FinancialsHistory"]] = relationship(
        back_populates="ticker_ref", cascade="all, delete-orphan"
    )
    prices_history: Mapped[list["PricesHistory"]] = relationship(back_populates="ticker_ref", cascade="all, delete-orphan")


class Metrics(Base):
    __tablename__ = "metrics"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker_symbol: Mapped[str] = mapped_column(String, ForeignKey("tickers.symbol"), index=True)
    ticker: Mapped[str | None] = mapped_column(String, nullable=True)
    asOf: Mapped[Date | None] = mapped_column(Date, nullable=True)
    as_of_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    data_source: Mapped[str | None] = mapped_column(String, nullable=True)

    price_current: Mapped[float | None] = mapped_column(Float, nullable=True)
    eps_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    eps_forward: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_fwd: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_fwd_sector: Mapped[float | None] = mapped_column(Float, nullable=True)
    ev_ebitda: Mapped[float | None] = mapped_column(Float, nullable=True)
    ev_ebitda_sector: Mapped[float | None] = mapped_column(Float, nullable=True)
    fcf_yield_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    peg_5y: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_12m: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_24m: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_36m: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_5y_low: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_5y_high: Mapped[float | None] = mapped_column(Float, nullable=True)
    pe_5y_median: Mapped[float | None] = mapped_column(Float, nullable=True)
    current_pe: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_cap: Mapped[float | None] = mapped_column(Float, nullable=True)
    roic_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    fcf_margin_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    cfo_to_ni: Mapped[float | None] = mapped_column(Float, nullable=True)
    fcf_to_ebit: Mapped[float | None] = mapped_column(Float, nullable=True)
    accruals_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    margin_stdev_5y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    buyback_yield_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    debt_to_equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    netdebt_to_ebitda: Mapped[float | None] = mapped_column(Float, nullable=True)
    interest_coverage_x: Mapped[float | None] = mapped_column(Float, nullable=True)
    eps_cagr_5y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    revenue_cagr_5y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    eps_cagr_3y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    revenue_cagr_3y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    moat_score_0_10: Mapped[float | None] = mapped_column(Float, nullable=True)
    recurring_revenue_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    insider_own_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    founder_led_bool: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    gm_trend_5y: Mapped[float | None] = mapped_column(Float, nullable=True)
    riskdownside_score_0_10: Mapped[float | None] = mapped_column(Float, nullable=True)
    beta_5y: Mapped[float | None] = mapped_column(Float, nullable=True)
    maxdrawdown_5y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    netcash_to_mktcap_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    sector_cyc_tag: Mapped[str | None] = mapped_column(String, nullable=True)
    macrofit_score_0_10: Mapped[float | None] = mapped_column(Float, nullable=True)
    narrative_score_0_10: Mapped[float | None] = mapped_column(Float, nullable=True)
    sharecount_change_5y_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    sbc_to_sales_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_growth_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_years: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_target_cagr: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_pe_custom: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_pe_bear_override: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_pe_bull_override: Mapped[float | None] = mapped_column(Float, nullable=True)
    proj_pe_mid_override: Mapped[float | None] = mapped_column(Float, nullable=True)
    partial_ttm: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    revenue_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_income_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    cfo_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    capex_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    ebit_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    ebitda_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_debt: Mapped[float | None] = mapped_column(Float, nullable=True)
    cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares_out: Mapped[float | None] = mapped_column(Float, nullable=True)
    interest_expense_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    depreciation_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)
    sbc_ttm: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    created_by_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String, nullable=True)
    is_sample: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    ticker_ref: Mapped[Ticker] = relationship(back_populates="metrics")


class FinancialsHistory(Base):
    __tablename__ = "financials_history"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str] = mapped_column(String, ForeignKey("tickers.symbol"), index=True)
    period_end: Mapped[Date | None] = mapped_column(Date, nullable=True)
    freq: Mapped[str | None] = mapped_column(String, nullable=True)
    revenue: Mapped[float | None] = mapped_column(Float, nullable=True)
    cost_of_revenue: Mapped[float | None] = mapped_column(Float, nullable=True)
    gross_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    operating_income: Mapped[float | None] = mapped_column(Float, nullable=True)
    ebit: Mapped[float | None] = mapped_column(Float, nullable=True)
    net_income: Mapped[float | None] = mapped_column(Float, nullable=True)
    research_development: Mapped[float | None] = mapped_column(Float, nullable=True)
    interest_expense: Mapped[float | None] = mapped_column(Float, nullable=True)
    eps_diluted: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares_diluted: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_assets: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_liabilities: Mapped[float | None] = mapped_column(Float, nullable=True)
    stockholder_equity: Mapped[float | None] = mapped_column(Float, nullable=True)
    cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    short_term_investments: Mapped[float | None] = mapped_column(Float, nullable=True)
    long_term_debt: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_debt: Mapped[float | None] = mapped_column(Float, nullable=True)
    shares_outstanding: Mapped[float | None] = mapped_column(Float, nullable=True)
    cfo: Mapped[float | None] = mapped_column(Float, nullable=True)
    capex: Mapped[float | None] = mapped_column(Float, nullable=True)
    fcf: Mapped[float | None] = mapped_column(Float, nullable=True)
    depreciation: Mapped[float | None] = mapped_column(Float, nullable=True)
    stock_based_compensation: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    as_of_date: Mapped[Date | None] = mapped_column(Date, nullable=True)

    created_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    created_by_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String, nullable=True)
    is_sample: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    ticker_ref: Mapped[Ticker] = relationship(back_populates="financials_history")


class PricesHistory(Base):
    __tablename__ = "prices_history"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str] = mapped_column(String, ForeignKey("tickers.symbol"), index=True)
    date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    close_adj: Mapped[float | None] = mapped_column(Float, nullable=True)
    open: Mapped[float | None] = mapped_column(Float, nullable=True)
    high: Mapped[float | None] = mapped_column(Float, nullable=True)
    low: Mapped[float | None] = mapped_column(Float, nullable=True)
    close: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    as_of_date: Mapped[Date | None] = mapped_column(Date, nullable=True)

    created_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    created_by_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String, nullable=True)
    is_sample: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    ticker_ref: Mapped[Ticker] = relationship(back_populates="prices_history")


class LensPreset(Base):
    __tablename__ = "lens_presets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, index=True)
    valuation: Mapped[float | None] = mapped_column(Float, nullable=True)
    quality: Mapped[float | None] = mapped_column(Float, nullable=True)
    capitalAllocation: Mapped[float | None] = mapped_column(Float, nullable=True)
    growth: Mapped[float | None] = mapped_column(Float, nullable=True)
    moat: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk: Mapped[float | None] = mapped_column(Float, nullable=True)
    macro: Mapped[float | None] = mapped_column(Float, nullable=True)
    narrative: Mapped[float | None] = mapped_column(Float, nullable=True)
    dilution: Mapped[float | None] = mapped_column(Float, nullable=True)
    buyThreshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    watchThreshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    mosThreshold: Mapped[float | None] = mapped_column(Float, nullable=True)
    scoringHints: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_date: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    created_by_id: Mapped[str | None] = mapped_column(String, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String, nullable=True)
    is_sample: Mapped[bool | None] = mapped_column(Boolean, nullable=True)


class ScoreSnapshot(Base):
    """
    Deterministic score snapshot for a (ticker, lens, as_of_date) triple.
    Invariant: same metrics + same lens + same SCORE_VERSION → same snapshot_hash.

    Recommendation = score-only (no MOS or confidence gating).
    MOS is a display signal only (mos_signal: +/0/-).
    Confidence is an audit signal only (confidence_grade: A/B/C/D).
    """
    __tablename__ = "score_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker_symbol: Mapped[str] = mapped_column(String, ForeignKey("tickers.symbol"), index=True)
    lens_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    lens_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Versioning
    score_version: Mapped[str | None] = mapped_column(String, nullable=True)
    data_version: Mapped[str | None] = mapped_column(String, nullable=True)

    # Scores
    final_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    category_scores: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON

    # Recommendation (score-only — no MOS or confidence gating)
    recommendation: Mapped[str | None] = mapped_column(String, nullable=True)

    # Confidence (audit/display only — does NOT gate recommendation)
    confidence_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_grade: Mapped[str | None] = mapped_column(String, nullable=True)

    # MOS (display signal only — does NOT gate recommendation)
    mos_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    mos_signal: Mapped[str | None] = mapped_column(String, nullable=True)

    # Explainability
    top_positive_contributors: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    top_negative_contributors: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON
    missing_critical_fields: Mapped[str | None] = mapped_column(Text, nullable=True)    # JSON
    resolution_warnings: Mapped[str | None] = mapped_column(Text, nullable=True)        # JSON

    # Determinism
    snapshot_hash: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # Timestamps
    as_of_date: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    created_at: Mapped[str | None] = mapped_column(String, nullable=True)


class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, index=True)
    base_currency: Mapped[str] = mapped_column(String, nullable=False, default="USD")
    owner_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class PortfolioSettings(Base):
    __tablename__ = "portfolio_settings"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    strict_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stale_trading_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    calendar_policy: Mapped[str] = mapped_column(String, nullable=False, default="union_required_all_inputs")
    default_history_range: Mapped[str | None] = mapped_column(String, nullable=True)
    cash_management_mode: Mapped[str] = mapped_column(String, nullable=False, default="track_cash")
    include_dividends_in_performance: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    reinvest_dividends_overlay: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    deleted_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class PortfolioTransaction(Base):
    __tablename__ = "portfolio_transactions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    security_id: Mapped[str | None] = mapped_column(String, ForeignKey("security_identities.security_id"), nullable=True, index=True)
    ticker_symbol_raw: Mapped[str] = mapped_column(String, nullable=False)
    ticker_symbol_normalized: Mapped[str] = mapped_column(String, index=True)
    tx_type: Mapped[str] = mapped_column(String, nullable=False)
    trade_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    shares: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    gross_amount: Mapped[float] = mapped_column(Float, nullable=False)
    fx_at_execution: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    gross_amount_base: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_generated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    generated_event_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    currency: Mapped[str] = mapped_column(String, nullable=False, default="USD")
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    deleted_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class SecurityIdentity(Base):
    __tablename__ = "security_identities"

    security_id: Mapped[str] = mapped_column(String, primary_key=True)
    normalized_symbol: Mapped[str] = mapped_column(String, unique=True, index=True)
    exchange: Mapped[str | None] = mapped_column(String, nullable=True)
    mic: Mapped[str | None] = mapped_column(String, nullable=True)
    vendor_symbol: Mapped[str | None] = mapped_column(String, nullable=True)
    raw_symbol_example: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class SecuritySymbolMap(Base):
    __tablename__ = "security_symbol_map"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    raw_input_symbol: Mapped[str] = mapped_column(String, unique=True, index=True)
    normalized_symbol: Mapped[str] = mapped_column(String, index=True)
    exchange: Mapped[str | None] = mapped_column(String, nullable=True)
    mic: Mapped[str | None] = mapped_column(String, nullable=True)
    vendor_symbol: Mapped[str | None] = mapped_column(String, nullable=True)
    security_id: Mapped[str] = mapped_column(String, ForeignKey("security_identities.security_id"), index=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class PortfolioProcessingRun(Base):
    __tablename__ = "portfolio_processing_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str | None] = mapped_column(String, ForeignKey("portfolios.id"), index=True, nullable=True)
    started_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String, index=True)
    warnings_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    hash_inputs: Mapped[str] = mapped_column(String, index=True)
    engine_version: Mapped[str | None] = mapped_column(String, nullable=True)


class PortfolioCoverageEvent(Base):
    __tablename__ = "portfolio_coverage_events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str | None] = mapped_column(String, ForeignKey("portfolios.id"), index=True, nullable=True)
    run_id: Mapped[str] = mapped_column(String, ForeignKey("portfolio_processing_runs.id"), index=True)
    security_id: Mapped[str | None] = mapped_column(String, ForeignKey("security_identities.security_id"), nullable=True, index=True)
    ticker: Mapped[str] = mapped_column(String, index=True)
    raw_input_symbol: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, index=True)
    warning_code: Mapped[str | None] = mapped_column(String, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    fallback_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_missing_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    last_missing_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    coverage_start: Mapped[Date | None] = mapped_column(Date, nullable=True)
    coverage_end: Mapped[Date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class PortfolioCorrectionEvent(Base):
    __tablename__ = "portfolio_correction_events"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str | None] = mapped_column(String, ForeignKey("portfolios.id"), index=True, nullable=True)
    run_id: Mapped[str] = mapped_column(String, ForeignKey("portfolio_processing_runs.id"), index=True)
    ticker: Mapped[str] = mapped_column(String, index=True)
    row_id: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_shares: Mapped[float] = mapped_column(Float, nullable=False)
    available_shares: Mapped[float] = mapped_column(Float, nullable=False)
    executed_shares: Mapped[float] = mapped_column(Float, nullable=False)
    delta_shares: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class CorporateAction(Base):
    __tablename__ = "corporate_actions"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    ticker: Mapped[str] = mapped_column(String, index=True)
    action_type: Mapped[str] = mapped_column(String, index=True)
    effective_date: Mapped[Date] = mapped_column(Date, index=True)
    factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    cash_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    deleted_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class LedgerSnapshot(Base):
    __tablename__ = "ledger_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    ledger_version: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    as_of: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    holdings_json: Mapped[str] = mapped_column(Text, nullable=False)
    basis_json: Mapped[str] = mapped_column(Text, nullable=False)
    cash: Mapped[float | None] = mapped_column(Float, nullable=True)
    input_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    ticker: Mapped[str] = mapped_column(String, index=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    as_of: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    input_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)


class FXRateSnapshot(Base):
    __tablename__ = "fx_rate_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    base_currency: Mapped[str] = mapped_column(String, nullable=False, index=True)
    quote_currency: Mapped[str] = mapped_column(String, nullable=False, index=True)
    rate: Mapped[float] = mapped_column(Float, nullable=False)
    as_of: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    input_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)


class ValuationSnapshot(Base):
    __tablename__ = "valuation_snapshots"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    valuation_version: Mapped[int] = mapped_column(Integer, nullable=False, index=True, default=1)
    ledger_snapshot_id: Mapped[str] = mapped_column(String, ForeignKey("ledger_snapshots.id"), index=True)
    nav: Mapped[float] = mapped_column(Float, nullable=False)
    nav_delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    holdings_delta_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    price_attribution_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    fx_attribution_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    transaction_attribution_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    corporate_action_attribution_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    price_change_component: Mapped[float | None] = mapped_column(Float, nullable=True)
    transaction_change_component: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_explained_delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    unexplained_delta: Mapped[float | None] = mapped_column(Float, nullable=True)
    currency: Mapped[str] = mapped_column(String, nullable=False)
    as_of: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    rebuild_duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    components_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class PortfolioEquityHistoryBuild(Base):
    __tablename__ = "portfolio_equity_history_builds"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    build_version: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    from_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    to_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    strict: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    engine_version: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, index=True)
    started_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    rows_written: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    forced: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class PortfolioEquityHistoryRow(Base):
    __tablename__ = "portfolio_equity_history_rows"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), index=True)
    build_version: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    total_equity: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    cash_balance: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    market_value_total: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    cost_basis_total: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    unrealized_gain_value: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    realized_gain_value: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    dividend_cash_value: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    day_change_value: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    day_change_percent: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    net_contribution: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False, default=0)
    market_return_component: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False, default=0)
    fx_return_component: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False, default=0)
    twr_index: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False, default=1)
    input_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class PriceHistory(Base):
    __tablename__ = "price_history"
    __table_args__ = (UniqueConstraint("ticker", "datetime_utc", name="uq_price_history_ticker_datetime"),)

    id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str] = mapped_column(String, nullable=False, index=True)
    datetime_utc: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    adjusted_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class FXRate(Base):
    __tablename__ = "fx_rates"
    __table_args__ = (
        UniqueConstraint("base_currency", "quote_currency", "datetime_utc", name="uq_fx_rates_pair_datetime"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    base_currency: Mapped[str] = mapped_column(String, nullable=False, index=True)
    quote_currency: Mapped[str] = mapped_column(String, nullable=False, index=True)
    datetime_utc: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    rate: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class TickerMetadata(Base):
    __tablename__ = "ticker_metadata"

    ticker_normalized: Mapped[str] = mapped_column(String, primary_key=True)
    exchange: Mapped[str | None] = mapped_column(String, nullable=True)
    native_currency: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class DividendEvent(Base):
    __tablename__ = "dividend_events"
    __table_args__ = (
        UniqueConstraint("source_hash", name="uq_dividend_events_source_hash"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker: Mapped[str] = mapped_column(String, nullable=False, index=True)
    ex_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    pay_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    amount_per_share: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    currency: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    source_hash: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint("portfolio_id", "snapshot_date", name="uq_portfolio_snapshots_portfolio_date"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), nullable=False, index=True)
    snapshot_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    total_equity: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    total_cash: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    unrealized: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    realized: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    market_component: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    fx_component: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)


class ClosedPosition(Base):
    __tablename__ = "closed_positions"
    __table_args__ = (
        UniqueConstraint("portfolio_id", "ticker", "close_date", name="uq_closed_positions_portfolio_ticker_close_date"),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True)
    portfolio_id: Mapped[str] = mapped_column(String, ForeignKey("portfolios.id"), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String, nullable=False, index=True)
    open_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    close_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    total_shares: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    total_cost_basis: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    total_proceeds: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    realized_gain: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    realized_gain_pct: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    fx_component: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    total_dividends: Mapped[float] = mapped_column(Numeric(24, 10), nullable=False)
    holding_period_days: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
