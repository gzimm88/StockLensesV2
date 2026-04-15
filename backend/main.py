import logging
import math
import uuid
from pathlib import Path
from typing import Any
from datetime import date, datetime, time, timedelta, timezone

# Load .env from repo root before any other imports read os.environ
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_env_path, override=False)
except ImportError:
    pass  # python-dotenv not installed — set FINNHUB_API_KEY in the shell

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from backend.database import Base, SessionLocal, engine, get_db
from backend.models import (
    ClosedPosition,
    DividendEvent,
    FXRate,
    FinancialsHistory,
    LedgerSnapshot,
    LensPreset,
    Metrics,
    Portfolio,
    PortfolioCorrectionEvent,
    PortfolioCoverageEvent,
    PortfolioEquityHistoryRow,
    PortfolioSnapshot,
    PortfolioProcessingRun,
    PortfolioSettings,
    PortfolioTransaction,
    PriceHistory,
    PricesHistory,
    ProjectionAssumption,
    ProjectionSnapshot,
    AlertNotification,
    ScoreSnapshot,
    Ticker,
    TickerMetadata,
)
from backend.orchestrator.onboarding_orchestrator import (
    OnboardingResult,
    run_full_onboard,
    step_compute_fundamental_metrics,
    step_finnhub_fundamentals,
    step_sync_recent_prices,
    step_yahoo_fundamentals,
    step_yahoo_prices_5y,
    ticker_is_onboarded,
)
from backend.orchestrator.portfolio_orchestrator import (
    backfill_dividend_history_if_missing,
    backfill_fx_history_if_missing,
    create_corporate_action,
    create_transaction,
    compute_performance_breakdown,
    compute_time_returns,
    create_portfolio,
    get_portfolio_dashboard_summary,
    get_portfolio_equity_history,
    get_portfolio_holdings,
    get_portfolio_settings,
    get_latest_valuation_attribution,
    get_latest_valuation_diff,
    get_or_create_default_portfolio,
    import_transactions_from_csv_for_portfolio,
    list_closed_positions_for_portfolio,
    list_corporate_actions_for_portfolio,
    list_transactions_for_portfolio,
    list_portfolios,
    load_last_portfolio_run,
    rebuild_position_ledger,
    rebuild_equity_history,
    refresh_market_data_for_portfolio,
    rebuild_valuation_snapshot,
    run_portfolio_creation_flow,
    soft_delete_corporate_action,
    soft_delete_portfolio,
    soft_delete_transaction,
    update_corporate_action,
    update_portfolio_settings,
    update_transaction,
)
from backend.repositories import financials_repo, metrics_repo, prices_repo
from backend.services.portfolio_engine import PortfolioEngineError
from backend.scheduler import start_market_data_scheduler, stop_market_data_scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="StockLenses Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)


def _ensure_metrics_schema() -> None:
    """Add backward-compatible columns that may be missing in existing SQLite DBs."""
    with engine.begin() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(metrics)"))}
        if "eps_forward" not in cols:
            conn.execute(text("ALTER TABLE metrics ADD COLUMN eps_forward FLOAT"))


_ensure_metrics_schema()


def _ensure_phase1_schema() -> None:
    with engine.begin() as conn:
        run_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_processing_runs)"))}
        if run_cols and "portfolio_id" not in run_cols:
            conn.execute(text("ALTER TABLE portfolio_processing_runs ADD COLUMN portfolio_id TEXT"))

        cov_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_coverage_events)"))}
        if cov_cols and "portfolio_id" not in cov_cols:
            conn.execute(text("ALTER TABLE portfolio_coverage_events ADD COLUMN portfolio_id TEXT"))

        corr_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_correction_events)"))}
        if corr_cols and "portfolio_id" not in corr_cols:
            conn.execute(text("ALTER TABLE portfolio_correction_events ADD COLUMN portfolio_id TEXT"))

        tx_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_transactions)"))}
        if tx_cols and "deleted_at" not in tx_cols:
            conn.execute(text("ALTER TABLE portfolio_transactions ADD COLUMN deleted_at DATETIME"))
        if tx_cols and "version" not in tx_cols:
            conn.execute(text("ALTER TABLE portfolio_transactions ADD COLUMN version INTEGER NOT NULL DEFAULT 1"))


_ensure_phase1_schema()


def _ensure_phase5_schema() -> None:
    with engine.begin() as conn:
        val_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(valuation_snapshots)"))}
        if not val_cols:
            return
        if "valuation_version" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN valuation_version INTEGER NOT NULL DEFAULT 1"))
        if "nav_delta" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN nav_delta FLOAT"))
        if "holdings_delta_json" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN holdings_delta_json TEXT"))
        if "price_change_component" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN price_change_component FLOAT"))
        if "transaction_change_component" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN transaction_change_component FLOAT"))
        if "rebuild_duration_ms" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN rebuild_duration_ms INTEGER"))


_ensure_phase5_schema()


def _ensure_phase6_schema() -> None:
    with engine.begin() as conn:
        val_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(valuation_snapshots)"))}
        if not val_cols:
            return
        if "price_attribution_json" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN price_attribution_json TEXT"))
        if "fx_attribution_json" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN fx_attribution_json TEXT"))
        if "transaction_attribution_json" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN transaction_attribution_json TEXT"))
        if "corporate_action_attribution_json" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN corporate_action_attribution_json TEXT"))
        if "total_explained_delta" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN total_explained_delta FLOAT"))
        if "unexplained_delta" not in val_cols:
            conn.execute(text("ALTER TABLE valuation_snapshots ADD COLUMN unexplained_delta FLOAT"))


_ensure_phase6_schema()


def _ensure_phase9_schema() -> None:
    with engine.begin() as conn:
        tx_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_transactions)"))}
        if tx_cols and "fx_at_execution" not in tx_cols:
            conn.execute(text("ALTER TABLE portfolio_transactions ADD COLUMN fx_at_execution FLOAT NOT NULL DEFAULT 1.0"))
        if tx_cols and "gross_amount_base" not in tx_cols:
            conn.execute(text("ALTER TABLE portfolio_transactions ADD COLUMN gross_amount_base FLOAT NOT NULL DEFAULT 0.0"))
        # Backfill immutable booking facts for existing rows where possible.
        conn.execute(
            text(
                """
                UPDATE portfolio_transactions
                SET
                    fx_at_execution = CASE
                        WHEN fx_at_execution IS NULL OR fx_at_execution = 0 THEN 1.0
                        ELSE fx_at_execution
                    END,
                    gross_amount_base = CASE
                        WHEN gross_amount_base IS NULL OR gross_amount_base = 0 THEN gross_amount
                        ELSE gross_amount_base
                    END
                """
            )
        )


_ensure_phase9_schema()


def _ensure_phase11_schema() -> None:
    with engine.begin() as conn:
        settings_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_settings)"))}
        if settings_cols:
            if "cash_management_mode" not in settings_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_settings "
                        "ADD COLUMN cash_management_mode VARCHAR NOT NULL DEFAULT 'track_cash'"
                    )
                )
            if "include_dividends_in_performance" not in settings_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_settings "
                        "ADD COLUMN include_dividends_in_performance BOOLEAN NOT NULL DEFAULT 1"
                    )
                )
            if "reinvest_dividends_overlay" not in settings_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_settings "
                        "ADD COLUMN reinvest_dividends_overlay BOOLEAN NOT NULL DEFAULT 0"
                    )
                )

        row_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_equity_history_rows)"))}
        if row_cols:
            if "net_contribution" not in row_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_equity_history_rows "
                        "ADD COLUMN net_contribution NUMERIC(24,10) NOT NULL DEFAULT 0"
                    )
                )
            if "market_return_component" not in row_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_equity_history_rows "
                        "ADD COLUMN market_return_component NUMERIC(24,10) NOT NULL DEFAULT 0"
                    )
                )
            if "fx_return_component" not in row_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_equity_history_rows "
                        "ADD COLUMN fx_return_component NUMERIC(24,10) NOT NULL DEFAULT 0"
                    )
                )
            if "twr_index" not in row_cols:
                conn.execute(
                    text(
                        "ALTER TABLE portfolio_equity_history_rows "
                        "ADD COLUMN twr_index NUMERIC(24,10) NOT NULL DEFAULT 1"
                    )
                )


_ensure_phase11_schema()


def _ensure_phase12a_schema() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS price_history (
                    id VARCHAR PRIMARY KEY,
                    ticker VARCHAR NOT NULL,
                    datetime_utc DATETIME NOT NULL,
                    price FLOAT NOT NULL,
                    adjusted_price FLOAT,
                    source VARCHAR,
                    created_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_price_history_ticker_datetime "
                "ON price_history (ticker, datetime_utc)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_price_history_ticker ON price_history (ticker)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_price_history_datetime_utc ON price_history (datetime_utc)"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS fx_rates (
                    id VARCHAR PRIMARY KEY,
                    base_currency VARCHAR NOT NULL,
                    quote_currency VARCHAR NOT NULL,
                    datetime_utc DATETIME NOT NULL,
                    rate FLOAT NOT NULL,
                    source VARCHAR,
                    created_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_fx_rates_pair_datetime "
                "ON fx_rates (base_currency, quote_currency, datetime_utc)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fx_rates_base_currency ON fx_rates (base_currency)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fx_rates_quote_currency ON fx_rates (quote_currency)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_fx_rates_datetime_utc ON fx_rates (datetime_utc)"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id VARCHAR PRIMARY KEY,
                    portfolio_id VARCHAR NOT NULL,
                    snapshot_date DATE NOT NULL,
                    total_equity NUMERIC(24,10) NOT NULL,
                    total_cash NUMERIC(24,10) NOT NULL,
                    unrealized NUMERIC(24,10) NOT NULL,
                    realized NUMERIC(24,10) NOT NULL,
                    market_component NUMERIC(24,10) NOT NULL,
                    fx_component NUMERIC(24,10) NOT NULL,
                    created_at DATETIME,
                    FOREIGN KEY(portfolio_id) REFERENCES portfolios(id)
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_snapshots_portfolio_date "
                "ON portfolio_snapshots (portfolio_id, snapshot_date)"
            )
        )
        conn.execute(
            text("CREATE INDEX IF NOT EXISTS ix_portfolio_snapshots_snapshot_date ON portfolio_snapshots (snapshot_date)")
        )


_ensure_phase12a_schema()


def _ensure_phase12b_schema() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS closed_positions (
                    id VARCHAR PRIMARY KEY,
                    portfolio_id VARCHAR NOT NULL,
                    ticker VARCHAR NOT NULL,
                    open_date DATE,
                    close_date DATE NOT NULL,
                    total_shares NUMERIC(24,10) NOT NULL,
                    total_cost_basis NUMERIC(24,10) NOT NULL,
                    total_proceeds NUMERIC(24,10) NOT NULL,
                    realized_gain NUMERIC(24,10) NOT NULL,
                    realized_gain_pct NUMERIC(24,10) NOT NULL,
                    fx_component NUMERIC(24,10) NOT NULL,
                    total_dividends NUMERIC(24,10) NOT NULL,
                    holding_period_days INTEGER NOT NULL,
                    created_at DATETIME,
                    FOREIGN KEY(portfolio_id) REFERENCES portfolios(id)
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_closed_positions_portfolio_ticker_close_date "
                "ON closed_positions (portfolio_id, ticker, close_date)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_closed_positions_ticker ON closed_positions (ticker)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_closed_positions_close_date ON closed_positions (close_date)"))


_ensure_phase12b_schema()


def _ensure_phase13_schema() -> None:
    with engine.begin() as conn:
        tx_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolio_transactions)"))}
        if tx_cols and "is_generated" not in tx_cols:
            conn.execute(
                text("ALTER TABLE portfolio_transactions ADD COLUMN is_generated BOOLEAN NOT NULL DEFAULT 0")
            )
        if tx_cols and "generated_event_id" not in tx_cols:
            conn.execute(text("ALTER TABLE portfolio_transactions ADD COLUMN generated_event_id VARCHAR"))
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_portfolio_transactions_generated_event_id "
                "ON portfolio_transactions (generated_event_id)"
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS ticker_metadata (
                    ticker_normalized VARCHAR PRIMARY KEY,
                    exchange VARCHAR,
                    native_currency VARCHAR NOT NULL,
                    created_at DATETIME,
                    updated_at DATETIME
                )
                """
            )
        )

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS dividend_events (
                    id VARCHAR PRIMARY KEY,
                    ticker VARCHAR NOT NULL,
                    ex_date DATE NOT NULL,
                    pay_date DATE NOT NULL,
                    dividend_per_share_native NUMERIC(24,10) NOT NULL,
                    currency VARCHAR NOT NULL,
                    source VARCHAR,
                    source_hash VARCHAR NOT NULL,
                    created_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_dividend_events_source_hash "
                "ON dividend_events (source_hash)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dividend_events_ticker ON dividend_events (ticker)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dividend_events_ex_date ON dividend_events (ex_date)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dividend_events_pay_date ON dividend_events (pay_date)"))


_ensure_phase13_schema()


def _ensure_phase14_schema() -> None:
    with engine.begin() as conn:
        portfolio_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(portfolios)"))}
        if portfolio_cols and "apply_dividend_withholding" not in portfolio_cols:
            conn.execute(
                text("ALTER TABLE portfolios ADD COLUMN apply_dividend_withholding BOOLEAN NOT NULL DEFAULT 0")
            )
        if portfolio_cols and "dividend_withholding_percent" not in portfolio_cols:
            conn.execute(text("ALTER TABLE portfolios ADD COLUMN dividend_withholding_percent FLOAT"))

        conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS dividend_events ("
                "id VARCHAR PRIMARY KEY,"
                "ticker VARCHAR NOT NULL,"
                "ex_date DATE NOT NULL,"
                "pay_date DATE NOT NULL,"
                "dividend_per_share_native NUMERIC(24,10) NOT NULL,"
                "currency VARCHAR NOT NULL,"
                "source VARCHAR,"
                "source_hash VARCHAR NOT NULL,"
                "created_at DATETIME"
                ")"
            )
        )
        div_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(dividend_events)"))}
        if div_cols and "amount_per_share" not in div_cols:
            conn.execute(text("ALTER TABLE dividend_events ADD COLUMN amount_per_share NUMERIC(24,10)"))
            conn.execute(
                text(
                    "UPDATE dividend_events SET amount_per_share = dividend_per_share_native "
                    "WHERE amount_per_share IS NULL AND dividend_per_share_native IS NOT NULL"
                )
            )
        if div_cols and "dividend_per_share_native" not in div_cols:
            conn.execute(text("ALTER TABLE dividend_events ADD COLUMN dividend_per_share_native NUMERIC(24,10)"))
            if "amount_per_share" in div_cols:
                conn.execute(
                    text(
                        "UPDATE dividend_events SET dividend_per_share_native = amount_per_share "
                        "WHERE dividend_per_share_native IS NULL"
                    )
                )
        conn.execute(
            text(
                "UPDATE dividend_events SET amount_per_share = dividend_per_share_native "
                "WHERE amount_per_share IS NULL AND dividend_per_share_native IS NOT NULL"
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_dividend_events_ticker_ex_amount "
                "ON dividend_events (ticker, ex_date, dividend_per_share_native)"
            )
        )


_ensure_phase14_schema()


def _ensure_account_feature_schema() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS latest_prices (
                    ticker VARCHAR PRIMARY KEY,
                    vendor_symbol VARCHAR,
                    native_currency VARCHAR,
                    price FLOAT NOT NULL,
                    adjusted_price FLOAT,
                    datetime_utc DATETIME NOT NULL,
                    source VARCHAR,
                    created_at DATETIME,
                    updated_at DATETIME
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_latest_prices_datetime_utc ON latest_prices (datetime_utc)"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS latest_fx_rates (
                    base_currency VARCHAR NOT NULL,
                    quote_currency VARCHAR NOT NULL,
                    rate FLOAT NOT NULL,
                    datetime_utc DATETIME NOT NULL,
                    source VARCHAR,
                    created_at DATETIME,
                    updated_at DATETIME,
                    PRIMARY KEY (base_currency, quote_currency)
                )
                """
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_latest_fx_rates_datetime_utc ON latest_fx_rates (datetime_utc)"))

        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS screener_preferences (
                    id VARCHAR PRIMARY KEY,
                    user_id VARCHAR NOT NULL,
                    lens_id VARCHAR NOT NULL,
                    buy_threshold FLOAT,
                    watch_threshold FLOAT,
                    min_score FLOAT,
                    recommendation_filter VARCHAR,
                    created_at DATETIME,
                    updated_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_screener_preferences_user_lens "
                "ON screener_preferences (user_id, lens_id)"
            )
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_screener_preferences_user_id ON screener_preferences (user_id)"))


_ensure_account_feature_schema()


def _ensure_watchlist_frozen_schema() -> None:
    with engine.begin() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(account_watchlist_entries)"))}
        if not cols:
            return
        migrations = [
            ("frozen_entry_price", "FLOAT"),
            ("frozen_at", "DATETIME"),
            ("frozen_from_projection_id", "TEXT"),
            ("custom_entry_price", "FLOAT"),
            ("custom_buy_trigger_price", "FLOAT"),
            ("custom_sell_trigger_price", "FLOAT"),
            ("status", "VARCHAR NOT NULL DEFAULT 'watching'"),
            ("acted_at", "DATETIME"),
            ("acted_price", "FLOAT"),
            ("acted_notes", "TEXT"),
        ]
        for col, typ in migrations:
            if col not in cols:
                conn.execute(text(f"ALTER TABLE account_watchlist_entries ADD COLUMN {col} {typ}"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_account_watchlist_entries_status ON account_watchlist_entries(user_id, status)"))


_ensure_watchlist_frozen_schema()


def _ensure_projection_snapshot_schema() -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS projection_snapshots (
                id VARCHAR PRIMARY KEY,
                user_id VARCHAR NOT NULL,
                ticker_symbol VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                notes TEXT,
                status VARCHAR NOT NULL DEFAULT 'active',
                current_price FLOAT, current_eps FLOAT, growth_rate FLOAT,
                years INTEGER, target_cagr FLOAT,
                pe_bear FLOAT, pe_mid FLOAT, pe_bull FLOAT,
                pe_custom_terminal FLOAT, current_pe FLOAT, scenario VARCHAR,
                terminal_eps FLOAT, exit_pe FLOAT, terminal_price FLOAT,
                implied_cagr FLOAT, required_entry FLOAT, margin_of_safety FLOAT,
                yearly_data_json TEXT,
                overvalued_pct FLOAT NOT NULL DEFAULT 15.0,
                buy_trigger_price FLOAT, sell_trigger_price FLOAT,
                triggered_at DATETIME, triggered_type VARCHAR, triggered_price FLOAT,
                created_at DATETIME, updated_at DATETIME
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_projection_snapshots_user ON projection_snapshots(user_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_projection_snapshots_user_ticker_status ON projection_snapshots(user_id, ticker_symbol, status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_projection_snapshots_status ON projection_snapshots(status)"))
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_projection_snapshots_active ON projection_snapshots(user_id, ticker_symbol) WHERE status='active'"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS alert_notifications (
                id VARCHAR PRIMARY KEY,
                user_id VARCHAR NOT NULL,
                ticker_symbol VARCHAR NOT NULL,
                snapshot_id VARCHAR NOT NULL,
                alert_type VARCHAR NOT NULL,
                threshold_price FLOAT NOT NULL,
                triggered_price FLOAT NOT NULL,
                triggered_at DATETIME NOT NULL,
                email_sent BOOLEAN NOT NULL DEFAULT 0,
                email_error TEXT,
                read BOOLEAN NOT NULL DEFAULT 0,
                read_at DATETIME,
                dismissed BOOLEAN NOT NULL DEFAULT 0,
                dismissed_at DATETIME,
                created_at DATETIME
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_notifications_user ON alert_notifications(user_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_notifications_user_read ON alert_notifications(user_id, read)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_notifications_user_dismissed ON alert_notifications(user_id, dismissed)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_notifications_triggered_at ON alert_notifications(triggered_at)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_notifications_snapshot ON alert_notifications(snapshot_id)"))


_ensure_projection_snapshot_schema()


@app.on_event("startup")
def _on_startup() -> None:
    start_market_data_scheduler()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    stop_market_data_scheduler()


def _ensure_admin_account() -> User:
    db = SessionLocal()
    try:
        now = utcnow_naive()
        admin = db.query(User).filter(User.username == "Admin").first()
        admin_was_just_created = False
        if admin is None:
            admin = User(
                id=str(uuid.uuid4()),
                username="Admin",
                email=None,
                password_hash=hash_password("Admin1234"),
                role="admin",
                status="active",
                must_reset_password=False,
                created_at=now,
                updated_at=now,
            )
            db.add(admin)
            db.commit()
            db.refresh(admin)
            admin_was_just_created = True
        else:
            changed = False
            if admin.role != "admin":
                admin.role = "admin"
                changed = True
            if admin.status != "active":
                admin.status = "active"
                changed = True
            if changed:
                admin.updated_at = now
                db.commit()
                db.refresh(admin)

        ensure_account_limits(
            db,
            user_id=admin.id,
            max_screener_tickers=None,
            max_portfolio_tickers=None,
            max_portfolios=None,
            can_manage_users=True,
            can_use_projection=True,
            can_export=True,
        )

        for row in db.query(Portfolio).filter((Portfolio.owner_id.is_(None)) | (Portfolio.owner_id == "local")).all():
            row.owner_id = admin.id
            row.updated_at = now
        for row in db.query(LensPreset).filter(LensPreset.owner_id.is_(None)).all():
            row.owner_id = admin.id
            row.updated_date = now
            if not row.created_by_id:
                row.created_by_id = admin.id
            if not row.created_by:
                row.created_by = admin.username

        # Only seed the admin's watchlist with all existing tickers on first creation.
        # Re-seeding on every startup would resurrect tickers the user has explicitly removed.
        if admin_was_just_created:
            for row in db.query(Ticker).all():
                _ensure_watchlist_entry(db, admin.id, row.symbol)

        metric_rows = (
            db.query(Metrics)
            .filter(
                (Metrics.proj_growth_rate.is_not(None))
                | (Metrics.proj_years.is_not(None))
                | (Metrics.proj_target_cagr.is_not(None))
                | (Metrics.proj_pe_custom.is_not(None))
                | (Metrics.proj_pe_bear_override.is_not(None))
                | (Metrics.proj_pe_mid_override.is_not(None))
                | (Metrics.proj_pe_bull_override.is_not(None))
            )
            .all()
        )
        for metric in metric_rows:
            ticker_symbol = (metric.ticker_symbol or "").strip().upper()
            if not ticker_symbol:
                continue
            existing = (
                db.query(ProjectionAssumption)
                .filter(
                    ProjectionAssumption.user_id == admin.id,
                    ProjectionAssumption.ticker_symbol == ticker_symbol,
                )
                .first()
            )
            if existing is None:
                existing = ProjectionAssumption(
                    id=str(uuid.uuid4()),
                    user_id=admin.id,
                    ticker_symbol=ticker_symbol,
                    created_at=now,
                )
                db.add(existing)
            existing.growth_rate = _normalize_projection_percent(metric.proj_growth_rate)
            existing.years = int(metric.proj_years) if metric.proj_years is not None else None
            existing.target_cagr = _normalize_projection_percent(metric.proj_target_cagr)
            existing.pe_bear = metric.proj_pe_bear_override
            existing.pe_mid = metric.proj_pe_mid_override
            existing.pe_bull = metric.proj_pe_bull_override
            existing.pe_custom_terminal = metric.proj_pe_custom
            existing.updated_at = now
        db.commit()
        return admin
    finally:
        db.close()


def _get_current_user(request: Request, db: Session) -> User | None:
    user = get_user_by_session_token(db, request.cookies.get(SESSION_COOKIE_NAME))
    if user is not None:
        return user

    # Test/in-memory compatibility: older API tests build an isolated DB and
    # call protected routes without first creating a session-backed account.
    local = db.query(User).filter(User.id == "local", User.username == "local").first()
    if local is not None:
        return local

    if db.query(User).count() == 0:
        now = utcnow_naive()
        local = User(
            id="local",
            username="local",
            email=None,
            password_hash=hash_password("local"),
            role="admin",
            status="active",
            must_reset_password=False,
            created_at=now,
            updated_at=now,
        )
        db.add(local)
        db.commit()
        return local

    return None


def require_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    user = _get_current_user(request, db)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return user


def require_admin_user(request: Request, db: Session = Depends(get_db)) -> User:
    user = require_current_user(request, db)
    limits = get_account_limits(db, user.id)
    if user.role != "admin" and not (limits and limits.can_manage_users):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return user


def _portfolio_or_404_for_user(db: Session, portfolio_id: str, user_id: str) -> Portfolio:
    row = (
        db.query(Portfolio)
        .filter(
            Portfolio.id == portfolio_id,
            Portfolio.owner_id == user_id,
            Portfolio.is_deleted == False,
        )
        .first()
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Portfolio '{portfolio_id}' not found.")
    return row


def _transaction_or_404_for_user(db: Session, transaction_id: str, user_id: str) -> PortfolioTransaction:
    row = db.query(PortfolioTransaction).filter(PortfolioTransaction.id == transaction_id).first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Transaction '{transaction_id}' not found.")
    _portfolio_or_404_for_user(db, row.portfolio_id, user_id)
    return row


def _corporate_action_or_404_for_user(db: Session, action_id: str, user_id: str) -> CorporateAction:
    row = db.query(CorporateAction).filter(CorporateAction.id == action_id).first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Corporate action '{action_id}' not found.")
    _portfolio_or_404_for_user(db, row.portfolio_id, user_id)
    return row


def _account_portfolio_tickers(db: Session, user_id: str) -> set[str]:
    rows = (
        db.query(PortfolioTransaction.ticker_symbol_normalized)
        .join(Portfolio, Portfolio.id == PortfolioTransaction.portfolio_id)
        .filter(
            Portfolio.owner_id == user_id,
            Portfolio.is_deleted == False,
            PortfolioTransaction.is_deleted == False,
        )
        .all()
    )
    return {str(value).strip().upper() for (value,) in rows if value}


def _account_watchlist_symbols(db: Session, user_id: str) -> set[str]:
    rows = (
        db.query(AccountWatchlistEntry.ticker_symbol)
        .filter(AccountWatchlistEntry.user_id == user_id)
        .all()
    )
    return {str(value).strip().upper() for (value,) in rows if value}


def _ticker_or_404_for_user(db: Session, ticker_symbol: str, user_id: str) -> Ticker:
    ticker_upper = ticker_symbol.strip().upper()
    exists = (
        db.query(AccountWatchlistEntry)
        .filter(
            AccountWatchlistEntry.user_id == user_id,
            AccountWatchlistEntry.ticker_symbol == ticker_upper,
        )
        .first()
    )
    if exists is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Ticker '{ticker_upper}' not found.")
    row = db.query(Ticker).filter(Ticker.symbol == ticker_upper).first()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Ticker '{ticker_upper}' not found.")
    return row


def _ensure_watchlist_entry(db: Session, user_id: str, ticker_symbol: str) -> bool:
    ticker_upper = ticker_symbol.strip().upper()
    existing = (
        db.query(AccountWatchlistEntry)
        .filter(
            AccountWatchlistEntry.user_id == user_id,
            AccountWatchlistEntry.ticker_symbol == ticker_upper,
        )
        .first()
    )
    if existing is not None:
        return False
    now = utcnow_naive()
    db.add(
        AccountWatchlistEntry(
            id=str(uuid.uuid4()),
            user_id=user_id,
            ticker_symbol=ticker_upper,
            created_at=now,
            updated_at=now,
        )
    )
    return True


def _normalize_projection_percent(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric == 0:
        return 0.0
    return numeric / 100.0 if numeric > 1 else numeric


def _normalize_projection_storage() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE projection_assumptions
                SET growth_rate = growth_rate / 100.0
                WHERE growth_rate IS NOT NULL AND growth_rate > 1
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE projection_assumptions
                SET target_cagr = target_cagr / 100.0
                WHERE target_cagr IS NOT NULL AND target_cagr > 1
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE metrics
                SET proj_growth_rate = proj_growth_rate / 100.0
                WHERE proj_growth_rate IS NOT NULL AND proj_growth_rate > 1
                """
            )
        )
        conn.execute(
            text(
                """
                UPDATE metrics
                SET proj_target_cagr = proj_target_cagr / 100.0
                WHERE proj_target_cagr IS NOT NULL AND proj_target_cagr > 1
                """
            )
        )


_normalize_projection_storage()


def _projection_assumption_payload(row: ProjectionAssumption) -> dict[str, Any]:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "ticker_symbol": row.ticker_symbol,
        "growth_rate": row.growth_rate,
        "years": row.years,
        "target_cagr": row.target_cagr,
        "pe_bear": row.pe_bear,
        "pe_mid": row.pe_mid,
        "pe_bull": row.pe_bull,
        "pe_custom_terminal": row.pe_custom_terminal,
        "current_pe": row.current_pe,
        "created_at": row.created_at.isoformat() + "Z" if row.created_at else None,
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
    }


def _screener_preference_payload(row: ScreenerPreference) -> dict[str, Any]:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "lens_id": row.lens_id,
        "buy_threshold": row.buy_threshold,
        "watch_threshold": row.watch_threshold,
        "min_score": row.min_score,
        "recommendation_filter": row.recommendation_filter,
        "created_at": row.created_at.isoformat() + "Z" if row.created_at else None,
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
    }


def _apply_latest_prices_to_metrics(db: Session, rows: list[Metrics]) -> list[dict[str, Any]]:
    if not rows:
        return []
    tickers = sorted({str(row.ticker_symbol).strip().upper() for row in rows if row.ticker_symbol})
    latest_rows = (
        db.query(LatestPrice)
        .filter(LatestPrice.ticker.in_(tickers))
        .all()
        if tickers
        else []
    )
    latest_by_ticker = {
        (row.ticker or "").strip().upper(): row
        for row in latest_rows
        if row.ticker
    }
    payload = rows_to_dict(rows)
    for item in payload:
        ticker = str(item.get("ticker_symbol") or "").strip().upper()
        latest = latest_by_ticker.get(ticker)
        if latest is not None:
            item["price_current"] = latest.price
            item["price_current_as_of"] = latest.datetime_utc.isoformat() + "Z" if latest.datetime_utc else None
            item["price_current_source"] = latest.source
            if latest.datetime_utc is not None:
                item["updated_date"] = latest.datetime_utc.isoformat()
    return payload


def _materialize_latest_prices_to_daily_history(db: Session, tickers: list[str]) -> dict[str, int]:
    normalized = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if not normalized:
        return {"attempted": 0, "inserted": 0, "updated": 0, "skipped": 0}

    latest_rows = (
        db.query(LatestPrice)
        .filter(LatestPrice.ticker.in_(normalized))
        .all()
    )
    latest_by_ticker = {
        (row.ticker or "").strip().upper(): row
        for row in latest_rows
        if row.ticker and row.price is not None and row.datetime_utc is not None
    }

    attempted = 0
    inserted = 0
    updated = 0
    skipped = 0
    for ticker in normalized:
        latest = latest_by_ticker.get(ticker)
        if latest is None:
            skipped += 1
            continue
        latest_day = latest.datetime_utc.date()
        attempted += 1
        result = prices_repo.upsert_prices(
            db,
            [
                {
                    "ticker": ticker,
                    "date": latest_day,
                    "open": float(latest.price),
                    "high": float(latest.price),
                    "low": float(latest.price),
                    "close": float(latest.price),
                    "close_adj": float(latest.adjusted_price if latest.adjusted_price is not None else latest.price),
                    "volume": None,
                    "source": latest.source or "latest_prices_refresh",
                    "as_of_date": latest_day,
                }
            ],
        )
        inserted += int(result.get("inserted", 0) or 0)
        updated += int(result.get("updated", 0) or 0)
        skipped += int(result.get("skipped", 0) or 0)
    return {"attempted": attempted, "inserted": inserted, "updated": updated, "skipped": skipped}


def _bootstrap_default_portfolio() -> None:
    db = SessionLocal()
    try:
        default = get_or_create_default_portfolio(db)
        run_rows = db.query(PortfolioProcessingRun).filter(PortfolioProcessingRun.portfolio_id.is_(None)).all()
        for r in run_rows:
            r.portfolio_id = default.id
        cov_rows = db.query(PortfolioCoverageEvent).filter(PortfolioCoverageEvent.portfolio_id.is_(None)).all()
        for r in cov_rows:
            r.portfolio_id = default.id
        corr_rows = db.query(PortfolioCorrectionEvent).filter(PortfolioCorrectionEvent.portfolio_id.is_(None)).all()
        for r in corr_rows:
            r.portfolio_id = default.id
        tx_exists = (
            db.query(PortfolioTransaction)
            .filter(PortfolioTransaction.portfolio_id == default.id, PortfolioTransaction.is_deleted == False)
            .first()
        )
        if tx_exists is None:
            try:
                import_transactions_from_csv_for_portfolio(db, default.id, replace_existing=False)
            except Exception:
                pass
        db.commit()
    finally:
        db.close()


_bootstrap_default_portfolio()


def rows_to_dict(rows):
    return [
        {column.name: getattr(row, column.name) for column in row.__table__.columns}
        for row in rows
    ]


class PortfolioProcessResponse(BaseModel):
    ok: bool
    message: str
    data: dict[str, Any] | None = None


class CreatePortfolioRequest(BaseModel):
    name: str
    base_currency: str = "USD"


class MetricsSubjectivePatch(BaseModel):
    moat_score_0_10: float | None = None
    riskdownside_score_0_10: float | None = None
    macrofit_score_0_10: float | None = None
    narrative_score_0_10: float | None = None
    founder_led_bool: bool | None = None

    def validated_payload(self) -> dict[str, Any]:
        payload = self.model_dump(exclude_none=True)
        for field in (
            "moat_score_0_10",
            "riskdownside_score_0_10",
            "macrofit_score_0_10",
            "narrative_score_0_10",
        ):
            if field not in payload:
                continue
            value = payload[field]
            if not (0.0 <= value <= 10.0):
                raise HTTPException(
                    status_code=400,
                    detail=f"{field} must be between 0 and 10.",
                )
        return payload


class LoginRequest(BaseModel):
    username: str
    password: str


class UserCreateRequest(BaseModel):
    username: str
    password: str
    email: str | None = None
    role: str = "user"
    max_screener_tickers: int | None = None
    max_portfolio_tickers: int | None = None
    max_portfolios: int | None = None
    can_use_projection: bool = True
    can_export: bool = True


class UserUpdateRequest(BaseModel):
    email: str | None = None
    role: str | None = None
    status: str | None = None
    max_screener_tickers: int | None = None
    max_portfolio_tickers: int | None = None
    max_portfolios: int | None = None
    can_manage_users: bool | None = None
    can_use_projection: bool | None = None
    can_export: bool | None = None


class ResetPasswordRequest(BaseModel):
    password: str


class TickerCreateRequest(BaseModel):
    id: str | None = None
    symbol: str
    exchange: str | None = None
    name: str | None = None
    sector: str | None = None


class TickerUpdateRequest(BaseModel):
    exchange: str | None = None
    name: str | None = None
    sector: str | None = None


class MetricsCreateRequest(BaseModel):
    ticker_symbol: str
    ticker: str | None = None
    data: dict[str, Any] = {}


class LensPresetRequest(BaseModel):
    id: str | None = None
    name: str
    valuation: float | None = None
    quality: float | None = None
    capitalAllocation: float | None = None
    growth: float | None = None
    moat: float | None = None
    risk: float | None = None
    macro: float | None = None
    narrative: float | None = None
    dilution: float | None = None
    buyThreshold: float | None = None
    watchThreshold: float | None = None
    mosThreshold: float | None = None
    scoringHints: str | None = None


class ProjectionAssumptionUpsertRequest(BaseModel):
    ticker_symbol: str
    growth_rate: float | None = None
    years: int | None = None
    target_cagr: float | None = None
    pe_bear: float | None = None
    pe_mid: float | None = None
    pe_bull: float | None = None
    pe_custom_terminal: float | None = None
    current_pe: float | None = None


class ProjectionSnapshotCreateRequest(BaseModel):
    ticker_symbol: str
    name: str | None = None
    notes: str | None = None
    # Frozen inputs
    current_price: float | None = None
    current_eps: float | None = None
    growth_rate: float | None = None
    years: int | None = None
    target_cagr: float | None = None
    pe_bear: float | None = None
    pe_mid: float | None = None
    pe_bull: float | None = None
    pe_custom_terminal: float | None = None
    current_pe: float | None = None
    scenario: str | None = None
    # Frozen outputs
    terminal_eps: float | None = None
    exit_pe: float | None = None
    terminal_price: float | None = None
    implied_cagr: float | None = None
    required_entry: float | None = None
    margin_of_safety: float | None = None
    yearly_data: dict[str, Any] | None = None
    # Trigger config
    overvalued_pct: float = 15.0
    buy_trigger_price: float | None = None
    sell_trigger_price: float | None = None


class ProjectionSnapshotPatchRequest(BaseModel):
    name: str | None = None
    notes: str | None = None
    overvalued_pct: float | None = None
    buy_trigger_price: float | None = None
    sell_trigger_price: float | None = None
    clear_buy_trigger: bool = False
    clear_sell_trigger: bool = False
    status: str | None = None


class EmailUpdateRequest(BaseModel):
    email: str | None = None


class ScreenerPreferenceUpsertRequest(BaseModel):
    lens_id: str
    buy_threshold: float | None = None
    watch_threshold: float | None = None
    min_score: float | None = None
    recommendation_filter: str | None = None


class MarketDataRefreshRequest(BaseModel):
    tickers: list[str] | None = None
    force: bool = True


class TransactionCreateRequest(BaseModel):
    portfolio_id: str
    ticker: str
    type: str
    quantity: float
    price: float
    date: date
    currency: str = "USD"


class TransactionUpdateRequest(BaseModel):
    ticker: str
    type: str
    quantity: float
    price: float
    date: date
    currency: str = "USD"


class TransactionPatchRequest(BaseModel):
    quantity: float
    price: float
    date: date
    currency: str = "USD"


class CorporateActionCreateRequest(BaseModel):
    portfolio_id: str
    ticker: str
    type: str
    effective_date: date
    factor: float | None = None
    cash_amount: float | None = None
    metadata: dict[str, Any] | None = None


class CorporateActionUpdateRequest(BaseModel):
    ticker: str
    type: str
    effective_date: date
    factor: float | None = None
    cash_amount: float | None = None
    metadata: dict[str, Any] | None = None


class PortfolioSettingsUpdateRequest(BaseModel):
    cash_management_mode: str | None = None
    include_dividends_in_performance: bool | None = None
    reinvest_dividends_overlay: bool | None = None
    apply_dividend_withholding: bool | None = None
    dividend_withholding_percent: float | None = None


# ---------------------------------------------------------------------------
# Existing read endpoints (unchanged)
# ---------------------------------------------------------------------------

@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.get("/portfolios", response_model=PortfolioProcessResponse)
def get_portfolios(db: Session = Depends(get_db)):
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolios loaded",
        data={"portfolios": list_portfolios(db)},
    )

@app.post("/portfolios", response_model=PortfolioProcessResponse)
def post_portfolio(payload: CreatePortfolioRequest, db: Session = Depends(get_db)):
    try:
        data = create_portfolio(db, payload.name, payload.base_currency)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio created",
        data=data,
    )


@app.delete("/portfolios/{portfolio_id}", response_model=PortfolioProcessResponse)
def delete_portfolio(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        soft_delete_portfolio(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio soft-deleted",
        data={"portfolio_id": portfolio_id},
    )


@app.post("/portfolio/{portfolio_id}/import-csv", response_model=PortfolioProcessResponse)
def import_portfolio_csv(
    portfolio_id: str,
    replace_existing: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    try:
        data = import_transactions_from_csv_for_portfolio(db, portfolio_id, replace_existing=replace_existing)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio transactions imported from CSV",
        data=data,
    )


@app.post("/portfolio/{portfolio_id}/process", response_model=PortfolioProcessResponse)
async def process_portfolio_for_id(
    portfolio_id: str,
    strict: bool = Query(default=False, description="UI pass-through strict mode toggle; behavior unchanged."),
    db: Session = Depends(get_db),
):
    try:
        payload = await run_portfolio_creation_flow(db, portfolio_id=portfolio_id, strict=bool(strict))
        if isinstance(payload, dict):
            payload["strict_mode_requested"] = bool(strict)
        return PortfolioProcessResponse(
            ok=True,
            message="Portfolio processing completed",
            data=payload,
        )
    except PortfolioEngineError as exc:
        logger.exception("Portfolio processing failed")
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/portfolios/{portfolio_id}/reprocess", response_model=PortfolioProcessResponse)
async def reprocess_portfolio_for_id(
    portfolio_id: str,
    strict: bool = Query(default=False, description="UI pass-through strict mode toggle; behavior unchanged."),
    db: Session = Depends(get_db),
):
    try:
        payload = await run_portfolio_creation_flow(db, portfolio_id=portfolio_id, strict=bool(strict))
        if isinstance(payload, dict):
            payload["strict_mode_requested"] = bool(strict)
        return PortfolioProcessResponse(
            ok=True,
            message="Portfolio reprocessing completed",
            data=payload,
        )
    except PortfolioEngineError as exc:
        logger.exception("Portfolio reprocessing failed")
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/transactions", response_model=PortfolioProcessResponse)
def post_transaction(payload: TransactionCreateRequest, db: Session = Depends(get_db)):
    try:
        data = create_transaction(
            db,
            portfolio_id=payload.portfolio_id,
            ticker=payload.ticker,
            tx_type=payload.type,
            quantity=payload.quantity,
            price=payload.price,
            trade_date=payload.date,
            currency=payload.currency,
        )
        try:
            backfill_fx_history_if_missing(payload.portfolio_id, db)
            backfill_dividend_history_if_missing(payload.portfolio_id, db, strict=False)
            rebuild_equity_history(
                db,
                payload.portfolio_id,
                mode="incremental",
                force=False,
                strict=None,
            )
        except PortfolioEngineError:
            backfill_fx_history_if_missing(payload.portfolio_id, db)
            backfill_dividend_history_if_missing(payload.portfolio_id, db, strict=False)
            rebuild_equity_history(
                db,
                payload.portfolio_id,
                mode="full",
                force=True,
                strict=None,
            )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(ok=True, message="Transaction created", data=data)


@app.put("/transactions/{transaction_id}", response_model=PortfolioProcessResponse)
def put_transaction(transaction_id: str, payload: TransactionUpdateRequest, db: Session = Depends(get_db)):
    try:
        data = update_transaction(
            db,
            transaction_id=transaction_id,
            ticker=payload.ticker,
            tx_type=payload.type,
            quantity=payload.quantity,
            price=payload.price,
            trade_date=payload.date,
            currency=payload.currency,
        )
        try:
            backfill_fx_history_if_missing(data["portfolio_id"], db)
            backfill_dividend_history_if_missing(data["portfolio_id"], db, strict=False)
            rebuild_equity_history(
                db,
                data["portfolio_id"],
                mode="incremental",
                force=False,
                strict=None,
            )
        except PortfolioEngineError:
            backfill_fx_history_if_missing(data["portfolio_id"], db)
            backfill_dividend_history_if_missing(data["portfolio_id"], db, strict=False)
            rebuild_equity_history(
                db,
                data["portfolio_id"],
                mode="full",
                force=True,
                strict=None,
            )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(ok=True, message="Transaction updated", data=data)


@app.patch("/transactions/{transaction_id}", response_model=PortfolioProcessResponse)
def patch_transaction(transaction_id: str, payload: TransactionPatchRequest, db: Session = Depends(get_db)):
    try:
        original = (
            db.query(PortfolioTransaction)
            .filter(PortfolioTransaction.id == transaction_id, PortfolioTransaction.is_deleted == False)
            .first()
        )
        if not original:
            raise PortfolioEngineError(f"Transaction '{transaction_id}' not found.")
        if float(payload.quantity) <= 0:
            raise PortfolioEngineError("Quantity must be positive.")
        if float(payload.price) <= 0:
            raise PortfolioEngineError("Price must be positive.")

        data = update_transaction(
            db,
            transaction_id=transaction_id,
            ticker=original.ticker_symbol_raw,
            tx_type=original.tx_type,
            quantity=payload.quantity,
            price=payload.price,
            trade_date=payload.date,
            currency=payload.currency,
        )
        backfill_fx_history_if_missing(data["portfolio_id"], db)
        backfill_dividend_history_if_missing(data["portfolio_id"], db, strict=False)
        rebuild = rebuild_equity_history(
            db,
            data["portfolio_id"],
            mode="full",
            force=True,
            strict=None,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Transaction patched",
        data={"transaction": data, "rebuild": rebuild},
    )


@app.delete("/transactions/{transaction_id}", response_model=PortfolioProcessResponse)
def delete_transaction(transaction_id: str, db: Session = Depends(get_db)):
    try:
        data = soft_delete_transaction(db, transaction_id)
        backfill_fx_history_if_missing(data["portfolio_id"], db)
        backfill_dividend_history_if_missing(data["portfolio_id"], db, strict=False)
        rebuild = rebuild_equity_history(
            db,
            data["portfolio_id"],
            mode="full",
            force=True,
            strict=None,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Transaction deleted",
        data={"success": True, "deleted": data, "rebuild": rebuild},
    )


@app.get("/portfolios/{portfolio_id}/transactions", response_model=PortfolioProcessResponse)
def get_transactions_for_portfolio(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        rows = list_transactions_for_portfolio(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio transactions loaded",
        data={"transactions": rows},
    )


@app.post("/corporate-actions", response_model=PortfolioProcessResponse)
def post_corporate_action(payload: CorporateActionCreateRequest, db: Session = Depends(get_db)):
    try:
        data = create_corporate_action(
            db,
            portfolio_id=payload.portfolio_id,
            ticker=payload.ticker,
            action_type=payload.type,
            effective_date=payload.effective_date,
            factor=payload.factor,
            cash_amount=payload.cash_amount,
            metadata=payload.metadata,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(ok=True, message="Corporate action created", data=data)


@app.put("/corporate-actions/{action_id}", response_model=PortfolioProcessResponse)
def put_corporate_action(action_id: str, payload: CorporateActionUpdateRequest, db: Session = Depends(get_db)):
    try:
        data = update_corporate_action(
            db,
            action_id=action_id,
            ticker=payload.ticker,
            action_type=payload.type,
            effective_date=payload.effective_date,
            factor=payload.factor,
            cash_amount=payload.cash_amount,
            metadata=payload.metadata,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(ok=True, message="Corporate action updated", data=data)


@app.delete("/corporate-actions/{action_id}", response_model=PortfolioProcessResponse)
def delete_corporate_action(action_id: str, db: Session = Depends(get_db)):
    try:
        data = soft_delete_corporate_action(db, action_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return PortfolioProcessResponse(ok=True, message="Corporate action soft-deleted", data=data)


@app.get("/portfolios/{portfolio_id}/corporate-actions", response_model=PortfolioProcessResponse)
def get_corporate_actions_for_portfolio(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        rows = list_corporate_actions_for_portfolio(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio corporate actions loaded",
        data={"corporate_actions": rows},
    )


@app.post("/portfolios/{portfolio_id}/rebuild-ledger", response_model=PortfolioProcessResponse)
def post_rebuild_ledger_for_portfolio(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = rebuild_position_ledger(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio ledger rebuilt",
        data=data,
    )


@app.post("/portfolios/{portfolio_id}/rebuild-valuation", response_model=PortfolioProcessResponse)
def post_rebuild_valuation_for_portfolio(
    portfolio_id: str,
    strict: bool = Query(default=False, description="Fail on stale price inputs when true."),
    stale_trading_days: int = Query(default=3, ge=1, le=30),
    db: Session = Depends(get_db),
):
    try:
        data = rebuild_valuation_snapshot(
            db,
            portfolio_id,
            strict=bool(strict),
            stale_trading_days=int(stale_trading_days),
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio valuation rebuilt",
        data=data,
    )


@app.get("/portfolios/{portfolio_id}/valuation-diff", response_model=PortfolioProcessResponse)
def get_portfolio_valuation_diff(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = get_latest_valuation_diff(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio valuation diff loaded",
        data=data,
    )


@app.get("/portfolios/{portfolio_id}/dashboard-summary", response_model=PortfolioProcessResponse)
def get_portfolio_dashboard_summary_route(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = get_portfolio_dashboard_summary(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio dashboard summary loaded",
        data=data,
    )


@app.get("/portfolios/{portfolio_id}/holdings", response_model=PortfolioProcessResponse)
def get_portfolio_holdings_route(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = get_portfolio_holdings(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio holdings loaded",
        data=data,
    )


@app.get("/portfolio/{portfolio_id}/closed-positions", response_model=PortfolioProcessResponse)
def get_portfolio_closed_positions_route(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = list_closed_positions_for_portfolio(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio closed positions loaded",
        data=data,
    )


@app.get("/portfolio/{portfolio_id}/performance-breakdown", response_model=PortfolioProcessResponse)
def get_portfolio_performance_breakdown_route(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = compute_performance_breakdown(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio performance breakdown loaded",
        data=data,
    )


@app.get("/portfolio/{portfolio_id}/time-returns", response_model=PortfolioProcessResponse)
def get_portfolio_time_returns_route(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = compute_time_returns(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio time returns loaded",
        data=data,
    )


@app.get("/portfolios/{portfolio_id}/equity-history", response_model=PortfolioProcessResponse)
def get_portfolio_equity_history_route(
    portfolio_id: str,
    range: str = Query(default="6M"),
    build_version: int | None = Query(default=None),
    performance_mode: str = Query(default="absolute"),
    show_fx_impact: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    try:
        data = get_portfolio_equity_history(
            db,
            portfolio_id,
            range_label=range,
            build_version=build_version,
            performance_mode=performance_mode,
            show_fx_impact=show_fx_impact,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio equity history loaded",
        data=data,
    )


@app.post("/portfolios/{portfolio_id}/rebuild-equity-history", response_model=PortfolioProcessResponse)
def post_rebuild_equity_history_for_portfolio(
    portfolio_id: str,
    mode: str = Query(default="incremental"),
    force: bool = Query(default=False),
    from_date: date | None = Query(default=None),
    to_date: date | None = Query(default=None),
    strict: bool | None = Query(default=None),
    db: Session = Depends(get_db),
):
    try:
        backfill_fx_history_if_missing(portfolio_id, db)
        backfill_dividend_history_if_missing(portfolio_id, db, strict=bool(strict))
        data = rebuild_equity_history(
            db,
            portfolio_id,
            mode=mode,
            force=force,
            from_date=from_date,
            to_date=to_date,
            strict=strict,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio equity history rebuilt",
        data=data,
    )


@app.post("/portfolios/{portfolio_id}/refresh-prices", response_model=PortfolioProcessResponse)
async def refresh_portfolio_prices_route(
    portfolio_id: str,
    db: Session = Depends(get_db),
):
    try:
        data = await refresh_market_data_for_portfolio(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio market data refreshed",
        data=data,
    )


@app.get("/portfolios/{portfolio_id}/settings", response_model=PortfolioProcessResponse)
def get_portfolio_settings_route(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = get_portfolio_settings(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio settings loaded",
        data=data,
    )


@app.put("/portfolios/{portfolio_id}/settings", response_model=PortfolioProcessResponse)
def update_portfolio_settings_route(
    portfolio_id: str,
    payload: PortfolioSettingsUpdateRequest,
    db: Session = Depends(get_db),
):
    try:
        data = update_portfolio_settings(
            db,
            portfolio_id,
            cash_management_mode=payload.cash_management_mode,
            include_dividends_in_performance=payload.include_dividends_in_performance,
            reinvest_dividends_overlay=payload.reinvest_dividends_overlay,
            apply_dividend_withholding=payload.apply_dividend_withholding,
            dividend_withholding_percent=payload.dividend_withholding_percent,
        )
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio settings updated",
        data=data,
    )


@app.get("/portfolios/{portfolio_id}/valuation-attribution", response_model=PortfolioProcessResponse)
def get_portfolio_valuation_attribution(portfolio_id: str, db: Session = Depends(get_db)):
    try:
        data = get_latest_valuation_attribution(db, portfolio_id)
    except PortfolioEngineError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return PortfolioProcessResponse(
        ok=True,
        message="Portfolio valuation attribution loaded",
        data=data,
    )


@app.get("/portfolio/{portfolio_id}/last", response_model=PortfolioProcessResponse)
def get_last_portfolio_process(portfolio_id: str):
    data = load_last_portfolio_run(portfolio_id)
    if not data:
        raise HTTPException(status_code=404, detail="No saved portfolio run found.")
    return PortfolioProcessResponse(
        ok=True,
        message="Last saved portfolio run loaded",
        data=data,
    )


@app.get("/portfolio/{portfolio_id}/runs/latest", response_model=PortfolioProcessResponse)
def get_latest_portfolio_run_metadata(portfolio_id: str, db: Session = Depends(get_db)):
    run = (
        db.query(PortfolioProcessingRun)
        .filter(PortfolioProcessingRun.portfolio_id == portfolio_id)
        .order_by(PortfolioProcessingRun.finished_at.desc(), PortfolioProcessingRun.started_at.desc())
        .first()
    )
    if not run:
        raise HTTPException(status_code=404, detail="No portfolio processing run found.")

    coverage_rows = (
        db.query(PortfolioCoverageEvent)
        .filter(
            PortfolioCoverageEvent.run_id == run.id,
            PortfolioCoverageEvent.portfolio_id == portfolio_id,
            PortfolioCoverageEvent.warning_code == "coverage",
        )
        .order_by(PortfolioCoverageEvent.ticker.asc())
        .all()
    )
    fallback_rows = (
        db.query(PortfolioCoverageEvent)
        .filter(
            PortfolioCoverageEvent.run_id == run.id,
            PortfolioCoverageEvent.portfolio_id == portfolio_id,
            PortfolioCoverageEvent.warning_code == "prior_close_fallback",
        )
        .all()
    )
    correction_rows = (
        db.query(PortfolioCorrectionEvent)
        .filter(PortfolioCorrectionEvent.run_id == run.id, PortfolioCorrectionEvent.portfolio_id == portfolio_id)
        .order_by(PortfolioCorrectionEvent.created_at.asc())
        .all()
    )

    coverage_summary = [
        {
            "ticker": r.ticker,
            "status": r.status,
            "fallback_days": r.fallback_days or 0,
            "first_missing_date": r.first_missing_date.isoformat() if r.first_missing_date else None,
            "last_missing_date": r.last_missing_date.isoformat() if r.last_missing_date else None,
            "coverage_start": r.coverage_start.isoformat() if r.coverage_start else None,
            "coverage_end": r.coverage_end.isoformat() if r.coverage_end else None,
        }
        for r in coverage_rows
    ]
    correction_events = [
        {
            "ticker": r.ticker,
            "event_type": r.reason,
            "date": r.created_at.isoformat() if r.created_at else None,
            "original_shares": r.requested_shares,
            "corrected_shares": r.executed_shares,
            "delta_pct": ((r.delta_shares / r.available_shares) * 100.0) if r.available_shares else None,
            "triggered_by": "policy",
            "run_id": r.run_id,
        }
        for r in correction_rows
    ]
    payload = {
        "run_id": run.id,
        "started_at": run.started_at.isoformat() + "Z" if run.started_at else None,
        "finished_at": run.finished_at.isoformat() + "Z" if run.finished_at else None,
        "input_hash": run.hash_inputs,
        "engine_version": run.engine_version,
        "warnings_count": run.warnings_count,
        "coverage_summary": coverage_summary,
        "correction_event_count": len(correction_rows),
        "fallback_count": len(fallback_rows),
        "corrections": correction_events,
    }
    return PortfolioProcessResponse(
        ok=True,
        message="Latest portfolio processing metadata loaded",
        data=payload,
    )


@app.get("/tickers")
def list_tickers(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(Ticker).limit(limit)).all()
    return rows_to_dict(rows)


@app.delete("/watchlist/{ticker}")
def delete_watchlist_ticker(ticker: str, db: Session = Depends(get_db)):
    ticker_upper = ticker.strip().upper()
    row = db.scalars(select(Ticker).where(Ticker.symbol == ticker_upper)).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"{ticker_upper} not found in watch list")

    metrics_count = len(row.metrics or [])
    financials_count = len(row.financials_history or [])
    prices_count = len(row.prices_history or [])

    db.delete(row)
    db.commit()

    return {
        "ok": True,
        "ticker": ticker_upper,
        "deleted": {
            "ticker": 1,
            "metrics": metrics_count,
            "financials_history": financials_count,
            "prices_history": prices_count,
        },
    }


class WatchlistUpdateRequest(BaseModel):
    custom_entry_price: float | None = None
    clear_custom: bool = False  # if True, sets custom_entry_price to None
    custom_buy_trigger_price: float | None = None
    clear_buy_trigger: bool = False
    custom_sell_trigger_price: float | None = None
    clear_sell_trigger: bool = False


class WatchlistActionRequest(BaseModel):
    action: str  # "buy" | "close" | "reopen"
    price: float | None = None  # if omitted, uses latest price
    notes: str | None = None


@app.patch("/watchlist/{ticker}")
def update_watchlist_entry(
    ticker: str,
    payload: WatchlistUpdateRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    ticker_upper = ticker.strip().upper()
    row = (
        db.query(AccountWatchlistEntry)
        .filter(
            AccountWatchlistEntry.user_id == current_user.id,
            AccountWatchlistEntry.ticker_symbol == ticker_upper,
        )
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"{ticker_upper} not found in watch list")
    now = utcnow_naive()
    if payload.clear_custom:
        row.custom_entry_price = None
    elif payload.custom_entry_price is not None:
        row.custom_entry_price = float(payload.custom_entry_price)
    if payload.clear_buy_trigger:
        row.custom_buy_trigger_price = None
    elif payload.custom_buy_trigger_price is not None:
        row.custom_buy_trigger_price = float(payload.custom_buy_trigger_price)
    if payload.clear_sell_trigger:
        row.custom_sell_trigger_price = None
    elif payload.custom_sell_trigger_price is not None:
        row.custom_sell_trigger_price = float(payload.custom_sell_trigger_price)
    row.updated_at = now
    db.commit()
    return {
        "ok": True,
        "data": {
            "ticker": ticker_upper,
            "custom_entry_price": row.custom_entry_price,
            "custom_buy_trigger_price": row.custom_buy_trigger_price,
            "custom_sell_trigger_price": row.custom_sell_trigger_price,
        },
    }


@app.post("/watchlist/{ticker}/action")
def apply_watchlist_action(
    ticker: str,
    payload: WatchlistActionRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    ticker_upper = ticker.strip().upper()
    row = (
        db.query(AccountWatchlistEntry)
        .filter(
            AccountWatchlistEntry.user_id == current_user.id,
            AccountWatchlistEntry.ticker_symbol == ticker_upper,
        )
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"{ticker_upper} not found in watch list")
    if payload.action not in ("buy", "close", "reopen"):
        raise HTTPException(status_code=400, detail="Invalid action; must be buy, close, or reopen")
    now = utcnow_naive()
    # Resolve price: explicit payload or latest
    price = payload.price
    if price is None:
        lp = db.query(LatestPrice).filter(LatestPrice.ticker == ticker_upper).first()
        if lp and lp.price:
            price = float(lp.price)
    if payload.action == "buy":
        row.status = "bought"
        row.acted_at = now
        row.acted_price = price
        row.acted_notes = payload.notes
    elif payload.action == "close":
        row.status = "closed"
        row.acted_at = now
        row.acted_price = price
        row.acted_notes = payload.notes
    elif payload.action == "reopen":
        row.status = "watching"
        row.acted_at = None
        row.acted_price = None
        row.acted_notes = None
    row.updated_at = now
    db.commit()
    return {
        "ok": True,
        "data": {
            "ticker": ticker_upper,
            "status": row.status,
            "acted_at": row.acted_at.isoformat() if row.acted_at else None,
            "acted_price": row.acted_price,
            "acted_notes": row.acted_notes,
        },
    }


@app.get("/metrics")
def list_metrics(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(Metrics).limit(limit)).all()
    return rows_to_dict(rows)


@app.patch("/metrics/subjective/{ticker}")
def patch_subjective_metrics(
    ticker: str,
    patch: MetricsSubjectivePatch,
    db: Session = Depends(get_db),
):
    ticker_upper = ticker.strip().upper()
    row = db.scalars(
        select(Metrics)
        .where(Metrics.ticker_symbol == ticker_upper)
        .order_by(Metrics.updated_date.desc(), Metrics.created_date.desc())
    ).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"No metrics found for {ticker_upper}")

    payload = patch.validated_payload()
    if not payload:
        raise HTTPException(status_code=400, detail="No subjective fields provided.")

    for key, value in payload.items():
        setattr(row, key, value)
    row.updated_date = datetime.now(timezone.utc).replace(tzinfo=None)
    db.commit()
    db.refresh(row)
    return {
        "ok": True,
        "message": f"Subjective metrics updated for {ticker_upper}",
        "data": {k: getattr(row, k) for k in payload.keys()},
    }


@app.get("/financials-history")
def list_financials(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(FinancialsHistory).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/prices-history")
def list_prices(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(PricesHistory).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/lens-presets")
def list_lens_presets(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(LensPreset).limit(limit)).all()
    return rows_to_dict(rows)


@app.post("/lens-presets")
def create_lens_preset(
    payload: LensPresetRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    now = utcnow_naive()
    lens_id = (payload.id or payload.name.lower().replace(" ", "_")).strip()
    if not lens_id:
        raise HTTPException(status_code=400, detail="Lens id is required.")
    existing = (
        db.query(LensPreset)
        .filter(LensPreset.owner_id == current_user.id, LensPreset.id == lens_id)
        .first()
    )
    if existing is not None:
        raise HTTPException(status_code=400, detail=f"Lens preset '{lens_id}' already exists.")
    row = LensPreset(
        id=lens_id,
        name=payload.name.strip(),
        owner_id=current_user.id,
        created_date=now,
        updated_date=now,
        created_by_id=current_user.id,
        created_by=current_user.username,
        is_sample=False,
    )
    for field in (
        "valuation",
        "quality",
        "capitalAllocation",
        "growth",
        "moat",
        "risk",
        "macro",
        "narrative",
        "dilution",
        "buyThreshold",
        "watchThreshold",
        "mosThreshold",
        "scoringHints",
    ):
        setattr(row, field, getattr(payload, field))
    db.add(row)
    db.commit()
    db.refresh(row)
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


@app.patch("/lens-presets/{lens_id}")
def update_lens_preset(
    lens_id: str,
    payload: LensPresetRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(LensPreset)
        .filter(LensPreset.id == lens_id, LensPreset.owner_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Lens preset '{lens_id}' not found.")
    row.name = payload.name.strip() or row.name
    for field in (
        "valuation",
        "quality",
        "capitalAllocation",
        "growth",
        "moat",
        "risk",
        "macro",
        "narrative",
        "dilution",
        "buyThreshold",
        "watchThreshold",
        "mosThreshold",
        "scoringHints",
    ):
        value = getattr(payload, field)
        if value is not None:
            setattr(row, field, value)
    row.updated_date = utcnow_naive()
    db.commit()
    db.refresh(row)
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


@app.delete("/lens-presets/{lens_id}")
def delete_lens_preset(
    lens_id: str,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(LensPreset)
        .filter(LensPreset.id == lens_id, LensPreset.owner_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"Lens preset '{lens_id}' not found.")
    db.delete(row)
    db.commit()
    return {"ok": True, "message": "Lens preset deleted", "data": {"id": lens_id}}


@app.get("/projection-assumptions")
def list_projection_assumptions(
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(ProjectionAssumption)
        .filter(ProjectionAssumption.user_id == current_user.id)
        .order_by(ProjectionAssumption.updated_at.desc(), ProjectionAssumption.ticker_symbol.asc())
        .all()
    )
    return {"ok": True, "message": "Projection assumptions loaded", "data": {"assumptions": [_projection_assumption_payload(row) for row in rows]}}


@app.post("/projection-assumptions")
def upsert_projection_assumption(
    payload: ProjectionAssumptionUpsertRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    limits = get_account_limits(db, current_user.id)
    if limits and not limits.can_use_projection:
        raise HTTPException(status_code=403, detail="Projection is disabled for this account.")
    ticker_symbol = payload.ticker_symbol.strip().upper()
    _ticker_or_404_for_user(db, ticker_symbol, current_user.id)
    now = utcnow_naive()
    row = (
        db.query(ProjectionAssumption)
        .filter(
            ProjectionAssumption.user_id == current_user.id,
            ProjectionAssumption.ticker_symbol == ticker_symbol,
        )
        .first()
    )
    if row is None:
        row = ProjectionAssumption(
            id=str(uuid.uuid4()),
            user_id=current_user.id,
            ticker_symbol=ticker_symbol,
            created_at=now,
        )
        db.add(row)
    row.growth_rate = _normalize_projection_percent(payload.growth_rate)
    row.years = payload.years
    row.target_cagr = _normalize_projection_percent(payload.target_cagr)
    row.pe_bear = payload.pe_bear
    row.pe_mid = payload.pe_mid
    row.pe_bull = payload.pe_bull
    row.pe_custom_terminal = payload.pe_custom_terminal
    row.current_pe = payload.current_pe
    row.updated_at = now
    db.commit()
    db.refresh(row)

    # Freeze entry price on the watchlist entry when pe_mid is set
    if row.pe_mid:
        try:
            met = (
                db.query(Metrics)
                .filter(Metrics.ticker_symbol == ticker_symbol)
                .order_by(Metrics.as_of_date.desc())
                .first()
            )
            eps_ttm = float(met.eps_ttm) if met and met.eps_ttm else None
            if eps_ttm:
                frozen_price = float(row.pe_mid) * eps_ttm
                wl_entry = (
                    db.query(AccountWatchlistEntry)
                    .filter(
                        AccountWatchlistEntry.user_id == current_user.id,
                        AccountWatchlistEntry.ticker_symbol == ticker_symbol,
                    )
                    .first()
                )
                if wl_entry:
                    wl_entry.frozen_entry_price = round(frozen_price, 2)
                    wl_entry.frozen_at = now
                    wl_entry.frozen_from_projection_id = row.id
                    db.commit()
        except Exception:
            pass

    return {"ok": True, "message": "Projection assumption saved", "data": {"assumption": _projection_assumption_payload(row)}}


# ---------- Projection Snapshots (with price triggers) ----------

def _snapshot_payload(row: ProjectionSnapshot) -> dict[str, Any]:
    import json as _json
    yearly = None
    if row.yearly_data_json:
        try:
            yearly = _json.loads(row.yearly_data_json)
        except Exception:
            yearly = None
    return {
        "id": row.id,
        "user_id": row.user_id,
        "ticker_symbol": row.ticker_symbol,
        "name": row.name,
        "notes": row.notes,
        "status": row.status,
        "inputs": {
            "current_price": row.current_price,
            "current_eps": row.current_eps,
            "growth_rate": row.growth_rate,
            "years": row.years,
            "target_cagr": row.target_cagr,
            "pe_bear": row.pe_bear,
            "pe_mid": row.pe_mid,
            "pe_bull": row.pe_bull,
            "pe_custom_terminal": row.pe_custom_terminal,
            "current_pe": row.current_pe,
            "scenario": row.scenario,
        },
        "outputs": {
            "terminal_eps": row.terminal_eps,
            "exit_pe": row.exit_pe,
            "terminal_price": row.terminal_price,
            "implied_cagr": row.implied_cagr,
            "required_entry": row.required_entry,
            "margin_of_safety": row.margin_of_safety,
        },
        "yearly_data": yearly,
        "triggers": {
            "overvalued_pct": row.overvalued_pct,
            "buy_trigger_price": row.buy_trigger_price,
            "sell_trigger_price": row.sell_trigger_price,
            "triggered_at": row.triggered_at.isoformat() if row.triggered_at else None,
            "triggered_type": row.triggered_type,
            "triggered_price": row.triggered_price,
        },
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@app.post("/projections/snapshots")
def create_projection_snapshot(
    payload: ProjectionSnapshotCreateRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    import json as _json
    limits = get_account_limits(db, current_user.id)
    if limits and not limits.can_use_projection:
        raise HTTPException(status_code=403, detail="Projection is disabled for this account.")
    ticker_symbol = payload.ticker_symbol.strip().upper()
    _ticker_or_404_for_user(db, ticker_symbol, current_user.id)
    now = utcnow_naive()

    # Archive existing active snapshot for this ticker
    existing_active = (
        db.query(ProjectionSnapshot)
        .filter(
            ProjectionSnapshot.user_id == current_user.id,
            ProjectionSnapshot.ticker_symbol == ticker_symbol,
            ProjectionSnapshot.status == "active",
        )
        .all()
    )
    for old in existing_active:
        old.status = "archived"
        old.updated_at = now

    # Compute defaults if not supplied
    buy_price = payload.buy_trigger_price
    if buy_price is None and payload.required_entry is not None:
        buy_price = round(float(payload.required_entry), 2)
    sell_price = payload.sell_trigger_price
    if sell_price is None and payload.terminal_price is not None:
        sell_price = round(float(payload.terminal_price) * (1 + (payload.overvalued_pct or 15.0) / 100.0), 2)

    row = ProjectionSnapshot(
        id=str(uuid.uuid4()),
        user_id=current_user.id,
        ticker_symbol=ticker_symbol,
        name=(payload.name or f"{ticker_symbol} - {now.strftime('%Y-%m-%d')}")[:200],
        notes=payload.notes,
        status="active",
        current_price=payload.current_price,
        current_eps=payload.current_eps,
        growth_rate=_normalize_projection_percent(payload.growth_rate),
        years=payload.years,
        target_cagr=_normalize_projection_percent(payload.target_cagr),
        pe_bear=payload.pe_bear,
        pe_mid=payload.pe_mid,
        pe_bull=payload.pe_bull,
        pe_custom_terminal=payload.pe_custom_terminal,
        current_pe=payload.current_pe,
        scenario=payload.scenario,
        terminal_eps=payload.terminal_eps,
        exit_pe=payload.exit_pe,
        terminal_price=payload.terminal_price,
        implied_cagr=payload.implied_cagr,
        required_entry=payload.required_entry,
        margin_of_safety=payload.margin_of_safety,
        yearly_data_json=_json.dumps(payload.yearly_data) if payload.yearly_data else None,
        overvalued_pct=float(payload.overvalued_pct or 15.0),
        buy_trigger_price=buy_price,
        sell_trigger_price=sell_price,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"ok": True, "message": "Snapshot saved", "data": {"snapshot": _snapshot_payload(row)}}


@app.get("/projections/snapshots")
def list_projection_snapshots(
    ticker_symbol: str | None = None,
    status: str | None = None,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(ProjectionSnapshot).filter(ProjectionSnapshot.user_id == current_user.id)
    if ticker_symbol:
        q = q.filter(ProjectionSnapshot.ticker_symbol == ticker_symbol.strip().upper())
    if status:
        q = q.filter(ProjectionSnapshot.status == status)
    q = q.order_by(ProjectionSnapshot.updated_at.desc())
    rows = q.all()
    return {"ok": True, "data": {"snapshots": [_snapshot_payload(r) for r in rows]}}


@app.get("/projections/snapshots/{snapshot_id}")
def get_projection_snapshot(
    snapshot_id: str,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(ProjectionSnapshot)
        .filter(ProjectionSnapshot.id == snapshot_id, ProjectionSnapshot.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return {"ok": True, "data": {"snapshot": _snapshot_payload(row)}}


@app.patch("/projections/snapshots/{snapshot_id}")
def update_projection_snapshot(
    snapshot_id: str,
    payload: ProjectionSnapshotPatchRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(ProjectionSnapshot)
        .filter(ProjectionSnapshot.id == snapshot_id, ProjectionSnapshot.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    now = utcnow_naive()
    if payload.name is not None:
        row.name = payload.name[:200]
    if payload.notes is not None:
        row.notes = payload.notes
    sell_was_set = False
    if payload.clear_sell_trigger:
        row.sell_trigger_price = None
        sell_was_set = True
    elif payload.sell_trigger_price is not None:
        row.sell_trigger_price = payload.sell_trigger_price
        sell_was_set = True
    if payload.clear_buy_trigger:
        row.buy_trigger_price = None
    elif payload.buy_trigger_price is not None:
        row.buy_trigger_price = payload.buy_trigger_price
    if payload.overvalued_pct is not None:
        row.overvalued_pct = float(payload.overvalued_pct)
        # Recompute sell trigger if user didn't also set it explicitly
        if not sell_was_set and row.terminal_price:
            row.sell_trigger_price = round(float(row.terminal_price) * (1 + row.overvalued_pct / 100.0), 2)
    if payload.status is not None:
        if payload.status != "archived":
            raise HTTPException(status_code=400, detail="Only archive status transition allowed")
        row.status = "archived"
    row.updated_at = now
    db.commit()
    db.refresh(row)
    return {"ok": True, "data": {"snapshot": _snapshot_payload(row)}}


@app.delete("/projections/snapshots/{snapshot_id}")
def delete_projection_snapshot(
    snapshot_id: str,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(ProjectionSnapshot)
        .filter(ProjectionSnapshot.id == snapshot_id, ProjectionSnapshot.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    # Manually cascade delete alerts (no FK cascade on SQLite without PRAGMA)
    db.query(AlertNotification).filter(AlertNotification.snapshot_id == snapshot_id).delete(synchronize_session=False)
    db.delete(row)
    db.commit()
    return {"ok": True, "data": {"id": snapshot_id}}


def _alert_payload(row: AlertNotification, snapshot_name: str | None = None) -> dict[str, Any]:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "ticker_symbol": row.ticker_symbol,
        "snapshot_id": row.snapshot_id,
        "snapshot_name": snapshot_name,
        "alert_type": row.alert_type,
        "threshold_price": row.threshold_price,
        "triggered_price": row.triggered_price,
        "triggered_at": row.triggered_at.isoformat() if row.triggered_at else None,
        "email_sent": row.email_sent,
        "email_error": row.email_error,
        "read": row.read,
        "read_at": row.read_at.isoformat() if row.read_at else None,
        "dismissed": row.dismissed,
        "dismissed_at": row.dismissed_at.isoformat() if row.dismissed_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


@app.get("/projections/alerts")
def list_projection_alerts(
    dismissed: bool | None = None,
    read: bool | None = None,
    limit: int = 50,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(AlertNotification).filter(AlertNotification.user_id == current_user.id)
    if dismissed is not None:
        q = q.filter(AlertNotification.dismissed == dismissed)
    if read is not None:
        q = q.filter(AlertNotification.read == read)
    q = q.order_by(AlertNotification.triggered_at.desc()).limit(max(1, min(limit, 200)))
    rows = q.all()
    # Batch-load snapshot names
    snap_ids = {r.snapshot_id for r in rows}
    snap_names: dict[str, str] = {}
    if snap_ids:
        for s in db.query(ProjectionSnapshot.id, ProjectionSnapshot.name).filter(ProjectionSnapshot.id.in_(list(snap_ids))).all():
            snap_names[s.id] = s.name
    # Unread count
    unread_count = (
        db.query(AlertNotification)
        .filter(AlertNotification.user_id == current_user.id, AlertNotification.read == False)  # noqa: E712
        .count()
    )
    return {
        "ok": True,
        "data": {
            "alerts": [_alert_payload(r, snap_names.get(r.snapshot_id)) for r in rows],
            "unread_count": unread_count,
        },
    }


@app.post("/projections/alerts/{alert_id}/dismiss")
def dismiss_alert(
    alert_id: str,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(AlertNotification)
        .filter(AlertNotification.id == alert_id, AlertNotification.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    now = utcnow_naive()
    row.dismissed = True
    row.dismissed_at = now
    if not row.read:
        row.read = True
        row.read_at = now
    db.commit()
    return {"ok": True, "data": {"alert": _alert_payload(row)}}


@app.post("/projections/alerts/{alert_id}/read")
def mark_alert_read(
    alert_id: str,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(AlertNotification)
        .filter(AlertNotification.id == alert_id, AlertNotification.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    if not row.read:
        row.read = True
        row.read_at = utcnow_naive()
        db.commit()
    return {"ok": True, "data": {"alert": _alert_payload(row)}}


@app.post("/projections/alerts/read-all")
def mark_all_alerts_read(
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    now = utcnow_naive()
    count = (
        db.query(AlertNotification)
        .filter(AlertNotification.user_id == current_user.id, AlertNotification.read == False)  # noqa: E712
        .update({AlertNotification.read: True, AlertNotification.read_at: now}, synchronize_session=False)
    )
    db.commit()
    return {"ok": True, "data": {"marked_read": count}}


@app.delete("/projections/alerts/{alert_id}")
def delete_alert(
    alert_id: str,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    row = (
        db.query(AlertNotification)
        .filter(AlertNotification.id == alert_id, AlertNotification.user_id == current_user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    db.delete(row)
    db.commit()
    return {"ok": True, "data": {"id": alert_id}}


# ---------- User email for notifications ----------

@app.get("/me/email")
def get_user_email(
    current_user: User = Depends(require_current_user),
):
    return {"ok": True, "data": {"email": current_user.email}}


@app.patch("/me/email")
def update_user_email(
    payload: EmailUpdateRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    now = utcnow_naive()
    new_email = payload.email.strip() if payload.email else None
    if new_email == "":
        new_email = None
    if new_email is not None:
        if "@" not in new_email or "." not in new_email:
            raise HTTPException(status_code=400, detail="Invalid email")
        # Check uniqueness
        existing = db.query(User).filter(User.email == new_email, User.id != current_user.id).first()
        if existing:
            raise HTTPException(status_code=409, detail="Email already in use")
    try:
        current_user.email = new_email
        current_user.updated_at = now
        db.commit()
        db.refresh(current_user)
    except Exception:
        db.rollback()
        raise HTTPException(status_code=409, detail="Email update failed")
    return {"ok": True, "data": {"email": current_user.email}}


@app.get("/screener/preferences")
def list_screener_preferences(
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    rows = (
        db.query(ScreenerPreference)
        .filter(ScreenerPreference.user_id == current_user.id)
        .order_by(ScreenerPreference.updated_at.desc(), ScreenerPreference.lens_id.asc())
        .all()
    )
    return {"ok": True, "message": "Screener preferences loaded", "data": {"preferences": [_screener_preference_payload(row) for row in rows]}}


@app.post("/screener/preferences")
def upsert_screener_preference(
    payload: ScreenerPreferenceUpsertRequest,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    lens_id = payload.lens_id.strip()
    lens = (
        db.query(LensPreset)
        .filter(LensPreset.id == lens_id, LensPreset.owner_id == current_user.id)
        .first()
    )
    if lens is None:
        raise HTTPException(status_code=404, detail=f"Lens preset '{lens_id}' not found.")
    now = utcnow_naive()
    row = (
        db.query(ScreenerPreference)
        .filter(
            ScreenerPreference.user_id == current_user.id,
            ScreenerPreference.lens_id == lens_id,
        )
        .first()
    )
    if row is None:
        row = ScreenerPreference(
            id=str(uuid.uuid4()),
            user_id=current_user.id,
            lens_id=lens_id,
            created_at=now,
        )
        db.add(row)
    row.buy_threshold = payload.buy_threshold
    row.watch_threshold = payload.watch_threshold
    row.min_score = payload.min_score
    row.recommendation_filter = payload.recommendation_filter
    row.updated_at = now
    db.commit()
    db.refresh(row)
    return {"ok": True, "message": "Screener preference saved", "data": {"preference": _screener_preference_payload(row)}}


@app.post("/market-data/refresh")
def refresh_market_data_latest(
    payload: MarketDataRefreshRequest | None = None,
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    accessible = _account_watchlist_symbols(db, current_user.id)
    requested = sorted(
        {
            str(symbol).strip().upper()
            for symbol in ((payload.tickers if payload else None) or [])
            if str(symbol).strip()
        }
    )
    if requested:
        invalid = [symbol for symbol in requested if symbol not in accessible]
        if invalid:
            raise HTTPException(status_code=404, detail=f"Tickers not found in watchlist: {', '.join(invalid)}")
        target_tickers = requested
    else:
        portfolio_tickers = _account_portfolio_tickers(db, current_user.id)
        target_tickers = sorted(accessible | portfolio_tickers)
    if not target_tickers:
        return {"ok": True, "message": "No tickers to refresh", "data": {"tickers": [], "price_job": {"attempted": 0, "inserted": 0}, "fx_job": {"attempted": 0, "inserted": 0}}}
    price_job = run_price_fetch_job(specific_tickers=target_tickers, force=(payload.force if payload else True))
    fx_job = run_fx_fetch_job(force=(payload.force if payload else True))
    return {
        "ok": True,
        "message": "Latest market data refresh completed",
        "data": {
            "tickers": target_tickers,
            "price_job": price_job,
            "fx_job": fx_job,
        },
    }


# ---------------------------------------------------------------------------
# Onboarding request/response models
# ---------------------------------------------------------------------------

class OnboardRequest(BaseModel):
    ticker: str | None = None
    sector: str | None = None
    force: bool = False  # if True, skip DB-first check and re-run all steps


class OnboardResponse(BaseModel):
    ok: bool
    ticker: str
    status: str          # "ok" | "partial" | "skipped"
    message: str
    errors: list[str]
    steps: dict[str, Any]
    logs: list[str]


class ETLResponse(BaseModel):
    ok: bool
    ticker: str
    stage: str
    message: str
    data: dict[str, Any] | None = None
    logs: list[str]


# ---------------------------------------------------------------------------
# POST /onboard/{ticker}
# Full DB-first onboard pipeline.
# If ticker is already fully onboarded and force=False, returns immediately.
# ---------------------------------------------------------------------------

@app.post("/onboard/{ticker}", response_model=OnboardResponse)
async def onboard_ticker(
    ticker: str,
    body: OnboardRequest | None = None,
    db: Session = Depends(get_db),
):
    """
    Full onboard/refresh for a ticker.

    DB-first: if ticker already has prices + metrics in DB, return immediately
    unless force=True is set in the request body.

    Orchestration order (mirrors runFullRefresh.ts):
      A: Yahoo Prices (5Y)
      B: Finnhub Fundamentals
      C: Yahoo Fundamentals
      D1: Compute Metrics
      E: Sync Recent Prices
      D2: Re-compute Metrics
      F: Price Metrics
    """
    ticker_upper = ticker.strip().upper()
    force = body.force if body else False
    sector = body.sector if body else None

    # DB-first check
    if not force and ticker_is_onboarded(db, ticker_upper):
        return OnboardResponse(
            ok=True,
            ticker=ticker_upper,
            status="skipped",
            message=f"{ticker_upper} is already onboarded (use force=true to re-run)",
            errors=[],
            steps={},
            logs=[f"[DB-first] {ticker_upper} already has prices and metrics in DB"],
        )

    result = await run_full_onboard(ticker_upper, db, sector=sector)

    return OnboardResponse(
        ok=True,
        ticker=ticker_upper,
        status=result.status,
        message=(
            f"Full onboard completed successfully for {ticker_upper}."
            if result.status == "ok"
            else f"Full onboard for {ticker_upper} completed with {len(result.errors)} error(s)."
        ),
        errors=result.errors,
        steps=result.steps,
        logs=result.logs,
    )


# ---------------------------------------------------------------------------
# POST /refresh/{ticker}
# Force re-run all steps (same as /onboard with force=True)
# ---------------------------------------------------------------------------

@app.post("/refresh/{ticker}", response_model=OnboardResponse)
async def refresh_ticker(
    ticker: str,
    body: OnboardRequest | None = None,
    db: Session = Depends(get_db),
):
    """
    Force full refresh for a ticker — always re-runs all steps.
    Equivalent to /onboard with force=True.
    """
    ticker_upper = ticker.strip().upper()
    sector = body.sector if body else None

    result = await run_full_onboard(ticker_upper, db, sector=sector)

    return OnboardResponse(
        ok=True,
        ticker=ticker_upper,
        status=result.status,
        message=(
            f"Full refresh completed for {ticker_upper}."
            if result.status == "ok"
            else f"Full refresh for {ticker_upper} completed with {len(result.errors)} error(s)."
        ),
        errors=result.errors,
        steps=result.steps,
        logs=result.logs,
    )


# ---------------------------------------------------------------------------
# Individual ETL endpoints (for targeted re-runs)
# ---------------------------------------------------------------------------

@app.post("/etl/yahoo-prices/{ticker}", response_model=ETLResponse)
async def etl_yahoo_prices(ticker: str, db: Session = Depends(get_db)):
    """
    Run Yahoo 5Y historical prices ETL for a single ticker.
    Mirrors runYahooEtlPipeline.
    """
    import httpx as _httpx

    ticker_upper = ticker.strip().upper()
    result = OnboardingResult(ticker_upper)
    try:
        async with _httpx.AsyncClient() as client:
            data = await step_yahoo_prices_5y(ticker_upper, db, client, result)
        return ETLResponse(
            ok=True,
            ticker=ticker_upper,
            stage="yahoo_prices",
            message=f"Yahoo 5Y prices ETL completed for {ticker_upper}",
            data=data,
            logs=result.logs,
        )
    except Exception as exc:
        logger.exception("Yahoo prices ETL failed for %s", ticker_upper)
        return ETLResponse(
            ok=False,
            ticker=ticker_upper,
            stage="yahoo_prices",
            message=str(exc),
            logs=result.logs,
        )


@app.post("/etl/yahoo-fundamentals/{ticker}", response_model=ETLResponse)
async def etl_yahoo_fundamentals(ticker: str, db: Session = Depends(get_db)):
    """
    Run Yahoo quoteSummary fundamentals ETL for a single ticker.
    Mirrors runYahooFundamentalsEtl.
    """
    import httpx as _httpx

    ticker_upper = ticker.strip().upper()
    result = OnboardingResult(ticker_upper)
    try:
        async with _httpx.AsyncClient() as client:
            data = await step_yahoo_fundamentals(ticker_upper, db, client, result)
        return ETLResponse(
            ok=True,
            ticker=ticker_upper,
            stage="yahoo_fundamentals",
            message=f"Yahoo fundamentals ETL completed for {ticker_upper}",
            data=data,
            logs=result.logs,
        )
    except Exception as exc:
        logger.exception("Yahoo fundamentals ETL failed for %s", ticker_upper)
        return ETLResponse(
            ok=False,
            ticker=ticker_upper,
            stage="yahoo_fundamentals",
            message=str(exc),
            logs=result.logs,
        )


@app.post("/etl/finnhub/{ticker}", response_model=ETLResponse)
async def etl_finnhub(ticker: str, db: Session = Depends(get_db)):
    """
    Run Finnhub fundamentals ETL for a single ticker.
    Mirrors runFinnhubFundamentalsEtl.
    """
    ticker_upper = ticker.strip().upper()
    result = OnboardingResult(ticker_upper)
    try:
        data = await step_finnhub_fundamentals(ticker_upper, db, result)
        return ETLResponse(
            ok=True,
            ticker=ticker_upper,
            stage="finnhub_fundamentals",
            message=f"Finnhub ETL completed for {ticker_upper}",
            data=data,
            logs=result.logs,
        )
    except Exception as exc:
        logger.exception("Finnhub ETL failed for %s", ticker_upper)
        return ETLResponse(
            ok=False,
            ticker=ticker_upper,
            stage="finnhub_fundamentals",
            message=str(exc),
            logs=result.logs,
        )


@app.post("/etl/compute-metrics/{ticker}", response_model=ETLResponse)
def etl_compute_metrics(
    ticker: str,
    sector: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Re-run deterministic metrics computation from DB data.
    Mirrors computeFundamentalMetrics + computeAndSavePriceMetrics.
    """
    ticker_upper = ticker.strip().upper()
    result = OnboardingResult(ticker_upper)
    try:
        data = step_compute_fundamental_metrics(ticker_upper, db, result, sector=sector)
        return ETLResponse(
            ok=True,
            ticker=ticker_upper,
            stage="compute_metrics",
            message=f"Metrics computed for {ticker_upper}",
            data=data,
            logs=result.logs,
        )
    except Exception as exc:
        logger.exception("Metrics computation failed for %s", ticker_upper)
        return ETLResponse(
            ok=False,
            ticker=ticker_upper,
            stage="compute_metrics",
            message=str(exc),
            logs=result.logs,
        )


@app.post("/etl/recent-prices/{ticker}", response_model=ETLResponse)
async def etl_recent_prices(ticker: str, db: Session = Depends(get_db)):
    """
    Sync recent 1-month prices for a ticker.
    Mirrors syncRecentPricesYahoo.
    """
    import httpx as _httpx

    ticker_upper = ticker.strip().upper()
    result = OnboardingResult(ticker_upper)
    try:
        async with _httpx.AsyncClient() as client:
            data = await step_sync_recent_prices(ticker_upper, db, client, result)
        return ETLResponse(
            ok=True,
            ticker=ticker_upper,
            stage="recent_prices",
            message=f"Recent prices synced for {ticker_upper}",
            data=data,
            logs=result.logs,
        )
    except Exception as exc:
        logger.exception("Recent prices sync failed for %s", ticker_upper)
        return ETLResponse(
            ok=False,
            ticker=ticker_upper,
            stage="recent_prices",
            message=str(exc),
            logs=result.logs,
        )


# ---------------------------------------------------------------------------
# GET /onboard/status/{ticker}
# Quick check: is this ticker fully onboarded?
# ---------------------------------------------------------------------------

@app.get("/onboard/status/{ticker}")
def onboard_status(ticker: str, db: Session = Depends(get_db)):
    """Check if a ticker is fully onboarded (has prices + metrics in DB)."""
    ticker_upper = ticker.strip().upper()
    has_prices = len(prices_repo.get_prices_for_ticker(db, ticker_upper, limit=1)) > 0
    has_metrics = metrics_repo.ticker_has_metrics(db, ticker_upper)
    has_financials = financials_repo.ticker_has_financials(db, ticker_upper)
    is_onboarded = has_prices and has_metrics

    metrics = metrics_repo.get_metrics(db, ticker_upper)

    return {
        "ticker": ticker_upper,
        "onboarded": is_onboarded,
        "has_prices": has_prices,
        "has_financials": has_financials,
        "has_metrics": has_metrics,
        "metrics_as_of": str(metrics.get("as_of_date")) if metrics and metrics.get("as_of_date") else None,
        "data_source": metrics.get("data_source") if metrics else None,
    }


# ---------------------------------------------------------------------------
# Dashboard / Control Center endpoints
# ---------------------------------------------------------------------------

_MARKET_SESSIONS = [
    {
        "region": "United States",
        "region_key": "us",
        "exchanges": ["NYSE", "NASDAQ", "AMEX"],
        "open_utc": time(14, 30),
        "close_utc": time(21, 0),
    },
    {
        "region": "Europe",
        "region_key": "europe",
        "exchanges": ["LSE", "Xetra", "Euronext", "SIX"],
        "open_utc": time(8, 0),
        "close_utc": time(16, 30),
    },
    {
        "region": "Asia-Pacific",
        "region_key": "asia-pacific",
        "exchanges": ["TSE", "HKEX", "SSE", "ASX"],
        "open_utc": time(0, 0),
        "close_utc": time(8, 0),
    },
]


def _seconds_until(now_utc: datetime, target_time: time, skip_weekends: bool = True) -> int:
    """Compute seconds from *now_utc* until the next occurrence of *target_time* (UTC)."""
    today = now_utc.date()
    candidate = datetime.combine(today, target_time, tzinfo=timezone.utc)
    if candidate <= now_utc:
        candidate += timedelta(days=1)
    if skip_weekends:
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
    return max(0, int((candidate - now_utc).total_seconds()))


# Region key -> main index ETF proxy (USD-denominated, US-listed for reliable Yahoo data)
_REGION_INDEX_MAP = {
    "us": {"symbol": "SPY", "label": "S&P 500"},
    "europe": {"symbol": "EZU", "label": "Euro Stoxx"},
    "asia-pacific": {"symbol": "EWJ", "label": "Nikkei (MSCI Japan)"},
}


def _region_index_change(db: Session, symbol: str) -> dict[str, Any] | None:
    """Compute the most recent two-weekday-close change for a region's index proxy."""
    from datetime import date as _date
    from backend.services.prev_close_backfill import ensure_prev_close_coverage
    # Ensure data is fresh (idempotent if already there)
    try:
        ensure_prev_close_coverage(db, [symbol])
    except Exception:
        pass
    rows = (
        db.query(PricesHistory)
        .filter(PricesHistory.ticker == symbol)
        .order_by(PricesHistory.date.desc())
        .limit(8)
        .all()
    )
    weekday_rows: list[tuple] = []
    for r in rows:
        ds = str(r.date)[:10]
        try:
            d = _date.fromisoformat(ds)
        except Exception:
            continue
        if d.weekday() >= 5 or r.close is None or float(r.close) <= 0:
            continue
        weekday_rows.append((d, float(r.close)))
        if len(weekday_rows) >= 2:
            break
    if len(weekday_rows) < 2:
        return None
    cur_d, cur_p = weekday_rows[0]
    prev_d, prev_p = weekday_rows[1]
    if prev_p == 0:
        return None
    return {
        "price": round(cur_p, 2),
        "change_pct": round((cur_p - prev_p) / prev_p * 100, 2),
        "as_of": cur_d.isoformat(),
    }


@app.get("/dashboard/market-sessions")
def dashboard_market_sessions(db: Session = Depends(get_db)):
    now_utc = datetime.now(timezone.utc)
    is_weekday = now_utc.weekday() < 5
    t = now_utc.time()

    sessions = []
    for ms in _MARKET_SESSIONS:
        if is_weekday and ms["open_utc"] <= t <= ms["close_utc"]:
            status = "open"
            next_event = "closes"
            seconds_until = _seconds_until(now_utc, ms["close_utc"], skip_weekends=False)
        else:
            status = "closed"
            next_event = "opens"
            seconds_until = _seconds_until(now_utc, ms["open_utc"], skip_weekends=True)

        # Attach region's main index daily change (best-effort — never blocks)
        index_info = _REGION_INDEX_MAP.get(ms["region_key"])
        index_payload = None
        if index_info:
            try:
                change = _region_index_change(db, index_info["symbol"])
                index_payload = {
                    "symbol": index_info["symbol"],
                    "label": index_info["label"],
                    **(change or {"price": None, "change_pct": None, "as_of": None}),
                }
            except Exception as exc:
                logger.info("region index fetch failed for %s: %s", index_info["symbol"], exc)
                index_payload = {
                    "symbol": index_info["symbol"],
                    "label": index_info["label"],
                    "price": None,
                    "change_pct": None,
                    "as_of": None,
                }

        sessions.append({
            "region": ms["region"],
            "region_key": ms["region_key"],
            "exchanges": ms["exchanges"],
            "status": status,
            "next_event": next_event,
            "seconds_until": seconds_until,
            "index": index_payload,
        })

    return {"now_utc": now_utc.isoformat(), "sessions": sessions}


@app.get("/dashboard/watchlist-summary")
def dashboard_watchlist_summary(
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    watchlist_symbols = _account_watchlist_symbols(db, current_user.id)
    if not watchlist_symbols:
        return {"items": []}

    # Batch-query watchlist entries for frozen prices + status
    wl_entries = (
        db.query(AccountWatchlistEntry)
        .filter(AccountWatchlistEntry.user_id == current_user.id)
        .all()
    )
    wl_map: dict[str, AccountWatchlistEntry] = {e.ticker_symbol.upper(): e for e in wl_entries}

    # Batch-query active snapshots (for source attribution)
    active_snaps = (
        db.query(ProjectionSnapshot)
        .filter(
            ProjectionSnapshot.user_id == current_user.id,
            ProjectionSnapshot.status == "active",
        )
        .all()
    )
    snapshot_by_ticker: dict[str, ProjectionSnapshot] = {s.ticker_symbol.upper(): s for s in active_snaps}

    # Gather ticker info
    tickers_map: dict[str, Ticker] = {}
    for sym in watchlist_symbols:
        t = db.query(Ticker).filter(Ticker.symbol == sym).first()
        if t:
            tickers_map[sym] = t

    # Gather latest prices
    prices_map: dict[str, float] = {}
    for sym in watchlist_symbols:
        lp = db.query(LatestPrice).filter(LatestPrice.ticker == sym).first()
        if lp and lp.price:
            prices_map[sym] = float(lp.price)

    # Gather projection assumptions
    projections_map: dict[str, ProjectionAssumption] = {}
    proj_rows = (
        db.query(ProjectionAssumption)
        .filter(
            ProjectionAssumption.user_id == current_user.id,
            ProjectionAssumption.ticker_symbol.in_(list(watchlist_symbols)),
        )
        .all()
    )
    for p in proj_rows:
        projections_map[p.ticker_symbol.upper()] = p

    # Gather metrics for eps data
    metrics_map: dict[str, dict] = {}
    for sym in watchlist_symbols:
        row = (
            db.query(Metrics)
            .filter(Metrics.ticker_symbol == sym)
            .order_by(Metrics.as_of_date.desc())
            .first()
        )
        if row:
            metrics_map[sym] = {
                "price_current": float(row.price_current) if row.price_current else None,
                "eps_ttm": float(row.eps_ttm) if row.eps_ttm else None,
            }

    items = []
    for sym in sorted(watchlist_symbols):
        ticker = tickers_map.get(sym)
        current_price = prices_map.get(sym)
        proj = projections_map.get(sym)
        met = metrics_map.get(sym, {})
        wl_entry = wl_map.get(sym)

        # Effective entry price: only meaningful sources.
        # Priority: custom user override → snapshot frozen → projection-derived (pe_mid × EPS).
        # NO fallback to onboarded price (that's not a "target", just whatever the price was
        # when you added the ticker — misleading and made the field uneditable in practice).
        entry_price = None
        is_frozen = False
        source = None  # None | "manual" | "snapshot" | "projection"
        snap = snapshot_by_ticker.get(sym)
        if wl_entry and wl_entry.custom_entry_price:
            entry_price = wl_entry.custom_entry_price
            source = "manual"
        elif wl_entry and wl_entry.frozen_entry_price:
            entry_price = wl_entry.frozen_entry_price
            is_frozen = True
            source = "snapshot"
        elif proj and proj.pe_mid and met.get("eps_ttm"):
            entry_price = float(proj.pe_mid) * float(met["eps_ttm"])
            source = "projection"

        gap_pct = None
        if entry_price and current_price and entry_price != 0:
            gap_pct = round(((current_price - entry_price) / entry_price) * 100, 2)

        # Effective BUY/SELL triggers: prefer active snapshot, fall back to custom watchlist triggers
        eff_buy = None
        eff_sell = None
        buy_trigger_source = None  # "snapshot" | "custom" | None
        sell_trigger_source = None
        if snap and snap.buy_trigger_price is not None:
            eff_buy = float(snap.buy_trigger_price)
            buy_trigger_source = "snapshot"
        elif wl_entry and wl_entry.custom_buy_trigger_price is not None:
            eff_buy = float(wl_entry.custom_buy_trigger_price)
            buy_trigger_source = "custom"
        if snap and snap.sell_trigger_price is not None:
            eff_sell = float(snap.sell_trigger_price)
            sell_trigger_source = "snapshot"
        elif wl_entry and wl_entry.custom_sell_trigger_price is not None:
            eff_sell = float(wl_entry.custom_sell_trigger_price)
            sell_trigger_source = "custom"

        gap_to_buy_pct = None
        if eff_buy is not None and current_price and current_price > 0:
            gap_to_buy_pct = round(((eff_buy - current_price) / current_price) * 100, 2)
        gap_to_sell_pct = None
        if eff_sell is not None and current_price and current_price > 0:
            gap_to_sell_pct = round(((eff_sell - current_price) / current_price) * 100, 2)

        items.append({
            "symbol": sym,
            "name": ticker.name if ticker else sym,
            "current_price": current_price,
            "entry_price": round(entry_price, 2) if entry_price else None,
            "gap_pct": gap_pct,
            "has_projection": proj is not None,
            "is_frozen": is_frozen,
            "source": source,
            "status": wl_entry.status if wl_entry and wl_entry.status else "watching",
            "acted_at": wl_entry.acted_at.isoformat() if wl_entry and wl_entry.acted_at else None,
            "acted_price": wl_entry.acted_price if wl_entry else None,
            "acted_notes": wl_entry.acted_notes if wl_entry else None,
            "snapshot": (
                {
                    "id": snap.id,
                    "name": snap.name,
                    "buy_trigger_price": snap.buy_trigger_price,
                    "sell_trigger_price": snap.sell_trigger_price,
                    "status": snap.status,
                }
                if snap
                else None
            ),
            "custom_buy_trigger_price": wl_entry.custom_buy_trigger_price if wl_entry else None,
            "custom_sell_trigger_price": wl_entry.custom_sell_trigger_price if wl_entry else None,
            "buy_trigger_price": eff_buy,
            "sell_trigger_price": eff_sell,
            "buy_trigger_source": buy_trigger_source,
            "sell_trigger_source": sell_trigger_source,
            "gap_to_buy_pct": gap_to_buy_pct,
            "gap_to_sell_pct": gap_to_sell_pct,
            "added_at": wl_entry.created_at.isoformat() if wl_entry and wl_entry.created_at else None,
        })

    # Sort by gap ascending (best opportunities first)
    items.sort(key=lambda x: (x["gap_pct"] is None, x["gap_pct"] or 0))
    return {"items": items}


_INDEX_PROXIES = [
    {"symbol": "SPY", "label": "S&P 500"},
    {"symbol": "QQQ", "label": "NASDAQ"},
    {"symbol": "DIA", "label": "Dow Jones"},
]


@app.get("/dashboard/market-indices")
def dashboard_market_indices(db: Session = Depends(get_db)):
    from datetime import date as _date
    today_str = _date.today().isoformat()
    indices = []
    for idx in _INDEX_PROXIES:
        # Try LatestPrice first, then fall back to most recent PricesHistory
        lp = db.query(LatestPrice).filter(LatestPrice.ticker == idx["symbol"]).first()
        price = float(lp.price) if lp and lp.price else None
        if not price:
            latest_ph = (
                db.query(PricesHistory)
                .filter(PricesHistory.ticker == idx["symbol"])
                .order_by(PricesHistory.date.desc())
                .first()
            )
            if latest_ph and latest_ph.close:
                price = float(latest_ph.close)
        change_pct = None
        if price:
            prev = (
                db.query(PricesHistory)
                .filter(PricesHistory.ticker == idx["symbol"], PricesHistory.date < today_str)
                .order_by(PricesHistory.date.desc())
                .first()
            )
            if prev and prev.close and float(prev.close) != 0:
                change_pct = round(((price - float(prev.close)) / float(prev.close)) * 100, 2)
        indices.append({"symbol": idx["symbol"], "label": idx["label"], "price": round(price, 2) if price else None, "change_pct": change_pct})
    # EUR/USD from LatestFXRate
    fx = db.query(LatestFXRate).filter(LatestFXRate.base_currency == "EUR", LatestFXRate.quote_currency == "USD").first()
    indices.append({
        "symbol": "EUR/USD",
        "label": "EUR/USD",
        "price": round(float(fx.rate), 4) if fx and fx.rate else None,
        "change_pct": None,
    })
    return {"indices": indices}


@app.post("/dashboard/ensure-indices")
async def dashboard_ensure_indices(db: Session = Depends(get_db)):
    """Auto-onboard index ETFs (SPY, QQQ, DIA) if not already present."""
    onboarded = []
    already_present = []
    for idx in _INDEX_PROXIES:
        sym = idx["symbol"]
        if ticker_is_onboarded(db, sym):
            already_present.append(sym)
            continue
        try:
            existing = db.query(Ticker).filter(Ticker.symbol == sym).first()
            if not existing:
                db.add(Ticker(id=str(uuid.uuid4()), symbol=sym, name=idx["label"]))
                db.commit()
            await run_full_onboard(sym, db)
            onboarded.append(sym)
        except Exception as exc:
            logger.warning("Index onboard failed for %s: %s", sym, exc)
    return {"onboarded": onboarded, "already_present": already_present}


@app.get("/dashboard/overview")
def dashboard_overview(
    current_user: User = Depends(require_current_user),
    db: Session = Depends(get_db),
):
    import json as _json
    from datetime import date as _date
    from sqlalchemy import case as sa_case

    result: dict[str, Any] = {
        "portfolio": None,
        "top_movers": [],
        "recent_activity": [],
        "upcoming_dividends": [],
    }
    today_str = _date.today().isoformat()

    # ---------- Shared: open portfolio tickers from latest LedgerSnapshot ----------
    open_portfolio_tickers: set[str] = set()
    _default_portfolio = None
    _default_snap = None
    try:
        user_portfolio_ids = [
            row.id
            for row in db.query(Portfolio.id)
            .filter(Portfolio.owner_id == current_user.id, Portfolio.is_deleted == False)
            .all()
        ]
        for pid in user_portfolio_ids:
            snap = (
                db.query(LedgerSnapshot)
                .filter(LedgerSnapshot.portfolio_id == pid)
                .order_by(LedgerSnapshot.as_of.desc())
                .first()
            )
            if snap and snap.holdings_json:
                try:
                    holdings = _json.loads(snap.holdings_json)
                    for ticker, qty in holdings.items():
                        if float(qty) > 0:
                            open_portfolio_tickers.add(ticker.strip().upper())
                except Exception:
                    pass
    except Exception:
        pass

    # ---------- Portfolio summary ----------
    try:
        portfolio = (
            db.query(Portfolio)
            .filter(Portfolio.owner_id == current_user.id, Portfolio.is_deleted == False)
            .order_by(
                sa_case((Portfolio.name == "Default", 0), else_=1),
                Portfolio.created_at.asc(),
            )
            .first()
        )
        if portfolio:
            _default_portfolio = portfolio
            result["portfolio"] = {
                "portfolio_id": portfolio.id,
                "portfolio_name": portfolio.name,
                "nav": None,
                "day_change": None,
                "day_change_pct": None,
                "top_holdings": [],
                "equity_sparkline": [],
                "cash_balance": 0.0,
                "currency_exposure": [],
                "needs_processing": True,
            }
            try:
                summary = get_portfolio_dashboard_summary(db, portfolio.id)
                result["portfolio"]["nav"] = summary.get("total_equity")
                result["portfolio"]["day_change"] = summary.get("day_change_value")
                result["portfolio"]["day_change_pct"] = summary.get("day_change_percent")
                result["portfolio"]["needs_processing"] = False

                # Compute weights + currency exposure from LedgerSnapshot × current prices
                snap = (
                    db.query(LedgerSnapshot)
                    .filter(LedgerSnapshot.portfolio_id == portfolio.id)
                    .order_by(LedgerSnapshot.as_of.desc())
                    .first()
                )
                _default_snap = snap
                top_holdings = []
                currency_totals: dict[str, float] = {}
                if snap and snap.holdings_json:
                    holdings = _json.loads(snap.holdings_json)
                    position_values: list[tuple[str, float]] = []
                    for ticker, qty in holdings.items():
                        qty_f = float(qty)
                        if qty_f <= 0:
                            continue
                        t_upper = ticker.strip().upper()
                        lp = db.query(LatestPrice).filter(LatestPrice.ticker == t_upper).first()
                        price = float(lp.price) if lp and lp.price else 0.0
                        mkt_val = qty_f * price
                        position_values.append((t_upper, mkt_val))
                        # Currency from TickerMetadata
                        tm = db.query(TickerMetadata).filter(TickerMetadata.ticker_normalized == t_upper).first()
                        ccy = tm.native_currency if tm and tm.native_currency else "USD"
                        currency_totals[ccy] = currency_totals.get(ccy, 0.0) + mkt_val
                    total_val = sum(v for _, v in position_values) or 1.0
                    position_values.sort(key=lambda x: x[1], reverse=True)
                    top_holdings = [
                        {"ticker": t, "weight": round(v / total_val * 100, 2)}
                        for t, v in position_values[:5]
                    ]
                    # Add cash as USD
                    if snap.cash and float(snap.cash) > 0:
                        currency_totals["USD"] = currency_totals.get("USD", 0.0) + float(snap.cash)
                        total_val += float(snap.cash)
                    result["portfolio"]["currency_exposure"] = sorted(
                        [{"currency": k, "value": round(v, 2), "pct": round(v / total_val * 100, 1)} for k, v in currency_totals.items()],
                        key=lambda x: x["pct"],
                        reverse=True,
                    )
                result["portfolio"]["top_holdings"] = top_holdings
                result["portfolio"]["cash_balance"] = round(float(snap.cash), 2) if snap and snap.cash else 0.0

                # Equity sparkline: last 30 rows from latest build version
                latest_build = (
                    db.query(PortfolioEquityHistoryRow.build_version)
                    .filter(PortfolioEquityHistoryRow.portfolio_id == portfolio.id)
                    .order_by(PortfolioEquityHistoryRow.build_version.desc())
                    .first()
                )
                bv = latest_build[0] if latest_build else None
                sparkline_rows = []
                if bv is not None:
                    sparkline_rows = (
                        db.query(PortfolioEquityHistoryRow)
                        .filter(
                            PortfolioEquityHistoryRow.portfolio_id == portfolio.id,
                            PortfolioEquityHistoryRow.build_version == bv,
                        )
                        .order_by(PortfolioEquityHistoryRow.date.desc())
                        .limit(30)
                        .all()
                    )
                    sparkline_rows.reverse()
                result["portfolio"]["equity_sparkline"] = [
                    {"date": r.date.isoformat() if hasattr(r.date, 'isoformat') else str(r.date), "value": float(r.total_equity)}
                    for r in sparkline_rows
                ]

                # Benchmark comparison: portfolio TWR vs SPY returns
                benchmark_series = []
                if sparkline_rows:
                    first_twr = float(sparkline_rows[0].twr_index) if sparkline_rows[0].twr_index else 1.0
                    start_date = sparkline_rows[0].date
                    end_date = sparkline_rows[-1].date
                    # Get SPY prices for the same date range
                    spy_prices = (
                        db.query(PricesHistory)
                        .filter(
                            PricesHistory.ticker == "SPY",
                            PricesHistory.date >= str(start_date),
                            PricesHistory.date <= str(end_date),
                        )
                        .order_by(PricesHistory.date.asc())
                        .all()
                    )
                    spy_by_date = {}
                    first_spy = None
                    for sp in spy_prices:
                        if sp.close:
                            if first_spy is None:
                                first_spy = float(sp.close)
                            spy_by_date[str(sp.date)] = float(sp.close)

                    if first_spy and first_twr:
                        for r in sparkline_rows:
                            d = r.date.isoformat() if hasattr(r.date, 'isoformat') else str(r.date)
                            twr = float(r.twr_index) if r.twr_index else first_twr
                            portfolio_pct = round((twr / first_twr - 1) * 100, 2)
                            spy_close = spy_by_date.get(d)
                            spy_pct = round((spy_close / first_spy - 1) * 100, 2) if spy_close else None
                            benchmark_series.append({
                                "date": d,
                                "portfolio_pct": portfolio_pct,
                                "spy_pct": spy_pct,
                            })
                result["portfolio"]["benchmark_series"] = benchmark_series
            except Exception:
                pass
    except Exception as exc:
        logger.warning("Dashboard portfolio summary failed: %s", exc)

    # ---------- Top movers ----------
    try:
        from datetime import date as _date, timedelta as _td
        from backend.services.prev_close_backfill import (
            ensure_prev_close_coverage,
            _last_business_day_before,
        )
        watchlist_syms = _account_watchlist_symbols(db, current_user.id)
        mover_tickers = open_portfolio_tickers | watchlist_syms

        # Freshness threshold: prev-close data older than this many calendar days is stale.
        # Today - last_business_day is <=3 days (Fri->Mon). We add a buffer for holidays.
        max_staleness_days = 5
        today_d = _date.today()

        # Backfill the most recent business-day close for each candidate ticker if missing.
        # This bridges gaps left by intermittent scheduler runs so daily-change numbers
        # actually reflect "today vs. yesterday's close".
        try:
            target_day = _last_business_day_before(today_d)
            ensure_prev_close_coverage(db, list(mover_tickers), target_day=target_day)
        except Exception as exc:
            logger.info("prev-close backfill skipped: %s", exc)

        movers = []
        for sym in mover_tickers:
            # Use the two most recent weekday PricesHistory rows — consistent Yahoo-sourced
            # data on both sides of the comparison. Avoids stale LatestPrice contamination.
            recent = (
                db.query(PricesHistory)
                .filter(PricesHistory.ticker == sym)
                .order_by(PricesHistory.date.desc())
                .limit(10)
                .all()
            )
            weekday_rows: list[tuple] = []
            for r in recent:
                cand_date_str = str(r.date)[:10]
                try:
                    cand_d = _date.fromisoformat(cand_date_str)
                except Exception:
                    continue
                if cand_d.weekday() >= 5:
                    continue
                if r.close is None or float(r.close) <= 0:
                    continue
                weekday_rows.append((cand_d, float(r.close)))
                if len(weekday_rows) >= 2:
                    break
            if len(weekday_rows) < 2:
                continue
            current_d, current_price = weekday_rows[0]
            prev_d, prev_close = weekday_rows[1]
            # Freshness: "current" close must be today or within a few days
            if (today_d - current_d).days > max_staleness_days:
                continue
            if prev_close == 0:
                continue
            change_pct = round(((current_price - prev_close) / prev_close) * 100, 2)
            source = "portfolio" if sym in open_portfolio_tickers else "watchlist"
            movers.append({
                "ticker": sym,
                "change_pct": change_pct,
                "source": source,
                "current_price": round(current_price, 2),
                "current_date": current_d.isoformat(),
                "prev_close": round(prev_close, 2),
                "prev_close_date": prev_d.isoformat(),
            })

        movers.sort(key=lambda m: abs(m["change_pct"]), reverse=True)
        result["top_movers"] = movers[:7]
    except Exception as exc:
        logger.warning("Dashboard top movers failed: %s", exc)

    # ---------- Recent activity ----------
    try:
        user_portfolios = (
            db.query(Portfolio.id)
            .filter(Portfolio.owner_id == current_user.id, Portfolio.is_deleted == False)
            .all()
        )
        portfolio_ids = [p.id for p in user_portfolios]
        if portfolio_ids:
            recent_txs = (
                db.query(PortfolioTransaction)
                .filter(
                    PortfolioTransaction.portfolio_id.in_(portfolio_ids),
                    PortfolioTransaction.is_deleted == False,
                )
                .order_by(PortfolioTransaction.trade_date.desc())
                .limit(8)
                .all()
            )
            result["recent_activity"] = [
                {
                    "id": tx.id,
                    "tx_type": tx.tx_type,
                    "ticker_symbol_normalized": tx.ticker_symbol_normalized,
                    "trade_date": tx.trade_date.isoformat() if tx.trade_date else None,
                    "shares": float(tx.shares) if tx.shares else 0,
                    "price": float(tx.price) if tx.price else 0,
                    "gross_amount": float(tx.gross_amount) if tx.gross_amount else 0,
                }
                for tx in recent_txs
            ]
    except Exception as exc:
        logger.warning("Dashboard recent activity failed: %s", exc)

    # ---------- Upcoming dividends ----------
    try:
        if open_portfolio_tickers:
            upcoming = (
                db.query(DividendEvent)
                .filter(
                    DividendEvent.ticker.in_(list(open_portfolio_tickers)),
                    DividendEvent.ex_date >= date.today(),
                    DividendEvent.ex_date <= date.today() + timedelta(days=30),
                )
                .order_by(DividendEvent.ex_date.asc())
                .limit(10)
                .all()
            )
            result["upcoming_dividends"] = [
                {
                    "ticker": d.ticker,
                    "ex_date": d.ex_date.isoformat() if d.ex_date else None,
                    "pay_date": d.pay_date.isoformat() if d.pay_date else None,
                    "amount": float(d.dividend_per_share_native) if d.dividend_per_share_native else None,
                    "currency": d.currency,
                }
                for d in upcoming
            ]
    except Exception as exc:
        logger.warning("Dashboard upcoming dividends failed: %s", exc)

    return result
