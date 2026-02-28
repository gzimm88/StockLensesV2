import logging
from pathlib import Path
from typing import Any
from datetime import date, datetime, timezone

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
    LensPreset,
    Metrics,
    Portfolio,
    PortfolioCorrectionEvent,
    PortfolioCoverageEvent,
    PortfolioSnapshot,
    PortfolioProcessingRun,
    PortfolioSettings,
    PortfolioTransaction,
    PriceHistory,
    PricesHistory,
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
                    amount_per_share NUMERIC(24,10) NOT NULL,
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


@app.on_event("startup")
def _on_startup() -> None:
    start_market_data_scheduler()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    stop_market_data_scheduler()


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
            rebuild_equity_history(
                db,
                payload.portfolio_id,
                mode="incremental",
                force=False,
                strict=None,
            )
        except PortfolioEngineError:
            backfill_fx_history_if_missing(payload.portfolio_id, db)
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
            rebuild_equity_history(
                db,
                data["portfolio_id"],
                mode="incremental",
                force=False,
                strict=None,
            )
        except PortfolioEngineError:
            backfill_fx_history_if_missing(data["portfolio_id"], db)
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
