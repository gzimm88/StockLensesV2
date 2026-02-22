import logging
from pathlib import Path
from typing import Any

# Load .env from repo root before any other imports read os.environ
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(_env_path, override=False)
except ImportError:
    pass  # python-dotenv not installed — set FINNHUB_API_KEY in the shell

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from backend.database import Base, engine, get_db
from backend.models import FinancialsHistory, LensPreset, Metrics, PricesHistory, ScoreSnapshot, Ticker
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
from backend.repositories import financials_repo, metrics_repo, prices_repo

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


def rows_to_dict(rows):
    return [
        {column.name: getattr(row, column.name) for column in row.__table__.columns}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Existing read endpoints (unchanged)
# ---------------------------------------------------------------------------

@app.get("/health")
def healthcheck():
    return {"status": "ok"}


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
# Score Snapshot endpoints (Phase 3)
# ---------------------------------------------------------------------------

import json as _json


def _deserialise_snapshot(row) -> dict:
    """Convert a ScoreSnapshot ORM row to a JSON-serialisable dict."""
    d = {col.name: getattr(row, col.name) for col in row.__table__.columns}
    _JSON_COLS = {
        "category_scores", "top_positive_contributors",
        "top_negative_contributors", "missing_critical_fields",
        "resolution_warnings",
    }
    for col in _JSON_COLS:
        if isinstance(d.get(col), str):
            try:
                d[col] = _json.loads(d[col])
            except Exception:
                pass
    return d


@app.get("/snapshots/{ticker}")
def get_snapshots_for_ticker(ticker: str, db: Session = Depends(get_db)):
    """Return all ScoreSnapshots for a ticker (one per lens, latest as_of_date)."""
    rows = (
        db.query(ScoreSnapshot)
        .filter(ScoreSnapshot.ticker_symbol == ticker.strip().upper())
        .order_by(ScoreSnapshot.as_of_date.desc())
        .all()
    )
    return [_deserialise_snapshot(r) for r in rows]


@app.get("/snapshots/{ticker}/{lens_id}")
def get_snapshot_for_ticker_lens(ticker: str, lens_id: str, db: Session = Depends(get_db)):
    """Return the latest ScoreSnapshot for a (ticker, lens) pair."""
    row = (
        db.query(ScoreSnapshot)
        .filter(
            ScoreSnapshot.ticker_symbol == ticker.strip().upper(),
            ScoreSnapshot.lens_id == lens_id,
        )
        .order_by(ScoreSnapshot.as_of_date.desc())
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"No snapshot found for {ticker}/{lens_id}")
    return _deserialise_snapshot(row)


@app.post("/snapshots/{ticker}/recompute")
def recompute_snapshots(ticker: str, db: Session = Depends(get_db)):
    """Recompute and persist ScoreSnapshots for all lenses without re-fetching external data."""
    from backend.services import snapshot_service
    from backend.services.metric_resolver import check_ttm_coverage
    from backend.repositories import financials_repo as _fr

    ticker_upper = ticker.strip().upper()
    latest_metrics = metrics_repo.get_metrics(db, ticker_upper)
    if not latest_metrics:
        raise HTTPException(status_code=404, detail=f"No metrics found for {ticker_upper}")

    lens_presets = db.query(LensPreset).all()
    q_rows = _fr.get_financials_for_ticker(db, ticker_upper, freq="quarterly", limit=4)
    ttm_info = check_ttm_coverage(q_rows, ticker=ticker_upper)

    results = []
    for lp in lens_presets:
        lens_dict = {
            "id": lp.id, "name": lp.name,
            "valuation": lp.valuation, "quality": lp.quality,
            "capitalAllocation": lp.capitalAllocation, "growth": lp.growth,
            "moat": lp.moat, "risk": lp.risk, "macro": lp.macro,
            "narrative": lp.narrative, "dilution": lp.dilution,
            "buyThreshold": lp.buyThreshold, "watchThreshold": lp.watchThreshold,
        }
        snap = snapshot_service.compute_snapshot(
            ticker_symbol=ticker_upper,
            lens=lens_dict,
            metrics=latest_metrics,
            resolution_warnings=ttm_info["warnings"],
        )
        snapshot_service.upsert_snapshot(db, snap)
        results.append({"lens": lp.name, "hash": snap["snapshot_hash"], "rec": snap["recommendation"]})

    return {"ticker": ticker_upper, "snapshots": results}


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
