import logging
import os
from pathlib import Path
from typing import Any

# Load .env from the repo root (two levels above this file: backend/main.py → repo/)
# Must happen before any module that reads os.environ (e.g. finnhub_client).
try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).resolve().parent.parent / ".env"
    if _env_path.exists():
        load_dotenv(_env_path, override=False)
        logging.getLogger(__name__).info("[Config] Loaded .env from %s", _env_path)
    else:
        logging.getLogger(__name__).info("[Config] No .env file found at %s", _env_path)
except ImportError:
    pass  # python-dotenv not installed — env vars must be set externally

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import Base, engine, get_db
from backend.models import FinancialsHistory, LensPreset, Metrics, PricesHistory, Ticker
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

# Allow the Vite dev server to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)


def rows_to_dict(rows):
    return [
        {column.name: getattr(row, column.name) for column in row.__table__.columns}
        for row in rows
    ]


def row_to_dict(row):
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.get("/tickers")
def list_tickers(limit: int = 500, db: Session = Depends(get_db)):
    rows = db.scalars(select(Ticker).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/metrics")
def list_metrics(limit: int = 500, db: Session = Depends(get_db)):
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
# Ticker CRUD
# ---------------------------------------------------------------------------

@app.get("/tickers/filter")
def filter_tickers(symbol: str | None = None, db: Session = Depends(get_db)):
    """Filter tickers by symbol (exact match, case-insensitive)."""
    q = select(Ticker)
    if symbol:
        q = q.where(Ticker.symbol == symbol.upper())
    rows = db.scalars(q).all()
    return rows_to_dict(rows)


@app.post("/tickers", status_code=201)
def create_ticker(body: dict, db: Session = Depends(get_db)):
    """Create a new ticker row."""
    # Prevent duplicate symbol
    existing = db.scalars(
        select(Ticker).where(Ticker.symbol == body.get("symbol", "").upper())
    ).first()
    if existing:
        raise HTTPException(status_code=409, detail="Ticker already exists")

    ticker = Ticker(**{k: v for k, v in body.items() if hasattr(Ticker, k)})
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    return row_to_dict(ticker)


@app.patch("/tickers/{ticker_id}")
def update_ticker(ticker_id: str, body: dict, db: Session = Depends(get_db)):
    """Partial-update a ticker row by id."""
    row = db.get(Ticker, ticker_id)
    if not row:
        raise HTTPException(status_code=404, detail="Ticker not found")
    for key, value in body.items():
        if hasattr(Ticker, key) and key != "id":
            setattr(row, key, value)
    db.commit()
    db.refresh(row)
    return row_to_dict(row)


# ---------------------------------------------------------------------------
# Metrics CRUD
# ---------------------------------------------------------------------------

@app.get("/metrics/filter")
def filter_metrics(ticker_symbol: str | None = None, db: Session = Depends(get_db)):
    """Filter metrics by ticker_symbol."""
    q = select(Metrics)
    if ticker_symbol:
        q = q.where(Metrics.ticker_symbol == ticker_symbol.upper())
    rows = db.scalars(q).all()
    return rows_to_dict(rows)


@app.post("/metrics", status_code=201)
def create_metrics(body: dict, db: Session = Depends(get_db)):
    """Create a new metrics row."""
    m = Metrics(**{k: v for k, v in body.items() if hasattr(Metrics, k)})
    db.add(m)
    db.commit()
    db.refresh(m)
    return row_to_dict(m)


@app.patch("/metrics/{metrics_id}")
def update_metrics(metrics_id: str, body: dict, db: Session = Depends(get_db)):
    """Partial-update a metrics row by id."""
    row = db.get(Metrics, metrics_id)
    if not row:
        raise HTTPException(status_code=404, detail="Metrics not found")
    for key, value in body.items():
        if hasattr(Metrics, key) and key != "id":
            setattr(row, key, value)
    db.commit()
    db.refresh(row)
    return row_to_dict(row)


# ---------------------------------------------------------------------------
# LensPreset CRUD
# ---------------------------------------------------------------------------

@app.post("/lens-presets", status_code=201)
def create_lens_preset(body: dict, db: Session = Depends(get_db)):
    """Create a new lens preset."""
    existing = db.get(LensPreset, body.get("id"))
    if existing:
        raise HTTPException(status_code=409, detail="Lens preset with this id already exists")
    lp = LensPreset(**{k: v for k, v in body.items() if hasattr(LensPreset, k)})
    db.add(lp)
    db.commit()
    db.refresh(lp)
    return row_to_dict(lp)


@app.patch("/lens-presets/{preset_id}")
def update_lens_preset(preset_id: str, body: dict, db: Session = Depends(get_db)):
    """Partial-update a lens preset by id."""
    row = db.get(LensPreset, preset_id)
    if not row:
        raise HTTPException(status_code=404, detail="Lens preset not found")
    for key, value in body.items():
        if hasattr(LensPreset, key) and key != "id":
            setattr(row, key, value)
    db.commit()
    db.refresh(row)
    return row_to_dict(row)


@app.delete("/lens-presets/{preset_id}", status_code=204)
def delete_lens_preset(preset_id: str, db: Session = Depends(get_db)):
    """Delete a lens preset by id."""
    row = db.get(LensPreset, preset_id)
    if not row:
        raise HTTPException(status_code=404, detail="Lens preset not found")
    db.delete(row)
    db.commit()
    return None


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
