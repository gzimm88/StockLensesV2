"""
Full onboard/refresh orchestrator.

Mirrors runFullRefresh.ts from Extract2 exactly:

Execution order (each step isolated — failure sets status="partial"):
  A: Yahoo Historical Prices (5Y)    → runYahooEtlPipeline
  B: Finnhub Fundamentals ETL        → runFinnhubFundamentalsEtl
  C: Yahoo Fundamentals ETL          → runYahooFundamentalsEtl
  D1: Compute Fundamental Metrics    → computeFundamentalMetrics
  E: Sync Recent Prices (1mo)        → syncRecentPricesYahoo
  D2: Re-compute Fundamental Metrics → computeFundamentalMetrics (with latest price)
  F: Compute Price-based Metrics     → computeAndSavePriceMetrics

Failure behavior:
  - Each step wrapped in try/except
  - On failure: append to errors[], set status="partial"
  - Orchestrator always returns without raising (caller sees summary)
  - On ABORT_ON_STEP_FAILURE steps: re-raise (Yahoo Prices is required)
"""

import asyncio
import logging
import math
from datetime import date, timedelta
from typing import Any

import httpx
from sqlalchemy.orm import Session

from backend.api_clients import yahoo_client, finnhub_client
from backend.normalizers import yahoo_normalizer, finnhub_normalizer
from backend.repositories import prices_repo, financials_repo, metrics_repo
from backend.services import metrics_calculator
from backend.models import Ticker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sector PE/EV medians (from Extract3 sector_medians config)
# ---------------------------------------------------------------------------
SECTOR_MEDIANS: dict[str, dict[str, float]] = {
    "Information Technology": {"pe_fwd": 27.0, "ev_ebitda": 20.0},
    "Financials": {"pe_fwd": 13.0, "ev_ebitda": 10.0},
    "Health Care": {"pe_fwd": 20.0, "ev_ebitda": 14.0},
    "Consumer Discretionary": {"pe_fwd": 19.0, "ev_ebitda": 13.0},
    "Consumer Staples": {"pe_fwd": 20.0, "ev_ebitda": 13.5},
    "Industrials": {"pe_fwd": 20.0, "ev_ebitda": 13.0},
    "Materials": {"pe_fwd": 15.0, "ev_ebitda": 9.0},
    "Energy": {"pe_fwd": 12.0, "ev_ebitda": 7.0},
    "Utilities": {"pe_fwd": 16.0, "ev_ebitda": 10.0},
    "Real Estate": {"pe_fwd": 35.0, "ev_ebitda": 20.0},
    "Communication Services": {"pe_fwd": 18.0, "ev_ebitda": 12.0},
}


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and math.isfinite(v)


class OnboardingResult:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.status: str = "ok"          # "ok" or "partial"
        self.errors: list[str] = []
        self.steps: dict[str, Any] = {}
        self.logs: list[str] = []

    def log(self, msg: str) -> None:
        logger.info(msg)
        self.logs.append(msg)

    def step_success(self, name: str, data: Any = None) -> None:
        self.log(f"[Step] SUCCESS: {name}")
        self.steps[name] = {"status": "success", "data": data}

    def step_failed(self, name: str, error: str) -> None:
        self.log(f"[Step] FAILED: {name} - {error}")
        self.status = "partial"
        self.errors.append(f"{name} failed: {error}")
        self.steps[name] = {"status": "failed", "error": error}


# ---------------------------------------------------------------------------
# Individual ETL steps
# ---------------------------------------------------------------------------

async def step_yahoo_prices_5y(
    ticker: str,
    db: Session,
    client: httpx.AsyncClient,
    result: OnboardingResult,
) -> dict:
    """Step A: Yahoo Historical Prices (5Y). Mirrors runYahooEtlPipeline."""
    result.log(f"[Step A] Yahoo Historical Prices (5Y) for {ticker}")
    price_data = await yahoo_client.fetch_5y_prices(ticker, client)
    normalized = yahoo_normalizer.normalize_prices(ticker, price_data)
    result.log(f"[Step A] Normalized {len(normalized)} price points")
    counts = prices_repo.upsert_prices(db, normalized)
    result.log(f"[Step A] Prices: inserted={counts['inserted']} updated={counts['updated']}")
    return {"prices_saved": counts["inserted"] + counts["updated"], **counts}


async def step_finnhub_fundamentals(
    ticker: str,
    db: Session,
    result: OnboardingResult,
) -> dict:
    """Step B: Finnhub Fundamentals ETL. Mirrors runFinnhubFundamentalsEtl."""
    result.log(f"[Step B] Finnhub Fundamentals ETL for {ticker}")

    q_data, a_data, basic_data = await asyncio.gather(
        finnhub_client.fetch_quarterly_financials(ticker),
        finnhub_client.fetch_annual_financials(ticker),
        finnhub_client.fetch_basic_financials(ticker),
    )

    quarters = finnhub_normalizer.normalize_and_quarterize(q_data, a_data)
    result.log(f"[Step B] Quarterized {len(quarters)} reports")

    if len(quarters) < 4:
        result.log(f"[Step B] WARNING: Only {len(quarters)} quarters available")

    # Map Finnhub quarters to FinancialsHistory format
    fin_records = []
    for q in quarters:
        fin_records.append({
            "ticker": ticker,
            "period_end": q["periodEnd"],
            "freq": "quarterly",
            "source": "finnhub",
            "as_of_date": date.today().isoformat(),
            "net_income": q.get("net_income"),
            "cfo": q.get("cfo"),
            "capex": q.get("capex"),
            "stock_based_compensation": q.get("sbc"),
            "depreciation": q.get("depreciation"),
            "interest_expense": q.get("interest_exp"),
            "ebit": q.get("ebit"),
            "eps_diluted": q.get("diluted_eps"),
            "shares_diluted": q.get("shares_diluted"),
            "cash": q.get("cash"),
            "total_debt": q.get("total_debt"),
            "stockholder_equity": q.get("equity"),
            "total_assets": q.get("total_assets"),
        })

    upserted = financials_repo.upsert_financials(db, fin_records)
    result.log(f"[Step B] Upserted {upserted} Finnhub quarterly records")

    # Build TTM and PE history
    ttm = finnhub_normalizer.build_ttm_metrics(quarters)
    if not ttm:
        result.log("[Step B] WARNING: Could not compute TTM (need 4 quarters)")
        return {"quarters_processed": len(quarters)}

    # Get price history from DB for PE calculation
    price_rows = prices_repo.get_prices_for_ticker(
        db, ticker, start_date=_five_years_ago(), order_desc=True
    )
    pe_hist = finnhub_normalizer.calculate_historical_pe(quarters, price_rows)

    # Get latest price for current PE
    latest_price = prices_repo.get_latest_price(db, ticker)
    eps_ttm = ttm.get("eps_ttm")
    pe_ttm_val = (latest_price / eps_ttm) if _is_num(latest_price) and _is_num(eps_ttm) and eps_ttm > 0 else None

    # Extract 5Y CAGR values from Finnhub basic financials (metric endpoint)
    fh_metric = basic_data.get("metric", {}) if isinstance(basic_data, dict) else {}
    def _fh_num(key: str) -> float | None:
        v = fh_metric.get(key)
        try:
            f = float(v)
            return f if f == f else None  # NaN check
        except (TypeError, ValueError):
            return None

    metrics_payload = {
        **ttm,
        "eps_ttm": eps_ttm,
        "pe_ttm": pe_ttm_val,
        "pe_12m": pe_hist.get("pe_12m"),
        "pe_24m": pe_hist.get("pe_24m"),
        "pe_36m": pe_hist.get("pe_36m"),
        "current_pe": pe_ttm_val,
        # 5Y CAGR from Finnhub /stock/metric (epsGrowth5Y, revenueGrowth5Y are already in %)
        "eps_cagr_5y_pct": _fh_num("epsGrowth5Y"),
        "revenue_cagr_5y_pct": _fh_num("revenueGrowth5Y"),
        # Also grab insider ownership if available
        "insider_own_pct": _fh_num("heldPercentInsidersAnnual") or _fh_num("insiderSharePercentage"),
    }

    metrics_repo.upsert_metrics(db, ticker, metrics_payload, source_tag="finnhub")
    result.log(f"[Step B] Finnhub metrics written for {ticker}")

    return {
        "quarters_processed": len(quarters),
        "metrics_populated": sum(1 for v in metrics_payload.values() if _is_num(v)),
    }


async def step_yahoo_fundamentals(
    ticker: str,
    db: Session,
    client: httpx.AsyncClient,
    result: OnboardingResult,
) -> dict:
    """Step C: Yahoo Fundamentals ETL. Mirrors runYahooFundamentalsEtl."""
    result.log(f"[Step C] Yahoo Fundamentals ETL for {ticker}")

    source_q = await yahoo_client.fetch_quote_summary(ticker, client)

    quarterly_records = yahoo_normalizer.normalize_quarterly_financials(ticker, source_q)
    annual_records = yahoo_normalizer.normalize_annual_financials(ticker, source_q)

    q_count = financials_repo.upsert_financials(db, quarterly_records)
    a_count = financials_repo.upsert_financials(db, annual_records)

    result.log(f"[Step C] Upserted {q_count} quarterly + {a_count} annual Yahoo records")

    # Build and upsert Yahoo metrics (from merged quarters + quoteSummary)
    # Use the quarterly records as merged data
    merged = [{
        "revenue": r.get("revenue"),
        "cfo": r.get("cfo"),
        "capex": r.get("capex"),
        "sbc": r.get("stock_based_compensation"),
        "ebit": r.get("ebit"),
        "depreciation": r.get("depreciation"),
        "net_income": r.get("net_income"),
        "interest_expense": r.get("interest_expense"),
        "cash": r.get("cash"),
        "short_debt": r.get("short_debt"),
        "long_debt": r.get("long_term_debt"),
        "equity": r.get("stockholder_equity"),
        "total_assets": r.get("total_assets"),
    } for r in quarterly_records]

    metrics_payload = yahoo_normalizer.build_yahoo_metrics_payload(ticker, source_q, merged)
    metrics_repo.upsert_metrics(db, ticker, metrics_payload)
    result.log(f"[Step C] Yahoo metrics written for {ticker}")

    return {
        "quarterly_count": q_count,
        "annual_count": a_count,
    }


def step_compute_fundamental_metrics(
    ticker: str,
    db: Session,
    result: OnboardingResult,
    sector: str | None = None,
) -> dict:
    """
    Step D: Compute fundamental metrics from DB data.
    Mirrors computeFundamentalMetrics from Extract1.
    """
    result.log(f"[Step D] Computing fundamental metrics for {ticker}")

    quarterly = financials_repo.get_financials_for_ticker(db, ticker, freq="quarterly", limit=20)
    annual = financials_repo.get_financials_for_ticker(db, ticker, freq="annual", limit=10)
    prices = prices_repo.get_prices_for_ticker(db, ticker, order_desc=True, limit=2000)

    # Get SPY prices for beta
    spy_prices = prices_repo.get_prices_for_ticker(db, "SPY", order_desc=True, limit=2000)

    if len(quarterly) < 4:
        result.log(f"[Step D] WARNING: Only {len(quarterly)} quarterly records, need 4 for TTM")

    payload = metrics_calculator.run_deterministic_pipeline(
        ticker=ticker,
        quarterly=quarterly,
        annual=annual,
        prices=prices,
        spy_prices=spy_prices,
        existing_metrics=metrics_repo.get_metrics(db, ticker),
    )
    result.log(
        f"[Step D][PE_FWD] ticker={ticker} price_current={payload.get('price_current')} "
        f"eps_forward={payload.get('eps_forward')} pe_fwd={payload.get('pe_fwd')} "
        "formula=price_current/eps_forward"
    )

    # Add sector PE/EV medians if sector known
    if sector and sector in SECTOR_MEDIANS:
        medians = SECTOR_MEDIANS[sector]
        payload["pe_fwd_sector"] = medians.get("pe_fwd")
        payload["ev_ebitda_sector"] = medians.get("ev_ebitda")

    metrics_repo.upsert_metrics_safe_patch(db, ticker, payload)
    result.log(f"[Step D] Fundamental metrics written for {ticker}")

    return {"metrics_written": len([v for v in payload.values() if _is_num(v)])}


async def step_sync_recent_prices(
    ticker: str,
    db: Session,
    client: httpx.AsyncClient,
    result: OnboardingResult,
) -> dict:
    """Step E: Sync recent prices (1mo). Mirrors syncRecentPricesYahoo."""
    result.log(f"[Step E] Syncing recent prices for {ticker}")
    price_data = await yahoo_client.fetch_recent_prices(ticker, client)
    normalized = yahoo_normalizer.normalize_prices(ticker, price_data)
    counts = prices_repo.upsert_prices(db, normalized)
    result.log(f"[Step E] Recent prices: inserted={counts['inserted']} updated={counts['updated']}")
    return counts


# ---------------------------------------------------------------------------
# Full orchestrator entry point
# ---------------------------------------------------------------------------

async def run_full_onboard(
    ticker: str,
    db: Session,
    sector: str | None = None,
) -> OnboardingResult:
    """
    Execute the full onboard pipeline in the canonical order from runFullRefresh.ts:
      A → B → C → D1 → E → D2 → F

    Each step is isolated — failure marks status="partial" but continues.
    Yahoo prices (step A) is considered critical; if it fails, we still continue
    but log the failure.
    """
    ticker = ticker.strip().upper()
    result = OnboardingResult(ticker)
    result.log(f"[Orchestrator] Starting full onboard/refresh for {ticker}")

    # Pre-step: Ensure a Ticker row exists (required by FK constraints and screener)
    try:
        import uuid as _uuid
        existing_ticker = db.query(Ticker).filter(Ticker.symbol == ticker).first()
        info = {}
        need_info_lookup = (
            not existing_ticker
            or not (existing_ticker.name or "").strip()
            or (existing_ticker.name or "").strip().upper() == ticker
            or not (existing_ticker.exchange or "").strip()
            or not (existing_ticker.sector or "").strip()
        )
        if need_info_lookup:
            import yfinance as yf
            info = yf.Ticker(ticker).info or {}

        if not existing_ticker:
            ticker_row = Ticker(
                id=str(_uuid.uuid4()),
                symbol=ticker,
                name=info.get("longName") or info.get("shortName") or ticker,
                exchange=info.get("exchange"),
                sector=sector or info.get("sector"),
            )
            db.add(ticker_row)
            db.commit()
            result.log(f"[Pre] Created ticker row for {ticker}")
        else:
            updated = False
            fetched_name = (info.get("longName") or info.get("shortName") or "").strip()
            if fetched_name and (
                not (existing_ticker.name or "").strip()
                or (existing_ticker.name or "").strip().upper() == ticker
            ):
                existing_ticker.name = fetched_name
                updated = True
                result.log(f"[Pre] Updated ticker name for {ticker} -> {fetched_name}")

            if info.get("exchange") and not (existing_ticker.exchange or "").strip():
                existing_ticker.exchange = info.get("exchange")
                updated = True

            # Update sector if provided and not already set; fallback to Yahoo sector.
            if sector and not existing_ticker.sector:
                existing_ticker.sector = sector
                updated = True
            elif info.get("sector") and not existing_ticker.sector:
                existing_ticker.sector = info.get("sector")
                updated = True

            if updated:
                db.commit()
                result.log(f"[Pre] Refreshed metadata for {ticker}")
            result.log(f"[Pre] Ticker row already exists for {ticker}")
    except Exception as exc:
        result.log(f"[Pre] Warning: could not upsert ticker row: {exc}")

    async with httpx.AsyncClient() as client:

        # Step A: Yahoo Historical Prices (5Y)
        result.log("[Step A] Yahoo Historical Prices (5Y)...")
        try:
            data = await step_yahoo_prices_5y(ticker, db, client, result)
            result.step_success("A:yahoo_prices", data)
        except Exception as exc:
            result.step_failed("A:yahoo_prices", str(exc))

        # Step B: Finnhub Fundamentals ETL
        result.log("[Step B] Finnhub Fundamentals ETL...")
        try:
            data = await step_finnhub_fundamentals(ticker, db, result)
            result.step_success("B:finnhub_fundamentals", data)
        except Exception as exc:
            result.step_failed("B:finnhub_fundamentals", str(exc))

        # Step C: Yahoo Fundamentals ETL
        result.log("[Step C] Yahoo Fundamentals ETL...")
        try:
            data = await step_yahoo_fundamentals(ticker, db, client, result)
            result.step_success("C:yahoo_fundamentals", data)
        except Exception as exc:
            result.step_failed("C:yahoo_fundamentals", str(exc))

        # Step D1: Compute Fundamental Metrics
        result.log("[Step D1] Computing fundamental metrics...")
        try:
            data = step_compute_fundamental_metrics(ticker, db, result, sector=sector)
            result.step_success("D1:fundamental_metrics", data)
        except Exception as exc:
            result.step_failed("D1:fundamental_metrics", str(exc))

        # Step E: Sync Recent Prices (1mo)
        result.log("[Step E] Syncing recent prices (1mo)...")
        try:
            data = await step_sync_recent_prices(ticker, db, client, result)
            result.step_success("E:recent_prices", data)
        except Exception as exc:
            result.step_failed("E:recent_prices", str(exc))

        # Step D2: Re-compute Fundamental Metrics (with latest price data)
        result.log("[Step D2] Re-computing fundamental metrics (post recent prices)...")
        try:
            data = step_compute_fundamental_metrics(ticker, db, result, sector=sector)
            result.step_success("D2:fundamental_metrics_rerun", data)
        except Exception as exc:
            result.step_failed("D2:fundamental_metrics_rerun", str(exc))

        # Step F: Compute Price-based Metrics (price_metrics pass only)
        result.log("[Step F] Computing price-based metrics...")
        try:
            quarterly = financials_repo.get_financials_for_ticker(db, ticker, freq="quarterly", limit=20)
            prices = prices_repo.get_prices_for_ticker(db, ticker, order_desc=True, limit=2000)
            existing = metrics_repo.get_metrics(db, ticker)
            eps_cagr = existing.get("eps_cagr_5y_pct") if existing else None
            market_cap = existing.get("market_cap") if existing else None
            price_m = metrics_calculator.compute_price_metrics(
                ticker, prices, quarterly, market_cap, eps_cagr
            )
            price_m["ticker_symbol"] = ticker
            price_m["as_of_date"] = date.today().isoformat()
            metrics_repo.upsert_metrics_safe_patch(db, ticker, price_m)
            result.step_success("F:price_metrics", {"fields": len(price_m)})
        except Exception as exc:
            result.step_failed("F:price_metrics", str(exc))

        # Step G: Compute Score Snapshots for all lens presets (Phase 3)
        result.log("[Step G] Computing ScoreSnapshots for all lens presets...")
        try:
            from backend.models import LensPreset
            from backend.services import snapshot_service
            from backend.services.metric_resolver import check_ttm_coverage
            from backend.repositories import financials_repo as _fr

            latest_metrics = metrics_repo.get_metrics(db, ticker) or {}
            lens_presets = db.query(LensPreset).all()

            # Collect TTM warnings for explainability
            q_rows = _fr.get_financials_for_ticker(db, ticker, freq="quarterly", limit=4)
            ttm_info = check_ttm_coverage(q_rows, ticker=ticker)
            resolution_warnings = ttm_info["warnings"]

            snapshot_count = 0
            for lp in lens_presets:
                lens_dict = {
                    "id":                lp.id,
                    "name":              lp.name,
                    "valuation":         lp.valuation,
                    "quality":           lp.quality,
                    "capitalAllocation": lp.capitalAllocation,
                    "growth":            lp.growth,
                    "moat":              lp.moat,
                    "risk":              lp.risk,
                    "macro":             lp.macro,
                    "narrative":         lp.narrative,
                    "dilution":          lp.dilution,
                    "buyThreshold":      lp.buyThreshold,
                    "watchThreshold":    lp.watchThreshold,
                }
                snap = snapshot_service.compute_snapshot(
                    ticker_symbol=ticker,
                    lens=lens_dict,
                    metrics=latest_metrics,
                    mos_pct=None,          # MOS is computed in Projection — not available here
                    resolution_warnings=resolution_warnings,
                )
                snapshot_service.upsert_snapshot(db, snap)
                snapshot_count += 1

            result.step_success("G:score_snapshots", {"snapshots_written": snapshot_count})
            result.log(f"[Step G] {snapshot_count} ScoreSnapshot(s) persisted for {ticker}")
        except Exception as exc:
            result.step_failed("G:score_snapshots", str(exc))

        # Final debug snapshot for traceability in UI log tab
        try:
            latest = metrics_repo.get_metrics(db, ticker) or {}
            result.log(
                "[Final] Metrics snapshot "
                f"pe_fwd={latest.get('pe_fwd')} pe_ttm={latest.get('pe_ttm')} "
                f"eps_ttm={latest.get('eps_ttm')} as_of_date={latest.get('as_of_date')} "
                f"data_source={latest.get('data_source')}"
            )
        except Exception as exc:
            result.log(f"[Final] Warning: could not load metrics snapshot: {exc}")

    final_status = "ok" if not result.errors else "partial"
    result.status = final_status
    result.log(
        f"[Orchestrator] {'✅ Completed' if final_status == 'ok' else '⚠️ Partial'} "
        f"for {ticker} ({len(result.errors)} error(s))"
    )
    return result


# ---------------------------------------------------------------------------
# DB-first check
# ---------------------------------------------------------------------------

def ticker_is_onboarded(db: Session, ticker: str) -> bool:
    """
    Return True if ticker has both price history and metrics in DB.
    Used for DB-first (skip API calls if already fully onboarded).
    """
    has_prices = len(prices_repo.get_prices_for_ticker(db, ticker, limit=1)) > 0
    has_metrics = metrics_repo.ticker_has_metrics(db, ticker)
    return has_prices and has_metrics


def _five_years_ago() -> str:
    d = date.today().replace(year=date.today().year - 5)
    return d.isoformat()
