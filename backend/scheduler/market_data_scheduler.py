from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import Portfolio
from backend.orchestrator.portfolio_orchestrator import (
    PortfolioEngineError,
    create_portfolio_daily_snapshot,
    get_active_open_tickers,
    get_required_fx_pairs_for_open_positions,
    insert_fx_rate_point,
    insert_price_history_point,
)

logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.background import BackgroundScheduler
except Exception:  # pragma: no cover - runtime environment fallback
    BackgroundScheduler = None  # type: ignore[assignment]


_scheduler: BackgroundScheduler | None = None


def _bucket_20m(ts: datetime) -> datetime:
    ts_utc = ts.astimezone(timezone.utc).replace(second=0, microsecond=0)
    minute = (ts_utc.minute // 20) * 20
    return ts_utc.replace(minute=minute)


def _within_us_market_hours(now_utc: datetime) -> bool:
    if now_utc.weekday() >= 5:
        return False
    t = now_utc.time()
    return time(14, 30) <= t <= time(21, 0)


def _fetch_latest_price_from_yahoo(ticker: str) -> float | None:
    import yfinance as yf

    hist = yf.Ticker(ticker).history(period="1d", interval="1m", auto_adjust=False, actions=False)
    if hist is None or hist.empty:
        return None
    last = hist.iloc[-1]
    close = float(last.get("Close")) if "Close" in last else None
    return close


def _fetch_latest_fx_from_yahoo(base_currency: str, quote_currency: str) -> float | None:
    import yfinance as yf

    pair = f"{quote_currency}{base_currency}=X"
    hist = yf.Ticker(pair).history(period="1d", interval="1m", auto_adjust=False, actions=False)
    if hist is None or hist.empty:
        return None
    last = hist.iloc[-1]
    close = float(last.get("Close")) if "Close" in last else None
    return close


def run_price_fetch_job(now_utc: datetime | None = None) -> dict[str, int]:
    ts = _bucket_20m(now_utc or datetime.now(timezone.utc))
    if not _within_us_market_hours(ts):
        return {"attempted": 0, "inserted": 0}

    db: Session = SessionLocal()
    attempted = 0
    inserted = 0
    try:
        tickers = get_active_open_tickers(db)
        for ticker in tickers:
            attempted += 1
            px = _fetch_latest_price_from_yahoo(ticker)
            if px is None:
                continue
            if insert_price_history_point(
                db,
                ticker=ticker,
                datetime_utc=ts.replace(tzinfo=None),
                price=px,
                adjusted_price=None,
                source="yahoo_scheduler",
            ):
                inserted += 1
        return {"attempted": attempted, "inserted": inserted}
    finally:
        db.close()


def run_fx_fetch_job(now_utc: datetime | None = None) -> dict[str, int]:
    ts = _bucket_20m(now_utc or datetime.now(timezone.utc))
    db: Session = SessionLocal()
    attempted = 0
    inserted = 0
    try:
        pairs = get_required_fx_pairs_for_open_positions(db)
        for base_currency, quote_currency in pairs:
            attempted += 1
            rate = _fetch_latest_fx_from_yahoo(base_currency, quote_currency)
            if rate is None:
                continue
            if insert_fx_rate_point(
                db,
                base_currency=base_currency,
                quote_currency=quote_currency,
                datetime_utc=ts.replace(tzinfo=None),
                rate=rate,
                source="yahoo_scheduler",
            ):
                inserted += 1
        return {"attempted": attempted, "inserted": inserted}
    finally:
        db.close()


def run_daily_snapshot_job(snapshot_date: date | None = None) -> dict[str, int]:
    snap_date = snapshot_date or (datetime.now(timezone.utc) - timedelta(days=1)).date()
    db: Session = SessionLocal()
    inserted = 0
    attempted = 0
    try:
        portfolios = (
            db.query(Portfolio)
            .filter(Portfolio.is_deleted == False)
            .order_by(Portfolio.id.asc())
            .all()
        )
        for p in portfolios:
            attempted += 1
            try:
                out = create_portfolio_daily_snapshot(db, portfolio_id=p.id, snapshot_date=snap_date)
                if bool(out.get("inserted")):
                    inserted += 1
            except PortfolioEngineError:
                # No deterministic row for that date — skip without side effects.
                continue
        return {"attempted": attempted, "inserted": inserted}
    finally:
        db.close()


def start_market_data_scheduler() -> None:
    global _scheduler
    if BackgroundScheduler is None:
        logger.warning("APScheduler is not installed; market-data scheduler not started.")
        return
    if _scheduler is not None and _scheduler.running:
        return

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(run_price_fetch_job, "interval", minutes=20, id="phase12a_price_fetch", replace_existing=True)
    scheduler.add_job(run_fx_fetch_job, "interval", minutes=20, id="phase12a_fx_fetch", replace_existing=True)
    scheduler.add_job(
        run_daily_snapshot_job,
        "cron",
        hour=21,
        minute=20,
        id="phase12a_daily_snapshot",
        replace_existing=True,
    )
    scheduler.start()
    _scheduler = scheduler
    logger.info("Phase 12A market-data scheduler started.")


def stop_market_data_scheduler() -> None:
    global _scheduler
    if _scheduler is None:
        return
    try:
        _scheduler.shutdown(wait=False)
    finally:
        _scheduler = None
        logger.info("Phase 12A market-data scheduler stopped.")
