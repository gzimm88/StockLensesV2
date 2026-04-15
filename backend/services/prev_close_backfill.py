"""
On-demand backfill of yesterday's / last-business-day prices_history.

The top-movers widget needs a reliable "previous close" per ticker. Scheduled
price fetches can miss days. This helper bridges gaps by fetching recent daily
closes from Yahoo Finance and idempotently upserting into PricesHistory.

Design notes:
  - Scoped to a single "target day" (usually the last US business day before
    today). We only fetch for tickers whose most recent PricesHistory row is
    older than the target day.
  - Uses yfinance history (period=\"10d\", interval=\"1d\") — small surface,
    enough to cover weekend/holiday gaps.
  - Idempotent via `(ticker, date)`: skips insert if row already exists.
  - Fails per-ticker: one bad symbol doesn't block the rest.
  - Soft-caps network cost to a max number of tickers per call.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy.orm import Session

from backend.models import PricesHistory

logger = logging.getLogger(__name__)

_MAX_TICKERS_PER_CALL = 40
_FETCH_PERIOD = "10d"


def _last_business_day_before(d: date) -> date:
    """Most recent weekday strictly before `d`. (Weekends only, no holiday calendar.)"""
    prev = d - timedelta(days=1)
    while prev.weekday() >= 5:
        prev -= timedelta(days=1)
    return prev


def _parse_row_date(value) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    s = str(value)[:10]
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def _existing_dates_by_ticker(
    db: Session, tickers: list[str], since: date
) -> dict[str, set[date]]:
    """Map ticker -> set of dates already in PricesHistory since `since` (inclusive)."""
    if not tickers:
        return {}
    rows = (
        db.query(PricesHistory.ticker, PricesHistory.date)
        .filter(PricesHistory.ticker.in_(tickers), PricesHistory.date >= since)
        .all()
    )
    out: dict[str, set[date]] = {t: set() for t in tickers}
    for t, d in rows:
        pd = _parse_row_date(d)
        if pd and t in out:
            out[t].add(pd)
    return out


def _resolve_fetch_symbols(db: Session, tickers: list[str]) -> dict[str, str]:
    """Resolve internal tickers to Yahoo-compatible symbols (e.g. MC -> MC.PA for Paris)."""
    try:
        from backend.scheduler.market_data_scheduler import _collect_active_fetch_symbols
        return _collect_active_fetch_symbols(db, specific_tickers=tickers)
    except Exception as exc:
        logger.info("fetch-symbol resolver unavailable, using tickers as-is: %s", exc)
        return {t: t for t in tickers}


def ensure_prev_close_coverage(
    db: Session,
    tickers: Iterable[str],
    *,
    target_day: date | None = None,
    max_tickers: int = _MAX_TICKERS_PER_CALL,
) -> dict[str, int]:
    """
    Ensure each ticker has a PricesHistory row for the last business day (or
    `target_day`). Returns {"checked", "fetched", "inserted", "skipped"}.

    Safe to call in the request path: times out per-ticker via yfinance's
    internal HTTP timeouts, swallows errors, and caps total work.
    """
    stats = {"checked": 0, "fetched": 0, "inserted": 0, "skipped": 0}
    tickers = sorted({t.strip().upper() for t in tickers if t})
    if not tickers:
        return stats
    if len(tickers) > max_tickers:
        logger.info("ensure_prev_close_coverage: capping from %d to %d tickers", len(tickers), max_tickers)
        tickers = tickers[:max_tickers]

    today = datetime.now(timezone.utc).date()
    tgt = target_day or _last_business_day_before(today)

    # Figure out which tickers are missing `tgt`
    lookback = tgt - timedelta(days=14)
    existing = _existing_dates_by_ticker(db, tickers, lookback)
    to_fetch = [t for t in tickers if tgt not in existing.get(t, set())]
    stats["checked"] = len(tickers)
    stats["skipped"] = len(tickers) - len(to_fetch)
    if not to_fetch:
        return stats

    # Resolve proper Yahoo symbols (e.g. European tickers need .PA / .DE / etc.)
    fetch_symbol_by_ticker = _resolve_fetch_symbols(db, to_fetch)

    # Lazy import yfinance so the scheduler module doesn't pay the cost if unused
    try:
        import yfinance as yf  # noqa: WPS433
    except Exception as exc:  # pragma: no cover - env-specific
        logger.warning("yfinance unavailable for backfill: %s", exc)
        return stats

    for sym in to_fetch:
        fetch_sym = fetch_symbol_by_ticker.get(sym, sym)
        stats["fetched"] += 1
        try:
            hist = yf.Ticker(fetch_sym).history(
                period=_FETCH_PERIOD,
                interval="1d",
                auto_adjust=False,
                actions=False,
            )
        except Exception as exc:
            logger.info("prev-close backfill fetch failed for %s (%s): %s", sym, fetch_sym, exc)
            continue
        if hist is None or hist.empty or "Close" not in hist.columns:
            continue

        already = existing.get(sym, set())
        new_rows: list[PricesHistory] = []
        for ts, row in hist.iterrows():
            d_val = ts.date() if hasattr(ts, "date") else None
            if d_val is None or d_val.weekday() >= 5:
                continue
            # Backfill days up to and INCLUDING today (so we have a consistent
            # Yahoo-sourced current close to compare against). Don't go past today.
            if d_val > today:
                continue
            if d_val in already:
                continue
            close = row.get("Close")
            if close is None:
                continue
            try:
                close_f = float(close)
            except (TypeError, ValueError):
                continue
            if close_f <= 0:
                continue
            open_f = float(row.get("Open")) if row.get("Open") is not None else None
            high_f = float(row.get("High")) if row.get("High") is not None else None
            low_f = float(row.get("Low")) if row.get("Low") is not None else None
            vol = row.get("Volume")
            try:
                vol_i = int(vol) if vol is not None else None
            except (TypeError, ValueError):
                vol_i = None
            new_rows.append(
                PricesHistory(
                    id=str(uuid.uuid4()),
                    ticker=sym,
                    date=d_val,
                    open=open_f,
                    high=high_f,
                    low=low_f,
                    close=close_f,
                    volume=vol_i,
                    source="yahoo_backfill",
                    as_of_date=today,
                )
            )
            already.add(d_val)

        if new_rows:
            try:
                db.add_all(new_rows)
                db.commit()
                stats["inserted"] += len(new_rows)
            except Exception as exc:
                db.rollback()
                logger.warning("prev-close backfill commit failed for %s: %s", sym, exc)

    return stats
