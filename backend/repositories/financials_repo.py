"""
FinancialsHistory repository.

Idempotency key: (ticker, period_end, freq)

Upsert behavior (mirrors runYahooFundamentalsEtl.ts + saveFinancialsHistory.ts):
  - For each record: check if (ticker, period_end, freq) already exists
  - If exists: update non-null fields
  - If not: create new record
  - Only include non-null fields in create/update payload
"""

import logging
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from backend.models import FinancialsHistory

logger = logging.getLogger(__name__)


def _parse_date(d: Any) -> date | None:
    if d is None:
        return None
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        try:
            return date.fromisoformat(d[:10])
        except ValueError:
            return None
    return None


def _row_to_dict(row: FinancialsHistory) -> dict[str, Any]:
    """
    Convert a FinancialsHistory ORM row to a plain dict.

    Date/datetime columns are serialised to ISO-8601 strings so callers
    (metrics_calculator, finnhub_normalizer) can safely do d[:7] slicing
    on period_end and other date fields.
    """
    result: dict[str, Any] = {}
    for col in row.__table__.columns:
        val = getattr(row, col.name)
        if isinstance(val, datetime):
            val = val.isoformat()
        elif isinstance(val, date):
            val = val.isoformat()   # "YYYY-MM-DD"
        result[col.name] = val
    return result


# Column names that are part of the idempotency key or metadata (not financial data)
_META_COLS = frozenset(["id", "ticker", "period_end", "freq", "source", "as_of_date",
                         "created_date", "updated_date", "created_by_id", "created_by", "is_sample"])


def upsert_financials(
    db: Session,
    records: list[dict[str, Any]],
) -> int:
    """
    Upsert FinancialsHistory records.
    Idempotency key: (ticker, period_end, freq).
    Returns count of records upserted.

    Only writes non-null values. If a record already exists, updates only
    fields that are non-null in the incoming data (mirrors runYahooFundamentalsEtl).
    """
    if not records:
        return 0

    upserted = 0
    for record in records:
        ticker = record.get("ticker")
        period_end_str = record.get("period_end")
        freq = record.get("freq")

        if not ticker or not period_end_str or not freq:
            continue

        period_end = _parse_date(period_end_str)
        if period_end is None:
            continue

        # Check for existing record by idempotency key
        existing = db.scalars(
            select(FinancialsHistory).where(
                and_(
                    FinancialsHistory.ticker == ticker,
                    FinancialsHistory.period_end == period_end,
                    FinancialsHistory.freq == freq,
                )
            )
        ).first()

        # Build clean data dict (only non-null values, exclude key fields)
        clean_data = {
            k: v
            for k, v in record.items()
            if k not in ("ticker", "period_end", "freq") and v is not None
        }

        if not clean_data:
            continue

        if existing:
            # Update: only overwrite with non-null incoming values
            for k, v in clean_data.items():
                if hasattr(existing, k):
                    setattr(existing, k, v)
            try:
                db.commit()
                upserted += 1
            except Exception as exc:
                db.rollback()
                logger.error("[DB][Financials] update failed for %s %s %s: %s",
                             ticker, period_end_str, freq, exc)
        else:
            # Insert new record
            obj = FinancialsHistory(
                id=str(uuid.uuid4()),
                ticker=ticker,
                period_end=period_end,
                freq=freq,
            )
            for k, v in clean_data.items():
                if hasattr(obj, k):
                    setattr(obj, k, v)
            try:
                db.add(obj)
                db.commit()
                upserted += 1
            except Exception as exc:
                db.rollback()
                logger.error("[DB][Financials] insert failed for %s %s %s: %s",
                             ticker, period_end_str, freq, exc)

    logger.info("[DB][Financials] upserted %d/%d records", upserted, len(records))
    return upserted


def get_financials_for_ticker(
    db: Session,
    ticker: str,
    freq: str | None = None,
    limit: int = 40,
    order_desc: bool = True,
) -> list[dict[str, Any]]:
    """Fetch FinancialsHistory rows for a ticker, optionally filtered by freq."""
    q = select(FinancialsHistory).where(FinancialsHistory.ticker == ticker)
    if freq:
        q = q.where(FinancialsHistory.freq == freq)
    if order_desc:
        q = q.order_by(FinancialsHistory.period_end.desc())
    else:
        q = q.order_by(FinancialsHistory.period_end.asc())
    q = q.limit(limit)
    rows = db.scalars(q).all()
    return [_row_to_dict(r) for r in rows]


def ticker_has_financials(db: Session, ticker: str) -> bool:
    """Return True if the ticker has any FinancialsHistory records (DB-first check)."""
    row = db.scalars(
        select(FinancialsHistory).where(FinancialsHistory.ticker == ticker).limit(1)
    ).first()
    return row is not None
