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
from datetime import date
from typing import Any

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from backend.models import FinancialsHistory

logger = logging.getLogger(__name__)

_DATE_FIELDS = frozenset(["period_end", "as_of_date"])


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


def _normalize_record_dates(record: dict[str, Any]) -> dict[str, Any]:
    """Ensure Date columns are Python date objects (or None)."""
    normalized = dict(record)
    for field in _DATE_FIELDS:
        if field in normalized:
            normalized[field] = _parse_date(normalized.get(field))
    return normalized


def _row_to_dict(row: FinancialsHistory) -> dict[str, Any]:
    return {
        col.name: getattr(row, col.name)
        for col in row.__table__.columns
    }


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
    inserted = 0
    updated = 0
    for record in records:
        record = _normalize_record_dates(record)
        ticker = record.get("ticker")
        period_end_val = record.get("period_end")
        freq = record.get("freq")

        if not ticker or not period_end_val or not freq:
            continue

        period_end = _parse_date(period_end_val)
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
                    if k in _DATE_FIELDS:
                        v = _parse_date(v)
                    setattr(existing, k, v)
            db.commit()
            upserted += 1
            updated += 1
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
                    if k in _DATE_FIELDS:
                        v = _parse_date(v)
                    setattr(obj, k, v)
            db.add(obj)
            db.commit()
            upserted += 1
            inserted += 1

    logger.info(
        "[DB][Financials] upserted %d/%d records (inserted=%d updated=%d)",
        upserted,
        len(records),
        inserted,
        updated,
    )
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
