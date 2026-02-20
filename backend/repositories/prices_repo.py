"""
PricesHistory repository.

Idempotency key: (ticker, date)

Upsert behavior (mirrors Extract5 / Extract2):
  - Batch size: 25 rows per batch (conservative, matching runYahooEtlPipeline.ts)
  - Check existing by (ticker, date in batch)
  - Insert new rows (bulkCreate equivalent)
  - Update existing rows with new data (syncRecentPricesYahoo updates existing)
  - Insert retry: 3 attempts, delays [1500, 3000, 5000] ms
  - On 401/403: raise immediately with code=UNAUTHORIZED
  - Inter-batch delay: 200 ms (Extract5 uses INTER_BATCH_DELAY)
"""

import asyncio
import logging
import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from backend.models import PricesHistory

logger = logging.getLogger(__name__)

BATCH_SIZE: int = 25
INTER_BATCH_DELAY_S: float = 0.2   # 200 ms
INSERT_RETRY_DELAYS: list[float] = [1.5, 3.0, 5.0]   # seconds


def _row_to_dict(row: PricesHistory) -> dict[str, Any]:
    """
    Convert a PricesHistory ORM row to a plain dict.

    Date/datetime columns are serialised to ISO-8601 strings so callers
    (finnhub_normalizer, metrics_calculator) can safely do d[:7] slicing.
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


def upsert_prices(
    db: Session,
    prices: list[dict[str, Any]],
) -> dict[str, int]:
    """
    Upsert PricesHistory records.
    Returns {"inserted": N, "updated": N, "skipped": N}.

    Idempotency key: (ticker, date).
    Batches of BATCH_SIZE rows. Updates existing rows with new values.
    """
    if not prices:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    ticker = prices[0]["ticker"]
    batches = [prices[i: i + BATCH_SIZE] for i in range(0, len(prices), BATCH_SIZE)]
    logger.info("[DB][Prices] %d rows in %d batches for %s", len(prices), len(batches), ticker)

    total_inserted = 0
    total_updated = 0
    total_skipped = 0

    for i, batch in enumerate(batches):
        dates = [p["date"] for p in batch]

        try:
            existing_rows = db.scalars(
                select(PricesHistory).where(
                    and_(
                        PricesHistory.ticker == ticker,
                        PricesHistory.date.in_(dates),
                    )
                )
            ).all()

            existing_map: dict[str, PricesHistory] = {
                str(row.date): row for row in existing_rows
            }

            to_insert = [p for p in batch if str(p["date"]) not in existing_map]
            to_update = [p for p in batch if str(p["date"]) in existing_map]

            logger.debug("[DB][Prices] Batch %d/%d: %d insert, %d update",
                         i + 1, len(batches), len(to_insert), len(to_update))

            # Inserts with retry
            if to_insert:
                _bulk_insert_with_retry(db, to_insert, i + 1)
                total_inserted += len(to_insert)

            # Updates (individual, for better error isolation)
            for row_data in to_update:
                existing = existing_map[str(row_data["date"])]
                for k, v in row_data.items():
                    if k not in ("id", "ticker", "date") and v is not None:
                        setattr(existing, k, v)
                total_updated += 1

            db.commit()

        except Exception as exc:
            db.rollback()
            logger.error("[DB][Prices] Batch %d failed: %s", i + 1, exc)
            total_skipped += len(batch)

        # Inter-batch delay (skip after last batch)
        if i < len(batches) - 1:
            import time
            time.sleep(INTER_BATCH_DELAY_S)

    logger.info("[DB][Prices] Done: inserted=%d updated=%d skipped=%d",
                total_inserted, total_updated, total_skipped)
    return {"inserted": total_inserted, "updated": total_updated, "skipped": total_skipped}


def _bulk_insert_with_retry(
    db: Session,
    rows: list[dict[str, Any]],
    batch_num: int,
) -> None:
    """Insert rows with up to 3 retries. Mirrors Insert retry in Extract5."""
    import time

    for attempt, delay in enumerate(INSERT_RETRY_DELAYS):
        try:
            objs = [
                PricesHistory(
                    id=str(uuid.uuid4()),
                    ticker=r["ticker"],
                    date=_parse_date(r["date"]),
                    open=r.get("open"),
                    high=r.get("high"),
                    low=r.get("low"),
                    close=r.get("close"),
                    close_adj=r.get("close_adj"),
                    volume=int(r["volume"]) if r.get("volume") is not None else None,
                    source=r.get("source", "yahoo"),
                    as_of_date=_parse_date(r["as_of_date"]),
                )
                for r in rows
            ]
            db.add_all(objs)
            db.flush()
            return
        except Exception as exc:
            db.rollback()
            if attempt == len(INSERT_RETRY_DELAYS) - 1:
                raise RuntimeError(
                    f"DB insert failed on batch {batch_num} after {len(INSERT_RETRY_DELAYS)} attempts: {exc}"
                ) from exc
            logger.warning("[DB][Prices] insert retry %d/%d batch=%d: %s",
                           attempt + 1, len(INSERT_RETRY_DELAYS), batch_num, exc)
            time.sleep(delay)


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


def get_prices_for_ticker(
    db: Session,
    ticker: str,
    start_date: str | None = None,
    order_desc: bool = True,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    """Fetch PricesHistory rows for a ticker, optionally filtered by start_date."""
    q = select(PricesHistory).where(PricesHistory.ticker == ticker)
    if start_date:
        q = q.where(PricesHistory.date >= _parse_date(start_date))
    if order_desc:
        q = q.order_by(PricesHistory.date.desc())
    else:
        q = q.order_by(PricesHistory.date.asc())
    q = q.limit(limit)
    rows = db.scalars(q).all()
    return [_row_to_dict(r) for r in rows]


def get_latest_price(db: Session, ticker: str) -> float | None:
    """
    Return the most recent close (split-adjusted only) for a ticker.
    Uses close, NOT close_adj (dividend-adjusted), to match calculateHistoricalPE_DB
    which always uses close for PE computation (mirrors Extract5 behavior).
    """
    row = db.scalars(
        select(PricesHistory)
        .where(PricesHistory.ticker == ticker)
        .order_by(PricesHistory.date.desc())
        .limit(1)
    ).first()
    return row.close if row else None
