"""
Metrics repository.

Idempotency key: ticker_symbol (one row per ticker)

Upsert behavior (mirrors upsertFinnhubMetrics + upsertWithGuard from Extract2):

  ALWAYS_UPDATE fields: always overwrite regardless of existing value.
  All other fields: only write if existing value is null/None.

  safePatch filter (from runDeterministicPipeline / computeFundamentalMetrics):
    Only write if isinstance(v, (int, float)) and math.isfinite(v)
    OR if it is a string metadata field (as_of_date, data_source, ticker_symbol).

  data_source: on update, append '+finnhub' if not already present.
"""

import logging
import math
import uuid
from datetime import date
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Metrics
from backend.normalizers.finnhub_normalizer import ALWAYS_UPDATE

logger = logging.getLogger(__name__)

# String fields allowed through the safePatch filter
_STRING_META_FIELDS = frozenset(["as_of_date", "data_source", "ticker_symbol"])


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


def _is_valid_value(v: Any) -> bool:
    """
    Mirrors safePatch filter from Extract1 / upsertWithGuard from Extract2:
    Accept finite numeric values. Accept non-empty strings for metadata fields.
    """
    if isinstance(v, bool):
        return True  # booleans (founder_led_bool, partial_ttm) pass through
    if isinstance(v, (int, float)):
        return math.isfinite(v)
    return False


def _safe_clean(
    payload: dict[str, Any],
    *,
    allow_strings: bool = True,
) -> dict[str, Any]:
    """
    Return only valid fields from payload.
    Numeric fields: only finite numbers.
    String metadata fields: only non-empty strings.
    Boolean fields: pass through.
    """
    clean: dict[str, Any] = {}
    for k, v in payload.items():
        if v is None:
            continue
        if k in _STRING_META_FIELDS and isinstance(v, str) and v:
            clean[k] = v
            continue
        if isinstance(v, bool):
            clean[k] = v
            continue
        if isinstance(v, (int, float)) and math.isfinite(v):
            clean[k] = v
    return clean


def _row_to_dict(row: Metrics) -> dict[str, Any]:
    return {col.name: getattr(row, col.name) for col in row.__table__.columns}


def get_metrics(db: Session, ticker_symbol: str) -> dict[str, Any] | None:
    """Fetch single Metrics row by ticker_symbol."""
    row = db.scalars(
        select(Metrics).where(Metrics.ticker_symbol == ticker_symbol)
    ).first()
    return _row_to_dict(row) if row else None


def upsert_metrics(
    db: Session,
    ticker_symbol: str,
    payload: dict[str, Any],
    *,
    source_tag: str | None = None,
) -> str:
    """
    Upsert Metrics row for ticker_symbol.

    ALWAYS_UPDATE fields: overwrite regardless of existing value.
    Other fields: only write if existing value is null/None.
    All values filtered through safePatch (finite numbers + string metadata).

    source_tag: if "finnhub", append '+finnhub' to data_source string.
    Returns "updated" or "inserted".
    """
    clean = _safe_clean(payload)
    clean.pop("ticker_symbol", None)  # will be set from parameter

    existing = db.scalars(
        select(Metrics).where(Metrics.ticker_symbol == ticker_symbol)
    ).first()

    if existing:
        updates: dict[str, Any] = {}
        for k, v in clean.items():
            if k in ALWAYS_UPDATE or getattr(existing, k, None) is None:
                updates[k] = v

        # Handle data_source append for Finnhub
        if source_tag == "finnhub":
            current_src = existing.data_source or ""
            if "finnhub" not in current_src:
                updates["data_source"] = (
                    f"{current_src}+finnhub" if current_src else "finnhub"
                )

        if updates:
            for k, v in updates.items():
                if not hasattr(existing, k):
                    continue
                if k == "as_of_date":
                    setattr(existing, k, _parse_date(v))
                else:
                    setattr(existing, k, v)
            try:
                db.commit()
                logger.debug("[DB][Metrics] updated %s: %s", ticker_symbol, list(updates.keys()))
            except Exception as exc:
                db.rollback()
                raise RuntimeError(f"Metrics update failed for {ticker_symbol}: {exc}") from exc
        else:
            logger.debug("[DB][Metrics] no fields changed for %s", ticker_symbol)
        return "updated"

    else:
        obj = Metrics(
            id=str(uuid.uuid4()),
            ticker_symbol=ticker_symbol,
        )
        if source_tag == "finnhub":
            clean["data_source"] = "finnhub"
        as_of = clean.pop("as_of_date", None)
        if as_of:
            obj.as_of_date = _parse_date(as_of)

        for k, v in clean.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        try:
            db.add(obj)
            db.commit()
            logger.debug("[DB][Metrics] created new row for %s", ticker_symbol)
        except Exception as exc:
            db.rollback()
            raise RuntimeError(f"Metrics insert failed for {ticker_symbol}: {exc}") from exc
        return "inserted"


def upsert_metrics_safe_patch(
    db: Session,
    ticker_symbol: str,
    payload: dict[str, Any],
) -> str:
    """
    Upsert Metrics using the strict safePatch rule from Extract1:
    Only write fields where value is finite number OR string metadata.
    No ALWAYS_UPDATE logic — caller controls which fields to include.
    Returns "updated" or "inserted".
    """
    clean = _safe_clean(payload)
    clean.pop("ticker_symbol", None)

    existing = db.scalars(
        select(Metrics).where(Metrics.ticker_symbol == ticker_symbol)
    ).first()

    if existing:
        for k, v in clean.items():
            if not hasattr(existing, k):
                continue
            if k == "as_of_date":
                setattr(existing, k, _parse_date(v))
            else:
                setattr(existing, k, v)
        try:
            db.commit()
        except Exception as exc:
            db.rollback()
            raise RuntimeError(f"Metrics safe_patch update failed for {ticker_symbol}: {exc}") from exc
        return "updated"
    else:
        obj = Metrics(id=str(uuid.uuid4()), ticker_symbol=ticker_symbol)
        as_of = clean.pop("as_of_date", None)
        if as_of:
            obj.as_of_date = _parse_date(as_of)
        for k, v in clean.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        try:
            db.add(obj)
            db.commit()
        except Exception as exc:
            db.rollback()
            raise RuntimeError(f"Metrics safe_patch insert failed for {ticker_symbol}: {exc}") from exc
        return "inserted"


def ticker_has_metrics(db: Session, ticker_symbol: str) -> bool:
    """Return True if a Metrics row exists for this ticker (DB-first check)."""
    row = db.scalars(
        select(Metrics).where(Metrics.ticker_symbol == ticker_symbol).limit(1)
    ).first()
    return row is not None
