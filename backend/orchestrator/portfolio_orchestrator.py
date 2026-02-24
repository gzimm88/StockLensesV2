from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy.orm import Session

from backend.api_clients import yahoo_client
from backend.models import (
    Portfolio,
    PortfolioCorrectionEvent,
    PortfolioCoverageEvent,
    PortfolioProcessingRun,
    PortfolioTransaction,
    SecurityIdentity,
    SecuritySymbolMap,
)
from backend.normalizers import yahoo_normalizer
from backend.repositories import prices_repo
from backend.services.portfolio_engine import (
    PRICES_PATH,
    PortfolioEngineError,
    Transaction,
    export_outputs,
    load_portfolio_transactions,
    run_portfolio_engine,
)

logger = logging.getLogger(__name__)
LAST_RUN_PATH = PRICES_PATH.parent / "last_portfolio_run.json"
RUN_CACHE_DIR = PRICES_PATH.parent / "engine_outputs" / "portfolio_runs"
_LOCK_MAP_GUARD = threading.Lock()
_TICKER_LOCKS: dict[str, asyncio.Lock] = {}


@dataclass(frozen=True)
class CoverageReport:
    requested_tickers: list[str]
    fetched_tickers: list[str]
    already_covered_tickers: list[str]
    coverage_start: date
    coverage_end: date
    warnings: list[str]
    status_by_ticker: dict[str, str]
    impact_by_ticker: dict[str, dict[str, object]]


YAHOO_SUFFIX_BY_EXCHANGE: dict[str, str] = {
    "ENXTAM": ".AS",  # Euronext Amsterdam
    "ENXTPA": ".PA",  # Euronext Paris
    "SWX": ".SW",     # SIX Swiss Exchange
    "XTRA": ".DE",    # Xetra (Germany)
}


def _latest_expected_market_day(end_date: date) -> date:
    # Use the latest completed market day (T-1), then roll back weekends.
    d = end_date - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _has_sufficient_coverage(rows: list[dict], start_date: date, end_date: date) -> tuple[bool, str]:
    if not rows:
        return False, "no prices"

    parsed = sorted(date.fromisoformat(str(r["date"])[:10]) for r in rows if r.get("date"))
    if not parsed:
        return False, "no valid dates"

    first = parsed[0]
    last = parsed[-1]

    lead_gap_days = (first - start_date).days
    if lead_gap_days > 0:
        if lead_gap_days > 5:
            return False, f"missing history before {first.isoformat()}"
        lead_gap_reason = f"bounded leading gap: first row {first.isoformat()} (+{lead_gap_days}d)"
    else:
        lead_gap_reason = "ok"

    latest_expected = _latest_expected_market_day(end_date)
    if last < latest_expected - timedelta(days=2):
        return False, f"latest row {last.isoformat()} is stale"

    for prev, nxt in zip(parsed, parsed[1:]):
        if (nxt - prev).days > 5 and nxt >= start_date:
            return False, f"gap detected between {prev.isoformat()} and {nxt.isoformat()}"

    return True, lead_gap_reason


def _ticker_lock(ticker: str) -> asyncio.Lock:
    with _LOCK_MAP_GUARD:
        lock = _TICKER_LOCKS.get(ticker)
        if lock is None:
            lock = asyncio.Lock()
            _TICKER_LOCKS[ticker] = lock
        return lock


def _normalize_tickers(tickers: list[str]) -> list[str]:
    return sorted({t.strip().upper() for t in tickers if t and t.strip()})


def _to_fetch_symbol(ticker: str, source_symbol: str | None = None) -> tuple[str, str | None]:
    """
    Map internal ticker format to Yahoo-compatible fetch symbol.
    Returns (symbol, warning_or_none).
    """
    if source_symbol and ":" in source_symbol:
        exchange, raw_core = source_symbol.split(":", 1)
        exchange = exchange.strip().upper()
        core = raw_core.strip().upper()
        suffix = YAHOO_SUFFIX_BY_EXCHANGE.get(exchange)
        if suffix:
            return f"{core}{suffix}", None
        # US/common listings generally use core as-is except class notation.
        if exchange in {"NYSE", "NASDAQGS", "NASDAQCM", "ARCA"}:
            return core.replace(".", "-"), None
        fallback = core.replace(".", "-")
        return fallback, f"WARNING[{ticker}] Unknown exchange '{exchange}' in '{source_symbol}'. Fallback symbol '{fallback}' used."

    fallback = ticker.replace(".", "-")
    return fallback, f"WARNING[{ticker}] No exchange prefix supplied; fallback symbol '{fallback}' used."


def _split_source_symbol(source_symbol: str | None, ticker: str) -> tuple[str | None, str, str | None]:
    if source_symbol and ":" in source_symbol:
        exchange, core = source_symbol.split(":", 1)
        exchange = exchange.strip().upper() or None
        core = core.strip().upper()
    else:
        exchange = None
        core = ticker.strip().upper()
    vendor_symbol, _ = _to_fetch_symbol(ticker.strip().upper(), source_symbol)
    return exchange, core, vendor_symbol


def _security_id(normalized_symbol: str, exchange: str | None) -> str:
    key = f"{(exchange or 'NA').upper()}::{normalized_symbol.upper()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


def _hash_transactions(transactions: list[Transaction]) -> str:
    canonical = [
        {
            "row_id": getattr(tx, "row_id", None),
            "ticker_symbol": getattr(tx, "ticker_symbol", getattr(tx, "ticker", "")),
            "ticker": getattr(tx, "ticker", ""),
            "trade_date": getattr(tx, "trade_date").isoformat() if getattr(tx, "trade_date", None) else "",
            "shares": getattr(tx, "shares", 0.0),
            "price": getattr(tx, "price", 0.0),
            "cost": getattr(tx, "cost", 0.0),
            "tx_type": getattr(tx, "tx_type", ""),
            "currency": getattr(tx, "currency", "USD"),
        }
        for tx in transactions
    ]
    blob = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _run_cache_path(portfolio_id: str) -> Path:
    safe = "".join(ch for ch in portfolio_id if ch.isalnum() or ch in ("-", "_")) or "portfolio"
    return RUN_CACHE_DIR / f"last_portfolio_run_{safe}.json"


def _tx_stats(transactions: list[Transaction]) -> dict[str, dict[str, object]]:
    stats: dict[str, dict[str, object]] = {}
    grouped: dict[str, list[Transaction]] = defaultdict(list)
    for tx in transactions:
        grouped[tx.ticker].append(tx)

    for ticker, txs in grouped.items():
        txs_sorted = sorted(txs, key=lambda t: (t.trade_date, t.row_id))
        raw_symbols = sorted({t.ticker_symbol for t in txs_sorted if t.ticker_symbol})
        net_shares = 0.0
        for tx in txs_sorted:
            if tx.tx_type == "Buy":
                net_shares += tx.shares
            elif tx.tx_type == "Sell":
                net_shares -= tx.shares
        stats[ticker] = {
            "first_trade_date": txs_sorted[0].trade_date,
            "last_trade_date": txs_sorted[-1].trade_date,
            "net_shares": net_shares,
            "closed_position": net_shares <= 1e-9,
            "source_symbol": raw_symbols[0] if raw_symbols else ticker,
            "multiple_source_symbols": len(raw_symbols) > 1,
        }
    return stats


async def ensure_price_coverage(
    db: Session,
    tickers: list[str],
    start_date: date,
    *,
    force: bool = False,
    end_date: date | None = None,
    closed_position_end_dates: dict[str, date] | None = None,
    ticker_start_dates: dict[str, date] | None = None,
    ticker_source_symbols: dict[str, str] | None = None,
) -> CoverageReport:
    """
    Ensure deterministic local price coverage for all tickers.

    Closed-position tickers may end coverage at their close date and may emit
    warnings (instead of hard failure) when Yahoo returns no history.
    """
    if not tickers:
        raise PortfolioEngineError("ensure_price_coverage called with empty ticker list")

    end_date = end_date or date.today()
    closed_position_end_dates = {
        k.strip().upper(): v for k, v in (closed_position_end_dates or {}).items()
    }
    ticker_start_dates = {
        k.strip().upper(): v for k, v in (ticker_start_dates or {}).items()
    }
    ticker_source_symbols = {
        k.strip().upper(): v for k, v in (ticker_source_symbols or {}).items()
    }
    normalized_tickers = _normalize_tickers(tickers)
    fetched: list[str] = []
    already_covered: list[str] = []
    warnings: list[str] = []
    status_by_ticker: dict[str, str] = {}
    impact_by_ticker: dict[str, dict[str, object]] = {}

    async with httpx.AsyncClient() as client:
        for ticker in normalized_tickers:
            ticker_start = ticker_start_dates.get(ticker, start_date)
            ticker_end = closed_position_end_dates.get(ticker, end_date)
            lock = _ticker_lock(ticker)
            async with lock:
                existing = prices_repo.get_prices_for_ticker(
                    db,
                    ticker,
                    start_date=ticker_start.isoformat(),
                    order_desc=False,
                    limit=100000,
                )
                covered, reason = _has_sufficient_coverage(existing, ticker_start, ticker_end)
                if covered:
                    logger.info("[PortfolioCoverage] %s already covered (%s)", ticker, reason)
                    status_by_ticker[ticker] = "OK"
                    impact_by_ticker[ticker] = {
                        "fallback_days": 0,
                        "first_missing_date": None,
                        "last_missing_date": None,
                    }
                    if reason != "ok":
                        status_by_ticker[ticker] = "BoundedLeadingGap"
                        lead_days = 0
                        first_missing = None
                        if "first row " in reason:
                            raw = reason.split("first row ", 1)[1].split(" ", 1)[0]
                            first_missing = ticker_start.isoformat()
                            try:
                                lead_days = max(0, (date.fromisoformat(raw) - ticker_start).days)
                            except Exception:
                                lead_days = 0
                        impact_by_ticker[ticker] = {
                            "fallback_days": lead_days,
                            "first_missing_date": first_missing,
                            "last_missing_date": None if lead_days <= 0 else (ticker_start + timedelta(days=lead_days - 1)).isoformat(),
                        }
                        msg = f"WARNING[{ticker}] Coverage accepted with {reason}."
                        warnings.append(msg)
                        logger.warning("[PortfolioCoverage] %s", msg)
                    already_covered.append(ticker)
                    continue

                logger.info("[PortfolioCoverage] Fetching %s (%s)", ticker, reason)
                normalized: list[dict] = []
                fetch_symbol, map_warning = _to_fetch_symbol(
                    ticker,
                    ticker_source_symbols.get(ticker),
                )
                if map_warning:
                    warnings.append(map_warning)
                    logger.warning("[PortfolioCoverage] %s", map_warning)
                try:
                    chart = await yahoo_client.fetch_prices_range(
                        fetch_symbol,
                        ticker_start.isoformat(),
                        (ticker_end + timedelta(days=1)).isoformat(),
                        client,
                    )
                    normalized = yahoo_normalizer.normalize_prices(ticker, chart)
                except Exception as exc:
                    if ticker in closed_position_end_dates:
                        status_by_ticker[ticker] = "NoFeed"
                        impact_by_ticker[ticker] = {
                            "fallback_days": max(0, (ticker_end - ticker_start).days + 1),
                            "first_missing_date": ticker_start.isoformat(),
                            "last_missing_date": ticker_end.isoformat(),
                        }
                        msg = (
                            f"WARNING[{ticker}] Closed position with unavailable market feed: {exc}. "
                            "Falling back to deterministic synthetic execution-mark pricing."
                        )
                        logger.warning("[PortfolioCoverage] %s", msg)
                        warnings.append(msg)
                        continue
                    raise PortfolioEngineError(f"API fetch failed for {ticker}: {exc}") from exc

                if not normalized:
                    if ticker in closed_position_end_dates:
                        status_by_ticker[ticker] = "NoFeed"
                        impact_by_ticker[ticker] = {
                            "fallback_days": max(0, (ticker_end - ticker_start).days + 1),
                            "first_missing_date": ticker_start.isoformat(),
                            "last_missing_date": ticker_end.isoformat(),
                        }
                        msg = (
                            f"WARNING[{ticker}] Closed position has no retrievable price history. "
                            "Falling back to deterministic synthetic execution-mark pricing."
                        )
                        logger.warning("[PortfolioCoverage] %s", msg)
                        warnings.append(msg)
                        continue
                    raise PortfolioEngineError(f"API fetch returned no normalized price rows for {ticker}")

                if force:
                    result = prices_repo.upsert_prices(db, normalized)
                else:
                    result = prices_repo.insert_missing_prices(db, normalized)

                logger.info(
                    "[PortfolioCoverage] %s persisted rows: inserted=%s updated=%s skipped=%s",
                    ticker,
                    result.get("inserted", 0),
                    result.get("updated", 0),
                    result.get("skipped", 0),
                )
                fetched.append(ticker)

                current = prices_repo.get_prices_for_ticker(
                    db,
                    ticker,
                    start_date=ticker_start.isoformat(),
                    order_desc=False,
                    limit=100000,
                )
                ok, why = _has_sufficient_coverage(current, ticker_start, ticker_end)
                if not ok:
                    if ticker in closed_position_end_dates:
                        status_by_ticker[ticker] = "MissingSegments"
                        impact_by_ticker[ticker] = {
                            "fallback_days": max(0, (ticker_end - ticker_start).days + 1),
                            "first_missing_date": ticker_start.isoformat(),
                            "last_missing_date": ticker_end.isoformat(),
                        }
                        msg = (
                            f"WARNING[{ticker}] Coverage remained incomplete after fetch ({why}). "
                            "Falling back to deterministic synthetic execution-mark pricing."
                        )
                        logger.warning("[PortfolioCoverage] %s", msg)
                        warnings.append(msg)
                        continue
                    raise PortfolioEngineError(f"Coverage incomplete after fetch for {ticker}: {why}")
                status_by_ticker[ticker] = "OK"
                impact_by_ticker[ticker] = {
                    "fallback_days": 0,
                    "first_missing_date": None,
                    "last_missing_date": None,
                }
                if why != "ok":
                    status_by_ticker[ticker] = "BoundedLeadingGap"
                    msg = f"WARNING[{ticker}] Coverage accepted with {why}."
                    warnings.append(msg)
                    logger.warning("[PortfolioCoverage] %s", msg)

    return CoverageReport(
        requested_tickers=normalized_tickers,
        fetched_tickers=fetched,
        already_covered_tickers=already_covered,
        coverage_start=start_date,
        coverage_end=end_date,
        warnings=warnings,
        status_by_ticker=status_by_ticker,
        impact_by_ticker=impact_by_ticker,
    )


def _iter_calendar_days(start_date: date, end_date: date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def _synthetic_prices_from_transactions(
    ticker: str,
    txs: list[Transaction],
    start_date: date,
    end_date: date,
) -> list[dict]:
    ticker_txs = [t for t in txs if t.ticker == ticker]
    if not ticker_txs:
        return []

    event_price: dict[date, float] = {}
    for tx in sorted(ticker_txs, key=lambda t: (t.trade_date, t.row_id)):
        event_price[tx.trade_date] = tx.price

    if not event_price:
        return []

    cursor = min(event_price)
    current_price = event_price[cursor]
    out: list[dict] = []
    as_of = date.today().isoformat()

    for d in _iter_calendar_days(start_date, end_date):
        if d in event_price:
            current_price = event_price[d]
        if d.weekday() < 5 or d in event_price:
            out.append(
                {
                    "ticker": ticker,
                    "date": d.isoformat(),
                    "close_adj": current_price,
                    "open": current_price,
                    "high": current_price,
                    "low": current_price,
                    "close": current_price,
                    "volume": None,
                    "source": "synthetic_execution_mark",
                    "as_of_date": as_of,
                }
            )
    return out


def export_prices_for_engine(
    db: Session,
    transactions: list[Transaction],
    start_date: date,
    end_date: date,
    ticker_stats: dict[str, dict[str, object]],
    output_path: Path = PRICES_PATH,
) -> tuple[Path, list[str]]:
    """
    Persist deterministic price export used by portfolio_engine hardcoded path.

    For closed positions with missing market data, generates deterministic
    synthetic daily marks (execution-price carry) and returns warnings.
    """
    rows: list[dict] = []
    warnings: list[str] = []

    for ticker in sorted(ticker_stats.keys()):
        stats = ticker_stats[ticker]
        ticker_end = (
            stats["last_trade_date"] if stats["closed_position"] else end_date
        )
        ticker_txs = [t for t in transactions if t.ticker == ticker]
        prices = prices_repo.get_prices_for_ticker(
            db,
            ticker,
            start_date=start_date.isoformat(),
            order_desc=False,
            limit=100000,
        )

        ticker_rows: list[dict] = []
        for p in prices:
            d = date.fromisoformat(str(p["date"])[:10])
            if d < start_date or d > ticker_end:
                continue
            ticker_rows.append(
                {
                    "ticker": ticker,
                    "date": d.isoformat(),
                    "close_adj": p.get("close_adj"),
                    "open": p.get("open"),
                    "high": p.get("high"),
                    "low": p.get("low"),
                    "close": p.get("close"),
                    "volume": p.get("volume"),
                    "source": p.get("source") or "yahoo",
                    "as_of_date": str(p.get("as_of_date") or date.today().isoformat())[:10],
                }
            )

        if not ticker_rows and stats["closed_position"]:
            synth = _synthetic_prices_from_transactions(ticker, transactions, start_date, ticker_end)
            if not synth:
                raise PortfolioEngineError(
                    f"Unable to build synthetic pricing for closed ticker {ticker}."
                )
            ticker_rows.extend(synth)
            warnings.append(
                f"WARNING[{ticker}] Used deterministic synthetic execution-mark pricing for closed position."
            )

        # Ensure every transaction date has a deterministic anchor row.
        # This prevents hard failures when transaction dates are non-trading days.
        if ticker_rows:
            existing_dates = {r["date"] for r in ticker_rows}
            tx_anchors_added = 0
            for tx in sorted(ticker_txs, key=lambda t: (t.trade_date, t.row_id)):
                if tx.trade_date < start_date or tx.trade_date > ticker_end:
                    continue
                key = tx.trade_date.isoformat()
                if key in existing_dates:
                    continue
                ticker_rows.append(
                    {
                        "ticker": ticker,
                        "date": key,
                        "close_adj": tx.price,
                        "open": tx.price,
                        "high": tx.price,
                        "low": tx.price,
                        "close": tx.price,
                        "volume": None,
                        "source": "synthetic_execution_mark_tx",
                        "as_of_date": date.today().isoformat(),
                    }
                )
                existing_dates.add(key)
                tx_anchors_added += 1
            if tx_anchors_added > 0:
                warnings.append(
                    f"WARNING[{ticker}] Added {tx_anchors_added} transaction-date synthetic price anchor(s)."
                )

        if not ticker_rows:
            raise PortfolioEngineError(
                f"No prices available for ticker {ticker} in required range."
            )

        rows.extend(ticker_rows)

    rows.sort(key=lambda r: (r["ticker"], r["date"]))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ticker",
                "date",
                "close_adj",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source",
                "as_of_date",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    return output_path, warnings


def _ensure_security_mapping(db: Session, ticker: str, raw_symbol: str | None) -> str:
    exchange, normalized_symbol, vendor_symbol = _split_source_symbol(raw_symbol, ticker)
    sec_id = _security_id(normalized_symbol, exchange)
    now = datetime.utcnow()

    ident = db.query(SecurityIdentity).filter(SecurityIdentity.security_id == sec_id).first()
    if ident is None:
        ident = SecurityIdentity(
            security_id=sec_id,
            normalized_symbol=normalized_symbol,
            exchange=exchange,
            mic=exchange,
            vendor_symbol=vendor_symbol,
            raw_symbol_example=raw_symbol or ticker,
            created_at=now,
            updated_at=now,
        )
        db.add(ident)
    else:
        ident.updated_at = now
        ident.vendor_symbol = ident.vendor_symbol or vendor_symbol
        ident.raw_symbol_example = ident.raw_symbol_example or (raw_symbol or ticker)

    if raw_symbol:
        mapped = db.query(SecuritySymbolMap).filter(SecuritySymbolMap.raw_input_symbol == raw_symbol).first()
        if mapped and mapped.security_id != sec_id:
            raise PortfolioEngineError(
                f"Ticker identity fork detected: raw symbol '{raw_symbol}' maps to multiple security ids."
            )
        if mapped is None:
            db.add(
                SecuritySymbolMap(
                    id=str(uuid.uuid4()),
                    raw_input_symbol=raw_symbol,
                    normalized_symbol=normalized_symbol,
                    exchange=exchange,
                    mic=exchange,
                    vendor_symbol=vendor_symbol,
                    security_id=sec_id,
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            mapped.updated_at = now
            mapped.vendor_symbol = mapped.vendor_symbol or vendor_symbol
    return sec_id


def get_or_create_default_portfolio(db: Session) -> Portfolio:
    row = (
        db.query(Portfolio)
        .filter(Portfolio.is_deleted == False, Portfolio.name == "Default")
        .first()
    )
    if row:
        return row
    now = datetime.utcnow()
    row = Portfolio(
        id=str(uuid.uuid4()),
        name="Default",
        base_currency="USD",
        owner_id="local",
        is_deleted=False,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_portfolio_or_error(db: Session, portfolio_id: str) -> Portfolio:
    row = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.is_deleted == False)
        .first()
    )
    if not row:
        raise PortfolioEngineError(f"Portfolio '{portfolio_id}' not found.")
    return row


def list_portfolios(db: Session) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    portfolios = (
        db.query(Portfolio)
        .filter(Portfolio.is_deleted == False)
        .order_by(Portfolio.created_at.asc())
        .all()
    )
    for p in portfolios:
        latest_run = (
            db.query(PortfolioProcessingRun)
            .filter(PortfolioProcessingRun.portfolio_id == p.id, PortfolioProcessingRun.status == "success")
            .order_by(PortfolioProcessingRun.finished_at.desc(), PortfolioProcessingRun.started_at.desc())
            .first()
        )
        last_nav = None
        last_run_id = None
        last_processed_at = None
        if latest_run:
            last_run_id = latest_run.id
            last_processed_at = latest_run.finished_at.isoformat() + "Z" if latest_run.finished_at else None
            cached = load_last_portfolio_run(p.id) or {}
            if cached.get("run_id") == latest_run.id:
                last_nav = cached.get("nav")
        out.append(
            {
                "id": p.id,
                "name": p.name,
                "base_currency": p.base_currency,
                "last_processed_at": last_processed_at,
                "last_nav": last_nav,
                "last_run_id": last_run_id,
            }
        )
    return out


def create_portfolio(db: Session, name: str, base_currency: str = "USD") -> dict[str, object]:
    clean_name = (name or "").strip()
    if not clean_name:
        raise PortfolioEngineError("Portfolio name is required.")
    exists = (
        db.query(Portfolio)
        .filter(Portfolio.name == clean_name, Portfolio.owner_id == "local", Portfolio.is_deleted == False)
        .first()
    )
    if exists:
        raise PortfolioEngineError(f"Portfolio '{clean_name}' already exists.")
    now = datetime.utcnow()
    row = Portfolio(
        id=str(uuid.uuid4()),
        name=clean_name,
        base_currency=(base_currency or "USD").strip().upper() or "USD",
        owner_id="local",
        is_deleted=False,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    return {"id": row.id, "name": row.name, "base_currency": row.base_currency}


def soft_delete_portfolio(db: Session, portfolio_id: str) -> None:
    row = get_portfolio_or_error(db, portfolio_id)
    row.is_deleted = True
    row.updated_at = datetime.utcnow()
    db.commit()


def import_transactions_from_csv_for_portfolio(
    db: Session,
    portfolio_id: str,
    *,
    replace_existing: bool = False,
) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    txs = load_portfolio_transactions()
    if replace_existing:
        db.query(PortfolioTransaction).filter(PortfolioTransaction.portfolio_id == portfolio_id).delete()
    now = datetime.utcnow()
    inserted = 0
    sec_cache: dict[tuple[str, str], str] = {}
    for tx in txs:
        key = (tx.ticker, tx.ticker_symbol)
        sec_id = sec_cache.get(key)
        if sec_id is None:
            sec_id = _ensure_security_mapping(db, tx.ticker, tx.ticker_symbol)
            sec_cache[key] = sec_id
        db.add(
            PortfolioTransaction(
                id=str(uuid.uuid4()),
                portfolio_id=portfolio_id,
                security_id=sec_id,
                ticker_symbol_raw=tx.ticker_symbol,
                ticker_symbol_normalized=tx.ticker,
                tx_type=tx.tx_type,
                trade_date=tx.trade_date,
                shares=tx.shares,
                price=tx.price,
                gross_amount=tx.cost,
                currency=tx.currency,
                metadata_json=json.dumps({"row_id": tx.row_id}, ensure_ascii=True),
                source="portfolio1.csv",
                created_at=now,
                updated_at=now,
                is_deleted=False,
            )
        )
        inserted += 1
    db.commit()
    return {"inserted": inserted}


def _load_portfolio_transactions_from_db(db: Session, portfolio_id: str) -> list[Transaction]:
    rows = (
        db.query(PortfolioTransaction)
        .filter(PortfolioTransaction.portfolio_id == portfolio_id, PortfolioTransaction.is_deleted == False)
        .order_by(PortfolioTransaction.trade_date.asc(), PortfolioTransaction.id.asc())
        .all()
    )
    out: list[Transaction] = []
    seq = 1
    for r in rows:
        out.append(
            Transaction(
                row_id=seq + 1,
                ticker_symbol=r.ticker_symbol_raw,
                ticker=r.ticker_symbol_normalized,
                trade_date=r.trade_date,
                shares=float(r.shares),
                price=float(r.price),
                cost=float(r.gross_amount),
                tx_type=r.tx_type,
                currency=r.currency,
            )
        )
        seq += 1
    return out


def _write_transactions_csv(transactions: list[Transaction], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Ticker Symbol", "Date", "Shares", "Price", "Cost", "Type", "Currency"],
        )
        writer.writeheader()
        for tx in transactions:
            ticker_symbol = getattr(tx, "ticker_symbol", getattr(tx, "ticker", ""))
            cost = getattr(tx, "cost", getattr(tx, "gross_amount", 0.0))
            writer.writerow(
                {
                    "Ticker Symbol": ticker_symbol,
                    "Date": tx.trade_date.isoformat(),
                    "Shares": tx.shares,
                    "Price": tx.price,
                    "Cost": cost,
                    "Type": tx.tx_type,
                    "Currency": getattr(tx, "currency", "USD"),
                }
            )
    return output_path


async def run_portfolio_creation_flow(db: Session, portfolio_id: str) -> dict[str, object]:
    """
    Backend-only flow:
      upload portfolio (already saved to hardcoded CSV)
      -> extract tickers
      -> ensure_price_coverage
      -> run deterministic engine
      -> persist outputs
    """
    portfolio = get_portfolio_or_error(db, portfolio_id)
    run_started = datetime.utcnow()
    transactions = _load_portfolio_transactions_from_db(db, portfolio_id)
    if not transactions:
        raise PortfolioEngineError(f"No transactions found for portfolio '{portfolio.name}'.")
    input_hash = _hash_transactions(transactions)
    run_row = PortfolioProcessingRun(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        started_at=run_started,
        status="running",
        warnings_count=0,
        hash_inputs=input_hash,
        engine_version="v1",
    )
    db.add(run_row)
    db.commit()

    stats = _tx_stats(transactions)
    tickers = sorted(stats.keys())
    earliest_trade = min(t.trade_date for t in transactions)
    security_id_by_ticker: dict[str, str] = {}
    for ticker, s in stats.items():
        security_id_by_ticker[ticker] = _ensure_security_mapping(db, ticker, str(s.get("source_symbol") or ticker))
    db.commit()

    closed_end_dates = {
        t: s["last_trade_date"]
        for t, s in stats.items()
        if bool(s["closed_position"])
    }
    ticker_start_dates = {
        t: s["first_trade_date"]
        for t, s in stats.items()
    }

    try:
        coverage = await ensure_price_coverage(
            db,
            tickers,
            earliest_trade,
            closed_position_end_dates=closed_end_dates,
            ticker_start_dates=ticker_start_dates,
            ticker_source_symbols={t: str(s["source_symbol"]) for t, s in stats.items()},
        )
        _, export_warnings = export_prices_for_engine(
            db,
            transactions,
            earliest_trade,
            date.today(),
            stats,
        )

        tx_csv_path = PRICES_PATH.parent / "engine_inputs" / f"{portfolio_id}_transactions.csv"
        _write_transactions_csv(transactions, tx_csv_path)
        outputs = run_portfolio_engine(portfolio_path=tx_csv_path, prices_path=PRICES_PATH)
        files = export_outputs(outputs)

        portfolio_nav = next((r["value"] for r in outputs.portfolio_summary if r["metric"] == "total_equity"), None)
        portfolio_irr = next((r["value"] for r in outputs.portfolio_summary if r["metric"] == "money_weighted_return_irr"), None)

        warnings = coverage.warnings + export_warnings + outputs.warnings

        for ticker in coverage.requested_tickers:
            impact = coverage.impact_by_ticker.get(ticker) or {}
            db.add(
                PortfolioCoverageEvent(
                    id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    run_id=run_row.id,
                    security_id=security_id_by_ticker.get(ticker),
                    ticker=ticker,
                    raw_input_symbol=str(stats.get(ticker, {}).get("source_symbol") or ticker),
                    status=coverage.status_by_ticker.get(ticker, "OK"),
                    warning_code="coverage",
                    message=None,
                    fallback_days=int(impact.get("fallback_days", 0) or 0),
                    first_missing_date=date.fromisoformat(impact["first_missing_date"]) if impact.get("first_missing_date") else None,
                    last_missing_date=date.fromisoformat(impact["last_missing_date"]) if impact.get("last_missing_date") else None,
                    coverage_start=coverage.coverage_start,
                    coverage_end=coverage.coverage_end,
                    created_at=datetime.utcnow(),
                )
            )

        for ff in outputs.prior_close_fallback:
            ticker = str(ff.get("ticker") or "")
            db.add(
                PortfolioCoverageEvent(
                    id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    run_id=run_row.id,
                    security_id=security_id_by_ticker.get(ticker),
                    ticker=ticker,
                    raw_input_symbol=str(stats.get(ticker, {}).get("source_symbol") or ticker),
                    status="PriorCloseFallbackUsed",
                    warning_code="prior_close_fallback",
                    message=str(ff.get("context") or "prior close fallback used"),
                    fallback_days=int(ff.get("fallback_days") or 0),
                    first_missing_date=date.fromisoformat(str(ff.get("first_missing_date"))),
                    last_missing_date=date.fromisoformat(str(ff.get("last_missing_date"))),
                    coverage_start=coverage.coverage_start,
                    coverage_end=coverage.coverage_end,
                    created_at=datetime.utcnow(),
                )
            )

        for evt in outputs.correction_events:
            db.add(
                PortfolioCorrectionEvent(
                    id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    run_id=run_row.id,
                    ticker=str(evt["ticker"]),
                    row_id=int(evt["row_id"]),
                    requested_shares=float(evt["requested_shares"]),
                    available_shares=float(evt["available_shares"]),
                    executed_shares=float(evt["executed_shares"]),
                    delta_shares=float(evt["delta_shares"]),
                    reason=str(evt["reason"]),
                    created_at=datetime.utcnow(),
                )
            )

        run_row.finished_at = datetime.utcnow()
        run_row.status = "success"
        run_row.warnings_count = len(warnings)
        db.commit()

        payload = {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio.name,
            "tickers": tickers,
            "start_date": earliest_trade.isoformat(),
            "fetched_tickers": coverage.fetched_tickers,
            "run_id": run_row.id,
            "input_hash": run_row.hash_inputs,
            "engine_version": run_row.engine_version,
            "warnings_count": len(warnings),
            "correction_event_count": len(outputs.correction_events),
            "fallback_count": len(outputs.prior_close_fallback),
            "coverage_status": {
                "requested_tickers": coverage.requested_tickers,
                "fetched_tickers": coverage.fetched_tickers,
                "already_covered_tickers": coverage.already_covered_tickers,
                "coverage_start": coverage.coverage_start.isoformat(),
                "coverage_end": coverage.coverage_end.isoformat(),
                "warnings": warnings,
                "coverage_summary": [
                    {
                        "ticker": t,
                        "status": coverage.status_by_ticker.get(t, "OK"),
                        "fallback_days": int((coverage.impact_by_ticker.get(t) or {}).get("fallback_days") or 0),
                        "first_missing_date": (coverage.impact_by_ticker.get(t) or {}).get("first_missing_date"),
                        "last_missing_date": (coverage.impact_by_ticker.get(t) or {}).get("last_missing_date"),
                    }
                    for t in coverage.requested_tickers
                ],
            },
            "output_files": [str(p) for p in files],
            "nav": portfolio_nav,
            "irr": portfolio_irr,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
        cache_path = _run_cache_path(portfolio_id)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
        LAST_RUN_PATH.parent.mkdir(parents=True, exist_ok=True)
        LAST_RUN_PATH.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
        return payload
    except Exception:
        run_row.finished_at = datetime.utcnow()
        run_row.status = "failed"
        db.commit()
        raise


def load_last_portfolio_run(portfolio_id: str | None = None) -> dict[str, object] | None:
    path = _run_cache_path(portfolio_id) if portfolio_id else LAST_RUN_PATH
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
