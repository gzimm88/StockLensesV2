from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import threading
import uuid
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy.orm import Session

from backend.api_clients import yahoo_client
from backend.models import (
    CorporateAction,
    LedgerSnapshot,
    Portfolio,
    PortfolioCorrectionEvent,
    PortfolioCoverageEvent,
    PortfolioProcessingRun,
    PortfolioTransaction,
    PriceSnapshot,
    PricesHistory,
    SecurityIdentity,
    SecuritySymbolMap,
    FXRateSnapshot,
    ValuationSnapshot,
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
_LEDGER_ACTION_TYPES = {"SPLIT", "REVERSE_SPLIT", "DIVIDEND", "SPINOFF", "TICKER_CHANGE", "MERGE"}


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
                deleted_at=None,
                version=1,
                is_deleted=False,
            )
        )
        inserted += 1
    db.commit()
    return {"inserted": inserted}


def _normalize_transaction_type(tx_type: str) -> str:
    value = (tx_type or "").strip().upper()
    if value not in {"BUY", "SELL", "DIVIDEND"}:
        raise PortfolioEngineError("Transaction type must be one of BUY, SELL, DIVIDEND.")
    return "Buy" if value == "BUY" else "Sell" if value == "SELL" else "Dividend"


def _normalize_ticker_for_transaction(raw_ticker: str) -> tuple[str, str]:
    ticker_raw = (raw_ticker or "").strip()
    if not ticker_raw:
        raise PortfolioEngineError("Ticker is required.")
    ticker_normalized = ticker_raw.split(":")[-1].strip().upper()
    if not ticker_normalized:
        raise PortfolioEngineError("Ticker is invalid.")
    return ticker_raw, ticker_normalized


def _normalize_corporate_action_type(action_type: str) -> str:
    value = (action_type or "").strip().upper()
    if value not in _LEDGER_ACTION_TYPES:
        raise PortfolioEngineError(
            "Corporate action type must be one of SPLIT, REVERSE_SPLIT, DIVIDEND, SPINOFF, TICKER_CHANGE, MERGE."
        )
    return value


def _sorted_active_transactions(
    db: Session,
    portfolio_id: str,
    *,
    exclude_id: str | None = None,
) -> list[PortfolioTransaction]:
    q = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.deleted_at.is_(None),
        )
    )
    if exclude_id:
        q = q.filter(PortfolioTransaction.id != exclude_id)
    rows = q.all()
    rows.sort(
        key=lambda r: (
            r.trade_date,
            r.created_at or datetime.min,
            r.id,
        )
    )
    return rows


def _sorted_active_corporate_actions(
    db: Session,
    portfolio_id: str,
    *,
    exclude_id: str | None = None,
) -> list[CorporateAction]:
    q = (
        db.query(CorporateAction)
        .filter(
            CorporateAction.portfolio_id == portfolio_id,
            CorporateAction.is_deleted == False,
            CorporateAction.deleted_at.is_(None),
        )
    )
    if exclude_id:
        q = q.filter(CorporateAction.id != exclude_id)
    rows = q.all()
    rows.sort(
        key=lambda r: (
            r.effective_date,
            r.created_at or datetime.min,
            r.id,
        )
    )
    return rows


def _validate_sell_inventory(
    rows: list[PortfolioTransaction],
    *,
    candidate_ticker: str,
    candidate_type: str,
    candidate_shares: float,
    candidate_date: date,
    candidate_created_at: datetime,
) -> None:
    if candidate_type != "Sell":
        return
    relevant: list[tuple[date, datetime, str, float]] = []
    for r in rows:
        if r.ticker_symbol_normalized != candidate_ticker:
            continue
        relevant.append(
            (
                r.trade_date,
                r.created_at or datetime.min,
                r.tx_type,
                float(r.shares or 0.0),
            )
        )
    relevant.append((candidate_date, candidate_created_at, candidate_type, float(candidate_shares)))
    relevant.sort(key=lambda t: (t[0], t[1]))

    shares_open = 0.0
    for _, _, tx_type, shares in relevant:
        if tx_type == "Buy":
            shares_open += shares
        elif tx_type == "Sell":
            if shares > shares_open + 1e-12:
                raise PortfolioEngineError(
                    f"Sell exceeds available shares. Sell={shares}, available={shares_open}."
                )
            shares_open -= shares


def _serialize_metadata(metadata: dict[str, object] | None) -> str | None:
    if metadata is None:
        return None
    return json.dumps(metadata, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _parse_metadata(metadata_json: str | None) -> dict[str, object] | None:
    if not metadata_json:
        return None
    try:
        obj = json.loads(metadata_json)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def list_transactions_for_portfolio(db: Session, portfolio_id: str) -> list[dict[str, object]]:
    get_portfolio_or_error(db, portfolio_id)
    rows = _sorted_active_transactions(db, portfolio_id)
    out: list[dict[str, object]] = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "portfolio_id": r.portfolio_id,
                "ticker": r.ticker_symbol_raw,
                "type": r.tx_type.upper(),
                "quantity": float(r.shares),
                "price": float(r.price),
                "date": r.trade_date.isoformat(),
                "currency": r.currency,
                "version": int(r.version or 1),
                "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                "updated_at": r.updated_at.isoformat() + "Z" if r.updated_at else None,
                "deleted_at": r.deleted_at.isoformat() + "Z" if r.deleted_at else None,
            }
        )
    return out


def list_corporate_actions_for_portfolio(db: Session, portfolio_id: str) -> list[dict[str, object]]:
    get_portfolio_or_error(db, portfolio_id)
    rows = _sorted_active_corporate_actions(db, portfolio_id)
    out: list[dict[str, object]] = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "portfolio_id": r.portfolio_id,
                "ticker": r.ticker,
                "type": r.action_type,
                "effective_date": r.effective_date.isoformat(),
                "factor": float(r.factor) if r.factor is not None else None,
                "cash_amount": float(r.cash_amount) if r.cash_amount is not None else None,
                "metadata": _parse_metadata(r.metadata_json),
                "version": int(r.version or 1),
                "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                "updated_at": r.updated_at.isoformat() + "Z" if r.updated_at else None,
                "deleted_at": r.deleted_at.isoformat() + "Z" if r.deleted_at else None,
            }
        )
    return out


def create_transaction(
    db: Session,
    *,
    portfolio_id: str,
    ticker: str,
    tx_type: str,
    quantity: float,
    price: float,
    trade_date: date,
    currency: str = "USD",
) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    ticker_raw, ticker_normalized = _normalize_ticker_for_transaction(ticker)
    tx_type_norm = _normalize_transaction_type(tx_type)
    qty = float(quantity)
    px = float(price)
    if tx_type_norm in {"Buy", "Sell"} and qty <= 0:
        raise PortfolioEngineError("Quantity must be positive for BUY/SELL.")
    if px <= 0:
        raise PortfolioEngineError("Price must be positive.")

    now = datetime.utcnow()
    existing = _sorted_active_transactions(db, portfolio_id)
    _validate_sell_inventory(
        existing,
        candidate_ticker=ticker_normalized,
        candidate_type=tx_type_norm,
        candidate_shares=qty,
        candidate_date=trade_date,
        candidate_created_at=now,
    )

    sec_id = _ensure_security_mapping(db, ticker_normalized, ticker_raw)
    gross_amount = qty * px if tx_type_norm != "Dividend" else px
    row = PortfolioTransaction(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        security_id=sec_id,
        ticker_symbol_raw=ticker_raw,
        ticker_symbol_normalized=ticker_normalized,
        tx_type=tx_type_norm,
        trade_date=trade_date,
        shares=qty,
        price=px,
        gross_amount=gross_amount,
        currency=(currency or "USD").strip().upper() or "USD",
        metadata_json=None,
        source="manual",
        created_at=now,
        updated_at=now,
        deleted_at=None,
        version=1,
        is_deleted=False,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {
        "id": row.id,
        "portfolio_id": row.portfolio_id,
        "ticker": row.ticker_symbol_raw,
        "type": row.tx_type.upper(),
        "quantity": float(row.shares),
        "price": float(row.price),
        "date": row.trade_date.isoformat(),
        "currency": row.currency,
        "version": int(row.version or 1),
        "created_at": row.created_at.isoformat() + "Z" if row.created_at else None,
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
        "deleted_at": row.deleted_at.isoformat() + "Z" if row.deleted_at else None,
    }


def update_transaction(
    db: Session,
    *,
    transaction_id: str,
    ticker: str,
    tx_type: str,
    quantity: float,
    price: float,
    trade_date: date,
    currency: str = "USD",
) -> dict[str, object]:
    original = (
        db.query(PortfolioTransaction)
        .filter(PortfolioTransaction.id == transaction_id, PortfolioTransaction.is_deleted == False)
        .first()
    )
    if not original:
        raise PortfolioEngineError(f"Transaction '{transaction_id}' not found.")

    portfolio_id = original.portfolio_id
    ticker_raw, ticker_normalized = _normalize_ticker_for_transaction(ticker)
    tx_type_norm = _normalize_transaction_type(tx_type)
    qty = float(quantity)
    px = float(price)
    if tx_type_norm in {"Buy", "Sell"} and qty <= 0:
        raise PortfolioEngineError("Quantity must be positive for BUY/SELL.")
    if px <= 0:
        raise PortfolioEngineError("Price must be positive.")

    now = datetime.utcnow()
    existing = _sorted_active_transactions(db, portfolio_id, exclude_id=original.id)
    _validate_sell_inventory(
        existing,
        candidate_ticker=ticker_normalized,
        candidate_type=tx_type_norm,
        candidate_shares=qty,
        candidate_date=trade_date,
        candidate_created_at=now,
    )

    sec_id = _ensure_security_mapping(db, ticker_normalized, ticker_raw)
    original.is_deleted = True
    original.deleted_at = now
    original.updated_at = now

    row = PortfolioTransaction(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        security_id=sec_id,
        ticker_symbol_raw=ticker_raw,
        ticker_symbol_normalized=ticker_normalized,
        tx_type=tx_type_norm,
        trade_date=trade_date,
        shares=qty,
        price=px,
        gross_amount=(qty * px if tx_type_norm != "Dividend" else px),
        currency=(currency or "USD").strip().upper() or "USD",
        metadata_json=original.metadata_json,
        source=original.source or "manual",
        created_at=now,
        updated_at=now,
        deleted_at=None,
        version=int(original.version or 1) + 1,
        is_deleted=False,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {
        "id": row.id,
        "portfolio_id": row.portfolio_id,
        "ticker": row.ticker_symbol_raw,
        "type": row.tx_type.upper(),
        "quantity": float(row.shares),
        "price": float(row.price),
        "date": row.trade_date.isoformat(),
        "currency": row.currency,
        "version": int(row.version or 1),
        "created_at": row.created_at.isoformat() + "Z" if row.created_at else None,
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
        "deleted_at": row.deleted_at.isoformat() + "Z" if row.deleted_at else None,
    }


def soft_delete_transaction(db: Session, transaction_id: str) -> dict[str, object]:
    row = (
        db.query(PortfolioTransaction)
        .filter(PortfolioTransaction.id == transaction_id, PortfolioTransaction.is_deleted == False)
        .first()
    )
    if not row:
        raise PortfolioEngineError(f"Transaction '{transaction_id}' not found.")
    now = datetime.utcnow()
    row.is_deleted = True
    row.deleted_at = now
    row.updated_at = now
    db.commit()
    return {
        "id": row.id,
        "portfolio_id": row.portfolio_id,
        "deleted_at": now.isoformat() + "Z",
    }


def create_corporate_action(
    db: Session,
    *,
    portfolio_id: str,
    ticker: str,
    action_type: str,
    effective_date: date,
    factor: float | None = None,
    cash_amount: float | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    ticker_raw, ticker_normalized = _normalize_ticker_for_transaction(ticker)
    action_type_norm = _normalize_corporate_action_type(action_type)
    factor_value = float(factor) if factor is not None else None
    cash_value = float(cash_amount) if cash_amount is not None else None

    if action_type_norm in {"SPLIT", "REVERSE_SPLIT"}:
        if factor_value is None or factor_value <= 0:
            raise PortfolioEngineError("Factor must be > 0 for SPLIT and REVERSE_SPLIT actions.")
    if action_type_norm == "DIVIDEND" and cash_value is None:
        raise PortfolioEngineError("cash_amount is required for DIVIDEND action.")

    now = datetime.utcnow()
    row = CorporateAction(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        ticker=ticker_normalized,
        action_type=action_type_norm,
        effective_date=effective_date,
        factor=factor_value,
        cash_amount=cash_value,
        metadata_json=_serialize_metadata(metadata),
        created_at=now,
        updated_at=now,
        deleted_at=None,
        version=1,
        is_deleted=False,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {
        "id": row.id,
        "portfolio_id": row.portfolio_id,
        "ticker": ticker_raw,
        "type": row.action_type,
        "effective_date": row.effective_date.isoformat(),
        "factor": float(row.factor) if row.factor is not None else None,
        "cash_amount": float(row.cash_amount) if row.cash_amount is not None else None,
        "metadata": _parse_metadata(row.metadata_json),
        "version": int(row.version or 1),
        "created_at": row.created_at.isoformat() + "Z" if row.created_at else None,
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
        "deleted_at": row.deleted_at.isoformat() + "Z" if row.deleted_at else None,
    }


def update_corporate_action(
    db: Session,
    *,
    action_id: str,
    ticker: str,
    action_type: str,
    effective_date: date,
    factor: float | None = None,
    cash_amount: float | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    original = (
        db.query(CorporateAction)
        .filter(CorporateAction.id == action_id, CorporateAction.is_deleted == False)
        .first()
    )
    if not original:
        raise PortfolioEngineError(f"Corporate action '{action_id}' not found.")

    ticker_raw, ticker_normalized = _normalize_ticker_for_transaction(ticker)
    action_type_norm = _normalize_corporate_action_type(action_type)
    factor_value = float(factor) if factor is not None else None
    cash_value = float(cash_amount) if cash_amount is not None else None
    if action_type_norm in {"SPLIT", "REVERSE_SPLIT"}:
        if factor_value is None or factor_value <= 0:
            raise PortfolioEngineError("Factor must be > 0 for SPLIT and REVERSE_SPLIT actions.")
    if action_type_norm == "DIVIDEND" and cash_value is None:
        raise PortfolioEngineError("cash_amount is required for DIVIDEND action.")

    now = datetime.utcnow()
    original.is_deleted = True
    original.deleted_at = now
    original.updated_at = now

    row = CorporateAction(
        id=str(uuid.uuid4()),
        portfolio_id=original.portfolio_id,
        ticker=ticker_normalized,
        action_type=action_type_norm,
        effective_date=effective_date,
        factor=factor_value,
        cash_amount=cash_value,
        metadata_json=_serialize_metadata(metadata),
        created_at=now,
        updated_at=now,
        deleted_at=None,
        version=int(original.version or 1) + 1,
        is_deleted=False,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {
        "id": row.id,
        "portfolio_id": row.portfolio_id,
        "ticker": ticker_raw,
        "type": row.action_type,
        "effective_date": row.effective_date.isoformat(),
        "factor": float(row.factor) if row.factor is not None else None,
        "cash_amount": float(row.cash_amount) if row.cash_amount is not None else None,
        "metadata": _parse_metadata(row.metadata_json),
        "version": int(row.version or 1),
        "created_at": row.created_at.isoformat() + "Z" if row.created_at else None,
        "updated_at": row.updated_at.isoformat() + "Z" if row.updated_at else None,
        "deleted_at": row.deleted_at.isoformat() + "Z" if row.deleted_at else None,
    }


def soft_delete_corporate_action(db: Session, action_id: str) -> dict[str, object]:
    row = (
        db.query(CorporateAction)
        .filter(CorporateAction.id == action_id, CorporateAction.is_deleted == False)
        .first()
    )
    if not row:
        raise PortfolioEngineError(f"Corporate action '{action_id}' not found.")
    now = datetime.utcnow()
    row.is_deleted = True
    row.deleted_at = now
    row.updated_at = now
    db.commit()
    return {"id": row.id, "portfolio_id": row.portfolio_id, "deleted_at": now.isoformat() + "Z"}


def _hash_ledger_inputs(
    transactions: list[PortfolioTransaction],
    actions: list[CorporateAction],
) -> str:
    canonical: list[dict[str, object]] = []
    for r in transactions:
        canonical.append(
            {
                "kind": "tx",
                "id": r.id,
                "trade_date": r.trade_date.isoformat(),
                "created_at": (r.created_at or datetime.min).isoformat(),
                "ticker": r.ticker_symbol_normalized,
                "tx_type": r.tx_type,
                "shares": float(r.shares),
                "price": float(r.price),
                "gross_amount": float(r.gross_amount),
                "currency": r.currency,
            }
        )
    for r in actions:
        canonical.append(
            {
                "kind": "action",
                "id": r.id,
                "effective_date": r.effective_date.isoformat(),
                "created_at": (r.created_at or datetime.min).isoformat(),
                "ticker": r.ticker,
                "action_type": r.action_type,
                "factor": float(r.factor) if r.factor is not None else None,
                "cash_amount": float(r.cash_amount) if r.cash_amount is not None else None,
                "metadata_json": r.metadata_json or "",
            }
        )

    canonical.sort(
        key=lambda e: (
            e.get("trade_date") or e.get("effective_date") or "",
            e.get("created_at") or "",
            str(e.get("id") or ""),
        )
    )
    blob = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def rebuild_position_ledger(db: Session, portfolio_id: str) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    tx_rows = _sorted_active_transactions(db, portfolio_id)
    action_rows = _sorted_active_corporate_actions(db, portfolio_id)

    events: list[dict[str, object]] = []
    for tx in tx_rows:
        events.append(
            {
                "kind": "tx",
                "event_date": tx.trade_date,
                "created_at": tx.created_at or datetime.min,
                "id": tx.id,
                "row": tx,
            }
        )
    for action in action_rows:
        events.append(
            {
                "kind": "action",
                "event_date": action.effective_date,
                "created_at": action.created_at or datetime.min,
                "id": action.id,
                "row": action,
            }
        )
    events.sort(key=lambda e: (e["event_date"], e["created_at"], e["id"]))

    qty: dict[str, float] = defaultdict(float)
    basis_value: dict[str, float] = defaultdict(float)
    cash = 0.0

    for event in events:
        if event["kind"] == "tx":
            tx: PortfolioTransaction = event["row"]  # type: ignore[assignment]
            ticker = tx.ticker_symbol_normalized
            shares = float(tx.shares or 0.0)
            price = float(tx.price or 0.0)
            gross = float(tx.gross_amount or (shares * price))
            if tx.tx_type == "Buy":
                qty[ticker] += shares
                basis_value[ticker] += shares * price
                cash -= gross
            elif tx.tx_type == "Sell":
                current_qty = qty[ticker]
                if shares > current_qty + 1e-12:
                    raise PortfolioEngineError(
                        f"Ledger rebuild failed: sell exceeds holdings for {ticker}. Sell={shares}, holdings={current_qty}."
                    )
                avg_cost = (basis_value[ticker] / current_qty) if current_qty > 0 else 0.0
                qty[ticker] = current_qty - shares
                basis_value[ticker] -= avg_cost * shares
                if qty[ticker] <= 1e-12:
                    qty[ticker] = 0.0
                    basis_value[ticker] = 0.0
                cash += gross
            elif tx.tx_type == "Dividend":
                cash += gross
        else:
            action: CorporateAction = event["row"]  # type: ignore[assignment]
            ticker = action.ticker
            if action.action_type == "SPLIT":
                factor = float(action.factor or 0.0)
                if factor <= 0:
                    raise PortfolioEngineError(f"Ledger rebuild failed: invalid split factor for {ticker}.")
                qty[ticker] *= factor
            elif action.action_type == "REVERSE_SPLIT":
                factor = float(action.factor or 0.0)
                if factor <= 0:
                    raise PortfolioEngineError(f"Ledger rebuild failed: invalid reverse split factor for {ticker}.")
                qty[ticker] /= factor
            elif action.action_type == "DIVIDEND":
                if action.cash_amount is None:
                    raise PortfolioEngineError(f"Ledger rebuild failed: dividend action missing cash_amount for {ticker}.")
                cash += float(action.cash_amount)

        # No shorts constraint after every event
        if any(v < -1e-12 for v in qty.values()):
            raise PortfolioEngineError("Ledger rebuild failed: negative holdings detected.")

    holdings: dict[str, float] = {}
    basis: dict[str, float] = {}
    for ticker in sorted(set(list(qty.keys()) + list(basis_value.keys()))):
        q = qty[ticker]
        if q <= 1e-12:
            continue
        holdings[ticker] = q
        basis[ticker] = basis_value[ticker] / q if q > 0 else 0.0

    input_hash = _hash_ledger_inputs(tx_rows, action_rows)
    current_version = (
        db.query(LedgerSnapshot)
        .filter(LedgerSnapshot.portfolio_id == portfolio_id)
        .order_by(LedgerSnapshot.ledger_version.desc())
        .first()
    )
    next_version = int(current_version.ledger_version if current_version else 0) + 1
    now = datetime.utcnow()
    snapshot = LedgerSnapshot(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        ledger_version=next_version,
        as_of=now,
        holdings_json=json.dumps(holdings, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        basis_json=json.dumps(basis, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        cash=float(cash),
        input_hash=input_hash,
        created_at=now,
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)
    return {
        "id": snapshot.id,
        "portfolio_id": snapshot.portfolio_id,
        "ledger_version": snapshot.ledger_version,
        "as_of": snapshot.as_of.isoformat() + "Z",
        "holdings": holdings,
        "basis": basis,
        "cash": float(snapshot.cash or 0.0),
        "input_hash": snapshot.input_hash,
        "created_at": snapshot.created_at.isoformat() + "Z" if snapshot.created_at else None,
    }


def _trading_days_since(last_date: date, as_of_date: date) -> int:
    if last_date >= as_of_date:
        return 0
    d = last_date
    count = 0
    while d < as_of_date:
        d += timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return count


def _latest_ledger_snapshot_or_error(db: Session, portfolio_id: str) -> LedgerSnapshot:
    row = (
        db.query(LedgerSnapshot)
        .filter(LedgerSnapshot.portfolio_id == portfolio_id)
        .order_by(LedgerSnapshot.ledger_version.desc())
        .first()
    )
    if not row:
        raise PortfolioEngineError(
            f"No ledger snapshot found for portfolio '{portfolio_id}'. Run /portfolios/{{id}}/rebuild-ledger first."
        )
    return row


def _ticker_currency_map_or_error(db: Session, portfolio_id: str) -> dict[str, str]:
    rows = _sorted_active_transactions(db, portfolio_id)
    currencies: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        currencies[r.ticker_symbol_normalized].add((r.currency or "").strip().upper() or "USD")
    out: dict[str, str] = {}
    for ticker, values in currencies.items():
        if len(values) > 1:
            raise PortfolioEngineError(
                f"Ticker {ticker} has mixed transaction currencies {sorted(values)}. Explicit currency normalization is required."
            )
        out[ticker] = next(iter(values))
    return out


def _price_hash(*, ticker: str, price: float, currency: str, source: str | None, as_of: date) -> str:
    payload = {
        "ticker": ticker,
        "price": float(price),
        "currency": currency,
        "source": source or "prices_history",
        "as_of": as_of.isoformat(),
    }
    blob = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _fx_hash(*, base_currency: str, quote_currency: str, rate: float, source: str | None, as_of: date) -> str:
    payload = {
        "base_currency": base_currency,
        "quote_currency": quote_currency,
        "rate": float(rate),
        "source": source or "prices_history",
        "as_of": as_of.isoformat(),
    }
    blob = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _resolve_price_snapshot(
    db: Session,
    *,
    portfolio_id: str,
    ticker: str,
    currency: str,
) -> PriceSnapshot:
    row = (
        db.query(PricesHistory)
        .filter(PricesHistory.ticker == ticker)
        .order_by(PricesHistory.date.desc())
        .first()
    )
    if not row or row.close is None or row.date is None:
        raise PortfolioEngineError(f"Missing price for ticker {ticker}.")
    as_of_dt = datetime(row.date.year, row.date.month, row.date.day)
    source = row.source or "prices_history"
    price = float(row.close)
    snap = PriceSnapshot(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        ticker=ticker,
        price=price,
        currency=currency,
        source=source,
        as_of=as_of_dt,
        created_at=datetime.utcnow(),
        input_hash=_price_hash(
            ticker=ticker,
            price=price,
            currency=currency,
            source=source,
            as_of=row.date,
        ),
    )
    db.add(snap)
    return snap


def _resolve_fx_rate_snapshot(
    db: Session,
    *,
    portfolio_id: str,
    base_currency: str,
    quote_currency: str,
) -> FXRateSnapshot:
    direct = f"{quote_currency}{base_currency}=X"
    inverse = f"{base_currency}{quote_currency}=X"

    direct_row = (
        db.query(PricesHistory)
        .filter(PricesHistory.ticker == direct)
        .order_by(PricesHistory.date.desc())
        .first()
    )
    inverse_row = (
        db.query(PricesHistory)
        .filter(PricesHistory.ticker == inverse)
        .order_by(PricesHistory.date.desc())
        .first()
    )

    rate: float | None = None
    as_of: date | None = None
    source: str | None = None

    if direct_row and direct_row.close is not None and direct_row.date is not None:
        rate = float(direct_row.close)
        as_of = direct_row.date
        source = direct_row.source or "prices_history"
    elif inverse_row and inverse_row.close and inverse_row.date is not None:
        if float(inverse_row.close) == 0.0:
            raise PortfolioEngineError(f"Invalid FX inverse rate for pair {inverse}.")
        rate = 1.0 / float(inverse_row.close)
        as_of = inverse_row.date
        source = inverse_row.source or "prices_history"

    if rate is None or as_of is None:
        raise PortfolioEngineError(
            f"Missing FX conversion from {quote_currency} to {base_currency}. Explicit FX snapshot is required."
        )
    if rate <= 0:
        raise PortfolioEngineError(
            f"Invalid FX conversion from {quote_currency} to {base_currency}. rate={rate}"
        )

    snap = FXRateSnapshot(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        base_currency=base_currency,
        quote_currency=quote_currency,
        rate=float(rate),
        as_of=datetime(as_of.year, as_of.month, as_of.day),
        source=source,
        created_at=datetime.utcnow(),
        input_hash=_fx_hash(
            base_currency=base_currency,
            quote_currency=quote_currency,
            rate=float(rate),
            source=source,
            as_of=as_of,
        ),
    )
    db.add(snap)
    return snap


def rebuild_valuation_snapshot(
    db: Session,
    portfolio_id: str,
    *,
    strict: bool = False,
    stale_trading_days: int = 3,
) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    ledger = _latest_ledger_snapshot_or_error(db, portfolio_id)
    holdings = json.loads(ledger.holdings_json or "{}")
    if not isinstance(holdings, dict):
        raise PortfolioEngineError("Invalid ledger snapshot holdings payload.")

    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    valuation_date = date.today()

    price_snaps: list[PriceSnapshot] = []
    fx_snaps: list[FXRateSnapshot] = []
    fx_by_quote: dict[str, FXRateSnapshot] = {}
    stale_tickers: list[str] = []
    excluded_tickers: list[str] = []
    components: list[dict[str, object]] = []
    nav = Decimal("0")

    sorted_tickers = sorted((str(k).upper(), float(v)) for k, v in holdings.items() if float(v) > 0.0)
    for ticker, qty_float in sorted_tickers:
        currency = ticker_currency.get(ticker, base_currency)
        price_snap = _resolve_price_snapshot(
            db,
            portfolio_id=portfolio_id,
            ticker=ticker,
            currency=currency,
        )
        price_snaps.append(price_snap)

        stale_days = _trading_days_since(price_snap.as_of.date(), valuation_date)
        is_stale = stale_days > int(stale_trading_days)
        if is_stale:
            stale_tickers.append(ticker)
            if strict:
                raise PortfolioEngineError(
                    f"Stale price for {ticker}: {price_snap.as_of.date().isoformat()} ({stale_days} trading days old)."
                )
            excluded_tickers.append(ticker)
            components.append(
                {
                    "ticker": ticker,
                    "quantity": qty_float,
                    "price": price_snap.price,
                    "currency": currency,
                    "included": False,
                    "reason": "stale_price",
                    "stale_trading_days": stale_days,
                }
            )
            continue

        fx_rate = Decimal("1")
        fx_source = None
        fx_as_of = None
        if currency != base_currency:
            fx = fx_by_quote.get(currency)
            if fx is None:
                fx = _resolve_fx_rate_snapshot(
                    db,
                    portfolio_id=portfolio_id,
                    base_currency=base_currency,
                    quote_currency=currency,
                )
                fx_by_quote[currency] = fx
                fx_snaps.append(fx)
            fx_rate = Decimal(str(fx.rate))
            fx_source = fx.source
            fx_as_of = fx.as_of.date().isoformat()

        qty = Decimal(str(qty_float))
        price = Decimal(str(price_snap.price))
        position_value = (qty * price * fx_rate).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        nav += position_value
        components.append(
            {
                "ticker": ticker,
                "quantity": float(qty),
                "price": float(price),
                "currency": currency,
                "included": True,
                "fx_rate": float(fx_rate),
                "fx_source": fx_source,
                "fx_as_of": fx_as_of,
                "value_base": float(position_value),
            }
        )

    valuation_hash_payload = {
        "ledger_input_hash": ledger.input_hash,
        "price_hashes": sorted(s.input_hash for s in price_snaps),
        "fx_hashes": sorted(s.input_hash for s in fx_snaps),
        "base_currency": base_currency,
        "strict": bool(strict),
        "stale_trading_days": int(stale_trading_days),
        "excluded_tickers": sorted(excluded_tickers),
    }
    valuation_input_hash = hashlib.sha256(
        json.dumps(valuation_hash_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    as_of_candidates = [s.as_of for s in price_snaps] + [s.as_of for s in fx_snaps]
    as_of = max(as_of_candidates) if as_of_candidates else datetime.utcnow()
    now = datetime.utcnow()
    nav_float = float(nav.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
    snapshot = ValuationSnapshot(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        ledger_snapshot_id=ledger.id,
        nav=nav_float,
        currency=base_currency,
        as_of=as_of,
        created_at=now,
        input_hash=valuation_input_hash,
        components_json=json.dumps(components, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)

    return {
        "valuation_snapshot_id": snapshot.id,
        "portfolio_id": portfolio_id,
        "ledger_snapshot_id": ledger.id,
        "nav": snapshot.nav,
        "currency": snapshot.currency,
        "as_of": snapshot.as_of.isoformat() + "Z",
        "input_hash": snapshot.input_hash,
        "strict": bool(strict),
        "stale_threshold_trading_days": int(stale_trading_days),
        "stale_tickers": sorted(stale_tickers),
        "excluded_tickers": sorted(excluded_tickers),
        "price_snapshot_count": len(price_snaps),
        "fx_snapshot_count": len(fx_snaps),
        "components": components,
    }


def _load_portfolio_transactions_from_db(db: Session, portfolio_id: str) -> list[Transaction]:
    rows = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.deleted_at.is_(None),
        )
        .order_by(PortfolioTransaction.trade_date.asc(), PortfolioTransaction.created_at.asc(), PortfolioTransaction.id.asc())
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
