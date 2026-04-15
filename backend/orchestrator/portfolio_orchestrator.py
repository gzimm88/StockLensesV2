from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from bisect import bisect_right
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy.orm import Session

from backend.api_clients import yahoo_client
from backend.models import (
    ClosedPosition,
    CorporateAction,
    DividendEvent,
    FXRate,
    FXRateSnapshot,
    LedgerSnapshot,
    Portfolio,
    PortfolioCorrectionEvent,
    PortfolioCoverageEvent,
    PortfolioEquityHistoryBuild,
    PortfolioEquityHistoryRow,
    PortfolioSnapshot,
    PortfolioProcessingRun,
    PortfolioSettings,
    PortfolioTransaction,
    PriceHistory,
    PriceSnapshot,
    PricesHistory,
    SecurityIdentity,
    SecuritySymbolMap,
    TickerMetadata,
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
_DECIMAL_MONEY_SCALE = Decimal("0.0000000001")
_DECIMAL_RATE_SCALE = Decimal("0.000000000001")
_EQUITY_ENGINE_VERSION = "phase14-v1"
<<<<<<< HEAD
_US_MARKET_EXCHANGES = {"NYSE", "NASDAQ", "NASDAQGS", "NASDAQGM", "NASDAQCM", "AMEX", "BATS", "ARCA"}
_EU_MARKET_EXCHANGES = {"ENXTAM", "ENXTPA", "XPAR", "AMS"}
=======
>>>>>>> origin/main

_EXCHANGE_NATIVE_CCY: dict[str, str] = {
    "NYSE": "USD",
    "NASDAQ": "USD",
    "NASDAQGS": "USD",
    "NASDAQGM": "USD",
    "NASDAQCM": "USD",
    "AMEX": "USD",
    "BATS": "USD",
    "ARCA": "USD",
    "ENXTAM": "EUR",
    "AMS": "EUR",
    "ENXTPA": "EUR",
    "XPAR": "EUR",
    "XETR": "EUR",
    "XTRA": "EUR",
    "FRA": "EUR",
    "BIT": "EUR",
    "XMIL": "EUR",
    "MCE": "EUR",
    "BME": "EUR",
    "SWX": "CHF",
    "LSE": "GBP",
    "XLON": "GBP",
    "TSX": "CAD",
    "TSE": "JPY",
    "JPX": "JPY",
    "ASX": "AUD",
    "HKEX": "HKD",
    "HKSE": "HKD",
    "BVMF": "BRL",
    "SSE": "CNY",
    "SZSE": "CNY",
}


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

    # If ticker already ends in a known Yahoo exchange suffix (e.g. MC.PA, ASML.AS),
    # don't mangle the dot — pass it through unchanged.
    known_suffixes = set(YAHOO_SUFFIX_BY_EXCHANGE.values())
    upper_t = ticker.strip().upper()
    for suffix in known_suffixes:
        if upper_t.endswith(suffix):
            return upper_t, None
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


async def refresh_market_data_for_portfolio(db: Session, portfolio_id: str) -> dict[str, object]:
    """
    Refresh persisted market data inputs for open positions.
    This does not run valuation/rebuild; it only refreshes stored price/FX/dividend inputs.
    """
    portfolio = get_portfolio_or_error(db, portfolio_id)
    tx_rows = _sorted_active_transactions_for_window(db, portfolio_id, include_generated=False)
    if not tx_rows:
        return {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio.name,
            "requested_tickers": [],
            "fetched_tickers": [],
            "already_covered_tickers": [],
            "fx_backfill": backfill_fx_history_if_missing(portfolio_id, db),
            "dividend_backfill": backfill_dividend_history_if_missing(portfolio_id, db, strict=False),
        }

    _ensure_ticker_metadata_from_transactions(db, tx_rows)
    db.commit()

    by_ticker: dict[str, dict[str, object]] = {}
    for r in tx_rows:
        ticker = (r.ticker_symbol_normalized or "").strip().upper()
        if not ticker:
            continue
        row = by_ticker.setdefault(
            ticker,
            {
                "first_trade_date": r.trade_date,
                "net_shares": Decimal("0"),
                "source_symbol": (r.ticker_symbol_raw or ticker),
            },
        )
        if r.trade_date < row["first_trade_date"]:
            row["first_trade_date"] = r.trade_date
        shares = _to_decimal(r.shares)
        tx_type = (r.tx_type or "").strip().upper()
        if tx_type == "BUY":
            row["net_shares"] = _to_decimal(row["net_shares"]) + shares
        elif tx_type == "SELL":
            row["net_shares"] = _to_decimal(row["net_shares"]) - shares

    open_tickers = sorted([t for t, s in by_ticker.items() if _to_decimal(s["net_shares"]) > Decimal("0")])
    if not open_tickers:
        return {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio.name,
            "requested_tickers": [],
            "fetched_tickers": [],
            "already_covered_tickers": [],
            "fx_backfill": backfill_fx_history_if_missing(portfolio_id, db),
            "dividend_backfill": backfill_dividend_history_if_missing(portfolio_id, db, strict=False),
        }

    ticker_start_dates = {t: by_ticker[t]["first_trade_date"] for t in open_tickers}
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    earliest = min(ticker_start_dates.values())
    today_utc = datetime.now(timezone.utc).date()
    refresh_start = max(earliest, today_utc - timedelta(days=14))
    coverage = await ensure_price_coverage(
        db,
        open_tickers,
        earliest,
        ticker_start_dates=ticker_start_dates,
        ticker_source_symbols={t: str(by_ticker[t]["source_symbol"]) for t in open_tickers},
    )
    refreshed_tickers: list[str] = list(coverage.fetched_tickers)
    live_refreshed_tickers: list[str] = []
    closed_market_tickers: list[str] = []
    live_fx_refreshed_pairs: list[str] = []
    closed_market_fx_pairs: list[str] = []
    refresh_warnings: list[str] = []

    def _latest_intraday_quotes(symbols: list[str]) -> tuple[dict[str, tuple[datetime, float]], dict[str, tuple[date, float]]]:
        import yfinance as yf

        if not symbols:
            return {}, {}
        symbol_list = sorted(set(symbols))
        quotes: dict[str, tuple[datetime, float]] = {}
        prev_closes: dict[str, tuple[date, float]] = {}
        try:
            hist = yf.download(
                tickers=" ".join(symbol_list),
                period="5d",
                interval="5m",
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=False,
            )
        except Exception:
            return {}, {}
        if hist is None or hist.empty:
            return {}, {}

        def _normalize_timestamp(ts_value) -> datetime | None:
            if ts_value is None:
                return None
            if hasattr(ts_value, "to_pydatetime"):
                ts = ts_value.to_pydatetime()
            elif isinstance(ts_value, datetime):
                ts = ts_value
            else:
                return None
            if ts.tzinfo is not None:
                return ts.astimezone(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
            return ts.replace(second=0, microsecond=0)

        try:
            columns = list(hist.columns)
        except Exception:
            return {}, {}

        is_multi = bool(columns and hasattr(columns[0], "__len__") and not isinstance(columns[0], str))

        if is_multi:
            for symbol in symbol_list:
                try:
                    frame = hist[symbol]
                except Exception:
                    continue
                if frame is None or frame.empty or "Close" not in frame.columns:
                    continue
                close_series = frame["Close"].dropna()
                if close_series.empty:
                    continue
                ts_utc = _normalize_timestamp(close_series.index[-1])
                if ts_utc is None:
                    continue
                try:
                    quotes[symbol] = (ts_utc, float(close_series.iloc[-1]))
                except Exception:
                    continue
                try:
                    daily_last = close_series.groupby(close_series.index.date).last()
                    latest_day = ts_utc.date()
                    prev_days = [d for d in daily_last.index if d < latest_day]
                    if prev_days:
                        prev_day = max(prev_days)
                        prev_closes[symbol] = (prev_day, float(daily_last.loc[prev_day]))
                except Exception:
                    pass
            return quotes, prev_closes

        if "Close" not in hist.columns:
            return {}, {}
        close_series = hist["Close"].dropna()
        if close_series.empty:
            return {}, {}
        ts_utc = _normalize_timestamp(close_series.index[-1])
        if ts_utc is None:
            return {}, {}
        try:
            quotes[symbol_list[0]] = (ts_utc, float(close_series.iloc[-1]))
        except Exception:
            return {}, {}
        try:
            daily_last = close_series.groupby(close_series.index.date).last()
            latest_day = ts_utc.date()
            prev_days = [d for d in daily_last.index if d < latest_day]
            if prev_days:
                prev_day = max(prev_days)
                prev_closes[symbol_list[0]] = (prev_day, float(daily_last.loc[prev_day]))
        except Exception:
            pass
        return quotes, prev_closes

    def _latest_intraday_fx(
        quote_currencies: list[str],
        *,
        base_currency: str,
    ) -> tuple[dict[str, tuple[datetime, float]], dict[str, tuple[date, float]]]:
        import yfinance as yf

        quotes = sorted(
            {
                (q or "").strip().upper()
                for q in quote_currencies
                if (q or "").strip().upper() and (q or "").strip().upper() != base_currency
            }
        )
        if not quotes:
            return {}, {}
        symbols = [f"{q}{base_currency}=X" for q in quotes]
        latest_by_quote: dict[str, tuple[datetime, float]] = {}
        prev_by_quote: dict[str, tuple[date, float]] = {}
        prev_close_cutoff_time = dt_time(16, 30)

        def _pick_prev_session_close(
            close_series,
            prev_day: date,
        ) -> float | None:
            try:
                day_points = []
                for idx, value in close_series.items():
                    if value is None:
                        continue
                    point_date = idx.date()
                    if point_date != prev_day:
                        continue
                    point_time = idx.time()
                    day_points.append((point_time, float(value)))
                if not day_points:
                    return None
                day_points.sort(key=lambda item: item[0])
                at_or_before_cutoff = [v for t, v in day_points if t <= prev_close_cutoff_time]
                if at_or_before_cutoff:
                    return at_or_before_cutoff[-1]
                return day_points[-1][1]
            except Exception:
                return None

        def _fallback_prev_close(symbol: str, latest_day: date | None) -> tuple[date, float] | None:
            try:
                daily = yf.Ticker(symbol).history(
                    period="10d",
                    interval="1d",
                    auto_adjust=False,
                    actions=False,
                )
            except Exception:
                return None
            if daily is None or daily.empty or "Close" not in daily.columns:
                return None
            close_series = daily["Close"].dropna()
            if close_series.empty:
                return None
            try:
                ordered = sorted(
                    [(idx.date(), float(v)) for idx, v in close_series.items() if v is not None],
                    key=lambda x: x[0],
                )
            except Exception:
                return None
            if not ordered:
                return None
            if latest_day is not None:
                candidates = [row for row in ordered if row[0] < latest_day]
                if candidates:
                    return candidates[-1]
            # fallback: use the latest available daily close
            return ordered[-1]

        try:
            hist = yf.download(
                tickers=" ".join(symbols),
                period="5d",
                interval="5m",
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=False,
            )
        except Exception:
            return {}, {}
        if hist is None or hist.empty:
            return {}, {}

        def _normalize_timestamp(ts_value) -> datetime | None:
            if ts_value is None:
                return None
            if hasattr(ts_value, "to_pydatetime"):
                ts = ts_value.to_pydatetime()
            elif isinstance(ts_value, datetime):
                ts = ts_value
            else:
                return None
            if ts.tzinfo is not None:
                return ts.astimezone(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
            return ts.replace(second=0, microsecond=0)

        try:
            columns = list(hist.columns)
        except Exception:
            return {}, {}
        is_multi = bool(columns and hasattr(columns[0], "__len__") and not isinstance(columns[0], str))

        if is_multi:
            for quote, symbol in zip(quotes, symbols):
                try:
                    frame = hist[symbol]
                except Exception:
                    continue
                if frame is None or frame.empty or "Close" not in frame.columns:
                    continue
                close_series = frame["Close"].dropna()
                if close_series.empty:
                    continue
                ts_utc = _normalize_timestamp(close_series.index[-1])
                if ts_utc is None:
                    continue
                try:
                    latest_by_quote[quote] = (ts_utc, float(close_series.iloc[-1]))
                except Exception:
                    continue
                try:
                    daily_last = close_series.groupby(close_series.index.date).last()
                    latest_day = ts_utc.date()
                    prev_days = [d for d in daily_last.index if d < latest_day]
                    if prev_days:
                        prev_day = max(prev_days)
                        session_prev = _pick_prev_session_close(close_series, prev_day)
                        if session_prev is not None:
                            prev_by_quote[quote] = (prev_day, float(session_prev))
                        else:
                            prev_by_quote[quote] = (prev_day, float(daily_last.loc[prev_day]))
                except Exception:
                    pass
                if quote not in prev_by_quote:
                    fallback_prev = _fallback_prev_close(symbol, ts_utc.date())
                    if fallback_prev is not None:
                        prev_by_quote[quote] = fallback_prev
            return latest_by_quote, prev_by_quote

        if "Close" not in hist.columns or len(quotes) != 1:
            return {}, {}
        close_series = hist["Close"].dropna()
        if close_series.empty:
            return {}, {}
        ts_utc = _normalize_timestamp(close_series.index[-1])
        if ts_utc is None:
            return {}, {}
        quote = quotes[0]
        try:
            latest_by_quote[quote] = (ts_utc, float(close_series.iloc[-1]))
        except Exception:
            return {}, {}
        try:
            daily_last = close_series.groupby(close_series.index.date).last()
            latest_day = ts_utc.date()
            prev_days = [d for d in daily_last.index if d < latest_day]
            if prev_days:
                prev_day = max(prev_days)
                session_prev = _pick_prev_session_close(close_series, prev_day)
                if session_prev is not None:
                    prev_by_quote[quote] = (prev_day, float(session_prev))
                else:
                    prev_by_quote[quote] = (prev_day, float(daily_last.loc[prev_day]))
        except Exception:
            pass
        if quote not in prev_by_quote:
            fallback_prev = _fallback_prev_close(symbols[0], ts_utc.date())
            if fallback_prev is not None:
                prev_by_quote[quote] = fallback_prev
        return latest_by_quote, prev_by_quote

    fetch_symbols_by_ticker: dict[str, str] = {}
    for ticker in open_tickers:
        fetch_symbol, map_warning = _to_fetch_symbol(
            ticker,
            str(by_ticker[ticker]["source_symbol"]),
        )
        fetch_symbols_by_ticker[ticker] = fetch_symbol
        if map_warning:
            refresh_warnings.append(map_warning)
            logger.warning("[PortfolioRefresh] %s", map_warning)

    live_quotes, prev_session_closes = _latest_intraday_quotes(list(fetch_symbols_by_ticker.values()))
    for ticker in open_tickers:
        fetch_symbol = fetch_symbols_by_ticker.get(ticker)
        latest_live = live_quotes.get(fetch_symbol) if fetch_symbol else None
        if latest_live is None:
            closed_market_tickers.append(ticker)
            continue
        ts_utc, px_live = latest_live
        if ts_utc.date() != today_utc:
            closed_market_tickers.append(ticker)
            continue
        if insert_price_history_point(
            db,
            ticker=ticker,
            datetime_utc=ts_utc,
            price=px_live,
            adjusted_price=None,
            source="yahoo_live_refresh",
        ):
            live_refreshed_tickers.append(ticker)
        prev_close = prev_session_closes.get(fetch_symbol) if fetch_symbol else None
        if prev_close is not None:
            prev_day, prev_px = prev_close
            prev_ts = datetime(prev_day.year, prev_day.month, prev_day.day, 20, 0, 0)
            insert_price_history_point(
                db,
                ticker=ticker,
                datetime_utc=prev_ts,
                price=prev_px,
                adjusted_price=None,
                source="yahoo_prev_close_refresh",
            )

    fx_quotes, fx_prev_closes = _latest_intraday_fx(
        [ticker_currency.get(t, base_currency) for t in open_tickers],
        base_currency=base_currency,
    )
    for quote_ccy in sorted({(ticker_currency.get(t, base_currency) or "").strip().upper() for t in open_tickers}):
        if not quote_ccy or quote_ccy == base_currency:
            continue
        latest_fx = fx_quotes.get(quote_ccy)
        if latest_fx is None:
            closed_market_fx_pairs.append(f"{quote_ccy}->{base_currency}")
            continue
        ts_utc, fx_live = latest_fx
        if ts_utc.date() != today_utc:
            closed_market_fx_pairs.append(f"{quote_ccy}->{base_currency}")
            continue
        if insert_fx_rate_point(
            db,
            base_currency=base_currency,
            quote_currency=quote_ccy,
            datetime_utc=ts_utc,
            rate=fx_live,
            source="yahoo_live_refresh",
        ):
            live_fx_refreshed_pairs.append(f"{quote_ccy}->{base_currency}")
        prev_fx = fx_prev_closes.get(quote_ccy)
        if prev_fx is not None:
            prev_day, prev_rate = prev_fx
            # Store explicit prior-session FX close at EU close anchor time so
            # day-change baseline can prefer session-close FX vs end-of-day rows.
            prev_ts = datetime(prev_day.year, prev_day.month, prev_day.day, 16, 30, 0)
            insert_fx_rate_point(
                db,
                base_currency=base_currency,
                quote_currency=quote_ccy,
                datetime_utc=prev_ts,
                rate=prev_rate,
                source="yahoo_prev_close_refresh",
            )
    fx_backfill = backfill_fx_history_if_missing(portfolio_id, db, to_date_override=today_utc)
    dividend_backfill = backfill_dividend_history_if_missing(portfolio_id, db, strict=False)
    return {
        "portfolio_id": portfolio_id,
        "portfolio_name": portfolio.name,
        "requested_tickers": coverage.requested_tickers,
        "fetched_tickers": coverage.fetched_tickers,
        "already_covered_tickers": coverage.already_covered_tickers,
        "coverage_start": coverage.coverage_start.isoformat(),
        "coverage_end": coverage.coverage_end.isoformat(),
        "warnings": [*coverage.warnings, *refresh_warnings],
        "refreshed_tickers": refreshed_tickers,
        "live_refreshed_tickers": sorted(set(live_refreshed_tickers)),
        "closed_market_tickers": sorted(set(closed_market_tickers)),
        "live_fx_refreshed_pairs": sorted(set(live_fx_refreshed_pairs)),
        "closed_market_fx_pairs": sorted(set(closed_market_fx_pairs)),
        "refresh_start": refresh_start.isoformat(),
        "fx_backfill": fx_backfill,
        "dividend_backfill": dividend_backfill,
    }


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
    now = datetime.utcnow()
    raw_has_exchange = bool(raw_symbol and ":" in raw_symbol)

    mapped = None
    if raw_symbol:
        mapped = db.query(SecuritySymbolMap).filter(SecuritySymbolMap.raw_input_symbol == raw_symbol).first()

    sec_id = _security_id(normalized_symbol, exchange)

    # For unqualified raw symbols (e.g. "MSFT"), reuse existing canonical identity if available.
    if not raw_has_exchange:
        if mapped is not None:
            sec_id = mapped.security_id
        else:
            normalized_rows = (
                db.query(SecurityIdentity)
                .filter(SecurityIdentity.normalized_symbol == normalized_symbol)
                .order_by(SecurityIdentity.created_at.asc(), SecurityIdentity.security_id.asc())
                .all()
            )
            if len(normalized_rows) == 1:
                sec_id = normalized_rows[0].security_id

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
                    exchange=ident.exchange,
                    mic=ident.mic,
                    vendor_symbol=vendor_symbol,
                    security_id=sec_id,
                    created_at=now,
                    updated_at=now,
                )
            )
        else:
            mapped.updated_at = now
            mapped.vendor_symbol = mapped.vendor_symbol or vendor_symbol
            mapped.normalized_symbol = mapped.normalized_symbol or normalized_symbol
            mapped.exchange = mapped.exchange or ident.exchange
            mapped.mic = mapped.mic or ident.mic
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
    portfolio = get_portfolio_or_error(db, portfolio_id)
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
        tx_currency = _resolve_native_currency_for_ticker(
            db,
            ticker_raw=tx.ticker_symbol,
            ticker_normalized=tx.ticker,
            fallback_currency=tx.currency,
        )
        fx_at_execution, gross_amount_base = _compute_tx_booking_facts(
            db,
            portfolio=portfolio,
            currency=tx_currency,
            gross_amount_local=tx.cost,
            trade_date=tx.trade_date,
        )
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
                fx_at_execution=fx_at_execution,
                gross_amount_base=gross_amount_base,
                is_generated=False,
                generated_event_id=None,
                currency=tx_currency,
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


def _parse_exchange_from_raw_ticker(raw_ticker: str) -> str | None:
    value = (raw_ticker or "").strip()
    if ":" not in value:
        return None
    exchange = value.split(":", 1)[0].strip().upper()
    return exchange or None


def _upsert_ticker_metadata(
    db: Session,
    *,
    ticker_normalized: str,
    exchange: str | None,
    native_currency: str,
) -> None:
    now = datetime.utcnow()
    row = db.query(TickerMetadata).filter(TickerMetadata.ticker_normalized == ticker_normalized).first()
    native = (native_currency or "USD").strip().upper() or "USD"
    if row is None:
        db.add(
            TickerMetadata(
                ticker_normalized=ticker_normalized,
                exchange=(exchange or "").strip().upper() or None,
                native_currency=native,
                created_at=now,
                updated_at=now,
            )
        )
        return
    row.exchange = (exchange or row.exchange or "").strip().upper() or None
    row.native_currency = native
    row.updated_at = now


def _resolve_native_currency_for_ticker(
    db: Session,
    *,
    ticker_raw: str,
    ticker_normalized: str,
    fallback_currency: str,
) -> str:
    existing = db.query(TickerMetadata).filter(TickerMetadata.ticker_normalized == ticker_normalized).first()
    if existing and existing.native_currency:
        return (existing.native_currency or "USD").strip().upper() or "USD"

    exchange = _parse_exchange_from_raw_ticker(ticker_raw)
    native = None
    if exchange:
        native = _EXCHANGE_NATIVE_CCY.get(exchange)
        if native is None:
            raise PortfolioEngineError(
                f"Missing native currency mapping for exchange '{exchange}' ({ticker_raw})."
            )
    if native is None:
        native = (fallback_currency or "USD").strip().upper() or "USD"

    _upsert_ticker_metadata(
        db,
        ticker_normalized=ticker_normalized,
        exchange=exchange,
        native_currency=native,
    )
    return native


def _ensure_ticker_metadata_from_transactions(db: Session, rows: list[PortfolioTransaction]) -> None:
    pending_seen: set[str] = set()
    for tx in rows:
        ticker_norm = (tx.ticker_symbol_normalized or "").strip().upper()
        if not ticker_norm or ticker_norm in pending_seen:
            continue
        pending_seen.add(ticker_norm)
        existing = db.query(TickerMetadata).filter(TickerMetadata.ticker_normalized == ticker_norm).first()
        if existing is not None and existing.native_currency:
            continue
        exchange = _parse_exchange_from_raw_ticker(tx.ticker_symbol_raw)
        native = None
        if exchange:
            native = _EXCHANGE_NATIVE_CCY.get(exchange)
            if native is None:
                raise PortfolioEngineError(
                    f"Missing native currency mapping for exchange '{exchange}' ({tx.ticker_symbol_raw})."
                )
        if native is None:
            native = (tx.currency or "USD").strip().upper() or "USD"
        _upsert_ticker_metadata(
            db,
            ticker_normalized=ticker_norm,
            exchange=exchange,
            native_currency=native,
        )
    db.flush()


def _validate_portfolio_dividend_withholding(portfolio: Portfolio) -> None:
    if not bool(portfolio.apply_dividend_withholding):
        return
    pct = float(portfolio.dividend_withholding_percent or 0.0)
    if pct < 0.0 or pct > 100.0:
        raise PortfolioEngineError("dividend_withholding_percent must be between 0 and 100 when withholding is enabled.")


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


def _lookup_close_fx_rate(
    db: Session,
    *,
    quote_currency: str,
    base_currency: str,
    on_date: date,
) -> Decimal:
    quote = (quote_currency or "").strip().upper()
    base = (base_currency or "").strip().upper()
    if not quote or not base:
        raise PortfolioEngineError("Invalid currency for FX lookup.")
    if quote == base:
        return Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)

    row_fx = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base,
            FXRate.quote_currency == quote,
            FXRate.datetime_utc <= datetime(on_date.year, on_date.month, on_date.day, 23, 59, 59),
        )
        .order_by(FXRate.datetime_utc.desc())
        .first()
    )
    if row_fx and row_fx.rate is not None:
        return Decimal(str(float(row_fx.rate))).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)

    # Backward-compatible fallback for older DBs still storing FX in PricesHistory.
    pair_candidates = [f"{quote}{base}=X", f"{quote}{base}"]
    row_legacy = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker.in_(pair_candidates),
            PricesHistory.date <= on_date,
        )
        .order_by(PricesHistory.date.desc())
        .first()
    )
    if row_legacy and row_legacy.close is not None:
        return Decimal(str(float(row_legacy.close))).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)

    raise PortfolioEngineError(
        f"Missing execution FX conversion from {quote} to {base} on or before {on_date.isoformat()}."
    )


def _compute_tx_booking_facts(
    db: Session,
    *,
    portfolio: Portfolio,
    currency: str,
    gross_amount_local: float,
    trade_date: date,
) -> tuple[float, float]:
    fx = _lookup_close_fx_rate(
        db,
        quote_currency=(currency or "USD"),
        base_currency=(portfolio.base_currency or "USD"),
        on_date=trade_date,
    )
    gross_local = Decimal(str(float(gross_amount_local)))
    gross_base = (gross_local * fx).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    return float(fx), float(gross_base)


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
                "gross_amount_base": float(r.gross_amount_base or 0.0),
                "fx_at_execution": float(r.fx_at_execution or 1.0),
                "date": r.trade_date.isoformat(),
                "currency": r.currency,
                "version": int(r.version or 1),
                "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
                "updated_at": r.updated_at.isoformat() + "Z" if r.updated_at else None,
                "deleted_at": r.deleted_at.isoformat() + "Z" if r.deleted_at else None,
                "is_generated": bool(r.is_generated),
                "generated_event_id": r.generated_event_id,
                "metadata": _parse_metadata(r.metadata_json),
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
    portfolio = get_portfolio_or_error(db, portfolio_id)
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
    tx_currency = _resolve_native_currency_for_ticker(
        db,
        ticker_raw=ticker_raw,
        ticker_normalized=ticker_normalized,
        fallback_currency=currency,
    )
    fx_at_execution, gross_amount_base = _compute_tx_booking_facts(
        db,
        portfolio=portfolio,
        currency=tx_currency,
        gross_amount_local=gross_amount,
        trade_date=trade_date,
    )
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
        fx_at_execution=fx_at_execution,
        gross_amount_base=gross_amount_base,
        is_generated=False,
        generated_event_id=None,
        currency=tx_currency,
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

    portfolio = get_portfolio_or_error(db, portfolio_id)
    tx_currency = _resolve_native_currency_for_ticker(
        db,
        ticker_raw=ticker_raw,
        ticker_normalized=ticker_normalized,
        fallback_currency=currency,
    )
    gross_amount = qty * px if tx_type_norm != "Dividend" else px
    fx_at_execution, gross_amount_base = _compute_tx_booking_facts(
        db,
        portfolio=portfolio,
        currency=tx_currency,
        gross_amount_local=gross_amount,
        trade_date=trade_date,
    )

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
        fx_at_execution=fx_at_execution,
        gross_amount_base=gross_amount_base,
        is_generated=False,
        generated_event_id=None,
        currency=tx_currency,
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


def _ledger_snapshot_by_id(db: Session, ledger_snapshot_id: str | None) -> LedgerSnapshot | None:
    if not ledger_snapshot_id:
        return None
    return db.query(LedgerSnapshot).filter(LedgerSnapshot.id == ledger_snapshot_id).first()


def _ticker_currency_map_or_error(db: Session, portfolio_id: str) -> dict[str, str]:
    rows = _sorted_active_transactions(db, portfolio_id)
    currencies: dict[str, set[str]] = defaultdict(set)
    sample_raw: dict[str, str] = {}
    for r in rows:
        currencies[r.ticker_symbol_normalized].add((r.currency or "").strip().upper() or "USD")
        sample_raw.setdefault(r.ticker_symbol_normalized, r.ticker_symbol_raw)
    metadata_rows = (
        db.query(TickerMetadata)
        .filter(TickerMetadata.ticker_normalized.in_(list(currencies.keys()) or [""]))
        .all()
    )
    metadata_map = {
        (m.ticker_normalized or "").strip().upper(): (m.native_currency or "").strip().upper()
        for m in metadata_rows
        if m.ticker_normalized and m.native_currency
    }
    out: dict[str, str] = {}
    for ticker, values in currencies.items():
        meta_ccy = metadata_map.get((ticker or "").strip().upper())
        if meta_ccy:
            out[ticker] = meta_ccy
            continue
        raw = sample_raw.get(ticker, ticker)
        exchange = _parse_exchange_from_raw_ticker(raw)
        if exchange:
            inferred = _EXCHANGE_NATIVE_CCY.get(exchange)
            if inferred is None:
                raise PortfolioEngineError(
                    f"Missing native currency mapping for exchange '{exchange}' ({raw})."
                )
            out[ticker] = inferred
            continue
        if len(values) > 1:
            raise PortfolioEngineError(
                f"Ticker {ticker} has mixed transaction currencies {sorted(values)}. Explicit currency normalization is required."
            )
        out[ticker] = next(iter(values))
    return out


def _ticker_exchange_map_or_none(db: Session, portfolio_id: str) -> dict[str, str]:
    rows = _sorted_active_transactions(db, portfolio_id)
    sample_raw: dict[str, str] = {}
    tickers: list[str] = []
    for r in rows:
        ticker = (r.ticker_symbol_normalized or "").strip().upper()
        if not ticker:
            continue
        if ticker not in sample_raw:
            sample_raw[ticker] = r.ticker_symbol_raw
            tickers.append(ticker)
    if not tickers:
        return {}
    metadata_rows = (
        db.query(TickerMetadata)
        .filter(TickerMetadata.ticker_normalized.in_(tickers))
        .all()
    )
    metadata_map = {
        (m.ticker_normalized or "").strip().upper(): (m.exchange or "").strip().upper()
        for m in metadata_rows
        if m.ticker_normalized and m.exchange
    }
    out: dict[str, str] = {}
    for ticker in tickers:
        ex = metadata_map.get(ticker)
        if ex:
            out[ticker] = ex
            continue
        raw = sample_raw.get(ticker, ticker)
        parsed = _parse_exchange_from_raw_ticker(raw)
        if parsed:
            out[ticker] = parsed
    return out


def _is_exchange_open_now(exchange: str | None, now_utc: datetime) -> bool:
    ex = (exchange or "").strip().upper()
    if not ex:
        # If exchange is unknown, preserve legacy behavior and allow day-change
        # calculation from available price history.
        return True
    if ex in _US_MARKET_EXCHANGES:
        local = now_utc.astimezone(ZoneInfo("America/New_York"))
        if local.weekday() >= 5:
            return False
        t = local.time()
        return dt_time(9, 30) <= t <= dt_time(16, 0)
    if ex in _EU_MARKET_EXCHANGES:
        local = now_utc.astimezone(ZoneInfo("Europe/Amsterdam"))
        if local.weekday() >= 5:
            return False
        t = local.time()
        return dt_time(9, 0) <= t <= dt_time(17, 30)
    return True


def _previous_trading_day_for_exchange(exchange: str | None, ref_date: date) -> date:
    d = ref_date - timedelta(days=1)
    ex = (exchange or "").strip().upper()
    # Weekend handling for supported exchanges; unknown defaults to weekday logic.
    if ex in _US_MARKET_EXCHANGES or ex in _EU_MARKET_EXCHANGES or not ex:
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return d


def _exchange_close_utc_naive(exchange: str | None, on_date: date) -> datetime:
    ex = (exchange or "").strip().upper()
    if ex in _US_MARKET_EXCHANGES:
        local_tz = ZoneInfo("America/New_York")
        local_dt = datetime(on_date.year, on_date.month, on_date.day, 16, 0, 0, tzinfo=local_tz)
        return local_dt.astimezone(timezone.utc).replace(tzinfo=None)
    if ex in _EU_MARKET_EXCHANGES:
        local_tz = ZoneInfo("Europe/Amsterdam")
        local_dt = datetime(on_date.year, on_date.month, on_date.day, 17, 30, 0, tzinfo=local_tz)
        return local_dt.astimezone(timezone.utc).replace(tzinfo=None)
    # Unknown exchange: preserve current behavior (end-of-day cutoff).
    return datetime(on_date.year, on_date.month, on_date.day, 23, 59, 59)


def _tx_share_deltas_in_window(
    db: Session,
    portfolio_id: str,
    *,
    start_exclusive: datetime,
    end_inclusive: datetime,
) -> dict[str, Decimal]:
    rows = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.deleted_at.is_(None),
            PortfolioTransaction.created_at.is_not(None),
            PortfolioTransaction.created_at > start_exclusive,
            PortfolioTransaction.created_at <= end_inclusive,
        )
        .order_by(PortfolioTransaction.trade_date.asc(), PortfolioTransaction.created_at.asc(), PortfolioTransaction.id.asc())
        .all()
    )
    out: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for r in rows:
        ticker = r.ticker_symbol_normalized
        shares = Decimal(str(float(r.shares or 0.0)))
        if r.tx_type == "Buy":
            out[ticker] += shares
        elif r.tx_type == "Sell":
            out[ticker] -= shares
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
    start_ts = time.perf_counter()
    portfolio = get_portfolio_or_error(db, portfolio_id)
    ledger = _latest_ledger_snapshot_or_error(db, portfolio_id)
    holdings = json.loads(ledger.holdings_json or "{}")
    if not isinstance(holdings, dict):
        raise PortfolioEngineError("Invalid ledger snapshot holdings payload.")

    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    valuation_date = date.today()

    # Guard against unstable holdings ordering / payload anomalies.
    holdings_items: list[tuple[str, float]] = []
    for k, v in holdings.items():
        qty = float(v)
        if qty < -1e-12:
            raise PortfolioEngineError(f"Negative holdings detected for {k} in ledger snapshot.")
        if qty > 0.0:
            holdings_items.append((str(k).upper(), qty))
    sorted_tickers = sorted(holdings_items, key=lambda x: x[0])
    if [t for t, _ in sorted_tickers] != sorted({t for t, _ in sorted_tickers}):
        raise PortfolioEngineError("Snapshot ordering unstable for valuation rebuild.")

    price_snaps: list[PriceSnapshot] = []
    fx_snaps: list[FXRateSnapshot] = []
    fx_by_quote: dict[str, FXRateSnapshot] = {}
    stale_tickers: list[str] = []
    missing_tickers: list[str] = []
    excluded_tickers: list[str] = []
    components: list[dict[str, object]] = []
    nav = Decimal("0")

    for ticker, qty_float in sorted_tickers:
        currency = ticker_currency.get(ticker, base_currency)
        try:
            price_snap = _resolve_price_snapshot(
                db,
                portfolio_id=portfolio_id,
                ticker=ticker,
                currency=currency,
            )
            price_snaps.append(price_snap)
        except PortfolioEngineError:
            if strict:
                raise
            missing_tickers.append(ticker)
            excluded_tickers.append(ticker)
            components.append(
                {
                    "ticker": ticker,
                    "quantity": qty_float,
                    "price": None,
                    "currency": currency,
                    "included": False,
                    "reason": "missing_price",
                }
            )
            continue

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

        fx_rate = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
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
            fx_rate = Decimal(str(fx.rate)).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
            fx_source = fx.source
            fx_as_of = fx.as_of.date().isoformat()

        qty = Decimal(str(qty_float))
        price = Decimal(str(price_snap.price))
        position_value = (qty * price * fx_rate).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
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
        "missing_tickers": sorted(missing_tickers),
    }
    valuation_input_hash = hashlib.sha256(
        json.dumps(valuation_hash_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    as_of_candidates = [s.as_of for s in price_snaps] + [s.as_of for s in fx_snaps]
    as_of = max(as_of_candidates) if as_of_candidates else datetime.utcnow()
    now = datetime.utcnow()
    nav_float = float(nav.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP))

    previous = (
        db.query(ValuationSnapshot)
        .filter(ValuationSnapshot.portfolio_id == portfolio_id)
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )
    next_version = int(previous.valuation_version if previous else 0) + 1

    # Hash validation guard: identical inputs must produce identical NAV.
    if previous and previous.input_hash == valuation_input_hash and abs(float(previous.nav) - nav_float) > 1e-12:
        raise PortfolioEngineError(
            "Hash mismatch guard failed: identical valuation input_hash produced different NAV."
        )

    prev_components = json.loads(previous.components_json) if previous and previous.components_json else []
    prev_map: dict[str, dict[str, object]] = {
        str(c.get("ticker")): c for c in (prev_components if isinstance(prev_components, list) else [])
    }
    curr_map: dict[str, dict[str, object]] = {str(c.get("ticker")): c for c in components}
    holdings_delta: dict[str, float] = {}
    for ticker in sorted(set(prev_map.keys()) | set(curr_map.keys())):
        q_prev = float(prev_map.get(ticker, {}).get("quantity") or 0.0)
        q_curr = float(curr_map.get(ticker, {}).get("quantity") or 0.0)
        delta_q = q_curr - q_prev
        if abs(delta_q) > 1e-12:
            holdings_delta[ticker] = delta_q

    prev_nav_dec = Decimal("0") if previous is None else Decimal(str(float(previous.nav)))
    nav_delta_dec = (Decimal(str(nav_float)) - prev_nav_dec).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    previous_ledger = _ledger_snapshot_by_id(db, previous.ledger_snapshot_id) if previous else None
    if previous_ledger and ledger.as_of < previous_ledger.as_of:
        raise PortfolioEngineError("Snapshot ordering mismatch: current ledger is older than previous valuation ledger.")

    tx_qty_delta_by_ticker: dict[str, Decimal] = {}
    if previous_ledger:
        tx_qty_delta_by_ticker = _tx_share_deltas_in_window(
            db,
            portfolio_id,
            start_exclusive=previous_ledger.as_of,
            end_inclusive=ledger.as_of,
        )

    transaction_attr: dict[str, Decimal] = {}
    price_attr: dict[str, Decimal] = {}
    fx_attr: dict[str, Decimal] = {}
    corporate_attr: dict[str, Decimal] = {}

    for ticker in sorted(set(prev_map.keys()) | set(curr_map.keys())):
        prev = prev_map.get(ticker, {})
        curr = curr_map.get(ticker, {})

        q_prev = Decimal(str(float(prev.get("quantity") or 0.0))) if bool(prev.get("included", False)) else Decimal("0")
        q_curr = Decimal(str(float(curr.get("quantity") or 0.0))) if bool(curr.get("included", False)) else Decimal("0")
        p_prev = Decimal(str(float(prev.get("price") or 0.0)))
        p_curr = Decimal(str(float(curr.get("price") or 0.0)))
        fx_prev = Decimal(str(float(prev.get("fx_rate") or 1.0)))
        fx_curr = Decimal(str(float(curr.get("fx_rate") or 1.0)))

        tx_qty = tx_qty_delta_by_ticker.get(ticker, Decimal("0"))
        total_qty_delta = (q_curr - q_prev)
        corp_qty = total_qty_delta - tx_qty

        tx_delta = (tx_qty * p_prev * fx_prev).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        corp_delta = (corp_qty * p_prev * fx_prev).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        price_delta = (q_curr * (p_curr - p_prev) * fx_prev).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        fx_delta = (q_curr * p_curr * (fx_curr - fx_prev)).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

        if tx_delta != Decimal("0"):
            transaction_attr[ticker] = tx_delta
        if corp_delta != Decimal("0"):
            corporate_attr[ticker] = corp_delta
        if price_delta != Decimal("0"):
            price_attr[ticker] = price_delta
        if fx_delta != Decimal("0"):
            fx_attr[ticker] = fx_delta

    tx_component = sum(transaction_attr.values(), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    price_component = sum(price_attr.values(), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    fx_component = sum(fx_attr.values(), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    corp_component = sum(corporate_attr.values(), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    total_explained_dec = (tx_component + price_component + fx_component + corp_component).quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    )
    unexplained_dec = (nav_delta_dec - total_explained_dec).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    if unexplained_dec != Decimal("0"):
        raise PortfolioEngineError(
            f"Attribution unexplained delta detected: {float(unexplained_dec)} (deterministic mode requires 0)."
        )
    if (prev_nav_dec + total_explained_dec).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP) != Decimal(
        str(nav_float)
    ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP):
        raise PortfolioEngineError("Attribution sum mismatch: previous_nav + explained_delta != current_nav.")

    nav_delta = float(nav_delta_dec)
    tx_component_float = float(tx_component)
    price_component_float = float(price_component)
    fx_component_float = float(fx_component)
    corp_component_float = float(corp_component)
    total_explained_float = float(total_explained_dec)
    unexplained_float = float(unexplained_dec)

    elapsed_ms = int((time.perf_counter() - start_ts) * 1000)
    logger.info(
        "[ValuationRebuild] portfolio=%s version=%s tickers=%s elapsed_ms=%s nav=%s strict=%s",
        portfolio_id,
        next_version,
        len(sorted_tickers),
        elapsed_ms,
        nav_float,
        bool(strict),
    )

    snapshot = ValuationSnapshot(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        valuation_version=next_version,
        ledger_snapshot_id=ledger.id,
        nav=nav_float,
        nav_delta=nav_delta if previous else 0.0,
        holdings_delta_json=json.dumps(holdings_delta, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        price_attribution_json=json.dumps(
            {k: float(v) for k, v in sorted(price_attr.items())},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        fx_attribution_json=json.dumps(
            {k: float(v) for k, v in sorted(fx_attr.items())},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        transaction_attribution_json=json.dumps(
            {k: float(v) for k, v in sorted(transaction_attr.items())},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        corporate_action_attribution_json=json.dumps(
            {k: float(v) for k, v in sorted(corporate_attr.items())},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        price_change_component=price_component_float if previous else 0.0,
        transaction_change_component=tx_component_float if previous else 0.0,
        total_explained_delta=total_explained_float if previous else 0.0,
        unexplained_delta=unexplained_float if previous else 0.0,
        currency=base_currency,
        as_of=as_of,
        created_at=now,
        rebuild_duration_ms=elapsed_ms,
        input_hash=valuation_input_hash,
        components_json=json.dumps(components, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)

    return {
        "valuation_snapshot_id": snapshot.id,
        "portfolio_id": portfolio_id,
        "valuation_version": snapshot.valuation_version,
        "ledger_snapshot_id": ledger.id,
        "nav": snapshot.nav,
        "nav_delta": snapshot.nav_delta,
        "holdings_delta": holdings_delta,
        "price_change_component": snapshot.price_change_component,
        "transaction_change_component": snapshot.transaction_change_component,
        "fx_change_component": fx_component_float if previous else 0.0,
        "corporate_action_change_component": corp_component_float if previous else 0.0,
        "total_explained_delta": snapshot.total_explained_delta,
        "unexplained_delta": snapshot.unexplained_delta,
        "currency": snapshot.currency,
        "as_of": snapshot.as_of.isoformat() + "Z",
        "input_hash": snapshot.input_hash,
        "strict": bool(strict),
        "stale_threshold_trading_days": int(stale_trading_days),
        "stale_tickers": sorted(stale_tickers),
        "missing_tickers": sorted(missing_tickers),
        "excluded_tickers": sorted(excluded_tickers),
        "price_snapshot_count": len(price_snaps),
        "fx_snapshot_count": len(fx_snaps),
        "rebuild_duration_ms": elapsed_ms,
        "price_attribution": {k: float(v) for k, v in sorted(price_attr.items())},
        "fx_attribution": {k: float(v) for k, v in sorted(fx_attr.items())},
        "transaction_attribution": {k: float(v) for k, v in sorted(transaction_attr.items())},
        "corporate_action_attribution": {k: float(v) for k, v in sorted(corporate_attr.items())},
        "components": components,
    }


def get_latest_valuation_diff(db: Session, portfolio_id: str) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    latest = (
        db.query(ValuationSnapshot)
        .filter(ValuationSnapshot.portfolio_id == portfolio_id)
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )
    if not latest:
        raise PortfolioEngineError(f"No valuation snapshot found for portfolio '{portfolio_id}'.")

    previous = (
        db.query(ValuationSnapshot)
        .filter(
            ValuationSnapshot.portfolio_id == portfolio_id,
            ValuationSnapshot.valuation_version < latest.valuation_version,
        )
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )

    holdings_delta = json.loads(latest.holdings_delta_json) if latest.holdings_delta_json else {}
    return {
        "portfolio_id": portfolio_id,
        "latest_snapshot_id": latest.id,
        "latest_valuation_version": int(latest.valuation_version or 0),
        "latest_nav": float(latest.nav),
        "latest_input_hash": latest.input_hash,
        "previous_snapshot_id": previous.id if previous else None,
        "previous_nav": float(previous.nav) if previous else None,
        "nav_delta": float(latest.nav_delta or 0.0),
        "holdings_delta": holdings_delta if isinstance(holdings_delta, dict) else {},
        "price_change_component": float(latest.price_change_component or 0.0),
        "transaction_change_component": float(latest.transaction_change_component or 0.0),
        "as_of": latest.as_of.isoformat() + "Z" if latest.as_of else None,
    }


def get_latest_valuation_attribution(db: Session, portfolio_id: str) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    latest = (
        db.query(ValuationSnapshot)
        .filter(ValuationSnapshot.portfolio_id == portfolio_id)
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )
    if not latest:
        raise PortfolioEngineError(f"No valuation snapshot found for portfolio '{portfolio_id}'.")
    previous = (
        db.query(ValuationSnapshot)
        .filter(
            ValuationSnapshot.portfolio_id == portfolio_id,
            ValuationSnapshot.valuation_version < latest.valuation_version,
        )
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )

    transaction = json.loads(latest.transaction_attribution_json or "{}")
    price = json.loads(latest.price_attribution_json or "{}")
    fx = json.loads(latest.fx_attribution_json or "{}")
    corporate = json.loads(latest.corporate_action_attribution_json or "{}")

    all_tickers = sorted(set(transaction.keys()) | set(price.keys()) | set(fx.keys()) | set(corporate.keys()))
    breakdown = {
        t: {
            "transaction_delta": float(transaction.get(t, 0.0)),
            "price_delta": float(price.get(t, 0.0)),
            "fx_delta": float(fx.get(t, 0.0)),
            "corporate_action_delta": float(corporate.get(t, 0.0)),
        }
        for t in all_tickers
    }
    prev_nav = Decimal(str(float(previous.nav))) if previous else Decimal("0")
    curr_nav = Decimal(str(float(latest.nav)))
    tx_delta = Decimal(str(float(latest.transaction_change_component or 0.0)))
    price_delta = Decimal(str(float(latest.price_change_component or 0.0)))
    fx_delta = Decimal(str(sum(float(v) for v in fx.values())))
    corp_delta = Decimal(str(sum(float(v) for v in corporate.values())))
    unexplained = Decimal(str(float(latest.unexplained_delta or 0.0)))
    explained = (tx_delta + price_delta + fx_delta + corp_delta).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    if unexplained != Decimal("0").quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP):
        raise PortfolioEngineError("Attribution guard failed: unexplained_delta must be 0.")
    if (prev_nav + explained).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP) != curr_nav.quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    ):
        raise PortfolioEngineError("Attribution guard failed: component sum does not match current NAV.")

    return {
        "portfolio_id": portfolio_id,
        "previous_nav": float(prev_nav),
        "current_nav": float(curr_nav),
        "transaction_delta": float(tx_delta),
        "price_delta": float(price_delta),
        "fx_delta": float(fx_delta),
        "corporate_action_delta": float(corp_delta),
        "unexplained_delta": float(unexplained),
        "total_explained_delta": float(explained),
        "breakdown_by_ticker": breakdown,
        "valuation_snapshot_id": latest.id,
        "valuation_version": int(latest.valuation_version or 0),
        "as_of": latest.as_of.isoformat() + "Z" if latest.as_of else None,
    }


def _latest_valuation_snapshot_or_error(db: Session, portfolio_id: str) -> ValuationSnapshot:
    row = (
        db.query(ValuationSnapshot)
        .filter(ValuationSnapshot.portfolio_id == portfolio_id)
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )
    if not row:
        raise PortfolioEngineError(f"No valuation snapshot found for portfolio '{portfolio_id}'.")
    return row


def _latest_valuation_snapshot_or_none(db: Session, portfolio_id: str) -> ValuationSnapshot | None:
    return (
        db.query(ValuationSnapshot)
        .filter(ValuationSnapshot.portfolio_id == portfolio_id)
        .order_by(ValuationSnapshot.valuation_version.desc(), ValuationSnapshot.created_at.desc())
        .first()
    )


def _load_portfolio_settings(db: Session, portfolio_id: str) -> dict[str, object]:
    row = (
        db.query(PortfolioSettings)
        .filter(
            PortfolioSettings.portfolio_id == portfolio_id,
            PortfolioSettings.is_deleted == False,
            PortfolioSettings.deleted_at.is_(None),
        )
        .order_by(PortfolioSettings.version.desc(), PortfolioSettings.updated_at.desc(), PortfolioSettings.created_at.desc())
        .first()
    )
    if row is None:
        return {
            "strict_mode": False,
            "stale_trading_days": None,
            "calendar_policy": "union_required_all_inputs",
            "default_history_range": "6M",
            "cash_management_mode": "track_cash",
            "include_dividends_in_performance": True,
            "reinvest_dividends_overlay": False,
            "version": 1,
        }
    return {
        "strict_mode": bool(row.strict_mode),
        "stale_trading_days": int(row.stale_trading_days) if row.stale_trading_days is not None else None,
        "calendar_policy": row.calendar_policy or "union_required_all_inputs",
        "default_history_range": row.default_history_range or "6M",
        "cash_management_mode": (row.cash_management_mode or "track_cash").strip().lower(),
        "include_dividends_in_performance": bool(row.include_dividends_in_performance),
        "reinvest_dividends_overlay": bool(row.reinvest_dividends_overlay),
        "version": int(row.version or 1),
    }


def get_portfolio_settings(db: Session, portfolio_id: str) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    settings = _load_portfolio_settings(db, portfolio_id)
    return {
        "portfolio_id": portfolio_id,
        "base_currency": (portfolio.base_currency or "USD").strip().upper() or "USD",
        "strict_mode": bool(settings["strict_mode"]),
        "stale_trading_days": settings["stale_trading_days"],
        "calendar_policy": str(settings["calendar_policy"]),
        "default_history_range": str(settings["default_history_range"]),
        "cash_management_mode": str(settings["cash_management_mode"]),
        "include_dividends_in_performance": bool(settings["include_dividends_in_performance"]),
        "reinvest_dividends_overlay": bool(settings["reinvest_dividends_overlay"]),
        "apply_dividend_withholding": bool(portfolio.apply_dividend_withholding),
        "dividend_withholding_percent": (
            float(portfolio.dividend_withholding_percent)
            if portfolio.dividend_withholding_percent is not None
            else None
        ),
        "version": int(settings["version"]),
    }


def update_portfolio_settings(
    db: Session,
    portfolio_id: str,
    *,
    cash_management_mode: str | None = None,
    include_dividends_in_performance: bool | None = None,
    reinvest_dividends_overlay: bool | None = None,
    apply_dividend_withholding: bool | None = None,
    dividend_withholding_percent: float | None = None,
) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    current_row = (
        db.query(PortfolioSettings)
        .filter(
            PortfolioSettings.portfolio_id == portfolio_id,
            PortfolioSettings.is_deleted == False,
            PortfolioSettings.deleted_at.is_(None),
        )
        .order_by(PortfolioSettings.version.desc(), PortfolioSettings.updated_at.desc(), PortfolioSettings.created_at.desc())
        .first()
    )
    current = _load_portfolio_settings(db, portfolio_id)
    next_mode = str(cash_management_mode or current["cash_management_mode"]).strip().lower()
    if next_mode not in {"track_cash", "ignore_cash"}:
        raise PortfolioEngineError("cash_management_mode must be 'track_cash' or 'ignore_cash'.")

    next_include_div = (
        bool(include_dividends_in_performance)
        if include_dividends_in_performance is not None
        else bool(current["include_dividends_in_performance"])
    )
    next_reinvest = (
        bool(reinvest_dividends_overlay)
        if reinvest_dividends_overlay is not None
        else bool(current["reinvest_dividends_overlay"])
    )
    next_apply_withholding = (
        bool(apply_dividend_withholding)
        if apply_dividend_withholding is not None
        else bool(portfolio.apply_dividend_withholding)
    )
    if next_apply_withholding:
        source_percent = (
            dividend_withholding_percent
            if dividend_withholding_percent is not None
            else portfolio.dividend_withholding_percent
        )
        if source_percent is None:
            raise PortfolioEngineError("dividend_withholding_percent is required when withholding is enabled.")
        next_withholding_percent = float(source_percent)
        if next_withholding_percent < 0 or next_withholding_percent > 100:
            raise PortfolioEngineError("dividend_withholding_percent must be between 0 and 100.")
    else:
        next_withholding_percent = None

    now = datetime.utcnow()
    if current_row is not None:
        current_row.is_deleted = True
        current_row.deleted_at = now
        current_row.updated_at = now
        next_version = int(current_row.version or 1) + 1
    else:
        next_version = int(current["version"]) + 1

    row = PortfolioSettings(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        strict_mode=bool(current["strict_mode"]),
        stale_trading_days=current["stale_trading_days"],
        calendar_policy=str(current["calendar_policy"]),
        default_history_range=str(current["default_history_range"]),
        cash_management_mode=next_mode,
        include_dividends_in_performance=next_include_div,
        reinvest_dividends_overlay=next_reinvest,
        version=next_version,
        created_at=now,
        updated_at=now,
        deleted_at=None,
        is_deleted=False,
    )
    db.add(row)
    portfolio.apply_dividend_withholding = next_apply_withholding
    portfolio.dividend_withholding_percent = next_withholding_percent
    portfolio.updated_at = now
    db.commit()
    return get_portfolio_settings(db, portfolio_id)


def _range_start_from_label(end_date: date, range_label: str) -> date | None:
    label = (range_label or "6M").strip().upper()
    if label in {"ALL", "MAX"}:
        return None
    if label == "YTD":
        return date(end_date.year, 1, 1)
    if label == "1D":
        return end_date - timedelta(days=1)
    if label == "5D":
        return end_date - timedelta(days=5)
    if label == "1W":
        return end_date - timedelta(days=7)
    if label.endswith("M") and label[:-1].isdigit():
        return end_date - timedelta(days=30 * int(label[:-1]))
    if label.endswith("Y") and label[:-1].isdigit():
        return end_date - timedelta(days=365 * int(label[:-1]))
    raise PortfolioEngineError(f"Unsupported range '{range_label}'. Use 1D, 5D, 1W, 1M, 3M, 6M, YTD, 1Y, 5Y, or ALL.")


def _sorted_active_transactions_for_window(
    db: Session,
    portfolio_id: str,
    *,
    to_date: date | None = None,
    include_generated: bool = True,
) -> list[PortfolioTransaction]:
    q = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.deleted_at.is_(None),
        )
    )
    if not include_generated:
        q = q.filter(PortfolioTransaction.is_generated == False)
    if to_date is not None:
        q = q.filter(PortfolioTransaction.trade_date <= to_date)
    rows = q.all()
    rows.sort(key=lambda r: (r.trade_date, r.created_at or datetime.min, r.id))
    return rows


def _sorted_active_actions_for_window(
    db: Session,
    portfolio_id: str,
    *,
    to_date: date | None = None,
) -> list[CorporateAction]:
    q = (
        db.query(CorporateAction)
        .filter(
            CorporateAction.portfolio_id == portfolio_id,
            CorporateAction.is_deleted == False,
            CorporateAction.deleted_at.is_(None),
        )
    )
    if to_date is not None:
        q = q.filter(CorporateAction.effective_date <= to_date)
    rows = q.all()
    rows.sort(key=lambda r: (r.effective_date, r.created_at or datetime.min, r.id))
    return rows


def _to_decimal(value: float | int | Decimal | None, *, scale: Decimal = _DECIMAL_MONEY_SCALE) -> Decimal:
    return Decimal(str(float(value or 0.0))).quantize(scale, rounding=ROUND_HALF_UP)


def _tx_base_amount(
    tx: PortfolioTransaction,
    *,
    db: Session | None = None,
    base_currency: str | None = None,
    ticker_currency: dict[str, str] | None = None,
) -> Decimal:
    gross = _to_decimal(tx.gross_amount)
    fx_exec = _to_decimal(tx.fx_at_execution if tx.fx_at_execution is not None else 1.0, scale=_DECIMAL_RATE_SCALE)
    gross_base = _to_decimal(tx.gross_amount_base) if tx.gross_amount_base is not None else (
        gross * fx_exec
    ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    if db is None:
        return gross_base

    base = (base_currency or "").strip().upper()
    if not base:
        portfolio = db.query(Portfolio).filter(Portfolio.id == tx.portfolio_id).first()
        base = ((portfolio.base_currency if portfolio else "USD") or "USD").strip().upper() or "USD"

    tx_ticker = (tx.ticker_symbol_normalized or "").strip().upper()
    native = ""
    if ticker_currency is not None:
        native = (ticker_currency.get(tx_ticker) or "").strip().upper()
    if not native and tx_ticker:
        md = db.query(TickerMetadata).filter(TickerMetadata.ticker_normalized == tx_ticker).first()
        native = ((md.native_currency if md else "") or "").strip().upper()
    native = native or base

    stored_ccy = (tx.currency or base).strip().upper() or base
    # Structural normalization for legacy-bad rows:
    # non-base ticker persisted as base currency should still be valued as native
    # currency at execution-date FX.
    if native != base and stored_ccy == base:
        fx_hist = _lookup_close_fx_rate(
            db,
            quote_currency=native,
            base_currency=base,
            on_date=tx.trade_date,
        )
        return (gross * fx_hist).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    return gross_base


def _canonical_settings_blob(settings: dict[str, object]) -> dict[str, object]:
    return {
        "strict_mode": bool(settings.get("strict_mode", False)),
        "stale_trading_days": settings.get("stale_trading_days"),
        "calendar_policy": str(settings.get("calendar_policy") or "union_required_all_inputs"),
        "default_history_range": str(settings.get("default_history_range") or "6M"),
        "version": int(settings.get("version", 1)),
    }


def _input_rows_hash(rows: list[dict[str, object]]) -> str:
    payload = json.dumps(rows, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _format_missing_market_inputs(on_date: date, reasons: list[str], *, base_currency: str = "BASE") -> str:
    normalized: list[str] = []
    for reason in sorted(set(reasons)):
        if reason.startswith("fx:"):
            ccy = reason.split(":", 1)[1]
            normalized.append(f"historical FX close rate missing ({ccy}->{base_currency})")
        elif reason.startswith("price:"):
            ticker = reason.split(":", 1)[1]
            normalized.append(f"price close missing ({ticker})")
        else:
            normalized.append(reason)
    return f"Missing required market inputs on {on_date.isoformat()}: {', '.join(normalized)}"


def _source_hash_for_window(
    *,
    engine_version: str,
    settings: dict[str, object],
    mode: str,
    force: bool,
    strict: bool,
    from_date: date,
    to_date: date,
    transactions: list[PortfolioTransaction],
    actions: list[CorporateAction],
    dividend_events: list[DividendEvent],
    prices_used: list[dict[str, object]],
    fx_used: list[dict[str, object]],
) -> str:
    tx_rows = [
        {
            "id": t.id,
            "trade_date": t.trade_date.isoformat(),
            "created_at": (t.created_at or datetime.min).isoformat(),
            "ticker": t.ticker_symbol_normalized,
            "type": t.tx_type,
            "shares": float(t.shares or 0.0),
            "price": float(t.price or 0.0),
            "gross_amount": float(t.gross_amount or 0.0),
            "gross_amount_base": float(t.gross_amount_base or 0.0),
            "fx_at_execution": float(t.fx_at_execution or 1.0),
            "currency": t.currency or "USD",
        }
        for t in transactions
    ]
    action_rows = [
        {
            "id": a.id,
            "effective_date": a.effective_date.isoformat(),
            "created_at": (a.created_at or datetime.min).isoformat(),
            "ticker": a.ticker,
            "type": a.action_type,
            "factor": float(a.factor or 0.0),
            "cash_amount": float(a.cash_amount or 0.0),
        }
        for a in actions
    ]
    dividend_rows = [
        {
            "id": e.id,
            "ticker": e.ticker,
            "ex_date": e.ex_date.isoformat(),
            "pay_date": e.pay_date.isoformat(),
            "dividend_per_share_native": float(e.dividend_per_share_native or 0.0),
            "currency": e.currency or "USD",
            "source_hash": e.source_hash or "",
        }
        for e in dividend_events
    ]
    price_rows = [
        {
            "ticker": str(p.get("ticker") or ""),
            "date": str(p.get("date") or ""),
            "close": float(p.get("close") or 0.0),
        }
        for p in prices_used
    ]
    fx_rows = [
        {
            "base_currency": str(p.get("base_currency") or ""),
            "quote_currency": str(p.get("quote_currency") or ""),
            "date": str(p.get("date") or ""),
            "rate": float(p.get("rate") or 0.0),
        }
        for p in fx_used
    ]
    payload = {
        "engine_version": engine_version,
        "settings": _canonical_settings_blob(settings),
        "params": {
            "mode": mode,
            "force": bool(force),
            "strict": bool(strict),
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
        },
        "transactions_hash": _input_rows_hash(tx_rows),
        "actions_hash": _input_rows_hash(action_rows),
        "dividend_events_hash": _input_rows_hash(dividend_rows),
        "prices_hash": _input_rows_hash(price_rows),
        "fx_hash": _input_rows_hash(fx_rows),
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _resolve_latest_equity_rows(
    db: Session,
    portfolio_id: str,
    *,
    build_version: int | None = None,
) -> list[PortfolioEquityHistoryRow]:
    q = db.query(PortfolioEquityHistoryRow).filter(PortfolioEquityHistoryRow.portfolio_id == portfolio_id)
    if build_version is not None:
        q = q.filter(PortfolioEquityHistoryRow.build_version == build_version)
        return q.order_by(PortfolioEquityHistoryRow.date.asc()).all()

    rows = q.order_by(PortfolioEquityHistoryRow.date.asc(), PortfolioEquityHistoryRow.build_version.asc()).all()
    by_date: dict[date, PortfolioEquityHistoryRow] = {}
    for row in rows:
        current = by_date.get(row.date)
        if current is None or int(row.build_version) > int(current.build_version):
            by_date[row.date] = row
    return [by_date[d] for d in sorted(by_date.keys())]


def _latest_equity_row_or_none(db: Session, portfolio_id: str) -> PortfolioEquityHistoryRow | None:
    rows = _resolve_latest_equity_rows(db, portfolio_id)
    return rows[-1] if rows else None


def _latest_completed_equity_build(db: Session, portfolio_id: str) -> PortfolioEquityHistoryBuild | None:
    return (
        db.query(PortfolioEquityHistoryBuild)
        .filter(
            PortfolioEquityHistoryBuild.portfolio_id == portfolio_id,
            PortfolioEquityHistoryBuild.status == "completed",
        )
        .order_by(PortfolioEquityHistoryBuild.build_version.desc(), PortfolioEquityHistoryBuild.finished_at.desc())
        .first()
    )


def _parse_provider_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except Exception:
        return None


def _dividend_source_hash(
    *,
    source: str,
    ticker: str,
    ex_date: date,
    pay_date: date,
    amount_per_share_native: Decimal,
    currency: str,
) -> str:
    raw = json.dumps(
        {
            "source": source,
            "ticker": ticker,
            "ex_date": ex_date.isoformat(),
            "pay_date": pay_date.isoformat(),
            "dividend_per_share_native": str(amount_per_share_native),
            "currency": currency,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _ticker_vendor_symbol_map_from_transactions(
    rows: list[PortfolioTransaction],
) -> dict[str, str]:
    by_ticker: dict[str, str] = {}
    ordered = sorted(rows, key=lambda r: (r.trade_date, r.created_at or datetime.min, r.id))
    for tx in ordered:
        ticker = (tx.ticker_symbol_normalized or "").strip().upper()
        if not ticker or ticker in by_ticker:
            continue
        fetch_symbol, _ = _to_fetch_symbol(ticker, tx.ticker_symbol_raw)
        by_ticker[ticker] = fetch_symbol
    return by_ticker


def _fetch_finnhub_dividend_rows(
    *,
    vendor_symbol: str,
    from_date: date,
    to_date: date,
) -> tuple[list[dict[str, object]], bool]:
    token = (os.environ.get("FINNHUB_API_KEY") or "").strip()
    if not token:
        return [], False
    url = "https://finnhub.io/api/v1/stock/dividend"
    params = {
        "symbol": vendor_symbol,
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "token": token,
    }
    try:
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(url, params=params)
        if not resp.is_success:
            logger.warning("[DividendBackfill] Finnhub failed for %s: HTTP %s", vendor_symbol, resp.status_code)
            return [], False
        payload = resp.json()
    except Exception as exc:
        logger.warning("[DividendBackfill] Finnhub failed for %s: %s", vendor_symbol, exc)
        return [], False

    items: list[dict[str, object]] = []
    if isinstance(payload, dict):
        maybe = payload.get("historical")
        if isinstance(maybe, list):
            items = [x for x in maybe if isinstance(x, dict)]
    elif isinstance(payload, list):
        items = [x for x in payload if isinstance(x, dict)]
    return items, True


def _fetch_yahoo_dividend_rows(
    *,
    vendor_symbol: str,
    from_date: date,
    to_date: date,
) -> tuple[list[dict[str, object]], bool]:
    try:
        import yfinance as yf
    except Exception as exc:
        logger.warning("[DividendBackfill] yfinance import failed: %s", exc)
        return [], False

    try:
        series = yf.Ticker(vendor_symbol).dividends
        if series is None or len(series) == 0:
            return [], True
        rows: list[dict[str, object]] = []
        for idx, val in series.items():
            ex_d = idx.date()
            if ex_d < from_date or ex_d > to_date:
                continue
            try:
                amt = float(val)
            except Exception:
                continue
            rows.append({"exDate": ex_d.isoformat(), "payDate": ex_d.isoformat(), "amount": amt})
        rows.sort(key=lambda r: str(r.get("exDate") or ""))
        return rows, True
    except Exception as exc:
        logger.warning("[DividendBackfill] Yahoo dividends failed for %s: %s", vendor_symbol, exc)
        return [], False


def backfill_dividend_history_if_missing(
    portfolio_id: str,
    db: Session,
    *,
    strict: bool = False,
) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    tx_rows = _sorted_active_transactions_for_window(db, portfolio_id, include_generated=False)
    if not tx_rows:
        return {
            "portfolio_id": portfolio_id,
            "inserted_rows": 0,
            "fetched_tickers": [],
            "from_date": None,
            "to_date": None,
            "strict": bool(strict),
        }

    _ensure_ticker_metadata_from_transactions(db, tx_rows)
    earliest_tx = min(r.trade_date for r in tx_rows)
    to_d = date.today()
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    vendor_map = _ticker_vendor_symbol_map_from_transactions(tx_rows)
    tickers = sorted({r.ticker_symbol_normalized for r in tx_rows if r.ticker_symbol_normalized})
    inserted_rows = 0
    fetched_tickers: list[str] = []
    known_dividend_tickers: set[str] = set()

    for ticker in tickers:
        existing_any = (
            db.query(DividendEvent.id)
            .filter(
                DividendEvent.ticker == ticker,
                DividendEvent.ex_date >= earliest_tx,
                DividendEvent.ex_date <= to_d,
            )
            .first()
            is not None
        )
        if existing_any:
            known_dividend_tickers.add(ticker)
            continue

        vendor_symbol = vendor_map.get(ticker, ticker)
        event_rows, finnhub_ok = _fetch_finnhub_dividend_rows(
            vendor_symbol=vendor_symbol,
            from_date=earliest_tx,
            to_date=to_d,
        )
        source = "finnhub"
        if not event_rows:
            yahoo_rows, yahoo_ok = _fetch_yahoo_dividend_rows(
                vendor_symbol=vendor_symbol,
                from_date=earliest_tx,
                to_date=to_d,
            )
            if yahoo_rows:
                event_rows = yahoo_rows
                source = "yahoo"
            elif not finnhub_ok and not yahoo_ok:
                # Provider availability failure should not break strict mode.
                continue

        fetched_tickers.append(ticker)
        if event_rows:
            known_dividend_tickers.add(ticker)

        for item in event_rows:
            ex_d = _parse_provider_date(item.get("exDate") or item.get("ex_date") or item.get("date"))
            pay_d = _parse_provider_date(
                item.get("payDate") or item.get("paymentDate") or item.get("pay_date") or item.get("date")
            )
            if ex_d is None:
                continue
            if pay_d is None:
                pay_d = ex_d
            try:
                amt = _to_decimal(
                    item.get("amount")
                    if item.get("amount") is not None
                    else item.get("dividend")
                    if item.get("dividend") is not None
                    else item.get("cashAmount"),
                    scale=_DECIMAL_MONEY_SCALE,
                )
            except Exception:
                continue
            if amt <= Decimal("0"):
                continue
            ccy = (
                str(item.get("currency") or ticker_currency.get(ticker) or portfolio.base_currency or "USD")
                .strip()
                .upper()
                or "USD"
            )
            shash = _dividend_source_hash(
                source=source,
                ticker=ticker,
                ex_date=ex_d,
                pay_date=pay_d,
                amount_per_share_native=amt,
                currency=ccy,
            )
            exists = (
                db.query(DividendEvent.id)
                .filter(
                    DividendEvent.source_hash == shash,
                )
                .first()
                is not None
            )
            if exists:
                continue
            duplicate = (
                db.query(DividendEvent.id)
                .filter(
                    DividendEvent.ticker == ticker,
                    DividendEvent.ex_date == ex_d,
                    DividendEvent.dividend_per_share_native == amt,
                )
                .first()
                is not None
            )
            if duplicate:
                continue
            db.add(
                DividendEvent(
                    id=str(uuid.uuid4()),
                    ticker=ticker,
                    ex_date=ex_d,
                    pay_date=pay_d,
                    amount_per_share=amt,
                    dividend_per_share_native=amt,
                    currency=ccy,
                    source=source,
                    source_hash=shash,
                    created_at=datetime.utcnow(),
                )
            )
            inserted_rows += 1

    db.commit()

    if strict:
        for ticker in sorted(known_dividend_tickers):
            has_rows = (
                db.query(DividendEvent.id)
                .filter(
                    DividendEvent.ticker == ticker,
                    DividendEvent.ex_date >= earliest_tx,
                    DividendEvent.ex_date <= to_d,
                )
                .first()
                is not None
            )
            if not has_rows:
                raise PortfolioEngineError(
                    f"Dividend history expected but unavailable for {ticker} after ingestion attempt."
                )

    return {
        "portfolio_id": portfolio_id,
        "inserted_rows": inserted_rows,
        "fetched_tickers": fetched_tickers,
        "from_date": earliest_tx.isoformat(),
        "to_date": to_d.isoformat(),
        "strict": bool(strict),
    }


def _active_dividend_events_for_tickers(
    db: Session,
    *,
    tickers: list[str],
    to_date: date,
) -> list[DividendEvent]:
    if not tickers:
        return []
    rows = (
        db.query(DividendEvent)
        .filter(
            DividendEvent.ticker.in_(tickers),
            DividendEvent.ex_date <= to_date,
            DividendEvent.pay_date <= to_date,
        )
        .order_by(DividendEvent.ex_date.asc(), DividendEvent.pay_date.asc(), DividendEvent.id.asc())
        .all()
    )
    return rows


def _apply_generated_dividend_transactions(
    db: Session,
    *,
    portfolio: Portfolio,
    portfolio_id: str,
    to_date: date,
    base_tx_rows: list[PortfolioTransaction],
    action_rows: list[CorporateAction],
    tickers: list[str],
) -> None:
    # Remove previous generated rows and recreate deterministically from dividend_events.
    db.query(PortfolioTransaction).filter(
        PortfolioTransaction.portfolio_id == portfolio_id,
        PortfolioTransaction.is_generated == True,
    ).delete(synchronize_session=False)
    if not tickers:
        return
    events = _active_dividend_events_for_tickers(db, tickers=tickers, to_date=to_date)
    if not events:
        return
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)

    tx_by_date: dict[date, list[PortfolioTransaction]] = defaultdict(list)
    for tx in base_tx_rows:
        tx_by_date[tx.trade_date].append(tx)
    for d in tx_by_date:
        tx_by_date[d].sort(key=lambda r: (r.trade_date, r.created_at or datetime.min, r.id))

    action_by_date: dict[date, list[CorporateAction]] = defaultdict(list)
    for r in action_rows:
        action_by_date[r.effective_date].append(r)
    for d in action_by_date:
        action_by_date[d].sort(key=lambda r: (r.effective_date, r.created_at or datetime.min, r.id))

    events_by_ex_date: dict[date, list[DividendEvent]] = defaultdict(list)
    for ev in events:
        events_by_ex_date[ev.ex_date].append(ev)

    ordered_dates = sorted(
        set(tx_by_date.keys())
        | set(action_by_date.keys())
        | set(events_by_ex_date.keys())
        | {ev.pay_date for ev in events}
    )
    lots: dict[str, list[tuple[Decimal, Decimal]]] = defaultdict(list)
    pending_by_pay: dict[date, list[tuple[DividendEvent, Decimal, Decimal, Decimal]]] = defaultdict(list)
    now = datetime.utcnow()
    generated_rows: list[PortfolioTransaction] = []

    for d in ordered_dates:
        for tx in tx_by_date.get(d, []):
            ticker = tx.ticker_symbol_normalized
            shares = _to_decimal(tx.shares)
            if tx.tx_type == "Buy":
                unit_base = Decimal("0")
                if shares > Decimal("0"):
                    unit_base = (
                        _tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency) / shares
                    ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    lots[ticker].append((shares, unit_base))
            elif tx.tx_type == "Sell":
                remaining = shares
                while remaining > Decimal("0"):
                    if not lots[ticker]:
                        raise PortfolioEngineError(f"Negative holdings detected for {ticker}.")
                    lot_shares, lot_cost = lots[ticker][0]
                    take = lot_shares if lot_shares <= remaining else remaining
                    if take == lot_shares:
                        lots[ticker].pop(0)
                    else:
                        lots[ticker][0] = ((lot_shares - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP), lot_cost)
                    remaining = (remaining - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

        for action in action_by_date.get(d, []):
            ticker = action.ticker
            if action.action_type == "SPLIT":
                factor = _to_decimal(action.factor, scale=_DECIMAL_RATE_SCALE)
                if factor <= Decimal("0"):
                    raise PortfolioEngineError(f"Invalid SPLIT factor for {ticker}.")
                lots[ticker] = [
                    ((ls * factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP), (lc / factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP))
                    for ls, lc in lots[ticker]
                ]
            elif action.action_type == "REVERSE_SPLIT":
                factor = _to_decimal(action.factor, scale=_DECIMAL_RATE_SCALE)
                if factor <= Decimal("0"):
                    raise PortfolioEngineError(f"Invalid REVERSE_SPLIT factor for {ticker}.")
                lots[ticker] = [
                    ((ls / factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP), (lc * factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP))
                    for ls, lc in lots[ticker]
                ]

        for ev in events_by_ex_date.get(d, []):
            ticker = ev.ticker
            entitled_shares = sum((ls for ls, _ in lots[ticker]), Decimal("0")).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )
            if entitled_shares <= Decimal("0"):
                continue
            amount_per_share = _to_decimal(ev.dividend_per_share_native)
            gross_local = (entitled_shares * amount_per_share).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            withholding_rate = Decimal("0")
            if bool(portfolio.apply_dividend_withholding):
                withholding_rate = _to_decimal(
                    float(portfolio.dividend_withholding_percent or 0.0),
                    scale=_DECIMAL_RATE_SCALE,
                ) / Decimal("100")
            withholding_local = (gross_local * withholding_rate).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            net_local = (gross_local - withholding_local).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            event_currency = (ev.currency or portfolio.base_currency or "USD").strip().upper() or "USD"
            fx_rate = _lookup_close_fx_rate(
                db,
                quote_currency=event_currency,
                base_currency=(portfolio.base_currency or "USD"),
                on_date=ev.pay_date,
            )
            net_base = (net_local * fx_rate).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            pending_by_pay[ev.pay_date].append((ev, entitled_shares, gross_local, net_base))

        for ev, entitled_shares, gross_local, gross_base in pending_by_pay.get(d, []):
            withholding_local = Decimal("0")
            withholding_rate_pct = float(portfolio.dividend_withholding_percent or 0.0) if bool(portfolio.apply_dividend_withholding) else 0.0
            if bool(portfolio.apply_dividend_withholding):
                withholding_local = (
                    _to_decimal(gross_local)
                    * (_to_decimal(withholding_rate_pct, scale=_DECIMAL_RATE_SCALE) / Decimal("100"))
                ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            net_local = (_to_decimal(gross_local) - withholding_local).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )
            metadata = {
                "generated": True,
                "dividend_event_id": ev.id,
                "ex_date": ev.ex_date.isoformat(),
                "pay_date": ev.pay_date.isoformat(),
                "entitled_shares": float(entitled_shares),
                "dividend_per_share_native": float(_to_decimal(ev.dividend_per_share_native)),
                "gross_amount_native": float(_to_decimal(gross_local)),
                "withholding_percent": withholding_rate_pct,
                "withholding_amount_native": float(withholding_local),
                "net_amount_native": float(net_local),
                "net_amount_base": float(_to_decimal(gross_base)),
            }
            generated_rows.append(
                PortfolioTransaction(
                    id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    security_id=None,
                    ticker_symbol_raw=ev.ticker,
                    ticker_symbol_normalized=ev.ticker,
                    tx_type="Dividend",
                    trade_date=ev.pay_date,
                    shares=float(entitled_shares),
                    price=float(net_local),
                    gross_amount=float(net_local),
                    fx_at_execution=float(
                        (_to_decimal(gross_base) / net_local).quantize(
                            _DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP
                        )
                        if net_local != Decimal("0")
                        else Decimal("1")
                    ),
                    gross_amount_base=float(gross_base),
                    is_generated=True,
                    generated_event_id=ev.id,
                    currency=(ev.currency or portfolio.base_currency or "USD").strip().upper() or "USD",
                    metadata_json=_serialize_metadata(metadata),
                    source=ev.source or "dividend_events",
                    created_at=now,
                    updated_at=now,
                    deleted_at=None,
                    version=1,
                    is_deleted=False,
                )
            )

    if generated_rows:
        db.add_all(generated_rows)
    db.flush()


def _daily_price_history_rows(
    db: Session,
    tickers: list[str],
    from_date: date,
    to_date: date,
) -> list[dict[str, object]]:
    if not tickers:
        return []
    start_dt = datetime.combine(from_date, datetime.min.time())
    end_dt = datetime.combine(to_date, datetime.max.time())
    rows = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker.in_(tickers),
            PriceHistory.datetime_utc >= start_dt,
            PriceHistory.datetime_utc <= end_dt,
        )
        .order_by(PriceHistory.ticker.asc(), PriceHistory.datetime_utc.asc(), PriceHistory.id.asc())
        .all()
    )
    by_ticker_day: dict[tuple[str, date], PriceHistory] = {}
    for row in rows:
        d = row.datetime_utc.date()
        by_ticker_day[(row.ticker, d)] = row
    out = [
        {
            "ticker": ticker,
            "date": d.isoformat(),
            "close": float(last.price),
            "source": last.source or "scheduler",
            "datetime_utc": last.datetime_utc.isoformat(),
        }
        for (ticker, d), last in sorted(by_ticker_day.items(), key=lambda x: (x[0][0], x[0][1]))
    ]
    return out


def _daily_legacy_price_rows(
    db: Session,
    tickers: list[str],
    from_date: date,
    to_date: date,
) -> list[dict[str, object]]:
    if not tickers:
        return []
    rows = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker.in_(tickers),
            PricesHistory.date >= from_date,
            PricesHistory.date <= to_date,
        )
        .order_by(PricesHistory.ticker.asc(), PricesHistory.date.asc(), PricesHistory.id.asc())
        .all()
    )
    return [
        {
            "ticker": str(r.ticker),
            "date": r.date.isoformat(),
            "close": float(r.close),
            "source": "fallback_legacy",
            "datetime_utc": datetime(r.date.year, r.date.month, r.date.day, 16, 0, 0).isoformat(),
        }
        for r in rows
        if r.date is not None and r.close is not None
    ]


def _resolve_price_value_on_or_before(
    *,
    ticker: str,
    as_of: date,
    primary_dates: dict[str, list[date]],
    primary_values: dict[tuple[str, date], Decimal],
    legacy_dates: dict[str, list[date]],
    legacy_values: dict[tuple[str, date], Decimal],
) -> tuple[Decimal | None, str | None]:
    ticker_key = (ticker or "").strip().upper()
    p_dates = primary_dates.get(ticker_key, [])
    if p_dates:
        idx = bisect_right(p_dates, as_of) - 1
        if idx >= 0:
            d = p_dates[idx]
            value = primary_values.get((ticker_key, d))
            if value is not None:
                return value, "price_history"

    l_dates = legacy_dates.get(ticker_key, [])
    if l_dates:
        idx = bisect_right(l_dates, as_of) - 1
        if idx >= 0:
            d = l_dates[idx]
            value = legacy_values.get((ticker_key, d))
            if value is not None:
                return value, "fallback_legacy"
    return None, None


def _resolve_fx_value_on_or_before(
    *,
    quote_currency: str,
    as_of: date,
    fx_dates: dict[str, list[date]],
    fx_values: dict[tuple[str, date], Decimal],
) -> Decimal | None:
    quote_key = (quote_currency or "").strip().upper()
    dates = fx_dates.get(quote_key, [])
    if not dates:
        return None
    idx = bisect_right(dates, as_of) - 1
    if idx < 0:
        return None
    d = dates[idx]
    return fx_values.get((quote_key, d))


def _daily_fx_rate_rows(
    db: Session,
    quote_currencies: list[str],
    base_currency: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, object]]:
    if not quote_currencies:
        return []
    end_dt = datetime.combine(to_date, datetime.max.time())
    rows = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base_currency,
            FXRate.quote_currency.in_(quote_currencies),
            FXRate.datetime_utc <= end_dt,
        )
        .order_by(FXRate.quote_currency.asc(), FXRate.datetime_utc.asc(), FXRate.id.asc())
        .all()
    )
    by_pair_day: dict[tuple[str, str, date], FXRate] = {}
    for row in rows:
        d = row.datetime_utc.date()
        by_pair_day[(row.base_currency, row.quote_currency, d)] = row
    out = [
        {
            "base_currency": base,
            "quote_currency": quote,
            "date": d.isoformat(),
            "rate": float(last.rate),
            "source": last.source or "scheduler",
            "datetime_utc": last.datetime_utc.isoformat(),
        }
        for (base, quote, d), last in sorted(by_pair_day.items(), key=lambda x: (x[0][0], x[0][1], x[0][2]))
    ]
    return out


def _compute_holdings_state_upto(
    db: Session,
    portfolio_id: str,
    *,
    as_of_date: date,
) -> dict[str, dict[str, Decimal]]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    tx_rows = _sorted_active_transactions_for_window(db, portfolio_id, to_date=as_of_date)
    lots: dict[str, list[tuple[Decimal, Decimal]]] = defaultdict(list)
    closed_tickers: set[str] = set()
    realized_by_ticker: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for tx in tx_rows:
        ticker = tx.ticker_symbol_normalized
        shares = _to_decimal(tx.shares)
        gross_base = _tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency)
        if tx.tx_type == "Buy":
            if shares > Decimal("0"):
                lots[ticker].append((shares, (gross_base / shares).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)))
        elif tx.tx_type == "Sell":
            remaining = shares
            consumed_cost = Decimal("0")
            while remaining > Decimal("0"):
                if not lots[ticker]:
                    raise PortfolioEngineError(f"Negative holdings detected for {ticker}.")
                lot_shares, lot_cost = lots[ticker][0]
                take = lot_shares if lot_shares <= remaining else remaining
                consumed_cost += (take * lot_cost).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                if take == lot_shares:
                    lots[ticker].pop(0)
                else:
                    lots[ticker][0] = ((lot_shares - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP), lot_cost)
                remaining = (remaining - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            realized_by_ticker[ticker] += (gross_base - consumed_cost).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    out: dict[str, dict[str, Decimal]] = {}
    for ticker in sorted(set(lots.keys()) | set(realized_by_ticker.keys())):
        open_shares = sum((ls for ls, _ in lots[ticker]), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        total_cost = sum(((ls * lc) for ls, lc in lots[ticker]), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        avg_cost = Decimal("0")
        if open_shares > Decimal("0"):
            avg_cost = (total_cost / open_shares).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        out[ticker] = {
            "quantity": open_shares,
            "total_cost_basis": total_cost,
            "avg_cost_basis": avg_cost,
            "realized_gain_value": realized_by_ticker[ticker].quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP),
        }
    return out


def rebuild_equity_history(
    db: Session,
    portfolio_id: str,
    *,
    mode: str = "incremental",
    force: bool = False,
    from_date: date | None = None,
    to_date: date | None = None,
    strict: bool | None = None,
) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    _validate_portfolio_dividend_withholding(portfolio)
    settings = _load_portfolio_settings(db, portfolio_id)
    mode_norm = (mode or "incremental").strip().lower()
    if mode_norm not in {"incremental", "full"}:
        raise PortfolioEngineError("mode must be 'incremental' or 'full'.")
    strict_mode = bool(settings["strict_mode"]) if strict is None else bool(strict)
    cash_mode = str(settings.get("cash_management_mode") or "track_cash").strip().lower()
    include_dividends_in_perf = bool(settings.get("include_dividends_in_performance", True))
    to_d = to_date or date.today()

    base_transactions_all = _sorted_active_transactions_for_window(db, portfolio_id, include_generated=False)
    actions_all = _sorted_active_actions_for_window(db, portfolio_id)
    latest_build = _latest_completed_equity_build(db, portfolio_id)
    if not base_transactions_all:
        build_version = int(latest_build.build_version if latest_build else 0) + 1
        now = datetime.utcnow()
        empty_hash = hashlib.sha256(
            json.dumps(
                {
                    "engine_version": _EQUITY_ENGINE_VERSION,
                    "portfolio_id": portfolio_id,
                    "settings": settings,
                    "mode": mode,
                    "force": bool(force),
                    "strict": bool(strict_mode),
                    "no_transactions": True,
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        build = PortfolioEquityHistoryBuild(
            id=str(uuid.uuid4()),
            portfolio_id=portfolio_id,
            build_version=build_version,
            mode=(mode or "incremental").strip().lower(),
            from_date=from_date or to_d,
            to_date=to_d,
            strict=bool(strict_mode),
            source_hash=empty_hash,
            engine_version=_EQUITY_ENGINE_VERSION,
            status="completed",
            started_at=now,
            finished_at=now,
            rows_written=0,
            forced=bool(force),
        )
        db.add(build)
        db.commit()
        return {
            "portfolio_id": portfolio_id,
            "build_id": build.id,
            "build_version": build.build_version,
            "mode": build.mode,
            "strict": bool(build.strict),
            "forced": bool(build.forced),
            "rows_written": 0,
            "from_date": build.from_date.isoformat() if build.from_date else None,
            "to_date": build.to_date.isoformat() if build.to_date else None,
            "status": build.status,
            "source_hash": build.source_hash,
            "engine_version": build.engine_version,
        }

    first_tx_date = min(r.trade_date for r in base_transactions_all)
    _ensure_ticker_metadata_from_transactions(db, base_transactions_all)
    active_tickers = sorted({r.ticker_symbol_normalized for r in base_transactions_all if r.ticker_symbol_normalized})
    _apply_generated_dividend_transactions(
        db,
        portfolio=portfolio,
        portfolio_id=portfolio_id,
        to_date=to_d,
        base_tx_rows=base_transactions_all,
        action_rows=actions_all,
        tickers=active_tickers,
    )
    transactions_all = _sorted_active_transactions_for_window(db, portfolio_id, include_generated=True)
    dividend_events_all = _active_dividend_events_for_tickers(db, tickers=active_tickers, to_date=to_d)
    if mode_norm == "full":
        start_date = from_date or first_tx_date
    else:
        if from_date is not None:
            start_date = from_date
        elif latest_build and latest_build.to_date:
            start_date = latest_build.to_date + timedelta(days=1)
        else:
            start_date = first_tx_date
    if start_date > to_d:
        start_date = to_d

    tx_upto_to = [r for r in transactions_all if r.trade_date <= to_d]
    action_upto_to = [r for r in actions_all if r.effective_date <= to_d]

    tickers = sorted({r.ticker_symbol_normalized for r in tx_upto_to})
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    fx_pairs = sorted({c for c in ticker_currency.values() if c != base_currency})
    price_rows_primary = _daily_price_history_rows(db, tickers, first_tx_date, to_d)
    price_rows_legacy = _daily_legacy_price_rows(db, tickers, first_tx_date, to_d)
    price_rows = [*price_rows_primary, *price_rows_legacy]
    fx_rows = _daily_fx_rate_rows(db, fx_pairs, base_currency, first_tx_date, to_d)

    if mode_norm == "incremental" and latest_build and not force and latest_build.to_date:
        tx_guard = [r for r in tx_upto_to if r.trade_date <= latest_build.to_date]
        actions_guard = [r for r in action_upto_to if r.effective_date <= latest_build.to_date]
        prices_guard = [r for r in price_rows if date.fromisoformat(str(r["date"])) <= latest_build.to_date]
        fx_guard = [r for r in fx_rows if date.fromisoformat(str(r["date"])) <= latest_build.to_date]
        guard_hash = _source_hash_for_window(
            engine_version=_EQUITY_ENGINE_VERSION,
            settings=settings,
            mode=mode_norm,
            force=force,
            strict=strict_mode,
            from_date=first_tx_date,
            to_date=latest_build.to_date,
            transactions=tx_guard,
            actions=actions_guard,
            dividend_events=dividend_events_all,
            prices_used=prices_guard,
            fx_used=fx_guard,
        )
        if guard_hash != latest_build.source_hash:
            raise PortfolioEngineError(
                "Historical inputs changed before last equity history date. Re-run with force=true."
            )

    build_source_hash = _source_hash_for_window(
        engine_version=_EQUITY_ENGINE_VERSION,
        settings=settings,
        mode=mode_norm,
        force=force,
        strict=strict_mode,
        from_date=first_tx_date,
        to_date=to_d,
        transactions=tx_upto_to,
        actions=action_upto_to,
        dividend_events=dividend_events_all,
        prices_used=price_rows,
        fx_used=fx_rows,
    )
    latest_build_version = int(latest_build.build_version if latest_build else 0)
    build_version = latest_build_version + 1
    build_now = datetime.utcnow()
    build = PortfolioEquityHistoryBuild(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        build_version=build_version,
        mode=mode_norm,
        from_date=start_date,
        to_date=to_d,
        strict=strict_mode,
        source_hash=build_source_hash,
        engine_version=_EQUITY_ENGINE_VERSION,
        status="started",
        started_at=build_now,
        finished_at=None,
        rows_written=0,
        forced=bool(force),
    )
    db.add(build)
    db.commit()

    tx_by_date: dict[date, list[PortfolioTransaction]] = defaultdict(list)
    for r in tx_upto_to:
        tx_by_date[r.trade_date].append(r)
    for d in tx_by_date:
        tx_by_date[d].sort(key=lambda r: (r.trade_date, r.created_at or datetime.min, r.id))

    action_by_date: dict[date, list[CorporateAction]] = defaultdict(list)
    for r in action_upto_to:
        action_by_date[r.effective_date].append(r)
    for d in action_by_date:
        action_by_date[d].sort(key=lambda r: (r.effective_date, r.created_at or datetime.min, r.id))

    price_by_ticker_date: dict[tuple[str, date], Decimal] = {}
    price_dates_by_ticker: dict[str, set[date]] = defaultdict(set)
    for r in price_rows_primary:
        d = date.fromisoformat(str(r["date"]))
        ticker = str(r["ticker"]).strip().upper()
        price_by_ticker_date[(ticker, d)] = _to_decimal(float(r.get("close") or 0.0))
        price_dates_by_ticker[ticker].add(d)
    legacy_price_by_ticker_date: dict[tuple[str, date], Decimal] = {}
    legacy_price_dates_by_ticker: dict[str, set[date]] = defaultdict(set)
    for r in price_rows_legacy:
        d = date.fromisoformat(str(r["date"]))
        ticker = str(r["ticker"]).strip().upper()
        legacy_price_by_ticker_date[(ticker, d)] = _to_decimal(float(r.get("close") or 0.0))
        legacy_price_dates_by_ticker[ticker].add(d)

    primary_price_dates_sorted = {k: sorted(v) for k, v in price_dates_by_ticker.items()}
    legacy_price_dates_sorted = {k: sorted(v) for k, v in legacy_price_dates_by_ticker.items()}

    fx_by_currency_date: dict[tuple[str, date], Decimal] = {}
    fx_dates_by_currency: dict[str, set[date]] = defaultdict(set)
    for row in fx_rows:
        quote_currency = str(row["quote_currency"]).strip().upper()
        d = date.fromisoformat(str(row["date"]))
        fx_by_currency_date[(quote_currency, d)] = _to_decimal(float(row.get("rate") or 0.0), scale=_DECIMAL_RATE_SCALE)
        fx_dates_by_currency[quote_currency].add(d)
    fx_dates_sorted = {k: sorted(v) for k, v in fx_dates_by_currency.items()}

    candidate_dates = set()
    candidate_dates.update(d for d in tx_by_date.keys() if first_tx_date <= d <= to_d)
    candidate_dates.update(d for d in action_by_date.keys() if first_tx_date <= d <= to_d)
    candidate_dates.update(d for d in (ev.ex_date for ev in dividend_events_all) if first_tx_date <= d <= to_d)
    candidate_dates.update(d for d in (ev.pay_date for ev in dividend_events_all) if first_tx_date <= d <= to_d)
    for _, d in price_by_ticker_date.keys():
        if first_tx_date <= d <= to_d:
            candidate_dates.add(d)
    for _, d in legacy_price_by_ticker_date.keys():
        if first_tx_date <= d <= to_d:
            candidate_dates.add(d)
    for _, d in fx_by_currency_date.keys():
        if first_tx_date <= d <= to_d:
            candidate_dates.add(d)
    ordered_dates = sorted(candidate_dates)

    lots: dict[str, list[tuple[Decimal, Decimal]]] = defaultdict(list)
    closed_tickers: set[str] = set()
    active_close_cycles: dict[str, dict[str, Decimal | date]] = {}
    closed_rows_raw: list[dict[str, object]] = []
    cash = Decimal("0")
    realized_total = Decimal("0")
    prev_included_total_equity: Decimal | None = None
    prev_included_components: dict[str, tuple[Decimal, Decimal, Decimal]] = {}
    prev_twr_index = Decimal("1").quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    rows_to_add: list[PortfolioEquityHistoryRow] = []

    try:
        for d in ordered_dates:
            if d < first_tx_date or d > to_d:
                continue

            day_dividend_cash = Decimal("0")
            day_net_contribution = Decimal("0")
            for tx in tx_by_date.get(d, []):
                ticker = tx.ticker_symbol_normalized
                shares = _to_decimal(tx.shares)
<<<<<<< HEAD
                gross_base = _tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency)
=======
                gross_base = _tx_base_amount(tx)
>>>>>>> origin/main
                ticker_ccy = (ticker_currency.get(ticker) or base_currency).strip().upper() or base_currency
                tx_local_notional = (shares * _to_decimal(tx.price)).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
                tx_stored_ccy = (tx.currency or "").strip().upper() or base_currency
                if ticker_ccy == base_currency:
                    tx_local_notional = gross_base
                    tx_fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                else:
                    tx_fx = _to_decimal(tx.fx_at_execution, scale=_DECIMAL_RATE_SCALE)
                    derived_fx = (
                        (gross_base / tx_local_notional).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                        if tx_local_notional > Decimal("0")
                        else Decimal("0")
                    )
                    # Legacy imports for non-base tickers were often persisted as USD+1.0 execution FX.
                    # For those rows, recover deterministic execution FX from historical close FX on trade date.
                    if (
                        tx_stored_ccy == base_currency
                        and tx_fx == Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                        and derived_fx == Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                    ):
                        try:
                            tx_fx = _lookup_close_fx_rate(
                                db,
                                quote_currency=ticker_ccy,
                                base_currency=base_currency,
                                on_date=tx.trade_date,
                            )
                        except PortfolioEngineError:
                            tx_fx = derived_fx if derived_fx > Decimal("0") else Decimal("1").quantize(
                                _DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP
                            )
                    elif derived_fx > Decimal("0"):
                        tx_fx = derived_fx
                    elif tx_fx <= Decimal("0"):
                        tx_fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                if tx.tx_type == "Buy":
                    cash -= gross_base
                    if cash_mode == "ignore_cash":
                        day_net_contribution += gross_base
                    cycle = active_close_cycles.get(ticker)
                    if cycle is None:
                        cycle = {
                            "open_date": d,
                            "total_shares": Decimal("0"),
                            "total_cost_basis": Decimal("0"),
                            "total_proceeds": Decimal("0"),
                            "buy_local_notional": Decimal("0"),
                            "sell_local_notional": Decimal("0"),
                            "total_dividends": Decimal("0"),
                            "buy_fx_weighted_notional": Decimal("0"),
                            "sell_fx_weighted_notional": Decimal("0"),
                        }
                        active_close_cycles[ticker] = cycle
                    cycle["total_shares"] = (cycle["total_shares"] + shares).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    cycle["total_cost_basis"] = (cycle["total_cost_basis"] + gross_base).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    cycle["buy_local_notional"] = (cycle["buy_local_notional"] + tx_local_notional).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                    cycle["buy_fx_weighted_notional"] = (
                        cycle["buy_fx_weighted_notional"] + (tx_local_notional * tx_fx)
                    ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    if shares > Decimal("0"):
                        lots[ticker].append((shares, (gross_base / shares).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)))
                        if ticker in closed_tickers:
                            closed_tickers.remove(ticker)
                elif tx.tx_type == "Sell":
                    cash += gross_base
                    if cash_mode == "ignore_cash":
                        day_net_contribution -= gross_base
                    remaining = shares
                    consumed_cost = Decimal("0")
                    while remaining > Decimal("0"):
                        if not lots[ticker]:
                            raise PortfolioEngineError(f"Negative holdings detected for {ticker}.")
                        lot_shares, lot_cost = lots[ticker][0]
                        take = lot_shares if lot_shares <= remaining else remaining
                        consumed_cost += (take * lot_cost).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                        if take == lot_shares:
                            lots[ticker].pop(0)
                        else:
                            lots[ticker][0] = ((lot_shares - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP), lot_cost)
                        remaining = (remaining - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    realized_total += (gross_base - consumed_cost).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    cycle = active_close_cycles.get(ticker)
                    if cycle is None:
                        # Defensive: should not happen because SELL inventory checks already passed.
                        cycle = {
                            "open_date": d,
                            "total_shares": Decimal("0"),
                            "total_cost_basis": Decimal("0"),
                            "total_proceeds": Decimal("0"),
                            "buy_local_notional": Decimal("0"),
                            "sell_local_notional": Decimal("0"),
                            "total_dividends": Decimal("0"),
                            "buy_fx_weighted_notional": Decimal("0"),
                            "sell_fx_weighted_notional": Decimal("0"),
                        }
                        active_close_cycles[ticker] = cycle
                    cycle["total_proceeds"] = (cycle["total_proceeds"] + gross_base).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                    cycle["sell_local_notional"] = (cycle["sell_local_notional"] + tx_local_notional).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                    cycle["sell_fx_weighted_notional"] = (
                        cycle["sell_fx_weighted_notional"] + (tx_local_notional * tx_fx)
                    ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                    open_qty = sum((ls for ls, _ in lots[ticker]), Decimal("0")).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                    if open_qty == Decimal("0"):
                        closed_tickers.add(ticker)
                        total_cost_basis = _to_decimal(cycle["total_cost_basis"])
                        total_proceeds = _to_decimal(cycle["total_proceeds"])
                        realized_gain = (total_proceeds - total_cost_basis).quantize(
                            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                        )
                        realized_gain_pct = Decimal("0")
                        if total_cost_basis != Decimal("0"):
                            realized_gain_pct = ((realized_gain / total_cost_basis) * Decimal("100")).quantize(
                                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                            )
                        buy_local_notional = _to_decimal(cycle["buy_local_notional"])
                        sell_local_notional = _to_decimal(cycle["sell_local_notional"])
                        buy_fx_component = _to_decimal(cycle["buy_fx_weighted_notional"])
                        sell_fx_component = _to_decimal(cycle["sell_fx_weighted_notional"])
                        avg_buy_fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                        avg_sell_fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                        if buy_local_notional != Decimal("0"):
                            avg_buy_fx = (buy_fx_component / buy_local_notional).quantize(
                                _DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP
                            )
                        if sell_local_notional != Decimal("0"):
                            avg_sell_fx = (sell_fx_component / sell_local_notional).quantize(
                                _DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP
                            )
                        fx_component = (buy_local_notional * (avg_sell_fx - avg_buy_fx)).quantize(
                            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                        )
                        open_date = cycle["open_date"]
                        holding_period_days = 0
                        if isinstance(open_date, date):
                            holding_period_days = max((d - open_date).days, 0)
                        closed_rows_raw.append(
                            {
                                "ticker": ticker,
                                "open_date": open_date if isinstance(open_date, date) else d,
                                "close_date": d,
                                "total_shares": _to_decimal(cycle["total_shares"]),
                                "total_cost_basis": total_cost_basis,
                                "total_proceeds": total_proceeds,
                                "realized_gain": realized_gain,
                                "realized_gain_pct": realized_gain_pct,
                                "fx_component": fx_component,
                                "total_dividends": _to_decimal(cycle["total_dividends"]),
                                "holding_period_days": holding_period_days,
                            }
                        )
                        active_close_cycles.pop(ticker, None)
                elif tx.tx_type == "Dividend":
                    cash += gross_base
                    day_dividend_cash += gross_base
                    if not include_dividends_in_perf:
                        day_net_contribution += gross_base
                    cycle = active_close_cycles.get(ticker)
                    if cycle is not None:
                        cycle["total_dividends"] = (cycle["total_dividends"] + gross_base).quantize(
                            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                        )

            for action in action_by_date.get(d, []):
                ticker = action.ticker
                if action.action_type == "SPLIT":
                    factor = _to_decimal(action.factor, scale=_DECIMAL_RATE_SCALE)
                    if factor <= Decimal("0"):
                        raise PortfolioEngineError(f"Invalid SPLIT factor for {ticker}.")
                    updated: list[tuple[Decimal, Decimal]] = []
                    for ls, lc in lots[ticker]:
                        new_shares = (ls * factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                        new_cost = (lc / factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                        updated.append((new_shares, new_cost))
                    lots[ticker] = updated
                elif action.action_type == "REVERSE_SPLIT":
                    factor = _to_decimal(action.factor, scale=_DECIMAL_RATE_SCALE)
                    if factor <= Decimal("0"):
                        raise PortfolioEngineError(f"Invalid REVERSE_SPLIT factor for {ticker}.")
                    updated = []
                    for ls, lc in lots[ticker]:
                        new_shares = (ls / factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                        new_cost = (lc * factor).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                        updated.append((new_shares, new_cost))
                    lots[ticker] = updated
                elif action.action_type == "DIVIDEND":
                    amount = _to_decimal(action.cash_amount)
                    cash += amount
                    day_dividend_cash += amount
                    if not include_dividends_in_perf:
                        day_net_contribution += amount
                    cycle = active_close_cycles.get(ticker)
                    if cycle is not None:
                        cycle["total_dividends"] = (cycle["total_dividends"] + amount).quantize(
                            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                        )

            if d < start_date:
                continue

            required_tickers = sorted([t for t, l in lots.items() if sum((ls for ls, _ in l), Decimal("0")) > Decimal("0")])
            missing_reasons: list[str] = []
            market_value = Decimal("0")
            cost_basis_total = Decimal("0")
            fx_day_component = Decimal("0")
            row_components: list[dict[str, object]] = []
            for ticker in required_tickers:
                qty = sum((ls for ls, _ in lots[ticker]), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                total_cost = sum(((ls * lc) for ls, lc in lots[ticker]), Decimal("0")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                cost_basis_total += total_cost

                px, _px_source = _resolve_price_value_on_or_before(
                    ticker=ticker,
                    as_of=d,
                    primary_dates=primary_price_dates_sorted,
                    primary_values=price_by_ticker_date,
                    legacy_dates=legacy_price_dates_sorted,
                    legacy_values=legacy_price_by_ticker_date,
                )
                if px is None:
                    missing_reasons.append(f"price:{ticker}")
                    continue
                ccy = ticker_currency.get(ticker, base_currency)
                fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
                if ccy != base_currency:
                    fx = _resolve_fx_value_on_or_before(
                        quote_currency=ccy,
                        as_of=d,
                        fx_dates=fx_dates_sorted,
                        fx_values=fx_by_currency_date,
                    )
                    if fx is None:
                        missing_reasons.append(f"fx:{ccy}")
                        continue
                value = (qty * px * fx).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
                market_value += value
                prev_comp = prev_included_components.get(ticker)
                if prev_comp is not None:
                    prev_qty, prev_px, prev_fx = prev_comp
                    overlap_qty = prev_qty if prev_qty <= qty else qty
                    if overlap_qty > Decimal("0"):
                        fx_day_component += (overlap_qty * px * (fx - prev_fx)).quantize(
                            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                        )
                row_components.append(
                    {
                        "ticker": ticker,
                        "qty": float(qty),
                        "price": float(px),
                        "fx": float(fx),
                        "value": float(value),
                    }
                )

            if missing_reasons:
                if strict_mode:
                    raise PortfolioEngineError(
                        _format_missing_market_inputs(d, missing_reasons, base_currency=base_currency)
                    )
                continue

            total_equity = (
                (cash + market_value) if cash_mode == "track_cash" else market_value
            ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            unrealized = (market_value - cost_basis_total).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            day_change_value = Decimal("0")
            day_change_pct = Decimal("0")
            market_return_component = Decimal("0")
            twr_index = prev_twr_index
            if prev_included_total_equity is not None:
                day_change_value = (total_equity - prev_included_total_equity).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
                if prev_included_total_equity != Decimal("0"):
                    day_change_pct = ((day_change_value / prev_included_total_equity) * Decimal("100")).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                    twr_period = ((day_change_value - day_net_contribution) / prev_included_total_equity).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                    twr_index = (prev_twr_index * (Decimal("1") + twr_period)).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                market_return_component = (day_change_value - day_net_contribution - fx_day_component).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
            prev_included_total_equity = total_equity
            prev_twr_index = twr_index
            prev_included_components = {
                c["ticker"]: (
                    _to_decimal(c["qty"]),
                    _to_decimal(c["price"]),
                    _to_decimal(c["fx"], scale=_DECIMAL_RATE_SCALE),
                )
                for c in row_components
            }

            row_payload = {
                "engine_version": _EQUITY_ENGINE_VERSION,
                "portfolio_id": portfolio_id,
                "build_version": build_version,
                "date": d.isoformat(),
                "cash_balance": float(cash if cash_mode == "track_cash" else Decimal("0")),
                "market_value_total": float(market_value),
                "cost_basis_total": float(cost_basis_total),
                "realized_gain_value": float(realized_total),
                "dividend_cash_value": float(day_dividend_cash),
                "net_contribution": float(day_net_contribution),
                "market_return_component": float(market_return_component),
                "fx_return_component": float(fx_day_component),
                "twr_index": float(twr_index),
                "closed_tickers": sorted(closed_tickers),
                "components": row_components,
            }
            input_hash = hashlib.sha256(
                json.dumps(row_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            rows_to_add.append(
                PortfolioEquityHistoryRow(
                    id=str(uuid.uuid4()),
                    portfolio_id=portfolio_id,
                    build_version=build_version,
                    date=d,
                    total_equity=float(total_equity),
                    cash_balance=float(cash if cash_mode == "track_cash" else Decimal("0")),
                    market_value_total=float(market_value),
                    cost_basis_total=float(cost_basis_total),
                    unrealized_gain_value=float(unrealized),
                    realized_gain_value=float(realized_total),
                    dividend_cash_value=float(day_dividend_cash),
                    day_change_value=float(day_change_value),
                    day_change_percent=float(day_change_pct),
                    net_contribution=float(day_net_contribution),
                    market_return_component=float(market_return_component),
                    fx_return_component=float(fx_day_component),
                    twr_index=float(twr_index),
                    input_hash=input_hash,
                    created_at=datetime.utcnow(),
                )
            )

        closed_rows_merged: dict[tuple[str, date], dict[str, object]] = {}
        for row in closed_rows_raw:
            key = (str(row["ticker"]), row["close_date"])
            existing = closed_rows_merged.get(key)
            if existing is None:
                closed_rows_merged[key] = row
                continue
            existing["open_date"] = min(existing["open_date"], row["open_date"])
            for field in (
                "total_shares",
                "total_cost_basis",
                "total_proceeds",
                "realized_gain",
                "fx_component",
                "total_dividends",
            ):
                existing[field] = (_to_decimal(existing[field]) + _to_decimal(row[field])).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
            existing["realized_gain_pct"] = Decimal("0")
            if _to_decimal(existing["total_cost_basis"]) != Decimal("0"):
                existing["realized_gain_pct"] = (
                    (_to_decimal(existing["realized_gain"]) / _to_decimal(existing["total_cost_basis"])) * Decimal("100")
                ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            existing["holding_period_days"] = max(
                (existing["close_date"] - existing["open_date"]).days,
                int(existing["holding_period_days"]),
                int(row["holding_period_days"]),
            )

        db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).delete(synchronize_session=False)
        if closed_rows_merged:
            db.add_all(
                [
                    ClosedPosition(
                        id=str(uuid.uuid4()),
                        portfolio_id=portfolio_id,
                        ticker=str(row["ticker"]),
                        open_date=row["open_date"],
                        close_date=row["close_date"],
                        total_shares=float(_to_decimal(row["total_shares"])),
                        total_cost_basis=float(_to_decimal(row["total_cost_basis"])),
                        total_proceeds=float(_to_decimal(row["total_proceeds"])),
                        realized_gain=float(_to_decimal(row["realized_gain"])),
                        realized_gain_pct=float(_to_decimal(row["realized_gain_pct"])),
                        fx_component=float(_to_decimal(row["fx_component"])),
                        total_dividends=float(_to_decimal(row["total_dividends"])),
                        holding_period_days=int(row["holding_period_days"]),
                        created_at=datetime.utcnow(),
                    )
                    for _, row in sorted(closed_rows_merged.items(), key=lambda item: (item[0][1], item[0][0]))
                ]
            )

        if rows_to_add:
            db.add_all(rows_to_add)
        build.status = "completed"
        build.rows_written = len(rows_to_add)
        build.finished_at = datetime.utcnow()
        if rows_to_add:
            build.from_date = rows_to_add[0].date
            build.to_date = rows_to_add[-1].date
        db.commit()
        return {
            "portfolio_id": portfolio_id,
            "build_id": build.id,
            "build_version": build.build_version,
            "mode": build.mode,
            "strict": bool(build.strict),
            "forced": bool(build.forced),
            "rows_written": int(build.rows_written or 0),
            "from_date": build.from_date.isoformat() if build.from_date else None,
            "to_date": build.to_date.isoformat() if build.to_date else None,
            "status": build.status,
            "source_hash": build.source_hash,
            "engine_version": build.engine_version,
        }
    except Exception:
        build.status = "failed"
        build.finished_at = datetime.utcnow()
        db.commit()
        raise


def get_portfolio_equity_history(
    db: Session,
    portfolio_id: str,
    *,
    range_label: str = "6M",
    build_version: int | None = None,
    performance_mode: str = "absolute",
    show_fx_impact: bool = False,
) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    perf_mode = (performance_mode or "absolute").strip().lower()
    if perf_mode not in {"absolute", "twr", "net_of_contributions"}:
        raise PortfolioEngineError("performance_mode must be one of: absolute, twr, net_of_contributions.")
    rows = _resolve_latest_equity_rows(db, portfolio_id, build_version=build_version)
    if not rows:
        _range_start_from_label(date.today(), range_label)
        return {
            "portfolio_id": portfolio_id,
            "range": (range_label or "6M").upper(),
            "performance_mode": perf_mode,
            "show_fx_impact": bool(show_fx_impact),
            "series": [],
        }

    start_date = _range_start_from_label(rows[-1].date, range_label)
    filtered = rows if start_date is None else [r for r in rows if r.date >= start_date]
    baseline = filtered[0] if filtered else None
    baseline_twr = Decimal(str(float(baseline.twr_index))) if baseline is not None else Decimal("1")
    cumulative_contrib = Decimal("0")
    cumulative_fx = Decimal("0")
    cumulative_market = Decimal("0")
    series = [
        None
        for _ in filtered
    ]
    for idx, r in enumerate(filtered):
        total_equity = Decimal(str(float(r.total_equity)))
        net_contribution = Decimal(str(float(r.net_contribution or 0.0)))
        day_change = Decimal(str(float(r.day_change_value)))
        twr_index = Decimal(str(float(r.twr_index or 1.0)))
        fx_component = Decimal(str(float(r.fx_return_component or 0.0)))
        market_component = Decimal(str(float(r.market_return_component or 0.0)))
        cumulative_contrib += net_contribution
        cumulative_fx += fx_component
        cumulative_market += market_component

        net_of_contrib_value = (total_equity - cumulative_contrib).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        twr_return_pct = Decimal("0")
        if baseline_twr != Decimal("0"):
            twr_return_pct = (((twr_index / baseline_twr) - Decimal("1")) * Decimal("100")).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )

        plotted = total_equity
        if perf_mode == "twr":
            plotted = twr_return_pct
        elif perf_mode == "net_of_contributions":
            plotted = net_of_contrib_value
        if show_fx_impact:
            plotted = (plotted + cumulative_fx).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

        series[idx] = {
            "date": r.date.isoformat(),
            "total_equity": float(total_equity),
            "day_change_value": float(day_change),
            "day_change_percent": float(r.day_change_percent),
            "net_contribution": float(net_contribution),
            "cumulative_net_contribution": float(cumulative_contrib),
            "market_return_component": float(market_component),
            "fx_return_component": float(fx_component),
            "cumulative_market_return_component": float(cumulative_market),
            "cumulative_fx_return_component": float(cumulative_fx),
            "twr_index": float(twr_index),
            "twr_return_pct": float(twr_return_pct),
            "net_of_contributions_value": float(net_of_contrib_value),
            "plotted_value": float(plotted),
        }
    return {
        "portfolio_id": portfolio_id,
        "range": (range_label or "6M").upper(),
        "performance_mode": perf_mode,
        "show_fx_impact": bool(show_fx_impact),
        "series": series,
    }


def get_portfolio_dashboard_summary(db: Session, portfolio_id: str) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    row = _latest_equity_row_or_none(db, portfolio_id)
    if row is None:
        raise PortfolioEngineError(f"No equity history rows found for portfolio '{portfolio_id}'.")
    market_move_component = Decimal(str(float(row.market_return_component or 0.0)))
    currency_move_component = Decimal(str(float(row.fx_return_component or 0.0)))
    holdings_state = _compute_holdings_state_upto(db, portfolio_id, as_of_date=row.date)
    open_tickers = sorted([t for t, s in holdings_state.items() if _to_decimal(s.get("quantity")) > Decimal("0")])
    last_prices_updated_at: str | None = None
    today_utc = datetime.now(timezone.utc).date()
    if open_tickers:
        per_ticker_latest: dict[str, tuple[datetime, str]] = {}
        for ticker in open_tickers:
            ts, source = _latest_price_timestamp_on_or_before(
                db,
                ticker=ticker,
                as_of_date=max(row.date, today_utc),
            )
            if ts is not None:
                per_ticker_latest[ticker] = (ts, source or "price_history")
        if per_ticker_latest:
            max_ts, max_source = max(per_ticker_latest.values(), key=lambda item: item[0])
            suffix = " (fallback)" if max_source == "fallback_legacy" else ""
            last_prices_updated_at = max_ts.isoformat() + "Z" + suffix
        else:
            last_prices_updated_at = "No price data"

    day_change_value = Decimal(str(float(row.day_change_value)))
    day_change_percent = Decimal(str(float(row.day_change_percent)))
    try:
        holdings_payload = get_portfolio_holdings(db, portfolio_id)
        holdings_rows = holdings_payload.get("holdings") or []
        if holdings_rows:
            live_day_change = Decimal("0")
            live_market_impact = Decimal("0")
            live_fx_impact = Decimal("0")
            for hr in holdings_rows:
                live_day_change += _to_decimal(hr.get("day_change_value"))
                live_market_impact += _to_decimal(hr.get("price_return_value"))
                live_fx_impact += _to_decimal(hr.get("fx_return_value"))
            market_move_component = live_market_impact.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            currency_move_component = live_fx_impact.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            # Keep day change exactly aligned to its displayed components.
            day_change_value = (market_move_component + currency_move_component).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )
            prior_equity = (_to_decimal(row.total_equity) - day_change_value).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )
            if prior_equity != Decimal("0"):
                day_change_percent = ((day_change_value / prior_equity) * Decimal("100")).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
            else:
                day_change_percent = Decimal("0").quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    except Exception:
        # Keep persisted snapshot day change as fallback.
        pass

    return {
        "portfolio_id": portfolio_id,
        "as_of": row.date.isoformat(),
        "total_equity": float(row.total_equity),
        "cash_balance": float(row.cash_balance),
        "cost_basis_total": float(row.cost_basis_total),
        "market_value_total": float(row.market_value_total),
        "day_change_value": float(day_change_value),
        "day_change_percent": float(day_change_percent),
        "unrealized_gain_value": float(row.unrealized_gain_value),
        "unrealized_gain_percent": float(
            Decimal("0")
            if Decimal(str(float(row.cost_basis_total))) == Decimal("0")
            else (
                (Decimal(str(float(row.unrealized_gain_value))) / Decimal(str(float(row.cost_basis_total)))) * Decimal("100")
            ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        ),
        "realized_gain_value": float(row.realized_gain_value),
        "dividend_cash_value": float(row.dividend_cash_value),
        "market_move_component": float(market_move_component),
        "currency_move_component": float(currency_move_component),
        "last_prices_updated_at": last_prices_updated_at,
    }


def get_portfolio_holdings(db: Session, portfolio_id: str) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    latest_equity = _latest_equity_row_or_none(db, portfolio_id)
    if latest_equity is None:
        raise PortfolioEngineError(f"No equity history rows found for portfolio '{portfolio_id}'.")
    as_of = latest_equity.date
    today_utc = datetime.now(timezone.utc).date()
    query_as_of = max(as_of, today_utc)
    now_utc = datetime.now(timezone.utc)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"

    state = _compute_holdings_state_upto(db, portfolio_id, as_of_date=as_of)
    lot_basis = _open_lot_basis_details_upto(db, portfolio_id, as_of_date=as_of)
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    ticker_exchange = _ticker_exchange_map_or_none(db, portfolio_id)
    ytd_start = date(as_of.year, 1, 1)
    dividend_tx_rows = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.deleted_at.is_(None),
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.trade_date <= as_of,
        )
        .all()
    )
    dividends_total_by_ticker: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    dividends_ytd_by_ticker: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for dtx in dividend_tx_rows:
        ticker_key = dtx.ticker_symbol_normalized
        amount = _tx_base_amount(dtx, db=db, base_currency=base_currency, ticker_currency=ticker_currency)
        dividends_total_by_ticker[ticker_key] = (
            dividends_total_by_ticker[ticker_key] + amount
        ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        if dtx.trade_date >= ytd_start:
            dividends_ytd_by_ticker[ticker_key] = (
                dividends_ytd_by_ticker[ticker_key] + amount
            ).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    rows: list[dict[str, object]] = []
    for ticker in sorted(state.keys()):
        qty = state[ticker]["quantity"]
        if qty <= Decimal("0"):
            continue
        ccy = ticker_currency.get(ticker, base_currency)
        # For current-day holdings, use latest persisted points up to "now" (UTC),
        # not end-of-day rows, so day-change and FX reflect live refresh correctly.
        if query_as_of == today_utc:
            as_of_dt = now_utc.replace(tzinfo=None)
            px_row = (
                db.query(PriceHistory)
                .filter(
                    PriceHistory.ticker == ticker,
                    PriceHistory.datetime_utc <= as_of_dt,
                )
                .order_by(PriceHistory.datetime_utc.desc(), PriceHistory.id.desc())
                .first()
            )
            if px_row is not None and px_row.price is not None and px_row.datetime_utc is not None:
                px_today = _to_decimal(px_row.price, scale=_DECIMAL_MONEY_SCALE)
                px_date = px_row.datetime_utc.date()
            else:
                px_today, px_date = _latest_price_point_on_or_before(
                    db,
                    ticker=ticker,
                    as_of_date=query_as_of,
                )

            fx_row = (
                db.query(FXRate)
                .filter(
                    FXRate.base_currency == base_currency,
                    FXRate.quote_currency == ccy,
                    FXRate.datetime_utc <= as_of_dt,
                )
                .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
                .first()
            )
            if fx_row is not None and fx_row.rate is not None and fx_row.datetime_utc is not None:
                fx_today = _to_decimal(fx_row.rate, scale=_DECIMAL_RATE_SCALE)
                fx_date = fx_row.datetime_utc.date()
            else:
                fx_today, fx_date = _latest_fx_rate_point_on_or_before(
                    db,
                    base_currency=base_currency,
                    quote_currency=ccy,
                    as_of_date=query_as_of,
                )
        else:
            # Historical holdings snapshot uses end-of-day lookup.
            px_today, px_date = _latest_price_point_on_or_before(db, ticker=ticker, as_of_date=query_as_of)
            fx_today, fx_date = _latest_fx_rate_point_on_or_before(
                db,
                base_currency=base_currency,
                quote_currency=ccy,
                as_of_date=query_as_of,
            )
        if px_today is None or fx_today is None or px_date is None or fx_date is None:
            continue
        market_value_native = (qty * px_today).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        market_price_base = (px_today * fx_today).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        market_value = (market_value_native * fx_today).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

        prev_value = None
        px_prev = None
        fx_prev = None
        exchange = ticker_exchange.get(ticker)
        prev_close_as_of = _previous_trading_day_for_exchange(exchange, query_as_of)
        # Strict day-change baseline: exact previous trading session close.
        px_prev, _ = _price_close_point_on_date(
            db,
            ticker=ticker,
            on_date=prev_close_as_of,
        )
        prev_fx_cutoff = _exchange_close_utc_naive(exchange, prev_close_as_of)
        # Primary baseline for day FX: latest row at/before previous session
        # close cutoff (e.g. 16:30 UTC for ENXTAM), so day FX matches session
        # close semantics and not end-of-day rows.
        fx_prev_row = (
            db.query(FXRate)
            .filter(
                FXRate.base_currency == base_currency,
                FXRate.quote_currency == ccy,
                FXRate.datetime_utc <= prev_fx_cutoff,
            )
            .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
            .first()
        )
        if fx_prev_row is not None and fx_prev_row.rate is not None:
            fx_prev = _to_decimal(fx_prev_row.rate, scale=_DECIMAL_RATE_SCALE)
        else:
            # Fallback: previous day close row if cutoff-aligned row is absent.
            fx_prev, _ = _fx_rate_point_on_date(
                db,
                base_currency=base_currency,
                quote_currency=ccy,
                on_date=prev_close_as_of,
            )
        if px_prev is not None and fx_prev is not None:
            prev_native_value = (qty * px_prev).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            prev_value = (prev_native_value * fx_prev).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        day_change = Decimal("0")
        day_change_pct = Decimal("0")
        price_return_value = Decimal("0")
        fx_return_value = Decimal("0")
        market_open_now = _is_exchange_open_now(exchange, now_utc)
        has_current_session_point = (px_date == query_as_of)
        # Show day change during market hours OR once a current-session point exists
        # (e.g. after market close on the same trading day). Keep zero when there
        # is no new session data (weekends/holidays/stale only).
        if prev_value is not None and (market_open_now or has_current_session_point):
            day_change = (market_value - prev_value).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            if prev_value != Decimal("0"):
                day_change_pct = ((day_change / prev_value) * Decimal("100")).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
            if px_prev is not None and fx_prev is not None:
                price_return_value = (qty * (px_today - px_prev) * fx_prev).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
                fx_return_value = (qty * px_today * (fx_today - fx_prev)).quantize(
                    _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                )
        total_cost = state[ticker]["total_cost_basis"]
        basis_row = lot_basis.get(ticker, {})
        total_cost_native = _to_decimal(basis_row.get("open_local_cost"))
        avg_cost_native = Decimal("0")
        if qty != Decimal("0"):
            avg_cost_native = (total_cost_native / qty).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        total_cost_base = _to_decimal(basis_row.get("open_base_cost")) if basis_row else total_cost
        avg_exec_fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
        if ccy != base_currency and total_cost_native != Decimal("0"):
            avg_exec_fx = (total_cost_base / total_cost_native).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
        fx_impact_value = Decimal("0")
        if ccy != base_currency:
            fx_impact_value = (market_value_native * (fx_today - avg_exec_fx)).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )
        unrealized = (market_value - total_cost).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        unrealized_pct = Decimal("0")
        if total_cost != Decimal("0"):
            unrealized_pct = ((unrealized / total_cost) * Decimal("100")).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        total_dividends = dividends_total_by_ticker.get(ticker, Decimal("0")).quantize(
            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
        )
        dividends_ytd = dividends_ytd_by_ticker.get(ticker, Decimal("0")).quantize(
            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
        )
        dividend_yield_pct = Decimal("0")
        if total_cost != Decimal("0"):
            dividend_yield_pct = ((total_dividends / total_cost) * Decimal("100")).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )
        combined_return_value = (price_return_value + fx_return_value).quantize(
            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
        )
        rows.append(
            {
                "ticker": ticker,
                "native_currency": ccy,
                "quantity": float(qty),
                "avg_cost_basis": float(state[ticker]["avg_cost_basis"]),
                "avg_cost_basis_native": float(avg_cost_native),
                "total_cost_basis": float(total_cost),
                "total_cost_basis_native": float(total_cost_native),
                "market_price": float(market_price_base),
                "market_price_native": float(px_today),
                "market_value": float(market_value),
                "market_value_native": float(market_value_native),
                "day_change_value": float(day_change),
                "day_change_percent": float(day_change_pct),
                "price_return_value": float(price_return_value),
                "fx_return_value": float(fx_return_value),
                "fx_impact_value": float(fx_impact_value),
                "combined_return_value": float(combined_return_value),
                "total_dividends": float(total_dividends),
                "dividends_ytd": float(dividends_ytd),
                "dividend_yield_pct": float(dividend_yield_pct),
                "unrealized_gain_value": float(unrealized),
                "unrealized_gain_percent": float(unrealized_pct),
                "realized_gain_value": float(
                    (state[ticker]["realized_gain_value"] + total_dividends).quantize(
                        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
                    )
                ),
            }
        )
    return {"portfolio_id": portfolio_id, "as_of": as_of.isoformat(), "holdings": rows}


def list_closed_positions_for_portfolio(db: Session, portfolio_id: str) -> dict[str, object]:
    get_portfolio_or_error(db, portfolio_id)
    rows = (
        db.query(ClosedPosition)
        .filter(ClosedPosition.portfolio_id == portfolio_id)
        .order_by(ClosedPosition.close_date.desc(), ClosedPosition.ticker.asc())
        .all()
    )
    return {
        "portfolio_id": portfolio_id,
        "closed_positions": [
            {
                "ticker": r.ticker,
                "open_date": r.open_date.isoformat() if r.open_date else None,
                "close_date": r.close_date.isoformat(),
                "total_cost_basis": float(r.total_cost_basis),
                "total_proceeds": float(r.total_proceeds),
                "realized_gain": float(r.realized_gain),
                "realized_gain_pct": float(r.realized_gain_pct),
                "fx_component": float(r.fx_component),
                "total_dividends": float(r.total_dividends),
                "holding_period_days": int(r.holding_period_days),
            }
            for r in rows
        ],
    }


def _latest_price_on_or_before(
    db: Session,
    *,
    ticker: str,
    as_of_date: date,
) -> Decimal | None:
    row = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker == ticker,
            PriceHistory.datetime_utc <= datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 59),
        )
        .order_by(PriceHistory.datetime_utc.desc(), PriceHistory.id.desc())
        .first()
    )
    if row is not None and row.price is not None:
        return _to_decimal(row.price)
    fallback = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker == ticker,
            PricesHistory.date <= as_of_date,
        )
        .order_by(PricesHistory.date.desc(), PricesHistory.id.desc())
        .first()
    )
    if fallback is None or fallback.close is None:
        return None
    return _to_decimal(fallback.close)


def _latest_price_point_on_or_before(
    db: Session,
    *,
    ticker: str,
    as_of_date: date,
) -> tuple[Decimal | None, date | None]:
    row = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker == ticker,
            PriceHistory.datetime_utc <= datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 59),
        )
        .order_by(PriceHistory.datetime_utc.desc(), PriceHistory.id.desc())
        .first()
    )
    if row is not None and row.price is not None and row.datetime_utc is not None:
        return _to_decimal(row.price), row.datetime_utc.date()
    fallback = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker == ticker,
            PricesHistory.date <= as_of_date,
        )
        .order_by(PricesHistory.date.desc(), PricesHistory.id.desc())
        .first()
    )
    if fallback is None or fallback.close is None or fallback.date is None:
        return None, None
    return _to_decimal(fallback.close), fallback.date


def _legacy_close_point_on_date(
    db: Session,
    *,
    ticker: str,
    on_date: date,
) -> tuple[Decimal | None, date | None]:
    """Return daily close on an exact date from legacy prices_history."""
    fallback = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker == ticker,
            PricesHistory.date == on_date,
        )
        .order_by(PricesHistory.id.desc())
        .first()
    )
    if fallback is None or fallback.close is None or fallback.date is None:
        return None, None
    return _to_decimal(fallback.close), fallback.date


def _price_close_point_on_date(
    db: Session,
    *,
    ticker: str,
    on_date: date,
) -> tuple[Decimal | None, date | None]:
    row = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker == ticker,
            PriceHistory.datetime_utc >= datetime(on_date.year, on_date.month, on_date.day, 0, 0, 0),
            PriceHistory.datetime_utc <= datetime(on_date.year, on_date.month, on_date.day, 23, 59, 59),
        )
        .order_by(PriceHistory.datetime_utc.desc(), PriceHistory.id.desc())
        .first()
    )
    if row is not None and row.price is not None and row.datetime_utc is not None:
        return _to_decimal(row.price), row.datetime_utc.date()
    return _legacy_close_point_on_date(db, ticker=ticker, on_date=on_date)


def _latest_price_timestamp_on_or_before(
    db: Session,
    *,
    ticker: str,
    as_of_date: date,
) -> tuple[datetime | None, str | None]:
    row = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker == ticker,
            PriceHistory.datetime_utc <= datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 59),
        )
        .order_by(PriceHistory.datetime_utc.desc(), PriceHistory.id.desc())
        .first()
    )
    if row is not None and row.datetime_utc is not None:
        return row.datetime_utc, "price_history"
    fallback = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker == ticker,
            PricesHistory.date <= as_of_date,
        )
        .order_by(PricesHistory.date.desc(), PricesHistory.id.desc())
        .first()
    )
    if fallback is None or fallback.date is None:
        return None, None
    return datetime(fallback.date.year, fallback.date.month, fallback.date.day, 16, 0, 0), "fallback_legacy"


def _latest_fx_rate_on_or_before(
    db: Session,
    *,
    base_currency: str,
    quote_currency: str,
    as_of_date: date,
) -> Decimal | None:
    base = (base_currency or "").strip().upper()
    quote = (quote_currency or "").strip().upper()
    if base == quote:
        return Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
    row = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base,
            FXRate.quote_currency == quote,
            FXRate.datetime_utc <= datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 59),
        )
        .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
        .first()
    )
    if row is None or row.rate is None:
        return None
    return _to_decimal(row.rate, scale=_DECIMAL_RATE_SCALE)


def _latest_fx_rate_point_on_or_before(
    db: Session,
    *,
    base_currency: str,
    quote_currency: str,
    as_of_date: date,
) -> tuple[Decimal | None, date | None]:
    base = (base_currency or "").strip().upper()
    quote = (quote_currency or "").strip().upper()
    if base == quote:
        return Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP), as_of_date
    row = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base,
            FXRate.quote_currency == quote,
            FXRate.datetime_utc <= datetime(as_of_date.year, as_of_date.month, as_of_date.day, 23, 59, 59),
        )
        .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
        .first()
    )
    if row is None or row.rate is None or row.datetime_utc is None:
        try:
            fx = _lookup_close_fx_rate(
                db,
                quote_currency=quote,
                base_currency=base,
                on_date=as_of_date,
            )
            return _to_decimal(fx, scale=_DECIMAL_RATE_SCALE), as_of_date
        except PortfolioEngineError:
            return None, None
    return _to_decimal(row.rate, scale=_DECIMAL_RATE_SCALE), row.datetime_utc.date()


def _latest_fx_rate_point_on_or_before_dt(
    db: Session,
    *,
    base_currency: str,
    quote_currency: str,
    as_of_dt: datetime,
) -> tuple[Decimal | None, date | None]:
    base = (base_currency or "").strip().upper()
    quote = (quote_currency or "").strip().upper()
    if base == quote:
        return Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP), as_of_dt.date()
    row = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base,
            FXRate.quote_currency == quote,
            FXRate.datetime_utc <= as_of_dt,
        )
        .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
        .first()
    )
    if row is None or row.rate is None or row.datetime_utc is None:
        return _latest_fx_rate_point_on_or_before(
            db,
            base_currency=base,
            quote_currency=quote,
            as_of_date=as_of_dt.date(),
        )
    return _to_decimal(row.rate, scale=_DECIMAL_RATE_SCALE), row.datetime_utc.date()


def _fx_rate_point_on_date(
    db: Session,
    *,
    base_currency: str,
    quote_currency: str,
    on_date: date,
) -> tuple[Decimal | None, date | None]:
    base = (base_currency or "").strip().upper()
    quote = (quote_currency or "").strip().upper()
    if base == quote:
        return Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP), on_date
    row = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base,
            FXRate.quote_currency == quote,
            FXRate.datetime_utc >= datetime(on_date.year, on_date.month, on_date.day, 0, 0, 0),
            FXRate.datetime_utc <= datetime(on_date.year, on_date.month, on_date.day, 23, 59, 59),
        )
        .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
        .first()
    )
    if row is None or row.rate is None or row.datetime_utc is None:
        return None, None
    return _to_decimal(row.rate, scale=_DECIMAL_RATE_SCALE), row.datetime_utc.date()


def _fetch_fx_daily_close_rows(
    *,
    base_currency: str,
    quote_currency: str,
    start_date: date,
    end_date: date,
) -> list[tuple[date, Decimal]]:
    import yfinance as yf

    pair = f"{quote_currency}{base_currency}=X"
    hist = yf.Ticker(pair).history(
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        auto_adjust=False,
        actions=False,
    )
    if hist is None or hist.empty:
        return []
    rows: list[tuple[date, Decimal]] = []
    for idx, row in hist.iterrows():
        close = row.get("Close")
        if close is None:
            continue
        try:
            d = idx.date()
            px = Decimal(str(float(close))).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
        except Exception:
            continue
        if px <= Decimal("0"):
            continue
        rows.append((d, px))
    rows.sort(key=lambda x: x[0])
    return rows


def _legacy_fx_daily_rows(
    *,
    db: Session,
    base_currency: str,
    quote_currency: str,
    start_date: date,
    end_date: date,
) -> list[tuple[date, Decimal]]:
    direct_candidates = [f"{quote_currency}{base_currency}=X", f"{quote_currency}{base_currency}"]
    inverse_candidates = [f"{base_currency}{quote_currency}=X", f"{base_currency}{quote_currency}"]

    direct_rows = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker.in_(direct_candidates),
            PricesHistory.date >= start_date,
            PricesHistory.date <= end_date,
        )
        .order_by(PricesHistory.date.asc())
        .all()
    )
    by_date: dict[date, Decimal] = {}
    for row in direct_rows:
        if row.date is None or row.close is None:
            continue
        rate = _to_decimal(row.close, scale=_DECIMAL_RATE_SCALE)
        if rate > Decimal("0"):
            by_date[row.date] = rate

    inverse_rows = (
        db.query(PricesHistory)
        .filter(
            PricesHistory.ticker.in_(inverse_candidates),
            PricesHistory.date >= start_date,
            PricesHistory.date <= end_date,
        )
        .order_by(PricesHistory.date.asc())
        .all()
    )
    for row in inverse_rows:
        if row.date is None or row.close is None:
            continue
        inv = _to_decimal(row.close, scale=_DECIMAL_RATE_SCALE)
        if inv <= Decimal("0"):
            continue
        by_date[row.date] = (Decimal("1") / inv).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)

    return sorted(by_date.items(), key=lambda x: x[0])


def backfill_fx_history_if_missing(
    portfolio_id: str,
    db: Session,
    *,
    to_date_override: date | None = None,
) -> dict[str, object]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    tx_rows = _sorted_active_transactions_for_window(db, portfolio_id, include_generated=False)
    if not tx_rows:
        return {
            "portfolio_id": portfolio_id,
            "base_currency": (portfolio.base_currency or "USD").strip().upper() or "USD",
            "required_pairs": [],
            "fetched_pairs": [],
            "inserted_rows": 0,
            "from_date": None,
            "to_date": None,
        }

    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    required_quotes = sorted({c for c in ticker_currency.values() if c != base_currency})
    if not required_quotes:
        return {
            "portfolio_id": portfolio_id,
            "base_currency": base_currency,
            "required_pairs": [],
            "fetched_pairs": [],
            "inserted_rows": 0,
            "from_date": None,
            "to_date": None,
        }

    earliest_tx = min(r.trade_date for r in tx_rows)
    latest_build = _latest_completed_equity_build(db, portfolio_id)
    to_d = (
        to_date_override
        if to_date_override is not None
        else (latest_build.to_date if latest_build and latest_build.to_date else date.today())
    )
    if to_d < earliest_tx:
        to_d = earliest_tx

    inserted_total = 0
    fetched_pairs: list[str] = []
    end_dt = datetime(to_d.year, to_d.month, to_d.day, 23, 59, 59)

    for quote in required_quotes:
        if quote == base_currency:
            continue
        latest_existing_dt = (
            db.query(FXRate.datetime_utc)
            .filter(
                FXRate.base_currency == base_currency,
                FXRate.quote_currency == quote,
                FXRate.datetime_utc <= end_dt,
            )
            .order_by(FXRate.datetime_utc.desc(), FXRate.id.desc())
            .first()
        )
        fetch_start = earliest_tx
        if latest_existing_dt and latest_existing_dt[0] is not None:
            fetch_start = latest_existing_dt[0].date() + timedelta(days=1)
        if fetch_start > to_d:
            continue

        fetched_pairs.append(f"{quote}->{base_currency}")
        daily_rows: list[tuple[date, Decimal]] = []
        try:
            daily_rows = _fetch_fx_daily_close_rows(
                base_currency=base_currency,
                quote_currency=quote,
                start_date=fetch_start,
                end_date=to_d,
            )
        except Exception as exc:
            logger.warning(
                "[FXBackfill] Yahoo FX fetch failed for %s->%s: %s",
                quote,
                base_currency,
                exc,
            )
            daily_rows = []
        if not daily_rows:
            daily_rows = _legacy_fx_daily_rows(
                db=db,
                base_currency=base_currency,
                quote_currency=quote,
                start_date=fetch_start,
                end_date=to_d,
            )
        if daily_rows:
            existing_dt = {
                r[0].replace(tzinfo=None)
                for r in db.query(FXRate.datetime_utc)
                .filter(
                    FXRate.base_currency == base_currency,
                    FXRate.quote_currency == quote,
                    FXRate.datetime_utc >= datetime(earliest_tx.year, earliest_tx.month, earliest_tx.day, 0, 0, 0),
                    FXRate.datetime_utc <= end_dt,
                )
                .all()
            }
            for d, rate in daily_rows:
                ts = datetime(d.year, d.month, d.day, 20, 0, 0)
                if ts in existing_dt:
                    continue
                db.add(
                    FXRate(
                        id=str(uuid.uuid4()),
                        base_currency=base_currency,
                        quote_currency=quote,
                        datetime_utc=ts,
                        rate=float(rate),
                        source="fx_backfill",
                        created_at=datetime.utcnow(),
                    )
                )
                inserted_total += 1
        db.commit()

        still_missing = (
            db.query(FXRate.id)
            .filter(
                FXRate.base_currency == base_currency,
                FXRate.quote_currency == quote,
                FXRate.datetime_utc <= end_dt,
            )
            .first()
            is None
        )
        if still_missing:
            raise PortfolioEngineError(
                f"Missing required market inputs: historical FX close rate missing ({quote}->base)"
            )

    return {
        "portfolio_id": portfolio_id,
        "base_currency": base_currency,
        "required_pairs": [f"{q}->{base_currency}" for q in required_quotes],
        "fetched_pairs": fetched_pairs,
        "inserted_rows": inserted_total,
        "from_date": earliest_tx.isoformat(),
        "to_date": to_d.isoformat(),
    }


def _open_lot_basis_details_upto(
    db: Session,
    portfolio_id: str,
    *,
    as_of_date: date,
) -> dict[str, dict[str, Decimal]]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)
    tx_rows = _sorted_active_transactions_for_window(db, portfolio_id, to_date=as_of_date)
    lots: dict[str, list[tuple[Decimal, Decimal, Decimal]]] = defaultdict(list)  # shares, local_unit_px, base_unit_px
    for tx in tx_rows:
        ticker = tx.ticker_symbol_normalized
        shares = _to_decimal(tx.shares)
        if tx.tx_type == "Buy":
            if shares <= Decimal("0"):
                continue
            ticker_ccy = (ticker_currency.get(ticker) or base_currency).strip().upper()
            base_amount = _tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency)
            base_unit = (base_amount / shares).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            local_unit = _to_decimal(tx.price)
            # local_unit must stay in the trade/native currency and must not be derived
            # from already-converted base amounts, otherwise legacy mismatches (e.g. EUR
            # trades persisted with USD currency labels) get double-converted.
            tx_ccy = (tx.currency or base_currency).strip().upper() or base_currency
            if ticker_ccy != tx_ccy:
                local_unit = _to_decimal(tx.price)
            lots[ticker].append((shares, local_unit, base_unit))
        elif tx.tx_type == "Sell":
            remaining = shares
            while remaining > Decimal("0"):
                if not lots[ticker]:
                    raise PortfolioEngineError(f"Negative holdings detected for {ticker}.")
                lot_shares, lot_local_unit, lot_base_unit = lots[ticker][0]
                take = lot_shares if lot_shares <= remaining else remaining
                if take == lot_shares:
                    lots[ticker].pop(0)
                else:
                    lots[ticker][0] = (
                        (lot_shares - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP),
                        lot_local_unit,
                        lot_base_unit,
                    )
                remaining = (remaining - take).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    out: dict[str, dict[str, Decimal]] = {}
    for ticker, lot_rows in lots.items():
        open_local_cost = Decimal("0")
        open_base_cost = Decimal("0")
        open_qty = Decimal("0")
        for shares, local_unit, base_unit in lot_rows:
            open_qty += shares
            open_local_cost += (shares * local_unit).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            open_base_cost += (shares * base_unit).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        out[ticker] = {
            "open_qty": open_qty.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP),
            "open_local_cost": open_local_cost.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP),
            "open_base_cost": open_base_cost.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP),
        }
    return out


def compute_performance_breakdown(db: Session, portfolio_id: str) -> dict[str, float]:
    portfolio = get_portfolio_or_error(db, portfolio_id)
    latest_row = _latest_equity_row_or_none(db, portfolio_id)
    if latest_row is None:
        raise PortfolioEngineError(f"No equity history rows found for portfolio '{portfolio_id}'.")
    as_of = latest_row.date
    base_currency = (portfolio.base_currency or "USD").strip().upper() or "USD"
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)

    realized_gain = (
        db.query(ClosedPosition)
        .filter(ClosedPosition.portfolio_id == portfolio_id)
        .with_entities(ClosedPosition.realized_gain)
        .all()
    )
    realized_total = sum((_to_decimal(r[0]) for r in realized_gain), Decimal("0")).quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    )

    fx_rows = (
        db.query(ClosedPosition)
        .filter(ClosedPosition.portfolio_id == portfolio_id)
        .with_entities(ClosedPosition.fx_component)
        .all()
    )
    closed_fx_total = sum((_to_decimal(r[0]) for r in fx_rows), Decimal("0")).quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    )

    dividend_rows = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.trade_date <= as_of,
        )
        .all()
    )
    dividend_total = sum(
        (_tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency) for tx in dividend_rows),
        Decimal("0"),
    ).quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    )
    dividend_gross_total = Decimal("0")
    dividend_withholding_total = Decimal("0")
    for tx in dividend_rows:
        meta = _parse_metadata(tx.metadata_json) or {}
        gross_native = _to_decimal(meta.get("gross_amount_native"))
        withholding_native = _to_decimal(meta.get("withholding_amount_native"))
        if gross_native == Decimal("0") and withholding_native == Decimal("0"):
            # Manual dividend entries are already net values.
<<<<<<< HEAD
            base_amount = _tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency)
=======
            base_amount = _tx_base_amount(tx)
>>>>>>> origin/main
            dividend_gross_total += base_amount
            continue
        fx_exec = _to_decimal(tx.fx_at_execution, scale=_DECIMAL_RATE_SCALE)
        dividend_gross_total += (gross_native * fx_exec).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        dividend_withholding_total += (withholding_native * fx_exec).quantize(
            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
        )
    dividend_gross_total = dividend_gross_total.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    dividend_withholding_total = dividend_withholding_total.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

    invested_rows = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.is_deleted == False,
            PortfolioTransaction.tx_type == "Buy",
            PortfolioTransaction.trade_date <= as_of,
        )
        .all()
    )
    invested_capital = sum(
        (_tx_base_amount(tx, db=db, base_currency=base_currency, ticker_currency=ticker_currency) for tx in invested_rows),
        Decimal("0"),
    ).quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    )

    holdings_state = _compute_holdings_state_upto(db, portfolio_id, as_of_date=as_of)
    lot_basis = _open_lot_basis_details_upto(db, portfolio_id, as_of_date=as_of)
    ticker_currency = _ticker_currency_map_or_error(db, portfolio_id)

    unrealized_total = Decimal("0")
    open_fx_total = Decimal("0")
    for ticker, state in holdings_state.items():
        qty = _to_decimal(state.get("quantity"))
        if qty <= Decimal("0"):
            continue
        px = _latest_price_on_or_before(db, ticker=ticker, as_of_date=as_of)
        if px is None:
            raise PortfolioEngineError(
                _format_missing_market_inputs(as_of, [f"price:{ticker}"], base_currency=base_currency)
            )
        ccy = (ticker_currency.get(ticker) or base_currency).strip().upper()
        fx = _latest_fx_rate_on_or_before(db, base_currency=base_currency, quote_currency=ccy, as_of_date=as_of)
        if fx is None:
            raise PortfolioEngineError(
                _format_missing_market_inputs(as_of, [f"fx:{ccy}"], base_currency=base_currency)
            )
        market_value_base = (qty * px * fx).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
        cost_basis_base = _to_decimal(state.get("total_cost_basis"))
        unrealized_total += (market_value_base - cost_basis_base).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)

        basis_row = lot_basis.get(ticker, {})
        open_local_cost = _to_decimal(basis_row.get("open_local_cost"))
        open_base_cost = _to_decimal(basis_row.get("open_base_cost"))
        avg_exec_fx = Decimal("1").quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
        if ccy != base_currency and open_local_cost != Decimal("0"):
            avg_exec_fx = (open_base_cost / open_local_cost).quantize(_DECIMAL_RATE_SCALE, rounding=ROUND_HALF_UP)
            current_local_value = (qty * px).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
            open_fx_total += (current_local_value * (fx - avg_exec_fx)).quantize(
                _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
            )

    unrealized_total = unrealized_total.quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    fx_total = (closed_fx_total + open_fx_total).quantize(_DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP)
    total_gain = (realized_total + unrealized_total + dividend_total).quantize(
        _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
    )
    total_gain_pct = Decimal("0")
    if invested_capital != Decimal("0"):
        total_gain_pct = ((total_gain / invested_capital) * Decimal("100")).quantize(
            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
        )
    return {
        "realized_gain": float(realized_total),
        "unrealized_gain": float(unrealized_total),
        "fx_gain": float(fx_total),
        "dividend_gain": float(dividend_total),
        "dividend_gross_gain": float(dividend_gross_total),
        "dividend_withholding": float(dividend_withholding_total),
        "dividend_net_gain": float(dividend_total),
        "total_gain": float(total_gain),
        "total_gain_pct": float(total_gain_pct),
    }


def _compute_return_pct(
    latest_value: Decimal,
    baseline_value: Decimal,
) -> float | None:
    if baseline_value == Decimal("0"):
        return None
    return float(
        (((latest_value - baseline_value) / baseline_value) * Decimal("100")).quantize(
            _DECIMAL_MONEY_SCALE, rounding=ROUND_HALF_UP
        )
    )


def compute_time_returns(db: Session, portfolio_id: str) -> dict[str, float | None]:
    get_portfolio_or_error(db, portfolio_id)
    settings = _load_portfolio_settings(db, portfolio_id)
    cash_mode = str(settings.get("cash_management_mode") or "track_cash").strip().lower()
    rows = _resolve_latest_equity_rows(db, portfolio_id)
    if not rows:
        raise PortfolioEngineError(f"No equity history rows found for portfolio '{portfolio_id}'.")

    latest = rows[-1]
    use_twr = cash_mode == "ignore_cash"

    if use_twr:
        latest_value = _to_decimal(latest.twr_index if latest.twr_index is not None else 1.0)
        inception_row = rows[0]
        inception_value = _to_decimal(inception_row.twr_index if inception_row.twr_index is not None else 1.0)
        since_inception = _compute_return_pct(latest_value, inception_value)

        ytd_start = date(latest.date.year, 1, 1)
        ytd_row = next((r for r in rows if r.date >= ytd_start), None)
        ytd_return = None
        if ytd_row is not None:
            ytd_return = _compute_return_pct(
                latest_value,
                _to_decimal(ytd_row.twr_index if ytd_row.twr_index is not None else 1.0),
            )

        one_year_return = None
        one_year_anchor = latest.date - timedelta(days=365)
        if rows[0].date <= one_year_anchor:
            one_year_row = None
            for r in reversed(rows):
                if r.date <= one_year_anchor:
                    one_year_row = r
                    break
            if one_year_row is not None:
                one_year_return = _compute_return_pct(
                    latest_value,
                    _to_decimal(one_year_row.twr_index if one_year_row.twr_index is not None else 1.0),
                )
    else:
        latest_value = _to_decimal(latest.total_equity)

        inception_row = rows[0]
        inception_value = _to_decimal(inception_row.total_equity)
        since_inception = _compute_return_pct(latest_value, inception_value)

        ytd_start = date(latest.date.year, 1, 1)
        ytd_row = next((r for r in rows if r.date >= ytd_start), None)
        ytd_return = None
        if ytd_row is not None:
            ytd_return = _compute_return_pct(latest_value, _to_decimal(ytd_row.total_equity))

        one_year_return = None
        one_year_anchor = latest.date - timedelta(days=365)
        if rows[0].date <= one_year_anchor:
            one_year_row = None
            for r in reversed(rows):
                if r.date <= one_year_anchor:
                    one_year_row = r
                    break
            if one_year_row is not None:
                one_year_return = _compute_return_pct(latest_value, _to_decimal(one_year_row.total_equity))

    return {
        "since_inception_return_pct": since_inception,
        "ytd_return_pct": ytd_return,
        "one_year_return_pct": one_year_return,
    }


def get_open_tickers_for_portfolio(
    db: Session,
    portfolio_id: str,
    *,
    as_of_date: date | None = None,
) -> list[str]:
    as_of = as_of_date or date.today()
    state = _compute_holdings_state_upto(db, portfolio_id, as_of_date=as_of)
    out: list[str] = []
    for ticker, item in state.items():
        qty = _to_decimal(item.get("quantity", 0.0))
        if qty > Decimal("0"):
            out.append(ticker)
    return sorted(out)


def get_active_open_tickers(db: Session) -> list[str]:
    portfolios = (
        db.query(Portfolio)
        .filter(Portfolio.is_deleted == False)
        .order_by(Portfolio.id.asc())
        .all()
    )
    merged: set[str] = set()
    for p in portfolios:
        merged.update(get_open_tickers_for_portfolio(db, p.id))
    return sorted(merged)


def get_required_fx_pairs_for_open_positions(db: Session) -> list[tuple[str, str]]:
    portfolios = (
        db.query(Portfolio)
        .filter(Portfolio.is_deleted == False)
        .order_by(Portfolio.id.asc())
        .all()
    )
    out: set[tuple[str, str]] = set()
    for p in portfolios:
        open_tickers = set(get_open_tickers_for_portfolio(db, p.id))
        if not open_tickers:
            continue
        ccy_map = _ticker_currency_map_or_error(db, p.id)
        base = (p.base_currency or "USD").strip().upper() or "USD"
        for t in sorted(open_tickers):
            quote = (ccy_map.get(t) or base).strip().upper()
            if quote != base:
                out.add((base, quote))
    return sorted(out)


def insert_price_history_point(
    db: Session,
    *,
    ticker: str,
    datetime_utc: datetime,
    price: float,
    adjusted_price: float | None,
    source: str,
) -> bool:
    existing = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker == ticker,
            PriceHistory.datetime_utc == datetime_utc,
        )
        .first()
    )
    if existing:
        return False
    row = PriceHistory(
        id=str(uuid.uuid4()),
        ticker=ticker,
        datetime_utc=datetime_utc,
        price=float(price),
        adjusted_price=(None if adjusted_price is None else float(adjusted_price)),
        source=source,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    return True


def insert_fx_rate_point(
    db: Session,
    *,
    base_currency: str,
    quote_currency: str,
    datetime_utc: datetime,
    rate: float,
    source: str,
) -> bool:
    existing = (
        db.query(FXRate)
        .filter(
            FXRate.base_currency == base_currency,
            FXRate.quote_currency == quote_currency,
            FXRate.datetime_utc == datetime_utc,
        )
        .first()
    )
    if existing:
        return False
    row = FXRate(
        id=str(uuid.uuid4()),
        base_currency=base_currency,
        quote_currency=quote_currency,
        datetime_utc=datetime_utc,
        rate=float(rate),
        source=source,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    return True


def create_portfolio_daily_snapshot(
    db: Session,
    *,
    portfolio_id: str,
    snapshot_date: date,
) -> dict[str, object]:
    existing = (
        db.query(PortfolioSnapshot)
        .filter(
            PortfolioSnapshot.portfolio_id == portfolio_id,
            PortfolioSnapshot.snapshot_date == snapshot_date,
        )
        .first()
    )
    if existing:
        return {
            "portfolio_id": portfolio_id,
            "snapshot_date": snapshot_date.isoformat(),
            "inserted": False,
            "snapshot_id": existing.id,
        }
    rows = _resolve_latest_equity_rows(db, portfolio_id)
    if not rows:
        raise PortfolioEngineError(f"No equity history rows found for portfolio '{portfolio_id}'.")
    target = None
    for row in rows:
        if row.date <= snapshot_date:
            target = row
    if target is None:
        raise PortfolioEngineError(
            f"No equity history row on or before {snapshot_date.isoformat()} for portfolio '{portfolio_id}'."
        )
    snap = PortfolioSnapshot(
        id=str(uuid.uuid4()),
        portfolio_id=portfolio_id,
        snapshot_date=snapshot_date,
        total_equity=float(target.total_equity),
        total_cash=float(target.cash_balance),
        unrealized=float(target.unrealized_gain_value),
        realized=float(target.realized_gain_value),
        market_component=float(target.market_return_component or 0.0),
        fx_component=float(target.fx_return_component or 0.0),
        created_at=datetime.utcnow(),
    )
    db.add(snap)
    db.commit()
    return {
        "portfolio_id": portfolio_id,
        "snapshot_date": snapshot_date.isoformat(),
        "inserted": True,
        "snapshot_id": snap.id,
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


async def run_portfolio_creation_flow(db: Session, portfolio_id: str, strict: bool = False) -> dict[str, object]:
    """
    Backend-only flow:
      upload portfolio (already saved to hardcoded CSV)
      -> extract tickers
      -> ensure_price_coverage
      -> run deterministic engine
      -> persist outputs
    """
    portfolio = get_portfolio_or_error(db, portfolio_id)
    _validate_portfolio_dividend_withholding(portfolio)
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

        # Phase 9.1 stabilization: keep legacy processing flow and ensure
        # deterministic equity history exists for dashboard endpoints.
        latest_eq_build = _latest_completed_equity_build(db, portfolio_id)
        eq_mode = "incremental" if latest_eq_build else "full"
        eq_force = False if latest_eq_build else True
        backfill_fx_history_if_missing(portfolio_id, db)
        backfill_dividend_history_if_missing(portfolio_id, db, strict=bool(strict))
        try:
            eq_build = rebuild_equity_history(
                db,
                portfolio_id,
                mode=eq_mode,
                force=eq_force,
                strict=bool(strict),
            )
        except PortfolioEngineError:
            # Settings or historical input changes can invalidate incremental mode.
            # Force a deterministic full rebuild in the same processing flow.
            eq_build = rebuild_equity_history(
                db,
                portfolio_id,
                mode="full",
                force=True,
                strict=bool(strict),
            )
        payload["equity_history_build"] = eq_build

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
