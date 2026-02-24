from __future__ import annotations

import csv
import math
import os
from bisect import bisect_right
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from statistics import pstdev
from typing import Iterable

BASE_CURRENCY = "USD"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PORTFOLIO_PATH = PROJECT_ROOT / "data_exports" / "portfolio1.csv"
PRICES_PATH = PROJECT_ROOT / "data_exports" / "PricesHistory_export.csv"
OUTPUT_DIR = PROJECT_ROOT / "data_exports" / "engine_outputs"

PORTFOLIO_COLUMN_ALIASES = {
    "ticker": ["Ticker Symbol", "Tycker Symbol", "ticker", "Ticker"],
    "date": ["Date", "date"],
    "shares": ["Shares", "shares"],
    "price": ["Price", "price"],
    "cost": ["Cost", "cost"],
    "type": ["Type", "type"],
    "currency": ["Currency", "currency"],
    "fx_rate": ["FX", "fx", "fx_rate", "fx_to_usd", "FX to USD"],
}

PRICES_COLUMN_ALIASES = {
    "ticker": ["Ticker Symbol", "ticker", "Ticker"],
    "date": ["Date", "date"],
    "close": ["Close price", "close", "close_adj", "Close"],
    "currency": ["Currency", "currency"],
    "fx_rate": ["FX", "fx", "fx_rate", "fx_to_usd", "FX to USD"],
}

DATE_FORMATS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d/%m/%y",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%m/%d/%y",
)


class PortfolioEngineError(RuntimeError):
    pass


def _auto_dict_reader(f, file_path: Path) -> csv.DictReader:
    """
    Build a DictReader with delimiter auto-detection for CSV-like exports.
    Supported delimiters: comma, tab, semicolon.
    """
    sample = f.read(4096)
    f.seek(0)

    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        delimiter = dialect.delimiter
    except csv.Error:
        first_line = sample.splitlines()[0] if sample else ""
        counts = {
            ",": first_line.count(","),
            ";": first_line.count(";"),
            "\t": first_line.count("\t"),
        }
        delimiter = max(counts, key=counts.get) if first_line else ","

    reader = csv.DictReader(f, delimiter=delimiter)
    if not reader.fieldnames:
        raise PortfolioEngineError(f"File has no headers: {file_path}")
    return reader


@dataclass(frozen=True)
class Transaction:
    row_id: int
    ticker_symbol: str
    ticker: str
    trade_date: date
    shares: float
    price: float
    cost: float
    tx_type: str
    currency: str


@dataclass(frozen=True)
class PricePoint:
    ticker_symbol: str
    ticker: str
    price_date: date
    close_price: float
    currency: str
    fx_to_usd: float


@dataclass
class Lot:
    lot_id: int
    ticker: str
    ticker_symbol: str
    entry_date: date
    entry_price: float
    entry_fx_rate: float
    shares_remaining: float
    original_shares: float


@dataclass(frozen=True)
class CashEvent:
    event_date: date
    ticker: str
    ticker_symbol: str
    tx_type: str
    amount: float
    row_id: int


@dataclass(frozen=True)
class RealizedEvent:
    ticker: str
    ticker_symbol: str
    trade_date: date
    sell_row_id: int
    lot_id: int
    matched_shares: float
    entry_date: date
    entry_price: float
    sell_price: float
    entry_fx: float
    sell_fx: float
    local_gain: float
    fx_gain: float
    total_realized: float


@dataclass(frozen=True)
class UnrealizedEvent:
    ticker: str
    ticker_symbol: str
    as_of_date: date
    lot_id: int
    shares_remaining: float
    entry_date: date
    entry_price: float
    current_price: float
    entry_fx: float
    current_fx: float
    local_unrealized: float
    fx_unrealized: float
    total_unrealized: float


@dataclass(frozen=True)
class DailyEquityPoint:
    as_of_date: date
    cash_balance: float
    market_value: float
    total_equity: float
    external_flow: float
    daily_return: float | None


@dataclass(frozen=True)
class EngineOutputs:
    lot_audit: list[dict[str, object]]
    realized_report: list[dict[str, object]]
    unrealized_snapshot: list[dict[str, object]]
    daily_equity_curve: list[dict[str, object]]
    fx_attribution: list[dict[str, object]]
    irr_summary: list[dict[str, object]]
    portfolio_summary: list[dict[str, object]]
    warnings: list[str]
    correction_events: list[dict[str, object]] = field(default_factory=list)
    prior_close_fallback: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class PortfolioScope:
    tickers: list[str]
    earliest_trade_date: date


def _norm_ticker(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    return value.split(":")[-1].strip().upper()


def _find_column(headers: Iterable[str], aliases: list[str], logical: str, file_path: Path) -> str:
    header_map = {h.strip().lower(): h for h in headers}
    for alias in aliases:
        matched = header_map.get(alias.strip().lower())
        if matched:
            return matched
    raise PortfolioEngineError(
        f"Missing required column '{logical}' in {file_path}. "
        f"Accepted aliases: {aliases}. Available headers: {list(headers)}"
    )


def _find_columns(headers: Iterable[str], aliases: list[str]) -> list[str]:
    header_map = {h.strip().lower(): h for h in headers}
    found: list[str] = []
    for alias in aliases:
        matched = header_map.get(alias.strip().lower())
        if matched:
            found.append(matched)
    return found


def _parse_date(raw: str, file_path: Path, row_idx: int, column: str) -> date:
    value = (raw or "").strip()
    if not value:
        raise PortfolioEngineError(
            f"Empty date at {file_path} row {row_idx} column '{column}'."
        )
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise PortfolioEngineError(
        f"Cannot parse date '{value}' at {file_path} row {row_idx} column '{column}'."
    )


def _parse_positive_float(raw: str, file_path: Path, row_idx: int, column: str, allow_empty: bool = False) -> float:
    value = (raw or "").strip()
    if not value:
        if allow_empty:
            return 0.0
        raise PortfolioEngineError(
            f"Missing numeric value at {file_path} row {row_idx} column '{column}'."
        )
    try:
        out = float(value.replace(",", ""))
    except ValueError as exc:
        raise PortfolioEngineError(
            f"Invalid numeric value '{value}' at {file_path} row {row_idx} column '{column}'."
        ) from exc
    if out <= 0:
        raise PortfolioEngineError(
            f"Expected positive number at {file_path} row {row_idx} column '{column}', got {out}."
        )
    return out


def _parse_optional_float(raw: str) -> float | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None


def _load_transactions(path: Path) -> list[Transaction]:
    if not path.exists():
        raise PortfolioEngineError(f"Missing required file: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = _auto_dict_reader(f, path)

        ticker_col = _find_column(reader.fieldnames, PORTFOLIO_COLUMN_ALIASES["ticker"], "ticker", path)
        date_col = _find_column(reader.fieldnames, PORTFOLIO_COLUMN_ALIASES["date"], "date", path)
        shares_col = _find_column(reader.fieldnames, PORTFOLIO_COLUMN_ALIASES["shares"], "shares", path)
        price_col = _find_column(reader.fieldnames, PORTFOLIO_COLUMN_ALIASES["price"], "price", path)
        cost_col = _find_column(reader.fieldnames, PORTFOLIO_COLUMN_ALIASES["cost"], "cost", path)
        type_col = _find_column(reader.fieldnames, PORTFOLIO_COLUMN_ALIASES["type"], "type", path)
        currency_col = None
        for c in PORTFOLIO_COLUMN_ALIASES["currency"]:
            if c in reader.fieldnames:
                currency_col = c
                break

        transactions: list[Transaction] = []
        for idx, row in enumerate(reader, start=2):
            ticker_symbol = (row.get(ticker_col) or "").strip()
            ticker = _norm_ticker(ticker_symbol)
            if not ticker:
                raise PortfolioEngineError(f"Missing ticker at {path} row {idx}.")

            trade_date = _parse_date(row.get(date_col, ""), path, idx, date_col)
            tx_type = (row.get(type_col) or "").strip()
            if tx_type not in {"Buy", "Sell", "Dividend"}:
                raise PortfolioEngineError(
                    f"Invalid Type '{tx_type}' at {path} row {idx}. Allowed: Buy, Sell, Dividend."
                )

            cost = _parse_positive_float(row.get(cost_col, ""), path, idx, cost_col)
            price = _parse_positive_float(row.get(price_col, ""), path, idx, price_col)
            shares = _parse_positive_float(
                row.get(shares_col, ""),
                path,
                idx,
                shares_col,
                allow_empty=(tx_type == "Dividend"),
            )
            if tx_type == "Dividend":
                shares = shares if shares > 0 else 0.0

            if tx_type in {"Buy", "Sell"} and shares <= 0:
                raise PortfolioEngineError(
                    f"Shares must be positive for {tx_type} at {path} row {idx}."
                )

            currency = (row.get(currency_col, "") if currency_col else "").strip() or BASE_CURRENCY
            transactions.append(
                Transaction(
                    row_id=idx,
                    ticker_symbol=ticker_symbol,
                    ticker=ticker,
                    trade_date=trade_date,
                    shares=shares,
                    price=price,
                    cost=cost,
                    tx_type=tx_type,
                    currency=currency,
                )
            )

    transactions.sort(key=lambda t: (t.trade_date, t.ticker, t.row_id))
    return transactions


def extract_portfolio_scope(path: Path = PORTFOLIO_PATH) -> PortfolioScope:
    transactions = _load_transactions(path)
    if not transactions:
        raise PortfolioEngineError(f"No transactions found in {path}")
    tickers = sorted({t.ticker for t in transactions})
    earliest = min(t.trade_date for t in transactions)
    return PortfolioScope(tickers=tickers, earliest_trade_date=earliest)


def load_portfolio_transactions(path: Path = PORTFOLIO_PATH) -> list[Transaction]:
    """
    Public loader for orchestration layers that need deterministic access
    to validated portfolio transactions.
    """
    return _load_transactions(path)


def _load_prices(path: Path) -> tuple[list[PricePoint], dict[tuple[str, date], PricePoint], dict[str, str]]:
    if not path.exists():
        raise PortfolioEngineError(f"Missing required file: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = _auto_dict_reader(f, path)

        ticker_col = _find_column(reader.fieldnames, PRICES_COLUMN_ALIASES["ticker"], "ticker", path)
        date_col = _find_column(reader.fieldnames, PRICES_COLUMN_ALIASES["date"], "date", path)
        close_cols = _find_columns(reader.fieldnames, PRICES_COLUMN_ALIASES["close"])
        if not close_cols:
            raise PortfolioEngineError(
                f"Missing required column 'close' in {path}. "
                f"Accepted aliases: {PRICES_COLUMN_ALIASES['close']}. Available headers: {reader.fieldnames}"
            )

        currency_col = None
        for alias in PRICES_COLUMN_ALIASES["currency"]:
            if alias in reader.fieldnames:
                currency_col = alias
                break

        fx_col = None
        for alias in PRICES_COLUMN_ALIASES["fx_rate"]:
            if alias in reader.fieldnames:
                fx_col = alias
                break

        points: list[PricePoint] = []
        by_key: dict[tuple[str, date], PricePoint] = {}
        ticker_currency: dict[str, str] = {}

        for idx, row in enumerate(reader, start=2):
            ticker_symbol = (row.get(ticker_col) or "").strip()
            ticker = _norm_ticker(ticker_symbol)
            if not ticker:
                raise PortfolioEngineError(f"Missing ticker at {path} row {idx}.")

            px_date = _parse_date(row.get(date_col, ""), path, idx, date_col)
            close_raw = ""
            close_source = close_cols[0]
            for c in close_cols:
                val = (row.get(c) or "").strip()
                if val:
                    close_raw = val
                    close_source = c
                    break
            close_price = _parse_positive_float(close_raw, path, idx, close_source)
            currency = (row.get(currency_col, "") if currency_col else "").strip() or BASE_CURRENCY
            fx_rate = _parse_optional_float(row.get(fx_col, "") if fx_col else "")

            if currency != BASE_CURRENCY and not fx_rate:
                raise PortfolioEngineError(
                    f"Ticker {ticker} has currency {currency} at {path} row {idx} but no FX rate column/value."
                )
            fx_to_usd = fx_rate if fx_rate else 1.0
            if fx_to_usd <= 0:
                raise PortfolioEngineError(
                    f"Invalid FX rate {fx_to_usd} at {path} row {idx}."
                )

            p = PricePoint(
                ticker_symbol=ticker_symbol,
                ticker=ticker,
                price_date=px_date,
                close_price=close_price,
                currency=currency,
                fx_to_usd=fx_to_usd,
            )
            by_key[(ticker, px_date)] = p
            points.append(p)
            ticker_currency[ticker] = currency

    points.sort(key=lambda p: (p.price_date, p.ticker))
    return points, by_key, ticker_currency


def _price_or_fail(price_map: dict[tuple[str, date], PricePoint], ticker: str, on_date: date, context: str) -> PricePoint:
    p = price_map.get((ticker, on_date))
    if not p:
        raise PortfolioEngineError(
            f"Missing price row for ticker={ticker} date={on_date.isoformat()} ({context})."
        )
    return p


def _price_on_or_before_or_fail(
    price_by_ticker: dict[str, dict[date, PricePoint]],
    sorted_dates_by_ticker: dict[str, list[date]],
    ticker: str,
    on_date: date,
    context: str,
) -> PricePoint:
    by_date = price_by_ticker.get(ticker) or {}
    exact = by_date.get(on_date)
    if exact:
        return exact
    dates = sorted_dates_by_ticker.get(ticker) or []
    if not dates:
        raise PortfolioEngineError(
            f"Missing price row for ticker={ticker} date={on_date.isoformat()} ({context})."
        )
    idx = bisect_right(dates, on_date) - 1
    if idx < 0:
        raise PortfolioEngineError(
            f"Missing price row for ticker={ticker} date={on_date.isoformat()} ({context})."
        )
    return by_date[dates[idx]]


def _irr_from_cashflows(cashflows: list[tuple[date, float]]) -> float | None:
    if not cashflows:
        return None
    has_pos = any(v > 0 for _, v in cashflows)
    has_neg = any(v < 0 for _, v in cashflows)
    if not (has_pos and has_neg):
        return None

    d0 = min(d for d, _ in cashflows)

    def npv(rate: float) -> float:
        total = 0.0
        for d, amount in cashflows:
            years = (d - d0).days / 365.2425
            total += amount / ((1.0 + rate) ** years)
        return total

    low = -0.999999
    high = 10.0
    f_low = npv(low)
    f_high = npv(high)
    guard = 0
    while f_low * f_high > 0 and guard < 30:
        high *= 2.0
        f_high = npv(high)
        guard += 1

    if f_low * f_high > 0:
        return None

    for _ in range(200):
        mid = (low + high) / 2
        f_mid = npv(mid)
        if abs(f_mid) < 1e-10:
            return mid
        if f_low * f_mid <= 0:
            high, f_high = mid, f_mid
        else:
            low, f_low = mid, f_mid
    return (low + high) / 2


def _clamp_sell_execution(
    ticker: str,
    row_id: int,
    requested_shares: float,
    available_shares: float,
    price: float,
    warnings: list[str],
    *,
    allow_clamp: bool,
    correction_events: list[dict[str, object]],
) -> tuple[float, float]:
    """
    Execute sell at available quantity only when overage is < 1%.
    Otherwise raise a hard validation error.
    Returns (executed_shares, executed_cost).
    """
    if available_shares <= 1e-12:
        raise PortfolioEngineError(
            f"Sell exceeds available shares for {ticker} at row {row_id}. "
            f"Sell={requested_shares}, available={available_shares}."
        )

    executed = min(requested_shares, available_shares)
    overage = requested_shares - executed
    if overage > 1e-12:
        if not allow_clamp:
            raise PortfolioEngineError(
                f"Sell exceeds available shares for {ticker} at row {row_id}. "
                "Clamping is disabled by policy. "
                f"Sell={requested_shares}, available={available_shares}."
            )
        overage_ratio = overage / available_shares if available_shares > 0 else float("inf")
        if overage_ratio >= 0.01:
            raise PortfolioEngineError(
                f"Sell exceeds available shares for {ticker} at row {row_id}. "
                f"Sell={requested_shares}, available={available_shares}."
            )
        correction_events.append(
            {
                "ticker": ticker,
                "row_id": row_id,
                "requested_shares": requested_shares,
                "available_shares": available_shares,
                "executed_shares": executed,
                "delta_shares": overage,
                "reason": "sell_clamp_lt_1pct",
            }
        )
        warnings.append(
            f"WARNING[{ticker}] row {row_id}: sell clamped from {requested_shares} to available {executed} shares."
        )
    return executed, executed * price


def run_portfolio_engine(
    portfolio_path: Path = PORTFOLIO_PATH,
    prices_path: Path = PRICES_PATH,
) -> EngineOutputs:
    transactions = _load_transactions(portfolio_path)
    prices, price_map, ticker_currency = _load_prices(prices_path)
    price_by_ticker: dict[str, dict[date, PricePoint]] = defaultdict(dict)
    for p in prices:
        price_by_ticker[p.ticker][p.price_date] = p
    sorted_dates_by_ticker: dict[str, list[date]] = {
        t: sorted(by_date.keys()) for t, by_date in price_by_ticker.items()
    }

    tx_tickers = {t.ticker for t in transactions}
    price_tickers = {p.ticker for p in prices}
    missing_price_tickers = sorted(tx_tickers - price_tickers)
    if missing_price_tickers:
        raise PortfolioEngineError(
            "Price history missing tickers required by portfolio: " + ", ".join(missing_price_tickers)
        )

    open_lots: dict[str, deque[Lot]] = defaultdict(deque)
    lot_seq = 0
    cash_balance = 0.0

    cash_events: list[CashEvent] = []
    realized_events: list[RealizedEvent] = []
    warnings: list[str] = []
    ffill_stats: dict[tuple[str, str], dict[str, date | int]] = {}
    sell_executed_shares: dict[int, float] = {}
    sell_executed_cost: dict[int, float] = {}
    correction_events: list[dict[str, object]] = []
    allow_sell_clamp = os.getenv("PORTFOLIO_ENABLE_SELL_CLAMP", "1").strip().lower() in {"1", "true", "yes"}

    for tx in transactions:
        tx_price = _price_or_fail(price_map, tx.ticker, tx.trade_date, context=f"transaction row {tx.row_id}")
        tx_fx = tx_price.fx_to_usd

        if tx.tx_type == "Buy":
            lot_seq += 1
            open_lots[tx.ticker].append(
                Lot(
                    lot_id=lot_seq,
                    ticker=tx.ticker,
                    ticker_symbol=tx.ticker_symbol,
                    entry_date=tx.trade_date,
                    entry_price=tx.price,
                    entry_fx_rate=tx_fx,
                    shares_remaining=tx.shares,
                    original_shares=tx.shares,
                )
            )
            cash_balance -= tx.cost
            cash_events.append(CashEvent(tx.trade_date, tx.ticker, tx.ticker_symbol, tx.tx_type, -tx.cost, tx.row_id))
            continue

        if tx.tx_type == "Dividend":
            cash_balance += tx.cost
            cash_events.append(CashEvent(tx.trade_date, tx.ticker, tx.ticker_symbol, tx.tx_type, tx.cost, tx.row_id))
            continue

        # Sell
        to_sell = tx.shares
        lots = open_lots[tx.ticker]
        available = sum(l.shares_remaining for l in lots)
        to_sell, executed_cost = _clamp_sell_execution(
            tx.ticker,
            tx.row_id,
            requested_shares=to_sell,
            available_shares=available,
            price=tx.price,
            warnings=warnings,
            allow_clamp=allow_sell_clamp,
            correction_events=correction_events,
        )
        sell_executed_shares[tx.row_id] = to_sell
        sell_executed_cost[tx.row_id] = executed_cost

        sell_fx = tx_fx
        while to_sell > 1e-12:
            if not lots:
                raise PortfolioEngineError(
                    f"Sell exceeds FIFO lots for {tx.ticker} at row {tx.row_id}."
                )
            lot = lots[0]
            matched = min(to_sell, lot.shares_remaining)
            local_gain = matched * (tx.price - lot.entry_price)
            fx_gain = matched * lot.entry_price * (sell_fx - lot.entry_fx_rate)
            total_realized = local_gain + fx_gain
            realized_events.append(
                RealizedEvent(
                    ticker=tx.ticker,
                    ticker_symbol=tx.ticker_symbol,
                    trade_date=tx.trade_date,
                    sell_row_id=tx.row_id,
                    lot_id=lot.lot_id,
                    matched_shares=matched,
                    entry_date=lot.entry_date,
                    entry_price=lot.entry_price,
                    sell_price=tx.price,
                    entry_fx=lot.entry_fx_rate,
                    sell_fx=sell_fx,
                    local_gain=local_gain,
                    fx_gain=fx_gain,
                    total_realized=total_realized,
                )
            )
            lot.shares_remaining -= matched
            to_sell -= matched
            if lot.shares_remaining <= 1e-12:
                lots.popleft()

        cash_balance += executed_cost
        cash_events.append(CashEvent(tx.trade_date, tx.ticker, tx.ticker_symbol, tx.tx_type, executed_cost, tx.row_id))

    for ticker, lots in open_lots.items():
        remaining = sum(l.shares_remaining for l in lots)
        if remaining < -1e-9:
            raise PortfolioEngineError(
                f"Negative inventory occurs for {ticker}. Remaining shares = {remaining}"
            )

    valuation_dates = sorted({p.price_date for p in prices if p.ticker in tx_tickers})
    if not valuation_dates:
        raise PortfolioEngineError("No valuation dates available for portfolio tickers in price history.")

    events_by_date: dict[date, list[CashEvent]] = defaultdict(list)
    for ev in cash_events:
        events_by_date[ev.event_date].append(ev)

    tx_dates = sorted(events_by_date.keys())
    tx_cursor = 0
    rolling_cash = 0.0
    shares_by_date_ticker: dict[str, float] = defaultdict(float)

    tx_by_date: dict[date, list[Transaction]] = defaultdict(list)
    for tx in transactions:
        tx_by_date[tx.trade_date].append(tx)

    equity_curve: list[DailyEquityPoint] = []
    prev_total = None

    for d in valuation_dates:
        # Apply all transactions for this date.
        for tx in tx_by_date.get(d, []):
            if tx.tx_type == "Buy":
                shares_by_date_ticker[tx.ticker] += tx.shares
            elif tx.tx_type == "Sell":
                shares_by_date_ticker[tx.ticker] -= sell_executed_shares.get(tx.row_id, tx.shares)
                if shares_by_date_ticker[tx.ticker] < -1e-9:
                    raise PortfolioEngineError(
                        f"Negative inventory occurs on valuation timeline for {tx.ticker} at {d.isoformat()}."
                    )
            # Dividend has no share effect

        external_flow = 0.0
        while tx_cursor < len(tx_dates) and tx_dates[tx_cursor] <= d:
            tx_day = tx_dates[tx_cursor]
            if tx_day == d:
                external_flow = sum(e.amount for e in events_by_date[tx_day])
            rolling_cash += sum(e.amount for e in events_by_date[tx_day])
            tx_cursor += 1

        market_value = 0.0
        for ticker, shares in shares_by_date_ticker.items():
            if shares <= 1e-12:
                continue
            px = _price_on_or_before_or_fail(
                price_by_ticker,
                sorted_dates_by_ticker,
                ticker,
                d,
                context="daily valuation",
            )
            if px.price_date != d:
                key = (ticker, "daily valuation")
                stat = ffill_stats.get(key)
                if stat is None:
                    ffill_stats[key] = {"count": 1, "first": d, "last": d}
                else:
                    stat["count"] = int(stat["count"]) + 1
                    stat["last"] = d
            market_value += shares * px.close_price * px.fx_to_usd

        total_equity = rolling_cash + market_value
        if abs(total_equity - (rolling_cash + market_value)) > 1e-9:
            raise PortfolioEngineError(
                f"Daily reconciliation failed at {d.isoformat()}: cash + holdings != NAV."
            )

        daily_return = None
        if prev_total is not None:
            denom = prev_total + external_flow
            if abs(denom) > 1e-12:
                daily_return = (total_equity - prev_total - external_flow) / denom
        prev_total = total_equity

        equity_curve.append(
            DailyEquityPoint(
                as_of_date=d,
                cash_balance=rolling_cash,
                market_value=market_value,
                total_equity=total_equity,
                external_flow=external_flow,
                daily_return=daily_return,
            )
        )

    as_of = valuation_dates[-1]

    unrealized_events: list[UnrealizedEvent] = []
    for ticker, lots in open_lots.items():
        current_px = _price_on_or_before_or_fail(
            price_by_ticker,
            sorted_dates_by_ticker,
            ticker,
            as_of,
            context="final unrealized snapshot",
        )
        if current_px.price_date != as_of:
            key = (ticker, "final unrealized snapshot")
            stat = ffill_stats.get(key)
            if stat is None:
                ffill_stats[key] = {"count": 1, "first": as_of, "last": as_of}
            else:
                stat["count"] = int(stat["count"]) + 1
                stat["last"] = as_of
        for lot in lots:
            if lot.shares_remaining <= 1e-12:
                continue
            local_un = lot.shares_remaining * (current_px.close_price - lot.entry_price)
            fx_un = lot.shares_remaining * lot.entry_price * (current_px.fx_to_usd - lot.entry_fx_rate)
            unrealized_events.append(
                UnrealizedEvent(
                    ticker=ticker,
                    ticker_symbol=lot.ticker_symbol,
                    as_of_date=as_of,
                    lot_id=lot.lot_id,
                    shares_remaining=lot.shares_remaining,
                    entry_date=lot.entry_date,
                    entry_price=lot.entry_price,
                    current_price=current_px.close_price,
                    entry_fx=lot.entry_fx_rate,
                    current_fx=current_px.fx_to_usd,
                    local_unrealized=local_un,
                    fx_unrealized=fx_un,
                    total_unrealized=local_un + fx_un,
                )
            )

    total_realized = sum(e.total_realized for e in realized_events)
    total_unrealized = sum(e.total_unrealized for e in unrealized_events)
    total_local_realized = sum(e.local_gain for e in realized_events)
    total_fx_realized = sum(e.fx_gain for e in realized_events)
    total_local_unrealized = sum(e.local_unrealized for e in unrealized_events)
    total_fx_unrealized = sum(e.fx_unrealized for e in unrealized_events)

    ending_cash = equity_curve[-1].cash_balance
    ending_market = equity_curve[-1].market_value
    ending_total_equity = equity_curve[-1].total_equity

    # Accounting reconciliation (excluding dividends):
    # total_buy_cost + (realized + unrealized) == total_sell_proceeds + ending_market
    total_buy_cost = sum(tx.cost for tx in transactions if tx.tx_type == "Buy")
    total_sell_proceeds = sum(
        sell_executed_cost.get(tx.row_id, tx.cost)
        for tx in transactions
        if tx.tx_type == "Sell"
    )
    identity_lhs = total_buy_cost + total_realized + total_unrealized
    identity_rhs = total_sell_proceeds + ending_market
    if abs(identity_lhs - identity_rhs) > 1e-6:
        raise PortfolioEngineError(
            "Reconciliation error: buy_cost + realized + unrealized != sell_proceeds + ending_market. "
            f"lhs={identity_lhs:.6f}, rhs={identity_rhs:.6f}"
        )

    twr = 1.0
    daily_rets = [p.daily_return for p in equity_curve if p.daily_return is not None]
    for r in daily_rets:
        twr *= (1.0 + r)
    twr -= 1.0

    total_days = max(1, (equity_curve[-1].as_of_date - equity_curve[0].as_of_date).days)
    annualized_return = (1.0 + twr) ** (365.2425 / total_days) - 1.0 if twr > -1.0 else -1.0

    if daily_rets:
        running = 1.0
        peaks = [1.0]
        curve = []
        for r in daily_rets:
            running *= (1.0 + r)
            curve.append(running)
            peaks.append(max(peaks[-1], running))
        max_drawdown = min((c / p - 1.0) for c, p in zip(curve, peaks[1:]))
        volatility = pstdev(daily_rets) * math.sqrt(252.0) if len(daily_rets) > 1 else 0.0
    else:
        max_drawdown = 0.0
        volatility = 0.0

    gross_sells = sum(sell_executed_cost.get(tx.row_id, tx.cost) for tx in transactions if tx.tx_type == "Sell")
    avg_equity = sum(p.total_equity for p in equity_curve) / len(equity_curve)
    turnover = (gross_sells / avg_equity) if abs(avg_equity) > 1e-12 else 0.0

    portfolio_cashflows = [(tx.trade_date, -tx.cost) for tx in transactions if tx.tx_type == "Buy"]
    portfolio_cashflows += [
        (tx.trade_date, sell_executed_cost.get(tx.row_id, tx.cost))
        for tx in transactions
        if tx.tx_type == "Sell"
    ]
    portfolio_cashflows += [(tx.trade_date, tx.cost) for tx in transactions if tx.tx_type == "Dividend"]
    portfolio_cashflows.append((as_of, ending_total_equity))
    mwrr = _irr_from_cashflows(portfolio_cashflows)

    per_ticker_realized: dict[str, float] = defaultdict(float)
    per_ticker_unrealized: dict[str, float] = defaultdict(float)
    per_ticker_fx: dict[str, float] = defaultdict(float)
    per_ticker_local: dict[str, float] = defaultdict(float)
    for e in realized_events:
        per_ticker_realized[e.ticker] += e.total_realized
        per_ticker_fx[e.ticker] += e.fx_gain
        per_ticker_local[e.ticker] += e.local_gain
    for e in unrealized_events:
        per_ticker_unrealized[e.ticker] += e.total_unrealized
        per_ticker_fx[e.ticker] += e.fx_unrealized
        per_ticker_local[e.ticker] += e.local_unrealized

    ticker_cashflows: dict[str, list[tuple[date, float]]] = defaultdict(list)
    first_date: dict[str, date] = {}
    last_date: dict[str, date] = {}
    for tx in transactions:
        if tx.tx_type == "Buy":
            ticker_cashflows[tx.ticker].append((tx.trade_date, -tx.cost))
        elif tx.tx_type == "Sell":
            ticker_cashflows[tx.ticker].append((tx.trade_date, sell_executed_cost.get(tx.row_id, tx.cost)))
        elif tx.tx_type == "Dividend":
            ticker_cashflows[tx.ticker].append((tx.trade_date, tx.cost))
        first_date[tx.ticker] = min(first_date.get(tx.ticker, tx.trade_date), tx.trade_date)
        last_date[tx.ticker] = max(last_date.get(tx.ticker, tx.trade_date), tx.trade_date)

    ending_shares: dict[str, float] = defaultdict(float)
    for ticker, lots in open_lots.items():
        ending_shares[ticker] = sum(l.shares_remaining for l in lots)

    for ticker, shares in ending_shares.items():
        if shares > 1e-12:
            px = _price_on_or_before_or_fail(
                price_by_ticker,
                sorted_dates_by_ticker,
                ticker,
                as_of,
                context="ticker terminal valuation",
            )
            if px.price_date != as_of:
                key = (ticker, "ticker terminal valuation")
                stat = ffill_stats.get(key)
                if stat is None:
                    ffill_stats[key] = {"count": 1, "first": as_of, "last": as_of}
                else:
                    stat["count"] = int(stat["count"]) + 1
                    stat["last"] = as_of
            ticker_cashflows[ticker].append((as_of, shares * px.close_price * px.fx_to_usd))

    # Summarize forward-fill warnings to keep output actionable.
    context_stats: dict[str, dict[str, object]] = {}
    for (ticker, context), stat in ffill_stats.items():
        agg = context_stats.get(context)
        if agg is None:
            agg = {
                "tickers": set(),
                "total_dates": 0,
                "first": stat["first"],
                "last": stat["last"],
            }
            context_stats[context] = agg
        agg["tickers"].add(ticker)
        agg["total_dates"] = int(agg["total_dates"]) + int(stat["count"])
        if stat["first"] < agg["first"]:
            agg["first"] = stat["first"]
        if stat["last"] > agg["last"]:
            agg["last"] = stat["last"]

    for context in sorted(context_stats.keys()):
        agg = context_stats[context]
        tickers_sorted = sorted(agg["tickers"])
        sample = ", ".join(tickers_sorted[:8])
        if len(tickers_sorted) > 8:
            sample += ", ..."
        warnings.append(
            f"WARNING[PRIOR_CLOSE] {context}: {int(agg['total_dates'])} fallback date-point(s) "
            f"across {len(tickers_sorted)} ticker(s), range {agg['first'].isoformat()} to {agg['last'].isoformat()}. "
            f"Sample tickers: {sample}"
        )
    prior_close_fallback_rows = [
        {
            "ticker": ticker,
            "context": context,
            "fallback_days": int(stat["count"]),
            "first_missing_date": stat["first"].isoformat(),
            "last_missing_date": stat["last"].isoformat(),
        }
        for (ticker, context), stat in sorted(ffill_stats.items(), key=lambda x: (x[0][0], x[0][1]))
    ]

    # Outputs.
    lot_audit_rows: list[dict[str, object]] = []
    for e in realized_events:
        lot_audit_rows.append(
            {
                "ticker": e.ticker,
                "ticker_symbol": e.ticker_symbol,
                "sell_date": e.trade_date.isoformat(),
                "sell_row_id": e.sell_row_id,
                "lot_id": e.lot_id,
                "matched_shares": round(e.matched_shares, 10),
                "entry_date": e.entry_date.isoformat(),
                "entry_price": e.entry_price,
                "sell_price": e.sell_price,
                "entry_fx": e.entry_fx,
                "sell_fx": e.sell_fx,
                "local_realized": e.local_gain,
                "fx_realized": e.fx_gain,
                "total_realized": e.total_realized,
            }
        )
    for ue in unrealized_events:
        lot_audit_rows.append(
            {
                "ticker": ue.ticker,
                "ticker_symbol": ue.ticker_symbol,
                "sell_date": "OPEN",
                "sell_row_id": "",
                "lot_id": ue.lot_id,
                "matched_shares": round(ue.shares_remaining, 10),
                "entry_date": ue.entry_date.isoformat(),
                "entry_price": ue.entry_price,
                "sell_price": ue.current_price,
                "entry_fx": ue.entry_fx,
                "sell_fx": ue.current_fx,
                "local_realized": "",
                "fx_realized": "",
                "total_realized": "",
            }
        )

    realized_by_ticker_year: dict[tuple[str, int], float] = defaultdict(float)
    for e in realized_events:
        realized_by_ticker_year[(e.ticker, e.trade_date.year)] += e.total_realized
    realized_rows = [
        {
            "ticker": t,
            "year": y,
            "realized_pnl": pnl,
        }
        for (t, y), pnl in sorted(realized_by_ticker_year.items())
    ]

    unrealized_rows = [
        {
            "as_of_date": ue.as_of_date.isoformat(),
            "ticker": ue.ticker,
            "lot_id": ue.lot_id,
            "shares_remaining": round(ue.shares_remaining, 10),
            "entry_date": ue.entry_date.isoformat(),
            "entry_price": ue.entry_price,
            "current_price": ue.current_price,
            "local_unrealized": ue.local_unrealized,
            "fx_unrealized": ue.fx_unrealized,
            "total_unrealized": ue.total_unrealized,
        }
        for ue in unrealized_events
    ]

    equity_rows = [
        {
            "date": p.as_of_date.isoformat(),
            "cash": p.cash_balance,
            "market_value": p.market_value,
            "total_equity": p.total_equity,
            "external_flow": p.external_flow,
            "daily_return": p.daily_return,
        }
        for p in equity_curve
    ]

    fx_rows = [
        {
            "ticker": t,
            "local_pnl": per_ticker_local[t],
            "fx_pnl": per_ticker_fx[t],
            "total_pnl": per_ticker_local[t] + per_ticker_fx[t],
        }
        for t in sorted(per_ticker_local.keys() | per_ticker_fx.keys())
    ]

    irr_rows = []
    for t in sorted(tx_tickers):
        irr_rows.append(
            {
                "scope": "ticker",
                "ticker": t,
                "irr": _irr_from_cashflows(ticker_cashflows[t]),
                "realized_pnl": per_ticker_realized[t],
                "unrealized_pnl": per_ticker_unrealized[t],
                "total_return": per_ticker_realized[t] + per_ticker_unrealized[t],
                "holding_period_days": (as_of - first_date[t]).days if t in first_date else 0,
            }
        )

    irr_rows.append(
        {
            "scope": "portfolio",
            "ticker": "ALL",
            "irr": mwrr,
            "realized_pnl": total_realized,
            "unrealized_pnl": total_unrealized,
            "total_return": total_realized + total_unrealized,
            "holding_period_days": total_days,
        }
    )

    summary_rows = [
        {"metric": "base_currency", "value": BASE_CURRENCY},
        {"metric": "as_of_date", "value": as_of.isoformat()},
        {"metric": "cash_balance", "value": ending_cash},
        {"metric": "market_value", "value": ending_market},
        {"metric": "total_equity", "value": ending_total_equity},
        {"metric": "total_realized_pnl", "value": total_realized},
        {"metric": "total_unrealized_pnl", "value": total_unrealized},
        {"metric": "total_return", "value": total_realized + total_unrealized},
        {"metric": "time_weighted_return", "value": twr},
        {"metric": "money_weighted_return_irr", "value": mwrr},
        {"metric": "annualized_return", "value": annualized_return},
        {"metric": "max_drawdown", "value": max_drawdown},
        {"metric": "volatility", "value": volatility},
        {"metric": "turnover", "value": turnover},
        {"metric": "local_realized", "value": total_local_realized},
        {"metric": "fx_realized", "value": total_fx_realized},
        {"metric": "local_unrealized", "value": total_local_unrealized},
        {"metric": "fx_unrealized", "value": total_fx_unrealized},
    ]

    return EngineOutputs(
        lot_audit=lot_audit_rows,
        realized_report=realized_rows,
        unrealized_snapshot=unrealized_rows,
        daily_equity_curve=equity_rows,
        fx_attribution=fx_rows,
        irr_summary=irr_rows,
        portfolio_summary=summary_rows,
        warnings=warnings,
        correction_events=correction_events,
        prior_close_fallback=prior_close_fallback_rows,
    )


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            f.write("")
        return
    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def export_outputs(outputs: EngineOutputs, output_dir: Path = OUTPUT_DIR) -> list[Path]:
    files = {
        "lot_audit.csv": outputs.lot_audit,
        "realized_pnl_by_ticker_year.csv": outputs.realized_report,
        "unrealized_snapshot.csv": outputs.unrealized_snapshot,
        "daily_equity_curve.csv": outputs.daily_equity_curve,
        "fx_attribution_report.csv": outputs.fx_attribution,
        "irr_summary.csv": outputs.irr_summary,
        "portfolio_summary.csv": outputs.portfolio_summary,
        "correction_events.csv": outputs.correction_events,
        "prior_close_fallback.csv": outputs.prior_close_fallback,
    }
    written: list[Path] = []
    for name, rows in files.items():
        p = output_dir / name
        _write_csv(p, rows)
        written.append(p)
    return written
