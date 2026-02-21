"""
Yahoo Finance API client — yfinance backend.

Replaces the fc.yahoo.com session bootstrap (blocked by Yahoo anti-bot).
Uses the yfinance library which handles cookie/crumb internally and is
actively maintained to track Yahoo's authentication changes.

Public API (unchanged, callers don't need to change):
  fetch_5y_prices(ticker, client)      → Yahoo chart dict (timestamp/indicators format)
  fetch_recent_prices(ticker, client)  → Yahoo chart dict (timestamp/indicators format)
  fetch_quote_summary(ticker, client)  → quoteSummary result dict

The `client` parameter is accepted for interface compatibility but ignored;
yfinance manages its own HTTP session.

Output formats are identical to what the old signed_fetch produced so that
yahoo_normalizer.py requires no changes.
"""

import logging
import math
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _num(v: Any) -> float | None:
    """Return float if v is a valid finite number, else None."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _ts(dt_index_val: Any) -> int | None:
    """Convert a pandas Timestamp (possibly tz-aware) to Unix seconds int."""
    try:
        if hasattr(dt_index_val, "timestamp"):
            return int(dt_index_val.timestamp())
        return int(dt_index_val)
    except Exception:
        return None


def _raw(v: Any) -> dict | None:
    """Wrap a numeric value in Yahoo's {raw: ...} format."""
    n = _num(v)
    return {"raw": n} if n is not None else None


def _date_raw(dt: Any) -> dict | None:
    """Wrap a pandas Timestamp in Yahoo's {raw: unix_ts} format."""
    try:
        if hasattr(dt, "timestamp"):
            return {"raw": int(dt.timestamp())}
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Price history → Yahoo chart format
#
# normalize_prices() in yahoo_normalizer.py expects:
#   result["timestamp"]                              → list[int]
#   result["indicators"]["quote"][0]["open"]         → list[float|None]
#   result["indicators"]["quote"][0]["high"]         → list[float|None]
#   result["indicators"]["quote"][0]["low"]          → list[float|None]
#   result["indicators"]["quote"][0]["close"]        → list[float|None]
#   result["indicators"]["quote"][0]["volume"]       → list[float|None]
#   result["indicators"]["adjclose"][0]["adjclose"]  → list[float|None]
# ---------------------------------------------------------------------------

def _history_to_chart_result(ticker: str, hist: Any) -> dict[str, Any]:
    """
    Convert a yfinance history DataFrame (auto_adjust=False) to the Yahoo
    chart API result[0] dict structure that yahoo_normalizer.normalize_prices
    consumes.
    """
    if hist is None or hist.empty:
        raise RuntimeError(f"No price history returned for {ticker}")

    # Rename 'Adj Close' to avoid attribute access issues with spaces
    if "Adj Close" in hist.columns:
        hist = hist.rename(columns={"Adj Close": "Adj_Close"})

    timestamps: list[int] = []
    opens: list[float | None] = []
    highs: list[float | None] = []
    lows: list[float | None] = []
    closes: list[float | None] = []
    volumes: list[float | None] = []
    adj_closes: list[float | None] = []

    for dt, row in zip(hist.index, hist.itertuples()):
        ts = _ts(dt)
        if ts is None:
            continue
        timestamps.append(ts)
        opens.append(_num(getattr(row, "Open", None)))
        highs.append(_num(getattr(row, "High", None)))
        lows.append(_num(getattr(row, "Low", None)))
        closes.append(_num(getattr(row, "Close", None)))
        volumes.append(_num(getattr(row, "Volume", None)))
        adj_closes.append(_num(getattr(row, "Adj_Close", None)))

    if not timestamps:
        raise RuntimeError(f"No valid price rows for {ticker}")

    return {
        "timestamp": timestamps,
        "indicators": {
            "quote": [{
                "open": opens,
                "high": highs,
                "low": lows,
                "close": closes,
                "volume": volumes,
            }],
            "adjclose": [{"adjclose": adj_closes}],
        },
    }


async def fetch_5y_prices(ticker: str, client: Any) -> dict[str, Any]:
    """Fetch 5-year daily price history via yfinance. Returns chart result dict."""
    import yfinance as yf

    logger.info("[yfinance] Fetching 5Y prices for %s", ticker)
    t = yf.Ticker(ticker)
    hist = t.history(period="5y", auto_adjust=False, actions=False)
    result = _history_to_chart_result(ticker, hist)
    logger.info("[yfinance] Got %d price points for %s (5Y)", len(result["timestamp"]), ticker)
    return result


async def fetch_recent_prices(ticker: str, client: Any) -> dict[str, Any]:
    """Fetch 1-month recent daily price history via yfinance. Returns chart result dict."""
    import yfinance as yf

    logger.info("[yfinance] Fetching 1mo prices for %s", ticker)
    t = yf.Ticker(ticker)
    hist = t.history(period="1mo", auto_adjust=False, actions=False)
    result = _history_to_chart_result(ticker, hist)
    logger.info("[yfinance] Got %d price points for %s (1mo)", len(result["timestamp"]), ticker)
    return result


# ---------------------------------------------------------------------------
# quoteSummary → format expected by yahoo_normalizer
#
# build_yahoo_metrics_payload() reads:
#   source_q["price"]["regularMarketPrice"]["raw"]
#   source_q["price"]["marketCap"]["raw"]
#   source_q["summaryDetail"]["forwardPE"]["raw"]
#   source_q["summaryDetail"]["trailingPE"]["raw"]
#   source_q["defaultKeyStatistics"]["beta"]["raw"]
#   source_q["defaultKeyStatistics"]["enterpriseValue"]["raw"]
#   source_q["defaultKeyStatistics"]["enterpriseToEbitda"]["raw"]
#   source_q["defaultKeyStatistics"]["sharesOutstanding"]["raw"]
#
# normalize_quarterly/annual_financials() read statement lists where each
# period dict has: {"endDate": {"raw": unix_ts}, "fieldName": {"raw": value}}
# ---------------------------------------------------------------------------

def _income_to_yahoo(df: Any) -> list[dict]:
    """Map yfinance income statement rows to Yahoo quoteSummary field names."""
    if df is None or df.empty:
        return []

    INCOME_MAP = {
        "Total Revenue": "totalRevenue",
        "Net Income": "netIncome",
        "EBIT": "ebit",
        "Interest Expense": "interestExpense",
        "Diluted Average Shares": "dilutedAverageShares",
        "Diluted EPS": "dilutedEps",
        "Stock Based Compensation": "stockBasedCompensation",
        "Research And Development": "researchDevelopment",
    }

    records = []
    for col in df.columns:
        period_ts = _date_raw(col)
        if period_ts is None:
            continue
        record: dict[str, Any] = {"endDate": period_ts}
        for yf_name, yahoo_name in INCOME_MAP.items():
            if yf_name in df.index:
                wrapped = _raw(df.loc[yf_name, col])
                if wrapped is not None:
                    record[yahoo_name] = wrapped
        records.append(record)
    return records


def _cashflow_to_yahoo(df: Any) -> list[dict]:
    """Map yfinance cashflow statement rows to Yahoo quoteSummary field names."""
    if df is None or df.empty:
        return []

    # yfinance → Yahoo field name mapping (first match wins)
    CF_MAP = [
        ("Operating Cash Flow", "totalCashFromOperatingActivities"),
        ("Capital Expenditure", "capitalExpenditures"),
        ("Depreciation And Amortization", "depreciation"),
        ("Depreciation Amortization Depletion", "depreciation"),
        ("Stock Based Compensation", "stockBasedCompensation"),
    ]

    records = []
    for col in df.columns:
        period_ts = _date_raw(col)
        if period_ts is None:
            continue
        record: dict[str, Any] = {"endDate": period_ts}
        for yf_name, yahoo_name in CF_MAP:
            if yf_name in df.index and yahoo_name not in record:
                wrapped = _raw(df.loc[yf_name, col])
                if wrapped is not None:
                    record[yahoo_name] = wrapped
        records.append(record)
    return records


def _balance_to_yahoo(df: Any) -> list[dict]:
    """Map yfinance balance sheet rows to Yahoo quoteSummary field names."""
    if df is None or df.empty:
        return []

    # yfinance → Yahoo field name mapping (first match wins)
    BS_MAP = [
        ("Cash And Cash Equivalents", "cash"),
        ("Cash Cash Equivalents And Short Term Investments", "cash"),
        ("Long Term Debt", "longTermDebt"),
        ("Current Debt", "shortLongTermDebt"),
        ("Short Long Term Debt", "shortLongTermDebt"),
        ("Stockholders Equity", "totalStockholderEquity"),
        ("Total Equity Gross Minority Interest", "totalStockholderEquity"),
        ("Total Assets", "totalAssets"),
    ]

    records = []
    for col in df.columns:
        period_ts = _date_raw(col)
        if period_ts is None:
            continue
        record: dict[str, Any] = {"endDate": period_ts}
        for yf_name, yahoo_name in BS_MAP:
            if yf_name in df.index and yahoo_name not in record:
                wrapped = _raw(df.loc[yf_name, col])
                if wrapped is not None:
                    record[yahoo_name] = wrapped
        records.append(record)
    return records


async def fetch_quote_summary(ticker: str, client: Any) -> dict[str, Any]:
    """
    Fetch fundamental data via yfinance and return a quoteSummary-like dict
    that yahoo_normalizer.py can consume without modification.
    """
    import yfinance as yf

    logger.info("[yfinance] Fetching fundamentals for %s", ticker)
    t = yf.Ticker(ticker)
    info = t.info or {}

    # --- Quarterly financial statements ---
    try:
        q_inc = t.quarterly_income_stmt
    except Exception:
        q_inc = None
    try:
        q_cf = t.quarterly_cashflow
    except Exception:
        q_cf = None
    try:
        q_bs = t.quarterly_balance_sheet
    except Exception:
        q_bs = None

    # --- Annual financial statements ---
    try:
        a_inc = t.income_stmt
    except Exception:
        a_inc = None
    try:
        a_cf = t.cashflow
    except Exception:
        a_cf = None
    try:
        a_bs = t.balance_sheet
    except Exception:
        a_bs = None

    q_income_list = _income_to_yahoo(q_inc)
    q_cashflow_list = _cashflow_to_yahoo(q_cf)
    q_balance_list = _balance_to_yahoo(q_bs)

    a_income_list = _income_to_yahoo(a_inc)
    a_cashflow_list = _cashflow_to_yahoo(a_cf)
    a_balance_list = _balance_to_yahoo(a_bs)

    logger.info(
        "[yfinance] %s: q_inc=%d q_cf=%d q_bs=%d | a_inc=%d a_cf=%d a_bs=%d",
        ticker,
        len(q_income_list), len(q_cashflow_list), len(q_balance_list),
        len(a_income_list), len(a_cashflow_list), len(a_balance_list),
    )

    return {
        "price": {
            "regularMarketPrice": _raw(info.get("regularMarketPrice") or info.get("currentPrice")),
            "marketCap": _raw(info.get("marketCap")),
            "regularMarketPreviousClose": _raw(info.get("regularMarketPreviousClose")),
        },
        "summaryDetail": {
            "forwardPE": _raw(info.get("forwardPE")),
            "trailingPE": _raw(info.get("trailingPE")),
            "regularMarketPreviousClose": _raw(info.get("regularMarketPreviousClose")),
        },
        "defaultKeyStatistics": {
            "beta": _raw(info.get("beta")),
            "enterpriseValue": _raw(info.get("enterpriseValue")),
            "enterpriseToEbitda": _raw(info.get("enterpriseToEbitda")),
            "sharesOutstanding": _raw(info.get("sharesOutstanding")),
            "trailingPE": _raw(info.get("trailingPE")),
        },
        "financialData": {
            "enterpriseValue": _raw(info.get("enterpriseValue")),
            "enterpriseToEbitda": _raw(info.get("enterpriseToEbitda")),
        },
        "incomeStatementHistoryQuarterly": {
            "incomeStatementHistory": q_income_list,
        },
        "cashflowStatementHistoryQuarterly": {
            "cashflowStatements": q_cashflow_list,
        },
        "balanceSheetHistoryQuarterly": {
            "balanceSheetStatements": q_balance_list,
        },
        "incomeStatementHistory": {
            "incomeStatementHistory": a_income_list,
        },
        "cashflowStatementHistory": {
            "cashflowStatements": a_cashflow_list,
        },
        "balanceSheetHistory": {
            "balanceSheetStatements": a_balance_list,
        },
    }
