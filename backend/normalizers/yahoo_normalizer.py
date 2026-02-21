"""
Yahoo Finance data normalizers.

Mirrors normalizePrices() from syncHistoricalPricesYahoo.ts / runYahooEtlPipeline.ts
(Extract5 / Extract2) and the quoteSummary field mapping from runYahooFundamentalsEtl.ts.
"""

import logging
import math
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _num_or_null(v: Any) -> float | None:
    """Return float if v is a valid finite number, else None."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _to_ymd(ts_seconds: int | float) -> str | None:
    """Convert Unix timestamp (seconds) to YYYY-MM-DD string."""
    try:
        return datetime.utcfromtimestamp(ts_seconds).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError):
        return None


def _today() -> str:
    return date.today().isoformat()


def _num_raw(n: Any) -> float | None:
    """
    Extract numeric value supporting Yahoo's {raw: ..., fmt: ...} wrapper.
    Mirrors the num() helper in runYahooFundamentalsEtl.ts.
    """
    if isinstance(n, dict):
        raw = n.get("raw")
        if isinstance(raw, (int, float)) and math.isfinite(raw):
            return float(raw)
        if isinstance(raw, str) and raw.strip():
            try:
                return float(raw)
            except ValueError:
                pass
    if isinstance(n, (int, float)) and math.isfinite(n):
        return float(n)
    if isinstance(n, str) and n.strip():
        try:
            return float(n)
        except ValueError:
            pass
    return None


def _pick_num(*candidates: Any) -> float | None:
    """Return first finite numeric value from candidates."""
    for c in candidates:
        v = _num_raw(c)
        if v is not None:
            return v
    return None


def _to_iso_date(d: Any) -> str | None:
    """
    Convert Yahoo date field (may be {raw: unix_ts}, unix int, or ISO string)
    to YYYY-MM-DD.
    """
    if d is None:
        return None
    if isinstance(d, dict):
        raw = d.get("raw")
        if isinstance(raw, (int, float)):
            d = raw
        else:
            return None
    if isinstance(d, (int, float)):
        try:
            return datetime.utcfromtimestamp(d).strftime("%Y-%m-%d")
        except (OSError, OverflowError):
            return None
    if isinstance(d, str):
        return d[:10]
    return None


# ---------------------------------------------------------------------------
# normalizePrices
# Mirrors normalizePrices() in Extract5 / Extract2 exactly.
# ---------------------------------------------------------------------------

def normalize_prices(ticker: str, price_data: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Normalize Yahoo chart API result[0] into a list of PricesHistory dicts.

    Field mapping (exact from Extract5):
      close     = quotes.close[i]      — split-adjusted only (Yahoo "Close")
      close_adj = adjclose[i]          — dividend & split-adjusted (Yahoo "Adj Close")

    Filter: require both close != None AND close_adj != None AND date != None.
    """
    timestamps = price_data.get("timestamp")
    if not isinstance(timestamps, list) or len(timestamps) == 0:
        return []

    logger.debug("[Yahoo][Normalize] processing %d price timestamps for %s",
                 len(timestamps), ticker)

    indicators = price_data.get("indicators", {})
    quote_list = indicators.get("quote", [{}])
    quotes = quote_list[0] if quote_list else {}

    adjclose_list = indicators.get("adjclose", [{}])
    adj_closes = adjclose_list[0].get("adjclose", []) if adjclose_list else []

    as_of = _today()
    normalized: list[dict[str, Any]] = []

    for i, ts in enumerate(timestamps):
        d = _to_ymd(ts)
        close = _num_or_null(quotes.get("close", [None])[i] if i < len(quotes.get("close", [])) else None)
        close_adj = _num_or_null(adj_closes[i] if i < len(adj_closes) else None)

        # Require both close and close_adj to be non-null
        if d is None or close is None or close_adj is None:
            continue

        normalized.append({
            "ticker": ticker,
            "date": d,
            "open": _num_or_null(quotes.get("open", [None])[i] if i < len(quotes.get("open", [])) else None),
            "high": _num_or_null(quotes.get("high", [None])[i] if i < len(quotes.get("high", [])) else None),
            "low": _num_or_null(quotes.get("low", [None])[i] if i < len(quotes.get("low", [])) else None),
            "close": close,
            "close_adj": close_adj,
            "volume": _num_or_null(quotes.get("volume", [None])[i] if i < len(quotes.get("volume", [])) else None),
            "source": "yahoo",
            "as_of_date": as_of,
        })

    logger.debug("[Yahoo][Normalize] %d valid price points for %s", len(normalized), ticker)
    return normalized


# ---------------------------------------------------------------------------
# normalize_quote_summary
# Mirrors normalizeSourceQ + field extraction in runYahooFundamentalsEtl.ts
# ---------------------------------------------------------------------------

def _as_array(v: Any) -> list:
    return v if isinstance(v, list) else []


def normalize_quarterly_financials(
    ticker: str, source_q: dict[str, Any]
) -> list[dict[str, Any]]:
    """
    Extract quarterly FinancialsHistory records from Yahoo quoteSummary result.
    Merges incomeStatementHistoryQuarterly + cashflowStatementHistoryQuarterly
    + balanceSheetHistoryQuarterly on endDate.
    Mirrors the quarterly processing loop in runYahooFundamentalsEtl.ts.
    """
    inc_stmts = _as_array(
        source_q.get("incomeStatementHistoryQuarterly", {}).get("incomeStatementHistory")
    )
    cfs_stmts = _as_array(
        source_q.get("cashflowStatementHistoryQuarterly", {}).get("cashflowStatements")
    )
    bss_stmts = _as_array(
        source_q.get("balanceSheetHistoryQuarterly", {}).get("balanceSheetStatements")
    )

    all_dates: list[str] = sorted(
        {
            d
            for d in [
                _to_iso_date(s.get("endDate")) for s in inc_stmts + cfs_stmts + bss_stmts
            ]
            if d is not None
        },
        reverse=True,
    )

    logger.debug("[Yahoo][QuartNorm] %d unique quarterly dates for %s",
                 len(all_dates), ticker)

    as_of = _today()
    records: list[dict[str, Any]] = []

    for d in all_dates:
        inc = next((s for s in inc_stmts if _to_iso_date(s.get("endDate")) == d), {})
        cfs = next((s for s in cfs_stmts if _to_iso_date(s.get("endDate")) == d), {})
        bss = next((s for s in bss_stmts if _to_iso_date(s.get("endDate")) == d), {})

        # interest_expense: take abs value, with fallback to interestPaid
        interest_exp_raw = _num_raw(inc.get("interestExpense")) if inc.get("interestExpense") is not None \
            else _num_raw(cfs.get("interestPaid"))
        interest_exp = abs(interest_exp_raw) if interest_exp_raw is not None else None

        cfo_v = _num_raw(cfs.get("totalCashFromOperatingActivities"))
        capex_v = _num_raw(cfs.get("capitalExpenditures"))
        fcf_v = (cfo_v - abs(capex_v)) if (
            cfo_v is not None and capex_v is not None
        ) else None

        short_debt = _num_raw(bss.get("shortLongTermDebt"))
        long_debt = _num_raw(bss.get("longTermDebt"))
        if short_debt is not None or long_debt is not None:
            total_debt = (short_debt or 0.0) + (long_debt or 0.0)
        else:
            total_debt = None

        records.append({
            "ticker": ticker,
            "period_end": d,
            "freq": "quarterly",
            "source": "yahoo",
            "as_of_date": as_of,
            # Income Statement
            "revenue": _num_raw(inc.get("totalRevenue")),
            "net_income": _num_raw(inc.get("netIncome")),
            "ebit": _num_raw(inc.get("ebit")),
            "interest_expense": interest_exp,
            "depreciation": _num_raw(cfs.get("depreciation")),
            "stock_based_compensation": (
                _num_raw(inc.get("stockBasedCompensation"))
                or _num_raw(cfs.get("stockBasedCompensation"))
            ),
            "shares_diluted": _num_raw(inc.get("dilutedAverageShares")),
            "eps_diluted": _num_raw(inc.get("dilutedEps")),
            # Cash Flow
            "cfo": cfo_v,
            "capex": capex_v,
            "fcf": fcf_v,
            # Balance Sheet
            "cash": _num_raw(bss.get("cash")),
            "long_term_debt": long_debt,
            "total_debt": total_debt,
            "stockholder_equity": _num_raw(bss.get("totalStockholderEquity")),
            "total_assets": _num_raw(bss.get("totalAssets")),
        })

    return records


def normalize_annual_financials(
    ticker: str, source_q: dict[str, Any]
) -> list[dict[str, Any]]:
    """
    Extract annual FinancialsHistory records from Yahoo quoteSummary result.
    Mirrors the annual processing loop in runYahooFundamentalsEtl.ts.
    """
    inc_stmts = _as_array(
        source_q.get("incomeStatementHistory", {}).get("incomeStatementHistory")
    )
    cfs_stmts = _as_array(
        source_q.get("cashflowStatementHistory", {}).get("cashflowStatements")
    )
    bss_stmts = _as_array(
        source_q.get("balanceSheetHistory", {}).get("balanceSheetStatements")
    )

    all_dates: list[str] = sorted(
        {
            d
            for d in [
                _to_iso_date(s.get("endDate")) for s in inc_stmts + cfs_stmts + bss_stmts
            ]
            if d is not None
        },
        reverse=True,
    )

    as_of = _today()
    records: list[dict[str, Any]] = []

    for d in all_dates:
        inc = next((s for s in inc_stmts if _to_iso_date(s.get("endDate")) == d), {})
        cfs = next((s for s in cfs_stmts if _to_iso_date(s.get("endDate")) == d), {})
        bss = next((s for s in bss_stmts if _to_iso_date(s.get("endDate")) == d), {})

        interest_exp_raw = _num_raw(inc.get("interestExpense")) if inc.get("interestExpense") is not None \
            else _num_raw(cfs.get("interestPaid"))
        interest_exp = abs(interest_exp_raw) if interest_exp_raw is not None else None

        cfo_v = _num_raw(cfs.get("totalCashFromOperatingActivities"))
        capex_v = _num_raw(cfs.get("capitalExpenditures"))
        fcf_v = (cfo_v - abs(capex_v)) if (
            cfo_v is not None and capex_v is not None
        ) else None

        short_debt = _num_raw(bss.get("shortLongTermDebt"))
        long_debt = _num_raw(bss.get("longTermDebt"))
        if short_debt is not None or long_debt is not None:
            total_debt = (short_debt or 0.0) + (long_debt or 0.0)
        else:
            total_debt = None

        records.append({
            "ticker": ticker,
            "period_end": d,
            "freq": "annual",
            "source": "yahoo",
            "as_of_date": as_of,
            "revenue": _num_raw(inc.get("totalRevenue")),
            "net_income": _num_raw(inc.get("netIncome")),
            "ebit": _num_raw(inc.get("ebit")),
            "interest_expense": interest_exp,
            "depreciation": _num_raw(cfs.get("depreciation")),
            "stock_based_compensation": (
                _num_raw(inc.get("stockBasedCompensation"))
                or _num_raw(cfs.get("stockBasedCompensation"))
            ),
            "shares_diluted": _num_raw(inc.get("dilutedAverageShares")),
            "eps_diluted": _num_raw(inc.get("dilutedEps")),
            "cfo": cfo_v,
            "capex": capex_v,
            "fcf": fcf_v,
            "cash": _num_raw(bss.get("cash")),
            "long_term_debt": long_debt,
            "total_debt": total_debt,
            "stockholder_equity": _num_raw(bss.get("totalStockholderEquity")),
            "total_assets": _num_raw(bss.get("totalAssets")),
        })

    return records


def build_yahoo_metrics_payload(
    ticker: str, source_q: dict[str, Any], merged_quarters: list[dict[str, Any]]
) -> dict[str, Any]:
    """
    Build Metrics upsert payload from Yahoo quoteSummary + merged quarterly data.
    Mirrors buildTTMFromMerged in runYahooFundamentalsEtl.ts.

    FCF = cfo_ttm - abs(capex_ttm)   (not cfo + capex)
    EV/EBITDA: prefer enterprise_value/ebitda_ttm, fallback to enterpriseToEbitda.
    """
    as_of = _today()

    def _sum4(key: str) -> float | None:
        vals = [q.get(key) for q in merged_quarters[:4]]
        nums = [v for v in vals if isinstance(v, (int, float)) and math.isfinite(v)]
        return sum(nums) if len(nums) == 4 else None

    revenue_ttm = _sum4("revenue")
    cfo_ttm = _sum4("cfo")
    capex_ttm = _sum4("capex")
    sbc_ttm = _sum4("sbc") or _sum4("stock_based_compensation")
    ebit_ttm = _sum4("ebit")
    depreciation_ttm = _sum4("depreciation")
    net_income_ttm = _sum4("net_income")
    interest_expense_ttm = _sum4("interest_expense")

    ebitda_ttm = (
        (ebit_ttm + depreciation_ttm)
        if ebit_ttm is not None and depreciation_ttm is not None
        else None
    )
    fcf_ttm = (
        (cfo_ttm - abs(capex_ttm))
        if cfo_ttm is not None and capex_ttm is not None
        else None
    )

    latest = merged_quarters[0] if merged_quarters else {}

    short_d = latest.get("short_debt")
    long_d = latest.get("long_debt")
    total_debt = (
        ((short_d or 0.0) + (long_d or 0.0))
        if (short_d is not None or long_d is not None)
        else None
    )

    price_current = _pick_num(
        source_q.get("price", {}).get("regularMarketPrice"),
        source_q.get("summaryDetail", {}).get("regularMarketPreviousClose"),
    )
    market_cap = _num_raw(source_q.get("price", {}).get("marketCap"))
    shares_out = _num_raw(source_q.get("defaultKeyStatistics", {}).get("sharesOutstanding"))
    pe_fwd = _num_raw(source_q.get("summaryDetail", {}).get("forwardPE"))
    pe_ttm = _num_raw(source_q.get("summaryDetail", {}).get("trailingPE"))
    beta_5y = _num_raw(source_q.get("defaultKeyStatistics", {}).get("beta"))

    enterprise_value = _pick_num(
        source_q.get("defaultKeyStatistics", {}).get("enterpriseValue"),
        source_q.get("financialData", {}).get("enterpriseValue"),
    )
    enterprise_to_ebitda = _pick_num(
        source_q.get("defaultKeyStatistics", {}).get("enterpriseToEbitda"),
        source_q.get("financialData", {}).get("enterpriseToEbitda"),
    )
    current_pe = _pick_num(
        source_q.get("summaryDetail", {}).get("trailingPE"),
        source_q.get("defaultKeyStatistics", {}).get("trailingPE"),
    )

    # EV/EBITDA: compute from EV and EBITDA_TTM first, fallback to direct field
    ev_ebitda: float | None = None
    if enterprise_value is not None and ebitda_ttm and ebitda_ttm != 0:
        ratio = enterprise_value / ebitda_ttm
        if math.isfinite(ratio):
            ev_ebitda = ratio
    elif enterprise_to_ebitda is not None:
        ev_ebitda = enterprise_to_ebitda

    payload: dict[str, Any] = {
        "ticker_symbol": ticker,
        "as_of_date": as_of,
        "data_source": "yahoo:multi_source_v1",
        "price_current": price_current,
        "revenue_ttm": revenue_ttm,
        "cfo_ttm": cfo_ttm,
        "capex_ttm": capex_ttm,
        "sbc_ttm": sbc_ttm,
        "fcf_ttm": fcf_ttm,
        "ebit_ttm": ebit_ttm,
        "depreciation_ttm": depreciation_ttm,
        "ebitda_ttm": ebitda_ttm,
        "net_income_ttm": net_income_ttm,
        "interest_expense_ttm": interest_expense_ttm,
        "cash": latest.get("cash"),
        "total_debt": total_debt,
        "equity": latest.get("equity") or latest.get("stockholder_equity"),
        "total_assets": latest.get("total_assets"),
        "market_cap": market_cap,
        "shares_out": shares_out,
        "pe_fwd": pe_fwd,
        "pe_ttm": pe_ttm,
        "beta_5y": beta_5y,
        "ev_ebitda": ev_ebitda,
        "current_pe": current_pe,
    }
    return payload
