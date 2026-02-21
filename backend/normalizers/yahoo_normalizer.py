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


def _safe_div(a: Any, b: Any) -> float | None:
    """Return a/b, or None if either is None/zero/non-finite."""
    try:
        fa, fb = float(a), float(b)
        if not (math.isfinite(fa) and math.isfinite(fb)) or fb == 0:
            return None
        result = fa / fb
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _cagr(end: Any, start: Any, years: float) -> float | None:
    """Compute annualised CAGR = (end/start)^(1/years) - 1, in %."""
    try:
        fe, fs = float(end), float(start)
        if not (math.isfinite(fe) and math.isfinite(fs)) or fs <= 0 or fe <= 0 or years <= 0:
            return None
        result = (fe / fs) ** (1.0 / years) - 1.0
        return result * 100 if math.isfinite(result) else None
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def build_yahoo_metrics_payload(
    ticker: str, source_q: dict[str, Any], merged_quarters: list[dict[str, Any]]
) -> dict[str, Any]:
    """
    Build Metrics upsert payload from Yahoo quoteSummary + merged quarterly data.
    Mirrors buildTTMFromMerged in runYahooFundamentalsEtl.ts.

    FCF = cfo_ttm - abs(capex_ttm)   (not cfo + capex)
    EV/EBITDA: prefer enterprise_value/ebitda_ttm, fallback to enterpriseToEbitda.
    Also computes derived metrics (ROIC, FCF Margin, CFO/NI, FCF/EBIT,
    interest coverage, buyback yield, SBC/Sales, share-count CAGR,
    EPS CAGR 3Y/5Y, Revenue CAGR 3Y/5Y, FCF Yield, insider ownership, PEG)
    using data from the yf_info dict and annual financial statements.
    """
    as_of = _today()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _sum4(key: str) -> float | None:
        vals = [q.get(key) for q in merged_quarters[:4]]
        nums = [v for v in vals if isinstance(v, (int, float)) and math.isfinite(v)]
        return sum(nums) if len(nums) == 4 else None

    # ------------------------------------------------------------------
    # TTM flow aggregates (from merged quarterly financials)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Balance sheet (latest quarter)
    # ------------------------------------------------------------------
    latest = merged_quarters[0] if merged_quarters else {}

    short_d = latest.get("short_debt")
    long_d = latest.get("long_debt")
    total_debt = (
        ((short_d or 0.0) + (long_d or 0.0))
        if (short_d is not None or long_d is not None)
        else None
    )
    equity = latest.get("equity") or latest.get("stockholder_equity")
    cash = latest.get("cash")
    total_assets = latest.get("total_assets")

    # ------------------------------------------------------------------
    # Quoted fields from source_q (yfinance-backed quoteSummary dict)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # EV/EBITDA: compute from EV and EBITDA_TTM first, fallback to direct field
    # ------------------------------------------------------------------
    ev_ebitda: float | None = None
    if enterprise_value is not None and ebitda_ttm and ebitda_ttm != 0:
        ratio = enterprise_value / ebitda_ttm
        if math.isfinite(ratio):
            ev_ebitda = ratio
    elif enterprise_to_ebitda is not None:
        ev_ebitda = enterprise_to_ebitda

    # ------------------------------------------------------------------
    # Extra yfinance info fields (populated in yahoo_client.py)
    # ------------------------------------------------------------------
    yf = source_q.get("yf_info", {})

    def _yf(key: str) -> float | None:
        v = yf.get(key)
        if v is None:
            return None
        try:
            f = float(v)
            return f if math.isfinite(f) else None
        except (TypeError, ValueError):
            return None

    # Prefer TTM values from merged quarters; fall back to yfinance aggregates
    _rev = revenue_ttm or _yf("totalRevenue")
    _cfo = cfo_ttm or _yf("operatingCashflow")
    _ni  = net_income_ttm or _yf("netIncomeToCommon")
    _fcf = fcf_ttm or _yf("freeCashflow")
    _ebitda = ebitda_ttm or _yf("ebitda")
    _cash = cash or _yf("totalCash")
    _debt = total_debt or _yf("totalDebt")

    # ------------------------------------------------------------------
    # Derived quality / balance metrics
    # ------------------------------------------------------------------

    # ROIC = NOPAT / Invested Capital  (NOPAT = EBIT * (1 - effective_tax_rate))
    # Fallback: use (NI + interest_expense) / (debt + equity) if EBIT missing
    roic_pct: float | None = None
    if ebit_ttm is not None and equity is not None:
        _inv_cap = (equity or 0) + (_debt or 0) - (_cash or 0)
        if _inv_cap and _inv_cap != 0:
            # Approximate tax rate via (NI/EBIT) if available, else default 21%
            if _ni is not None and ebit_ttm != 0:
                effective_tax = max(0.0, min(0.5, 1.0 - _ni / ebit_ttm))
            else:
                effective_tax = 0.21
            nopat = ebit_ttm * (1.0 - effective_tax)
            roic_pct = _safe_div(nopat * 100, _inv_cap)

    # FCF Margin % = FCF / Revenue * 100
    fcf_margin_pct = _safe_div((_fcf or 0) * 100, _rev) if _fcf is not None and _rev else None

    # CFO/NI
    cfo_to_ni = _safe_div(_cfo, _ni) if _cfo is not None and _ni else None

    # FCF/EBIT
    fcf_to_ebit = _safe_div(_fcf, ebit_ttm) if _fcf is not None and ebit_ttm else None

    # Accruals Ratio = (NI - CFO) / avg(Total Assets)
    # Use latest quarter total_assets for both if we only have one
    accruals_ratio: float | None = None
    if _ni is not None and _cfo is not None and total_assets:
        accruals_ratio = _safe_div(_ni - _cfo, total_assets)

    # Interest Coverage = EBIT / |Interest Expense|
    interest_coverage_x: float | None = None
    if ebit_ttm is not None and interest_expense_ttm and interest_expense_ttm != 0:
        interest_coverage_x = ebit_ttm / abs(interest_expense_ttm)

    # Debt / Equity — prefer yfinance (it's reported D/E ratio × 100 in some sources)
    debt_to_equity: float | None = None
    yf_de = _yf("debtToEquity")
    if yf_de is not None:
        # yfinance reports as % (e.g., 54.6 means 54.6%), normalize to ratio
        debt_to_equity = yf_de / 100.0 if yf_de > 5 else yf_de
    elif _debt is not None and equity and equity != 0:
        debt_to_equity = _safe_div(_debt, equity)

    # Net Debt / EBITDA
    netdebt_to_ebitda: float | None = None
    if _debt is not None and _cash is not None and _ebitda and _ebitda != 0:
        net_debt = _debt - _cash
        netdebt_to_ebitda = _safe_div(net_debt, _ebitda)

    # Net Cash / Market Cap %
    netcash_to_mktcap_pct: float | None = None
    if _cash is not None and _debt is not None and market_cap and market_cap != 0:
        netcash_to_mktcap_pct = _safe_div((_cash - _debt) * 100, market_cap)

    # ------------------------------------------------------------------
    # FCF Yield % = FCF / Market Cap * 100
    # ------------------------------------------------------------------
    fcf_yield_pct: float | None = None
    if _fcf is not None and market_cap and market_cap != 0:
        fcf_yield_pct = _safe_div(_fcf * 100, market_cap)

    # ------------------------------------------------------------------
    # Insider ownership %
    # ------------------------------------------------------------------
    insider_own_pct: float | None = None
    yf_ins = _yf("heldPercentInsiders")
    if yf_ins is not None:
        insider_own_pct = yf_ins * 100  # convert 0.0058 → 0.58%

    # ------------------------------------------------------------------
    # PEG 5Y  (yfinance trailingPegRatio)
    # ------------------------------------------------------------------
    peg_5y = _yf("trailingPegRatio")

    # ------------------------------------------------------------------
    # SBC / Sales %
    # ------------------------------------------------------------------
    sbc_to_sales_pct: float | None = None
    if sbc_ttm is not None and _rev and _rev != 0:
        sbc_to_sales_pct = _safe_div(sbc_ttm * 100, _rev)

    # ------------------------------------------------------------------
    # CAGR calculations from annual financial statements
    # ------------------------------------------------------------------
    # Build annual records from source_q (newest first)
    annual_inc = source_q.get("incomeStatementHistory", {}).get("incomeStatementHistory", [])
    annual_dates = sorted(
        {_to_iso_date(s.get("endDate")) for s in annual_inc if _to_iso_date(s.get("endDate"))},
        reverse=True,
    )

    # Annual EPS and Revenue series (newest first)
    annual_eps_series: list[float] = []
    annual_rev_series: list[float] = []
    annual_shares_series: list[float] = []

    for d in annual_dates:
        inc_row = next((s for s in annual_inc if _to_iso_date(s.get("endDate")) == d), {})
        eps_v = _num_raw(inc_row.get("dilutedEps"))
        rev_v = _num_raw(inc_row.get("totalRevenue"))
        sh_v  = _num_raw(inc_row.get("dilutedAverageShares"))
        if eps_v is not None:
            annual_eps_series.append(eps_v)
        if rev_v is not None:
            annual_rev_series.append(rev_v)
        if sh_v is not None:
            annual_shares_series.append(sh_v)

    eps_cagr_3y_pct: float | None = None
    eps_cagr_5y_pct: float | None = None
    revenue_cagr_3y_pct: float | None = None
    revenue_cagr_5y_pct: float | None = None
    sharecount_change_5y_pct: float | None = None
    buyback_yield_pct: float | None = None

    # 3Y CAGR requires at least 4 data points (4 annual → 3 years of change)
    if len(annual_eps_series) >= 4:
        eps_cagr_3y_pct = _cagr(annual_eps_series[0], annual_eps_series[3], 3)
    if len(annual_rev_series) >= 4:
        revenue_cagr_3y_pct = _cagr(annual_rev_series[0], annual_rev_series[3], 3)

    # 5Y CAGR: yfinance typically provides 4 annual data points (not 6),
    # so use Finnhub-supplied epsGrowth5Y / revenueGrowth5Y if available,
    # otherwise use whatever annual range we have.
    # We prefer our own calculation when possible.
    if len(annual_eps_series) >= 6:
        eps_cagr_5y_pct = _cagr(annual_eps_series[0], annual_eps_series[5], 5)
    if len(annual_rev_series) >= 6:
        revenue_cagr_5y_pct = _cagr(annual_rev_series[0], annual_rev_series[5], 5)

    # Share-count change (buyback/dilution) over available annual range
    # Using yfinance annual balance sheet shares (newest-first from merged_quarters)
    # Derive from annual financials shares_diluted if available
    if len(annual_shares_series) >= 2:
        n_years = len(annual_shares_series) - 1
        # Negative = shares declining (buybacks), positive = dilution
        sc_cagr = _cagr(annual_shares_series[0], annual_shares_series[-1], n_years)
        if sc_cagr is not None:
            sharecount_change_5y_pct = sc_cagr  # annualised % change (negative = buybacks)

        # Buyback Yield ≈ (shares_prev_year - shares_now) / shares_prev_year * 100
        if annual_shares_series[0] > 0 and annual_shares_series[1] > 0:
            buyback_yield_pct = (
                (annual_shares_series[1] - annual_shares_series[0]) / annual_shares_series[1] * 100
            )

    # ------------------------------------------------------------------
    # Assemble payload
    # ------------------------------------------------------------------
    payload: dict[str, Any] = {
        "ticker_symbol": ticker,
        "as_of_date": as_of,
        "data_source": "yahoo:multi_source_v1",
        # Price / market
        "price_current": price_current,
        "market_cap": market_cap,
        "shares_out": shares_out,
        # Valuation multiples
        "pe_fwd": pe_fwd,
        "pe_ttm": pe_ttm,
        "current_pe": current_pe,
        "ev_ebitda": ev_ebitda,
        "fcf_yield_pct": fcf_yield_pct,
        "peg_5y": peg_5y,
        # TTM flow totals
        "revenue_ttm": revenue_ttm,
        "cfo_ttm": cfo_ttm,
        "capex_ttm": capex_ttm,
        "sbc_ttm": sbc_ttm,
        "ebit_ttm": ebit_ttm,
        "depreciation_ttm": depreciation_ttm,
        "ebitda_ttm": ebitda_ttm,
        "net_income_ttm": net_income_ttm,
        "interest_expense_ttm": interest_expense_ttm,
        # Balance sheet
        "cash": _cash,
        "total_debt": _debt,
        "equity": equity,
        "total_assets": total_assets,
        # Quality metrics
        "roic_pct": roic_pct,
        "fcf_margin_pct": fcf_margin_pct,
        "cfo_to_ni": cfo_to_ni,
        "fcf_to_ebit": fcf_to_ebit,
        "accruals_ratio": accruals_ratio,
        # Capital allocation
        "buyback_yield_pct": buyback_yield_pct,
        "debt_to_equity": debt_to_equity,
        "netdebt_to_ebitda": netdebt_to_ebitda,
        "interest_coverage_x": interest_coverage_x,
        # Risk / leverage
        "beta_5y": beta_5y,
        "netcash_to_mktcap_pct": netcash_to_mktcap_pct,
        # Growth
        "eps_cagr_3y_pct": eps_cagr_3y_pct,
        "eps_cagr_5y_pct": eps_cagr_5y_pct,
        "revenue_cagr_3y_pct": revenue_cagr_3y_pct,
        "revenue_cagr_5y_pct": revenue_cagr_5y_pct,
        # Dilution
        "sharecount_change_5y_pct": sharecount_change_5y_pct,
        "sbc_to_sales_pct": sbc_to_sales_pct,
        # Moat / ownership
        "insider_own_pct": insider_own_pct,
    }
    return payload
