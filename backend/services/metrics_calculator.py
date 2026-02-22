"""
Deterministic metrics calculator.

Mirrors the computeFundamentalMetrics / computeGrowthMetrics /
computeAndSavePriceMetrics / runDeterministicPipeline logic from Extract1.

Key formulas (exact from extracts):
  FCF       = cfo_ttm - abs(capex_ttm)            (NOT cfo + capex)
  EBITDA    = ebit_ttm + depreciation_ttm
  ROIC %    = 100 * (NI + interest_expense) / (total_debt + equity)
  EPS_TTM   = net_income_ttm / avg(shares_diluted) [avg of 4 quarters]
  FCF yield = 100 * fcf_ttm / market_cap
  FCF margin= 100 * fcf_ttm / revenue_ttm
  CFO/NI    = cfo_ttm / net_income_ttm
  FCF/EBIT  = fcf_ttm / ebit_ttm
  Debt/Eq   = total_debt / stockholder_equity
  NetDebt/EBITDA = (total_debt - cash) / ebitda_ttm
  IntCov    = ebit_ttm / abs(interest_expense_ttm)
  Beta      = cov(stock_log_ret, spy_log_ret) / var(spy_log_ret)  [weekly]
  MaxDD     = max over daily close_adj of (peak - price) / peak   [*100 for %]
  Cyclicality = stdev(yoy_revenue_changes) * 100
  CAGR      = (end/start)^(1/years) - 1  (exact from Extract3)
  Buyback yield: 100 * (shares[n-1] - shares[n]) / shares[n-1]   [most recent 2 annuals]
  SBC/Sales = 100 * sbc_ttm / revenue_ttm

  PE history (price_metrics):
    Uses monthly close_adj from last 60 months.
    pe_5y_low, pe_5y_high, pe_5y_median from the monthly PE series.
    PEG = current_pe / (eps_cagr_5y_pct / 100)
"""

import logging
import math
import statistics
from datetime import date, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Basic numeric helpers
# ---------------------------------------------------------------------------

def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and math.isfinite(v)


def _safe_div(a: Any, b: Any) -> float | None:
    """Return a/b or None if either is non-numeric or b==0."""
    if not _is_num(a) or not _is_num(b) or b == 0:
        return None
    return a / b


def _parse_date(d: Any) -> date | None:
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        try:
            return date.fromisoformat(d[:10])
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# TTM builder (mirrors ttmSum / computeFundamentalMetrics in Extract1)
# ---------------------------------------------------------------------------

def build_ttm(quarterly: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Build TTM (trailing twelve months) and BALANCE from the most recent
    4 quarterly records.

    quarterly: list of FinancialsHistory dicts, sorted newest-first.
    Returns (TTM dict, BALANCE dict).

    TTM flows require all 4 quarters to be non-null; returns None if any missing.
    BALANCE comes from quarterly[0] (most recent).
    """
    # Take the 4 most recent quarters
    last4 = quarterly[:4]

    def ttm_sum(field: str) -> float | None:
        vals = [q.get(field) for q in last4]
        nums = [v for v in vals if _is_num(v)]
        return sum(nums) if len(nums) == 4 else None

    def ttm_avg(field: str) -> float | None:
        vals = [q.get(field) for q in last4]
        nums = [v for v in vals if _is_num(v)]
        return (sum(nums) / 4) if len(nums) == 4 else None

    TTM = {
        "revenue": ttm_sum("revenue"),
        "net_income": ttm_sum("net_income"),
        "cfo": ttm_sum("cfo"),
        "capex": ttm_sum("capex"),
        "ebit": ttm_sum("ebit"),
        "interest_expense": ttm_sum("interest_expense"),
        "depreciation": ttm_sum("depreciation"),
        "sbc": ttm_sum("stock_based_compensation"),
        "shares_diluted_avg": ttm_avg("shares_diluted"),
    }

    latest = quarterly[0] if quarterly else {}
    BALANCE = {
        "cash": latest.get("cash"),
        "total_debt": latest.get("total_debt"),
        "stockholder_equity": latest.get("stockholder_equity"),
        "total_assets": latest.get("total_assets"),
        "shares_outstanding": latest.get("shares_outstanding") or latest.get("shares_diluted"),
    }

    return TTM, BALANCE


# ---------------------------------------------------------------------------
# Quality metrics (mirrors computeQualityMetrics)
# ---------------------------------------------------------------------------

def compute_quality_metrics(
    TTM: dict,
    BALANCE: dict,
    quarterly: list[dict],
) -> dict[str, Any]:
    """
    cfo_to_ni, fcf_margin_pct, accruals_ratio, margin_stdev_5y_pct
    """
    out: dict[str, Any] = {}

    cfo = TTM.get("cfo")
    ni = TTM.get("net_income")
    fcf = TTM.get("cfo")
    capex = TTM.get("capex")
    rev = TTM.get("revenue")

    # FCF = cfo - abs(capex)  (not cfo + capex)
    if _is_num(cfo) and _is_num(capex):
        fcf_val = cfo - abs(capex)
    else:
        fcf_val = None

    out["cfo_to_ni"] = _safe_div(cfo, ni)
    out["fcf_margin_pct"] = _safe_div_pct(fcf_val, rev)

    # FCF/EBIT
    ebit = TTM.get("ebit")
    out["fcf_to_ebit"] = _safe_div(fcf_val, ebit)

    # Accruals ratio = (NI - CFO) / avg(total_assets)
    # Use quarterly[0] and quarterly[3] for avg assets approximation
    if len(quarterly) >= 4 and _is_num(ni) and _is_num(cfo):
        ta0 = quarterly[0].get("total_assets")
        ta3 = quarterly[3].get("total_assets")
        if _is_num(ta0) and _is_num(ta3) and ta0 + ta3 != 0:
            out["accruals_ratio"] = _safe_div(ni - cfo, (ta0 + ta3) / 2)

    # Gross margin stdev over last 5 years quarterly (if available)
    # Use operating_income / revenue as proxy for margin if gross_profit unavailable
    margins = []
    for q in quarterly[:20]:
        r = q.get("revenue")
        gp = q.get("gross_profit") or q.get("operating_income")
        if _is_num(r) and _is_num(gp) and r != 0:
            margins.append(gp / r)
    if len(margins) >= 4:
        out["margin_stdev_5y_pct"] = statistics.stdev(margins) * 100

    return {k: v for k, v in out.items() if v is not None}


def _safe_div_pct(a: Any, b: Any) -> float | None:
    d = _safe_div(a, b)
    return d * 100 if d is not None else None


# ---------------------------------------------------------------------------
# Capital allocation metrics (mirrors computeCapitalAllocationMetrics)
# ---------------------------------------------------------------------------

def compute_capital_allocation_metrics(
    TTM: dict,
    BALANCE: dict,
    quarterly: list[dict],
    annual: list[dict],
) -> dict[str, Any]:
    """
    roic_pct, fcf_yield_pct, debt_to_equity, netdebt_to_ebitda,
    interest_coverage_x, buyback_yield_pct, sbc_to_sales_pct
    """
    out: dict[str, Any] = {}

    ni = TTM.get("net_income")
    ie = TTM.get("interest_expense")
    total_debt = BALANCE.get("total_debt")
    equity = BALANCE.get("stockholder_equity")
    cash = BALANCE.get("cash")
    ebit = TTM.get("ebit")
    dep = TTM.get("depreciation")
    rev = TTM.get("revenue")
    sbc = TTM.get("sbc")
    cfo = TTM.get("cfo")
    capex = TTM.get("capex")

    fcf_val = (cfo - abs(capex)) if _is_num(cfo) and _is_num(capex) else None
    ebitda = (ebit + dep) if _is_num(ebit) and _is_num(dep) else None

    # ROIC = (NI + interest_expense) / (total_debt + equity)
    # Mirrors exact formula from Extract1
    if _is_num(ni) and _is_num(ie) and _is_num(total_debt) and _is_num(equity):
        invested_capital = total_debt + equity
        if invested_capital != 0:
            out["roic_pct"] = 100 * (ni + ie) / invested_capital

    # Debt / Equity
    if _is_num(total_debt) and _is_num(equity) and equity != 0:
        out["debt_to_equity"] = total_debt / equity

    # Net Debt / EBITDA
    if _is_num(total_debt) and _is_num(cash) and _is_num(ebitda) and ebitda != 0:
        out["netdebt_to_ebitda"] = (total_debt - cash) / ebitda

    # Interest coverage = ebit / abs(interest_expense)
    if _is_num(ebit) and _is_num(ie) and abs(ie) > 0:
        out["interest_coverage_x"] = ebit / abs(ie)

    # SBC / Sales
    if _is_num(sbc) and _is_num(rev) and rev != 0:
        out["sbc_to_sales_pct"] = 100 * sbc / rev

    # Buyback yield: 100 * (shares[1] - shares[0]) / shares[1]
    # Uses the 2 most recent annual shares_diluted values
    # shares[0] = most recent, shares[1] = prior year
    annual_shares = [
        a.get("shares_diluted") for a in annual[:2]
        if _is_num(a.get("shares_diluted"))
    ]
    if len(annual_shares) == 2:
        s0, s1 = annual_shares[0], annual_shares[1]
        if s1 != 0:
            out["buyback_yield_pct"] = 100 * (s1 - s0) / s1

    # Net cash / market cap — requires market_cap (computed downstream)
    return {k: v for k, v in out.items() if _is_num(v)}


# ---------------------------------------------------------------------------
# Growth metrics (mirrors computeGrowthMetrics)
# ---------------------------------------------------------------------------

def _cagr(series: list[dict], field: str, years: int) -> float | None:
    """
    Compute CAGR for a given field over `years` years.
    Mirrors the exact cagr() helper from Extract3:
      - series: sorted oldest-to-newest list of {date: ..., value: ...}
      - find start point as latest record with date <= (end_date - years)
      - require actualYears >= years - 0.5
    """
    if not series:
        return None

    # Build value series
    pts = []
    for s in series:
        d = _parse_date(s.get("period_end") or s.get("date"))
        v = s.get(field)
        if d is not None and _is_num(v) and v > 0:
            pts.append((d, v))

    if len(pts) < 2:
        return None

    pts.sort(key=lambda x: x[0])
    end_date, end_val = pts[-1]

    # Find start: latest point where date <= end_date - years
    cutoff = date(end_date.year - years, end_date.month, end_date.day)
    candidates = [(d, v) for d, v in pts if d <= cutoff]
    if not candidates:
        return None

    start_date, start_val = max(candidates, key=lambda x: x[0])

    actual_years = (end_date - start_date).days / 365.25
    if actual_years < years - 0.5:
        return None

    try:
        return (math.pow(end_val / start_val, 1 / actual_years) - 1) * 100
    except (ValueError, ZeroDivisionError):
        return None


def compute_growth_metrics(
    quarterly: list[dict],
    annual: list[dict],
) -> dict[str, Any]:
    """
    eps_cagr_5y_pct, eps_cagr_3y_pct, revenue_cagr_5y_pct, revenue_cagr_3y_pct,
    sharecount_change_5y_pct (same as buyback_yield approximation over 5y)
    """
    out: dict[str, Any] = {}

    # Build annual EPS series for CAGR
    annual_eps = []
    for a in annual:
        eps = a.get("eps_diluted")
        rev = a.get("revenue")
        d = _parse_date(a.get("period_end"))
        if d and _is_num(eps) and eps > 0:
            annual_eps.append({"period_end": d.isoformat(), "eps": eps})
        if d and _is_num(rev) and rev > 0:
            pass  # included in annual records

    for years in (5, 3):
        eps_c = _cagr(annual_eps, "eps", years)
        if _is_num(eps_c):
            out[f"eps_cagr_{years}y_pct"] = eps_c

        rev_c = _cagr(annual, "revenue", years)
        if _is_num(rev_c):
            out[f"revenue_cagr_{years}y_pct"] = rev_c

    # Cyclicality = stdev(YoY revenue changes) * 100
    annual_revs = sorted(
        [(a.get("period_end"), a.get("revenue")) for a in annual if _is_num(a.get("revenue"))],
        key=lambda x: x[0],
    )
    if len(annual_revs) >= 3:
        yoy_changes = []
        for i in range(1, len(annual_revs)):
            r_prev = annual_revs[i - 1][1]
            r_curr = annual_revs[i][1]
            if r_prev and r_prev != 0:
                yoy_changes.append((r_curr - r_prev) / r_prev)
        if len(yoy_changes) >= 2:
            out["cyclicality_pct"] = statistics.stdev(yoy_changes) * 100

    return {k: v for k, v in out.items() if _is_num(v)}


# ---------------------------------------------------------------------------
# Risk metrics (mirrors computeRiskMetrics)
# ---------------------------------------------------------------------------

def compute_risk_metrics(
    prices: list[dict],
    spy_prices: list[dict],
) -> dict[str, Any]:
    """
    beta_5y, maxdrawdown_5y_pct

    Beta: weekly log-return covariance(stock, SPY) / variance(SPY).
    Requires >104 weekly data points.

    MaxDrawdown: iterate daily close_adj, track running peak,
    drawdown = (peak - price) / peak.
    """
    out: dict[str, Any] = {}

    # Max drawdown (daily close_adj)
    sorted_prices = sorted(prices, key=lambda p: p.get("date", ""))
    if sorted_prices:
        peak = 0.0
        max_dd = 0.0
        for p in sorted_prices:
            price = p.get("close_adj") or p.get("close")
            if not _is_num(price):
                continue
            if price > peak:
                peak = price
            if peak > 0:
                dd = (peak - price) / peak
                if dd > max_dd:
                    max_dd = dd
        out["maxdrawdown_5y_pct"] = max_dd * 100

    # Beta (weekly log returns vs SPY)
    if spy_prices:
        stock_weekly = _week_end_log_returns(prices)
        spy_weekly = _week_end_log_returns(spy_prices)

        # Align on same dates
        spy_map = {d: r for d, r in spy_weekly}
        aligned_stock = []
        aligned_spy = []
        for d, r in stock_weekly:
            if d in spy_map:
                aligned_stock.append(r)
                aligned_spy.append(spy_map[d])

        if len(aligned_stock) >= 104:
            beta = _cov_var(aligned_stock, aligned_spy)
            if beta is not None:
                out["beta_5y"] = beta

    return {k: v for k, v in out.items() if _is_num(v)}


def _week_end_log_returns(prices: list[dict]) -> list[tuple[str, float]]:
    """
    Convert daily prices to weekly log returns.
    Week-end = Friday of each week (or last trading day).
    Mirrors weekEndSeries logic in Extract1.
    """
    sorted_p = sorted(prices, key=lambda p: p.get("date", ""))

    # Group by ISO week (year, week_number)
    from collections import defaultdict
    weekly: dict[tuple, list[dict]] = defaultdict(list)
    for p in sorted_p:
        d = _parse_date(p.get("date"))
        if d is None:
            continue
        # Use ISO calendar week
        iso = d.isocalendar()
        weekly[(iso.year, iso.week)].append(p)

    # Take last trading day of each week
    week_ends = []
    for (y, w), pts in sorted(weekly.items()):
        last = max(pts, key=lambda p: p.get("date", ""))
        c = last.get("close_adj") or last.get("close")
        if _is_num(c):
            week_ends.append((last["date"], c))

    week_ends.sort(key=lambda x: x[0])

    returns: list[tuple[str, float]] = []
    for i in range(1, len(week_ends)):
        d_curr, p_curr = week_ends[i]
        _, p_prev = week_ends[i - 1]
        if p_prev > 0 and p_curr > 0:
            returns.append((d_curr, math.log(p_curr / p_prev)))

    return returns


def _cov_var(x: list[float], y: list[float]) -> float | None:
    """Return cov(x,y) / var(y) or None."""
    if len(x) < 2 or len(x) != len(y):
        return None
    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y)) / (n - 1)
    var_y = sum((yi - mean_y) ** 2 for yi in y) / (n - 1)
    if var_y == 0:
        return None
    return cov / var_y


# ---------------------------------------------------------------------------
# Price metrics (mirrors computeAndSavePriceMetrics)
# ---------------------------------------------------------------------------

def compute_price_metrics(
    ticker: str,
    prices: list[dict],
    quarterly: list[dict],
    market_cap: float | None,
    eps_cagr_5y_pct: float | None,
) -> dict[str, Any]:
    """
    price_current, pe_ttm, pe_5y_low, pe_5y_high, pe_5y_median,
    fcf_yield_pct, peg_5y, ev_ebitda (if EV available),
    current_pe.

    Uses monthly close_adj series (last 60 months).
    PE uses close_adj for 5Y band computation (from price data).
    """
    out: dict[str, Any] = {}

    sorted_prices = sorted(prices, key=lambda p: p.get("date", ""), reverse=True)
    if not sorted_prices:
        return out

    # Current price = most recent close_adj
    price_current = sorted_prices[0].get("close_adj") or sorted_prices[0].get("close")
    if _is_num(price_current):
        out["price_current"] = price_current

    # Build monthly close_adj series (last 60 months)
    monthly_series = _month_end_series(sorted_prices, max_months=60)

    # Build monthly EPS TTM map from quarterly data
    q_asc = sorted(quarterly, key=lambda q: q.get("period_end", ""))
    monthly_eps = {}
    for i in range(len(q_asc)):
        window = q_asc[max(0, i - 3): i + 1]
        if len(window) == 4:
            ni_vals = [q.get("net_income") for q in window]
            sh_vals = [q.get("shares_diluted") for q in window]
            if all(_is_num(v) for v in ni_vals) and all(_is_num(v) for v in sh_vals):
                sh_avg = sum(sh_vals) / 4
                if sh_avg > 0:
                    eps = sum(ni_vals) / sh_avg
                    if eps > 0:
                        period_end = window[3].get("period_end")
                        period_d = _parse_date(period_end)
                        if period_d is not None:
                            monthly_eps[period_d.strftime("%Y-%m")] = eps

    # Build PE series using monthly prices and EPS TTM
    pe_series = []
    for month_key, close_adj in monthly_series:
        eps_keys = sorted(monthly_eps.keys(), reverse=True)
        eps_key = next((k for k in eps_keys if k <= month_key), None)
        if eps_key and monthly_eps[eps_key] >= 0.01:
            pe = close_adj / monthly_eps[eps_key]
            if _is_num(pe) and 0 < pe < 1000:
                pe_series.append(pe)

    if pe_series:
        out["pe_5y_low"] = min(pe_series)
        out["pe_5y_high"] = max(pe_series)
        out["pe_5y_median"] = statistics.median(pe_series)

    # current_pe from price_current and latest EPS TTM
    if _is_num(price_current) and monthly_eps:
        today_key = date.today().strftime("%Y-%m")
        eps_keys = sorted(monthly_eps.keys(), reverse=True)
        eps_key = next((k for k in eps_keys if k <= today_key), None)
        if eps_key and monthly_eps[eps_key] >= 0.01:
            pe_now = price_current / monthly_eps[eps_key]
            if _is_num(pe_now):
                out["pe_ttm"] = pe_now
                out["current_pe"] = pe_now

    # PEG = current_pe / (eps_cagr_5y_pct / 100)
    if _is_num(out.get("current_pe")) and _is_num(eps_cagr_5y_pct) and eps_cagr_5y_pct > 0:
        peg = out["current_pe"] / (eps_cagr_5y_pct / 100)
        if _is_num(peg):
            out["peg_5y"] = peg

    return {k: v for k, v in out.items() if _is_num(v)}


def _month_end_series(
    prices: list[dict],
    max_months: int = 60,
) -> list[tuple[str, float]]:
    """
    Return list of (YYYY-MM, close_adj) for each month-end in prices,
    up to max_months months. prices must be sorted newest-first.
    """
    from collections import defaultdict
    by_month: dict[str, list[dict]] = defaultdict(list)
    for p in prices:
        d = p.get("date")
        d_parsed = _parse_date(d)
        month_key = d_parsed.strftime("%Y-%m") if d_parsed else (str(d)[:7] if d else None)
        if month_key:
            by_month[month_key].append(p)

    result = []
    for month_key in sorted(by_month.keys(), reverse=True)[:max_months]:
        month_prices = by_month[month_key]
        last = max(month_prices, key=lambda p: p.get("date", ""))
        c = last.get("close_adj") or last.get("close")
        if _is_num(c):
            result.append((month_key, c))

    # Return ascending for consistent processing
    result.sort(key=lambda x: x[0])
    return result


# ---------------------------------------------------------------------------
# Full deterministic pipeline entry point
# ---------------------------------------------------------------------------

def run_deterministic_pipeline(
    ticker: str,
    quarterly: list[dict],
    annual: list[dict],
    prices: list[dict],
    spy_prices: list[dict],
    existing_metrics: dict | None = None,
) -> dict[str, Any]:
    """
    Run all deterministic metric computations in the canonical order:
      1. computePriceMetrics
      2. computeQualityMetrics
      3. computeCapitalAllocationMetrics
      4. computeGrowthMetrics
      5. computeRiskMetrics

    Returns merged payload dict for metrics upsert.

    Phase 1.1 — TTM integrity:
      If quarterly coverage < 4, TTM flow fields (eps_ttm, pe_ttm, fcf_ttm, etc.)
      are set to None. partial_ttm flag is set to True.
      Do NOT backfill with projections.
    """
    # --- Phase 1.1: TTM coverage check ---
    from backend.services.metric_resolver import check_ttm_coverage, validate_eps_forward
    ttm_coverage = check_ttm_coverage(quarterly, ticker=ticker)
    partial_ttm = not ttm_coverage["sufficient"]

    TTM, BALANCE = build_ttm(quarterly)

    # Derive FCF TTM
    cfo = TTM.get("cfo")
    capex = TTM.get("capex")
    fcf_ttm = (cfo - abs(capex)) if _is_num(cfo) and _is_num(capex) else None

    # Derive EBITDA TTM
    ebit = TTM.get("ebit")
    dep = TTM.get("depreciation")
    ebitda_ttm = (ebit + dep) if _is_num(ebit) and _is_num(dep) else None

    # EPS TTM
    ni_ttm = TTM.get("net_income")
    sh_avg = TTM.get("shares_diluted_avg")
    eps_ttm = (ni_ttm / sh_avg) if _is_num(ni_ttm) and _is_num(sh_avg) and sh_avg > 0 else None

    # market_cap from latest price * shares_outstanding
    sorted_prices = sorted(prices, key=lambda p: p.get("date", ""), reverse=True)
    price_now = sorted_prices[0].get("close_adj") if sorted_prices else None
    shares_out = BALANCE.get("shares_outstanding")
    market_cap = (price_now * shares_out) if _is_num(price_now) and _is_num(shares_out) else None

    # Growth metrics are still used elsewhere (PEG, projection hints, etc.)
    growth_metrics_seed = compute_growth_metrics(quarterly, annual)
    eps_cagr_5y = growth_metrics_seed.get("eps_cagr_5y_pct")
    if not _is_num(eps_cagr_5y) and existing_metrics:
        eps_cagr_5y = existing_metrics.get("eps_cagr_5y_pct")

    # EPS trace for auditability
    logger.info(
        "[EPS_TRACE] %s eps_ttm_fields=financials_history.net_income(4q_sum)/financials_history.shares_diluted(4q_avg) eps_ttm=%s",
        ticker,
        eps_ttm,
    )

    # Forward EPS policy (Phase 1.2 — metric_resolver):
    # consensus next-12-month EPS only (from quote endpoint ingest into metrics.eps_forward)
    # Never derived from CAGR projection. Validated via metric_resolver.
    eps_forward_raw = (existing_metrics or {}).get("eps_forward") if isinstance(existing_metrics, dict) else None

    logger.info(
        "[EPS_TRACE] %s eps_forward_field=metrics.eps_forward(consensus_ntm_quote) eps_forward_raw=%s",
        ticker,
        eps_forward_raw,
    )

    # Use centralized validator from metric_resolver
    eps_forward = validate_eps_forward(eps_forward_raw, eps_ttm, ticker=ticker)

    # Deterministic forward PE = current price / forward EPS (no API-provided PE)
    pe_fwd = None
    if _is_num(price_now) and _is_num(eps_forward) and eps_forward > 0:
        pe_fwd_calc = price_now / eps_forward
        if _is_num(pe_fwd_calc):
            pe_fwd = pe_fwd_calc

    # Seed payload with TTM and BALANCE
    payload: dict[str, Any] = {
        "ticker_symbol": ticker,
        "as_of_date": date.today().isoformat(),
        "data_source": "computed",
        "partial_ttm": partial_ttm,          # Phase 1.1: flag insufficient TTM coverage
        "cfo_ttm": cfo,
        "capex_ttm": capex,
        "ebit_ttm": ebit,
        "depreciation_ttm": dep,
        "ebitda_ttm": ebitda_ttm,
        "net_income_ttm": ni_ttm,
        "interest_expense_ttm": TTM.get("interest_expense"),
        "sbc_ttm": TTM.get("sbc"),
        "revenue_ttm": TTM.get("revenue"),
        "fcf_ttm": fcf_ttm,
        "eps_ttm": eps_ttm,
        "eps_forward": eps_forward,
        "cash": BALANCE.get("cash"),
        "total_debt": BALANCE.get("total_debt"),
        "equity": BALANCE.get("stockholder_equity"),
        "total_assets": BALANCE.get("total_assets"),
        "shares_out": shares_out,
        "market_cap": market_cap,
        "pe_fwd": pe_fwd,
    }

    # 1. Price metrics
    price_m = compute_price_metrics(ticker, prices, quarterly, market_cap, eps_cagr_5y)
    payload.update(price_m)

    # 2. Quality metrics
    quality_m = compute_quality_metrics(TTM, BALANCE, quarterly)
    payload.update(quality_m)

    # 3. Capital allocation metrics
    ca_m = compute_capital_allocation_metrics(TTM, BALANCE, quarterly, annual)
    payload.update(ca_m)

    # FCF yield (needs market_cap from price step)
    mc = payload.get("market_cap")
    if _is_num(fcf_ttm) and _is_num(mc) and mc != 0:
        payload["fcf_yield_pct"] = 100 * fcf_ttm / mc

    # Net cash / market cap
    cash_v = BALANCE.get("cash")
    td = BALANCE.get("total_debt")
    if _is_num(cash_v) and _is_num(td) and _is_num(mc) and mc != 0:
        payload["netcash_to_mktcap_pct"] = 100 * (cash_v - td) / mc

    # 4. Growth metrics
    payload.update(growth_metrics_seed)

    # 5. Risk metrics
    risk_m = compute_risk_metrics(prices, spy_prices)
    payload.update(risk_m)

    # Sector PE/EV medians (from Extract3 sector_medians config)
    # These are read-only lookups; returned as-is for the orchestrator to fill in.

    if ticker.upper() in {"MSFT", "NFLX"}:
        logger.info(
            "[%s][PE_FWD] price_current=%s eps_forward=%s pe_fwd=%s formula=price_current/eps_forward",
            ticker.upper(),
            price_now,
            eps_forward,
            pe_fwd,
        )

    return payload
