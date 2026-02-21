"""
Finnhub data normalizer.

Mirrors normalizeAndQuarterizeData, buildTTMMetrics, calculateHistoricalPE_DB,
and upsertFinnhubMetrics from runFinnhubFundamentalsEtl.ts (Extract2).

Key behaviors:
  - PATTERNS: regex concept matching against XBRL ic/bs/cf arrays
  - normalizeShares: auto-detect "in millions" (v < 1e6 → multiply by 1e6)
  - toQ: normalize quarter label Q1/1/QUARTER1 → "Q1"
  - YTD differencing: Q1=YTD, Q2=YTD-Q1_YTD, Q3=YTD-Q2_YTD, Q4=YTD-Q3_YTD
  - Q4 synthesis: when only 3 quarters + annual available
  - TTM: last 4 ascending quarters, sum flows, avg shares
  - total_debt = latest.total_debt || (short_debt + long_debt)
  - eps_ttm = net_income_ttm / avg(shares_diluted) [average of 4 quarters]
  - PE history: uses close (split-adj, NOT close_adj), MIN_EPS=0.01
  - ALWAYS_UPDATE set controls Metrics upsert behavior
"""

import logging
import math
import re
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _num(v: Any) -> float | None:
    """Return finite float or None. Mirrors num() in Extract2."""
    if isinstance(v, (int, float)) and math.isfinite(v):
        return float(v)
    if isinstance(v, str) and v.strip():
        try:
            f = float(v)
            return f if math.isfinite(f) else None
        except ValueError:
            pass
    return None


def normalize_shares(v: Any) -> float | None:
    """
    Auto-detect if shares are reported in millions and convert to raw count.
    Mirrors normalizeShares in Extract2:
      if v >= 1e8 → raw (already full count)
      if 0 < v < 1e6 → multiply by 1e6 (likely "in millions")
      else → return as-is
    """
    n = _num(v)
    if n is None:
        return None
    if n >= 1e8:
        return n
    if 0 < n < 1e6:
        return n * 1e6
    return n


def to_q(q: Any) -> str:
    """
    Normalize quarter label to "Q1"/"Q2"/"Q3"/"Q4".
    Accepts "Q1"/"1"/1/"QUARTER1" etc.
    Mirrors toQ in Extract2.
    """
    s = str(q).upper().replace("QUARTER", "").strip()
    m = re.match(r"^[Q]?([1-4])$", s)
    if m:
        return f"Q{m.group(1)}"
    if s in ("Q1", "Q2", "Q3", "Q4"):
        return s
    return "Q1"


def q_num(q: Any) -> int:
    """Return 1-4 integer for quarter label."""
    return {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}.get(to_q(q), 1)


# ---------------------------------------------------------------------------
# PATTERNS — regex sets for XBRL concept/label matching
# Mirrors PATTERNS dict in Extract2 exactly.
# ---------------------------------------------------------------------------

PATTERNS: dict[str, list[re.Pattern]] = {
    "CFO": [
        re.compile(r"us-gaap_netcashprovidedbyusedinoperatingactivities", re.I),
        re.compile(r"operating.*cash.*flow", re.I),
        re.compile(r"net.*cash.*provided.*operating", re.I),
    ],
    "CapEx": [
        re.compile(r"paymentstoacquirepropertyplantandequipment", re.I),
        re.compile(r"paymentstoacquireproductiveassets", re.I),
        re.compile(r"acquisitionofpropertyplantandequipment", re.I),
        re.compile(r"(acquire|purchase).*property.*plant.*equipment", re.I),
        re.compile(r"productiveassets", re.I),
        re.compile(r"capital.*expend", re.I),
    ],
    "SBC": [
        re.compile(r"us-gaap_sharebasedcompensation", re.I),
        re.compile(r"share.*based.*comp", re.I),
        re.compile(r"stock.*based.*comp", re.I),
    ],
    "Depreciation": [
        re.compile(r"us-gaap_depreciationdepletionandamortization", re.I),
        re.compile(r"deprecia(tion)?(?!.*tax)", re.I),
        re.compile(r"deprecia.*amor[tz]", re.I),
    ],
    "InterestExp": [
        re.compile(r"interest.*expense", re.I),
        re.compile(r"interest.*paid", re.I),
    ],
    "Cash": [
        re.compile(r"cash(?!.*flow)", re.I),
        re.compile(r"cash.*equivalents", re.I),
        re.compile(r"cash.*short.*term.*invest", re.I),
    ],
    "ShortDebt": [
        re.compile(r"commercial.*paper", re.I),
        re.compile(r"short.*term.*debt", re.I),
        re.compile(r"current.*portion.*long.*term.*debt", re.I),
    ],
    "LongDebt": [
        re.compile(r"longtermdebtnoncurrent", re.I),
        re.compile(r"long.*term.*debt(?!.*current)", re.I),
    ],
    "TotalDebt": [
        re.compile(r"total.*debt", re.I),
        re.compile(r"total.*interest.*bearing.*debt", re.I),
    ],
    "Equity": [
        re.compile(r"total.*share.*holder.*equity", re.I),
        re.compile(r"totalstockholdersequity", re.I),
    ],
    "TotalAssets": [
        re.compile(r"total.*assets", re.I),
    ],
    "EBIT": [
        re.compile(r"us-gaap_earningsbeforeinterestandtaxes", re.I),
        re.compile(r"us-gaap_operatingincomeloss", re.I),
        re.compile(r"^ebit$", re.I),
        re.compile(r"earnings.*before.*interest.*tax", re.I),
    ],
    "NetIncome": [
        re.compile(r"us-gaap_netincomeloss", re.I),
        re.compile(r"^net.*income", re.I),
    ],
    "SharesDiluted": [
        re.compile(r"us-gaap_weightedaveragenumberofdilutedshare", re.I),
        re.compile(r"weighted.*average.*shares.*diluted", re.I),
    ],
}


def _val(arr: list[dict], patterns: list[re.Pattern]) -> float | None:
    """
    Scan arr for first item whose concept or label matches any pattern.
    Returns the numeric value of that item, or None.
    Mirrors val() in Extract2.
    """
    if not isinstance(arr, list):
        return None
    for pattern in patterns:
        for item in arr:
            if not isinstance(item, dict):
                continue
            concept = str(item.get("concept", "")).lower()
            label = str(item.get("label", "")).lower()
            if pattern.search(concept) or pattern.search(label):
                n = _num(item.get("value"))
                if n is not None:
                    return n
    return None


# ---------------------------------------------------------------------------
# normalizeAndQuarterizeData
# Mirrors normalizeAndQuarterizeData in Extract2 exactly.
# ---------------------------------------------------------------------------

def normalize_and_quarterize(
    quarterly_data: dict | None,
    annual_data: dict | None,
) -> list[dict[str, Any]]:
    """
    Parse raw Finnhub financials-reported response and produce a list of
    quarterized records sorted newest-first.

    Flow fields (cfo, capex, sbc, depreciation, interest_exp, ebit, net_income)
    are YTD-cumulative from Finnhub — we difference them to get per-quarter values.
    Balance sheet items (cash, debt, equity, assets) are taken directly.

    Q4 synthesis: if a fiscal year has only 3 quarters and annual data is
    available, synthesize Q4 = annual_YTD - Q3_YTD.
    """
    reports: list[dict] = []
    annual: dict[int, dict] = {}

    if quarterly_data and quarterly_data.get("data"):
        for r in quarterly_data["data"]:
            if not r.get("report"):
                continue
            fy = r.get("year") or (
                datetime.strptime(
                    (r.get("endDate") or r.get("acceptedDate") or "2000-01-01")[:10],
                    "%Y-%m-%d",
                ).year
            )
            quarter = to_q(
                r.get("quarter") or r.get("fiscalQuarter")
                or r.get("report", {}).get("fp")
                or r.get("period")
                or "Q1"
            )
            end_date = r.get("endDate") or r.get("acceptedDate") or r.get("period")
            if not end_date:
                continue
            period_end = end_date[:10]
            ic = r["report"].get("ic") or []
            bs = r["report"].get("bs") or []
            cf = r["report"].get("cf") or []
            reports.append({
                "fiscalYear": fy,
                "quarter": quarter,
                "periodEnd": period_end,
                "ic": ic,
                "bs": bs,
                "cf": cf,
            })

    if annual_data and annual_data.get("data"):
        for r in annual_data["data"]:
            if not r.get("report"):
                continue
            fy = r.get("year") or (
                datetime.strptime(
                    (r.get("endDate") or r.get("acceptedDate") or "2000-01-01")[:10],
                    "%Y-%m-%d",
                ).year
            )
            annual[fy] = {
                "ic": r["report"].get("ic") or [],
                "cf": r["report"].get("cf") or [],
            }

    # Log concepts from most recent report
    if reports:
        reports.sort(
            key=lambda r: (r["fiscalYear"], q_num(r["quarter"])), reverse=True
        )
        f = reports[0]
        logger.debug("[Finnhub][IC] concepts=%s", [x.get("concept") for x in f["ic"][:5]])
        logger.debug("[Finnhub][CF] concepts=%s", [x.get("concept") for x in f["cf"][:5]])
        logger.debug("[Finnhub][BS] concepts=%s", [x.get("concept") for x in f["bs"][:5]])

    # Quarterize per fiscal year
    years = sorted({r["fiscalYear"] for r in reports}, reverse=True)
    quarterized: list[dict[str, Any]] = []

    for fy in years:
        yr = sorted(
            [r for r in reports if r["fiscalYear"] == fy],
            key=lambda r: q_num(r["quarter"]),
        )
        if not yr:
            continue

        logger.debug("[Quarterize] FY %d with %d quarters", fy, len(yr))
        ytd_flows: dict[str, float] = {}
        annual_flows = annual.get(fy, {"ic": [], "cf": []})
        out: list[dict[str, Any]] = []

        flow_defs = [
            ("cfo", "CFO", "cf"),
            ("capex", "CapEx", "cf"),
            ("sbc", "SBC", "cf"),
            ("depreciation", "Depreciation", "cf"),
            ("interest_exp", "InterestExp", "ic"),
            ("ebit", "EBIT", "ic"),
            ("net_income", "NetIncome", "ic"),
        ]

        for r in yr:
            shares_raw = _val(r["ic"], PATTERNS["SharesDiluted"])
            shares_diluted = normalize_shares(shares_raw)

            qd: dict[str, Any] = {
                "periodEnd": r["periodEnd"],
                "fiscalYear": fy,
                "quarter": to_q(r["quarter"]),
                "cfo": None,
                "capex": None,
                "sbc": None,
                "depreciation": None,
                "interest_exp": None,
                "ebit": None,
                "net_income": None,
                "diluted_eps": None,
                "shares_diluted": shares_diluted,
                # Balance sheet (point-in-time, no differencing)
                "cash": _val(r["bs"], PATTERNS["Cash"]),
                "short_debt": _val(r["bs"], PATTERNS["ShortDebt"]),
                "long_debt": _val(r["bs"], PATTERNS["LongDebt"]),
                "total_debt": _val(r["bs"], PATTERNS["TotalDebt"]),
                "equity": _val(r["bs"], PATTERNS["Equity"]),
                "total_assets": _val(r["bs"], PATTERNS["TotalAssets"]),
            }

            for key, pat_key, section in flow_defs:
                src = r["ic"] if section == "ic" else r["cf"]
                ytd = _val(src, PATTERNS[pat_key])
                if ytd is not None:
                    prev = ytd_flows.get(key, 0.0)
                    logger.debug("[Quarterize][Probe] FY %d %s %s_YTD=%s prev_YTD=%s",
                                 fy, q_num(r["quarter"]), key, ytd, prev)
                    # Q1 = YTD directly; subsequent quarters = YTD - prev_YTD
                    qd[key] = ytd if q_num(r["quarter"]) == 1 else (ytd - prev)
                    ytd_flows[key] = ytd

            if qd["net_income"] is not None and shares_diluted and shares_diluted > 0:
                qd["diluted_eps"] = qd["net_income"] / shares_diluted
                logger.debug("[EPS-Qtr] FY %d %s eps=%s", fy, to_q(r["quarter"]), qd["diluted_eps"])

            out.append(qd)

        # Q4 synthesis: if only 3 quarters and annual data available
        if len(out) == 3 and (annual_flows.get("ic") or annual_flows.get("cf")):
            all_annual = list(annual_flows.get("ic", [])) + list(annual_flows.get("cf", []))
            q3 = next((x for x in out if x["quarter"] == "Q3"), None)
            if q3 is not None:
                sQ4 = dict(q3)
                sQ4["quarter"] = "Q4"
                q4_flow_defs = [
                    ("cfo", "CFO"),
                    ("capex", "CapEx"),
                    ("sbc", "SBC"),
                    ("depreciation", "Depreciation"),
                    ("interest_exp", "InterestExp"),
                    ("ebit", "EBIT"),
                    ("net_income", "NetIncome"),
                ]
                for k, pat_key in q4_flow_defs:
                    a = _val(all_annual, PATTERNS[pat_key])
                    y = ytd_flows.get(k)
                    if a is not None and y is not None and math.isfinite(a) and math.isfinite(y):
                        sQ4[k] = a - y
                    else:
                        sQ4[k] = None
                    logger.debug("[Q4] fy=%d %s: annual=%s ytd_q3=%s q4=%s", fy, k, a, y, sQ4[k])

                if sQ4["net_income"] is not None and sQ4.get("shares_diluted") and sQ4["shares_diluted"] > 0:
                    sQ4["diluted_eps"] = sQ4["net_income"] / sQ4["shares_diluted"]
                else:
                    sQ4["diluted_eps"] = None

                out.append(sQ4)
                out.sort(key=lambda x: q_num(x["quarter"]))

        quarterized.extend(out)
        logger.debug("[Quarterize] Flows differenced for FY %d", fy)

    # Sort newest first
    quarterized.sort(key=lambda r: r["periodEnd"], reverse=True)
    logger.info("[Finnhub] Quarterized %d reports", len(quarterized))
    return quarterized


# ---------------------------------------------------------------------------
# buildTTMMetrics
# Mirrors buildTTMMetrics in Extract2.
# ---------------------------------------------------------------------------

def build_ttm_metrics(quarters: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Build TTM (trailing twelve months) metrics from the last 4 quarterly records.

    Ascending sort → last4 = oldest of the 4 to newest.
    sum() returns null if ANY of the 4 quarters has null for that field.
    avg(shares_diluted) for eps_ttm denominator.
    total_debt = latest.total_debt || (short_debt + long_debt).
    """
    q_asc = sorted(quarters, key=lambda r: r["periodEnd"])
    last4 = q_asc[-4:]
    if len(last4) < 4:
        logger.warning("[TTM] Not enough quarters for TTM (found %d)", len(last4))
        return {}

    logger.debug("[TTM][Quarters] %s", [q["periodEnd"] for q in last4])

    def _sum(field: str) -> float | None:
        vals = [q.get(field) for q in last4]
        if any(v is None for v in vals):
            return None
        return sum(vals)

    def _avg(field: str) -> float | None:
        vals = [q.get(field) for q in last4]
        if any(v is None for v in vals):
            return None
        return sum(vals) / 4

    latest = last4[-1]  # most recent quarter

    ebit_ttm = _sum("ebit")
    depreciation_ttm = _sum("depreciation")
    ebitda_ttm = (
        (ebit_ttm + depreciation_ttm)
        if ebit_ttm is not None and depreciation_ttm is not None
        else None
    )
    net_income_ttm = _sum("net_income")
    cfo_ttm = _sum("cfo")
    capex_ttm = _sum("capex")
    sbc_ttm = _sum("sbc")
    interest_expense_ttm = _sum("interest_exp")

    eps_ttm: float | None = None
    sh_avg = _avg("shares_diluted")
    if net_income_ttm is not None and sh_avg and sh_avg > 0:
        eps_ttm = net_income_ttm / sh_avg

    # total_debt: prefer explicit total_debt, fallback to short + long
    total_debt_calc = (latest.get("short_debt") or 0.0) + (latest.get("long_debt") or 0.0)
    total_debt = latest.get("total_debt") or (total_debt_calc if total_debt_calc > 0 else None)

    out = {
        "cfo_ttm": cfo_ttm,
        "capex_ttm": capex_ttm,
        "sbc_ttm": sbc_ttm,
        "depreciation_ttm": depreciation_ttm,
        "ebit_ttm": ebit_ttm,
        "ebitda_ttm": ebitda_ttm,
        "net_income_ttm": net_income_ttm,
        "interest_expense_ttm": interest_expense_ttm,
        "eps_ttm": eps_ttm,
        "cash": latest.get("cash"),
        "total_debt": total_debt,
        "equity": latest.get("equity"),
        "total_assets": latest.get("total_assets"),
    }

    logger.debug("[TTM] ni=%s cfo=%s capex=%s eps_ttm=%s",
                 net_income_ttm, cfo_ttm, capex_ttm,
                 f"{eps_ttm:.4f}" if eps_ttm is not None else None)
    return out


# ---------------------------------------------------------------------------
# calculateHistoricalPE_DB
# Mirrors calculateHistoricalPE_DB in Extract2.
# Uses close (split-adj), NOT close_adj (dividend-adj).
# MIN_EPS = 0.01
# ---------------------------------------------------------------------------

MIN_EPS: float = 0.01


def calculate_historical_pe(
    quarters: list[dict[str, Any]],
    price_history: list[dict[str, Any]],
) -> dict[str, float | None]:
    """
    Calculate pe_12m, pe_24m, pe_36m from DB price history + quarterly TTM EPS.

    price_history: list of PricesHistory dicts sorted newest-first.
    Each entry must have 'date' (YYYY-MM-DD str) and 'close' (float).

    PE uses close (split-adjusted only), NOT close_adj (dividend-adjusted).
    MIN_EPS = 0.01 (PE is None if eps < MIN_EPS).
    """
    if not price_history:
        logger.warning("[PE-Hist] No price history provided")
        return {"pe_12m": None, "pe_24m": None, "pe_36m": None}

    # Sort prices newest-first for pickMonthEnd
    prices = sorted(price_history, key=lambda p: p["date"], reverse=True)

    # Sort quarters ascending for rolling window
    q_asc = sorted(quarters, key=lambda q: q["periodEnd"])

    # Build monthly EPS TTM map {YYYY-MM: eps}
    monthly_eps_ttm: dict[str, float] = {}
    for i in range(len(q_asc)):
        window = q_asc[max(0, i - 3): i + 1]
        if len(window) == 4:
            ni_vals = [q.get("net_income") for q in window]
            sh_vals = [q.get("shares_diluted") for q in window]
            if all(v is not None for v in ni_vals) and all(v is not None for v in sh_vals):
                ni_total = sum(ni_vals)
                sh_avg = sum(sh_vals) / 4
                if sh_avg > 0:
                    eps = ni_total / sh_avg
                    if eps > 0:
                        key = window[3]["periodEnd"][:7]  # YYYY-MM
                        monthly_eps_ttm[key] = eps
                        logger.debug("[PE-Hist] EPS TTM for %s: %.4f", key, eps)

    def pick_month_end(target_month: date) -> dict | None:
        """Return last trading day of target_month from prices."""
        y, m = target_month.year, target_month.month
        in_month = [
            p for p in prices
            if p["date"][:7] == f"{y:04d}-{m:02d}"
        ]
        if not in_month:
            return None
        return max(in_month, key=lambda p: p["date"])

    def pe_at(months_ago: int) -> float | None:
        today = date.today()
        # compute target month by subtracting months
        m = today.month - months_ago
        y = today.year + m // 12
        m = m % 12
        if m <= 0:
            m += 12
            y -= 1
        month_key = f"{y:04d}-{m:02d}"

        # Find closest EPS TTM key <= month_key
        keys = sorted(monthly_eps_ttm.keys(), reverse=True)
        eps_key = next((k for k in keys if k <= month_key), None)
        if not eps_key:
            logger.debug("[PE-Hist] No EPS TTM for %dm ago (%s)", months_ago, month_key)
            return None

        eps = monthly_eps_ttm[eps_key]
        if eps < MIN_EPS:
            return None

        # Find month-end price
        target = date(y, m, 1)
        month_end_price = pick_month_end(target)
        if not month_end_price:
            logger.debug("[PE-Hist] No month-end price for %dm ago (%s)", months_ago, month_key)
            return None

        # Use close (split-adj), NOT close_adj
        price = month_end_price.get("close")
        if not price or price <= 0:
            return None

        pe = price / eps
        logger.debug("[PE-Hist] %dm ago: price=%.2f (%s), eps=%.4f, pe=%.2f",
                     months_ago, price, month_end_price["date"], eps, pe)
        return pe

    pe_12m = pe_at(12)
    pe_24m = pe_at(24)
    pe_36m = pe_at(36)

    logger.info("[PE-Hist] PE_12m=%s PE_24m=%s PE_36m=%s",
                f"{pe_12m:.2f}" if pe_12m else None,
                f"{pe_24m:.2f}" if pe_24m else None,
                f"{pe_36m:.2f}" if pe_36m else None)

    return {"pe_12m": pe_12m, "pe_24m": pe_24m, "pe_36m": pe_36m}


# ---------------------------------------------------------------------------
# ALWAYS_UPDATE set (for Metrics upsert)
# Mirrors ALWAYS_UPDATE in Extract2.
# ---------------------------------------------------------------------------

ALWAYS_UPDATE: frozenset[str] = frozenset([
    "eps_ttm", "pe_ttm", "pe_12m", "pe_24m", "pe_36m", "current_pe",
    "cfo_ttm", "capex_ttm", "sbc_ttm", "depreciation_ttm", "ebit_ttm", "ebitda_ttm",
    "net_income_ttm", "cash", "total_debt", "equity", "total_assets",
])
