"""
Metric Resolution Layer — Phase 1.2

Single source of truth for:
  - canonical source per metric
  - fallback order
  - overwrite rule (ALWAYS_UPDATE vs PATCH_ONLY)
  - staleness policy (days)
  - validation guards

Usage:
    from backend.services.metric_resolver import validate_eps_forward, check_ttm_coverage
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)

UpdatePolicy = Literal["ALWAYS_UPDATE", "PATCH_ONLY"]


# ---------------------------------------------------------------------------
# Metric specification
# ---------------------------------------------------------------------------

@dataclass
class MetricSpec:
    field: str
    canonical_source: str
    fallback_sources: list[str] = field(default_factory=list)
    update_policy: UpdatePolicy = "PATCH_ONLY"
    staleness_days: int | None = None
    description: str = ""


# ---------------------------------------------------------------------------
# Registry — one entry per critical metric
# ---------------------------------------------------------------------------

METRIC_REGISTRY: dict[str, MetricSpec] = {
    "price_current": MetricSpec(
        field="price_current",
        canonical_source="prices_history.close_adj (most recent row)",
        fallback_sources=["prices_history.close"],
        update_policy="ALWAYS_UPDATE",
        staleness_days=1,
        description="Most recent adjusted close price.",
    ),
    "eps_ttm": MetricSpec(
        field="eps_ttm",
        canonical_source="financials_history: sum(net_income, 4Q) / avg(shares_diluted, 4Q)",
        fallback_sources=[],
        update_policy="ALWAYS_UPDATE",
        staleness_days=90,
        description=(
            "TTM EPS from quarterly financials. "
            "Null if fewer than 4 valid quarterly records exist. "
            "Never backfilled from projections."
        ),
    ),
    "eps_forward": MetricSpec(
        field="eps_forward",
        canonical_source="Yahoo quoteSummary defaultKeyStatistics.forwardEps (consensus NTM)",
        fallback_sources=[],
        update_policy="ALWAYS_UPDATE",
        staleness_days=30,
        description=(
            "Consensus next-12-month EPS from Yahoo Finance. "
            "Must come from consensus feed — never derived from CAGR or growth-rate projection. "
            "Invalid (set to null) if eps_forward > 3 * eps_ttm."
        ),
    ),
    "pe_ttm": MetricSpec(
        field="pe_ttm",
        canonical_source="computed: price_current / eps_ttm",
        fallback_sources=[],
        update_policy="ALWAYS_UPDATE",
        staleness_days=1,
        description="Trailing PE. Null if eps_ttm is null or <= 0.",
    ),
    "pe_fwd": MetricSpec(
        field="pe_fwd",
        canonical_source="computed: price_current / eps_forward",
        fallback_sources=[],
        update_policy="ALWAYS_UPDATE",
        staleness_days=1,
        description=(
            "Forward PE = current price / validated eps_forward. "
            "Null if eps_forward fails validation."
        ),
    ),
}


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and math.isfinite(v)


# ---------------------------------------------------------------------------
# Validation guards
# ---------------------------------------------------------------------------

def validate_eps_forward(
    eps_forward: float | None,
    eps_ttm: float | None,
    ticker: str = "",
) -> float | None:
    """
    Validate eps_forward from consensus feed.

    Rules:
    - Must be a positive finite number.
    - If eps_forward > 3 * eps_ttm (and eps_ttm > 0) → treat as invalid, return None.
    - Never derived from CAGR projections (caller's responsibility).

    Returns validated eps_forward or None.
    """
    if not _is_num(eps_forward) or eps_forward <= 0:
        logger.debug("[METRIC_RESOLVER] %s: eps_forward=%s is not a positive number. Dropping.", ticker, eps_forward)
        return None

    if _is_num(eps_ttm) and eps_ttm > 0 and eps_forward > 3 * eps_ttm:
        logger.warning(
            "[METRIC_RESOLVER] %s: eps_forward=%.4f is > 3x eps_ttm=%.4f. Treating as invalid.",
            ticker, eps_forward, eps_ttm,
        )
        return None

    return eps_forward


def compute_pe_fwd(
    price_current: float | None,
    eps_forward_validated: float | None,
) -> float | None:
    """
    pe_fwd = price_current / eps_forward (validated).
    Returns None if either input is invalid.
    """
    if not _is_num(price_current) or not _is_num(eps_forward_validated) or eps_forward_validated <= 0:
        return None
    result = price_current / eps_forward_validated
    return result if _is_num(result) else None


# ---------------------------------------------------------------------------
# TTM coverage check — Phase 1.1
# ---------------------------------------------------------------------------

_TTM_FLOW_FIELDS = [
    "net_income", "cfo", "capex", "ebit",
    "revenue", "shares_diluted", "interest_expense",
    "depreciation", "stock_based_compensation",
]


def check_ttm_coverage(
    quarterly: list[dict[str, Any]],
    ticker: str = "",
    required_fields: list[str] | None = None,
) -> dict[str, Any]:
    """
    Check TTM data coverage for the most recent 4 quarters.

    Returns a coverage report:
    {
        "quarter_count": int,       # how many quarterly records available
        "sufficient": bool,         # True if >= 4 quarters
        "field_coverage": {field: n_quarters_present},
        "warnings": [str],
        "null_fields": [str],       # fields that will be forced to null due to < 4Q
    }

    Rules (Phase 1.1):
    - If quarterly_coverage < 4 → eps_ttm, pe_ttm, fcf_ttm, cfo_ttm, capex_ttm,
      ebit_ttm are null. Do NOT backfill with projections.
    - Log explicit warning when coverage is insufficient.
    """
    if required_fields is None:
        required_fields = _TTM_FLOW_FIELDS

    last4 = quarterly[:4]
    quarter_count = len(last4)
    warnings: list[str] = []
    null_fields: list[str] = []

    field_coverage: dict[str, int] = {}
    for f in required_fields:
        present = sum(1 for q in last4 if _is_num(q.get(f)))
        field_coverage[f] = present

    sufficient = quarter_count >= 4

    if not sufficient:
        msg = (
            f"[TTM_INTEGRITY] {ticker}: Only {quarter_count}/4 quarterly records available. "
            f"TTM flow metrics will be null. Do NOT backfill with projections."
        )
        logger.warning(msg)
        warnings.append(msg)
        # All TTM flow fields become null
        null_fields = [
            "eps_ttm", "pe_ttm", "fcf_ttm", "cfo_ttm", "capex_ttm",
            "ebit_ttm", "net_income_ttm", "revenue_ttm", "ebitda_ttm",
        ]
    else:
        # Even with 4Q, individual fields may be partially missing
        for f, count in field_coverage.items():
            if count < 4:
                msg = (
                    f"[TTM_INTEGRITY] {ticker}: field '{f}' has only {count}/4 "
                    f"quarters populated. TTM sum for this field will be null."
                )
                logger.warning(msg)
                warnings.append(msg)

    return {
        "quarter_count": quarter_count,
        "sufficient": sufficient,
        "field_coverage": field_coverage,
        "warnings": warnings,
        "null_fields": null_fields,
    }
