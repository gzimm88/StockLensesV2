"""
Lens-Weighted Coverage Confidence — Phase 2.3

Replaces the raw "% fields present" approach with a per-lens weighted formula.

Formula:
    confidence_pct = sum(weights of present required fields)
                   / sum(all required weights)
                   * 100

Grades:
    A >= 85%
    B >= 70%
    C >= 50%
    D <  50%

Rules:
- Confidence NEVER changes recommendation (score-only gating).
- Missing required field lowers confidence grade only.
"""

from __future__ import annotations

import math
from typing import Any


# ---------------------------------------------------------------------------
# Per-lens required fields with weights
# ---------------------------------------------------------------------------
# Higher weight = field is more critical for this lens's scoring integrity.

LENS_REQUIRED_FIELDS: dict[str, dict[str, float]] = {
    "Conservative": {
        # Valuation
        "pe_fwd": 1.5,
        "pe_ttm": 1.0,
        "ev_ebitda": 1.2,
        "fcf_yield_pct": 1.2,
        "peg_5y": 0.8,
        "pe_5y_low": 0.8,
        "pe_5y_high": 0.8,
        # Quality
        "roic_pct": 1.5,
        "fcf_margin_pct": 1.2,
        "cfo_to_ni": 1.0,
        "fcf_to_ebit": 0.8,
        "accruals_ratio": 0.8,
        "margin_stdev_5y_pct": 0.8,
        # Capital
        "buyback_yield_pct": 1.0,
        "interest_coverage_x": 1.2,
        "netdebt_to_ebitda": 1.0,
        # Growth
        "eps_cagr_5y_pct": 1.0,
        "revenue_cagr_5y_pct": 1.0,
        "eps_cagr_3y_pct": 0.8,
        # Risk
        "beta_5y": 0.8,
        "maxdrawdown_5y_pct": 0.8,
    },
    "Value Purist": {
        "pe_fwd": 2.0,
        "pe_ttm": 1.5,
        "ev_ebitda": 1.5,
        "fcf_yield_pct": 2.0,
        "peg_5y": 1.0,
        "roic_pct": 1.5,
        "fcf_margin_pct": 1.5,
        "cfo_to_ni": 1.2,
        "netdebt_to_ebitda": 1.5,
        "interest_coverage_x": 1.2,
        "eps_cagr_5y_pct": 1.0,
        "revenue_cagr_5y_pct": 1.0,
        "moat_score_0_10": 1.0,
        "buyback_yield_pct": 1.0,
        "beta_5y": 0.8,
    },
    "Growth/Momentum": {
        "eps_cagr_5y_pct": 2.0,
        "revenue_cagr_5y_pct": 2.0,
        "eps_cagr_3y_pct": 1.5,
        "revenue_cagr_3y_pct": 1.5,
        "recurring_revenue_pct": 1.2,   # now part of Growth score (durability)
        "pe_fwd": 1.0,
        "peg_5y": 1.5,
        "roic_pct": 1.0,
        "fcf_margin_pct": 0.8,
        "moat_score_0_10": 0.8,
        "beta_5y": 0.8,
        "maxdrawdown_5y_pct": 0.8,
    },
    "Asymmetry Hunter": {
        "pe_fwd": 1.0,
        "fcf_yield_pct": 1.5,
        "eps_cagr_5y_pct": 1.0,
        "moat_score_0_10": 1.5,
        "maxdrawdown_5y_pct": 1.5,
        "beta_5y": 1.0,
        "netcash_to_mktcap_pct": 1.5,
        "insider_own_pct": 1.0,
        "founder_led_bool": 1.0,
        "narrative_score_0_10": 1.5,
    },
    "Macro-Thematic": {
        "macrofit_score_0_10": 2.0,
        "narrative_score_0_10": 2.0,
        "beta_5y": 1.0,
        "revenue_cagr_5y_pct": 1.0,
        "eps_cagr_5y_pct": 1.0,
        "pe_fwd": 1.0,
        "sector_cyc_tag": 1.0,
        "moat_score_0_10": 0.8,
    },
    "Quality Compounder": {
        "roic_pct": 2.0,
        "fcf_margin_pct": 1.5,
        "cfo_to_ni": 1.2,
        "fcf_to_ebit": 1.0,
        "accruals_ratio": 1.0,
        "margin_stdev_5y_pct": 1.0,
        "eps_cagr_5y_pct": 1.5,
        "revenue_cagr_5y_pct": 1.0,
        "moat_score_0_10": 1.5,
        "insider_own_pct": 0.8,
        "buyback_yield_pct": 0.8,
        "netdebt_to_ebitda": 1.0,
        "interest_coverage_x": 1.0,
        "pe_fwd": 1.0,
        "peg_5y": 0.8,
    },
    # ------------------------------------------------------------------
    # Famous investor lenses
    # ------------------------------------------------------------------
    "Warren Buffett": {
        # Moat is the primary lens — must be quantified
        "moat_score_0_10": 2.5,
        # Quality of the business (owner earnings proxy)
        "roic_pct": 2.0,
        "fcf_margin_pct": 1.5,
        "fcf_to_ebit": 1.0,
        "cfo_to_ni": 1.0,
        "margin_stdev_5y_pct": 1.0,
        # Capital allocation — disciplined return of capital
        "buyback_yield_pct": 1.2,
        "interest_coverage_x": 1.2,
        "netdebt_to_ebitda": 1.0,
        # Growth — sustainable earnings growth
        "eps_cagr_5y_pct": 1.0,
        "revenue_cagr_5y_pct": 0.8,
        # Valuation — fair price (FCF yield is the owner-earnings check)
        "fcf_yield_pct": 1.5,
        "pe_fwd": 1.0,
        "pe_ttm": 0.8,
        "pe_5y_low": 0.8,
        "pe_5y_high": 0.8,
    },
    "Benjamin Graham": {
        # Valuation — statistical cheapness is paramount
        "pe_fwd": 2.0,
        "pe_ttm": 1.5,
        "fcf_yield_pct": 2.0,
        "ev_ebitda": 1.5,
        "pe_5y_low": 1.2,
        "pe_5y_high": 1.2,
        # Risk / Balance sheet safety — the margin-of-safety backbone
        "netdebt_to_ebitda": 2.0,
        "interest_coverage_x": 1.5,
        "beta_5y": 1.0,
        "maxdrawdown_5y_pct": 1.0,
        "netcash_to_mktcap_pct": 1.0,
        # Quality — earnings stability and accruals (no funny accounting)
        "margin_stdev_5y_pct": 1.5,
        "accruals_ratio": 1.2,
        "cfo_to_ni": 1.0,
        "roic_pct": 1.0,
        # Dilution — net-net discipline
        "sharecount_change_5y_pct": 0.8,
    },
    "Peter Lynch": {
        # Growth — the core GARP criterion
        "eps_cagr_5y_pct": 2.0,
        "revenue_cagr_5y_pct": 1.5,
        "eps_cagr_3y_pct": 1.5,
        "revenue_cagr_3y_pct": 1.0,
        "recurring_revenue_pct": 1.0,   # growth durability signal
        # Valuation — PEG is Lynch's signature metric
        "peg_5y": 2.0,
        "pe_fwd": 1.2,
        "fcf_yield_pct": 1.0,
        # Quality — earnings quality behind the growth
        "roic_pct": 1.0,
        "fcf_margin_pct": 0.8,
        "cfo_to_ni": 0.8,
        # Risk — debt check (especially for cyclicals)
        "netdebt_to_ebitda": 1.0,
        "interest_coverage_x": 0.8,
        # Moat / Narrative — "invest in what you know"
        "moat_score_0_10": 0.8,
        "narrative_score_0_10": 1.0,
    },
}

# Fallback for unknown lens names
_DEFAULT_LENS = "Conservative"


def _is_present(v: Any) -> bool:
    """True if the value is usable (finite number, non-empty string, or bool)."""
    if isinstance(v, bool):
        return True
    if isinstance(v, str):
        return len(v.strip()) > 0
    if isinstance(v, (int, float)):
        return math.isfinite(v)
    return False


def compute_confidence(
    metrics: dict[str, Any],
    lens_name: str = "Conservative",
) -> dict[str, Any]:
    """
    Compute lens-weighted coverage confidence.

    Does NOT affect recommendation — display and audit only.

    Returns:
        {
            "confidence_pct": float,        # 0–100
            "confidence_grade": str,        # A / B / C / D
            "present_fields": [str],
            "missing_fields": [str],
            "total_weight": float,
            "present_weight": float,
        }
    """
    required = LENS_REQUIRED_FIELDS.get(lens_name) or LENS_REQUIRED_FIELDS[_DEFAULT_LENS]

    total_weight = sum(required.values())
    present_weight = 0.0
    present_fields: list[str] = []
    missing_fields: list[str] = []

    for field_name, weight in required.items():
        if _is_present(metrics.get(field_name)):
            present_weight += weight
            present_fields.append(field_name)
        else:
            missing_fields.append(field_name)

    confidence_pct = (present_weight / total_weight * 100) if total_weight > 0 else 0.0

    if confidence_pct >= 85:
        grade = "A"
    elif confidence_pct >= 70:
        grade = "B"
    elif confidence_pct >= 50:
        grade = "C"
    else:
        grade = "D"

    return {
        "confidence_pct": round(confidence_pct, 1),
        "confidence_grade": grade,
        "present_fields": present_fields,
        "missing_fields": missing_fields,
        "total_weight": round(total_weight, 4),
        "present_weight": round(present_weight, 4),
    }
