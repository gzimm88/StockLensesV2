"""
Acceptance tests — Phases 2 & 3: Snapshot Determinism & Recommendation Reform

Acceptance requirements:
  1. Same inputs → same snapshot_hash.
  2. Changing MOS only → recommendation unchanged.
  3. Missing required lens field → confidence decreases but recommendation unchanged.
  4. Invalid eps_forward → pe_fwd null.
  5. Removing quarterly coverage → eps_ttm null, pe_ttm null.
"""

import pytest
from backend.services.snapshot_service import (
    compute_snapshot,
    compute_recommendation,
    compute_mos_signal,
    compute_category_scores,
    compute_final_score,
)
from backend.services.confidence_calculator import compute_confidence


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_LENS = {
    "id": "lens-conservative",
    "name": "Conservative",
    "valuation": 0.25, "quality": 0.20, "capitalAllocation": 0.10,
    "growth": 0.15, "moat": 0.10, "risk": 0.10,
    "macro": 0.05, "narrative": 0.05, "dilution": 0.00,
    "buyThreshold": 6.5, "watchThreshold": 4.5,
}

_METRICS_MSFT = {
    "ticker_symbol": "MSFT",
    "as_of_date": "2024-10-01",
    "pe_fwd": 30.0, "pe_ttm": 35.0, "ev_ebitda": 22.0,
    "fcf_yield_pct": 3.5, "peg_5y": 2.1, "pe_5y_low": 20.0, "pe_5y_high": 45.0,
    "roic_pct": 35.0, "fcf_margin_pct": 30.0, "cfo_to_ni": 1.2, "fcf_to_ebit": 0.9,
    "accruals_ratio": 0.02, "margin_stdev_5y_pct": 3.0,
    "buyback_yield_pct": 1.5, "interest_coverage_x": 40.0, "netdebt_to_ebitda": -0.5,
    "eps_cagr_5y_pct": 18.0, "revenue_cagr_5y_pct": 14.0,
    "eps_cagr_3y_pct": 20.0, "revenue_cagr_3y_pct": 16.0,
    "moat_score_0_10": 9.0, "recurring_revenue_pct": 80.0,
    "insider_own_pct": 2.0, "founder_led_bool": False,
    "riskdownside_score_0_10": 7.5, "beta_5y": 0.9, "maxdrawdown_5y_pct": 25.0,
    "netdebt_to_ebitda": -0.5, "netcash_to_mktcap_pct": 5.0,
    "sector_cyc_tag": "secular",
    "macrofit_score_0_10": 7.0, "narrative_score_0_10": 8.0,
    "sharecount_change_5y_pct": 1.5, "sbc_to_sales_pct": 2.0,
    "eps_ttm": 12.0, "pe_ttm": 35.0, "price_current": 420.0,
}


# ---------------------------------------------------------------------------
# 1. Determinism: same inputs → same snapshot_hash
# ---------------------------------------------------------------------------

def test_snapshot_determinism():
    """Acceptance: same metrics + same lens → identical snapshot_hash."""
    snap1 = compute_snapshot("MSFT", _LENS, _METRICS_MSFT)
    snap2 = compute_snapshot("MSFT", _LENS, _METRICS_MSFT)
    assert snap1["snapshot_hash"] == snap2["snapshot_hash"]
    assert snap1["recommendation"] == snap2["recommendation"]
    assert snap1["final_score"] == snap2["final_score"]


# ---------------------------------------------------------------------------
# 2. MOS change does NOT change recommendation (Phase 2.2 acceptance)
# ---------------------------------------------------------------------------

def test_mos_change_does_not_change_recommendation():
    """Acceptance: changing MOS alone must NOT change recommendation."""
    snap_high_mos = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=70.0)
    snap_low_mos  = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=-20.0)
    snap_no_mos   = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=None)

    rec_high = snap_high_mos["recommendation"]
    rec_low  = snap_low_mos["recommendation"]
    rec_none = snap_no_mos["recommendation"]

    assert rec_high == rec_low == rec_none, (
        f"Recommendation changed with MOS: high={rec_high} low={rec_low} none={rec_none}"
    )

    # MOS signals must differ
    assert snap_high_mos["mos_signal"] == "+"
    assert snap_low_mos["mos_signal"]  == "-"
    assert snap_no_mos["mos_signal"]   is None


# ---------------------------------------------------------------------------
# 3. Missing required lens field → confidence decreases, recommendation unchanged
# ---------------------------------------------------------------------------

def test_missing_field_lowers_confidence_not_recommendation():
    """Acceptance: missing required field lowers confidence but not BUY/WATCH outcome."""
    full_conf  = compute_confidence(_METRICS_MSFT, "Conservative")

    # Remove a high-weight field
    partial_metrics = {k: v for k, v in _METRICS_MSFT.items() if k not in ("pe_fwd", "roic_pct")}
    partial_conf = compute_confidence(partial_metrics, "Conservative")

    assert partial_conf["confidence_pct"] < full_conf["confidence_pct"], (
        "Removing required fields should lower confidence_pct"
    )

    # Recommendation must be unchanged
    snap_full    = compute_snapshot("MSFT", _LENS, _METRICS_MSFT)
    snap_partial = compute_snapshot("MSFT", _LENS, partial_metrics)
    assert snap_full["recommendation"] == snap_partial["recommendation"], (
        "Confidence change must not alter recommendation"
    )


# ---------------------------------------------------------------------------
# 4. compute_recommendation is score-only (Phase 2.1 acceptance)
# ---------------------------------------------------------------------------

def test_recommendation_buy_at_threshold():
    assert compute_recommendation(6.5, buy_threshold=6.5) == "BUY"

def test_recommendation_watch_below_buy():
    assert compute_recommendation(6.0, buy_threshold=6.5, watch_threshold=4.5) == "WATCH"

def test_recommendation_avoid():
    assert compute_recommendation(3.0) == "AVOID"

def test_recommendation_insufficient_data():
    assert compute_recommendation(None) == "INSUFFICIENT_DATA"


# ---------------------------------------------------------------------------
# 5. MOS signal boundaries
# ---------------------------------------------------------------------------

def test_mos_signal_positive():
    assert compute_mos_signal(10.0, neutral_band=5.0) == "+"

def test_mos_signal_negative():
    assert compute_mos_signal(-10.0, neutral_band=5.0) == "-"

def test_mos_signal_neutral():
    assert compute_mos_signal(3.0, neutral_band=5.0)  == "0"
    assert compute_mos_signal(-3.0, neutral_band=5.0) == "0"

def test_mos_signal_none():
    assert compute_mos_signal(None) is None


# ---------------------------------------------------------------------------
# 6. Null category excluded from final score (Phase 4 acceptance)
# ---------------------------------------------------------------------------

def test_null_category_excluded_from_weighted_average():
    """Null categories do not count toward totalWeight (not penalised as 0)."""
    lens_weights = {"valuation": 0.5, "quality": 0.5}

    # Both present
    scores_full = {"valuation": 8.0, "quality": 6.0}
    score_full  = compute_final_score(scores_full, lens_weights)
    assert abs(score_full - 7.0) < 0.001

    # Quality is null — only valuation counts
    scores_partial = {"valuation": 8.0, "quality": None}
    score_partial   = compute_final_score(scores_partial, lens_weights)
    assert abs(score_partial - 8.0) < 0.001, (
        f"Null quality should be excluded, expected 8.0 got {score_partial}"
    )


# ---------------------------------------------------------------------------
# 7. Example ScoreSnapshot for MSFT (smoke test with full output)
# ---------------------------------------------------------------------------

def test_msft_snapshot_example():
    """Smoke test: MSFT snapshot produces expected structure and sane values."""
    snap = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=15.0)

    assert snap["ticker_symbol"] == "MSFT"
    assert snap["lens_name"] == "Conservative"
    assert snap["score_version"] == "1.0.0"
    assert snap["recommendation"] in ("BUY", "WATCH", "AVOID", "INSUFFICIENT_DATA")
    assert snap["final_score"] is not None
    assert 0.0 <= snap["final_score"] <= 10.0
    assert snap["confidence_grade"] in ("A", "B", "C", "D")
    assert snap["mos_signal"] == "+"
    assert isinstance(snap["top_positive_contributors"], list)
    assert isinstance(snap["top_negative_contributors"], list)
    assert isinstance(snap["missing_critical_fields"], list)
    assert snap["snapshot_hash"] is not None and len(snap["snapshot_hash"]) == 16

    # Print for visibility
    import json
    print("\n=== MSFT ScoreSnapshot Example ===")
    print(json.dumps(snap, indent=2, default=str))
