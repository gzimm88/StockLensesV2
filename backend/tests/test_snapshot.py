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
    _score_growth,
    _score_moat,
    _score_risk,
    _score_quality,
    _score_capital_allocation,
    _score_dilution,
    _score_valuation,
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
# 7. compute_recommendation is invariant to mos_pct (direct unit test)
# ---------------------------------------------------------------------------

def test_recommendation_invariant_to_mos_pct():
    """
    compute_recommendation accepts only final_score and thresholds — no MOS param.
    Passing different mos_pct values into compute_snapshot must never change
    the recommendation field.
    """
    # Function-level: compute_recommendation signature has no MOS parameter
    assert compute_recommendation(7.0) == "BUY"
    assert compute_recommendation(5.0) == "WATCH"
    assert compute_recommendation(3.0) == "AVOID"

    # Snapshot-level: extreme positive, extreme negative, and absent MOS
    snap_pos = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=+80.0)
    snap_neg = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=-80.0)
    snap_nil = compute_snapshot("MSFT", _LENS, _METRICS_MSFT, mos_pct=None)

    assert snap_pos["recommendation"] == snap_neg["recommendation"] == snap_nil["recommendation"], (
        "Recommendation must be identical regardless of mos_pct value"
    )

    # MOS signals must differ — MOS is recorded but never gates recommendation
    assert snap_pos["mos_signal"] == "+"
    assert snap_neg["mos_signal"] == "-"
    assert snap_nil["mos_signal"] is None


# ---------------------------------------------------------------------------
# 8. Scoring calibration: growth — high CAGR with modest deceleration
# ---------------------------------------------------------------------------

def test_growth_high_cagr_scores_appropriately():
    """
    A company with 16% EPS CAGR and 12% revenue CAGR (e.g. Google) should
    score ≥7.0 on growth, even with slight deceleration in the 3Y window.

    OLD formula: avg(8, 6, 2.75, 6) = 5.69  — deceleration sub crushed it.
    NEW formula: w-avg(8, 8, 5.65, 7) ≈ 7.50 — appropriate for scale.
    """
    m = {
        "eps_cagr_5y_pct": 16.0, "revenue_cagr_5y_pct": 12.0,
        "eps_cagr_3y_pct": 11.0, "revenue_cagr_3y_pct": 12.5,
    }
    score = _score_growth(m)
    assert score is not None, "Growth score must not be null with full CAGR data"
    assert score >= 7.0, (
        f"16% EPS CAGR + 12% rev CAGR with minor decel should score ≥7.0, got {score:.2f}"
    )


def test_growth_acceleration_bonus():
    """Accelerating growth (3Y > 5Y) should score higher than decelerating growth."""
    accel = {"eps_cagr_5y_pct": 14.0, "revenue_cagr_5y_pct": 10.0,
             "eps_cagr_3y_pct": 18.0, "revenue_cagr_3y_pct": 14.0}
    decel = {"eps_cagr_5y_pct": 14.0, "revenue_cagr_5y_pct": 10.0,
             "eps_cagr_3y_pct": 10.0, "revenue_cagr_3y_pct": 7.0}
    assert _score_growth(accel) > _score_growth(decel)


# ---------------------------------------------------------------------------
# 9. Scoring calibration: moat — base score dominates
# ---------------------------------------------------------------------------

def test_moat_base_score_dominates():
    """
    A company with moat_score=8 (e.g. Google, ASML) should score ≥5.5 on moat
    regardless of low insider ownership — structural moat is the anchor.

    OLD: avg(8, 3.0, 1.0) = 4.03 for ASML (near-monopoly on EUV).
    NEW: w-avg(8, 3.0, 2.0) with [0.55,0.30,0.15] = 5.60.
    """
    m_asml_like = {
        "moat_score_0_10": 8.0,
        "recurring_revenue_pct": 30.0,
        "insider_own_pct": 0.01,
        "founder_led_bool": True,
    }
    score = _score_moat(m_asml_like)
    assert score is not None
    assert score >= 5.5, (
        f"moat_base=8 (near-monopoly) should score ≥5.5 even with low insider, got {score:.2f}"
    )


def test_moat_google_like():
    """Google-like moat inputs should score ≥6.5."""
    m = {
        "moat_score_0_10": 8.0,
        "recurring_revenue_pct": 45.0,
        "insider_own_pct": 6.65,
        "founder_led_bool": True,
    }
    score = _score_moat(m)
    assert score is not None
    assert score >= 6.5, f"Google-like moat should score ≥6.5, got {score:.2f}"


# ---------------------------------------------------------------------------
# 10. Scoring calibration: risk — maxdrawdown fix (was always 0 for real stocks)
# ---------------------------------------------------------------------------

def test_risk_maxdrawdown_nonzero_for_quality_stock():
    """
    Bug: old formula `_clamp(10 - abs(35)) = 0` made sub_dd = 0 for any stock
    with drawdown > 10%.  With the fix, a 35% drawdown (typical for quality
    large-caps in a correction year) should contribute positively.
    """
    m = {
        "riskdownside_score_0_10": 7.5,
        "maxdrawdown_5y_pct": 35.0,
        "beta_5y": 1.0,
        "sector_cyc_tag": "secular",
    }
    score = _score_risk(m)
    assert score is not None
    assert score >= 5.5, (
        f"Quality stock with 35% drawdown should score ≥5.5 on risk, got {score:.2f}"
    )


def test_risk_high_drawdown_penalised():
    """A 70% drawdown (e.g. NFLX 2022) should score lower than a 30% drawdown."""
    m_stable  = {"riskdownside_score_0_10": 7.0, "maxdrawdown_5y_pct": 30.0,
                 "beta_5y": 0.9, "sector_cyc_tag": "secular"}
    m_volatile = {"riskdownside_score_0_10": 7.0, "maxdrawdown_5y_pct": 70.0,
                  "beta_5y": 1.4, "sector_cyc_tag": "growth"}
    assert _score_risk(m_stable) > _score_risk(m_volatile)


# ---------------------------------------------------------------------------
# 11. Example ScoreSnapshot for MSFT (smoke test with full output)
# ---------------------------------------------------------------------------

def test_msft_snapshot_example():
    """Smoke test: MSFT snapshot produces expected structure and sane values (calibrated scorers)."""
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


# ---------------------------------------------------------------------------
# 12. Quality calibration tests
# ---------------------------------------------------------------------------

def test_quality_accruals_null_is_excluded_not_inflated():
    """
    Acceptance: missing accruals_ratio must NOT inflate quality score.
    OLD: null accruals → 10 (assumed clean). NEW: null → excluded from avg.
    """
    full = {"roic_pct": 20.0, "fcf_margin_pct": 15.0, "cfo_to_ni": 1.0,
            "fcf_to_ebit": 0.8, "accruals_ratio": 0.05, "margin_stdev_5y_pct": 4.0}
    missing_accruals = {k: v for k, v in full.items() if k != "accruals_ratio"}

    score_full = _score_quality(full)
    score_missing = _score_quality(missing_accruals)

    # Missing should NOT give a higher score than having a clean-but-nonzero accruals ratio
    assert score_missing is not None, "Quality must be computable without accruals"
    assert score_full is not None
    # Both should be similar (missing excluded, not rewarded)
    assert abs(score_missing - score_full) < 2.0, (
        f"Missing accruals should not inflate by >2 pts: full={score_full:.2f} missing={score_missing:.2f}"
    )


def test_quality_roic_dominant():
    """High ROIC company should outscore low ROIC even with otherwise similar metrics."""
    high_roic = {"roic_pct": 35.0, "fcf_margin_pct": 20.0, "cfo_to_ni": 1.1}
    low_roic  = {"roic_pct": 5.0,  "fcf_margin_pct": 20.0, "cfo_to_ni": 1.1}
    assert _score_quality(high_roic) > _score_quality(low_roic)


# ---------------------------------------------------------------------------
# 13. Capital allocation calibration tests
# ---------------------------------------------------------------------------

def test_capital_allocation_zero_buyback_is_neutral():
    """
    Acceptance: 0% buyback yield should score ~5 (neutral), not 1 (penalised).
    A high-reinvestment company should not be punished for not returning cash.
    """
    m = {"buyback_yield_pct": 0.0, "interest_coverage_x": 20.0}
    score = _score_capital_allocation(m)
    assert score is not None
    assert score >= 5.0, (
        f"0% buyback should score ≥5 (neutral), got {score:.2f}"
    )


def test_capital_allocation_negative_buyback_penalised():
    """Net share issuance (negative buyback yield) should score below neutral."""
    m_issuing  = {"buyback_yield_pct": -3.0, "interest_coverage_x": 20.0}
    m_neutral  = {"buyback_yield_pct":  0.0, "interest_coverage_x": 20.0}
    assert _score_capital_allocation(m_issuing) < _score_capital_allocation(m_neutral)


# ---------------------------------------------------------------------------
# 14. Risk calibration tests
# ---------------------------------------------------------------------------

def test_risk_net_debt_null_is_excluded():
    """
    Bug acceptance: null netdebt_to_ebitda must NOT produce score 10.
    OLD: (3 - null) / 2 = 1.5 → clamp → 10 (treated as best-case zero-debt).
    NEW: null → excluded from weighted average.
    """
    m_with_debt = {"riskdownside_score_0_10": 6.0, "netdebt_to_ebitda": 2.5,
                   "beta_5y": 1.0, "maxdrawdown_5y_pct": 35.0, "sector_cyc_tag": "secular"}
    m_null_debt = {k: v for k, v in m_with_debt.items() if k != "netdebt_to_ebitda"}

    score_with  = _score_risk(m_with_debt)
    score_null  = _score_risk(m_null_debt)

    # A company with verified 2.5x leverage should score strictly lower than
    # one where leverage is unknown (excluded) — not equal or higher
    assert score_with < score_null, (
        f"Known high leverage (2.5x) should score lower than unknown: {score_with:.2f} vs {score_null:.2f}"
    )


def test_risk_net_cash_indebted_penalised():
    """Net-debt company should score lower on net_cash sub than net-cash company."""
    m_cash = {"riskdownside_score_0_10": 7.0, "netcash_to_mktcap_pct": 15.0,
              "maxdrawdown_5y_pct": 30.0, "beta_5y": 0.9, "sector_cyc_tag": "secular"}
    m_debt = {**m_cash, "netcash_to_mktcap_pct": -20.0}
    assert _score_risk(m_cash) > _score_risk(m_debt), (
        "Net-cash company should score higher on risk than net-debt company"
    )


# ---------------------------------------------------------------------------
# 15. Dilution calibration tests
# ---------------------------------------------------------------------------

def test_dilution_buyback_scores_high():
    """Strong buyback programme (5%+ share reduction) should score ≥8."""
    m = {"sharecount_change_5y_pct": 6.0, "sbc_to_sales_pct": 1.5}
    score = _score_dilution(m)
    assert score is not None
    assert score >= 8.0, f"Strong buyback (6% share reduction) should score ≥8, got {score:.2f}"


def test_dilution_heavy_dilution_penalised():
    """Heavy dilution (-8% share change) should score ≤3."""
    m = {"sharecount_change_5y_pct": -8.0, "sbc_to_sales_pct": 8.0}
    score = _score_dilution(m)
    assert score is not None
    assert score <= 3.0, f"Heavy dilution should score ≤3, got {score:.2f}"


def test_dilution_partial_data_scores_from_available():
    """If only one field is available, scorer should still return a score (not null)."""
    m_only_change = {"sharecount_change_5y_pct": 3.0}
    m_only_sbc    = {"sbc_to_sales_pct": 2.0}
    assert _score_dilution(m_only_change) is not None, "Should score with only sharecount_change"
    assert _score_dilution(m_only_sbc)    is not None, "Should score with only sbc_to_sales"


# ---------------------------------------------------------------------------
# 16. Valuation PEG bug regression tests
# ---------------------------------------------------------------------------

def test_valuation_peg_unit_correct():
    """
    Bug regression: PEG was computed as pe / (eps_cagr / 100) instead of pe / eps_cagr.
    With eps_cagr=14.5% and pe_ttm=24.85, the correct PEG is ~1.71 (neutral sub-score 5),
    not 171 (which would clamp to score 1).
    Validate via sub_peg: correct PEG 1.71 should produce valuation score > 4.5
    whereas the buggy PEG 171 would cap it at ~4.1.
    """
    # MSFT-like: good PE fwd, low EV/EBITDA missing, correct PEG ~1.71
    m_correct_peg = {
        "pe_fwd": 21.0, "peg_5y": 1.71, "ev_ebitda": 23.0,
        "fcf_yield_pct": 2.6, "pe_ttm": 24.85, "pe_5y_low": 23.0, "pe_5y_high": 45.0,
    }
    m_buggy_peg = {**m_correct_peg, "peg_5y": 171.0}

    score_correct = _score_valuation(m_correct_peg)
    score_buggy   = _score_valuation(m_buggy_peg)

    assert score_correct > score_buggy, (
        f"Correct PEG (~1.71) should score higher than buggy PEG (171): "
        f"{score_correct:.2f} vs {score_buggy:.2f}"
    )
    assert score_correct > 4.5, (
        f"Correct PEG of 1.71 (sub_peg=5) should give valuation > 4.5, got {score_correct:.2f}"
    )


def test_valuation_peg_low_growth_penalised():
    """PEG > 3 (growth stock trading at premium) should score sub_peg = 1."""
    m = {"pe_fwd": 40.0, "peg_5y": 4.0, "pe_ttm": 40.0, "pe_5y_low": 30.0, "pe_5y_high": 60.0}
    score = _score_valuation(m)
    # With peg=4 (score 1, weight 0.15) and pe_fwd=40 (score 1, weight 0.35),
    # valuation should be quite low
    assert score < 5.0, f"High-premium low-growth stock should score < 5.0, got {score:.2f}"


def test_valuation_sub_hist_near_5y_low_scores_high():
    """
    When TTM PE is near the stock's own 5Y low, sub_hist should score near 10,
    reflecting the stock is historically cheap on a relative basis.
    """
    m = {
        "pe_fwd": 21.0, "peg_5y": 1.71, "fcf_yield_pct": 2.6,
        "pe_ttm": 24.0, "pe_5y_low": 23.0, "pe_5y_high": 45.0,   # near the low
    }
    score = _score_valuation(m)
    # sub_hist = 10 * (45-24)/(45-23) = 10 * 21/22 ≈ 9.55  → boosts overall
    assert score >= 4.5, f"PE near 5Y low should boost valuation to ≥4.5, got {score:.2f}"


# ---------------------------------------------------------------------------
# 17. Growth durability (sub_rec) regression tests
# ---------------------------------------------------------------------------

def test_growth_recurring_revenue_differentiates_quality():
    """
    Bug regression: sub_stage was a second rev-CAGR lookup (redundant).
    After fix, recurring_revenue_pct acts as a growth-durability signal.
    An ADBE-like company (93% recurring) should outscore a MOH-like company
    (40% recurring) even when raw CAGR rates are similar.
    """
    adbe_like = {
        "eps_cagr_5y_pct": 18.45, "revenue_cagr_5y_pct": 9.01,
        "eps_cagr_3y_pct": 9.0,   "revenue_cagr_3y_pct": 10.85,
        "recurring_revenue_pct": 93.0,
    }
    moh_like = {
        "eps_cagr_5y_pct": 12.4,  "revenue_cagr_5y_pct": 11.2,
        "eps_cagr_3y_pct": 9.0,   "revenue_cagr_3y_pct": 12.42,
        "recurring_revenue_pct": 40.0,
    }
    score_adbe = _score_growth(adbe_like)
    score_moh  = _score_growth(moh_like)

    assert score_adbe > score_moh, (
        f"High-recurring growth (ADBE-like, 93%) should outscore low-recurring "
        f"(MOH-like, 40%): {score_adbe:.2f} vs {score_moh:.2f}"
    )
    assert score_adbe - score_moh > 0.5, (
        f"Gap should be meaningful (>0.5): got {score_adbe:.2f} vs {score_moh:.2f}"
    )


def test_growth_rec_null_falls_back_gracefully():
    """
    If recurring_revenue_pct is missing, sub_rec is excluded (weight redistributed).
    A company with 0% recurring should score lower than one where recurring is unknown,
    because 0% is actively bad, while unknown is neutral (excluded, not penalised).
    """
    m_zero_rec    = {"eps_cagr_5y_pct": 15.0, "revenue_cagr_5y_pct": 12.0,
                     "recurring_revenue_pct": 0.0}
    m_without_rec = {"eps_cagr_5y_pct": 15.0, "revenue_cagr_5y_pct": 12.0}

    s_zero    = _score_growth(m_zero_rec)
    s_without = _score_growth(m_without_rec)

    # Both should return a score (not null)
    assert s_zero    is not None, "Should score with 0% recurring"
    assert s_without is not None, "Should score without recurring_revenue_pct"
    # 0% recurring (sub_rec=0) should drag score below unknown (sub_rec excluded)
    assert s_zero < s_without, (
        f"0% recurring (sub_rec=0) should score lower than unknown (excluded): "
        f"{s_zero:.2f} vs {s_without:.2f}"
    )
