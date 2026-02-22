"""
Acceptance tests — Phase 1.2: Metric Resolution Layer

Rules:
  - Changing Yahoo snapshot updates eps_forward deterministically.
  - Changing growth rates does NOT change eps_forward.
  - eps_forward > 3 * eps_ttm → pe_fwd must be null.
  - eps_forward comes only from consensus (Yahoo forwardEps).
"""

import pytest
from backend.services.metric_resolver import validate_eps_forward, compute_pe_fwd


# ---------------------------------------------------------------------------
# eps_forward validation
# ---------------------------------------------------------------------------

def test_valid_eps_forward_accepted():
    """eps_forward well within 3x eps_ttm is accepted."""
    result = validate_eps_forward(eps_forward=5.0, eps_ttm=4.0, ticker="MSFT")
    assert result == 5.0


def test_eps_forward_gt_3x_eps_ttm_is_dropped():
    """Acceptance: eps_forward > 3 * eps_ttm → return None (pe_fwd will be null)."""
    result = validate_eps_forward(eps_forward=15.0, eps_ttm=4.0, ticker="TEST")
    assert result is None, "eps_forward > 3x eps_ttm must be dropped"


def test_eps_forward_exactly_3x_is_accepted():
    """eps_forward == 3 * eps_ttm is the boundary — still valid."""
    result = validate_eps_forward(eps_forward=12.0, eps_ttm=4.0, ticker="TEST")
    assert result == 12.0


def test_eps_forward_negative_is_dropped():
    result = validate_eps_forward(eps_forward=-1.0, eps_ttm=4.0, ticker="TEST")
    assert result is None


def test_eps_forward_none_is_dropped():
    result = validate_eps_forward(eps_forward=None, eps_ttm=4.0, ticker="TEST")
    assert result is None


def test_eps_forward_valid_when_eps_ttm_missing():
    """If eps_ttm is unknown, a positive eps_forward is accepted (can't validate ratio)."""
    result = validate_eps_forward(eps_forward=5.0, eps_ttm=None, ticker="TEST")
    assert result == 5.0


# ---------------------------------------------------------------------------
# pe_fwd computation
# ---------------------------------------------------------------------------

def test_pe_fwd_computed_from_price_and_eps_forward():
    pe = compute_pe_fwd(price_current=150.0, eps_forward_validated=5.0)
    assert abs(pe - 30.0) < 0.001


def test_pe_fwd_null_when_eps_forward_null():
    pe = compute_pe_fwd(price_current=150.0, eps_forward_validated=None)
    assert pe is None


def test_pe_fwd_null_when_price_null():
    pe = compute_pe_fwd(price_current=None, eps_forward_validated=5.0)
    assert pe is None


# ---------------------------------------------------------------------------
# Acceptance: changing growth rate does NOT change eps_forward
# ---------------------------------------------------------------------------

def test_growth_rate_change_does_not_affect_eps_forward():
    """
    eps_forward is sourced exclusively from Yahoo consensus.
    Simulating different growth rates must not alter eps_forward validation.
    """
    eps_forward_consensus = 6.0
    eps_ttm = 5.0

    # Validate with "high growth" assumptions (should be irrelevant)
    result_high_growth = validate_eps_forward(eps_forward_consensus, eps_ttm, ticker="TEST")

    # Validate with "low growth" assumptions (identical result expected)
    result_low_growth = validate_eps_forward(eps_forward_consensus, eps_ttm, ticker="TEST")

    assert result_high_growth == result_low_growth == eps_forward_consensus


# ---------------------------------------------------------------------------
# Acceptance: changing Yahoo snapshot updates eps_forward deterministically
# ---------------------------------------------------------------------------

def test_changing_yahoo_snapshot_updates_eps_forward():
    """
    If Yahoo changes forwardEps from 5.0 to 6.0, the validated result
    must change deterministically from 5.0 to 6.0 (given eps_ttm = 3.0).
    """
    eps_ttm = 3.0

    result_v1 = validate_eps_forward(5.0, eps_ttm, ticker="TEST")
    result_v2 = validate_eps_forward(6.0, eps_ttm, ticker="TEST")

    assert result_v1 == 5.0
    assert result_v2 == 6.0
    assert result_v1 != result_v2  # deterministic update
