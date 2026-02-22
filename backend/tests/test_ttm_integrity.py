"""
Acceptance tests — Phase 1.1: TTM Data Sufficiency

Rules:
  - If quarterly_coverage < 4 → eps_ttm must be null.
  - If 4 valid quarters exist → eps_ttm must compute correctly.
  - TTM fields must never be backfilled from projections.
"""

import pytest
from backend.services.metrics_calculator import build_ttm, run_deterministic_pipeline
from backend.services.metric_resolver import check_ttm_coverage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _quarter(net_income, cfo, capex, ebit, revenue, shares, period_end):
    return {
        "net_income": net_income, "cfo": cfo, "capex": capex,
        "ebit": ebit, "revenue": revenue, "shares_diluted": shares,
        "interest_expense": -500, "depreciation": 1000,
        "stock_based_compensation": 200,
        "period_end": period_end,
        "total_assets": 50_000, "total_debt": 10_000,
        "stockholder_equity": 30_000, "cash": 5_000,
        "shares_outstanding": shares,
    }


FULL_4Q = [
    _quarter(3_000, 4_000, -1_000, 3_500, 20_000, 1_000, "2024-09-30"),
    _quarter(2_800, 3_800, -900,  3_200, 19_000, 1_000, "2024-06-30"),
    _quarter(2_600, 3_600, -850,  3_000, 18_500, 1_010, "2024-03-31"),
    _quarter(2_400, 3_400, -800,  2_800, 18_000, 1_010, "2023-12-31"),
]

ONLY_3Q = FULL_4Q[:3]


# ---------------------------------------------------------------------------
# Test: 3 quarters → eps_ttm must be null (Phase 1.1 acceptance)
# ---------------------------------------------------------------------------

def test_3_quarters_eps_ttm_is_null():
    """Acceptance test: fewer than 4 quarters → eps_ttm = null."""
    TTM, _ = build_ttm(ONLY_3Q)

    # ttm_sum requires all 4 values; with only 3 quarters it should return None
    assert TTM["net_income"] is None, "net_income TTM should be null with < 4 quarters"

    # check_ttm_coverage should flag as insufficient
    report = check_ttm_coverage(ONLY_3Q, ticker="TEST")
    assert report["sufficient"] is False
    assert report["quarter_count"] == 3
    assert len(report["warnings"]) > 0
    assert "eps_ttm" in report["null_fields"]


def test_3_quarters_pipeline_nulls_ttm_metrics():
    """Pipeline with 3 quarters sets partial_ttm=True and eps_ttm=None."""
    payload = run_deterministic_pipeline(
        ticker="TEST",
        quarterly=ONLY_3Q,
        annual=[],
        prices=[],
        spy_prices=[],
    )
    assert payload.get("partial_ttm") is True, "partial_ttm should be True with < 4 quarters"
    assert payload.get("eps_ttm") is None, "eps_ttm should be null with < 4 quarters"


# ---------------------------------------------------------------------------
# Test: 4 valid quarters → eps_ttm must compute correctly (Phase 1.1 acceptance)
# ---------------------------------------------------------------------------

def test_4_quarters_eps_ttm_computed_correctly():
    """Acceptance test: 4 valid quarters → eps_ttm computes correctly."""
    TTM, _ = build_ttm(FULL_4Q)

    total_ni     = sum(q["net_income"] for q in FULL_4Q)       # 10_800
    avg_shares   = sum(q["shares_diluted"] for q in FULL_4Q) / 4  # 1_005

    expected_eps = total_ni / avg_shares

    assert TTM["net_income"] == total_ni
    assert abs(TTM["shares_diluted_avg"] - avg_shares) < 0.01

    # Full pipeline
    prices = [{"date": "2024-10-01", "close_adj": 150.0, "close": 150.0}]
    payload = run_deterministic_pipeline(
        ticker="AAPL",
        quarterly=FULL_4Q,
        annual=[],
        prices=prices,
        spy_prices=[],
    )

    assert payload.get("partial_ttm") is False
    assert payload.get("eps_ttm") is not None
    assert abs(payload["eps_ttm"] - expected_eps) < 0.01


def test_4_quarters_coverage_report_sufficient():
    report = check_ttm_coverage(FULL_4Q, ticker="TEST")
    assert report["sufficient"] is True
    assert report["quarter_count"] == 4
    assert len(report["null_fields"]) == 0
