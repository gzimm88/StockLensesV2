import pytest

from backend.services.portfolio_engine import _clamp_sell_execution


def test_sell_is_clamped_to_available_with_warning():
    warnings = []
    correction_events = []
    executed_shares, executed_cost = _clamp_sell_execution(
        ticker="VOO",
        row_id=91,
        requested_shares=58.51,
        available_shares=58.50603,
        price=100.0,
        warnings=warnings,
        allow_clamp=True,
        correction_events=correction_events,
    )

    assert abs(executed_shares - 58.50603) < 1e-9
    assert abs(executed_cost - (58.50603 * 100.0)) < 1e-9
    assert warnings
    assert "sell clamped" in warnings[0]
    assert correction_events and correction_events[0]["reason"] == "sell_clamp_lt_1pct"


def test_sell_with_zero_available_is_hard_error():
    warnings = []
    correction_events = []
    with pytest.raises(Exception):
        _clamp_sell_execution(
            ticker="TEST",
            row_id=10,
            requested_shares=1.0,
            available_shares=0.0,
            price=50.0,
            warnings=warnings,
            allow_clamp=True,
            correction_events=correction_events,
        )


def test_sell_overage_at_or_above_one_percent_is_hard_error():
    warnings = []
    correction_events = []
    with pytest.raises(Exception):
        _clamp_sell_execution(
            ticker="TEST",
            row_id=11,
            requested_shares=101.0,
            available_shares=100.0,
            price=20.0,
            warnings=warnings,
            allow_clamp=True,
            correction_events=correction_events,
        )


def test_sell_overage_below_one_percent_is_hard_error_when_clamp_disabled():
    warnings = []
    correction_events = []
    with pytest.raises(Exception):
        _clamp_sell_execution(
            ticker="TEST",
            row_id=12,
            requested_shares=100.5,
            available_shares=100.0,
            price=20.0,
            warnings=warnings,
            allow_clamp=False,
            correction_events=correction_events,
        )
