from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.models import Portfolio, PricesHistory
from backend.orchestrator.portfolio_orchestrator import (
    create_corporate_action,
    create_transaction,
    get_portfolio_equity_history,
    rebuild_equity_history,
    update_portfolio_settings,
)
from backend.services.portfolio_engine import PortfolioEngineError


def _make_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _seed_portfolio(db: Session, *, name: str = "EquityHistory") -> str:
    row = Portfolio(
        id=str(uuid.uuid4()),
        name=name,
        base_currency="USD",
        owner_id="local",
        is_deleted=False,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    return row.id


def _seed_price(db: Session, ticker: str, d: date, close: float) -> None:
    db.add(
        PricesHistory(
            id=str(uuid.uuid4()),
            ticker=ticker,
            date=d,
            close=close,
            close_adj=close,
            open=close,
            high=close,
            low=close,
            volume=1000,
            source="seed",
            as_of_date=d,
        )
    )
    db.commit()


def test_equity_history_deterministic_same_inputs():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="Deterministic")
    d0 = date.today() - timedelta(days=2)
    d1 = date.today() - timedelta(days=1)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    _seed_price(db, "AAPL", d0, 101)
    _seed_price(db, "AAPL", d1, 102)

    first = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    second = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    assert first["rows_written"] == second["rows_written"]
    history_first = get_portfolio_equity_history(db, portfolio_id, range_label="ALL", build_version=first["build_version"])
    history_second = get_portfolio_equity_history(db, portfolio_id, range_label="ALL", build_version=second["build_version"])
    assert history_first["series"] == history_second["series"]


def test_strict_missing_price_fails():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="StrictFail")
    d0 = date.today() - timedelta(days=1)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="BUY",
        quantity=1,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    with pytest.raises(PortfolioEngineError, match="Missing required market inputs"):
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)


def test_nonstrict_missing_price_skips_day_without_fill():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="NonStrictSkip")
    d0 = date.today() - timedelta(days=2)
    d1 = date.today() - timedelta(days=1)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AMZN",
        tx_type="BUY",
        quantity=2,
        price=50,
        trade_date=d0,
        currency="USD",
    )
    _seed_price(db, "AMZN", d1, 60)
    out = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=False)
    assert out["rows_written"] == 1
    history = get_portfolio_equity_history(db, portfolio_id, range_label="ALL")
    assert [r["date"] for r in history["series"]] == [d1.isoformat()]


def test_incremental_mutation_guard_and_force_rebuild_new_version():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="MutationGuard")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="CRM",
        tx_type="BUY",
        quantity=1,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    _seed_price(db, "CRM", d0, 100)
    _seed_price(db, "CRM", d1, 101)
    first = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="CRM",
        tx_type="BUY",
        quantity=1,
        price=90,
        trade_date=d0,
        currency="USD",
    )
    with pytest.raises(PortfolioEngineError, match="Historical inputs changed before last equity history date"):
        rebuild_equity_history(db, portfolio_id, mode="incremental", force=False, strict=True)

    forced = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    assert forced["build_version"] > first["build_version"]


def test_day_change_dividend_and_realized_rules():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DividendRules")
    d0 = date.today() - timedelta(days=4)
    d1 = date.today() - timedelta(days=3)
    d2 = date.today() - timedelta(days=2)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="V",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    create_corporate_action(
        db,
        portfolio_id=portfolio_id,
        ticker="V",
        action_type="DIVIDEND",
        effective_date=d1,
        cash_amount=20.0,
    )
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="V",
        tx_type="SELL",
        quantity=5,
        price=120,
        trade_date=d2,
        currency="USD",
    )
    _seed_price(db, "V", d0, 100)
    _seed_price(db, "V", d1, 101)
    _seed_price(db, "V", d2, 102)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    history = get_portfolio_equity_history(db, portfolio_id, range_label="ALL")
    assert len(history["series"]) == 3
    assert history["series"][1]["day_change_value"] == 30.0
    latest_equity = history["series"][-1]["total_equity"]
    # Buy -1000, dividend +20, sell +600, remaining 5*102 => 130
    assert latest_equity == 130.0


def test_non_base_valuation_uses_close_fx_execution_fx_only_for_cash():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="FXRules")
    d0 = date.today() - timedelta(days=2)
    d1 = date.today() - timedelta(days=1)
    _seed_price(db, "EURUSD=X", d0, 1.2)
    _seed_price(db, "EURUSD=X", d1, 1.1)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="SAP",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="EUR",
    )
    _seed_price(db, "SAP", d0, 100)
    _seed_price(db, "SAP", d1, 100)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    history = get_portfolio_equity_history(db, portfolio_id, range_label="ALL")
    assert len(history["series"]) == 2
    # cash booked at execution FX 1.2 => -1200; day1 market uses close FX 1.1 => 1100; total=-100
    assert history["series"][-1]["total_equity"] == -100.0
    assert history["series"][-1]["fx_return_component"] == -100.0
    assert history["series"][-1]["market_return_component"] == 0.0


def test_contribution_neutrality_in_net_of_contributions_mode():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="ContributionNeutral")
    d0 = date.today() - timedelta(days=2)
    d1 = date.today() - timedelta(days=1)
    _seed_price(db, "AAPL", d0, 100)
    _seed_price(db, "AAPL", d1, 100)
    update_portfolio_settings(db, portfolio_id, cash_management_mode="ignore_cash")
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=5,
        price=100,
        trade_date=d1,
        currency="USD",
    )
    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    history = get_portfolio_equity_history(
        db,
        portfolio_id,
        range_label="ALL",
        performance_mode="net_of_contributions",
    )
    assert len(history["series"]) == 2
    assert history["series"][1]["day_change_value"] == 500.0
    assert history["series"][1]["net_contribution"] == 500.0
    assert history["series"][1]["plotted_value"] == history["series"][0]["plotted_value"]
