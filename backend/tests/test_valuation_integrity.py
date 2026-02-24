from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.models import Portfolio, PricesHistory, ValuationSnapshot
from backend.orchestrator.portfolio_orchestrator import (
    create_transaction,
    get_latest_valuation_diff,
    rebuild_position_ledger,
    rebuild_valuation_snapshot,
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


def _seed_portfolio(db: Session, *, name: str = "ValuationTest", base_currency: str = "USD") -> str:
    row = Portfolio(
        id=str(uuid.uuid4()),
        name=name,
        base_currency=base_currency,
        owner_id="local",
        is_deleted=False,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    return row.id


def _seed_price(db: Session, ticker: str, d: date, close: float, source: str = "seed") -> None:
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
            source=source,
            as_of_date=d,
        )
    )
    db.commit()


def test_stale_price_detection():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P4_Stale")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=date.today() - timedelta(days=20),
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)
    _seed_price(db, "AAPL", date.today() - timedelta(days=8), 120.0)

    out = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    assert "AAPL" in out["stale_tickers"]
    assert "AAPL" in out["excluded_tickers"]
    assert float(out["nav"]) == 0.0


def test_missing_price_nonstrict_excluded():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P4_Missing")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="BUY",
        quantity=5,
        price=100,
        trade_date=date.today(),
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)

    out = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
    assert "MSFT" in out["missing_tickers"]
    assert "MSFT" in out["excluded_tickers"]
    assert float(out["nav"]) == 0.0


def test_fx_mismatch_throws_error():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P4_FX")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="SAP",
        tx_type="BUY",
        quantity=2,
        price=50,
        trade_date=date.today(),
        currency="EUR",
    )
    rebuild_position_ledger(db, portfolio_id)
    _seed_price(db, "SAP", date.today(), 60.0)

    with pytest.raises(PortfolioEngineError, match="Missing FX conversion from EUR to USD"):
        rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)


def test_nav_identical_across_two_rebuilds():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P4_Stable")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=date.today(),
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)
    _seed_price(db, "AAPL", date.today(), 150.0)

    first = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
    second = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    assert first["nav"] == second["nav"]
    assert first["input_hash"] == second["input_hash"]
    assert first["valuation_version"] + 1 == second["valuation_version"]


def test_price_snapshot_changes_nav_hash_changes():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P4_HashShift")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=1,
        price=100,
        trade_date=date.today(),
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)
    _seed_price(db, "AAPL", date.today(), 100.0)

    first = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    row = db.query(PricesHistory).filter(PricesHistory.ticker == "AAPL").first()
    assert row is not None
    row.close = 101.0
    row.close_adj = 101.0
    db.commit()

    second = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    assert first["input_hash"] != second["input_hash"]


def test_deterministic_rounding_stability_no_float_drift():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P4_Drift")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="QQQ",
        tx_type="BUY",
        quantity=0.1,
        price=10.0,
        trade_date=date.today(),
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)
    _seed_price(db, "QQQ", date.today(), 0.2)

    first = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
    second = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    assert first["nav"] == second["nav"]
    assert first["nav"] == 0.02


def test_large_portfolio_scaling_simulation():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P5_Large")
    n = 300
    today = date.today()
    for i in range(n):
        ticker = f"T{i:03d}"
        create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker=ticker,
            tx_type="BUY",
            quantity=1,
            price=10.0,
            trade_date=today,
            currency="USD",
        )
        _seed_price(db, ticker, today, 11.0)

    rebuild_position_ledger(db, portfolio_id)
    out = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    assert out["price_snapshot_count"] == n
    assert out["rebuild_duration_ms"] >= 0
    assert out["nav"] == float(n * 11.0)


def test_snapshot_immutability_across_rebuilds():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P5_Immutable")
    today = date.today()
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="IBM",
        tx_type="BUY",
        quantity=2,
        price=100.0,
        trade_date=today,
        currency="USD",
    )
    _seed_price(db, "IBM", today, 110.0)
    rebuild_position_ledger(db, portfolio_id)

    first = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
    first_row = db.query(ValuationSnapshot).filter(ValuationSnapshot.id == first["valuation_snapshot_id"]).first()
    assert first_row is not None
    first_nav = float(first_row.nav)
    first_hash = first_row.input_hash

    second = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
    second_row = db.query(ValuationSnapshot).filter(ValuationSnapshot.id == second["valuation_snapshot_id"]).first()
    assert second_row is not None
    assert first_row.id != second_row.id
    # Old row remains unchanged and therefore immutable for audit.
    reloaded_first = db.query(ValuationSnapshot).filter(ValuationSnapshot.id == first_row.id).first()
    assert reloaded_first is not None
    assert float(reloaded_first.nav) == first_nav
    assert reloaded_first.input_hash == first_hash


def test_valuation_delta_correctness():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P5_Delta")
    today = date.today()
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=90.0,
        trade_date=today,
        currency="USD",
    )
    _seed_price(db, "AAPL", today, 100.0)
    rebuild_position_ledger(db, portfolio_id)
    rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=5,
        price=95.0,
        trade_date=today,
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)
    out = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)

    assert out["nav_delta"] == 500.0
    assert out["transaction_change_component"] == 500.0
    assert out["price_change_component"] == 0.0
    diff = get_latest_valuation_diff(db, portfolio_id)
    assert diff["nav_delta"] == 500.0
    assert diff["holdings_delta"]["AAPL"] == 5.0


def test_strict_mode_failure_enforcement():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P5_Strict")
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="BUY",
        quantity=5,
        price=100,
        trade_date=date.today() - timedelta(days=20),
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)

    with pytest.raises(PortfolioEngineError, match="Missing price for ticker MSFT"):
        rebuild_valuation_snapshot(db, portfolio_id, strict=True, stale_trading_days=3)

    _seed_price(db, "MSFT", date.today() - timedelta(days=10), 110.0)
    with pytest.raises(PortfolioEngineError, match="Stale price for MSFT"):
        rebuild_valuation_snapshot(db, portfolio_id, strict=True, stale_trading_days=3)


def test_hash_mismatch_guard_enforced():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="P5_HashGuard")
    today = date.today()
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=1,
        price=100.0,
        trade_date=today,
        currency="USD",
    )
    rebuild_position_ledger(db, portfolio_id)
    _seed_price(db, "AAPL", today, 150.0)

    first = rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
    row = db.query(ValuationSnapshot).filter(ValuationSnapshot.id == first["valuation_snapshot_id"]).first()
    assert row is not None
    row.nav = 999.0
    db.commit()

    with pytest.raises(PortfolioEngineError, match="Hash mismatch guard failed"):
        rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)
