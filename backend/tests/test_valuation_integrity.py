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
    create_transaction,
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


def test_missing_price_fails_deterministically():
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

    with pytest.raises(PortfolioEngineError, match="Missing price for ticker MSFT"):
        rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=3)


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


def test_no_floating_drift():
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
