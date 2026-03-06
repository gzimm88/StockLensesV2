from __future__ import annotations

import uuid
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.models import FXRate, PriceHistory, PricesHistory
from backend.orchestrator.portfolio_orchestrator import (
    PortfolioEngineError,
    create_portfolio,
    create_transaction,
    rebuild_equity_history,
)
from backend.scheduler import market_data_scheduler


def _make_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


def test_price_history_unique_constraint_enforced():
    db = _make_session()
    ts = datetime(2026, 1, 5, 15, 0, 0)
    db.add(
        PriceHistory(
            id=str(uuid.uuid4()),
            ticker="AAPL",
            datetime_utc=ts,
            price=100.0,
            adjusted_price=None,
            source="test",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()
    db.add(
        PriceHistory(
            id=str(uuid.uuid4()),
            ticker="AAPL",
            datetime_utc=ts,
            price=101.0,
            adjusted_price=None,
            source="test",
            created_at=datetime.utcnow(),
        )
    )
    with pytest.raises(IntegrityError):
        db.commit()
    db.close()


def test_fx_rates_unique_constraint_enforced():
    db = _make_session()
    ts = datetime(2026, 1, 5, 15, 0, 0)
    db.add(
        FXRate(
            id=str(uuid.uuid4()),
            base_currency="USD",
            quote_currency="EUR",
            datetime_utc=ts,
            rate=1.08,
            source="test",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()
    db.add(
        FXRate(
            id=str(uuid.uuid4()),
            base_currency="USD",
            quote_currency="EUR",
            datetime_utc=ts,
            rate=1.09,
            source="test",
            created_at=datetime.utcnow(),
        )
    )
    with pytest.raises(IntegrityError):
        db.commit()
    db.close()


def test_scheduler_price_job_idempotent(monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(market_data_scheduler, "SessionLocal", SessionLocal)
    monkeypatch.setattr(market_data_scheduler, "get_active_open_tickers", lambda db: ["AAPL"])
    monkeypatch.setattr(market_data_scheduler, "_fetch_latest_price_from_yahoo", lambda ticker: 123.45)
    monkeypatch.setattr(market_data_scheduler, "get_required_fx_pairs_for_open_positions", lambda db: [("USD", "EUR")])
    monkeypatch.setattr(market_data_scheduler, "_fetch_latest_fx_from_yahoo", lambda base, quote: 1.1)

    run_ts = datetime(2026, 1, 5, 15, 31, tzinfo=timezone.utc)
    first = market_data_scheduler.run_price_fetch_job(run_ts)
    second = market_data_scheduler.run_price_fetch_job(run_ts)

    db = SessionLocal()
    try:
        price_count = db.query(PriceHistory).count()
        fx_count = db.query(FXRate).count()
        assert first["inserted"] == 1
        assert second["inserted"] == 0
        assert first["fx_inserted"] == 1
        assert second["fx_inserted"] == 0
        assert price_count == 1
        assert fx_count == 1
    finally:
        db.close()


def test_scheduler_fx_job_idempotent(monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(market_data_scheduler, "SessionLocal", SessionLocal)
    monkeypatch.setattr(market_data_scheduler, "get_required_fx_pairs_for_open_positions", lambda db: [("USD", "EUR")])
    monkeypatch.setattr(market_data_scheduler, "_fetch_latest_fx_from_yahoo", lambda base, quote: 1.1)

    run_ts = datetime(2026, 1, 5, 15, 31, tzinfo=timezone.utc)
    first = market_data_scheduler.run_fx_fetch_job(run_ts)
    second = market_data_scheduler.run_fx_fetch_job(run_ts)

    db = SessionLocal()
    try:
        count = db.query(FXRate).count()
        assert first["inserted"] == 1
        assert second["inserted"] == 0
        assert count == 1
    finally:
        db.close()


def test_equity_history_rebuild_uses_deterministic_legacy_fallback_when_price_history_missing():
    db = _make_session()
    portfolio_id = create_portfolio(db, "P12A_EQ", "USD")["id"]
    tx_date = date(2026, 2, 1)
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=tx_date,
        currency="USD",
    )

    # Legacy table row should be accepted as deterministic fallback.
    db.add(
        PricesHistory(
            id="legacy-aapl-2026-02-01",
            ticker="AAPL",
            date=tx_date,
            close=150.0,
            close_adj=150.0,
            open=150.0,
            high=150.0,
            low=150.0,
            volume=1000,
            source="legacy",
            as_of_date=tx_date,
        )
    )
    db.commit()

    out_legacy = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    assert out_legacy["rows_written"] >= 1

    # Insert Phase12A row and rebuild still succeeds (primary source takes precedence).
    db.add(
        PriceHistory(
            id=str(uuid.uuid4()),
            ticker="AAPL",
            datetime_utc=datetime(2026, 2, 1, 20, 0, 0),
            price=150.0,
            adjusted_price=None,
            source="test",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()
    out_primary = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    assert out_primary["rows_written"] >= 1
    db.close()
