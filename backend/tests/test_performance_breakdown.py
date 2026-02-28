from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base, get_db
from backend.main import app
from backend.models import FXRate, Portfolio, PriceHistory
from backend.orchestrator.portfolio_orchestrator import create_transaction, rebuild_equity_history


def _build_client() -> tuple[TestClient, sessionmaker]:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    Base.metadata.create_all(bind=engine)

    def _override_get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = _override_get_db
    return TestClient(app), SessionLocal


def _seed_portfolio(db: Session, *, name: str = "PerfBreakdown") -> str:
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
        PriceHistory(
            id=str(uuid.uuid4()),
            ticker=ticker,
            datetime_utc=datetime(d.year, d.month, d.day, 20, 0, 0),
            price=close,
            adjusted_price=None,
            source="seed",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()


def _seed_fx(db: Session, quote_currency: str, d: date, rate: float) -> None:
    db.add(
        FXRate(
            id=str(uuid.uuid4()),
            base_currency="USD",
            quote_currency=quote_currency,
            datetime_utc=datetime(d.year, d.month, d.day, 20, 0, 0),
            rate=rate,
            source="seed",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()


def _get_breakdown(client: TestClient, portfolio_id: str) -> dict:
    resp = client.get(f"/portfolio/{portfolio_id}/performance-breakdown")
    assert resp.status_code == 200
    return resp.json()["data"]


def test_performance_breakdown_open_positions_only():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="PerfOpen")
        d0 = date.today() - timedelta(days=2)
        create_transaction(db, portfolio_id=portfolio_id, ticker="AAPL", tx_type="BUY", quantity=10, price=100, trade_date=d0, currency="USD")
        _seed_price(db, "AAPL", d0, 120)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d0)

        data = _get_breakdown(client, portfolio_id)
        assert data["realized_gain"] == 0.0
        assert data["unrealized_gain"] == 200.0
        assert data["dividend_gain"] == 0.0
        assert data["total_gain"] == 200.0
        assert data["total_gain_pct"] == 20.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_performance_breakdown_closed_positions_only():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="PerfClosed")
        d0 = date.today() - timedelta(days=3)
        d1 = date.today() - timedelta(days=2)
        create_transaction(db, portfolio_id=portfolio_id, ticker="MSFT", tx_type="BUY", quantity=10, price=100, trade_date=d0, currency="USD")
        create_transaction(db, portfolio_id=portfolio_id, ticker="MSFT", tx_type="SELL", quantity=10, price=130, trade_date=d1, currency="USD")
        _seed_price(db, "MSFT", d0, 100)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)

        data = _get_breakdown(client, portfolio_id)
        assert data["realized_gain"] == 300.0
        assert data["unrealized_gain"] == 0.0
        assert data["total_gain"] == 300.0
        assert data["total_gain_pct"] == 30.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_performance_breakdown_fx_positions_exposes_fx_gain():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="PerfFX")
        d0 = date.today() - timedelta(days=3)
        d1 = date.today() - timedelta(days=2)
        _seed_fx(db, "EUR", d0, 1.2)
        _seed_fx(db, "EUR", d1, 1.1)
        create_transaction(db, portfolio_id=portfolio_id, ticker="SAP", tx_type="BUY", quantity=10, price=100, trade_date=d0, currency="EUR")
        create_transaction(db, portfolio_id=portfolio_id, ticker="SAP", tx_type="SELL", quantity=10, price=100, trade_date=d1, currency="EUR")
        _seed_price(db, "SAP", d0, 100)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)

        data = _get_breakdown(client, portfolio_id)
        assert data["realized_gain"] == -100.0
        assert data["fx_gain"] == -100.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_performance_breakdown_dividends_included():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="PerfDiv")
        d0 = date.today() - timedelta(days=2)
        create_transaction(db, portfolio_id=portfolio_id, ticker="V", tx_type="BUY", quantity=1, price=100, trade_date=d0, currency="USD")
        create_transaction(db, portfolio_id=portfolio_id, ticker="V", tx_type="Dividend", quantity=0, price=25, trade_date=d0, currency="USD")
        _seed_price(db, "V", d0, 100)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d0)

        data = _get_breakdown(client, portfolio_id)
        assert data["dividend_gain"] == 25.0
        assert data["total_gain"] == 25.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_performance_breakdown_total_gain_consistency():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="PerfConsistency")
        d0 = date.today() - timedelta(days=4)
        d1 = date.today() - timedelta(days=3)
        d2 = date.today() - timedelta(days=2)
        create_transaction(db, portfolio_id=portfolio_id, ticker="AAPL", tx_type="BUY", quantity=10, price=100, trade_date=d0, currency="USD")
        create_transaction(db, portfolio_id=portfolio_id, ticker="AAPL", tx_type="SELL", quantity=5, price=120, trade_date=d1, currency="USD")
        create_transaction(db, portfolio_id=portfolio_id, ticker="AAPL", tx_type="Dividend", quantity=0, price=15, trade_date=d2, currency="USD")
        _seed_price(db, "AAPL", d0, 100)
        _seed_price(db, "AAPL", d1, 120)
        _seed_price(db, "AAPL", d2, 130)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d2)

        data = _get_breakdown(client, portfolio_id)
        total = round(float(data["realized_gain"] + data["unrealized_gain"] + data["dividend_gain"]), 10)
        assert round(float(data["total_gain"]), 10) == total
    finally:
        db.close()
        app.dependency_overrides.clear()
