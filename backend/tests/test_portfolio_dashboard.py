from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base, get_db
from backend.main import app
from backend.models import Portfolio, PricesHistory
from backend.orchestrator.portfolio_orchestrator import (
    create_transaction,
    rebuild_position_ledger,
    rebuild_valuation_snapshot,
)


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


def _seed_portfolio(db: Session, *, name: str = "DashboardParity") -> str:
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
    return str(row.id)


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


def test_dashboard_summary_holdings_and_history_deterministic():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db)
        trade_date = date.today() - timedelta(days=2)
        day1 = date.today() - timedelta(days=1)
        day2 = date.today()

        create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="AAPL",
            tx_type="BUY",
            quantity=10.0,
            price=100.0,
            trade_date=trade_date,
            currency="USD",
        )
        rebuild_position_ledger(db, portfolio_id)

        _seed_price(db, "AAPL", day1, 110.0)
        rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=30)

        _seed_price(db, "AAPL", day2, 120.0)
        rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=30)

        summary_resp = client.get(f"/portfolios/{portfolio_id}/dashboard-summary")
        assert summary_resp.status_code == 200
        summary = summary_resp.json()["data"]
        assert summary["total_equity"] == 200.0
        assert summary["cash_balance"] == -1000.0
        assert summary["market_value_total"] == 1200.0
        assert summary["cost_basis_total"] == 1000.0
        assert summary["day_change_value"] == 100.0
        assert summary["day_change_percent"] == 100.0
        assert summary["unrealized_gain_value"] == 200.0
        assert summary["unrealized_gain_percent"] == 20.0
        assert summary["realized_gain_value"] == 0.0

        holdings_resp = client.get(f"/portfolios/{portfolio_id}/holdings")
        assert holdings_resp.status_code == 200
        holdings = holdings_resp.json()["data"]["holdings"]
        assert len(holdings) == 1
        row = holdings[0]
        assert row["ticker"] == "AAPL"
        assert row["quantity"] == 10.0
        assert row["avg_cost_basis"] == 100.0
        assert row["total_cost_basis"] == 1000.0
        assert row["market_price"] == 120.0
        assert row["market_value"] == 1200.0
        assert row["day_change_value"] == 100.0
        assert row["unrealized_gain_value"] == 200.0
        assert row["realized_gain_value"] == 0.0

        history_resp = client.get(f"/portfolios/{portfolio_id}/equity-history?range=6M")
        assert history_resp.status_code == 200
        series = history_resp.json()["data"]["series"]
        assert len(series) == 2
        assert series[0]["date"] == day1.isoformat()
        assert series[0]["total_equity"] == 100.0
        assert series[1]["date"] == day2.isoformat()
        assert series[1]["total_equity"] == 200.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_equity_history_invalid_range_returns_400():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="BadRange")
        trade_date = date.today() - timedelta(days=1)
        create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="MSFT",
            tx_type="BUY",
            quantity=1.0,
            price=100.0,
            trade_date=trade_date,
            currency="USD",
        )
        rebuild_position_ledger(db, portfolio_id)
        _seed_price(db, "MSFT", date.today(), 101.0)
        rebuild_valuation_snapshot(db, portfolio_id, strict=False, stale_trading_days=30)
        resp = client.get(f"/portfolios/{portfolio_id}/equity-history?range=BAD")
        assert resp.status_code == 400
        assert "Unsupported range" in resp.json()["detail"]
    finally:
        db.close()
        app.dependency_overrides.clear()
