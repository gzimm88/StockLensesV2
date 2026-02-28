from __future__ import annotations

import uuid
from datetime import date, datetime

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base, get_db
from backend.main import app
from backend.models import Portfolio, PortfolioEquityHistoryRow


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


def _seed_portfolio(db: Session, *, name: str = "TimeReturns") -> str:
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


def _seed_equity_rows(db: Session, portfolio_id: str, points: list[tuple[date, float]]) -> None:
    for d, eq in points:
        db.add(
            PortfolioEquityHistoryRow(
                id=str(uuid.uuid4()),
                portfolio_id=portfolio_id,
                build_version=1,
                date=d,
                total_equity=eq,
                cash_balance=0.0,
                market_value_total=eq,
                cost_basis_total=eq,
                unrealized_gain_value=0.0,
                realized_gain_value=0.0,
                dividend_cash_value=0.0,
                day_change_value=0.0,
                day_change_percent=0.0,
                net_contribution=0.0,
                market_return_component=0.0,
                fx_return_component=0.0,
                twr_index=1.0,
                input_hash=f"hash-{d.isoformat()}",
                created_at=datetime.utcnow(),
            )
        )
    db.commit()


def _get_time_returns(client: TestClient, portfolio_id: str) -> dict:
    resp = client.get(f"/portfolio/{portfolio_id}/time-returns")
    assert resp.status_code == 200
    return resp.json()["data"]


def test_time_returns_basic_increasing_equity_curve():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="TimeReturnsBasic")
        _seed_equity_rows(
            db,
            portfolio_id,
            [
                (date(2026, 1, 2), 100.0),
                (date(2026, 1, 20), 110.0),
                (date(2026, 2, 10), 120.0),
            ],
        )
        data = _get_time_returns(client, portfolio_id)
        assert data["since_inception_return_pct"] == 20.0
        assert data["ytd_return_pct"] == 20.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_time_returns_ytd_boundary_uses_latest_year_start():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="TimeReturnsYTD")
        _seed_equity_rows(
            db,
            portfolio_id,
            [
                (date(2025, 12, 31), 100.0),
                (date(2026, 1, 2), 110.0),
                (date(2026, 2, 10), 121.0),
            ],
        )
        data = _get_time_returns(client, portfolio_id)
        assert data["since_inception_return_pct"] == 21.0
        assert data["ytd_return_pct"] == 10.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_time_returns_one_year_only_when_data_available():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_short = _seed_portfolio(db, name="TimeReturnsShort")
        _seed_equity_rows(
            db,
            portfolio_short,
            [
                (date(2026, 1, 2), 100.0),
                (date(2026, 2, 10), 120.0),
            ],
        )
        short_data = _get_time_returns(client, portfolio_short)
        assert short_data["one_year_return_pct"] is None

        portfolio_long = _seed_portfolio(db, name="TimeReturnsLong")
        _seed_equity_rows(
            db,
            portfolio_long,
            [
                (date(2024, 1, 1), 100.0),
                (date(2025, 1, 2), 150.0),
            ],
        )
        long_data = _get_time_returns(client, portfolio_long)
        assert long_data["one_year_return_pct"] == 50.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_time_returns_deterministic_rerun_same_response():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="TimeReturnsDeterministic")
        _seed_equity_rows(
            db,
            portfolio_id,
            [
                (date(2024, 1, 1), 100.0),
                (date(2026, 2, 10), 160.0),
            ],
        )
        first = _get_time_returns(client, portfolio_id)
        second = _get_time_returns(client, portfolio_id)
        assert first == second
    finally:
        db.close()
        app.dependency_overrides.clear()
