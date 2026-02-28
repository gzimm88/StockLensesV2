from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base, get_db
from backend.main import app
from backend.models import ClosedPosition, FXRate, Portfolio, PriceHistory
from backend.orchestrator.portfolio_orchestrator import (
    create_transaction,
    rebuild_equity_history,
    update_transaction,
)


def _make_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


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


def _seed_portfolio(db: Session, *, name: str = "ClosedPos") -> str:
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


def test_close_position_creates_closed_position_row():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="ClosedCreate")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)

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
        tx_type="SELL",
        quantity=10,
        price=120,
        trade_date=d1,
        currency="USD",
    )
    _seed_price(db, "AAPL", d0, 100)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)

    rows = db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).all()
    assert len(rows) == 1
    row = rows[0]
    assert row.ticker == "AAPL"
    assert float(row.total_cost_basis) == 1000.0
    assert float(row.total_proceeds) == 1200.0
    assert float(row.realized_gain) == 200.0


def test_rebuild_twice_no_duplicate_closed_positions():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="ClosedNoDup")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="BUY",
        quantity=5,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="SELL",
        quantity=5,
        price=110,
        trade_date=d1,
        currency="USD",
    )
    _seed_price(db, "MSFT", d0, 100)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)

    count = db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).count()
    assert count == 1


def test_edit_sell_transaction_updates_closed_positions():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="ClosedEdit")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="GOOGL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    sell_tx = create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="GOOGL",
        tx_type="SELL",
        quantity=10,
        price=110,
        trade_date=d1,
        currency="USD",
    )
    _seed_price(db, "GOOGL", d0, 100)
    _seed_price(db, "GOOGL", d1, 110)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    assert db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).count() == 1

    update_transaction(
        db,
        transaction_id=sell_tx["id"],
        ticker="GOOGL",
        tx_type="SELL",
        quantity=5,
        price=110,
        trade_date=d1,
        currency="USD",
    )
    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)

    assert db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).count() == 0


def test_partial_sell_does_not_create_closed_position():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="ClosedPartial")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="CRM",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="CRM",
        tx_type="SELL",
        quantity=2,
        price=120,
        trade_date=d1,
        currency="USD",
    )
    _seed_price(db, "CRM", d0, 100)
    _seed_price(db, "CRM", d1, 120)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
    assert db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).count() == 0


def test_fx_close_stores_fx_component():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="ClosedFX")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)

    _seed_fx(db, "EUR", d0, 1.2)
    _seed_fx(db, "EUR", d1, 1.1)

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
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="SAP",
        tx_type="SELL",
        quantity=10,
        price=100,
        trade_date=d1,
        currency="EUR",
    )
    _seed_price(db, "SAP", d0, 100)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)

    row = db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).one()
    assert float(row.realized_gain) == -100.0
    assert float(row.fx_component) == -100.0


def test_closed_positions_endpoint_returns_rows():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="ClosedEndpoint")
        d0 = date.today() - timedelta(days=3)
        d1 = date.today() - timedelta(days=2)
        create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="V",
            tx_type="BUY",
            quantity=1,
            price=100,
            trade_date=d0,
            currency="USD",
        )
        create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="V",
            tx_type="SELL",
            quantity=1,
            price=110,
            trade_date=d1,
            currency="USD",
        )
        _seed_price(db, "V", d0, 100)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)

        resp = client.get(f"/portfolio/{portfolio_id}/closed-positions")
        assert resp.status_code == 200
        payload = resp.json()["data"]["closed_positions"]
        assert len(payload) == 1
        assert payload[0]["ticker"] == "V"
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_patch_edit_closed_position_updates_row_values():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="ClosedPatchUpdate")
        d0 = date.today() - timedelta(days=3)
        d1 = date.today() - timedelta(days=2)
        buy = create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="AAPL",
            tx_type="BUY",
            quantity=10,
            price=100,
            trade_date=d0,
            currency="USD",
        )
        sell = create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="AAPL",
            tx_type="SELL",
            quantity=10,
            price=110,
            trade_date=d1,
            currency="USD",
        )
        _seed_price(db, "AAPL", d0, 100)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
        before = db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).one()
        assert float(before.realized_gain) == 100.0

        patched = client.patch(
            f"/transactions/{sell['id']}",
            json={"quantity": 10, "price": 130, "date": d1.isoformat(), "currency": "USD"},
        )
        assert patched.status_code == 200
        after = db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).one()
        assert float(after.realized_gain) == 300.0
    finally:
        db.close()
        app.dependency_overrides.clear()


def test_patch_edit_can_invalidate_closure_and_remove_closed_row():
    client, SessionLocal = _build_client()
    db: Session = SessionLocal()
    try:
        portfolio_id = _seed_portfolio(db, name="ClosedPatchInvalidate")
        d0 = date.today() - timedelta(days=3)
        d1 = date.today() - timedelta(days=2)
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
        sell = create_transaction(
            db,
            portfolio_id=portfolio_id,
            ticker="AAPL",
            tx_type="SELL",
            quantity=10,
            price=110,
            trade_date=d1,
            currency="USD",
        )
        _seed_price(db, "AAPL", d0, 100)
        _seed_price(db, "AAPL", d1, 110)
        rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True)
        assert db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).count() == 1

        patched = client.patch(
            f"/transactions/{sell['id']}",
            json={"quantity": 2, "price": 110, "date": d1.isoformat(), "currency": "USD"},
        )
        assert patched.status_code == 200
        assert db.query(ClosedPosition).filter(ClosedPosition.portfolio_id == portfolio_id).count() == 0
    finally:
        db.close()
        app.dependency_overrides.clear()
