from __future__ import annotations

from datetime import date

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base, get_db
from backend.main import app
from backend.models import PortfolioEquityHistoryBuild, PortfolioTransaction, PricesHistory
from backend.orchestrator.portfolio_orchestrator import create_portfolio


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


def _create_portfolio_id(SessionLocal: sessionmaker, name: str = "Phase2_Test") -> str:
    db: Session = SessionLocal()
    try:
        row = create_portfolio(db, name=name, base_currency="USD")
        return str(row["id"])
    finally:
        db.close()


def _seed_price(db: Session, ticker: str, d: date, close: float) -> None:
    db.add(
        PricesHistory(
            id=f"{ticker}-{d.isoformat()}",
            ticker=ticker,
            date=d,
            close=close,
            close_adj=close,
            open=close,
            high=close,
            low=close,
            volume=100,
            source="seed",
            as_of_date=d,
        )
    )
    db.commit()


def test_transaction_crud_soft_delete_and_list():
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase2_CRUD")

    create_resp = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 10,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert create_resp.status_code == 200
    tx_id = create_resp.json()["data"]["id"]

    list_resp = client.get(f"/portfolios/{portfolio_id}/transactions")
    assert list_resp.status_code == 200
    txs = list_resp.json()["data"]["transactions"]
    assert len(txs) == 1
    assert txs[0]["id"] == tx_id

    delete_resp = client.delete(f"/transactions/{tx_id}")
    assert delete_resp.status_code == 200

    list_after_delete = client.get(f"/portfolios/{portfolio_id}/transactions")
    assert list_after_delete.status_code == 200
    assert list_after_delete.json()["data"]["transactions"] == []

    db: Session = SessionLocal()
    try:
        stored = db.query(PortfolioTransaction).filter(PortfolioTransaction.id == tx_id).first()
        assert stored is not None
        assert stored.is_deleted is True
        assert stored.deleted_at is not None
    finally:
        db.close()
    app.dependency_overrides.clear()


def test_oversell_rejected_and_no_side_effects():
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase2_Oversell")

    buy_resp = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 5,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert buy_resp.status_code == 200

    oversell_resp = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "SELL",
            "quantity": 1000,
            "price": 120,
            "date": "2026-02-02",
            "currency": "USD",
        },
    )
    assert oversell_resp.status_code == 400

    db: Session = SessionLocal()
    try:
        active = (
            db.query(PortfolioTransaction)
            .filter(
                PortfolioTransaction.portfolio_id == portfolio_id,
                PortfolioTransaction.is_deleted == False,
            )
            .all()
        )
        assert len(active) == 1
    finally:
        db.close()
    app.dependency_overrides.clear()


def test_update_creates_new_version_and_soft_deletes_old():
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase2_Update")

    create_resp = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 10,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert create_resp.status_code == 200
    original_id = create_resp.json()["data"]["id"]

    update_resp = client.put(
        f"/transactions/{original_id}",
        json={
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 8,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert update_resp.status_code == 200
    updated = update_resp.json()["data"]
    assert updated["version"] == 2
    assert updated["id"] != original_id

    db: Session = SessionLocal()
    try:
        rows = (
            db.query(PortfolioTransaction)
            .filter(PortfolioTransaction.portfolio_id == portfolio_id)
            .all()
        )
        assert len(rows) == 2
        active = [r for r in rows if not r.is_deleted]
        deleted = [r for r in rows if r.is_deleted]
        assert len(active) == 1
        assert len(deleted) == 1
        assert deleted[0].id == original_id
        assert deleted[0].deleted_at is not None
        assert active[0].version == 2
    finally:
        db.close()
    app.dependency_overrides.clear()


def test_edit_transaction_triggers_equity_history_rebuild():
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase11_Rebuild")
    d0 = date(2026, 2, 1)
    d1 = date(2026, 2, 2)
    db: Session = SessionLocal()
    try:
        _seed_price(db, "AAPL", d0, 100.0)
        _seed_price(db, "AAPL", d1, 110.0)
    finally:
        db.close()

    created = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 10,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert created.status_code == 200
    tx_id = created.json()["data"]["id"]

    db = SessionLocal()
    try:
        builds_after_create = (
            db.query(PortfolioEquityHistoryBuild)
            .filter(PortfolioEquityHistoryBuild.portfolio_id == portfolio_id)
            .order_by(PortfolioEquityHistoryBuild.build_version.asc())
            .all()
        )
        assert len(builds_after_create) == 1
        first_version = builds_after_create[0].build_version
    finally:
        db.close()

    updated = client.put(
        f"/transactions/{tx_id}",
        json={
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 8,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert updated.status_code == 200

    db = SessionLocal()
    try:
        builds_after_update = (
            db.query(PortfolioEquityHistoryBuild)
            .filter(PortfolioEquityHistoryBuild.portfolio_id == portfolio_id)
            .order_by(PortfolioEquityHistoryBuild.build_version.asc())
            .all()
        )
        assert len(builds_after_update) >= 2
        assert builds_after_update[-1].build_version > first_version
    finally:
        db.close()
    app.dependency_overrides.clear()


def test_reprocess_determinism_for_same_transactions(monkeypatch):
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase2_Reprocess")

    created = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 10,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert created.status_code == 200

    counter = {"n": 0}

    async def _fake_run(db, portfolio_id: str, strict: bool = False):
        counter["n"] += 1
        return {
            "run_id": f"run-{counter['n']}",
            "nav": 1000.0,
            "irr": 0.123,
            "input_hash": "stable-hash",
        }

    monkeypatch.setattr("backend.main.run_portfolio_creation_flow", _fake_run)

    first = client.post(f"/portfolios/{portfolio_id}/reprocess")
    second = client.post(f"/portfolios/{portfolio_id}/reprocess")

    assert first.status_code == 200
    assert second.status_code == 200
    first_data = first.json()["data"]
    second_data = second.json()["data"]
    assert first_data["run_id"] != second_data["run_id"]
    assert first_data["nav"] == second_data["nav"]
    assert first_data["irr"] == second_data["irr"]
    assert first_data["input_hash"] == second_data["input_hash"]
    app.dependency_overrides.clear()


def test_patch_transaction_updates_and_forces_full_rebuild():
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase12C_Patch")

    created = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 10,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert created.status_code == 200
    tx_id = created.json()["data"]["id"]

    patched = client.patch(
        f"/transactions/{tx_id}",
        json={
            "quantity": 8,
            "price": 105,
            "date": "2026-02-02",
            "currency": "USD",
        },
    )
    assert patched.status_code == 200
    payload = patched.json()["data"]
    assert payload["transaction"]["quantity"] == 8
    assert payload["transaction"]["price"] == 105
    assert payload["rebuild"]["mode"] == "full"
    assert payload["rebuild"]["forced"] is True
    app.dependency_overrides.clear()


def test_delete_transaction_triggers_rebuild_and_returns_success():
    client, SessionLocal = _build_client()
    portfolio_id = _create_portfolio_id(SessionLocal, name="Phase12C_Delete")

    created = client.post(
        "/transactions",
        json={
            "portfolio_id": portfolio_id,
            "ticker": "AAPL",
            "type": "BUY",
            "quantity": 10,
            "price": 100,
            "date": "2026-02-01",
            "currency": "USD",
        },
    )
    assert created.status_code == 200
    tx_id = created.json()["data"]["id"]

    deleted = client.delete(f"/transactions/{tx_id}")
    assert deleted.status_code == 200
    payload = deleted.json()["data"]
    assert payload["success"] is True
    assert payload["deleted"]["id"] == tx_id
    assert payload["rebuild"]["mode"] == "full"
    assert payload["rebuild"]["forced"] is True
    app.dependency_overrides.clear()
