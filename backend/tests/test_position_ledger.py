from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.models import CorporateAction, Portfolio, PortfolioTransaction
from backend.orchestrator.portfolio_orchestrator import (
    create_corporate_action,
    create_transaction,
    rebuild_position_ledger,
    soft_delete_transaction,
    update_transaction,
)


def _make_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    Base.metadata.create_all(bind=engine)
    return SessionLocal()


def _seed_portfolio(db: Session, name: str = "Ledger_Test") -> str:
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


def test_ledger_rebuild_buy_sell_no_shorts():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, "P3_NoShorts")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=date(2026, 2, 1),
        currency="USD",
    )
    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="SELL",
        quantity=4,
        price=120,
        trade_date=date(2026, 2, 2),
        currency="USD",
    )

    snap = rebuild_position_ledger(db, portfolio_id)

    assert round(float(snap["holdings"]["AAPL"]), 6) == 6.0
    assert round(float(snap["basis"]["AAPL"]), 6) == 100.0


def test_ledger_rebuild_deterministic_hash_same_inputs():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, "P3_HashStable")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="BUY",
        quantity=5,
        price=200,
        trade_date=date(2026, 2, 1),
        currency="USD",
    )

    first = rebuild_position_ledger(db, portfolio_id)
    second = rebuild_position_ledger(db, portfolio_id)

    assert first["input_hash"] == second["input_hash"]
    assert first["holdings"] == second["holdings"]
    assert first["basis"] == second["basis"]
    assert first["ledger_version"] + 1 == second["ledger_version"]


def test_split_adjusts_qty_and_avg_cost_correctly():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, "P3_Split")

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="NVDA",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=date(2026, 2, 1),
        currency="USD",
    )
    create_corporate_action(
        db,
        portfolio_id=portfolio_id,
        ticker="NVDA",
        action_type="SPLIT",
        effective_date=date(2026, 2, 3),
        factor=4.0,
        cash_amount=None,
        metadata=None,
    )

    snap = rebuild_position_ledger(db, portfolio_id)
    assert round(float(snap["holdings"]["NVDA"]), 6) == 40.0
    assert round(float(snap["basis"]["NVDA"]), 6) == 25.0


def test_soft_delete_ignored_in_rebuild():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, "P3_SoftDelete")

    created = create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=3,
        price=100,
        trade_date=date(2026, 2, 1),
        currency="USD",
    )
    soft_delete_transaction(db, str(created["id"]))

    snap = rebuild_position_ledger(db, portfolio_id)
    assert snap["holdings"] == {}
    assert snap["basis"] == {}


def test_edit_increments_version_and_changes_hash():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, "P3_EditVersion")

    created = create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=date(2026, 2, 1),
        currency="USD",
    )

    first = rebuild_position_ledger(db, portfolio_id)

    updated = update_transaction(
        db,
        transaction_id=str(created["id"]),
        ticker="AAPL",
        tx_type="BUY",
        quantity=8,
        price=100,
        trade_date=date(2026, 2, 1),
        currency="USD",
    )
    second = rebuild_position_ledger(db, portfolio_id)

    assert int(updated["version"]) == 2
    assert first["input_hash"] != second["input_hash"]


def test_ordering_tie_breaker_stability():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, "P3_TieBreaker")

    created_at = datetime(2026, 2, 1, 10, 0, 0)
    buy = PortfolioTransaction(
        id="00000000-0000-0000-0000-000000000001",
        portfolio_id=portfolio_id,
        security_id=None,
        ticker_symbol_raw="AAPL",
        ticker_symbol_normalized="AAPL",
        tx_type="Buy",
        trade_date=date(2026, 2, 1),
        shares=10.0,
        price=100.0,
        gross_amount=1000.0,
        currency="USD",
        metadata_json=None,
        source="manual",
        created_at=created_at,
        updated_at=created_at,
        deleted_at=None,
        version=1,
        is_deleted=False,
    )
    sell = PortfolioTransaction(
        id="00000000-0000-0000-0000-000000000002",
        portfolio_id=portfolio_id,
        security_id=None,
        ticker_symbol_raw="AAPL",
        ticker_symbol_normalized="AAPL",
        tx_type="Sell",
        trade_date=date(2026, 2, 1),
        shares=5.0,
        price=110.0,
        gross_amount=550.0,
        currency="USD",
        metadata_json=None,
        source="manual",
        created_at=created_at,
        updated_at=created_at,
        deleted_at=None,
        version=1,
        is_deleted=False,
    )
    action = CorporateAction(
        id="00000000-0000-0000-0000-000000000003",
        portfolio_id=portfolio_id,
        ticker="AAPL",
        action_type="SPLIT",
        effective_date=date(2026, 2, 1),
        factor=2.0,
        cash_amount=None,
        metadata_json=None,
        created_at=created_at,
        updated_at=created_at,
        deleted_at=None,
        version=1,
        is_deleted=False,
    )
    db.add_all([buy, sell, action])
    db.commit()

    first = rebuild_position_ledger(db, portfolio_id)
    second = rebuild_position_ledger(db, portfolio_id)

    assert first["input_hash"] == second["input_hash"]
    assert first["holdings"] == second["holdings"]
    assert round(float(first["holdings"]["AAPL"]), 6) == 10.0
    assert round(float(first["basis"]["AAPL"]), 6) == 50.0
