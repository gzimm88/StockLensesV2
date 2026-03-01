from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import backend.orchestrator.portfolio_orchestrator as po
from backend.database import Base
from backend.models import DividendEvent, FXRate, Portfolio, PortfolioTransaction, PricesHistory
from backend.orchestrator.portfolio_orchestrator import (
    backfill_dividend_history_if_missing,
    create_transaction,
    rebuild_equity_history,
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


def _seed_portfolio(db: Session, *, name: str = "Phase14") -> str:
    row = Portfolio(
        id=str(uuid.uuid4()),
        name=name,
        base_currency="USD",
        apply_dividend_withholding=False,
        dividend_withholding_percent=None,
        owner_id="local",
        is_deleted=False,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    return row.id


def _seed_legacy_price(db: Session, ticker: str, d: date, close: float) -> None:
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
            volume=0,
            source="seed-legacy",
            as_of_date=d,
            created_date=datetime.utcnow(),
            updated_date=datetime.utcnow(),
            created_by_id=None,
            created_by=None,
            is_sample=False,
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


def test_dividend_event_ingestion_idempotent(monkeypatch):
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DivIngest")
    d0 = date.today() - timedelta(days=60)

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

    monkeypatch.setattr(
        po,
        "_fetch_finnhub_dividend_rows",
        lambda **kwargs: ([{"exDate": str(d0 + timedelta(days=10)), "payDate": str(d0 + timedelta(days=20)), "amount": 1.5, "currency": "USD"}], True),
    )

    out1 = backfill_dividend_history_if_missing(portfolio_id, db, strict=True)
    out2 = backfill_dividend_history_if_missing(portfolio_id, db, strict=True)

    rows = db.query(DividendEvent).filter(DividendEvent.ticker == "AAPL").all()
    assert out1["inserted_rows"] == 1
    assert out2["inserted_rows"] == 0
    assert len(rows) == 1


def test_dividend_generated_only_if_shares_held():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DivHeld")
    ex_d = date.today() - timedelta(days=4)
    pay_d = date.today() - timedelta(days=3)
    buy_d = date.today() - timedelta(days=2)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="AAPL",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=buy_d,
        currency="USD",
    )
    _seed_legacy_price(db, "AAPL", buy_d, 101)
    _seed_legacy_price(db, "AAPL", pay_d, 102)

    db.add(
        DividendEvent(
            id=str(uuid.uuid4()),
            ticker="AAPL",
            ex_date=ex_d,
            pay_date=pay_d,
            dividend_per_share_native=1.0,
            currency="USD",
            source="seed",
            source_hash="div-held-aapl",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=buy_d)

    generated = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.is_generated == True,
        )
        .count()
    )
    assert generated == 0


def test_dividend_respects_withholding_percent():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DivWithholding")
    d0 = date.today() - timedelta(days=4)
    d1 = date.today() - timedelta(days=3)
    _seed_fx(db, "EUR", d0, 1.15)

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
    _seed_legacy_price(db, "AAPL", d0, 100)
    _seed_legacy_price(db, "AAPL", d1, 100)

    p = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    assert p is not None
    p.apply_dividend_withholding = True
    p.dividend_withholding_percent = 30.0
    db.commit()

    db.add(
        DividendEvent(
            id=str(uuid.uuid4()),
            ticker="AAPL",
            ex_date=d0,
            pay_date=d1,
            dividend_per_share_native=1.0,
            currency="USD",
            source="seed",
            source_hash="div-withhold-aapl",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)

    generated = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.is_generated == True,
        )
        .first()
    )
    assert generated is not None
    assert round(float(generated.gross_amount_base), 10) == 7.0


def test_dividend_deleted_and_regenerated_on_rebuild():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DivRegenerate")
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
    _seed_legacy_price(db, "AAPL", d0, 100)
    _seed_legacy_price(db, "AAPL", d1, 100)

    event = DividendEvent(
        id=str(uuid.uuid4()),
        ticker="AAPL",
        ex_date=d0,
        pay_date=d1,
        dividend_per_share_native=1.0,
        currency="USD",
        source="seed",
        source_hash="div-regenerate-aapl",
        created_at=datetime.utcnow(),
    )
    db.add(event)
    db.commit()

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)
    first_row = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.is_generated == True,
        )
        .first()
    )
    assert first_row is not None
    first_id = first_row.id

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)
    rows = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.is_generated == True,
        )
        .all()
    )
    assert len(rows) == 1
    assert rows[0].id != first_id


def test_dividend_fx_conversion_on_pay_date():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DivFx")
    d0 = date.today() - timedelta(days=4)
    d1 = date.today() - timedelta(days=3)
    _seed_fx(db, "EUR", d0, 1.15)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="ENXTAM:ASML",
        tx_type="BUY",
        quantity=10,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    _seed_legacy_price(db, "ASML", d0, 100)
    _seed_legacy_price(db, "ASML", d1, 100)
    _seed_fx(db, "EUR", d1, 1.2)

    db.add(
        DividendEvent(
            id=str(uuid.uuid4()),
            ticker="ASML",
            ex_date=d0,
            pay_date=d1,
            dividend_per_share_native=1.0,
            currency="EUR",
            source="seed",
            source_hash="div-fx-asml",
            created_at=datetime.utcnow(),
        )
    )
    db.commit()

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)

    generated = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.is_generated == True,
        )
        .first()
    )
    assert generated is not None
    assert round(float(generated.gross_amount_base), 10) == 12.0
