from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from backend.database import Base
from backend.models import DividendEvent, FXRate, Portfolio, PortfolioTransaction, PricesHistory
from backend.orchestrator.portfolio_orchestrator import (
    compute_performance_breakdown,
    create_transaction,
    get_portfolio_dashboard_summary,
    get_portfolio_holdings,
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


def _seed_portfolio(db: Session, *, name: str = "Phase13") -> str:
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


def test_dividend_auto_posting_from_dividend_events():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="DivAuto")
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
    _seed_legacy_price(db, "AAPL", d1, 101)

    event = DividendEvent(
        id=str(uuid.uuid4()),
        ticker="AAPL",
        ex_date=d0,
        pay_date=d1,
        amount_per_share=1.0,
        currency="USD",
        source="seed",
        source_hash="div-auto-aapl",
        created_at=datetime.utcnow(),
    )
    db.add(event)
    db.commit()

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)

    generated = (
        db.query(PortfolioTransaction)
        .filter(
            PortfolioTransaction.portfolio_id == portfolio_id,
            PortfolioTransaction.tx_type == "Dividend",
            PortfolioTransaction.is_generated == True,
            PortfolioTransaction.generated_event_id == event.id,
        )
        .all()
    )
    assert len(generated) == 1
    assert float(generated[0].gross_amount_base) == 10.0

    breakdown = compute_performance_breakdown(db, portfolio_id)
    assert breakdown["dividend_gain"] == 10.0

    holdings = get_portfolio_holdings(db, portfolio_id)
    assert len(holdings["holdings"]) == 1
    assert holdings["holdings"][0]["total_dividends"] == 10.0


def test_fx_impact_separated_for_native_currency_holding():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="FxSeparation")
    d0 = date.today() - timedelta(days=3)
    d1 = date.today() - timedelta(days=2)

    _seed_fx(db, "EUR", d0, 1.2)
    _seed_fx(db, "EUR", d1, 1.1)

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

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d1)
    holdings = get_portfolio_holdings(db, portfolio_id)
    row = holdings["holdings"][0]

    assert row["native_currency"] == "EUR"
    assert row["price_return_value"] == 0.0
    assert row["fx_impact_value"] != 0.0


def test_price_fallback_uses_legacy_prices_when_price_history_empty():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="LegacyFallback")
    d0 = date.today() - timedelta(days=2)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="ADBE",
        tx_type="BUY",
        quantity=2,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    _seed_legacy_price(db, "ADBE", d0, 111)

    out = rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d0)
    assert out["rows_written"] >= 1

    holdings = get_portfolio_holdings(db, portfolio_id)
    assert len(holdings["holdings"]) == 1
    assert holdings["holdings"][0]["market_price"] == 111.0


def test_last_price_timestamp_reports_fallback_source():
    db = _make_session()
    portfolio_id = _seed_portfolio(db, name="LastUpdatedFallback")
    d0 = date.today() - timedelta(days=2)

    create_transaction(
        db,
        portfolio_id=portfolio_id,
        ticker="MSFT",
        tx_type="BUY",
        quantity=1,
        price=100,
        trade_date=d0,
        currency="USD",
    )
    _seed_legacy_price(db, "MSFT", d0, 105)

    rebuild_equity_history(db, portfolio_id, mode="full", force=True, strict=True, to_date=d0)
    summary = get_portfolio_dashboard_summary(db, portfolio_id)
    assert isinstance(summary.get("last_prices_updated_at"), str)
    assert "fallback" in summary["last_prices_updated_at"].lower()
