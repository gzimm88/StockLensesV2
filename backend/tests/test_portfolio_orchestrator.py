from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from backend.database import Base
from backend.models import Portfolio, PricesHistory, Ticker
from backend.orchestrator.portfolio_orchestrator import (
    CoverageReport,
    ensure_price_coverage,
    run_portfolio_creation_flow,
)
from backend.services.portfolio_engine import EngineOutputs


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return SessionLocal()


def _seed_ticker(db: Session, symbol: str) -> None:
    db.add(
        Ticker(
            id=str(uuid.uuid4()),
            symbol=symbol,
            exchange="NYSE",
            name=symbol,
        )
    )
    db.commit()


def _seed_portfolio(db: Session, name: str = "Default") -> str:
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


def _seed_price(db: Session, symbol: str, d: date, close: float) -> None:
    db.add(
        PricesHistory(
            id=str(uuid.uuid4()),
            ticker=symbol,
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


def _fake_chart(start: date, end: date) -> dict:
    ts = []
    close = []
    current = start
    px = 100.0
    while current <= end:
        if current.weekday() < 5:
            ts.append(int(datetime(current.year, current.month, current.day).timestamp()))
            close.append(px)
            px += 1.0
        current += timedelta(days=1)
    return {
        "timestamp": ts,
        "indicators": {
            "quote": [{"open": close, "high": close, "low": close, "close": close, "volume": [1000.0] * len(close)}],
            "adjclose": [{"adjclose": close}],
        },
    }


def test_missing_ticker_triggers_backend_fetch(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "MSFT")

    calls: list[tuple[str, str, str]] = []

    async def _fetch(ticker: str, start_date: str, end_date: str, client=None):
        calls.append((ticker, start_date, end_date))
        return _fake_chart(date.fromisoformat(start_date), date.fromisoformat(end_date) - timedelta(days=1))

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["MSFT"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 12),
        )
    )

    assert calls, "Expected backend Yahoo fetch to be called for missing ticker coverage"
    assert report.fetched_tickers == ["MSFT"]


def test_covered_ticker_does_not_refetch(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "AAPL")

    start = date(2024, 1, 2)
    end = date(2024, 1, 12)
    current = start
    px = 150.0
    while current <= end:
        if current.weekday() < 5:
            _seed_price(db, "AAPL", current, px)
            px += 1.0
        current += timedelta(days=1)

    async def _fetch_should_not_run(*args, **kwargs):
        raise AssertionError("Fetch should not run when price coverage is already sufficient")

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch_should_not_run,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["AAPL"],
            start_date=start,
            end_date=end,
        )
    )

    assert report.fetched_tickers == []


def test_same_input_portfolio_produces_identical_nav_and_irr(monkeypatch):
    db = _make_session()
    portfolio_id = _seed_portfolio(db)

    tx = type(
        "Tx",
        (),
        {
            "ticker": "MSFT",
            "trade_date": date(2024, 1, 2),
            "row_id": 2,
            "tx_type": "Buy",
            "shares": 10.0,
            "price": 100.0,
        },
    )()

    async def _ensure(*args, **kwargs):
        return CoverageReport(
            requested_tickers=["MSFT"],
            fetched_tickers=[],
            already_covered_tickers=["MSFT"],
            coverage_start=date(2024, 1, 2),
            coverage_end=date(2024, 1, 12),
            warnings=[],
            status_by_ticker={"MSFT": "OK"},
            impact_by_ticker={"MSFT": {"fallback_days": 0, "first_missing_date": None, "last_missing_date": None}},
        )

    def _engine_outputs() -> EngineOutputs:
        return EngineOutputs(
            lot_audit=[],
            realized_report=[],
            unrealized_snapshot=[],
            daily_equity_curve=[],
            fx_attribution=[],
            irr_summary=[{"scope": "portfolio", "ticker": "ALL", "irr": 0.1234}],
            portfolio_summary=[
                {"metric": "total_equity", "value": 250000.0},
                {"metric": "money_weighted_return_irr", "value": 0.1234},
            ],
            warnings=[],
        )

    monkeypatch.setattr("backend.orchestrator.portfolio_orchestrator._load_portfolio_transactions_from_db", lambda _db, _pid: [tx])
    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator._tx_stats",
        lambda txs: {
                "MSFT": {
                    "first_trade_date": date(2024, 1, 2),
                    "last_trade_date": date(2024, 1, 12),
                    "net_shares": 10.0,
                    "closed_position": False,
                    "source_symbol": "NasdaqGS:MSFT",
                    "multiple_source_symbols": False,
                }
            },
        )
    monkeypatch.setattr("backend.orchestrator.portfolio_orchestrator.ensure_price_coverage", _ensure)
    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.export_prices_for_engine",
        lambda *a, **k: (Path("/tmp/prices.csv"), []),
    )
    monkeypatch.setattr("backend.orchestrator.portfolio_orchestrator.run_portfolio_engine", lambda *a, **k: _engine_outputs())
    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.export_outputs",
        lambda outputs: [Path("/tmp/portfolio_summary.csv")],
    )

    first = asyncio.run(run_portfolio_creation_flow(db, portfolio_id))
    second = asyncio.run(run_portfolio_creation_flow(db, portfolio_id))

    assert first["nav"] == second["nav"]
    assert first["irr"] == second["irr"]
    assert first["coverage_status"]["requested_tickers"] == ["MSFT"]
    assert first["coverage_status"]["already_covered_tickers"] == ["MSFT"]


def test_repeated_coverage_checks_do_not_refetch_without_force(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "MSFT")

    calls = {"count": 0}

    async def _fetch(ticker: str, start_date: str, end_date: str, client=None):
        calls["count"] += 1
        return _fake_chart(date.fromisoformat(start_date), date.fromisoformat(end_date) - timedelta(days=1))

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch,
    )

    start = date(2024, 1, 2)
    end = date(2024, 1, 12)

    first = asyncio.run(ensure_price_coverage(db, ["MSFT"], start_date=start, end_date=end))
    second = asyncio.run(ensure_price_coverage(db, ["MSFT"], start_date=start, end_date=end))

    assert first.fetched_tickers == ["MSFT"]
    assert second.fetched_tickers == []
    assert second.already_covered_tickers == ["MSFT"]
    assert calls["count"] == 1, "Second run without force must not refetch already-covered ticker"


def test_overlapping_requests_fetch_ticker_once(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "MSFT")

    calls = {"count": 0}

    async def _fetch(ticker: str, start_date: str, end_date: str, client=None):
        calls["count"] += 1
        await asyncio.sleep(0.05)
        return _fake_chart(date.fromisoformat(start_date), date.fromisoformat(end_date) - timedelta(days=1))

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch,
    )

    start = date(2024, 1, 2)
    end = date(2024, 1, 12)

    async def _run_both():
        return await asyncio.gather(
            ensure_price_coverage(db, ["MSFT"], start_date=start, end_date=end),
            ensure_price_coverage(db, ["MSFT"], start_date=start, end_date=end),
        )

    r1, r2 = asyncio.run(_run_both())

    assert calls["count"] == 1, "Ticker-level lock should prevent duplicate concurrent fetches"
    assert sorted([r1.fetched_tickers, r2.fetched_tickers]) == [[], ["MSFT"]]


def test_closed_ticker_no_history_uses_warning_path(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "AZUL")

    async def _fetch_fails(*args, **kwargs):
        raise RuntimeError("No price history returned for AZUL")

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch_fails,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["AZUL"],
            start_date=date(2020, 8, 1),
            end_date=date(2026, 2, 23),
            closed_position_end_dates={"AZUL": date(2021, 2, 5)},
        )
    )

    assert report.fetched_tickers == []
    assert any("AZUL" in w for w in report.warnings)


def test_fetch_symbol_normalization_for_yahoo(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "BRK.B")

    seen = {"ticker": None}

    async def _fetch(ticker: str, start_date: str, end_date: str, client=None):
        seen["ticker"] = ticker
        return _fake_chart(date.fromisoformat(start_date), date.fromisoformat(end_date) - timedelta(days=1))

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["BRK.B"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 12),
        )
    )

    assert seen["ticker"] == "BRK-B"
    assert report.fetched_tickers == ["BRK.B"]


def test_per_ticker_start_date_avoids_false_pre_ipo_failure(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "DUOL")

    seen = {"start": None}

    async def _fetch(ticker: str, start_date: str, end_date: str, client=None):
        seen["start"] = start_date
        return _fake_chart(date.fromisoformat(start_date), date.fromisoformat(end_date) - timedelta(days=1))

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["DUOL"],
            start_date=date(2020, 1, 1),  # global portfolio start
            end_date=date(2021, 8, 10),
            ticker_start_dates={"DUOL": date(2021, 7, 28)},  # first DUOL trade/IPO window
        )
    )

    assert seen["start"] == "2021-07-28"
    assert report.fetched_tickers == ["DUOL"]


def test_exchange_prefix_maps_to_european_yahoo_symbol(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "ASML")

    seen = {"ticker": None}

    async def _fetch(ticker: str, start_date: str, end_date: str, client=None):
        seen["ticker"] = ticker
        return _fake_chart(date.fromisoformat(start_date), date.fromisoformat(end_date) - timedelta(days=1))

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["ASML"],
            start_date=date(2025, 4, 7),
            end_date=date(2025, 4, 30),
            ticker_source_symbols={"ASML": "ENXTAM:ASML"},
        )
    )

    assert seen["ticker"] == "ASML.AS"
    assert report.fetched_tickers == ["ASML"]


def test_bounded_leading_gap_is_warning_not_failure(monkeypatch):
    db = _make_session()
    _seed_ticker(db, "MOH")

    # Seed from start+3d only (bounded leading gap <= 5d)
    start = date(2024, 7, 5)
    end = date(2024, 7, 15)
    for d, px in [(date(2024, 7, 8), 100.0), (date(2024, 7, 9), 101.0), (date(2024, 7, 10), 102.0)]:
        _seed_price(db, "MOH", d, px)

    async def _fetch_should_not_run(*args, **kwargs):
        raise AssertionError("Fetch should not run when bounded leading-gap coverage exists")

    monkeypatch.setattr(
        "backend.orchestrator.portfolio_orchestrator.yahoo_client.fetch_prices_range",
        _fetch_should_not_run,
    )

    report = asyncio.run(
        ensure_price_coverage(
            db,
            ["MOH"],
            start_date=start,
            end_date=end,
        )
    )

    assert report.already_covered_tickers == ["MOH"]
    assert any("bounded leading gap" in w for w in report.warnings)
