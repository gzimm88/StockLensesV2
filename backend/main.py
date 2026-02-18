from fastapi import Depends, FastAPI
from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.database import Base, engine, get_db
from backend.models import FinancialsHistory, LensPreset, Metrics, PricesHistory, Ticker

app = FastAPI(title="StockLenses Backend")

Base.metadata.create_all(bind=engine)


def rows_to_dict(rows):
    return [
        {column.name: getattr(row, column.name) for column in row.__table__.columns}
        for row in rows
    ]


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.get("/tickers")
def list_tickers(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(Ticker).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/metrics")
def list_metrics(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(Metrics).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/financials-history")
def list_financials(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(FinancialsHistory).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/prices-history")
def list_prices(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(PricesHistory).limit(limit)).all()
    return rows_to_dict(rows)


@app.get("/lens-presets")
def list_lens_presets(limit: int = 100, db: Session = Depends(get_db)):
    rows = db.scalars(select(LensPreset).limit(limit)).all()
    return rows_to_dict(rows)
