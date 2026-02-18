<<<<<<< ours
from fastapi import FastAPI
from backend.database import engine, SessionLocal
from backend import models

# Create tables
models.Base.metadata.create_all(bind=engine)

# Create app FIRST
app = FastAPI()


@app.get("/metrics")
def get_metrics():
    db = SessionLocal()
    try:
        data = db.query(models.Metrics).all()
        return [
            {
                "id": m.id,
                "ticker_symbol": m.ticker_symbol,
                "ticker": m.ticker,
                "asOf": m.asOf,
                "as_of_date": m.as_of_date,
            }
            for m in data
        ]
    finally:
        db.close()
=======
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
>>>>>>> theirs
