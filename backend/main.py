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
