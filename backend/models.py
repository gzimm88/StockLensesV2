from sqlalchemy import Column, Integer, String, Float
from database import Base

class Metric(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, index=True)
    ticker_symbol = Column(String, index=True)
    score = Column(Float)
    value = Column(Float)
    quality = Column(Float)
    growth = Column(Float)
    risk = Column(Float)