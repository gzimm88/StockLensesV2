# StockLensesV2

Frontend: Vite + React.

Backend: FastAPI + SQLAlchemy (SQLite), with import support for CSV exports in `data_exports/`.

## Backend quickstart

```bash
python -m backend.scripts.import_exports
uvicorn backend.main:app --reload
```
