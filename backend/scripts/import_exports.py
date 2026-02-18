import csv
import logging
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.sql.sqltypes import Boolean, Date, DateTime, Float, Integer

from backend.database import Base, SessionLocal, engine
from backend.models import FinancialsHistory, LensPreset, Metrics, PricesHistory, Ticker

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

NULL_VALUES = {"", "null", "none", "na", "nan", "n/a"}


def parse_date(value: str) -> date:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1]
    if "T" in text:
        return datetime.fromisoformat(text).date()
    return date.fromisoformat(text)


def parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1]
    return datetime.fromisoformat(text)


def parse_bool(value: str) -> bool:
    text = value.strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"cannot parse bool: {value!r}")


def parse_numeric(value: str, parser: Callable[[str], Any]) -> Any:
    text = value.strip().replace(",", "")
    return parser(text)


def parse_value(raw: str | None, column_type):
    if raw is None:
        return None

    text = raw.strip()
    if text.lower() in NULL_VALUES:
        return None

    if isinstance(column_type, Boolean):
        return parse_bool(text)
    if isinstance(column_type, Integer):
        return parse_numeric(text, int)
    if isinstance(column_type, Float):
        return parse_numeric(text, float)
    if isinstance(column_type, Date):
        return parse_date(text)
    if isinstance(column_type, DateTime):
        return parse_datetime(text)

    return text


def import_csv(session: Session, csv_path: Path, model) -> tuple[int, int]:
    mapped_columns = {col.name: col for col in model.__table__.columns}
    imported_count = 0
    skipped_count = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for line_no, row in enumerate(reader, start=2):
            try:
                payload = {}
                for column_name, column in mapped_columns.items():
                    if column_name not in row:
                        continue
                    payload[column_name] = parse_value(row.get(column_name), column.type)

                if not payload.get("id"):
                    skipped_count += 1
                    logger.warning("%s:%s skipped row with empty id", csv_path.name, line_no)
                    continue

                session.merge(model(**payload))
                imported_count += 1
            except Exception as exc:  # noqa: BLE001
                skipped_count += 1
                logger.warning("%s:%s skipped row: %s", csv_path.name, line_no, exc)

    session.commit()
    return imported_count, skipped_count


def run_import() -> None:
    Base.metadata.create_all(bind=engine)
    export_dir = Path(__file__).resolve().parents[2] / "data_exports"

    import_sequence = [
        ("Ticker_export.csv", Ticker),
        ("Metrics_export_2.csv", Metrics),
        ("FinancialsHistory_export.csv", FinancialsHistory),
        ("PricesHistory_export.csv", PricesHistory),
        ("LensPreset_export.csv", LensPreset),
    ]

    with SessionLocal() as session:
        for file_name, model in import_sequence:
            csv_path = export_dir / file_name
            if not csv_path.exists():
                logger.warning("Missing export file: %s", csv_path)
                continue

            imported_count, skipped_count = import_csv(session, csv_path, model)
            logger.info("%s -> imported=%s skipped=%s", file_name, imported_count, skipped_count)


if __name__ == "__main__":
    run_import()
