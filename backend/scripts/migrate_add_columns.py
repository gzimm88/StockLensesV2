"""
Migration script: add depreciation_ttm and sbc_ttm columns to the metrics table.

Run from the project root:
    python3 -m backend.scripts.migrate_add_columns

These columns are required by the Finnhub ETL pipeline (ALWAYS_UPDATE set)
and are computed as TTM sums from quarterly Finnhub data.

SQLite supports ADD COLUMN only (no DROP, no type changes).
Running this on a DB that already has the columns is safe — the script checks first.
"""

import sqlite3
import os
import sys

DB_PATHS = [
    os.path.join(os.path.dirname(__file__), "..", "stocklenses.db"),
]

NEW_COLUMNS = [
    ("depreciation_ttm", "REAL"),  # TTM sum of quarterly depreciation
    ("sbc_ttm", "REAL"),            # TTM sum of quarterly stock-based compensation
]


def migrate(db_path: str) -> None:
    abs_path = os.path.abspath(db_path)
    if not os.path.exists(abs_path):
        print(f"  [SKIP] DB not found: {abs_path}")
        return

    conn = sqlite3.connect(abs_path)
    try:
        cursor = conn.execute("PRAGMA table_info(metrics)")
        existing_cols = {row[1] for row in cursor.fetchall()}

        added = []
        for col_name, col_type in NEW_COLUMNS:
            if col_name in existing_cols:
                print(f"  [SKIP] Column '{col_name}' already exists in metrics")
            else:
                conn.execute(f"ALTER TABLE metrics ADD COLUMN {col_name} {col_type}")
                added.append(col_name)
                print(f"  [ADD]  Column '{col_name}' ({col_type}) added to metrics")

        if added:
            conn.commit()
            print(f"  [OK]   Migration committed — added: {added}")
        else:
            print("  [OK]   No migration needed (all columns already present)")
    finally:
        conn.close()


if __name__ == "__main__":
    print("Running metrics table migration...")
    for path in DB_PATHS:
        print(f"\nDatabase: {os.path.abspath(path)}")
        migrate(path)
    print("\nMigration complete.")
