from __future__ import annotations

import sys

from backend.services.portfolio_engine import (
    OUTPUT_DIR,
    PortfolioEngineError,
    export_outputs,
    run_portfolio_engine,
)


def main() -> int:
    try:
        outputs = run_portfolio_engine()
        files = export_outputs(outputs, OUTPUT_DIR)
    except PortfolioEngineError as exc:
        print(f"PORTFOLIO_ENGINE_ERROR: {exc}")
        return 1

    print("Portfolio engine completed successfully.")
    print("Generated CSV outputs:")
    for p in files:
        print(f"- {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
