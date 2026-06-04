#!/usr/bin/env python3
"""
Re-bootstrap continuo: solo interpreta datos ya cacheados (PG/Mongo).

Sin ingesta Finnhub/FinBERT/yfinance. Valida caché por (fecha, ticker).
Reports con continuous_run_id interpret-{start}_{end} y pipeline_start/end del rango.

Uso (desde tfm/):
  .venv/bin/python scripts/run_interpret_pipeline.py --start 2025-01-01 --end 2026-06-02
  .venv/bin/python scripts/run_interpret_pipeline.py --start 2025-01-01 --end 2026-06-02 --tickers SPY,NVDA --verbose

Equivalente:
  .venv/bin/python bootstrap_365_days.py --interpret-only --start ... --end ...

Requisitos: POSTGRES_*, MONGODB_URI; tablas technical_indicators, sentiment_scores
(o raw_news en Mongo), market_regime_state / macro_sentiment_scores.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO))

from dotenv import load_dotenv

load_dotenv(_REPO / ".env")


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Pipeline interpret-only: Bayesian + exposición sobre caché existente"
    )
    parser.add_argument("--start", type=str, required=True, help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="Fecha fin YYYY-MM-DD")
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Subset de tickers separados por coma, ej: SPY,NVDA",
    )
    parser.add_argument("--verbose", action="store_true", help="Activa logs INFO")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    import bootstrap_365_days as boot

    boot._configure_logging(verbose=args.verbose)
    tickers_list = (
        [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    )
    boot.run_pipeline(
        args.start,
        args.end,
        tickers_override=tickers_list,
        decisions_only=True,
        interpret_only=True,
    )


if __name__ == "__main__":
    main()
