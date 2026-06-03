#!/usr/bin/env python3
"""
Re-run trading decisions without news/macro re-ingestion.

Middle path between full bootstrap_365_days.py (hours: Finnhub, FinBERT, Groq,
macro news) and recompute_exposure_reports.py (metrics only, no inference).

This script re-executes per day/ticker:
  evidence assembly → Bayesian/discriminative inference → apply_sentiment_to_prob_up
  → prob_to_exposure / EWM smooth → position_state, trading_signals, bayesian traces
  → daily Mongo reports (same end-of-day loop as bootstrap).

Still downloads OHLCV via yfinance (cached) for ADX, momentum, EMA-55; PG
technical_indicators only has basic columns.

Reads from existing stores (no FinBERT/Groq/news APIs):
  - PostgreSQL sentiment_scores → aggregate_sentiment_local / sentiment_scoring
  - PostgreSQL market_regime_state + macro_sentiment_scores (Mongo macro_context fallback)

Usage (from tfm/):
  python scripts/recompute_decisions.py --start 2024-06-01 --end 2025-06-01
  python scripts/recompute_decisions.py --start 2024-06-01 --end 2025-06-01 --tickers SPY,NVDA --verbose

Equivalent:
  python bootstrap_365_days.py --decisions-only --start ... --end ...

Optional — refresh report metrics from position_state only (no inference):
  python scripts/recompute_exposure_reports.py --start ... --end ...

Requires .env: POSTGRES_*, MONGODB_URI, optional API keys not needed for this mode.
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
        description="Recompute TFM decisions (inference + exposure) without news ingest"
    )
    parser.add_argument("--start", type=str, help="Fecha inicio YYYY-MM-DD", default=None)
    parser.add_argument("--end", type=str, help="Fecha fin YYYY-MM-DD", default=None)
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Subset de tickers separados por coma, ej: SPY,NVDA",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Activa logs INFO",
    )
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
    )


if __name__ == "__main__":
    main()
