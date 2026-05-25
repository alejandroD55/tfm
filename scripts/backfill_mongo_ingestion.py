#!/usr/bin/env python3
"""
Rellena raw_news y ohlcv en MongoDB desde caché Finnhub + yfinance.

Tras sync_mongo_from_postgres.py tienes trazas y señales, pero el explorador
de tickers necesita las colecciones de ingesta (lambda_ingestion).

No toca PostgreSQL ni recalcula Bayesian. Usa la misma lógica que Fase 1/3
del local_backtest_runner (incluye cache/news/*.json si existe).

Uso:
  source .venv/bin/activate
  python scripts/backfill_mongo_ingestion.py
  python scripts/backfill_mongo_ingestion.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "shared"))

load_dotenv(ROOT / ".env")

from local_backtest_runner import (  # noqa: E402
    DAYS_BACK,
    TICKERS,
    fetch_news_historical,
    fetch_ohlcv_all,
)
from mongo_utils import upsert_ohlcv_bulk, upsert_raw_news  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Solo cuenta, no escribe")
    args = parser.parse_args()

    end_d = datetime.now().date()
    start_d = end_d - timedelta(days=DAYS_BACK)

    print(f"Tickers: {TICKERS}")
    print(f"Período: {start_d} → {end_d} ({DAYS_BACK} días)")
    print(f"Modo: {'DRY-RUN' if args.dry_run else 'APPLY → MongoDB'}")

    print("\n1/2 OHLCV (yfinance)…")
    ohlcv_all = fetch_ohlcv_all(TICKERS, DAYS_BACK)

    print("\n2/2 Noticias (caché Finnhub o API)…")
    news_all: dict[str, dict[str, list]] = {}
    for t in TICKERS:
        news_all[t] = fetch_news_historical(t, start_d, end_d)
        n = sum(len(v) for v in news_all[t].values())
        print(f"  {t}: {n} artículos en {len(news_all[t])} fechas")

    business_days = pd.bdate_range(start=str(start_d), end=str(end_d))
    raw_writes = 0
    ohlcv_writes = 0

    for bd in tqdm(business_days, desc="Mongo ingestion", unit="día"):
        date_str = bd.strftime("%Y-%m-%d")
        for ticker in TICKERS:
            articles = news_all.get(ticker, {}).get(date_str, [])
            if articles and not args.dry_run:
                upsert_raw_news(date_str, ticker, articles)
            if articles:
                raw_writes += 1

            ohlcv_df = ohlcv_all.get(ticker)
            if ohlcv_df is None:
                continue
            target_dt = pd.to_datetime(date_str)
            window = ohlcv_df[ohlcv_df.index <= target_dt].tail(90)
            rows = []
            for idx, row in window.iterrows():
                rows.append({
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": float(row.get("Open", 0) or 0),
                    "high": float(row.get("High", 0) or 0),
                    "low": float(row.get("Low", 0) or 0),
                    "close": float(row.get("Close", 0) or 0),
                    "volume": float(row.get("Volume", 0) or 0),
                })
            if rows and not args.dry_run:
                upsert_ohlcv_bulk(date_str, ticker, rows)
            if rows:
                ohlcv_writes += 1

    if args.dry_run:
        print(f"\nEscribiría ~{raw_writes} docs raw_news y ~{ohlcv_writes} docs ohlcv")
        print("Ejecuta sin --dry-run para aplicar.")
        return

    from pymongo import MongoClient

    client = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)
    db = client[os.getenv("MONGODB_DB", "tfm")]
    print("\n--- Verificación MongoDB ---")
    print(f"  raw_news: {db['raw_news'].count_documents({})}")
    print(f"  ohlcv:    {db['ohlcv'].count_documents({})}")
    sample = db["raw_news"].find_one({}, {"batch_date": 1, "ticker": 1, "count": 1})
    if sample:
        print(f"  ejemplo: {sample.get('ticker')} {sample.get('batch_date')} ({sample.get('count')} artículos)")
    client.close()


if __name__ == "__main__":
    main()
