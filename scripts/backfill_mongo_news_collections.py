#!/usr/bin/env python3
"""
Rellena news, news_filtered y macro_news en MongoDB.

- news / news_filtered: desde sentiment_scores (PostgreSQL) + metadatos de cache/news.
- macro_news: titulares macro de cache Finnhub (filtro por keywords) + Finnhub market-news.

Uso:
  source .venv/bin/activate
  python scripts/backfill_mongo_news_collections.py
  python scripts/backfill_mongo_news_collections.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "shared"))

load_dotenv(ROOT / ".env")

TICKERS = ["SPY", "IWM", "GLD"]
CACHE_DIR = ROOT / "cache" / "news"
MACRO_RE = re.compile(
    r"\b(fed|federal reserve|inflation|cpi|pce|rate hike|interest rate|recession|"
    r"gdp|opec|crude oil|geopolit|tariff|ecb|treasury|yield curve|unemployment|"
    r"payroll|hawkish|dovish|stagflation|central bank|war|sanctions)\b",
    re.I,
)


def pg_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        user=os.getenv("POSTGRES_USER", "tfmadmin"),
        password=os.getenv("POSTGRES_PASSWORD", "localpassword123"),
        database=os.getenv("POSTGRES_DB", "tfm"),
    )


def load_news_cache() -> dict[str, dict[str, list]]:
    """{ticker: {date: [articles]}} desde cache/news/*.json"""
    out: dict[str, dict[str, list]] = {t: {} for t in TICKERS}
    for path in CACHE_DIR.glob("*.json"):
        name = path.stem
        parts = name.split("_")
        if len(parts) < 3:
            continue
        ticker = parts[0].upper()
        if ticker not in out:
            continue
        with open(path, encoding="utf-8") as fh:
            out[ticker] = json.load(fh)
    return out


def build_article_index(news_by_ticker: dict) -> dict[tuple, dict]:
    idx: dict[tuple, dict] = {}
    for ticker, by_date in news_by_ticker.items():
        for date_str, articles in (by_date or {}).items():
            for art in articles or []:
                hl = (art.get("headline") or "").strip()
                if hl:
                    idx[(date_str[:10], ticker.upper(), hl)] = art
    return idx


def backfill_news_from_pg(apply: bool) -> tuple[int, int]:
    from mongo_utils import upsert_filtered_news, upsert_news

    news_cache = load_news_cache()
    art_index = build_article_index(news_cache)

    conn = pg_conn()
    with conn.cursor() as c:
        c.execute(
            """
            SELECT batch_date::text, ticker, headline, sentiment, confidence, justification
            FROM sentiment_scores
            ORDER BY batch_date, ticker, confidence DESC
            """
        )
        rows = c.fetchall()
    conn.close()

    filtered_groups: dict[tuple, list[str]] = defaultdict(list)
    news_count = 0

    for batch_date, ticker, headline, sentiment, confidence, justification in rows:
        bd = batch_date[:10]
        tu = ticker.upper()
        art = art_index.get((bd, tu, headline.strip()), {})
        article = {
            "headline": headline,
            "url": art.get("url", ""),
            "source": art.get("source", "finnhub"),
            "datetime": art.get("datetime", bd),
            "summary": art.get("summary", ""),
        }
        sdata = {
            "sentiment": sentiment,
            "confidence": float(confidence),
            "justification": justification or f"FinBERT (PG backfill) {sentiment}",
        }
        if apply:
            upsert_news(bd, tu, article, sdata)
        news_count += 1
        filtered_groups[(bd, tu)].append(headline)

    filtered_count = 0
    for (bd, tu), headlines in filtered_groups.items():
        if not headlines:
            continue
        if apply:
            upsert_filtered_news(
                bd,
                tu,
                headlines[:10],
                f"Backfill PG: {len(headlines)} titulares con FinBERT.",
            )
        filtered_count += 1

    return news_count, filtered_count


def macro_from_cache(apply: bool) -> int:
    from mongo_utils import upsert_macro_news

    news_cache = load_news_cache()
    by_date: dict[str, list] = defaultdict(list)
    seen: set[str] = set()

    for _ticker, dates in news_cache.items():
        for date_str, articles in (dates or {}).items():
            bd = date_str[:10]
            for art in articles or []:
                hl = (art.get("headline") or "").strip()
                if not hl or not MACRO_RE.search(hl):
                    continue
                url = art.get("url", "")
                fp = url or hl
                if fp in seen:
                    continue
                seen.add(fp)
                by_date[bd].append({
                    "headline": hl,
                    "url": url,
                    "source": art.get("source", "finnhub"),
                    "datetime": art.get("datetime", bd),
                    "summary": (art.get("summary") or "")[:500],
                    "category": "macro_proxy",
                    "query_tag": "etf_headlines_filtered",
                })

    total = 0
    for bd, articles in sorted(by_date.items()):
        if apply and articles:
            upsert_macro_news(bd, articles)
        total += len(articles)
    return total


def macro_from_finnhub(apply: bool) -> int:
    """Últimas noticias generales Finnhub (solo fechas recientes en API)."""
    from mongo_utils import upsert_macro_news

    key = os.getenv("FINNHUB_API_KEY", "")
    if not key:
        print("  FINNHUB_API_KEY ausente — omitiendo market-news")
        return 0

    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/news",
            params={"category": "general", "token": key},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  Finnhub market-news HTTP {resp.status_code}")
            return 0
        items = resp.json() or []
    except Exception as exc:
        print(f"  Finnhub market-news error: {exc}")
        return 0

    by_date: dict[str, list] = defaultdict(list)
    for art in items:
        ts = art.get("datetime", 0)
        if not ts:
            continue
        bd = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        hl = (art.get("headline") or "").strip()
        if not hl:
            continue
        by_date[bd].append({
            "headline": hl,
            "url": art.get("url", ""),
            "source": art.get("source", "finnhub"),
            "datetime": bd,
            "summary": (art.get("summary") or "")[:500],
            "category": "general",
            "query_tag": "finnhub_market_news",
        })

    n = 0
    for bd, articles in by_date.items():
        if apply and articles:
            upsert_macro_news(bd, articles)
        n += len(articles)
    time.sleep(0.5)
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    apply = not args.dry_run

    print(f"Modo: {'APPLY' if apply else 'DRY-RUN'}")

    print("\n1/3 news + news_filtered (PostgreSQL sentiment_scores)…")
    n_news, n_filtered = backfill_news_from_pg(apply)
    print(f"  → {n_news} documentos news, {n_filtered} grupos news_filtered")

    print("\n2/3 macro_news (cache ETF + keywords macro)…")
    n_macro_cache = macro_from_cache(apply)
    print(f"  → {n_macro_cache} artículos macro (proxy histórico)")

    print("\n3/3 macro_news (Finnhub general reciente)…")
    n_macro_api = macro_from_finnhub(apply)
    print(f"  → {n_macro_api} artículos macro (API reciente)")

    if not apply:
        print("\nEjecuta sin --dry-run para escribir en MongoDB.")
        return

    from pymongo import MongoClient

    client = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)
    db = client[os.getenv("MONGODB_DB", "tfm")]
    print("\n--- Verificación MongoDB ---")
    for col in ("news", "news_filtered", "macro_news"):
        print(f"  {col}: {db[col].count_documents({})}")
    sample = db["news"].find_one({}, {"batch_date": 1, "ticker": 1, "headline": 1})
    if sample:
        print(f"  ejemplo news: {sample.get('ticker')} {sample.get('batch_date')}")
    macro_sample = db["macro_news"].find_one({}, {"batch_date": 1, "headline": 1})
    if macro_sample:
        print(f"  ejemplo macro_news: {macro_sample.get('batch_date')}")
    client.close()


if __name__ == "__main__":
    main()
