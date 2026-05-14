"""
lambda_macro_ingestion
──────────────────────
Ingesta noticias macroeconómicas y geopolíticas GLOBALES.

NO busca noticias de tickers concretos.
Busca contexto sistémico financiero global para alimentar MacroSentiment y RiskRegime.

Fuentes:
  - NewsAPI   → queries temáticas sobre macro/geopolítica
  - Reuters   → RSS feed mercados globales
  - CNBC      → RSS feed mercados
  - FT        → RSS feed economía global

Salida:
  MongoDB colección: macro_news
  Documento: { headline, summary, url, source, datetime, category, query_tag, batch_date }
"""

import json
import boto3
import requests
import logging
import hashlib
import time
import feedparser
from datetime import datetime, timedelta, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")

try:
    from mongo_utils import upsert_macro_news as _upsert_macro_news
    logger.info("mongo_utils (macro_ingestion) cargado OK")
except ImportError:
    logger.warning("mongo_utils no disponible")
    _upsert_macro_news = None


# ─── Queries macro para NewsAPI ───────────────────────────────────────────────
# Agrupadas por categoría para facilitar el etiquetado posterior

MACRO_QUERIES = {
    "monetary_policy": [
        "Federal Reserve interest rates decision",
        "Fed hawkish dovish monetary policy",
        "ECB interest rates inflation Europe",
        "central bank rate hike cut",
    ],
    "inflation": [
        "CPI inflation data consumer prices",
        "PCE inflation Federal Reserve target",
        "stagflation recession inflation",
        "energy prices inflation impact",
    ],
    "macro_economy": [
        "GDP growth recession economic outlook",
        "unemployment jobs report nonfarm payrolls",
        "global economic slowdown IMF World Bank",
        "US economy contraction expansion",
    ],
    "geopolitical": [
        "geopolitical tensions war conflict markets",
        "China Taiwan strait military tensions",
        "Russia Ukraine war sanctions economy",
        "Middle East conflict oil markets",
    ],
    "commodities": [
        "oil prices OPEC crude production cut",
        "energy crisis oil supply demand",
        "commodity prices gold silver metals",
        "natural gas prices energy markets",
    ],
    "financial_stability": [
        "banking crisis financial stability systemic risk",
        "credit crunch liquidity financial markets",
        "market volatility VIX fear index",
        "yield curve inversion recession signal",
    ],
    "trade_tech": [
        "semiconductor restrictions export controls China",
        "supply chain disruption global trade",
        "tariffs trade war protectionism",
        "tech sector regulation antitrust",
    ],
}

# ─── RSS feeds macro ──────────────────────────────────────────────────────────

RSS_FEEDS = {
    "reuters_markets":  "https://feeds.reuters.com/reuters/businessNews",
    "cnbc_markets":     "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "ft_world":         "https://www.ft.com/world?format=rss",
    "marketwatch":      "https://feeds.content.dowjones.io/public/rss/mw_topstories",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TFM-MacroBot/1.0)",
    "Accept-Language": "en-US,en;q=0.9",
}


# ─── Contexto del pipeline ────────────────────────────────────────────────────

def resolve_batch_date(event):
    raw = (event or {}).get("batch_date") or (event or {}).get("date")
    return raw[:10] if raw else datetime.now().strftime("%Y-%m-%d")


def resolve_pipeline_context(event):
    ctx = (event or {}).get("pipeline_context", {}) if isinstance(event, dict) else {}
    request = ctx.get("request", {}) if isinstance(ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}
    batch_date   = resolve_batch_date(request) if request.get("batch_date") else resolve_batch_date(ctx)
    run_id       = ctx.get("run_id") or (event or {}).get("run_id") or f"legacy-{batch_date}"
    trigger_type = request.get("trigger_type")
    if trigger_type not in ("manual", "scheduled"):
        trigger_type = "manual" if request.get("ticker") or request.get("tickers") else "scheduled"
    return {"batch_date": batch_date, "run_id": run_id, "trigger_type": trigger_type}


def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        return json.loads(response["SecretBinary"])
    except Exception as exc:
        logger.error(f"Error retrieving secret {secret_name}: {exc}")
        raise


def _fingerprint(url: str, headline: str) -> str:
    key = (url or headline or "").strip().lower()
    return hashlib.md5(key.encode()).hexdigest()


def _normalize(headline: str, url: str, source: str,
               published_at, summary: str = "",
               category: str = "macro", query_tag: str = "") -> dict:
    if isinstance(published_at, (int, float)):
        dt_str = datetime.utcfromtimestamp(published_at).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(published_at, datetime):
        dt_str = published_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        dt_str = str(published_at or "")
    return {
        "headline":  headline.strip() if headline else "",
        "url":       url or "",
        "source":    source or "unknown",
        "datetime":  dt_str,
        "summary":   summary or "",
        "category":  category,
        "query_tag": query_tag,
    }


# ─── Fuente 1: NewsAPI ────────────────────────────────────────────────────────

def _fetch_newsapi(newsapi_key: str, batch_date: str) -> list:
    from newsapi import NewsApiClient
    client   = NewsApiClient(api_key=newsapi_key)
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=1)
    articles = []
    seen     = set()

    for category, queries in MACRO_QUERIES.items():
        for query in queries:
            try:
                resp = client.get_everything(
                    q          = query,
                    from_param = start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    to         = end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    language   = "en",
                    sort_by    = "relevancy",
                    page_size  = 10,
                )
                for item in (resp.get("articles") or []):
                    headline = item.get("title", "")
                    url      = item.get("url", "")
                    if not headline or headline == "[Removed]":
                        continue
                    fp = _fingerprint(url, headline)
                    if fp in seen:
                        continue
                    seen.add(fp)
                    articles.append(_normalize(
                        headline    = headline,
                        url         = url,
                        source      = (item.get("source") or {}).get("name", "newsapi"),
                        published_at= item.get("publishedAt", ""),
                        summary     = item.get("description", ""),
                        category    = category,
                        query_tag   = query,
                    ))
                time.sleep(0.2)   # respetar rate limit NewsAPI
            except Exception as exc:
                logger.warning(f"NewsAPI error para query '{query}': {exc}")

    logger.info(f"NewsAPI macro: {len(articles)} artículos únicos")
    return articles


# ─── Fuente 2: RSS feeds ──────────────────────────────────────────────────────

def _fetch_rss(batch_date: str) -> list:
    articles = []
    seen     = set()
    cutoff   = datetime.now(timezone.utc) - timedelta(hours=36)

    for feed_name, feed_url in RSS_FEEDS.items():
        try:
            parsed = feedparser.parse(feed_url)
            count  = 0
            for entry in parsed.entries:
                headline = entry.get("title", "").strip()
                url      = entry.get("link", "")
                summary  = entry.get("summary", "") or entry.get("description", "")
                pub      = entry.get("published_parsed")

                if not headline:
                    continue

                # Filtrar artículos más viejos de 36h
                if pub:
                    pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        continue

                fp = _fingerprint(url, headline)
                if fp in seen:
                    continue
                seen.add(fp)

                articles.append(_normalize(
                    headline    = headline,
                    url         = url,
                    source      = feed_name,
                    published_at= entry.get("published", ""),
                    summary     = summary[:500] if summary else "",
                    category    = "macro",
                    query_tag   = feed_name,
                ))
                count += 1

            logger.info(f"RSS {feed_name}: {count} artículos")
        except Exception as exc:
            logger.warning(f"RSS error para {feed_name}: {exc}")

    logger.info(f"RSS total: {len(articles)} artículos únicos")
    return articles


# ─── Handler ──────────────────────────────────────────────────────────────────

def handler(event, context):
    logger.info("lambda_macro_ingestion iniciado")
    ctx   = resolve_pipeline_context(event)
    today = ctx["batch_date"]

    # NewsAPI key (obligatoria para macro queries)
    try:
        newsapi_key = get_secret("newsapi/api_key")["api_key"]
    except Exception as exc:
        logger.error(f"No se pudo obtener newsapi/api_key: {exc}")
        return {"statusCode": 500, "body": json.dumps({"error": str(exc)})}

    # Recopilar de todas las fuentes
    newsapi_articles = _fetch_newsapi(newsapi_key, today)
    rss_articles     = _fetch_rss(today)

    # Fusión y deduplicación global
    seen     = set()
    combined = []
    for art in newsapi_articles + rss_articles:
        fp = _fingerprint(art["url"], art["headline"])
        if fp not in seen:
            seen.add(fp)
            combined.append(art)

    logger.info(f"Total artículos macro únicos: {len(combined)} "
                f"(NewsAPI={len(newsapi_articles)}, RSS={len(rss_articles)})")

    # Persistir en MongoDB
    if not _upsert_macro_news:
        return {"statusCode": 500, "body": json.dumps({"error": "mongo_utils no disponible"})}

    _upsert_macro_news(today, combined)

    # Resumen por categoría para los KPIs
    by_category = {}
    for art in combined:
        cat = art.get("category", "macro")
        by_category[cat] = by_category.get(cat, 0) + 1

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":         "Ingesta macro completada",
            "batch_date":      today,
            "total_articles":  len(combined),
            "by_category":     by_category,
            "sources": {
                "newsapi": len(newsapi_articles),
                "rss":     len(rss_articles),
            },
        }),
    }
