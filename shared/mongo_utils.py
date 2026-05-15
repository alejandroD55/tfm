"""
mongo_utils.py — Utilidades compartidas para MongoDB Atlas
============================================================
Patron singleton: un solo MongoClient por instancia Lambda (warm reuse).
Todas las escrituras son best-effort; un fallo en MongoDB NUNCA cancela
el pipeline principal.

Colecciones en la BD 'tfm':
  etf_universe      → lista de tickers del universo (documento _id 'default')
  raw_news          → articulos Finnhub sin clasificar (pre-FinBERT)
  ohlcv             → datos OHLCV diarios de yfinance
  news              → articulos con scoring FinBERT completo
  bayesian_reports  → traza por ticker: raw values, discretizacion, inferencia
  bayesian_traces   → JSON completo de traza bayesiana por batch_date (API /trace)
  reports           → reporte diario completo (backtesting, metricas, senales)
  macro_news        → noticias macro globales (FED, inflacion, geopolitica…)
  macro_context     → MacroSentiment + RiskRegime calculados por batch_date

Indices recomendados (ejecutar via POST /mongo/setup-indexes):
  etf_universe:     {_id:1}
  raw_news:    {batch_date:1, ticker:1}
  ohlcv:       {batch_date:1, ticker:1, date:1}  (unico)
  news:        {batch_date:1, ticker:1}, {ticker:1, batch_date:-1}, headline:text
  bayesian_reports: {batch_date:1, ticker:1} unico, {ticker:1, batch_date:-1}
  bayesian_traces:  {batch_date:1} unico
  reports:     {report_date:1} unico
  macro_news:  {batch_date:1}, {category:1}
  macro_context: {batch_date:1} unico
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_mongo_client = None
_MONGO_DB_NAME = os.getenv("MONGODB_DB", "tfm")


# ─── Conexion ────────────────────────────────────────────────────────────────

def _get_db():
    global _mongo_client
    if _mongo_client is None:
        try:
            from pymongo import MongoClient
            uri = _read_mongo_uri()
            if not uri:
                return None
            _mongo_client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5_000,
                connectTimeoutMS=5_000,
                socketTimeoutMS=8_000,
                maxPoolSize=1,
            )
            _mongo_client.admin.command("ping")
            logger.info("MongoDB Atlas: conexion establecida")
        except Exception as exc:
            logger.warning(f"MongoDB no disponible: {exc}")
            _mongo_client = None
            return None
    try:
        return _mongo_client[_MONGO_DB_NAME]
    except Exception as exc:
        logger.warning(f"MongoDB error al obtener DB: {exc}")
        return None


def _read_mongo_uri() -> Optional[str]:
    uri = os.getenv("MONGODB_URI")
    if uri:
        return uri
    try:
        import boto3
        region = os.getenv("AWS_REGION", "eu-north-1")
        client = boto3.client("secretsmanager", region_name=region)
        resp   = client.get_secret_value(SecretId="mongodb/connection_string")
        secret = json.loads(resp["SecretString"])
        return secret.get("connection_string") or secret.get("uri")
    except Exception as exc:
        logger.warning(f"No se pudo leer mongodb/connection_string: {exc}")
        return None


def is_available() -> bool:
    """Comprueba si MongoDB esta disponible sin lanzar excepciones."""
    return _get_db() is not None


# ─── raw_news: articulos Finnhub crudos (antes de FinBERT) ───────────────────

def upsert_raw_news(batch_date: str, ticker: str, articles: list):
    """
    Guarda todos los articulos de Finnhub para un ticker en una fecha.
    Sustituye el fichero raw/{DATE}/news.json de S3.
    """
    try:
        db = _get_db()
        if db is None:
            return
        if not articles:
            return
        now = datetime.now(timezone.utc)
        # Upsert del documento completo (un doc por ticker/fecha, array de articulos)
        db["raw_news"].update_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {
                "$set": {
                    "batch_date":  batch_date,
                    "ticker":      ticker.upper(),
                    "articles":    articles,
                    "count":       len(articles),
                    "updated_at":  now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_raw_news failed ({ticker}): {exc}")


def read_raw_news(batch_date: str) -> dict:
    """
    Lee todos los articulos de una fecha (equivalente a leer news.json de S3).
    Devuelve dict {ticker: [articles...]}.
    """
    db = _get_db()
    if db is None:
        return {}
    try:
        result = {}
        for doc in db["raw_news"].find({"batch_date": batch_date}, {"_id": 0}):
            result[doc["ticker"]] = doc.get("articles", [])
        return result
    except Exception as exc:
        logger.warning(f"MongoDB read_raw_news failed: {exc}")
        return {}


def read_raw_news_ticker(batch_date: str, ticker: str) -> list:
    """Lee los articulos de un ticker concreto para una fecha."""
    db = _get_db()
    if db is None:
        return []
    try:
        doc = db["raw_news"].find_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {"_id": 0, "articles": 1},
        )
        return doc.get("articles", []) if doc else []
    except Exception as exc:
        logger.warning(f"MongoDB read_raw_news_ticker failed ({ticker}): {exc}")
        return []


# ─── ohlcv: datos OHLCV de yfinance (reemplaza raw/*.csv) ────────────────────

def upsert_ohlcv_bulk(batch_date: str, ticker: str, rows: list):
    """
    Guarda las filas OHLCV de un ticker para una fecha de ingestion.
    Cada fila: {date, open, high, low, close, volume}
    Sustituye el fichero raw/{DATE}/ohlcv.csv de S3.
    """
    try:
        db = _get_db()
        if db is None:
            return
        if not rows:
            return
        now = datetime.now(timezone.utc)
        # Un documento por (batch_date, ticker) con array de filas
        db["ohlcv"].update_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {
                "$set": {
                    "batch_date": batch_date,
                    "ticker":     ticker.upper(),
                    "rows":       rows,
                    "count":      len(rows),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_ohlcv_bulk failed ({ticker}): {exc}")


def read_ohlcv(batch_date: str) -> dict:
    """
    Lee todos los OHLCV de una fecha.
    Devuelve dict {ticker: [rows...]} compatible con lo que leia lambda_indicators.
    """
    db = _get_db()
    if db is None:
        return {}
    try:
        result = {}
        for doc in db["ohlcv"].find({"batch_date": batch_date}, {"_id": 0}):
            result[doc["ticker"]] = doc.get("rows", [])
        return result
    except Exception as exc:
        logger.warning(f"MongoDB read_ohlcv failed: {exc}")
        return {}


def read_ohlcv_ticker(batch_date: str, ticker: str) -> list:
    """Lee las filas OHLCV de un ticker concreto para una fecha."""
    db = _get_db()
    if db is None:
        return []
    try:
        doc = db["ohlcv"].find_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {"_id": 0, "rows": 1},
        )
        return doc.get("rows", []) if doc else []
    except Exception as exc:
        logger.warning(f"MongoDB read_ohlcv_ticker failed ({ticker}): {exc}")
        return []


# ─── etf_universe: lista editable de tickers (sustituye etf_universe.json en S3) ─

_ETF_DOC_ID = "default"


def get_etf_tickers() -> List[str]:
    """Devuelve tickers en mayusculas; lista vacia si Mongo no hay datos."""
    db = _get_db()
    if db is None:
        return []
    try:
        doc = db["etf_universe"].find_one({"_id": _ETF_DOC_ID})
        if not doc:
            doc = db["etf_universe"].find_one({})
        if not doc:
            return []
        raw = doc.get("tickers", [])
        if not isinstance(raw, list):
            return []
        return [str(t).strip().upper() for t in raw if t]
    except Exception as exc:
        logger.warning(f"MongoDB get_etf_tickers failed: {exc}")
        return []


def upsert_etf_universe(tickers: List[str], doc_id: str = _ETF_DOC_ID) -> None:
    """Guarda el universo ETF (reemplaza configuracion en bucket de config)."""
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        clean = [str(t).strip().upper() for t in tickers if t]
        db["etf_universe"].update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "_id": doc_id,
                    "tickers": clean,
                    "count": len(clean),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_etf_universe failed: {exc}")


# ─── bayesian_traces: JSON completo por dia (sustituye results/.../bayesian_trace.json)

def upsert_bayesian_trace(batch_date: str, trace: Dict[str, Any]) -> None:
    """Persiste la traza bayesiana completa (schema_version, tickers, model_config, ...)."""
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        db["bayesian_traces"].update_one(
            {"batch_date": batch_date},
            {
                "$set": {
                    "batch_date": batch_date,
                    "trace": trace,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_bayesian_trace failed ({batch_date}): {exc}")


def read_bayesian_trace(batch_date: str) -> Optional[Dict[str, Any]]:
    db = _get_db()
    if db is None:
        return None
    try:
        doc = db["bayesian_traces"].find_one({"batch_date": batch_date}, {"_id": 0, "trace": 1})
        return doc.get("trace") if doc else None
    except Exception as exc:
        logger.warning(f"MongoDB read_bayesian_trace failed: {exc}")
        return None


def _bayesian_report_doc_to_ticker_trace(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Reconstruye el subdocumento de traza por ticker desde bayesian_reports."""
    return {
        "raw_values": doc.get("raw_values", {}),
        "discretization": doc.get("discretization", {}),
        "sentiment_detail": doc.get("sentiment_detail", {}),
        "inference": {
            "signal": doc.get("signal"),
            "prob_up": doc.get("prob_up"),
            "prob_down": doc.get("prob_down"),
            "threshold_used": doc.get("threshold_used"),
        },
        "reasoning": doc.get("reasoning"),
    }


def read_bayesian_report(batch_date: str, ticker: str) -> Optional[Dict[str, Any]]:
    """Traza de un ticker desde bayesian_reports (fallback si bayesian_traces.tickers está vacío)."""
    db = _get_db()
    if db is None:
        return None
    try:
        doc = db["bayesian_reports"].find_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {"_id": 0},
        )
        return _bayesian_report_doc_to_ticker_trace(doc) if doc else None
    except Exception as exc:
        logger.warning(f"MongoDB read_bayesian_report failed ({ticker}): {exc}")
        return None


def list_bayesian_report_tickers(batch_date: str) -> list:
    db = _get_db()
    if db is None:
        return []
    try:
        return sorted(db["bayesian_reports"].distinct("ticker", {"batch_date": batch_date}))
    except Exception as exc:
        logger.warning(f"MongoDB list_bayesian_report_tickers failed: {exc}")
        return []


def distinct_raw_news_tickers(batch_date: str) -> list:
    db = _get_db()
    if db is None:
        return []
    try:
        return sorted(db["raw_news"].distinct("ticker", {"batch_date": batch_date}))
    except Exception as exc:
        logger.warning(f"MongoDB distinct_raw_news_tickers failed: {exc}")
        return []


# ─── news_filtered: titulares limpios generados por Bedrock ──────────────────

def upsert_filtered_news(batch_date: str, ticker: str, filtered_headlines: list, daily_context: str = ""):
    """Guarda los titulares ya filtrados/normalizados por Claude Haiku para un ticker."""
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        doc = {
            "batch_date":          batch_date,
            "ticker":              ticker.upper(),
            "filtered_headlines":  filtered_headlines,
            "daily_context":       daily_context,
            "headline_count":      len(filtered_headlines),
            "updated_at":          now,
        }
        db["news_filtered"].update_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_filtered_news failed ({ticker}): {exc}")


def read_filtered_news(batch_date: str) -> dict:
    """Devuelve {ticker: {"headlines": [...], "daily_context": str}} desde news_filtered."""
    try:
        db = _get_db()
        if db is None:
            return {}
        docs = list(db["news_filtered"].find({"batch_date": batch_date}))
        result = {}
        for doc in docs:
            ticker = doc.get("ticker", "")
            if ticker:
                result[ticker] = {
                    "headlines":     doc.get("filtered_headlines", []),
                    "daily_context": doc.get("daily_context", ""),
                }
        return result
    except Exception as exc:
        logger.warning(f"MongoDB read_filtered_news failed: {exc}")
        return {}


# ─── news: articulos con scoring FinBERT ─────────────────────────────────────

def upsert_news(batch_date: str, ticker: str, article: dict, sentiment_data: dict):
    """Inserta/actualiza un articulo con su scoring FinBERT."""
    try:
        db = _get_db()
        if db is None:
            return
        headline = article.get("headline", "")
        now = datetime.now(timezone.utc)
        doc = {
            "batch_date":    batch_date,
            "ticker":        ticker.upper(),
            "headline":      headline,
            "url":           article.get("url"),
            "datetime":      article.get("datetime"),
            "source":        article.get("source"),
            "sentiment":     sentiment_data.get("sentiment"),
            "confidence":    sentiment_data.get("confidence"),
            "justification": sentiment_data.get("justification"),
            "updated_at":    now,
        }
        db["news"].update_one(
            {"batch_date": batch_date, "ticker": ticker.upper(), "headline": headline},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_news failed ({ticker}): {exc}")


# ─── bayesian_reports: traza por ticker ──────────────────────────────────────

def upsert_bayesian_report(batch_date: str, ticker: str, ticker_trace: dict, model_version: str):
    """Inserta/actualiza la traza bayesiana de un ticker para una fecha."""
    try:
        db = _get_db()
        if db is None:
            return
        inference = ticker_trace.get("inference", {})
        now = datetime.now(timezone.utc)
        doc = {
            "batch_date":       batch_date,
            "ticker":           ticker.upper(),
            "signal":           inference.get("signal"),
            "prob_up":          inference.get("prob_up"),
            "prob_down":        inference.get("prob_down"),
            "threshold_used":   inference.get("threshold_used"),
            "raw_values":       ticker_trace.get("raw_values", {}),
            "discretization":   ticker_trace.get("discretization", {}),
            "sentiment_detail": ticker_trace.get("sentiment_detail", {}),
            "reasoning":        ticker_trace.get("reasoning"),
            "model_version":    model_version,
            "updated_at":       now,
        }
        db["bayesian_reports"].update_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_bayesian_report failed ({ticker}): {exc}")


# ─── reports: reporte diario completo ────────────────────────────────────────

def upsert_report(report_data: dict):
    """Inserta/actualiza el reporte diario completo."""
    try:
        db = _get_db()
        if db is None:
            return
        report_date = report_data.get("report_date")
        now = datetime.now(timezone.utc)
        doc = {
            "report_date":             report_date,
            "pipeline_health":         report_data.get("pipeline_health", {}),
            "summary":                 report_data.get("summary", {}),
            "backtesting_metrics":     report_data.get("backtesting_metrics", {}),
            "benchmark_comparison":    report_data.get("benchmark_comparison", {}),
            "top_signal_explanations": report_data.get("top_signal_explanations", []),
            "signal_diagnostics":      report_data.get("signal_diagnostics", {}),
            "backtesting_config":      report_data.get("backtesting_config", {}),
            "data_period_days":        report_data.get("data_period_days"),
            "updated_at":              now,
        }
        db["reports"].update_one(
            {"report_date": report_date},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_report failed ({report_date}): {exc}")


# ─── macro_news: noticias macroeconómicas globales ────────────────────────────

def upsert_macro_news(batch_date: str, articles: list):
    """
    Inserta noticias macroeconómicas globales en la colección macro_news.
    Deduplica por URL usando upsert.
    """
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        inserted = 0
        for art in articles:
            url = art.get("url", "")
            headline = art.get("headline", "")
            if not headline:
                continue
            doc = {
                "batch_date": batch_date,
                "headline":   headline,
                "summary":    art.get("summary", ""),
                "url":        url,
                "source":     art.get("source", ""),
                "datetime":   art.get("datetime", ""),
                "category":   art.get("category", "macro"),
                "query_tag":  art.get("query_tag", ""),
                "updated_at": now,
            }
            db["macro_news"].update_one(
                {"url": url} if url else {"batch_date": batch_date, "headline": headline},
                {"$set": doc, "$setOnInsert": {"created_at": now}},
                upsert=True,
            )
            inserted += 1
        logger.info(f"MongoDB upsert_macro_news: {inserted} artículos para {batch_date}")
    except Exception as exc:
        logger.warning(f"MongoDB upsert_macro_news failed: {exc}")


def read_macro_news(batch_date: str) -> list:
    """Devuelve todos los artículos macro de una fecha concreta."""
    try:
        db = _get_db()
        if db is None:
            return []
        return list(db["macro_news"].find({"batch_date": batch_date}))
    except Exception as exc:
        logger.warning(f"MongoDB read_macro_news failed: {exc}")
        return []


# ─── macro_context: MacroSentiment + RiskRegime ───────────────────────────────

def upsert_macro_context(batch_date: str, macro_sentiment: str, risk_regime: str,
                         macro_adjustment: float, detail: dict):
    """
    Persiste el contexto macro calculado por lambda_macro_context.
    Un único documento por batch_date (upsert).
    """
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        doc = {
            "batch_date":       batch_date,
            "macro_sentiment":  macro_sentiment,   # bullish / neutral / bearish
            "risk_regime":      risk_regime,        # RISK_ON / NEUTRAL / RISK_OFF
            "macro_adjustment": macro_adjustment,   # ej: +0.08
            "detail":           detail,             # desglose de inputs
            "updated_at":       now,
        }
        db["macro_context"].update_one(
            {"batch_date": batch_date},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        logger.info(f"MongoDB upsert_macro_context: {macro_sentiment}/{risk_regime} "
                    f"adj={macro_adjustment:+.3f} para {batch_date}")
    except Exception as exc:
        logger.warning(f"MongoDB upsert_macro_context failed: {exc}")


def read_macro_context(batch_date: str) -> dict:
    """Devuelve el contexto macro de una fecha. Vacío si no existe."""
    try:
        db = _get_db()
        if db is None:
            return {}
        doc = db["macro_context"].find_one({"batch_date": batch_date})
        return doc if doc else {}
    except Exception as exc:
        logger.warning(f"MongoDB read_macro_context failed: {exc}")
        return {}
