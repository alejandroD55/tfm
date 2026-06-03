"""
mongo_utils.py — Utilidades compartidas para MongoDB Atlas
============================================================
Patron singleton: un solo MongoClient por instancia Lambda (warm reuse).
Todas las escrituras son best-effort; un fallo en MongoDB NUNCA cancela
el pipeline principal.

Colecciones en la BD 'tfm':
  etf_universe      → copia legacy en Mongo (el pipeline lee etf_universe.json)
  watchlists        → cartera de seguimiento del usuario (documento _id 'default')
  raw_news          → articulos Finnhub sin clasificar (pre-FinBERT)
  ohlcv             → datos OHLCV diarios de yfinance
  news              → articulos con scoring FinBERT completo
  bayesian_reports  → traza por ticker: raw values, discretizacion, inferencia
  bayesian_traces   → JSON completo de traza bayesiana por batch_date (API /trace)
  reports           → reporte diario completo (backtesting, metricas, senales)
  quant_audit_reports → observabilidad cuantitativa agregada por report_date
  macro_news        → noticias macro globales (FED, inflacion, geopolitica…)
  macro_context     → MacroSentiment + RiskRegime calculados por batch_date
  feature_snapshots → vector de features por (batch_date, ticker) para modelos/API
  catalyst_events   → eventos catalizadores detectados (opcional)
  fundamental_snapshots → métricas fundamentales semanales (opcional)
  model_traces      → trazas por modelo (gbm_v1, bayesian_v1.2, …)

Indices recomendados (ejecutar via POST /mongo/setup-indexes):
  etf_universe:     {_id:1}
  raw_news:    {batch_date:1, ticker:1}
  ohlcv:       {batch_date:1, ticker:1, date:1}  (unico)
  news:        {batch_date:1, ticker:1}, {ticker:1, batch_date:-1}, headline:text
  bayesian_reports: {batch_date:1, ticker:1} unico, {ticker:1, batch_date:-1}
  bayesian_traces:  {batch_date:1} unico
  reports:     {report_date:1} unico
  quant_audit_reports: {report_date:1} unico
  macro_news:  {batch_date:1}, {category:1}
  macro_context: {batch_date:1} unico
  feature_snapshots: {batch_date:1, ticker:1} unico, {ticker:1, batch_date:-1}
  catalyst_events: {batch_date:1, ticker:1}
  fundamental_snapshots: {ticker:1, week_start:1} unico
  model_traces: {batch_date:1, model_id:1} unico
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


# ─── watchlist / etf_universe: cartera de seguimiento → pipeline ───────────────

_ETF_DOC_ID = "default"
_WATCHLIST_DOC_ID = "default"
_ETF_UNIVERSE_CACHE: Optional[List[str]] = None
_DEFAULT_WATCHLIST_SEED = [
    "SPY", "IWM", "XLE", "GLD",
]


def _etf_universe_json_paths() -> List[str]:
    """Rutas candidatas para etf_universe.json (local, Lambda, repo)."""
    paths: List[str] = []
    env_path = os.getenv("ETF_UNIVERSE_JSON")
    if env_path:
        paths.append(env_path)
    here = os.path.dirname(os.path.abspath(__file__))
    paths.extend([
        os.path.join(here, "etf_universe.json"),
        os.path.join(here, "..", "etf_universe.json"),
        "/var/task/etf_universe.json",
    ])
    seen = set()
    out: List[str] = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _load_tickers_from_s3() -> List[str]:
    bucket = os.getenv("CONFIG_BUCKET", "tfm-unir-config")
    key = os.getenv("ETF_UNIVERSE_S3_KEY", "etf_universe.json")
    try:
        import boto3
        region = os.getenv("AWS_REGION", "eu-north-1")
        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return _clean_ticker_list(data.get("tickers", []))
    except Exception as exc:
        logger.warning(f"No se pudo leer s3://{bucket}/{key}: {exc}")
        return []


def load_etf_universe_tickers() -> List[str]:
    """Lee tickers desde etf_universe.json (fichero local o S3 config bucket)."""
    global _ETF_UNIVERSE_CACHE
    if _ETF_UNIVERSE_CACHE is not None:
        return list(_ETF_UNIVERSE_CACHE)

    tickers: List[str] = []
    for path in _etf_universe_json_paths():
        if not os.path.isfile(path):
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            tickers = _clean_ticker_list(data.get("tickers", []))
            if tickers:
                logger.info(f"etf_universe cargado desde {path}: {tickers}")
                break
        except Exception as exc:
            logger.warning(f"Error leyendo {path}: {exc}")

    if not tickers:
        tickers = _load_tickers_from_s3()
        if tickers:
            logger.info(f"etf_universe cargado desde S3: {tickers}")

    if not tickers:
        tickers = list(_DEFAULT_WATCHLIST_SEED)
        logger.warning(f"etf_universe: usando seed por defecto {tickers}")

    _ETF_UNIVERSE_CACHE = tickers
    return list(tickers)


def _clean_ticker_list(tickers: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for t in tickers or []:
        u = str(t).strip().upper()
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _read_legacy_etf_universe_tickers() -> List[str]:
    db = _get_db()
    if db is None:
        return []
    try:
        doc = db["etf_universe"].find_one({"_id": _ETF_DOC_ID}) or db["etf_universe"].find_one({})
        if not doc:
            return []
        raw = doc.get("tickers", [])
        return _clean_ticker_list(raw if isinstance(raw, list) else [])
    except Exception as exc:
        logger.warning(f"MongoDB _read_legacy_etf_universe_tickers failed: {exc}")
        return []


def get_watchlist() -> Optional[Dict[str, Any]]:
    db = _get_db()
    if db is None:
        return None
    try:
        doc = db["watchlists"].find_one({"_id": _WATCHLIST_DOC_ID}, {"_id": 0})
        if doc:
            doc["_id"] = _WATCHLIST_DOC_ID
        return doc
    except Exception as exc:
        logger.warning(f"MongoDB get_watchlist failed: {exc}")
        return None


def get_watchlist_tickers() -> List[str]:
    doc = get_watchlist()
    if not doc:
        return []
    return _clean_ticker_list(doc.get("tickers", []))


def upsert_watchlist(
    tickers: List[str],
    name: str = "Cartera de seguimiento",
    doc_id: str = _WATCHLIST_DOC_ID,
) -> List[str]:
    """Guarda la cartera y sincroniza etf_universe (lo que leen las Lambdas)."""
    clean = _clean_ticker_list(tickers)
    if not clean:
        return []
    try:
        db = _get_db()
        if db is None:
            return clean
        now = datetime.now(timezone.utc)
        db["watchlists"].update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "_id": doc_id,
                    "name": name,
                    "tickers": clean,
                    "count": len(clean),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        upsert_etf_universe(clean, doc_id=_ETF_DOC_ID)
        logger.info(f"Watchlist actualizada: {len(clean)} tickers")
        return clean
    except Exception as exc:
        logger.warning(f"MongoDB upsert_watchlist failed: {exc}")
        return clean


def ensure_watchlist_initialized() -> List[str]:
    """Devuelve tickers de la cartera; si no existe, inicializa desde etf_universe o seed."""
    existing = get_watchlist_tickers()
    if existing:
        return existing
    legacy = _read_legacy_etf_universe_tickers()
    seed = legacy or list(_DEFAULT_WATCHLIST_SEED)
    return upsert_watchlist(seed)


def get_etf_tickers() -> List[str]:
    """Tickers para el pipeline: etf_universe.json (fichero empaquetado o S3 config)."""
    return load_etf_universe_tickers()


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


def add_watchlist_ticker(ticker: str) -> List[str]:
    current = ensure_watchlist_initialized()
    sym = str(ticker).strip().upper()
    if not sym or sym in current:
        return current
    return upsert_watchlist(current + [sym])


def remove_watchlist_ticker(ticker: str) -> List[str]:
    current = ensure_watchlist_initialized()
    sym = str(ticker).strip().upper()
    filtered = [t for t in current if t != sym]
    if not filtered:
        return current
    return upsert_watchlist(filtered)


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
    inference = doc.get("inference") or {
        "signal": doc.get("signal"),
        "prob_up": doc.get("prob_up"),
        "prob_down": doc.get("prob_down"),
        "threshold_used": doc.get("threshold_used"),
    }
    return {
        "raw_values": doc.get("raw_values", {}),
        "discretization": doc.get("discretization", {}),
        "sentiment_detail": doc.get("sentiment_detail", {}),
        "inference": inference,
        "contribution_analysis": doc.get("contribution_analysis", {}),
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

def upsert_filtered_news(
    batch_date: str,
    ticker: str,
    filtered_headlines: list,
    daily_context: str = "",
    filtered_articles: Optional[list] = None,
):
    """Guarda resúmenes Bedrock/Groq. filtered_articles: [{original_headline, summary}, ...]."""
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
        if filtered_articles:
            doc["filtered_articles"] = filtered_articles
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
            "inference":        inference,
            "contribution_analysis": ticker_trace.get("contribution_analysis", {}),
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
            "pipeline_start":          report_data.get("pipeline_start"),
            "pipeline_end":            report_data.get("pipeline_end"),
            "generated_at":            report_data.get("generated_at"),
            "pipeline_health":         report_data.get("pipeline_health", {}),
            "summary":                 report_data.get("summary", {}),
            "backtesting_metrics":     report_data.get("backtesting_metrics", {}),
            "benchmark_comparison":    report_data.get("benchmark_comparison", {}),
            "top_signal_explanations": report_data.get("top_signal_explanations", []),
            "signal_diagnostics":      report_data.get("signal_diagnostics", {}),
            "backtesting_config":      report_data.get("backtesting_config", {}),
            "data_period_days":        report_data.get("data_period_days"),
            "exposure_backtesting_metrics": report_data.get("exposure_backtesting_metrics", {}),
            "exposure_backtesting_diagnostics": report_data.get(
                "exposure_backtesting_diagnostics", {}
            ),
            "exposure_vs_binary_comparison": report_data.get(
                "exposure_vs_binary_comparison", {}
            ),
            "inference_engine":        report_data.get("inference_engine"),
            "updated_at":              now,
        }
        db["reports"].update_one(
            {"report_date": report_date},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_report failed ({report_date}): {exc}")


def upsert_quant_audit_report(report_date: str, audit_report: dict):
    """Persiste el reporte agregado de observabilidad cuantitativa."""
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        doc = dict(audit_report or {})
        doc["report_date"] = report_date
        doc["updated_at"] = now
        db["quant_audit_reports"].update_one(
            {"report_date": report_date},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_quant_audit_report failed ({report_date}): {exc}")


def read_quant_audit_report(report_date: str) -> Optional[Dict[str, Any]]:
    """Lee el reporte agregado de observabilidad cuantitativa."""
    db = _get_db()
    if db is None:
        return None
    try:
        doc = db["quant_audit_reports"].find_one({"report_date": report_date}, {"_id": 0})
        return doc if doc else None
    except Exception as exc:
        logger.warning(f"MongoDB read_quant_audit_report failed ({report_date}): {exc}")
        return None


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


# ─── feature_snapshots ───────────────────────────────────────────────────────

def upsert_feature_snapshot(batch_date: str, ticker: str, snapshot: Dict[str, Any]) -> None:
    """Persiste feature_snapshot por (batch_date, ticker)."""
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        ticker_u = ticker.upper()
        doc = dict(snapshot or {})
        doc["batch_date"] = batch_date
        doc["ticker"] = ticker_u
        doc["updated_at"] = now
        db["feature_snapshots"].update_one(
            {"batch_date": batch_date, "ticker": ticker_u},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_feature_snapshot failed ({ticker}): {exc}")


def read_feature_snapshot(batch_date: str, ticker: str) -> Optional[Dict[str, Any]]:
    db = _get_db()
    if db is None:
        return None
    try:
        doc = db["feature_snapshots"].find_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {"_id": 0},
        )
        return doc if doc else None
    except Exception as exc:
        logger.warning(f"MongoDB read_feature_snapshot failed ({ticker}): {exc}")
        return None


def list_feature_snapshot_tickers(batch_date: str) -> List[str]:
    db = _get_db()
    if db is None:
        return []
    try:
        return sorted(
            db["feature_snapshots"].distinct("ticker", {"batch_date": batch_date})
        )
    except Exception as exc:
        logger.warning(f"MongoDB list_feature_snapshot_tickers failed: {exc}")
        return []


# ─── catalyst_events (opcional) ──────────────────────────────────────────────

def upsert_catalyst_events(
    batch_date: str, ticker: str, events: List[Dict[str, Any]]
) -> None:
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        db["catalyst_events"].update_one(
            {"batch_date": batch_date, "ticker": ticker.upper()},
            {
                "$set": {
                    "batch_date": batch_date,
                    "ticker": ticker.upper(),
                    "events": events or [],
                    "count": len(events or []),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_catalyst_events failed ({ticker}): {exc}")


# ─── fundamental_snapshots (semanal, opcional) ─────────────────────────────

def upsert_fundamental_snapshot(
    ticker: str, week_start: str, fundamentals: Dict[str, Any]
) -> None:
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        doc = dict(fundamentals or {})
        doc["ticker"] = ticker.upper()
        doc["week_start"] = week_start
        doc["updated_at"] = now
        db["fundamental_snapshots"].update_one(
            {"ticker": ticker.upper(), "week_start": week_start},
            {"$set": doc, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_fundamental_snapshot failed ({ticker}): {exc}")


def read_fundamental_snapshot(ticker: str, week_start: str) -> Optional[Dict[str, Any]]:
    db = _get_db()
    if db is None:
        return None
    try:
        return db["fundamental_snapshots"].find_one(
            {"ticker": ticker.upper(), "week_start": week_start},
            {"_id": 0},
        )
    except Exception as exc:
        logger.warning(f"MongoDB read_fundamental_snapshot failed: {exc}")
        return None


# ─── model_traces (GBM / comparación de modelos) ─────────────────────────────

def upsert_model_trace(batch_date: str, model_id: str, trace: Dict[str, Any]) -> None:
    try:
        db = _get_db()
        if db is None:
            return
        now = datetime.now(timezone.utc)
        db["model_traces"].update_one(
            {"batch_date": batch_date, "model_id": model_id},
            {
                "$set": {
                    "batch_date": batch_date,
                    "model_id": model_id,
                    "trace": trace,
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
    except Exception as exc:
        logger.warning(f"MongoDB upsert_model_trace failed ({model_id}): {exc}")


def read_model_trace(batch_date: str, model_id: str) -> Optional[Dict[str, Any]]:
    db = _get_db()
    if db is None:
        return None
    try:
        doc = db["model_traces"].find_one(
            {"batch_date": batch_date, "model_id": model_id},
            {"_id": 0, "trace": 1},
        )
        return doc.get("trace") if doc else None
    except Exception as exc:
        logger.warning(f"MongoDB read_model_trace failed: {exc}")
        return None
