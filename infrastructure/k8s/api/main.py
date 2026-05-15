"""
TFM Dashboard API - FastAPI
===========================
Endpoints:
  GET /health
  GET /reports                      - Lista fechas (reports + trazas + raw_news reciente)
  GET /reports/{date}               - Report completo (MongoDB)
  GET /trace/{date}                 - Traza bayesiana completa (MongoDB bayesian_traces)
  GET /trace/{date}/{ticker}        - Traza por ticker
  GET /model                        - Config del modelo bayesiano
  GET /tickers                      - Lista ETFs (cartera watchlist)
  GET /watchlist                    - Cartera de seguimiento
  PUT /watchlist                    - Reemplaza la cartera
  POST /watchlist/tickers           - Anade un ticker
  DELETE /watchlist/tickers/{sym}   - Quita un ticker
  GET /watchlist/coverage           - Cobertura pipeline por ticker/fecha
  POST /watchlist/run-pipeline      - Pipeline para toda la cartera (o huecos)
  POST /mongo/etf-universe          - Actualiza universo (sincroniza watchlist)
  GET /raw/{date}/news/{ticker}     - Noticias raw (MongoDB raw_news)
  GET /raw/{date}/ohlcv/{ticker}    - OHLCV (MongoDB ohlcv)
  POST /pipeline/run                - Lanza pipeline para ticker(s)
  GET /pipeline/status              - Estado de una ejecucion Step Functions
  GET /search/instruments           - Busca ETFs y fondos via Finnhub (nuevo)
  GET /instrument/{symbol}/profile  - Perfil detallado de un instrumento (nuevo)
  GET /sentiment/{date}/{ticker}    - Sentimiento detallado
  GET /indicators/{date}/{ticker}   - Indicadores tecnicos
  GET /files                        - 410 (reemplazado por Mongo)
  GET /files/presign                - 410
  GET /stats                        - 410 (usar GET /mongo/stats)
"""

import os
import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, List

import boto3
from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tfm-api")

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")
PRESIGN_TTL = int(os.getenv("PRESIGN_TTL_SEC", "900"))
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN", "")
MONGODB_DB = os.getenv("MONGODB_DB", "tfm")

try:
    from mongo_utils import (
        get_etf_tickers,
        upsert_etf_universe,
        get_watchlist,
        get_watchlist_tickers,
        upsert_watchlist,
        ensure_watchlist_initialized,
        add_watchlist_ticker,
        remove_watchlist_ticker,
        read_bayesian_trace,
        read_bayesian_report,
        list_bayesian_report_tickers,
        read_raw_news_ticker,
        read_ohlcv_ticker,
    )
except ImportError:
    get_etf_tickers = None  # type: ignore[misc, assignment]
    upsert_etf_universe = None  # type: ignore[misc, assignment]
    get_watchlist = None  # type: ignore[misc, assignment]
    get_watchlist_tickers = None  # type: ignore[misc, assignment]
    upsert_watchlist = None  # type: ignore[misc, assignment]
    ensure_watchlist_initialized = None  # type: ignore[misc, assignment]
    add_watchlist_ticker = None  # type: ignore[misc, assignment]
    remove_watchlist_ticker = None  # type: ignore[misc, assignment]
    read_bayesian_trace = None  # type: ignore[misc, assignment]
    read_bayesian_report = None  # type: ignore[misc, assignment]
    list_bayesian_report_tickers = None  # type: ignore[misc, assignment]
    read_raw_news_ticker = None  # type: ignore[misc, assignment]
    read_ohlcv_ticker = None  # type: ignore[misc, assignment]

sfn = boto3.client("stepfunctions", region_name=AWS_REGION)
lmb = boto3.client("lambda", region_name=AWS_REGION)
secrets_api = boto3.client("secretsmanager", region_name=AWS_REGION)

_PIPELINE_STAGE_ORDER = [
    "ingestion",
    "parallel",
    "bayesian",
    "report",
]

_STATE_TO_STAGE = {
    "lambda_ingestion": "ingestion",
    "lambda_sentiment": "parallel",
    "lambda_indicators": "parallel",
    "parallel_analysis": "parallel",
    "lambda_bayesian": "bayesian",
    "lambda_report": "report",
}

# Cache de claves externas (se leen una vez por instancia del pod)
_finnhub_key_cache: Optional[str] = None

# ─── MongoDB client (singleton por pod) ──────────────────────────────────────
_mongo_client = None

def _get_mongo_db():
    global _mongo_client
    if _mongo_client is None:
        uri = os.getenv("MONGODB_URI")
        if not uri:
            try:
                resp = secrets_api.get_secret_value(SecretId="mongodb/connection_string")
                secret = json.loads(resp["SecretString"])
                uri = secret.get("connection_string") or secret.get("uri")
            except Exception as e:
                logger.warning(f"mongodb/connection_string no disponible: {e}")
        if not uri:
            return None
        try:
            from pymongo import MongoClient
            _mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            _mongo_client.admin.command("ping")
            logger.info("MongoDB Atlas: conexion establecida en pod API")
        except Exception as e:
            logger.warning(f"MongoDB no disponible: {e}")
            _mongo_client = None
            return None
    try:
        return _mongo_client[MONGODB_DB]
    except Exception:
        return None

def _require_mongo():
    db = _get_mongo_db()
    if db is None:
        raise HTTPException(status_code=503, detail="MongoDB no disponible. Verifica mongodb/connection_string en Secrets Manager.")
    return db

def _serialize_doc(doc: dict) -> dict:
    if doc is None:
        return {}
    result = {}
    for k, v in doc.items():
        if k == "_id":
            result["_id"] = str(v)
        elif hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        elif isinstance(v, dict):
            result[k] = _serialize_doc(v)
        elif isinstance(v, list):
            result[k] = [_serialize_doc(i) if isinstance(i, dict) else i for i in v]
        else:
            result[k] = v
    return result


def _get_finnhub_key() -> str:
    """Lee la API Key de Finnhub desde Secrets Manager (con cache por pod)."""
    global _finnhub_key_cache
    if not _finnhub_key_cache:
        try:
            resp = secrets_api.get_secret_value(SecretId="finnhub/api_key")
            _finnhub_key_cache = json.loads(resp["SecretString"])["api_key"]
        except Exception as e:
            logger.error(f"Error leyendo finnhub/api_key: {e}")
            raise HTTPException(
                status_code=503,
                detail="Finnhub API key no configurada en Secrets Manager",
            )
    return _finnhub_key_cache


def _finnhub_get(path: str) -> dict:
    """Realiza una peticion GET a la API de Finnhub."""
    key = _get_finnhub_key()
    sep = "&" if "?" in path else "?"
    url = f"https://finnhub.io/api/v1{path}{sep}token={key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise HTTPException(status_code=e.code, detail=f"Finnhub error: {e.reason}")
    except Exception as e:
        raise HTTPException(
            status_code=502, detail=f"Error contactando Finnhub: {str(e)}"
        )


app = FastAPI(
    title="TFM Dashboard API",
    description="API de observabilidad e interpretabilidad del sistema de trading bayesiano",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "OPTIONS"],
    allow_headers=["*"],
)


# ─── Auth ─────────────────────────────────────────────────────────────────────


def check_api_key(x_api_key: str = Header(default="")):
    if DASHBOARD_API_KEY and x_api_key != DASHBOARD_API_KEY:
        raise HTTPException(status_code=403, detail="API Key invalida o ausente")


# ─── Helpers: lecturas desde Mongo (mongo_utils en la imagen API) ────────────


def _require_mongo_pipeline_helpers():
    if read_bayesian_trace is None or read_raw_news_ticker is None or read_ohlcv_ticker is None:
        raise HTTPException(
            status_code=503,
            detail="mongo_utils no disponible en la imagen de la API",
        )


def _tickers_in_mongo_collection(db, collection: str, batch_date: str) -> list[str]:
    """Lista tickers con documento para batch_date en una coleccion Mongo."""
    return sorted(
        {
            str(d["ticker"]).upper()
            for d in db[collection].find({"batch_date": batch_date}, {"ticker": 1})
            if d.get("ticker")
        }
    )


def _pipeline_coverage(db, batch_date: str, ticker: str) -> dict:
    """Estado de datos por capa del pipeline para un ticker/fecha."""
    ticker_u = ticker.upper()
    trace = None
    if read_bayesian_trace:
        trace = read_bayesian_trace(batch_date)  # type: ignore[misc]
    trace_tickers = sorted(trace.get("tickers", {}).keys()) if trace else []
    report_tickers: list[str] = []
    if list_bayesian_report_tickers:
        try:
            report_tickers = list_bayesian_report_tickers(batch_date)  # type: ignore[misc]
        except Exception:
            report_tickers = []
    universe = []
    if get_etf_tickers:
        try:
            universe = [t.upper() for t in get_etf_tickers()]  # type: ignore[misc]
        except Exception:
            universe = []
    raw = _tickers_in_mongo_collection(db, "raw_news", batch_date)
    filtered = _tickers_in_mongo_collection(db, "news_filtered", batch_date)
    return {
        "batch_date": batch_date,
        "ticker": ticker_u,
        "has_bayesian_trace_doc": trace is not None,
        "ticker_in_trace": ticker_u in trace_tickers if trace else False,
        "tickers_in_trace": trace_tickers,
        "tickers_in_bayesian_reports": report_tickers,
        "ticker_in_bayesian_reports": ticker_u in report_tickers,
        "tickers_with_raw_news": raw,
        "tickers_with_news_filtered": filtered,
        "ticker_has_raw_news": ticker_u in raw,
        "ticker_has_news_filtered": ticker_u in filtered,
        "in_etf_universe": ticker_u in universe,
        "etf_universe": universe,
    }


def _trace_not_found_response(code: str, message: str, coverage: dict) -> JSONResponse:
    return JSONResponse(
        status_code=404,
        content={
            "error": code,
            "message": message,
            "coverage": coverage,
        },
    )


def _load_bayesian_trace(date: str) -> dict | None:
    """Carga traza o None. Usa _require_mongo (503) si la BD no responde."""
    _require_mongo_pipeline_helpers()
    _require_mongo()
    return read_bayesian_trace(date)  # type: ignore[misc]


def _bayesian_trace_for_date(date: str) -> dict:
    trace = _load_bayesian_trace(date)
    if not trace:
        raise HTTPException(
            status_code=404,
            detail=f"No hay traza en MongoDB (coleccion bayesian_traces) para {date}",
        )
    return trace


# ─── Sistema ──────────────────────────────────────────────────────────────────


@app.get("/health", tags=["Sistema"])
def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "2.0.0",
    }


# ─── Reports ──────────────────────────────────────────────────────────────────


@app.get("/reports", tags=["Reports"])
def list_reports(x_api_key: str = Header(default="")):
    """
    Fechas para el selector del dashboard: unión de
    - `reports.report_date` (pipeline llegó a lambda_report), y
    - `bayesian_traces.batch_date` (hay traza aunque el reporte falte o falle).

    Las ejecuciones de Step Functions por sí solas no generan entradas aquí:
    hace falta al menos datos escritos en Mongo.
    """
    check_api_key(x_api_key)
    try:
        db = _require_mongo()

        def norm_date(v) -> str | None:
            if not v:
                return None
            s = str(v)[:10]
            return s if len(s) == 10 else None

        trace_dates = {
            d
            for x in db["bayesian_traces"].distinct("batch_date")
            if (d := norm_date(x))
        }

        by_date: dict[str, dict] = {}

        for doc in (
            db["reports"]
            .find({}, {"report_date": 1, "updated_at": 1, "created_at": 1})
            .sort("report_date", -1)
            .limit(500)
        ):
            d = norm_date(doc.get("report_date"))
            if not d:
                continue
            lm = doc.get("updated_at") or doc.get("created_at")
            ts = lm.isoformat() if hasattr(lm, "isoformat") and lm else None
            row = {
                "date": d,
                "storage": "mongo",
                "lastModified": ts,
                "has_trace": d in trace_dates,
            }
            prev = by_date.get(d)
            if not prev or (ts and prev.get("lastModified") and ts > prev["lastModified"]):
                by_date[d] = row

        for d in trace_dates:
            if d in by_date:
                continue
            doc = db["bayesian_traces"].find_one(
                {"batch_date": d}, {"updated_at": 1, "created_at": 1}
            )
            lm = (doc or {}).get("updated_at") or (doc or {}).get("created_at")
            ts = lm.isoformat() if lm and hasattr(lm, "isoformat") else None
            by_date[d] = {
                "date": d,
                "storage": "mongo",
                "lastModified": ts,
                "has_trace": True,
            }

        raw_dates = sorted(
            {
                d
                for x in db["raw_news"].distinct("batch_date")
                if (d := norm_date(x))
            },
            reverse=True,
        )[:120]
        for d in raw_dates:
            if d in by_date:
                continue
            by_date[d] = {
                "date": d,
                "storage": "mongo",
                "lastModified": None,
                "has_trace": d in trace_dates,
            }

        dates = sorted(by_date.values(), key=lambda x: x["date"], reverse=True)[:500]
        return {"dates": dates, "total": len(dates)}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("list_reports error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/reports/{date}", tags=["Reports"])
def get_report(date: str, x_api_key: str = Header(default="")):
    """Reporte diario completo desde MongoDB (coleccion reports)."""
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    db = _require_mongo()
    doc = db["reports"].find_one({"report_date": date})
    if not doc:
        raise HTTPException(
            status_code=404, detail=f"No hay reporte en MongoDB para {date}"
        )
    return _serialize_doc(doc)


# ─── Trazabilidad bayesiana (NUEVO) ───────────────────────────────────────────


@app.get("/trace/{date}", tags=["Trazabilidad"])
def get_trace(date: str, x_api_key: str = Header(default="")):
    """
    Traza bayesiana completa del dia: configuracion del modelo, evidencias raw,
    estados discretizados, probabilidades y razonamiento por ticker.
    Generado por lambda_bayesian en MongoDB (coleccion bayesian_traces).
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    return _bayesian_trace_for_date(date)


@app.get("/trace/{date}/{ticker}/coverage", tags=["Trazabilidad"])
def get_trace_coverage(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
  Diagnostico: que capas del pipeline tienen datos para ticker+fecha
  (raw_news, news_filtered, bayesian_traces) sin exigir traza completa.
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    db = _require_mongo()
    return _pipeline_coverage(db, date, ticker)


@app.get("/trace/{date}/{ticker}", tags=["Trazabilidad"])
def get_trace_ticker(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
    Traza bayesiana de un ticker especifico para una fecha.
    Incluye: valores raw, discretizacion, distribucion de sentimiento,
    probabilidades posteriores y razonamiento textual.

    404 con cuerpo JSON si falta la traza o el ticker no fue procesado por lambda_bayesian.
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    db = _require_mongo()
    ticker_upper = ticker.upper()
    coverage = _pipeline_coverage(db, date, ticker_upper)

    trace = _load_bayesian_trace(date)
    ticker_trace = None
    trace_source = "bayesian_traces"

    if trace and ticker_upper in trace.get("tickers", {}):
        ticker_trace = trace["tickers"][ticker_upper]
    elif read_bayesian_report:
        ticker_trace = read_bayesian_report(date, ticker_upper)  # type: ignore[misc]
        if ticker_trace:
            trace_source = "bayesian_reports"

    if ticker_trace is not None:
        return {
            "date": date,
            "ticker": ticker_upper,
            "model_config": (trace or {}).get("model_config"),
            "execution": (trace or {}).get("execution"),
            "trace": ticker_trace,
            "audit_notes": (trace or {}).get("audit_notes"),
            "source": trace_source,
            "coverage": coverage,
        }

    if not trace:
        return _trace_not_found_response(
            "no_trace_for_date",
            f"No existe documento bayesian_traces para {date}. "
            "El pipeline debe completar lambda_bayesian (y guardar en Mongo).",
            coverage,
        )

    avail = sorted(trace.get("tickers", {}).keys())
    reports_avail = coverage.get("tickers_in_bayesian_reports") or []
    hint = ""
    if not coverage["ticker_has_raw_news"]:
        hint = (
            f" {ticker_upper} no tiene raw_news en {date} "
            f"(no paso por ingestion o no esta en etf_universe de Mongo)."
        )
    elif not coverage["ticker_has_news_filtered"]:
        hint = f" Hay raw_news pero falta news_filtered (lambda_news_filter)."
    elif not coverage["in_etf_universe"]:
        hint = f" {ticker_upper} no esta en etf_universe de MongoDB."
    elif trace and not avail and reports_avail:
        hint = (
            " La traza agregada existe pero tickers esta vacio "
            f"(lambda_bayesian no genero senales). Informes por ticker: {reports_avail}."
        )
    elif trace and not avail:
        exec_meta = (trace.get("execution") or {})
        skipped = exec_meta.get("skipped_detail") or []
        if skipped:
            hint = f" Todos los tickers fueron omitidos: {skipped[:5]}."
        else:
            hint = (
                " Sin tickers en Aurora (sentiment_scores/technical_indicators) "
                f"para {date} o indicators incompletos."
            )
    return _trace_not_found_response(
        "ticker_not_in_trace",
        f"Ticker '{ticker_upper}' no esta en la traza de {date}.{hint} "
        f"Tickers en traza: {avail}. Informes: {reports_avail}",
        coverage,
    )


# ─── Configuracion del modelo (NUEVO) ─────────────────────────────────────────


@app.get("/model", tags=["Modelo Bayesiano"])
def get_model_config(
    date: str = Query(
        default=None, description="Fecha del trace (ultima disponible si no se indica)"
    ),
    x_api_key: str = Header(default=""),
):
    """
    Devuelve la configuracion completa del modelo bayesiano:
    - Thresholds de discretizacion (RSI, volatilidad, tendencia)
    - Thresholds de senal (BUY/SELL/HOLD)
    - Distribuciones prior
    - CPT completa de MarketDirection
    - Limitaciones conocidas del modelo
    """
    check_api_key(x_api_key)
    try:
        # Encontrar el trace mas reciente si no se especifica fecha
        if not date:
            db = _require_mongo()
            dates_found = [
                d
                for d in db["bayesian_traces"].distinct("batch_date")
                if d and len(str(d)) == 10
            ]
            if not dates_found:
                raise HTTPException(status_code=404, detail="No hay traces disponibles")
            date = sorted([str(x)[:10] for x in dates_found], reverse=True)[0]

        trace = _bayesian_trace_for_date(date)
        return {
            "source_date": date,
            "schema_version": trace.get("schema_version"),
            "model_config": trace.get("model_config"),
            "audit_notes": trace.get("audit_notes"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_model_config error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Sentimiento detallado (NUEVO) ────────────────────────────────────────────


@app.get("/sentiment/{date}/{ticker}", tags=["Trazabilidad"])
def get_sentiment_detail(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
    Distribucion completa de sentimientos FinBERT para un ticker y fecha.
    Muestra todos los headlines analizados, no solo el dominante.
    """
    check_api_key(x_api_key)
    try:
        trace = _bayesian_trace_for_date(date)
        ticker_upper = ticker.upper()
        ticker_data = trace.get("tickers", {}).get(ticker_upper)
        if not ticker_data:
            raise HTTPException(
                status_code=404,
                detail=f"Ticker {ticker_upper} no encontrado para {date}",
            )
        return {
            "date": date,
            "ticker": ticker_upper,
            "sentiment_detail": ticker_data.get("sentiment_detail", {}),
            "used_in_inference": ticker_data.get("discretization", {}).get(
                "sentiment_state"
            ),
            "limitation": (
                "Solo el headline con mayor confidence score influye en la inferencia. "
                "Los demas titulares son trazados pero no usados."
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_sentiment_detail error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Indicadores tecnicos raw (NUEVO) ─────────────────────────────────────────


@app.get("/indicators/{date}/{ticker}", tags=["Trazabilidad"])
def get_indicators_detail(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
    Valores crudos de los indicadores tecnicos y su discretizacion para un ticker.
    Muestra el 'antes' (valor numerico) y el 'despues' (estado discreto) de cada variable.
    """
    check_api_key(x_api_key)
    try:
        trace = _bayesian_trace_for_date(date)
        ticker_upper = ticker.upper()
        ticker_data = trace.get("tickers", {}).get(ticker_upper)
        if not ticker_data:
            raise HTTPException(
                status_code=404,
                detail=f"Ticker {ticker_upper} no encontrado para {date}",
            )

        model_cfg = trace.get("model_config", {}).get("discretization", {})
        return {
            "date": date,
            "ticker": ticker_upper,
            "raw_values": ticker_data.get("raw_values", {}),
            "discretized": ticker_data.get("discretization", {}),
            "discretization_rules": {
                "rsi": model_cfg.get("rsi", {}),
                "trend": model_cfg.get("trend", {}),
                "volatility": model_cfg.get("volatility", {}),
            },
            "reasoning": ticker_data.get("reasoning"),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_indicators_detail error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Files ────────────────────────────────────────────────────────────────────


@app.get("/files", tags=["S3"])
def list_files(
    prefix: str = Query(default="", description="Prefijo S3"),
    maxKeys: int = Query(default=200, ge=1, le=1000),
    delimiter: str = Query(default="/"),
    continuationToken: str = Query(default=None),
    x_api_key: str = Header(default=""),
):
    check_api_key(x_api_key)
    raise HTTPException(
        status_code=410,
        detail="Descontinuado: el datalake ya no usa S3. Usa GET /mongo/reports, /mongo/stats y GET /raw/*.",
    )


@app.get("/files/presign", tags=["S3"])
def presign(
    key: str = Query(...),
    ttl: int = Query(default=PRESIGN_TTL),
    x_api_key: str = Header(default=""),
):
    check_api_key(x_api_key)
    raise HTTPException(
        status_code=410,
        detail="Descontinuado: no hay URLs firmadas S3. Lee datos via GET /reports y /trace.",
    )


# ─── Tickers ──────────────────────────────────────────────────────────────────


@app.get("/tickers", tags=["Tickers"])
def list_tickers(x_api_key: str = Header(default="")):
    """Lista la cartera de seguimiento (watchlist), usada por pipeline y explorador."""
    check_api_key(x_api_key)
    if not ensure_watchlist_initialized:
        raise HTTPException(status_code=503, detail="mongo_utils no disponible")
    try:
        tickers = ensure_watchlist_initialized()  # type: ignore[misc]
        wl = get_watchlist() if get_watchlist else None  # type: ignore[misc]
        return {
            "tickers": tickers,
            "total": len(tickers),
            "source": "watchlist",
            "name": (wl or {}).get("name", "Cartera de seguimiento"),
            "updated_at": (wl or {}).get("updated_at"),
        }
    except Exception as e:
        logger.exception("list_tickers error")
        raise HTTPException(status_code=500, detail=str(e))


class EtfUniverseBody(BaseModel):
    tickers: List[str]


class WatchlistBody(BaseModel):
    tickers: List[str]
    name: Optional[str] = "Cartera de seguimiento"


class WatchlistTickerBody(BaseModel):
    ticker: str


class WatchlistRunBody(BaseModel):
    batch_date: Optional[str] = None
    tickers: Optional[List[str]] = None
    only_missing: bool = False


def _require_watchlist_helpers():
    if not ensure_watchlist_initialized or not upsert_watchlist:
        raise HTTPException(status_code=503, detail="mongo_utils watchlist no disponible")


def _start_sfn_pipeline(payload: dict) -> dict:
    if not STATE_MACHINE_ARN:
        raise HTTPException(
            status_code=503,
            detail="STATE_MACHINE_ARN no configurado. Contacta al administrador.",
        )
    run_name = (
        f"watchlist-{'partial' if payload.get('tickers') else 'full'}-"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    )
    exec_resp = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=run_name,
        input=json.dumps(payload),
    )
    return {
        "executionArn": exec_resp["executionArn"],
        "status": "RUNNING",
        "startDate": exec_resp["startDate"].isoformat(),
        "payload": payload,
    }


@app.get("/watchlist", tags=["Cartera"])
def get_watchlist_endpoint(x_api_key: str = Header(default="")):
    """Cartera de seguimiento persistente (MongoDB watchlists)."""
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    tickers = ensure_watchlist_initialized()  # type: ignore[misc]
    doc = get_watchlist() if get_watchlist else {}  # type: ignore[misc]
    return {
        "name": (doc or {}).get("name", "Cartera de seguimiento"),
        "tickers": tickers,
        "total": len(tickers),
        "updated_at": (doc or {}).get("updated_at"),
        "created_at": (doc or {}).get("created_at"),
    }


@app.put("/watchlist", tags=["Cartera"])
def put_watchlist(body: WatchlistBody, x_api_key: str = Header(default="")):
    """Reemplaza la cartera y sincroniza etf_universe para el pipeline."""
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    if not body.tickers:
        raise HTTPException(status_code=400, detail="La lista tickers no puede estar vacia")
    clean = upsert_watchlist(body.tickers, name=body.name or "Cartera de seguimiento")  # type: ignore[misc]
    return {"ok": True, "tickers": clean, "total": len(clean)}


@app.post("/watchlist/tickers", tags=["Cartera"])
def post_watchlist_ticker(body: WatchlistTickerBody, x_api_key: str = Header(default="")):
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    sym = (body.ticker or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="ticker requerido")
    tickers = add_watchlist_ticker(sym)  # type: ignore[misc]
    return {"ok": True, "ticker": sym, "tickers": tickers, "total": len(tickers)}


@app.delete("/watchlist/tickers/{symbol}", tags=["Cartera"])
def delete_watchlist_ticker(symbol: str, x_api_key: str = Header(default="")):
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    sym = symbol.strip().upper()
    tickers = remove_watchlist_ticker(sym)  # type: ignore[misc]
    return {"ok": True, "removed": sym, "tickers": tickers, "total": len(tickers)}


@app.get("/watchlist/coverage", tags=["Cartera"])
def watchlist_coverage(
    date: str = Query(..., description="YYYY-MM-DD"),
    x_api_key: str = Header(default=""),
):
    """Estado del pipeline por ticker de la cartera para una fecha."""
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    db = _require_mongo()
    tickers = ensure_watchlist_initialized()  # type: ignore[misc]
    trace = read_bayesian_trace(date) if read_bayesian_trace else None  # type: ignore[misc]
    trace_keys = set(trace.get("tickers", {}).keys()) if trace else set()
    report_tickers = set()
    if list_bayesian_report_tickers:
        report_tickers = set(list_bayesian_report_tickers(date))  # type: ignore[misc]
    rows = []
    complete = 0
    for t in tickers:
        cov = _pipeline_coverage(db, date, t)
        has_trace = t in trace_keys or t in report_tickers
        row = {
            "ticker": t,
            "has_raw_news": cov["ticker_has_raw_news"],
            "has_news_filtered": cov["ticker_has_news_filtered"],
            "has_trace": has_trace,
            "complete": bool(
                cov["ticker_has_raw_news"]
                and cov["ticker_has_news_filtered"]
                and has_trace
            ),
        }
        if row["complete"]:
            complete += 1
        rows.append(row)
    return {
        "batch_date": date,
        "tickers": rows,
        "total": len(rows),
        "complete": complete,
        "missing": len(rows) - complete,
        "coverage_ratio": round(complete / len(rows), 4) if rows else 0.0,
    }


@app.post("/watchlist/run-pipeline", tags=["Cartera"])
def watchlist_run_pipeline(
    body: WatchlistRunBody, x_api_key: str = Header(default="")
):
    """
    Lanza Step Functions para la cartera.
    only_missing=true: solo tickers sin traza completa en batch_date.
    """
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    batch_date = body.batch_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_tickers = ensure_watchlist_initialized()  # type: ignore[misc]
    tickers = [t.upper() for t in (body.tickers or all_tickers) if t]

    if body.only_missing:
        db = _require_mongo()
        trace = read_bayesian_trace(batch_date) if read_bayesian_trace else None  # type: ignore[misc]
        trace_keys = set(trace.get("tickers", {}).keys()) if trace else set()
        missing = []
        for t in tickers:
            cov = _pipeline_coverage(db, batch_date, t)
            has_trace = t in trace_keys
            if not (
                cov["ticker_has_raw_news"]
                and cov["ticker_has_news_filtered"]
                and has_trace
            ):
                missing.append(t)
        tickers = missing
        if not tickers:
            return {
                "status": "SKIPPED",
                "message": f"Todos los instrumentos de la cartera tienen datos para {batch_date}",
                "batch_date": batch_date,
            }

    if not tickers:
        raise HTTPException(status_code=400, detail="No hay tickers para ejecutar")

    payload: dict = {
        "trigger_type": "manual",
        "batch_date": batch_date,
        "tickers": tickers,
    }
    try:
        result = _start_sfn_pipeline(payload)
        result["message"] = (
            f"Pipeline iniciado para {len(tickers)} ticker(s) en {batch_date}"
        )
        result["tickers"] = tickers
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("watchlist_run_pipeline error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/mongo/etf-universe", tags=["MongoDB"])
def mongo_get_etf_universe(x_api_key: str = Header(default="")):
    """Devuelve la cartera (alias de GET /watchlist)."""
    return get_watchlist_endpoint(x_api_key)


@app.post("/mongo/etf-universe", tags=["MongoDB"])
def mongo_post_etf_universe(
    body: EtfUniverseBody, x_api_key: str = Header(default="")
):
    """Reemplaza cartera + etf_universe (pipeline)."""
    check_api_key(x_api_key)
    _require_watchlist_helpers()
    if not body.tickers:
        raise HTTPException(status_code=400, detail="La lista tickers no puede estar vacia")
    clean = upsert_watchlist(body.tickers)  # type: ignore[misc]
    return {"ok": True, "total": len(clean), "tickers": clean}


# ─── Raw data: noticias y OHLCV ───────────────────────────────────────────────


@app.get("/raw/{date}/news/{ticker}", tags=["Raw Data"])
def get_news(
    date: str,
    ticker: str,
    fallback_latest: bool = Query(
        default=True,
        description="Si no hay noticias en la fecha pedida, devolver el ultimo batch_date con datos",
    ),
    x_api_key: str = Header(default=""),
):
    """
    Noticias raw (ingestion) para un ticker y batch_date.
    Fuente: MongoDB coleccion raw_news.
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato de fecha: YYYY-MM-DD")
    if not read_raw_news_ticker:
        raise HTTPException(status_code=503, detail="mongo_utils no disponible")
    try:
        ticker_u = ticker.upper()
        db = _require_mongo()
        requested_date = date
        articles = read_raw_news_ticker(requested_date, ticker_u)  # type: ignore[misc]
        resolved_date = requested_date
        hint = None

        dates_for_ticker = sorted(
            {
                str(d)[:10]
                for d in db["raw_news"].distinct("batch_date", {"ticker": ticker_u})
                if d and len(str(d)[:10]) == 10
            },
            reverse=True,
        )

        if not articles and fallback_latest and dates_for_ticker:
            resolved_date = dates_for_ticker[0]
            if resolved_date != requested_date:
                articles = read_raw_news_ticker(resolved_date, ticker_u)  # type: ignore[misc]
                hint = (
                    f"No hay noticias para {ticker_u} en {requested_date}. "
                    f"Mostrando el lote mas reciente: {resolved_date}."
                )

        if not articles and not dates_for_ticker:
            hint = (
                f"No hay noticias en MongoDB para {ticker_u}. "
                "Ejecuta la ingesta (pipeline) para ese ticker o revisa el universo ETF."
            )

        return {
            "date": resolved_date,
            "requested_date": requested_date,
            "ticker": ticker_u,
            "articles": articles,
            "total": len(articles),
            "source": "mongo",
            "batch_dates_available": dates_for_ticker,
            "hint": hint,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_news error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/raw/{date}/ohlcv/{ticker}", tags=["Raw Data"])
def get_ohlcv(
    date: str,
    ticker: str,
    limit: int = Query(default=90, ge=1, le=365, description="Max filas a devolver"),
    x_api_key: str = Header(default=""),
):
    """
    Datos OHLCV para un ticker en una fecha de ingestion.
    Fuente: MongoDB coleccion ohlcv.
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato de fecha: YYYY-MM-DD")
    if not read_ohlcv_ticker:
        raise HTTPException(status_code=503, detail="mongo_utils no disponible")
    try:
        ticker_u = ticker.upper()
        rows = read_ohlcv_ticker(date, ticker_u)  # type: ignore[misc]
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No hay OHLCV en MongoDB para {ticker_u} en {date}",
            )
        norm = []
        for r in rows:
            norm.append(
                {
                    "date": r.get("date", ""),
                    "open": float(r.get("open", 0) or 0),
                    "high": float(r.get("high", 0) or 0),
                    "low": float(r.get("low", 0) or 0),
                    "close": float(r.get("close", 0) or 0),
                    "volume": float(r.get("volume", 0) or 0),
                }
            )
        norm.sort(key=lambda x: x["date"], reverse=True)
        norm = norm[:limit]
        norm.sort(key=lambda x: x["date"])
        latest = norm[-1] if norm else {}
        return {
            "date": date,
            "ticker": ticker_u,
            "records": len(norm),
            "latest": latest,
            "data": norm,
            "source": "mongo",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_ohlcv error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Pipeline trigger ─────────────────────────────────────────────────────────


class PipelineRunRequest(BaseModel):
    ticker: Optional[str] = None  # ticker unico
    tickers: Optional[List[str]] = None  # lista de tickers
    batch_date: Optional[str] = None  # YYYY-MM-DD; hoy si omitido


@app.post("/pipeline/run", tags=["Pipeline"])
def run_pipeline(body: PipelineRunRequest, x_api_key: str = Header(default="")):
    """
    Lanza el pipeline de Step Functions para uno o varios tickers.
    Si no se especifica ticker, ejecuta el pipeline completo (todos los ETFs).

    Requiere STATE_MACHINE_ARN configurado como variable de entorno en el pod.
    El pipeline acepta el parametro 'ticker' en el evento para filtrar.
    """
    check_api_key(x_api_key)

    if not STATE_MACHINE_ARN:
        raise HTTPException(
            status_code=503,
            detail="STATE_MACHINE_ARN no configurado. Contacta al administrador.",
        )

    payload: dict = {}
    payload["trigger_type"] = "manual"
    if body.batch_date:
        payload["batch_date"] = body.batch_date
    if body.ticker:
        payload["ticker"] = body.ticker.upper()
    elif body.tickers:
        payload["tickers"] = [t.upper() for t in body.tickers]

    try:
        run_name = (
            f"dashboard-{body.ticker or 'full'}-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        )
        exec_resp = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=run_name,
            input=json.dumps(payload),
        )
        return {
            "executionArn": exec_resp["executionArn"],
            "status": "RUNNING",
            "startDate": exec_resp["startDate"].isoformat(),
            "payload": payload,
            "message": (
                f"Pipeline iniciado para ticker '{body.ticker}'"
                if body.ticker
                else "Pipeline completo iniciado"
            ),
        }
    except Exception as e:
        logger.exception("run_pipeline error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pipeline/status", tags=["Pipeline"])
def pipeline_status(
    execution_arn: str = Query(
        ..., description="ARN de la ejecucion de Step Functions"
    ),
    x_api_key: str = Header(default=""),
):
    """
    Estado de una ejecucion de Step Functions.
    Devuelve: RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED
    """
    check_api_key(x_api_key)
    try:
        desc = sfn.describe_execution(executionArn=execution_arn)
        history_resp = sfn.get_execution_history(
            executionArn=execution_arn,
            maxResults=500,
            reverseOrder=False,
        )
        entered_stages = set()
        exited_stages = set()
        failed_stage = None
        for ev in history_resp.get("events", []):
            entered = ev.get("stateEnteredEventDetails", {})
            exited = ev.get("stateExitedEventDetails", {})
            failed = ev.get("executionFailedEventDetails", {})
            state_name = entered.get("name") or exited.get("name")
            stage = _STATE_TO_STAGE.get(state_name)
            if stage and entered:
                entered_stages.add(stage)
            if stage and exited:
                exited_stages.add(stage)
            if failed and not failed_stage:
                # Si falla, dejamos la última etapa "entered" como candidata.
                ordered_entered = [s for s in _PIPELINE_STAGE_ORDER if s in entered_stages]
                failed_stage = ordered_entered[-1] if ordered_entered else None

        execution_status = desc["status"]
        stages = []
        current_stage = None
        for stage_name in _PIPELINE_STAGE_ORDER:
            st = "PENDING"
            if stage_name in exited_stages:
                st = "SUCCEEDED"
            elif stage_name in entered_stages:
                st = "RUNNING" if execution_status == "RUNNING" else "SUCCEEDED"
            stages.append({"name": stage_name, "status": st})
            if st == "RUNNING":
                current_stage = stage_name

        if execution_status in ("FAILED", "ABORTED", "TIMED_OUT"):
            failed_target = failed_stage or current_stage
            if failed_target:
                for st in stages:
                    if st["name"] == failed_target:
                        st["status"] = "FAILED"
                        break

        progress_done = sum(1 for st in stages if st["status"] == "SUCCEEDED")
        progress_pct = int((progress_done / max(len(stages), 1)) * 100)
        if execution_status == "SUCCEEDED":
            progress_pct = 100

        return {
            "executionArn": execution_arn,
            "status": execution_status,
            "startDate": desc["startDate"].isoformat(),
            "stopDate": (
                desc.get("stopDate", {}) and desc["stopDate"].isoformat()
                if desc.get("stopDate")
                else None
            ),
            "input": json.loads(desc.get("input", "{}")),
            "stages": stages,
            "currentStage": current_stage,
            "progressPct": progress_pct,
        }
    except Exception as e:
        logger.exception("pipeline_status error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Búsqueda de instrumentos financieros ────────────────────────────────────

# Tipos Finnhub relevantes para ETFs y fondos
_INSTRUMENT_TYPES = {
    "ETP": "ETF",
    "ETF": "ETF",
    "FUND": "Fondo",
    "CLOSED-END": "Fondo Cerrado",
    "MUTUAL FUND": "Fondo Mutuo",
    "INDEX": "Índice",
    "Common Stock": "Acción",
    "PREFERRED": "Preferente",
    "ADR": "ADR",
    "REIT": "REIT",
}


@app.get("/search/instruments", tags=["Búsqueda"])
def search_instruments(
    q: str = Query(..., min_length=1, description="Símbolo o nombre del instrumento"),
    filter_type: str = Query(
        default="", description="Filtrar por tipo: ETF, FUND, Stock..."
    ),
    limit: int = Query(default=20, ge=1, le=50),
    x_api_key: str = Header(default=""),
):
    """
    Busca ETFs, fondos y acciones usando la API de Finnhub.
    Devuelve símbolo, nombre, tipo y exchange para cada resultado.

    Tipos más comunes:
      ETP / ETF     → Exchange Traded Products (ETFs, ETNs)
      FUND          → Fondos de inversión
      Common Stock  → Acciones ordinarias
      REIT          → Real Estate Investment Trusts

    Ejemplo: q=SPY → devuelve todos los instrumentos que coincidan con SPY
    """
    check_api_key(x_api_key)

    encoded_q = urllib.parse.quote(q)
    data = _finnhub_get(f"/search?q={encoded_q}")
    results = data.get("result", [])

    # Enriquecer con tipo legible y filtrar si se pide
    enriched = []
    for r in results:
        raw_type = r.get("type", "")
        readable = _INSTRUMENT_TYPES.get(raw_type, raw_type)
        is_etf_fund = raw_type.upper() in ("ETP", "ETF", "FUND", "CLOSED-END", "REIT")

        # Filtro por tipo si se especifica (ETF, FUND, Stock)
        if filter_type:
            ft = filter_type.upper()
            if ft in ("ETF", "ETP") and raw_type.upper() not in ("ETP", "ETF"):
                continue
            elif ft == "FUND" and raw_type.upper() not in (
                "FUND",
                "CLOSED-END",
                "REIT",
            ):
                continue
            elif ft == "STOCK" and raw_type.upper() != "COMMON STOCK":
                continue

        enriched.append(
            {
                "symbol": r.get("symbol", ""),
                "displaySymbol": r.get("displaySymbol", r.get("symbol", "")),
                "description": r.get("description", ""),
                "type": raw_type,
                "typeLabel": readable,
                "isEtfOrFund": is_etf_fund,
            }
        )
        if len(enriched) >= limit:
            break

    # Ordenar: ETFs y fondos primero
    enriched.sort(key=lambda r: (0 if r["isEtfOrFund"] else 1, r["symbol"]))

    return {
        "query": q,
        "results": enriched,
        "total": len(enriched),
    }


@app.get("/instrument/{symbol}/profile", tags=["Búsqueda"])
def get_instrument_profile(symbol: str, x_api_key: str = Header(default="")):
    """
    Perfil completo de un instrumento: nombre, sector, industria, capitalización,
    país, descripción y logo (si está disponible).
    Fuente: Finnhub /stock/profile2
    """
    check_api_key(x_api_key)
    ticker = symbol.upper()
    try:
        profile = _finnhub_get(f"/stock/profile2?symbol={ticker}")
    except HTTPException:
        profile = {}

    # Datos de cotización básica (si existe)
    quote: dict = {}
    try:
        quote = _finnhub_get(f"/quote?symbol={ticker}")
    except HTTPException:
        pass

    return {
        "symbol": ticker,
        "name": profile.get("name", ticker),
        "country": profile.get("country", ""),
        "currency": profile.get("currency", "USD"),
        "exchange": profile.get("exchange", ""),
        "industry": profile.get("finnhubIndustry", ""),
        "marketCap": profile.get("marketCapitalization"),
        "shareOutstanding": profile.get("shareOutstanding"),
        "logo": profile.get("logo", ""),
        "weburl": profile.get("weburl", ""),
        "ipo": profile.get("ipo", ""),
        # Cotización
        "currentPrice": quote.get("c"),
        "change": quote.get("d"),
        "changePct": quote.get("dp"),
        "high52w": quote.get("h"),
        "low52w": quote.get("l"),
        "prevClose": quote.get("pc"),
        "openPrice": quote.get("o"),
    }


@app.get("/stats", tags=["S3"])
def stats(x_api_key: str = Header(default="")):
    check_api_key(x_api_key)
    raise HTTPException(
        status_code=410,
        detail="Descontinuado: usa GET /mongo/stats para conteos por coleccion.",
    )


# ─── MongoDB endpoints ────────────────────────────────────────────────────────

@app.get("/mongo/news/{ticker}", tags=["MongoDB"])
def mongo_news_by_ticker(
    ticker: str,
    limit: int = Query(default=50, ge=1, le=500),
    skip: int = Query(default=0, ge=0),
    date: str = Query(default=None),
    sentiment: str = Query(default=None),
    x_api_key: str = Header(default=""),
):
    """Todas las noticias de un ticker con scoring FinBERT. Filtra por fecha y/o sentimiento."""
    check_api_key(x_api_key)
    db = _require_mongo()
    query: dict = {"ticker": ticker.upper()}
    if date:
        query["batch_date"] = date
    if sentiment:
        query["sentiment"] = sentiment.lower()
    cursor = db["news"].find(query).sort("batch_date", -1).skip(skip).limit(limit)
    docs = [_serialize_doc(d) for d in cursor]
    total = db["news"].count_documents(query)
    return {"ticker": ticker.upper(), "total": total, "skip": skip, "limit": limit, "results": docs}


@app.get("/mongo/news/{date}/{ticker}", tags=["MongoDB"])
def mongo_news_by_date_ticker(date: str, ticker: str, x_api_key: str = Header(default="")):
    """Noticias de un ticker en una fecha concreta, ordenadas por confidence desc."""
    check_api_key(x_api_key)
    db = _require_mongo()
    docs = [_serialize_doc(d) for d in
            db["news"].find({"batch_date": date, "ticker": ticker.upper()}).sort("confidence", -1)]
    return {"date": date, "ticker": ticker.upper(), "total": len(docs), "articles": docs}


@app.get("/mongo/news-detail/{date}/{ticker}", tags=["MongoDB"])
def mongo_news_detail(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
    Detalle completo de noticias para un ticker y fecha:
    - Artículos con scoring FinBERT (headline, url, source, sentiment, confidence)
    - Resúmenes generados por lambda_news_filter (Bedrock)
    - Joined por posición cuando coincide el número de artículos
    """
    check_api_key(x_api_key)
    db = _require_mongo()
    ticker_upper = ticker.upper()

    # Noticias con scoring FinBERT
    raw_articles = [
        _serialize_doc(d) for d in
        db["news"].find({"batch_date": date, "ticker": ticker_upper}).sort("confidence", -1)
    ]

    # Resúmenes de Bedrock (news_filtered)
    filtered_doc = db["news_filtered"].find_one({"batch_date": date, "ticker": ticker_upper})
    bedrock_summaries = []
    daily_context = ""
    if filtered_doc:
        bedrock_summaries = filtered_doc.get("filtered_headlines", [])
        daily_context     = filtered_doc.get("daily_context", "")

    # Enriquecer cada artículo con su resumen Bedrock si existe
    # La colección news almacena el headline original; news_filtered los resúmenes en orden
    # Intentamos mapear por índice cuando las listas tienen el mismo tamaño,
    # o dejamos el campo vacío si no hay correspondencia
    articles_out = []
    for i, art in enumerate(raw_articles):
        bedrock_summary = bedrock_summaries[i] if i < len(bedrock_summaries) else ""
        articles_out.append({
            "headline":       art.get("headline", ""),
            "bedrock_summary": bedrock_summary,
            "url":            art.get("url", ""),
            "source":         art.get("source", ""),
            "datetime":       art.get("datetime", ""),
            "sentiment":      art.get("sentiment", ""),
            "confidence":     art.get("confidence", 0),
            "justification":  art.get("justification", ""),
        })

    return {
        "date":          date,
        "ticker":        ticker_upper,
        "total":         len(articles_out),
        "daily_context": daily_context,
        "articles":      articles_out,
    }


@app.get("/mongo/bayesian/{ticker}", tags=["MongoDB"])
def mongo_bayesian_history(
    ticker: str,
    limit: int = Query(default=30, ge=1, le=365),
    signal: str = Query(default=None),
    x_api_key: str = Header(default=""),
):
    """Historial de reportes bayesianos para un ticker."""
    check_api_key(x_api_key)
    db = _require_mongo()
    query: dict = {"ticker": ticker.upper()}
    if signal:
        query["signal"] = signal.upper()
    docs = [_serialize_doc(d) for d in
            db["bayesian_reports"].find(query).sort("batch_date", -1).limit(limit)]
    return {"ticker": ticker.upper(), "total": len(docs), "results": docs}


@app.get("/mongo/bayesian/{date}/{ticker}", tags=["MongoDB"])
def mongo_bayesian_by_date(date: str, ticker: str, x_api_key: str = Header(default="")):
    """Reporte bayesiano de un ticker en una fecha concreta."""
    check_api_key(x_api_key)
    db = _require_mongo()
    doc = db["bayesian_reports"].find_one({"batch_date": date, "ticker": ticker.upper()})
    if not doc:
        raise HTTPException(status_code=404, detail=f"No hay reporte bayesiano para {ticker.upper()} en {date}")
    return _serialize_doc(doc)


@app.get("/mongo/reports", tags=["MongoDB"])
def mongo_reports(
    limit: int = Query(default=30, ge=1, le=365),
    skip: int = Query(default=0, ge=0),
    x_api_key: str = Header(default=""),
):
    """Lista todos los reportes diarios, del mas reciente al mas antiguo."""
    check_api_key(x_api_key)
    db = _require_mongo()
    docs = [_serialize_doc(d) for d in
            db["reports"].find({}, {"top_signal_explanations": 0})
                         .sort("report_date", -1).skip(skip).limit(limit)]
    total = db["reports"].count_documents({})
    return {"total": total, "skip": skip, "limit": limit, "results": docs}


@app.get("/mongo/reports/{date}", tags=["MongoDB"])
def mongo_report_by_date(date: str, x_api_key: str = Header(default="")):
    """Reporte diario completo para una fecha concreta."""
    check_api_key(x_api_key)
    db = _require_mongo()
    doc = db["reports"].find_one({"report_date": date})
    if not doc:
        raise HTTPException(status_code=404, detail=f"No hay reporte para {date}")
    return _serialize_doc(doc)


@app.get("/mongo/analytics/ticker/{ticker}", tags=["MongoDB"])
def mongo_ticker_analytics(
    ticker: str,
    days: int = Query(default=30, ge=7, le=365),
    x_api_key: str = Header(default=""),
):
    """Analisis historico de un ticker: evolucion de senal, P(up) e indicadores."""
    check_api_key(x_api_key)
    db = _require_mongo()
    from datetime import datetime, timedelta, timezone
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    pipeline = [
        {"$match": {"ticker": ticker.upper(), "batch_date": {"$gte": since}}},
        {"$sort": {"batch_date": 1}},
        {"$project": {
            "batch_date": 1, "signal": 1, "prob_up": 1, "prob_down": 1,
            "rsi_14": "$raw_values.rsi_14",
            "sma_spread": "$raw_values.sma_spread",
            "bb_width": "$raw_values.bb_width_ratio",
            "sentiment": "$discretization.sentiment_state",
            "trend": "$discretization.trend_state",
            "volatility": "$discretization.volatility_state",
            "reasoning": 1,
        }},
    ]
    docs = [_serialize_doc(d) for d in db["bayesian_reports"].aggregate(pipeline)]
    signal_dist = {"BUY": 0, "SELL": 0, "HOLD": 0}
    for d in docs:
        s = d.get("signal", "HOLD")
        signal_dist[s] = signal_dist.get(s, 0) + 1
    return {"ticker": ticker.upper(), "period_days": days, "since": since,
            "total_records": len(docs), "signal_distribution": signal_dist, "timeline": docs}


@app.get("/mongo/stats", tags=["MongoDB"])
def mongo_stats(x_api_key: str = Header(default="")):
    """Estadisticas de la base de datos MongoDB: documentos por coleccion."""
    check_api_key(x_api_key)
    db = _require_mongo()
    trace_dates = []
    for doc in db["bayesian_traces"].find({}, {"batch_date": 1, "trace.tickers": 1, "_id": 0}):
        bd = str(doc.get("batch_date", ""))[:10]
        tickers = sorted((doc.get("trace") or {}).get("tickers", {}).keys())
        trace_dates.append(
            {"batch_date": bd, "ticker_count": len(tickers), "tickers": tickers[:30]}
        )
    trace_dates.sort(key=lambda x: x["batch_date"], reverse=True)
    return {
        "database": MONGODB_DB,
        "collections": {
            "etf_universe": db["etf_universe"].count_documents({}),
            "raw_news": db["raw_news"].count_documents({}),
            "ohlcv": db["ohlcv"].count_documents({}),
            "news": db["news"].count_documents({}),
            "news_filtered": db["news_filtered"].count_documents({}),
            "bayesian_reports": db["bayesian_reports"].count_documents({}),
            "bayesian_traces": db["bayesian_traces"].count_documents({}),
            "reports": db["reports"].count_documents({}),
        },
        "bayesian_traces_by_date": trace_dates,
    }


@app.post("/mongo/setup-indexes", tags=["MongoDB"])
def mongo_setup_indexes(x_api_key: str = Header(default="")):
    """Crea los indices recomendados en MongoDB. Ejecutar una sola vez."""
    check_api_key(x_api_key)
    db = _require_mongo()
    from pymongo import ASCENDING, DESCENDING, TEXT
    created = []
    db["news"].create_index([("batch_date", ASCENDING), ("ticker", ASCENDING)])
    db["news"].create_index([("ticker", ASCENDING), ("batch_date", DESCENDING)])
    db["news"].create_index([("headline", TEXT)])
    created.append("news: 3 indices")
    db["bayesian_reports"].create_index(
        [("batch_date", ASCENDING), ("ticker", ASCENDING)], unique=True)
    db["bayesian_reports"].create_index([("ticker", ASCENDING), ("batch_date", DESCENDING)])
    db["bayesian_reports"].create_index([("signal", ASCENDING)])
    created.append("bayesian_reports: 3 indices (1 unico)")
    db["reports"].create_index([("report_date", ASCENDING)], unique=True)
    created.append("reports: 1 indice unico")
    db["bayesian_traces"].create_index([("batch_date", ASCENDING)], unique=True)
    created.append("bayesian_traces: indice unico batch_date")
    db["raw_news"].create_index([("batch_date", ASCENDING), ("ticker", ASCENDING)])
    created.append("raw_news: batch_date+ticker")
    db["ohlcv"].create_index([("batch_date", ASCENDING), ("ticker", ASCENDING)])
    created.append("ohlcv: batch_date+ticker")
    db["macro_news"].create_index([("batch_date", ASCENDING)])
    created.append("macro_news: batch_date")
    db["macro_context"].create_index([("batch_date", ASCENDING)], unique=True)
    created.append("macro_context: indice unico batch_date")
    return {"message": "Indices creados correctamente", "details": created}


# =============================================================================
# MACRO — Contexto macroeconómico global
# =============================================================================

@app.get("/macro/context/{date}", tags=["Macro"])
def get_macro_context(date: str, x_api_key: str = Header(default="")):
    """MacroSentiment + RiskRegime + macro_adjustment calculados para una fecha."""
    check_api_key(x_api_key)
    db = _require_mongo()
    doc = db["macro_context"].find_one({"batch_date": date})
    if not doc:
        raise HTTPException(
            status_code=404,
            detail=f"No hay contexto macro para {date}. Ejecuta lambda_macro_context primero."
        )
    return _serialize_doc(doc)


@app.get("/macro/news/{date}", tags=["Macro"])
def get_macro_news(
    date: str,
    category: str = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    x_api_key: str = Header(default=""),
):
    """Noticias macroeconómicas del día con su categoría temática."""
    check_api_key(x_api_key)
    db = _require_mongo()
    query: dict = {"batch_date": date}
    if category:
        query["category"] = category
    docs = list(
        db["macro_news"].find(query)
        .sort("datetime", -1)
        .limit(limit)
    )
    return {
        "date":     date,
        "total":    db["macro_news"].count_documents({"batch_date": date}),
        "articles": [_serialize_doc(d) for d in docs],
    }


@app.get("/macro/history", tags=["Macro"])
def get_macro_history(
    limit: int = Query(default=30, ge=1, le=365),
    x_api_key: str = Header(default=""),
):
    """Historial de MacroSentiment y RiskRegime de los últimos N días."""
    check_api_key(x_api_key)
    db = _require_mongo()
    docs = list(
        db["macro_context"]
        .find({}, {"batch_date":1,"macro_sentiment":1,"risk_regime":1,
                   "macro_adjustment":1,"detail.vix":1})
        .sort("batch_date", -1)
        .limit(limit)
    )
    return {"total": len(docs), "history": [_serialize_doc(d) for d in docs]}
