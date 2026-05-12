"""
TFM Dashboard API - FastAPI
===========================
Endpoints:
  GET /health
  GET /reports                      - Lista fechas disponibles
  GET /reports/{date}               - Report completo
  GET /trace/{date}                 - Traza bayesiana completa
  GET /trace/{date}/{ticker}        - Traza por ticker
  GET /model                        - Config del modelo bayesiano
  GET /tickers                      - Lista ETFs del universo
  GET /raw/{date}/news/{ticker}     - Noticias raw de Finnhub por ticker
  GET /raw/{date}/ohlcv/{ticker}    - Datos OHLCV por ticker
  POST /pipeline/run                - Lanza pipeline para ticker(s)
  GET /pipeline/status              - Estado de una ejecucion Step Functions
  GET /search/instruments           - Busca ETFs y fondos via Finnhub (nuevo)
  GET /instrument/{symbol}/profile  - Perfil detallado de un instrumento (nuevo)
  GET /sentiment/{date}/{ticker}    - Sentimiento detallado
  GET /indicators/{date}/{ticker}   - Indicadores tecnicos
  GET /files
  GET /files/presign
  GET /stats
"""

import os
import csv
import io
import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, List

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query, Header
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tfm-api")

DATALAKE_BUCKET = os.getenv("DATALAKE_BUCKET", "tfm-unir-datalake")
CONFIG_BUCKET = os.getenv("CONFIG_BUCKET", "tfm-unir-config")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")
PRESIGN_TTL = int(os.getenv("PRESIGN_TTL_SEC", "900"))
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN", "")
MONGODB_DB = os.getenv("MONGODB_DB", "tfm")

s3 = boto3.client("s3", region_name=AWS_REGION)
sfn = boto3.client("stepfunctions", region_name=AWS_REGION)
lmb = boto3.client("lambda", region_name=AWS_REGION)
secrets_api = boto3.client("secretsmanager", region_name=AWS_REGION)

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
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)


# ─── Auth ─────────────────────────────────────────────────────────────────────


def check_api_key(x_api_key: str = Header(default="")):
    if DASHBOARD_API_KEY and x_api_key != DASHBOARD_API_KEY:
        raise HTTPException(status_code=403, detail="API Key invalida o ausente")


# ─── Helper: leer JSON de S3 ──────────────────────────────────────────────────


def _read_s3_json(key: str) -> dict:
    try:
        resp = s3.get_object(Bucket=DATALAKE_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {key}")
        raise HTTPException(status_code=500, detail=str(e))


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
    """Lista todas las fechas con report.json disponible."""
    check_api_key(x_api_key)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        dates = []
        for page in paginator.paginate(
            Bucket=DATALAKE_BUCKET, Prefix="results/", Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                prefix = cp.get("Prefix", "")
                date_str = prefix.replace("results/", "").rstrip("/")
                if len(date_str) == 10:
                    key = f"results/{date_str}/report.json"
                    try:
                        head = s3.head_object(Bucket=DATALAKE_BUCKET, Key=key)
                        has_trace = False
                        try:
                            s3.head_object(
                                Bucket=DATALAKE_BUCKET,
                                Key=f"results/{date_str}/bayesian_trace.json",
                            )
                            has_trace = True
                        except ClientError:
                            pass
                        dates.append(
                            {
                                "date": date_str,
                                "s3Key": key,
                                "lastModified": head["LastModified"].isoformat(),
                                "sizeBytes": head["ContentLength"],
                                "has_trace": has_trace,
                            }
                        )
                    except ClientError:
                        pass
        dates.sort(key=lambda x: x["date"], reverse=True)
        return {"dates": dates, "total": len(dates)}
    except Exception as e:
        logger.exception("list_reports error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/reports/{date}", tags=["Reports"])
def get_report(date: str, x_api_key: str = Header(default="")):
    """Report.json completo para una fecha (YYYY-MM-DD)."""
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    return _read_s3_json(f"results/{date}/report.json")


# ─── Trazabilidad bayesiana (NUEVO) ───────────────────────────────────────────


@app.get("/trace/{date}", tags=["Trazabilidad"])
def get_trace(date: str, x_api_key: str = Header(default="")):
    """
    Traza bayesiana completa del dia: configuracion del modelo, evidencias raw,
    estados discretizados, probabilidades y razonamiento por ticker.
    Generado por lambda_bayesian en results/{date}/bayesian_trace.json
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato: YYYY-MM-DD")
    return _read_s3_json(f"results/{date}/bayesian_trace.json")


@app.get("/trace/{date}/{ticker}", tags=["Trazabilidad"])
def get_trace_ticker(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
    Traza bayesiana de un ticker especifico para una fecha.
    Incluye: valores raw, discretizacion, distribucion de sentimiento,
    probabilidades posteriores y razonamiento textual.
    """
    check_api_key(x_api_key)
    trace = _read_s3_json(f"results/{date}/bayesian_trace.json")
    ticker_upper = ticker.upper()
    if ticker_upper not in trace.get("tickers", {}):
        available = list(trace.get("tickers", {}).keys())
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker_upper}' no encontrado en la traza de {date}. Disponibles: {available}",
        )
    return {
        "date": date,
        "ticker": ticker_upper,
        "model_config": trace.get("model_config"),
        "execution": trace.get("execution"),
        "trace": trace["tickers"][ticker_upper],
        "audit_notes": trace.get("audit_notes"),
    }


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
            paginator = s3.get_paginator("list_objects_v2")
            dates_found = []
            for page in paginator.paginate(
                Bucket=DATALAKE_BUCKET, Prefix="results/", Delimiter="/"
            ):
                for cp in page.get("CommonPrefixes", []):
                    d = cp.get("Prefix", "").replace("results/", "").rstrip("/")
                    if len(d) == 10:
                        dates_found.append(d)
            if not dates_found:
                raise HTTPException(status_code=404, detail="No hay traces disponibles")
            date = sorted(dates_found, reverse=True)[0]

        trace = _read_s3_json(f"results/{date}/bayesian_trace.json")
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
        trace = _read_s3_json(f"results/{date}/bayesian_trace.json")
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
        trace = _read_s3_json(f"results/{date}/bayesian_trace.json")
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
    try:
        kwargs = {
            "Bucket": DATALAKE_BUCKET,
            "Prefix": prefix,
            "MaxKeys": maxKeys,
            "Delimiter": delimiter,
        }
        if continuationToken:
            kwargs["ContinuationToken"] = continuationToken
        resp = s3.list_objects_v2(**kwargs)
        folders = [
            {
                "key": cp["Prefix"],
                "name": cp["Prefix"].replace(prefix, "").rstrip("/"),
                "isFolder": True,
                "size": 0,
            }
            for cp in resp.get("CommonPrefixes", [])
        ]
        files = [
            {
                "key": obj["Key"],
                "name": obj["Key"].split("/")[-1],
                "isFolder": False,
                "size": obj["Size"],
                "lastModified": obj["LastModified"].isoformat(),
                "etag": obj.get("ETag", "").strip('"'),
                "storageClass": obj.get("StorageClass", "STANDARD"),
            }
            for obj in resp.get("Contents", [])
            if obj["Key"] != prefix
        ]
        return {
            "items": folders + files,
            "prefix": prefix,
            "isTruncated": resp.get("IsTruncated", False),
            "nextContinuationToken": resp.get("NextContinuationToken"),
        }
    except Exception as e:
        logger.exception("list_files error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files/presign", tags=["S3"])
def presign(
    key: str = Query(...),
    ttl: int = Query(default=PRESIGN_TTL),
    x_api_key: str = Header(default=""),
):
    check_api_key(x_api_key)
    if not key:
        raise HTTPException(status_code=400, detail="Parametro 'key' requerido")
    try:
        url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": DATALAKE_BUCKET, "Key": key}, ExpiresIn=ttl
        )
        return {"url": url, "expiresInSeconds": ttl}
    except Exception as e:
        logger.exception("presign error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Tickers ──────────────────────────────────────────────────────────────────


@app.get("/tickers", tags=["Tickers"])
def list_tickers(x_api_key: str = Header(default="")):
    """Lista los ETFs del universo de inversion (tfm-unir-config/etf_universe.json)."""
    check_api_key(x_api_key)
    try:
        resp = s3.get_object(Bucket=CONFIG_BUCKET, Key="etf_universe.json")
        data = json.loads(resp["Body"].read().decode("utf-8"))
        tickers = data.get("tickers", data) if isinstance(data, dict) else data
        return {"tickers": tickers, "total": len(tickers)}
    except Exception as e:
        logger.exception("list_tickers error")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Raw data: noticias y OHLCV ───────────────────────────────────────────────


@app.get("/raw/{date}/news/{ticker}", tags=["Raw Data"])
def get_news(date: str, ticker: str, x_api_key: str = Header(default="")):
    """
    Noticias raw de Finnhub para un ticker en una fecha concreta.
    Fuente: s3://tfm-unir-datalake/raw/{date}/news.json
    Incluye: headline, url, datetime, source para cada articulo.
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato de fecha: YYYY-MM-DD")
    try:
        resp = s3.get_object(Bucket=DATALAKE_BUCKET, Key=f"raw/{date}/news.json")
        all_news = json.loads(resp["Body"].read().decode("utf-8"))
        ticker_u = ticker.upper()
        articles = all_news.get(ticker_u, [])
        return {
            "date": date,
            "ticker": ticker_u,
            "articles": articles,
            "total": len(articles),
            "all_tickers_in_file": list(all_news.keys()),
        }
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail=f"No hay news.json para {date}")
        raise HTTPException(status_code=500, detail=str(e))
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
    Datos OHLCV (Open/High/Low/Close/Volume) para un ticker en una fecha concreta.
    Fuente: s3://tfm-unir-datalake/raw/{date}/ohlcv.csv
    Devuelve hasta 'limit' filas (por defecto 90, los ultimos 90 dias).
    """
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(status_code=400, detail="Formato de fecha: YYYY-MM-DD")
    try:
        resp = s3.get_object(Bucket=DATALAKE_BUCKET, Key=f"raw/{date}/ohlcv.csv")
        content = resp["Body"].read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))
        ticker_u = ticker.upper()
        rows = []
        for row in reader:
            row_ticker = row.get("Ticker", row.get("ticker", "")).upper()
            if row_ticker == ticker_u:
                # Normalizar columnas a minuscula
                clean = {k.strip(): v for k, v in row.items()}
                rows.append(
                    {
                        "date": clean.get("Date", clean.get("date", "")),
                        "open": float(clean.get("Open", clean.get("open", 0) or 0)),
                        "high": float(clean.get("High", clean.get("high", 0) or 0)),
                        "low": float(clean.get("Low", clean.get("low", 0) or 0)),
                        "close": float(clean.get("Close", clean.get("close", 0) or 0)),
                        "volume": float(
                            clean.get("Volume", clean.get("volume", 0) or 0)
                        ),
                    }
                )

        # Ordenar por fecha desc y limitar
        rows.sort(key=lambda r: r["date"], reverse=True)
        rows = rows[:limit]
        rows.sort(key=lambda r: r["date"])  # devolver cronologico

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Ticker '{ticker_u}' no encontrado en ohlcv.csv de {date}. "
                f"Verifica que el ticker existe en el universo ETF.",
            )

        latest = rows[-1] if rows else {}
        return {
            "date": date,
            "ticker": ticker_u,
            "records": len(rows),
            "latest": latest,
            "data": rows,
        }
    except HTTPException:
        raise
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail=f"No hay ohlcv.csv para {date}")
        raise HTTPException(status_code=500, detail=str(e))
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
        return {
            "executionArn": execution_arn,
            "status": desc["status"],
            "startDate": desc["startDate"].isoformat(),
            "stopDate": (
                desc.get("stopDate", {}) and desc["stopDate"].isoformat()
                if desc.get("stopDate")
                else None
            ),
            "input": json.loads(desc.get("input", "{}")),
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
    try:
        prefixes = {"results": "results/", "raw": "raw/"}
        total_files, total_bytes, last_updated = 0, 0, None
        breakdown = []
        for label, pref in prefixes.items():
            pf, pb = 0, 0
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=DATALAKE_BUCKET, Prefix=pref):
                for obj in page.get("Contents", []):
                    pf += 1
                    pb += obj["Size"]
                    if last_updated is None or obj["LastModified"] > last_updated:
                        last_updated = obj["LastModified"]
            breakdown.append({"prefix": label, "fileCount": pf, "sizeBytes": pb})
            total_files += pf
            total_bytes += pb
        return {
            "bucket": DATALAKE_BUCKET,
            "totalFiles": total_files,
            "totalBytes": total_bytes,
            "lastUpdated": last_updated.isoformat() if last_updated else None,
            "breakdown": breakdown,
        }
    except Exception as e:
        logger.exception("stats error")
        raise HTTPException(status_code=500, detail=str(e))


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
    return {
        "database": MONGODB_DB,
        "collections": {
            "news": db["news"].count_documents({}),
            "bayesian_reports": db["bayesian_reports"].count_documents({}),
            "reports": db["reports"].count_documents({}),
        },
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
    return {"message": "Indices creados correctamente", "details": created}
