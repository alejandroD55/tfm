"""
TFM Dashboard API - FastAPI
===========================
Endpoints:
  GET /health
  GET /reports                    - Lista fechas disponibles
  GET /reports/{date}             - Report completo
  GET /trace/{date}               - Traza bayesiana completa (nuevo)
  GET /trace/{date}/{ticker}      - Traza por ticker (nuevo)
  GET /model                      - Config del modelo bayesiano (nuevo)
  GET /sentiment/{date}/{ticker}  - Sentimiento detallado (nuevo)
  GET /indicators/{date}/{ticker} - Indicadores tecnicos (nuevo)
  GET /files
  GET /files/presign
  GET /stats
"""
import os
import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tfm-api")

DATALAKE_BUCKET   = os.getenv("DATALAKE_BUCKET", "tfm-unir-datalake")
AWS_REGION        = os.getenv("AWS_REGION", "eu-north-1")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")
PRESIGN_TTL       = int(os.getenv("PRESIGN_TTL_SEC", "900"))

s3 = boto3.client("s3", region_name=AWS_REGION)

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
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat(), "version": "2.0.0"}


# ─── Reports ──────────────────────────────────────────────────────────────────

@app.get("/reports", tags=["Reports"])
def list_reports(x_api_key: str = Header(default="")):
    """Lista todas las fechas con report.json disponible."""
    check_api_key(x_api_key)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        dates = []
        for page in paginator.paginate(Bucket=DATALAKE_BUCKET, Prefix="results/", Delimiter="/"):
            for cp in page.get("CommonPrefixes", []):
                prefix   = cp.get("Prefix", "")
                date_str = prefix.replace("results/", "").rstrip("/")
                if len(date_str) == 10:
                    key = f"results/{date_str}/report.json"
                    try:
                        head = s3.head_object(Bucket=DATALAKE_BUCKET, Key=key)
                        has_trace = False
                        try:
                            s3.head_object(Bucket=DATALAKE_BUCKET,
                                           Key=f"results/{date_str}/bayesian_trace.json")
                            has_trace = True
                        except ClientError:
                            pass
                        dates.append({
                            "date":         date_str,
                            "s3Key":        key,
                            "lastModified": head["LastModified"].isoformat(),
                            "sizeBytes":    head["ContentLength"],
                            "has_trace":    has_trace,
                        })
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
            detail=f"Ticker '{ticker_upper}' no encontrado en la traza de {date}. Disponibles: {available}"
        )
    return {
        "date":         date,
        "ticker":       ticker_upper,
        "model_config": trace.get("model_config"),
        "execution":    trace.get("execution"),
        "trace":        trace["tickers"][ticker_upper],
        "audit_notes":  trace.get("audit_notes"),
    }


# ─── Configuracion del modelo (NUEVO) ─────────────────────────────────────────

@app.get("/model", tags=["Modelo Bayesiano"])
def get_model_config(date: str = Query(default=None, description="Fecha del trace (ultima disponible si no se indica)"),
                     x_api_key: str = Header(default="")):
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
            for page in paginator.paginate(Bucket=DATALAKE_BUCKET,
                                           Prefix="results/", Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    d = cp.get("Prefix", "").replace("results/", "").rstrip("/")
                    if len(d) == 10:
                        dates_found.append(d)
            if not dates_found:
                raise HTTPException(status_code=404, detail="No hay traces disponibles")
            date = sorted(dates_found, reverse=True)[0]

        trace = _read_s3_json(f"results/{date}/bayesian_trace.json")
        return {
            "source_date":    date,
            "schema_version": trace.get("schema_version"),
            "model_config":   trace.get("model_config"),
            "audit_notes":    trace.get("audit_notes"),
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
        ticker_data  = trace.get("tickers", {}).get(ticker_upper)
        if not ticker_data:
            raise HTTPException(status_code=404,
                                detail=f"Ticker {ticker_upper} no encontrado para {date}")
        return {
            "date":             date,
            "ticker":           ticker_upper,
            "sentiment_detail": ticker_data.get("sentiment_detail", {}),
            "used_in_inference": ticker_data.get("discretization", {}).get("sentiment_state"),
            "limitation": ("Solo el headline con mayor confidence score influye en la inferencia. "
                           "Los demas titulares son trazados pero no usados.")
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
        ticker_data  = trace.get("tickers", {}).get(ticker_upper)
        if not ticker_data:
            raise HTTPException(status_code=404,
                                detail=f"Ticker {ticker_upper} no encontrado para {date}")

        model_cfg = trace.get("model_config", {}).get("discretization", {})
        return {
            "date":    date,
            "ticker":  ticker_upper,
            "raw_values":      ticker_data.get("raw_values", {}),
            "discretized":     ticker_data.get("discretization", {}),
            "discretization_rules": {
                "rsi":        model_cfg.get("rsi", {}),
                "trend":      model_cfg.get("trend", {}),
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
        kwargs = {"Bucket": DATALAKE_BUCKET, "Prefix": prefix,
                  "MaxKeys": maxKeys, "Delimiter": delimiter}
        if continuationToken:
            kwargs["ContinuationToken"] = continuationToken
        resp = s3.list_objects_v2(**kwargs)
        folders = [{"key": cp["Prefix"],
                    "name": cp["Prefix"].replace(prefix, "").rstrip("/"),
                    "isFolder": True, "size": 0}
                   for cp in resp.get("CommonPrefixes", [])]
        files = [{"key": obj["Key"],
                  "name": obj["Key"].split("/")[-1],
                  "isFolder": False,
                  "size": obj["Size"],
                  "lastModified": obj["LastModified"].isoformat(),
                  "etag": obj.get("ETag", "").strip('"'),
                  "storageClass": obj.get("StorageClass", "STANDARD")}
                 for obj in resp.get("Contents", [])
                 if obj["Key"] != prefix]
        return {"items": folders + files,
                "prefix": prefix,
                "isTruncated": resp.get("IsTruncated", False),
                "nextContinuationToken": resp.get("NextContinuationToken")}
    except Exception as e:
        logger.exception("list_files error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files/presign", tags=["S3"])
def presign(key: str = Query(...), ttl: int = Query(default=PRESIGN_TTL),
            x_api_key: str = Header(default="")):
    check_api_key(x_api_key)
    if not key:
        raise HTTPException(status_code=400, detail="Parametro 'key' requerido")
    try:
        url = s3.generate_presigned_url("get_object",
              Params={"Bucket": DATALAKE_BUCKET, "Key": key}, ExpiresIn=ttl)
        return {"url": url, "expiresInSeconds": ttl}
    except Exception as e:
        logger.exception("presign error")
        raise HTTPException(status_code=500, detail=str(e))


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
                    pf += 1; pb += obj["Size"]
                    if last_updated is None or obj["LastModified"] > last_updated:
                        last_updated = obj["LastModified"]
            breakdown.append({"prefix": label, "fileCount": pf, "sizeBytes": pb})
            total_files += pf; total_bytes += pb
        return {"bucket": DATALAKE_BUCKET, "totalFiles": total_files,
                "totalBytes": total_bytes,
                "lastUpdated": last_updated.isoformat() if last_updated else None,
                "breakdown": breakdown}
    except Exception as e:
        logger.exception("stats error")
        raise HTTPException(status_code=500, detail=str(e))
