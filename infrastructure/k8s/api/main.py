"""
TFM Dashboard API — FastAPI
===========================
Reemplaza lambda_api.py para correr como pod en EKS.
Mismos endpoints, misma lógica, sin Lambda overhead.

Endpoints:
  GET /health
  GET /reports
  GET /reports/{date}
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

# ─── Config ──────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tfm-api")

DATALAKE_BUCKET = os.getenv("DATALAKE_BUCKET", "tfm-unir-datalake")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")  # vacío = sin auth
PRESIGN_TTL = int(os.getenv("PRESIGN_TTL_SEC", "900"))

s3 = boto3.client("s3", region_name=AWS_REGION)

# ─── App ─────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="TFM Dashboard API",
    description="Acceso al datalake S3 del sistema de trading algorítmico",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ─── Auth helper ─────────────────────────────────────────────────────────────


def check_api_key(x_api_key: str = Header(default="")):
    """Valida la API Key si DASHBOARD_API_KEY está configurada."""
    if DASHBOARD_API_KEY and x_api_key != DASHBOARD_API_KEY:
        raise HTTPException(status_code=403, detail="API Key inválida o ausente")


# ─── Endpoints ───────────────────────────────────────────────────────────────


@app.get("/health", tags=["Sistema"])
def health():
    """Health check — sin autenticación."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/reports", tags=["Reports"])
def list_reports(x_api_key: str = Header(default="")):
    """Lista todos los reportes disponibles en S3 (results/{date}/report.json)."""
    check_api_key(x_api_key)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        dates = []
        for page in paginator.paginate(
            Bucket=DATALAKE_BUCKET, Prefix="results/", Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                prefix = cp.get("Prefix", "")  # 'results/2024-01-15/'
                date_str = prefix.replace("results/", "").rstrip("/")
                if len(date_str) == 10:
                    key = f"results/{date_str}/report.json"
                    try:
                        head = s3.head_object(Bucket=DATALAKE_BUCKET, Key=key)
                        dates.append(
                            {
                                "date": date_str,
                                "s3Key": key,
                                "lastModified": head["LastModified"].isoformat(),
                                "sizeBytes": head["ContentLength"],
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
    """Devuelve el report.json completo de una fecha (YYYY-MM-DD)."""
    check_api_key(x_api_key)
    if not date or len(date) != 10:
        raise HTTPException(
            status_code=400, detail="Fecha inválida. Formato: YYYY-MM-DD"
        )
    try:
        key = f"results/{date}/report.json"
        response = s3.get_object(Bucket=DATALAKE_BUCKET, Key=key)
        body = json.loads(response["Body"].read().decode("utf-8"))
        return body
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            raise HTTPException(
                status_code=404, detail=f"Report no encontrado para {date}"
            )
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.exception("get_report error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/files", tags=["S3"])
def list_files(
    prefix: str = Query(default="", description="Prefijo S3"),
    maxKeys: int = Query(default=200, ge=1, le=1000),
    delimiter: str = Query(default="/"),
    continuationToken: str = Query(default=None),
    x_api_key: str = Header(default=""),
):
    """Lista objetos S3 bajo un prefijo."""
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
    key: str = Query(..., description="Clave S3 del objeto"),
    ttl: int = Query(default=PRESIGN_TTL, description="Segundos de validez"),
    x_api_key: str = Header(default=""),
):
    """Genera URL prefirmada para descarga directa (sin credenciales AWS en el cliente)."""
    check_api_key(x_api_key)
    if not key:
        raise HTTPException(status_code=400, detail="Parámetro 'key' requerido")
    try:
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": DATALAKE_BUCKET, "Key": key},
            ExpiresIn=ttl,
        )
        return {"url": url, "expiresInSeconds": ttl}
    except Exception as e:
        logger.exception("presign error")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", tags=["S3"])
def stats(x_api_key: str = Header(default="")):
    """Estadísticas del bucket: ficheros totales, tamaño y desglose por prefijo."""
    check_api_key(x_api_key)
    try:
        prefixes = {"results": "results/", "raw": "raw/"}
        total_files = 0
        total_bytes = 0
        last_updated = None
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
