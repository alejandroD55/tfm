#!/usr/bin/env python3
"""
Limpieza MongoDB Atlas — conservar solo SPY, IWM, XLE, GLD.

Uso:
  export MONGODB_URI="mongodb+srv://..."
  python scripts/cleanup_universe_mongo.py --dry-run   # solo muestra conteos
  python scripts/cleanup_universe_mongo.py --apply    # borra / recorta documentos

No toca: macro_news, macro_context (datos globales, sin ticker de ETF).
Actualiza: etf_universe y watchlists al universo de 4 tickers.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Set

ALLOWED: Set[str] = {"SPY", "IWM", "XLE", "GLD"}

# Colecciones con campo "ticker" en la raíz del documento
TICKER_COLLECTIONS = (
    "raw_news",
    "ohlcv",
    "news",
    "news_filtered",
    "bayesian_reports",
)


def _get_client():
    uri = os.getenv("MONGODB_URI")
    if not uri:
        try:
            import boto3

            region = os.getenv("AWS_REGION", "eu-north-1")
            sm = boto3.client("secretsmanager", region_name=region)
            resp = sm.get_secret_value(SecretId="mongodb/connection_string")
            secret = json.loads(resp["SecretString"])
            uri = secret.get("connection_string") or secret.get("uri")
        except Exception as exc:
            print(f"No MONGODB_URI y no se pudo leer Secrets Manager: {exc}", file=sys.stderr)
            sys.exit(1)
    if not uri:
        print("MONGODB_URI vacío.", file=sys.stderr)
        sys.exit(1)

    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    return client


def preview_ticker_collections(db, apply: bool) -> Dict[str, int]:
    deleted: Dict[str, int] = {}
    filt = {"ticker": {"$nin": list(ALLOWED)}}
    for name in TICKER_COLLECTIONS:
        col = db[name]
        n = col.count_documents(filt)
        deleted[name] = n
        print(f"  {name}: {n} documentos a eliminar (ticker ∉ {sorted(ALLOWED)})")
        if apply and n:
            res = col.delete_many(filt)
            deleted[name] = res.deleted_count
    return deleted


def cleanup_bayesian_traces(db, apply: bool) -> Dict[str, int]:
    """Quita tickers ajenos del subdocumento trace.tickers; borra doc si queda vacío."""
    col = db["bayesian_traces"]
    removed_keys = 0
    deleted_docs = 0
    updated_docs = 0

    for doc in col.find({}, {"_id": 1, "batch_date": 1, "trace": 1}):
        trace = doc.get("trace") or {}
        tickers = trace.get("tickers") or {}
        if not isinstance(tickers, dict):
            continue
        extra = [k for k in tickers if str(k).upper() not in ALLOWED]
        if not extra:
            continue
        batch = doc.get("batch_date", "?")
        print(f"  bayesian_traces {batch}: quitar {extra}")
        if not apply:
            removed_keys += len(extra)
            continue
        for k in extra:
            tickers.pop(k, None)
        if not tickers:
            col.delete_one({"_id": doc["_id"]})
            deleted_docs += 1
        else:
            col.update_one(
                {"_id": doc["_id"]},
                {"$set": {"trace.tickers": tickers}},
            )
            updated_docs += 1
            removed_keys += len(extra)

    print(
        f"  bayesian_traces: {removed_keys} claves fuera de universo "
        f"(docs borrados={deleted_docs}, actualizados={updated_docs} en apply)"
    )
    return {
        "keys_removed": removed_keys,
        "docs_deleted": deleted_docs,
        "docs_updated": updated_docs,
    }


def cleanup_reports(db, apply: bool) -> int:
    """
    Borra reportes cuyo backtesting_metrics o summary referencian tickers fuera del universo.
    Más simple y seguro que recortar JSON anidado.
    """
    col = db["reports"]
    to_delete = []
    for doc in col.find({}, {"report_date": 1, "backtesting_metrics": 1, "summary": 1}):
        metrics = doc.get("backtesting_metrics") or {}
        keys = {str(k).upper() for k in metrics.keys()} if isinstance(metrics, dict) else set()
        extra = keys - ALLOWED
        if extra:
            to_delete.append(doc.get("report_date") or doc.get("_id"))
            print(f"  reports {doc.get('report_date')}: métricas con {sorted(extra)}")
    print(f"  reports: {len(to_delete)} documentos a eliminar")
    if apply and to_delete:
        res = col.delete_many({"report_date": {"$in": to_delete}})
        return res.deleted_count
    return len(to_delete)


def sync_universe_docs(db, apply: bool) -> None:
    tickers = sorted(ALLOWED)
    print(f"  etf_universe / watchlists → {tickers}")
    if not apply:
        return
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    for coll, doc_id in (("etf_universe", "default"), ("watchlists", "default")):
        db[coll].update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "_id": doc_id,
                    "tickers": tickers,
                    "count": len(tickers),
                    "updated_at": now,
                    "name": "Universo TFM (etf_universe.json)",
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Limpia MongoDB al universo SPY/IWM/XLE/GLD")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Ejecutar borrados (por defecto solo --dry-run)",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("MONGODB_DB", "tfm"),
        help="Nombre de base de datos (default: tfm)",
    )
    args = parser.parse_args()
    apply = args.apply
    mode = "APPLY" if apply else "DRY-RUN"

    print(f"=== MongoDB cleanup [{mode}] universo={sorted(ALLOWED)} ===\n")

    client = _get_client()
    db = client[args.db]

    print("Colecciones por ticker:")
    preview_ticker_collections(db, apply)

    print("\nTrazas bayesianas:")
    cleanup_bayesian_traces(db, apply)

    print("\nReportes diarios:")
    cleanup_reports(db, apply)

    print("\nSincronizar universo en Mongo:")
    sync_universe_docs(db, apply)

    print("\nNo modificado (macro global): macro_news, macro_context")
    if not apply:
        print("\nPara borrar de verdad: python scripts/cleanup_universe_mongo.py --apply")
    else:
        print("\nListo. Regenera reportes con el pipeline o lambda_report si los necesitas.")


if __name__ == "__main__":
    main()
