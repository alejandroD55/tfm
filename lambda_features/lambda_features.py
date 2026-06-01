# deploy: feature layer
"""
lambda_features — Materializa feature_snapshots tras sentiment + indicators
============================================================================
Ejecutar después del stage paralelo (sentiment + indicators) y antes de bayesian.
"""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
rds_client = boto3.client("rds")

try:
    from mongo_utils import (
        read_macro_context as _read_macro_context,
        read_raw_news_ticker as _read_raw_news_ticker,
        read_filtered_news as _read_filtered_news,
        upsert_feature_snapshot as _upsert_feature_snapshot,
        upsert_catalyst_events as _upsert_catalyst_events,
        upsert_fundamental_snapshot as _upsert_fundamental_snapshot,
        distinct_raw_news_tickers as _distinct_raw_news_tickers,
    )
    from feature_builder import (
        build_feature_snapshot,
        fetch_fundamentals_finnhub,
        MODEL_ID_BAYESIAN,
        MODEL_ID_GBM,
    )

    logger.info("mongo_utils + feature_builder cargados")
except ImportError as exc:
    logger.warning(f"Imports feature layer: {exc}")
    _read_macro_context = None
    _read_raw_news_ticker = None
    _read_filtered_news = None
    _upsert_feature_snapshot = None
    _upsert_catalyst_events = None
    _upsert_fundamental_snapshot = None
    _distinct_raw_news_tickers = None
    build_feature_snapshot = None
    fetch_fundamentals_finnhub = None
    MODEL_ID_BAYESIAN = "bayesian_v1.2"
    MODEL_ID_GBM = "gbm_v1"


def resolve_batch_date(event):
    raw = (event or {}).get("batch_date") or (event or {}).get("date")
    return raw[:10] if raw else datetime.now(timezone.utc).strftime("%Y-%m-%d")


def resolve_pipeline_context(event):
    pipeline_ctx = (
        (event or {}).get("pipeline_context", {}) if isinstance(event, dict) else {}
    )
    request = pipeline_ctx.get("request", {}) if isinstance(pipeline_ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}
    batch_date = (
        resolve_batch_date(request)
        if request.get("batch_date")
        else resolve_batch_date(pipeline_ctx)
    )
    run_id = (
        pipeline_ctx.get("run_id")
        or (event or {}).get("run_id")
        or f"legacy-{batch_date}"
    )
    return {"batch_date": batch_date, "run_id": run_id}


def get_secret(secret_name):
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(resp.get("SecretString", resp.get("SecretBinary")))


def connect_to_aurora(aurora_creds):
    auth_mode = str(aurora_creds.get("auth_mode", "")).lower()
    region = os.getenv("AWS_REGION", "eu-north-1")
    host = aurora_creds["host"]
    port = int(aurora_creds.get("port", 5432))
    username = aurora_creds["username"]
    dbname = aurora_creds.get("dbname", "tfm")

    if auth_mode == "iam":
        token = rds_client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=username,
            Region=region,
        )
        return psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=token,
            database=dbname,
            sslmode="require",
        )

    return psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=aurora_creds["password"],
        database=dbname,
    )


def _week_start(batch_date: str) -> str:
    from datetime import datetime as dt

    d = dt.strptime(batch_date[:10], "%Y-%m-%d")
    monday = d - __import__("datetime").timedelta(days=d.weekday())
    return monday.strftime("%Y-%m-%d")


def _headlines_for_ticker(batch_date: str, ticker: str) -> list:
    if _read_filtered_news:
        filtered = _read_filtered_news(batch_date)
        block = filtered.get(ticker.upper()) if filtered else None
        if block:
            return [
                {"headline": h, "source": "bedrock_filtered"}
                for h in block.get("headlines", [])
                if h and str(h).strip()
            ]
    if _read_raw_news_ticker:
        return _read_raw_news_ticker(batch_date, ticker) or []
    return []


def _list_tickers(connection, batch_date: str) -> list:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT ticker FROM (
            SELECT ticker FROM sentiment_scores WHERE batch_date = %s
            UNION
            SELECT ticker FROM technical_indicators WHERE batch_date = %s
        ) t ORDER BY ticker
        """,
        (batch_date, batch_date),
    )
    tickers = [r[0] for r in cursor.fetchall()]
    cursor.close()
    if not tickers and _distinct_raw_news_tickers:
        tickers = _distinct_raw_news_tickers(batch_date)
    return tickers


def _ticker_data(connection, batch_date: str, ticker: str):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT sentiment, confidence, headline, justification
        FROM sentiment_scores
        WHERE batch_date = %s AND ticker = %s
        ORDER BY confidence DESC
        """,
        (batch_date, ticker),
    )
    sentiment_rows = cursor.fetchall()
    cursor.execute(
        """
        SELECT rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower
        FROM technical_indicators
        WHERE batch_date = %s AND ticker = %s LIMIT 1
        """,
        (batch_date, ticker),
    )
    indicators = cursor.fetchone()
    cursor.close()
    return sentiment_rows, indicators


def handler(event, context):
    logger.info("lambda_features iniciado")
    if not build_feature_snapshot or not _upsert_feature_snapshot:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "feature_builder no disponible"}),
        }

    ctx = resolve_pipeline_context(event)
    batch_date = ctx["batch_date"]
    macro_doc = _read_macro_context(batch_date) if _read_macro_context else {}

    aurora_creds = get_secret("aurora/credentials")
    connection = connect_to_aurora(aurora_creds)
    tickers = _list_tickers(connection, batch_date)
    built, skipped = 0, []

    week_start = _week_start(batch_date)

    for ticker in tickers:
        try:
            sentiment_rows, indicators = _ticker_data(connection, batch_date, ticker)
            if not sentiment_rows and not indicators:
                skipped.append({"ticker": ticker, "reason": "no_data"})
                continue

            headlines = _headlines_for_ticker(batch_date, ticker)
            fundamentals = fetch_fundamentals_finnhub(ticker) if fetch_fundamentals_finnhub else None

            snapshot = build_feature_snapshot(
                batch_date,
                ticker,
                sentiment_rows=sentiment_rows,
                indicators_row=indicators,
                macro_doc=macro_doc,
                headlines=headlines,
                fundamentals=fundamentals,
                model_id=os.getenv("FEATURE_MODEL_ID", MODEL_ID_GBM),
            )
            _upsert_feature_snapshot(batch_date, ticker, snapshot)

            catalyst_events = snapshot.get("catalysts", {}).get("events_sample", [])
            if _upsert_catalyst_events and catalyst_events:
                _upsert_catalyst_events(batch_date, ticker, catalyst_events)

            if _upsert_fundamental_snapshot and fundamentals:
                _upsert_fundamental_snapshot(ticker, week_start, fundamentals)

            built += 1
        except Exception as exc:
            logger.error(f"feature_snapshot {ticker}: {exc}")
            skipped.append({"ticker": ticker, "reason": str(exc)})

    connection.close()
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "feature_snapshots built",
                "batch_date": batch_date,
                "built": built,
                "skipped": skipped,
            }
        ),
    }
