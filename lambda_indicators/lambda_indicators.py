# deploy: 2026-05-12 18:03 UTC
import json
import boto3
import pandas as pd
import psycopg2
import os
from datetime import datetime
import logging
import pandas_ta_classic as ta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client('secretsmanager')
rds_client = boto3.client('rds')

# ── MongoDB helper ────────────────────────────────────────────────────────────
try:
    from mongo_utils import read_ohlcv as _mongo_read_ohlcv
    logger.info("mongo_utils (indicators) cargado")
except ImportError:
    logger.warning("mongo_utils no disponible")
    _mongo_read_ohlcv = None

def resolve_batch_date(event):
    raw_date = (event or {}).get('batch_date') or (event or {}).get('date')
    if raw_date:
        return raw_date[:10]
    return datetime.now().strftime('%Y-%m-%d')


def resolve_pipeline_context(event):
    pipeline_ctx = (event or {}).get('pipeline_context', {}) if isinstance(event, dict) else {}
    request = pipeline_ctx.get('request', {}) if isinstance(pipeline_ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}
    batch_date = resolve_batch_date(request) if request.get('batch_date') else resolve_batch_date(pipeline_ctx)
    run_id = pipeline_ctx.get('run_id') or (event or {}).get('run_id') or f"legacy-{batch_date}"
    trigger_type = request.get('trigger_type')
    if trigger_type not in ('manual', 'scheduled'):
        trigger_type = 'manual' if request.get('ticker') or request.get('tickers') else 'scheduled'
    return {'batch_date': batch_date, 'run_id': run_id, 'trigger_type': trigger_type}

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def connect_to_aurora(aurora_creds):
    auth_mode = str(aurora_creds.get('auth_mode', '')).lower()
    region = os.getenv('AWS_REGION', 'eu-north-1')
    host = aurora_creds['host']
    port = int(aurora_creds.get('port', 5432))
    username = aurora_creds['username']
    dbname = aurora_creds.get('dbname', 'tfm')

    if auth_mode == 'iam':
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
            sslmode='require',
        )

    return psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=aurora_creds['password'],
        database=dbname,
    )

def read_ohlcv_for_batch(batch_date):
    if not _mongo_read_ohlcv:
        raise RuntimeError(
            "mongo_utils no disponible: se requiere read_ohlcv desde MongoDB."
        )
    try:
        mongo_data = _mongo_read_ohlcv(batch_date)
    except Exception as exc:
        logger.error(f"MongoDB read_ohlcv falló: {exc}")
        raise
    if not mongo_data:
        raise ValueError(
            f"No hay OHLCV en MongoDB para batch_date={batch_date}. "
            "Ejecuta antes la lambda de ingestion."
        )
    logger.info(f"OHLCV cargado desde MongoDB ({len(mongo_data)} tickers)")
    all_rows = []
    for ticker_sym, rows in mongo_data.items():
        for r in rows:
            all_rows.append({
                "Date":   r.get("date", ""),
                "Ticker": ticker_sym,
                "Open":   float(r.get("open",   0) or 0),
                "High":   float(r.get("high",   0) or 0),
                "Low":    float(r.get("low",    0) or 0),
                "Close":  float(r.get("close",  0) or 0),
                "Volume": float(r.get("volume", 0) or 0),
            })
    if not all_rows:
        raise ValueError(f"MongoDB ohlcv sin filas para {batch_date}")
    df = pd.DataFrame(all_rows)
    df.set_index("Date", inplace=True)
    return df

def calculate_technical_indicators(df):
    try:
        indicators_by_ticker = {}
        for ticker, group in df.groupby('Ticker'):
            if len(group) < 50:
                continue
            group = group.copy().sort_index()
            group = group.copy().reset_index(drop=True)
            rsi_14 = ta.rsi(group['Close'], length=14)
            sma_20 = ta.sma(group['Close'], length=20)
            sma_50 = ta.sma(group['Close'], length=50)
            bbands = ta.bbands(group['Close'], length=20, std=2)

            results = pd.DataFrame({
                'ticker': ticker,
                'date': group.index,
                'close_price': group['Close'].values,
                'rsi_14': rsi_14.values,
                'sma_20': sma_20.values,
                'sma_50': sma_50.values,
                'bb_upper': bbands.iloc[:, 2].values if len(bbands.columns) > 2 else None,
                'bb_middle': bbands.iloc[:, 1].values if len(bbands.columns) > 1 else None,
                'bb_lower': bbands.iloc[:, 0].values if len(bbands.columns) > 0 else None,
            })
            indicators_by_ticker[ticker] = results
        return indicators_by_ticker
    except Exception as e:
        logger.error(f"Error calculating technical indicators: {str(e)}")
        raise

def insert_technical_indicators(connection, batch_date, ticker, indicators_df):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO technical_indicators
            (batch_date, ticker, close_price, rsi_14, sma_20, sma_50, bb_upper, bb_middle, bb_lower)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker) DO NOTHING
        """
        latest_row = indicators_df.iloc[-1]
        cursor.execute(query, (
            batch_date, ticker,
            float(latest_row['close_price']) if pd.notna(latest_row['close_price']) else None,
            float(latest_row['rsi_14']) if pd.notna(latest_row['rsi_14']) else None,
            float(latest_row['sma_20']) if pd.notna(latest_row['sma_20']) else None,
            float(latest_row['sma_50']) if pd.notna(latest_row['sma_50']) else None,
            float(latest_row['bb_upper']) if pd.notna(latest_row['bb_upper']) else None,
            float(latest_row['bb_middle']) if pd.notna(latest_row['bb_middle']) else None,
            float(latest_row['bb_lower']) if pd.notna(latest_row['bb_lower']) else None,
        ))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error inserting technical indicators: {str(e)}")
        raise

def upsert_pipeline_kpi(connection, batch_date, run_id, trigger_type, stage, metrics):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO pipeline_kpis (batch_date, run_id, trigger_type, stage, metrics)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id, stage) DO UPDATE
            SET metrics = EXCLUDED.metrics,
                updated_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (batch_date, run_id, trigger_type, stage, json.dumps(metrics)))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error upserting pipeline KPI: {str(e)}")
        raise

def handler(event, context):
    try:
        logger.info("Lambda technical indicators started")
        ctx = resolve_pipeline_context(event)
        today = ctx['batch_date']
        aurora_creds = get_secret('aurora/credentials')
        ohlcv_df = read_ohlcv_for_batch(today)
        indicators_by_ticker = calculate_technical_indicators(ohlcv_df)

        connection = connect_to_aurora(aurora_creds)

        for ticker, indicators_df in indicators_by_ticker.items():
            try:
                insert_technical_indicators(connection, today, ticker, indicators_df)
            except Exception as e:
                logger.error(f"Error inserting indicators for {ticker}: {str(e)}")
                continue

        upsert_pipeline_kpi(connection, today, ctx['run_id'], ctx['trigger_type'], 'indicators', {
            'tickers_in_ohlcv': int(ohlcv_df['Ticker'].nunique()) if 'Ticker' in ohlcv_df.columns else 0,
            'tickers_with_indicators': len(indicators_by_ticker),
            'ohlcv_rows_total': int(len(ohlcv_df)),
            'trigger_type': ctx['trigger_type'],
        })

        connection.close()
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Technical indicators calculation completed',
                'tickers_processed': len(indicators_by_ticker)
            })
        }
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
