import json
import boto3
import yfinance as yf
import requests
import psycopg2
import os
from datetime import datetime, timedelta
import pandas as pd
import logging
from io import StringIO

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
rds_client = boto3.client('rds')

# ── MongoDB helper ────────────────────────────────────────────────────────────
try:
    from mongo_utils import upsert_raw_news as _mongo_upsert_raw_news
    from mongo_utils import upsert_ohlcv_bulk as _mongo_upsert_ohlcv
    logger.info("mongo_utils (ingestion) cargado")
except ImportError:
    _mongo_upsert_raw_news = None
    _mongo_upsert_ohlcv   = None


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


def resolve_batch_date(event):
    """Resolve a consistent batch date from Step Functions payload."""
    raw_date = (event or {}).get('batch_date') or (event or {}).get('date')
    if raw_date:
        return raw_date[:10]
    return datetime.now().strftime('%Y-%m-%d')


def resolve_pipeline_context(event):
    """Normalize execution metadata for manual/scheduled runs."""
    pipeline_ctx = (event or {}).get('pipeline_context', {}) if isinstance(event, dict) else {}
    request = pipeline_ctx.get('request', {}) if isinstance(pipeline_ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}

    batch_date = resolve_batch_date(request) if request.get('batch_date') else resolve_batch_date(pipeline_ctx)
    run_id = pipeline_ctx.get('run_id') or (event or {}).get('run_id') or f"legacy-{batch_date}"
    execution_name = pipeline_ctx.get('execution_name')

    requested_tickers = []
    if request.get('ticker'):
        requested_tickers = [str(request['ticker']).upper()]
    elif request.get('tickers'):
        requested_tickers = [str(t).upper() for t in request['tickers'] if t]

    trigger_type = request.get('trigger_type')
    if trigger_type not in ('manual', 'scheduled'):
        trigger_type = 'manual' if requested_tickers else 'scheduled'

    return {
        'batch_date': batch_date,
        'run_id': run_id,
        'execution_name': execution_name,
        'trigger_type': trigger_type,
        'requested_tickers': requested_tickers,
    }


def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def read_etf_config():
    """Read ETF configuration from S3"""
    try:
        response = s3_client.get_object(Bucket='tfm-unir-config', Key='etf_universe.json')
        config = json.loads(response['Body'].read())
        return config.get('tickers', [])
    except Exception as e:
        logger.error(f"Error reading ETF config: {str(e)}")
        raise
    

def download_ohlcv_data(tickers):
    """Download OHLCV data for the last 90 days using yfinance"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90) # <-- 90 DÍAS

        all_data = {}
        for ticker in tickers:
            try:
                data = yf.download(ticker, start=start_date, end=end_date, progress=False)
                if not data.empty:
                    # --- EL FIX SALVAVIDAS: Quitar la cabecera doble de yfinance ---
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.droplevel(1)
                    # ---------------------------------------------------------------
                    
                    data['Ticker'] = ticker
                    all_data[ticker] = data
                    logger.info(f"Downloaded OHLCV data for {ticker}")
                else:
                    logger.warning(f"No data found for ticker {ticker}")
            except Exception as e:
                logger.error(f"Error downloading data for {ticker}: {str(e)}")
                continue

        return all_data
    except Exception as e:
        logger.error(f"Error in download_ohlcv_data: {str(e)}")
        raise


def download_news(tickers, finnhub_key):
    """Download financial news for each ticker from Finnhub API"""
    try:
        news_data = {}
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1)

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        for ticker in tickers:
            try:
                url = 'https://finnhub.io/api/v1/company-news'
                params = {
                    'symbol': ticker,
                    'from': start_date_str,
                    'to': end_date_str,
                    'token': finnhub_key
                }
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    news_data[ticker] = response.json()
                    logger.info(f"Downloaded news for {ticker}")
                else:
                    logger.warning(f"API error for {ticker}: {response.status_code}")
                    news_data[ticker] = []
            except Exception as e:
                logger.error(f"Error downloading news for {ticker}: {str(e)}")
                news_data[ticker] = []

        return news_data
    except Exception as e:
        logger.error(f"Error in download_news: {str(e)}")
        raise


def save_to_s3(data, bucket, key, is_json=True):
    """Save data to S3 bucket"""
    try:
        if is_json:
            content = json.dumps(data)
        else:
            content = data

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content
        )
        logger.info(f"Saved data to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Error saving to S3: {str(e)}")
        raise


def insert_batch_log(connection, batch_date, run_id, trigger_type, execution_name, requested_tickers, status, tickers_processed):
    """Insert batch log entry to Aurora PostgreSQL"""
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO batch_log (batch_date, run_id, trigger_type, execution_name, requested_tickers, status, tickers_processed)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (run_id) DO UPDATE
            SET updated_at = CURRENT_TIMESTAMP, 
                batch_date = EXCLUDED.batch_date,
                status = EXCLUDED.status, 
                tickers_processed = EXCLUDED.tickers_processed
        """
        cursor.execute(
            query,
            (
                batch_date,
                run_id,
                trigger_type,
                execution_name,
                json.dumps(requested_tickers),
                status,
                tickers_processed,
            ),
        )
        connection.commit()
        cursor.close()
        logger.info(f"Batch log upserted: run_id={run_id}, date={batch_date}, status={status}, tickers={tickers_processed}")
    except Exception as e:
        logger.error(f"Error inserting batch log: {str(e)}")
        raise


def upsert_pipeline_kpi(connection, batch_date, run_id, trigger_type, stage, metrics):
    """Persist stage KPIs for observability."""
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
    """Main Lambda handler.

    Acepta un parametro opcional 'ticker' (o 'tickers') en el evento para
    ejecutar el pipeline solo para un subconjunto de ETFs.

    Ejemplos de evento:
      {}                          -> procesa todos los tickers del universo
      {"ticker": "SPY"}           -> procesa solo SPY
      {"tickers": ["SPY","QQQ"]}  -> procesa SPY y QQQ
      {"batch_date": "2024-01-15","ticker": "SPY"}
    """
    try:
        logger.info("Lambda ingestion started")
        logger.info(f"Event received: {json.dumps(event or {})}")

        # Get configurations
        aurora_creds = get_secret('aurora/credentials')
        finnhub_key = get_secret('finnhub/api_key')['api_key']

        # Read ETF configuration (universe completo)
        all_tickers = read_etf_config()
        ctx = resolve_pipeline_context(event)
        batch_date = ctx['batch_date']

        # ── Filtrar por ticker si se especifica en el evento ──────────────────
        if len(ctx['requested_tickers']) == 1:
            tickers = ctx['requested_tickers']
            logger.info(f"Single-ticker mode: {tickers[0]}")
        elif len(ctx['requested_tickers']) > 1:
            tickers = ctx['requested_tickers']
            logger.info(f"Multi-ticker mode: {tickers}")
        else:
            tickers = all_tickers
            logger.info(f"Full-universe mode: {len(tickers)} tickers")
        # ─────────────────────────────────────────────────────────────────────

        logger.info(f"Processing {len(tickers)} tickers for batch_date={batch_date}")

        # Download OHLCV data
        ohlcv_data = download_ohlcv_data(tickers)

        # Download news
        news_data = download_news(tickers, finnhub_key)

        # Combine all OHLCV data into a single DataFrame
        if not ohlcv_data:
            raise ValueError("No OHLCV data downloaded for any ticker")
        combined_ohlcv = pd.concat([df for df in ohlcv_data.values()])

        today = batch_date

        # ── MongoDB PRIMARY: guardar OHLCV y news en MongoDB ─────────────────
        # MongoDB es ahora la fuente principal de datos raw.
        # S3 queda como backup (se puede eliminar cuando MongoDB este validado).
        if _mongo_upsert_ohlcv:
            for ticker_sym, ticker_df in ohlcv_data.items():
                rows = []
                for idx, row in ticker_df.iterrows():
                    rows.append({
                        "date":   str(idx.date()) if hasattr(idx, 'date') else str(idx),
                        "open":   float(row.get("Open", 0) or 0),
                        "high":   float(row.get("High", 0) or 0),
                        "low":    float(row.get("Low",  0) or 0),
                        "close":  float(row.get("Close",0) or 0),
                        "volume": float(row.get("Volume",0) or 0),
                    })
                _mongo_upsert_ohlcv(today, ticker_sym, rows)
            logger.info(f"MongoDB: OHLCV guardado para {len(ohlcv_data)} tickers")

        if _mongo_upsert_raw_news:
            for ticker_sym, articles in news_data.items():
                if articles:
                    _mongo_upsert_raw_news(today, ticker_sym, articles)
            logger.info(f"MongoDB: noticias guardadas para {sum(1 for a in news_data.values() if a)} tickers")

        # ── S3 BACKUP: mantener escritura S3 durante la transicion ───────────
        # Una vez validado MongoDB, puedes comentar o eliminar este bloque.
        csv_buffer = StringIO()
        combined_ohlcv.to_csv(csv_buffer)
        ohlcv_key = f"raw/{today}/ohlcv.csv"
        save_to_s3(csv_buffer.getvalue(), 'tfm-unir-datalake', ohlcv_key, is_json=False)

        news_key = f"raw/{today}/news.json"
        save_to_s3(news_data, 'tfm-unir-datalake', news_key, is_json=True)
        # ─────────────────────────────────────────────────────────────────────

        # Connect to Aurora and insert batch log
        connection = connect_to_aurora(aurora_creds)

        insert_batch_log(
            connection,
            today,
            ctx['run_id'],
            ctx['trigger_type'],
            ctx['execution_name'],
            tickers,
            'STARTED',
            len(tickers),
        )
        upsert_pipeline_kpi(connection, today, ctx['run_id'], ctx['trigger_type'], 'ingestion', {
            'tickers_expected': len(tickers),
            'tickers_with_ohlcv': len(ohlcv_data),
            'tickers_with_news': sum(1 for _, items in news_data.items() if items),
            'headlines_total': sum(len(items) for _, items in news_data.items()),
            'ohlcv_rows_total': int(len(combined_ohlcv)),
            'trigger_type': ctx['trigger_type'],
        })

        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Ingestion completed successfully',
                'tickers_processed': len(tickers),
                'ohlcv_saved': ohlcv_key,
                'news_saved': news_key
            })
        }

    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
