# deploy: 2026-05-12 18:03 UTC
import json
import psycopg2
from datetime import datetime
import logging
import boto3
import os
import time
from huggingface_hub import InferenceClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializamos clientes de AWS globales (boto3)
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
rds_client = boto3.client('rds')

# Definimos el ID del modelo a nivel global
MODEL_ID = "ProsusAI/finbert"

# ── MongoDB helper ────────────────────────────────────────────────────────────
try:
    from mongo_utils import (
        upsert_news   as _mongo_upsert_news,
        read_raw_news as _mongo_read_news,
    )
    logger.info("mongo_utils (sentiment) cargado")
except ImportError:
    logger.warning("mongo_utils no disponible")
    _mongo_upsert_news = None
    _mongo_read_news   = None

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

def read_news_from_s3(batch_date):
    # ── 1. MongoDB PRIMARY ──────────────────────────────────────────────────
    if _mongo_read_news:
        try:
            news = _mongo_read_news(batch_date)
            if news:
                logger.info(f"Noticias cargadas desde MongoDB para {batch_date} ({len(news)} tickers)")
                return news
            logger.info("MongoDB no tiene noticias para esta fecha, cayendo a S3")
        except Exception as exc:
            logger.warning(f"MongoDB read_raw_news falló, usando S3: {exc}")

    # ── 2. S3 FALLBACK ───────────────────────────────────────────────────────
    try:
        response = s3_client.get_object(
            Bucket='tfm-unir-datalake',
            Key=f'raw/{batch_date}/news.json'
        )
        logger.info(f"Noticias cargadas desde S3 (fallback) para {batch_date}")
        return json.loads(response['Body'].read())
    except Exception as e:
        logger.error(f"Error reading news from S3: {str(e)}")
        raise

def analyze_sentiment(headline, hf_client):
    # Hacemos hasta 3 intentos por si el modelo está dormido (Cold Start)
    for attempt in range(3):
        try:
            # El SDK se encarga de la URL y de toda la burocracia de la API
            result = hf_client.text_classification(headline, model=MODEL_ID)
            
            if result:
                # Dependiendo de la versión del SDK, devuelve objetos o diccionarios
                if hasattr(result[0], 'score'):
                    top_prediction = max(result, key=lambda x: x.score)
                    label = top_prediction.label.lower()
                    score = top_prediction.score
                else:
                    top_prediction = max(result, key=lambda x: x.get('score', 0))
                    label = top_prediction.get('label', '').lower()
                    score = top_prediction.get('score', 0.5)
                
                # Mapeamos la salida original de FinBERT a nuestra jerga de Trading
                sentiment_map = {'positive': 'bullish', 'negative': 'bearish', 'neutral': 'neutral'}
                final_sentiment = sentiment_map.get(label, 'neutral')
                
                return {
                    'sentiment': final_sentiment,
                    'confidence': round(float(score), 4),
                    'justification': f"FinBERT classified as {label} with {round(score*100, 1)}% confidence"
                }
            return None
                
        except Exception as e:
            error_msg = str(e).lower()
            # Si el modelo está arrancando (503), esperamos y reintentamos
            if "503" in error_msg or "loading" in error_msg:
                logger.info(f"Model is warming up on Hugging Face. Retrying... (Attempt {attempt+1}/3)")
                time.sleep(5)
            else:
                logger.error(f"Hugging Face Inference Error: {str(e)}")
                return None
            
    return None

def insert_sentiment_scores(connection, batch_date, ticker, headline, sentiment_data):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO sentiment_scores (batch_date, ticker, headline, sentiment, confidence, justification)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker, headline) DO NOTHING
        """
        cursor.execute(query, (
            batch_date, ticker, headline,
            sentiment_data['sentiment'], sentiment_data['confidence'], sentiment_data['justification']
        ))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error inserting sentiment score: {str(e)}")
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
        logger.info("Lambda sentiment analysis (FinBERT SDK) started")

        # OBTENEMOS LAS CREDENCIALES DE FORMA SEGURA
        aurora_creds = get_secret('aurora/credentials')
        hf_creds = get_secret('huggingface/api_key')
        
        # INICIALIZAMOS EL CLIENTE
        hf_client = InferenceClient(token=hf_creds['api_key'])
        
        ctx = resolve_pipeline_context(event)
        today = ctx['batch_date']
        news_data = read_news_from_s3(today)

        connection = connect_to_aurora(aurora_creds)

        total_headlines = 0
        processed_headlines = 0
        skipped_headlines = 0

        for ticker, headlines in news_data.items():
            if not headlines: continue
            for article in headlines:
                try:
                    total_headlines += 1
                    headline = article.get('headline', '')
                    if not headline:
                        skipped_headlines += 1
                        continue

                    # Pasamos el hf_client inicializado a la función
                    sentiment_data = analyze_sentiment(headline, hf_client)
                    if sentiment_data is None:
                        skipped_headlines += 1
                        continue

                    insert_sentiment_scores(connection, today, ticker, headline, sentiment_data)
                    processed_headlines += 1

                    # MongoDB: guardar articulo con scoring FinBERT
                    if _mongo_upsert_news:
                        _mongo_upsert_news(today, ticker, article, sentiment_data)
                except Exception as e:
                    logger.error(f"Error processing headline for {ticker}: {str(e)}")
                    skipped_headlines += 1
                    continue

        upsert_pipeline_kpi(connection, today, ctx['run_id'], ctx['trigger_type'], 'sentiment', {
            'tickers_in_news': len(news_data),
            'headlines_total': total_headlines,
            'headlines_processed': processed_headlines,
            'headlines_skipped': skipped_headlines,
            'trigger_type': ctx['trigger_type'],
        })

        connection.close()
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sentiment analysis completed via FinBERT',
                'total_headlines': total_headlines,
                'processed_headlines': processed_headlines,
                'skipped_headlines': skipped_headlines
            })
        }
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
