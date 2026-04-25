import json
import psycopg2
from datetime import datetime
import logging
import boto3
import time
from huggingface_hub import InferenceClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

# --- CONFIGURACIÓN DE HUGGING FACE ---
HUGGINGFACE_API_KEY = "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"
# Usamos el SDK oficial para que HF gestione el enrutamiento dinámico (evita el 404)
hf_client = InferenceClient(token=HUGGINGFACE_API_KEY)
MODEL_ID = "ProsusAI/finbert"
# -----------------------------------------------

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise

def read_news_from_s3():
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        response = s3_client.get_object(
            Bucket='tfm-unir-datalake',
            Key=f'raw/{today}/news.json'
        )
        return json.loads(response['Body'].read())
    except Exception as e:
        logger.error(f"Error reading news from S3: {str(e)}")
        raise

def analyze_sentiment(headline):
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

def handler(event, context):
    try:
        logger.info("Lambda sentiment analysis (FinBERT SDK) started")
        aurora_creds = get_secret('aurora/credentials')
        news_data = read_news_from_s3()
        today = datetime.now().strftime('%Y-%m-%d')

        connection = psycopg2.connect(
            host=aurora_creds['host'], port=aurora_creds['port'],
            user=aurora_creds['username'], password=aurora_creds['password'],
            database=aurora_creds['dbname']
        )

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

                    sentiment_data = analyze_sentiment(headline)
                    if sentiment_data is None:
                        skipped_headlines += 1
                        continue

                    insert_sentiment_scores(connection, today, ticker, headline, sentiment_data)
                    processed_headlines += 1
                except Exception as e:
                    logger.error(f"Error processing headline for {ticker}: {str(e)}")
                    skipped_headlines += 1
                    continue

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
