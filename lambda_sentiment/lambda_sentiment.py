import json
import psycopg2
from datetime import datetime
import logging
import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

ANTHROPIC_API_KEY = "PEGAR_AQUI_LA_API_KEY"

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
    try:
        system_prompt = "You are a financial analyst. Analyze the sentiment of the given financial news headline. Return ONLY a JSON object with exactly these three fields: 'sentiment' (must be 'bullish', 'bearish', or 'neutral'), 'confidence' (a float between 0.0 and 1.0), and 'justification' (a single sentence explaining the sentiment). Do not include any other text or markdown formatting."
        user_message = f"Analyze the sentiment of this financial news headline: {headline}"

        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        data = {
            "model": "claude-3-haiku-20240307", # Modelo súper rápido y eficiente en costes
            "max_tokens": 256,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}]
        }

        response = requests.post(url, headers=headers, json=data, timeout=15)

        if response.status_code == 200:
            response_body = response.json()
            content = response_body.get('content', [{}])[0].get('text', '{}')
            return json.loads(content)
        else:
            logger.error(f"Anthropic API Error: {response.status_code} - {response.text}")
            return None

    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {str(e)}")
        raise

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
            sentiment_data['sentiment'], float(sentiment_data['confidence']), sentiment_data['justification']
        ))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error inserting sentiment score: {str(e)}")
        raise

def handler(event, context):
    try:
        logger.info("Lambda sentiment analysis (Direct API) started")
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
                'message': 'Sentiment analysis completed via Anthropic API',
                'total_headlines': total_headlines,
                'processed_headlines': processed_headlines,
                'skipped_headlines': skipped_headlines
            })
        }
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
