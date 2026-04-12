import json
import boto3
import psycopg2
from datetime import datetime
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
bedrock_client = boto3.client('bedrock-runtime')


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


def read_news_from_s3():
    """Read news JSON file from S3"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        response = s3_client.get_object(
            Bucket='tfm-datalake',
            Key=f'raw/{today}/news.json'
        )
        news_data = json.loads(response['Body'].read())
        return news_data
    except Exception as e:
        logger.error(f"Error reading news from S3: {str(e)}")
        raise


def analyze_sentiment(headline):
    """Call Amazon Bedrock to analyze sentiment of a headline"""
    try:
        system_prompt = "You are a financial analyst. Analyze the sentiment of the given financial news headline. Return ONLY a JSON object with exactly these three fields: 'sentiment' (must be 'bullish', 'bearish', or 'neutral'), 'confidence' (a float between 0 and 1), and 'justification' (a single sentence explaining the sentiment). Do not include any other text or markdown formatting."

        user_message = f"Analyze the sentiment of this financial news headline: {headline}"

        response = bedrock_client.invoke_model(
            modelId='anthropic.claude-3-haiku-20240307-v1:0',
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                'anthropic_version': 'bedrock-2023-06-01',
                'max_tokens': 256,
                'system': system_prompt,
                'messages': [
                    {
                        'role': 'user',
                        'content': user_message
                    }
                ]
            })
        )

        response_body = json.loads(response['body'].read())
        content = response_body.get('content', [{}])[0].get('text', '{}')

        # Parse the JSON response
        sentiment_data = json.loads(content)

        return sentiment_data

    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error from Bedrock: {str(e)}, content: {content}")
        return None
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {str(e)}")
        raise


def insert_sentiment_scores(connection, batch_date, ticker, headline, sentiment_data):
    """Insert sentiment analysis results into Aurora PostgreSQL"""
    try:
        cursor = connection.cursor()

        query = """
            INSERT INTO sentiment_scores (batch_date, ticker, headline, sentiment, confidence, justification)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor.execute(query, (
            batch_date,
            ticker,
            headline,
            sentiment_data['sentiment'],
            sentiment_data['confidence'],
            sentiment_data['justification']
        ))

        connection.commit()
        cursor.close()

    except Exception as e:
        logger.error(f"Error inserting sentiment score: {str(e)}")
        raise


def handler(event, context):
    """Main Lambda handler"""
    try:
        logger.info("Lambda sentiment analysis started")

        # Get Aurora credentials
        aurora_creds = get_secret('aurora/credentials')

        # Read news from S3
        news_data = read_news_from_s3()
        logger.info(f"Read news data with {len(news_data)} tickers")

        today = datetime.now().strftime('%Y-%m-%d')

        # Connect to Aurora
        connection = psycopg2.connect(
            host=aurora_creds['host'],
            port=aurora_creds['port'],
            user=aurora_creds['username'],
            password=aurora_creds['password'],
            database=aurora_creds['dbname']
        )

        total_headlines = 0
        processed_headlines = 0
        skipped_headlines = 0

        # Process each ticker's news
        for ticker, headlines in news_data.items():
            if not headlines:
                logger.warning(f"No headlines for ticker {ticker}")
                continue

            for article in headlines:
                try:
                    total_headlines += 1
                    headline = article.get('headline', '')

                    if not headline:
                        logger.warning(f"Empty headline for {ticker}")
                        skipped_headlines += 1
                        continue

                    # Analyze sentiment using Bedrock
                    sentiment_data = analyze_sentiment(headline)

                    if sentiment_data is None:
                        logger.warning(f"Failed to parse sentiment for headline: {headline}")
                        skipped_headlines += 1
                        continue

                    # Insert into database
                    insert_sentiment_scores(
                        connection,
                        today,
                        ticker,
                        headline,
                        sentiment_data
                    )

                    processed_headlines += 1
                    logger.info(f"Processed headline for {ticker}: {headline[:50]}...")

                except Exception as e:
                    logger.error(f"Error processing headline for {ticker}: {str(e)}")
                    skipped_headlines += 1
                    continue

        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sentiment analysis completed',
                'total_headlines': total_headlines,
                'processed_headlines': processed_headlines,
                'skipped_headlines': skipped_headlines
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
