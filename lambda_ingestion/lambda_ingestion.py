import json
import boto3
import yfinance as yf
import requests
import psycopg2
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


def insert_batch_log(connection, batch_date, status, tickers_processed):
    """Insert batch log entry to Aurora PostgreSQL"""
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO batch_log (batch_date, status, tickers_processed)
            VALUES (%s, %s, %s)
            ON CONFLICT (batch_date) DO UPDATE 
            SET updated_at = CURRENT_TIMESTAMP, 
                status = EXCLUDED.status, 
                tickers_processed = EXCLUDED.tickers_processed
        """
        cursor.execute(query, (batch_date, status, tickers_processed))
        connection.commit()
        cursor.close()
        logger.info(f"Batch log inserted: {batch_date}, {status}, {tickers_processed} tickers")
    except Exception as e:
        logger.error(f"Error inserting batch log: {str(e)}")
        raise


def handler(event, context):
    """Main Lambda handler"""
    try:
        logger.info("Lambda ingestion started")

        # Get configurations
        aurora_creds = get_secret('aurora/credentials')
        finnhub_key = get_secret('finnhub/api_key')['api_key']

        # Read ETF configuration
        tickers = read_etf_config()
        logger.info(f"Processing {len(tickers)} tickers")

        # Download OHLCV data
        ohlcv_data = download_ohlcv_data(tickers)

        # Download news
        news_data = download_news(tickers, finnhub_key)

        # Combine all OHLCV data into a single DataFrame
        combined_ohlcv = pd.concat([df for df in ohlcv_data.values()])

        # Save to S3
        today = datetime.now().strftime('%Y-%m-%d')

        # Save OHLCV as CSV
        csv_buffer = StringIO()
        combined_ohlcv.to_csv(csv_buffer)
        ohlcv_key = f"raw/{today}/ohlcv.csv"
        save_to_s3(csv_buffer.getvalue(), 'tfm-unir-datalake', ohlcv_key, is_json=False)

        # Save news as JSON
        news_key = f"raw/{today}/news.json"
        save_to_s3(news_data, 'tfm-unir-datalake', news_key, is_json=True)

        # Connect to Aurora and insert batch log
        connection = psycopg2.connect(
            host=aurora_creds['host'],
            port=aurora_creds['port'],
            user=aurora_creds['username'],
            password=aurora_creds['password'],
            database=aurora_creds['dbname']
        )

        insert_batch_log(connection, today, 'STARTED', len(tickers))

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
