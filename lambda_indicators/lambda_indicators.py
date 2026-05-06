import json
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from io import StringIO
import logging
import pandas_ta_classic as ta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

def resolve_batch_date(event):
    raw_date = (event or {}).get('batch_date') or (event or {}).get('date')
    if raw_date:
        return raw_date[:10]
    return datetime.now().strftime('%Y-%m-%d')

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise

def read_ohlcv_from_s3(batch_date):
    try:
        response = s3_client.get_object(
            Bucket='tfm-unir-datalake',
            Key=f'raw/{batch_date}/ohlcv.csv'
        )
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df
    except Exception as e:
        logger.error(f"Error reading OHLCV from S3: {str(e)}")
        raise

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

def upsert_pipeline_kpi(connection, batch_date, stage, metrics):
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO pipeline_kpis (batch_date, stage, metrics)
            VALUES (%s, %s, %s::jsonb)
            ON CONFLICT (batch_date, stage) DO UPDATE
            SET metrics = EXCLUDED.metrics,
                updated_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (batch_date, stage, json.dumps(metrics)))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error upserting pipeline KPI: {str(e)}")
        raise

def handler(event, context):
    try:
        logger.info("Lambda technical indicators started")
        today = resolve_batch_date(event)
        aurora_creds = get_secret('aurora/credentials')
        ohlcv_df = read_ohlcv_from_s3(today)
        indicators_by_ticker = calculate_technical_indicators(ohlcv_df)

        connection = psycopg2.connect(
            host=aurora_creds['host'], port=aurora_creds['port'],
            user=aurora_creds['username'], password=aurora_creds['password'],
            database=aurora_creds['dbname']
        )

        for ticker, indicators_df in indicators_by_ticker.items():
            try:
                insert_technical_indicators(connection, today, ticker, indicators_df)
            except Exception as e:
                logger.error(f"Error inserting indicators for {ticker}: {str(e)}")
                continue

        upsert_pipeline_kpi(connection, today, 'indicators', {
            'tickers_in_ohlcv': int(ohlcv_df['Ticker'].nunique()) if 'Ticker' in ohlcv_df.columns else 0,
            'tickers_with_indicators': len(indicators_by_ticker),
            'ohlcv_rows_total': int(len(ohlcv_df))
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
