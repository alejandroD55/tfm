import json
import boto3
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response: return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        raise

def get_trading_data(connection, days_back=90):
    try:
        cursor = connection.cursor()
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        cursor.execute("SELECT batch_date, ticker, signal, prob_up, prob_down FROM trading_signals WHERE batch_date >= %s AND batch_date <= %s ORDER BY batch_date, ticker", (start_date, end_date))
        signals_df = pd.DataFrame(cursor.fetchall(), columns=['batch_date', 'ticker', 'signal', 'prob_up', 'prob_down'])
        
        cursor.execute("SELECT batch_date, ticker, sentiment, confidence FROM sentiment_scores WHERE batch_date >= %s AND batch_date <= %s ORDER BY batch_date, ticker", (start_date, end_date))
        sentiment_df = pd.DataFrame(cursor.fetchall(), columns=['batch_date', 'ticker', 'sentiment', 'confidence'])
        
        cursor.close()
        return signals_df, sentiment_df
    except Exception as e:
        raise

def calculate_backtesting_metrics(signals_df):
    try:
        metrics = {}
        for ticker in signals_df['ticker'].unique():
            ticker_signals = signals_df[signals_df['ticker'] == ticker].sort_values('batch_date')
            starting_capital, current_capital = 10000, 10000
            equity_curve = [starting_capital]
            in_position, entry_price = False, None

            for idx, row in ticker_signals.iterrows():
                if row['signal'] == 'BUY' and not in_position:
                    in_position, entry_price = True, current_capital * row['prob_up']
                elif row['signal'] == 'SELL' and in_position:
                    in_position = False
                    exit_price = current_capital * row['prob_down']
                    current_capital *= (1 + (exit_price - entry_price) / entry_price)
                equity_curve.append(current_capital)

            if in_position and entry_price:
                current_capital *= (1 + (current_capital - entry_price) / entry_price)

            cumulative_return = (current_capital - starting_capital) / starting_capital
            daily_returns = np.diff(equity_curve) / equity_curve[:-1]
            excess_returns = daily_returns - 0.02 / 252
            sharpe_ratio = np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252) if np.std(excess_returns) > 0 else 0
            max_drawdown = np.min((equity_curve - np.maximum.accumulate(equity_curve)) / np.maximum.accumulate(equity_curve))

            metrics[ticker] = {
                'cumulative_return': float(cumulative_return), 'sharpe_ratio': float(sharpe_ratio),
                'max_drawdown': float(max_drawdown), 'final_equity': float(current_capital)
            }
        return metrics
    except Exception as e:
        raise

def save_report_to_s3(report_data):
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        key = f"results/{today}/report.json"
        s3_client.put_object(Bucket='tfm-unir-datalake', Key=key, Body=json.dumps(report_data, indent=2, default=str))
        return key
    except Exception as e:
        raise

def handler(event, context):
    try:
        logger.info("Lambda report generation started")
        aurora_creds = get_secret('aurora/credentials')
        connection = psycopg2.connect(**aurora_creds)
        today = datetime.now().strftime('%Y-%m-%d')

        signals_df, sentiment_df = get_trading_data(connection, days_back=90)
        backtest_metrics = calculate_backtesting_metrics(signals_df) if not signals_df.empty else {}

        report_data = {
            'report_date': today, 'data_period_days': 90, 'backtesting_metrics': backtest_metrics,
            'summary': {
                'total_tickers': len(backtest_metrics),
                'avg_cumulative_return': np.mean([m['cumulative_return'] for m in backtest_metrics.values()]) if backtest_metrics else 0,
                'avg_sharpe_ratio': np.mean([m['sharpe_ratio'] for m in backtest_metrics.values()]) if backtest_metrics else 0,
                'avg_max_drawdown': np.mean([m['max_drawdown'] for m in backtest_metrics.values()]) if backtest_metrics else 0
            }
        }
        report_key = save_report_to_s3(report_data)

        cursor = connection.cursor()
        cursor.execute("UPDATE batch_log SET status = %s WHERE batch_date = %s", ('COMPLETED', today))
        connection.commit()
        cursor.close()
        connection.close()

        return {'statusCode': 200, 'body': json.dumps({'message': 'Success', 'report': report_key})}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
