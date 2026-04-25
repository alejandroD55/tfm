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

        # CRUZAMOS LAS TABLAS PARA OBTENER EL PRECIO REAL (close_price)
        query = """
            SELECT ts.batch_date, ts.ticker, ts.signal, ti.close_price
            FROM trading_signals ts
            JOIN technical_indicators ti ON ts.batch_date = ti.batch_date AND ts.ticker = ti.ticker
            WHERE ts.batch_date >= %s AND ts.batch_date <= %s 
            ORDER BY ts.batch_date, ts.ticker
        """
        cursor.execute(query, (start_date, end_date))
        signals_df = pd.DataFrame(cursor.fetchall(), columns=['batch_date', 'ticker', 'signal', 'close_price'])
        
        cursor.close()
        return signals_df
    except Exception as e:
        raise

def calculate_backtesting_metrics(signals_df):
    try:
        metrics = {}
        for ticker in signals_df['ticker'].unique():
            ticker_signals = signals_df[signals_df['ticker'] == ticker].sort_values('batch_date')
            starting_capital = 10000.0
            current_capital = starting_capital
            equity_curve = [starting_capital]
            in_position = False
            entry_price = 0.0

            for idx, row in ticker_signals.iterrows():
                current_price = float(row['close_price']) if row['close_price'] else 0.0
                if current_price == 0:
                    continue

                if row['signal'] == 'BUY' and not in_position:
                    in_position = True
                    entry_price = current_price
                elif row['signal'] == 'SELL' and in_position:
                    in_position = False
                    # Retorno real basado en la variación del precio
                    trade_return = (current_price - entry_price) / entry_price
                    current_capital *= (1 + trade_return)
                    
                equity_curve.append(current_capital)

            # Si seguimos dentro al final del periodo, valoramos a precio de mercado
            if in_position and entry_price > 0:
                final_price = float(ticker_signals.iloc[-1]['close_price'])
                unrealized_return = (final_price - entry_price) / entry_price
                final_equity = current_capital * (1 + unrealized_return)
            else:
                final_equity = current_capital

            cumulative_return = (final_equity - starting_capital) / starting_capital
            
            # Cálculos estadísticos (solo si hay más de 1 día de datos)
            if len(equity_curve) > 2:
                daily_returns = np.diff(equity_curve) / equity_curve[:-1]
                excess_returns = daily_returns - (0.02 / 252) # Risk-free rate 2%
                std_dev = np.std(excess_returns)
                sharpe_ratio = (np.mean(excess_returns) / std_dev * np.sqrt(252)) if std_dev > 0 else 0.0
                
                peak = np.maximum.accumulate(equity_curve)
                drawdown = (equity_curve - peak) / peak
                max_drawdown = np.min(drawdown)
            else:
                sharpe_ratio = 0.0
                max_drawdown = 0.0

            metrics[ticker] = {
                'cumulative_return': round(float(cumulative_return), 4), 
                'sharpe_ratio': round(float(sharpe_ratio), 4),
                'max_drawdown': round(float(max_drawdown), 4), 
                'final_equity': round(float(final_equity), 2)
            }
        return metrics
    except Exception as e:
        logger.error(f"Error in math: {e}")
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
        connection = psycopg2.connect(
            host=aurora_creds['host'], port=aurora_creds['port'],
            user=aurora_creds['username'], password=aurora_creds['password'],
            database=aurora_creds['dbname']
        )
        today = datetime.now().strftime('%Y-%m-%d')

        signals_df = get_trading_data(connection, days_back=90)
        backtest_metrics = calculate_backtesting_metrics(signals_df) if not signals_df.empty else {}

        report_data = {
            'report_date': today, 
            'data_period_days': 90, 
            'backtesting_metrics': backtest_metrics,
            'summary': {
                'total_tickers': len(backtest_metrics),
                'avg_cumulative_return': round(np.mean([m['cumulative_return'] for m in backtest_metrics.values()]), 4) if backtest_metrics else 0,
                'avg_sharpe_ratio': round(np.mean([m['sharpe_ratio'] for m in backtest_metrics.values()]), 4) if backtest_metrics else 0,
                'avg_max_drawdown': round(np.mean([m['max_drawdown'] for m in backtest_metrics.values()]), 4) if backtest_metrics else 0
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
        logger.error(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
