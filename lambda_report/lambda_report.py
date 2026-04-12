import json
import boto3
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

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


def get_trading_data(connection, days_back=90):
    """Get trading signals and sentiment scores for the last N days"""
    try:
        cursor = connection.cursor()

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        # Get trading signals
        signals_query = """
            SELECT batch_date, ticker, signal, prob_up, prob_down
            FROM trading_signals
            WHERE batch_date >= %s AND batch_date <= %s
            ORDER BY batch_date, ticker
        """
        cursor.execute(signals_query, (start_date, end_date))
        signals_rows = cursor.fetchall()

        # Convert to DataFrame
        signals_df = pd.DataFrame(
            signals_rows,
            columns=['batch_date', 'ticker', 'signal', 'prob_up', 'prob_down']
        )
        signals_df['batch_date'] = pd.to_datetime(signals_df['batch_date'])

        # Get sentiment scores
        sentiment_query = """
            SELECT batch_date, ticker, sentiment, confidence
            FROM sentiment_scores
            WHERE batch_date >= %s AND batch_date <= %s
            ORDER BY batch_date, ticker
        """
        cursor.execute(sentiment_query, (start_date, end_date))
        sentiment_rows = cursor.fetchall()

        sentiment_df = pd.DataFrame(
            sentiment_rows,
            columns=['batch_date', 'ticker', 'sentiment', 'confidence']
        )
        sentiment_df['batch_date'] = pd.to_datetime(sentiment_df['batch_date'])

        cursor.close()

        return signals_df, sentiment_df

    except Exception as e:
        logger.error(f"Error getting trading data: {str(e)}")
        raise


def calculate_backtesting_metrics(signals_df):
    """Calculate backtesting metrics for all tickers"""
    try:
        metrics = {}

        for ticker in signals_df['ticker'].unique():
            ticker_signals = signals_df[signals_df['ticker'] == ticker].copy()
            ticker_signals = ticker_signals.sort_values('batch_date')

            # Initialize tracking
            starting_capital = 10000
            position_size = 1.0  # 100% of capital
            equity_curve = [starting_capital]
            dates = [ticker_signals.iloc[0]['batch_date']]

            current_capital = starting_capital
            in_position = False
            entry_price = None

            for idx, row in ticker_signals.iterrows():
                signal = row['signal']

                if signal == 'BUY' and not in_position:
                    in_position = True
                    # Use prob_up as a proxy for entry price estimation (simplification)
                    entry_price = current_capital * row['prob_up']

                elif signal == 'SELL' and in_position:
                    in_position = False
                    # Use prob_down as a proxy for exit price estimation (simplification)
                    exit_price = current_capital * row['prob_down']

                    # Calculate return
                    if entry_price > 0:
                        pnl = (exit_price - entry_price) / entry_price
                        current_capital = current_capital * (1 + pnl)

                equity_curve.append(current_capital)
                dates.append(row['batch_date'])

            # Close any open position at the end
            if in_position and entry_price:
                exit_price = current_capital
                pnl = (exit_price - entry_price) / entry_price
                current_capital = current_capital * (1 + pnl)

            # Calculate metrics
            cumulative_return = (current_capital - starting_capital) / starting_capital
            equity_curve = np.array(equity_curve)
            daily_returns = np.diff(equity_curve) / equity_curve[:-1]

            # Sharpe Ratio (annualized)
            risk_free_rate = 0.02
            excess_returns = daily_returns - risk_free_rate / 252  # Daily risk-free rate
            sharpe_ratio = np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252) if np.std(excess_returns) > 0 else 0

            # Maximum Drawdown
            cumulative_max = np.maximum.accumulate(equity_curve)
            drawdown = (equity_curve - cumulative_max) / cumulative_max
            max_drawdown = np.min(drawdown)

            metrics[ticker] = {
                'cumulative_return': float(cumulative_return),
                'sharpe_ratio': float(sharpe_ratio),
                'max_drawdown': float(max_drawdown),
                'final_equity': float(current_capital),
                'starting_capital': starting_capital
            }

            logger.info(f"Backtesting metrics for {ticker}: Return={cumulative_return:.2%}, Sharpe={sharpe_ratio:.2f}, MaxDD={max_drawdown:.2%}")

        return metrics

    except Exception as e:
        logger.error(f"Error calculating backtesting metrics: {str(e)}")
        raise


def save_report_to_s3(report_data):
    """Save report JSON to S3"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        key = f"results/{today}/report.json"

        s3_client.put_object(
            Bucket='tfm-datalake',
            Key=key,
            Body=json.dumps(report_data, indent=2, default=str)
        )

        logger.info(f"Report saved to s3://tfm-datalake/{key}")
        return key

    except Exception as e:
        logger.error(f"Error saving report to S3: {str(e)}")
        raise


def update_batch_log(connection, batch_date, status):
    """Update batch_log table with completion status"""
    try:
        cursor = connection.cursor()

        query = """
            UPDATE batch_log
            SET status = %s
            WHERE batch_date = %s
        """

        cursor.execute(query, (status, batch_date))
        connection.commit()
        cursor.close()

        logger.info(f"Batch log updated: {batch_date} -> {status}")

    except Exception as e:
        logger.error(f"Error updating batch log: {str(e)}")
        raise


def handler(event, context):
    """Main Lambda handler"""
    try:
        logger.info("Lambda report generation started")

        # Get Aurora credentials
        aurora_creds = get_secret('aurora/credentials')

        # Connect to Aurora
        connection = psycopg2.connect(
            host=aurora_creds['host'],
            port=aurora_creds['port'],
            user=aurora_creds['username'],
            password=aurora_creds['password'],
            database=aurora_creds['dbname']
        )

        today = datetime.now().strftime('%Y-%m-%d')

        # Get trading data for the last 90 days
        signals_df, sentiment_df = get_trading_data(connection, days_back=90)
        logger.info(f"Retrieved {len(signals_df)} trading signals and {len(sentiment_df)} sentiment scores")

        # Calculate backtesting metrics
        backtest_metrics = calculate_backtesting_metrics(signals_df)

        # Assemble report
        report_data = {
            'report_date': today,
            'data_period_days': 90,
            'backtesting_metrics': backtest_metrics,
            'summary': {
                'total_tickers': len(backtest_metrics),
                'avg_cumulative_return': np.mean([m['cumulative_return'] for m in backtest_metrics.values()]),
                'avg_sharpe_ratio': np.mean([m['sharpe_ratio'] for m in backtest_metrics.values()]),
                'avg_max_drawdown': np.mean([m['max_drawdown'] for m in backtest_metrics.values()])
            }
        }

        # Save report to S3
        report_key = save_report_to_s3(report_data)

        # Update batch_log
        update_batch_log(connection, today, 'COMPLETED')

        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Report generation completed',
                'report_location': report_key,
                'tickers_analyzed': len(backtest_metrics),
                'summary': report_data['summary']
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
