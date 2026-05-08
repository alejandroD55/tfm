import json
import boto3
import psycopg2
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
rds_client = boto3.client('rds')

# --- CONFIGURACIÓN GLOBAL ---
DAYS_BACK = 365  # <-- Cambiado a 1 año de histórico

def resolve_batch_date(event):
    raw_date = (event or {}).get('batch_date') or (event or {}).get('date')
    if raw_date:
        return raw_date[:10]
    return datetime.now().strftime('%Y-%m-%d')

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response: return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception:
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
            host=host, port=port, user=username,
            password=token, database=dbname, sslmode='require',
        )

    return psycopg2.connect(
        host=host, port=port, user=username,
        password=aurora_creds['password'], database=dbname,
    )

def get_trading_data(connection, report_date, days_back=DAYS_BACK):
    try:
        cursor = connection.cursor()
        end_date = pd.to_datetime(report_date).date()
        start_date = end_date - timedelta(days=days_back)

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
    except Exception:
        raise

def calculate_backtesting_metrics(signals_df):
    try:
        metrics = {}
        diagnostics = {}
        for ticker in signals_df['ticker'].unique():
            ticker_signals = signals_df[signals_df['ticker'] == ticker].sort_values('batch_date')
            starting_capital = 10000.0
            current_capital = starting_capital
            equity_curve = [starting_capital]
            
            # --- NUEVA LÓGICA LONG/SHORT ---
            current_position = 0 # 1 = Long, -1 = Short, 0 = Cash
            entry_price = 0.0
            trades_returns = []
            
            signals_count = ticker_signals['signal'].value_counts().to_dict()

            for idx, row in ticker_signals.iterrows():
                current_price = float(row['close_price']) if row['close_price'] else 0.0
                if current_price == 0:
                    continue

                signal = row['signal']

                # Lógica de cierre y apertura de posiciones
                if signal == 'BUY':
                    if current_position == -1: # Cerrar Short
                        trade_return = (entry_price - current_price) / entry_price
                        current_capital *= (1 + trade_return)
                        trades_returns.append(float(trade_return))
                    
                    if current_position != 1: # Abrir Long
                        current_position = 1
                        entry_price = current_price

                elif signal == 'SELL':
                    if current_position == 1: # Cerrar Long
                        trade_return = (current_price - entry_price) / entry_price
                        current_capital *= (1 + trade_return)
                        trades_returns.append(float(trade_return))
                    
                    if current_position != -1: # Abrir Short
                        current_position = -1
                        entry_price = current_price

                elif signal == 'HOLD':
                    if current_position == 1: # Cerrar Long
                        trade_return = (current_price - entry_price) / entry_price
                        current_capital *= (1 + trade_return)
                        trades_returns.append(float(trade_return))
                        current_position = 0
                    elif current_position == -1: # Cerrar Short
                        trade_return = (entry_price - current_price) / entry_price
                        current_capital *= (1 + trade_return)
                        trades_returns.append(float(trade_return))
                        current_position = 0

                equity_curve.append(current_capital)

            # Valoración MTM al final del periodo
            final_equity = current_capital
            if current_position == 1 and entry_price > 0:
                final_price = float(ticker_signals.iloc[-1]['close_price'])
                unrealized_return = (final_price - entry_price) / entry_price
                final_equity = current_capital * (1 + unrealized_return)
            elif current_position == -1 and entry_price > 0:
                final_price = float(ticker_signals.iloc[-1]['close_price'])
                unrealized_return = (entry_price - final_price) / entry_price
                final_equity = current_capital * (1 + unrealized_return)

            cumulative_return = (final_equity - starting_capital) / starting_capital
            
            if len(equity_curve) > 2:
                daily_returns = np.diff(equity_curve) / equity_curve[:-1]
                excess_returns = daily_returns - (0.02 / 252)
                std_dev = np.std(excess_returns)
                sharpe_ratio = (np.mean(excess_returns) / std_dev * np.sqrt(252)) if std_dev > 1e-6 else 0.0
                
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
            
            wins = sum(1 for value in trades_returns if value > 0)
            gross_profit = sum(value for value in trades_returns if value > 0)
            gross_loss = abs(sum(value for value in trades_returns if value < 0))
            profit_factor = (gross_profit / gross_loss) if gross_loss > 1e-9 else (gross_profit if gross_profit > 0 else 0.0)

            diagnostics[ticker] = {
                'signals': {
                    'BUY': int(signals_count.get('BUY', 0)),
                    'SELL': int(signals_count.get('SELL', 0)),
                    'HOLD': int(signals_count.get('HOLD', 0))
                },
                'trades_closed': len(trades_returns),
                'win_rate': round(float(wins / len(trades_returns)), 4) if trades_returns else 0.0,
                'avg_trade_return': round(float(np.mean(trades_returns)), 4) if trades_returns else 0.0,
                'profit_factor': round(float(profit_factor), 4),
                'time_in_market_ratio': round(float((signals_count.get('BUY', 0) + signals_count.get('SELL', 0)) / max(len(ticker_signals), 1)), 4)
            }

        return metrics, diagnostics
    except Exception as e:
        logger.error(f"Error in math: {e}")
        raise

def get_pipeline_health(connection, report_date):
    cursor = connection.cursor()
    cursor.execute("SELECT tickers_processed, status FROM batch_log WHERE batch_date = %s LIMIT 1", (report_date,))
    batch_row = cursor.fetchone()

    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM technical_indicators WHERE batch_date = %s", (report_date,))
    indicator_tickers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM trading_signals WHERE batch_date = %s", (report_date,))
    signal_tickers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sentiment_scores WHERE batch_date = %s", (report_date,))
    headlines = cursor.fetchone()[0]

    cursor.execute("SELECT stage, metrics FROM pipeline_kpis WHERE batch_date = %s", (report_date,))
    stage_metrics = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()

    tickers_expected = int(batch_row[0]) if batch_row and batch_row[0] is not None else 0
    return {
        'batch_status': batch_row[1] if batch_row else 'UNKNOWN',
        'tickers_expected': tickers_expected,
        'tickers_with_indicators': int(indicator_tickers or 0),
        'tickers_with_signals': int(signal_tickers or 0),
        'headlines_scored': int(headlines or 0),
        'coverage_ratio': round(float((signal_tickers or 0) / tickers_expected), 4) if tickers_expected else 0.0,
        'stage_kpis': stage_metrics
    }

def get_explanations_sample(connection, report_date, limit=10):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT e.ticker, ts.signal, ts.prob_up, ts.prob_down,
               e.sentiment_state, e.rsi_state, e.trend_state, e.volatility_state
        FROM signal_explanations e
        JOIN trading_signals ts
          ON ts.batch_date = e.batch_date AND ts.ticker = e.ticker
        WHERE e.batch_date = %s
        ORDER BY ts.prob_up DESC
        LIMIT %s
    """, (report_date, limit))
    rows = cursor.fetchall()
    cursor.close()
    return [
        {
            'ticker': row[0], 'signal': row[1],
            'prob_up': round(float(row[2]), 4) if row[2] is not None else None,
            'prob_down': round(float(row[3]), 4) if row[3] is not None else None,
            'evidence': {'sentiment': row[4], 'rsi': row[5], 'trend': row[6], 'volatility': row[7]}
        } for row in rows
    ]

def compute_benchmark(signals_df):
    benchmark = {}
    for ticker in signals_df['ticker'].unique():
        ticker_df = signals_df[signals_df['ticker'] == ticker].sort_values('batch_date')
        if ticker_df.empty: continue
        first_price = float(ticker_df.iloc[0]['close_price']) if ticker_df.iloc[0]['close_price'] else 0.0
        last_price = float(ticker_df.iloc[-1]['close_price']) if ticker_df.iloc[-1]['close_price'] else 0.0
        buy_hold_return = ((last_price - first_price) / first_price) if first_price > 0 else 0.0
        benchmark[ticker] = round(float(buy_hold_return), 4)
    return benchmark

def upsert_pipeline_kpi(connection, batch_date, stage, metrics):
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO pipeline_kpis (batch_date, stage, metrics)
        VALUES (%s, %s, %s::jsonb)
        ON CONFLICT (batch_date, stage) DO UPDATE
        SET metrics = EXCLUDED.metrics, updated_at = CURRENT_TIMESTAMP
    """, (batch_date, stage, json.dumps(metrics)))
    connection.commit()
    cursor.close()

def save_report_to_s3(report_data, report_date):
    try:
        key = f"results/{report_date}/report.json"
        s3_client.put_object(Bucket='tfm-unir-datalake', Key=key, Body=json.dumps(report_data, indent=2, default=str))
        return key
    except Exception:
        raise

def handler(event, context):
    try:
        logger.info("Lambda report generation started (Long/Short Strategy)")
        aurora_creds = get_secret('aurora/credentials')
        connection = connect_to_aurora(aurora_creds)
        today = resolve_batch_date(event)

        signals_df = get_trading_data(connection, today, days_back=DAYS_BACK)
        backtest_metrics, diagnostics = calculate_backtesting_metrics(signals_df) if not signals_df.empty else ({}, {})
        pipeline_health = get_pipeline_health(connection, today)
        explanations = get_explanations_sample(connection, today, limit=10)
        benchmark = compute_benchmark(signals_df) if not signals_df.empty else {}

        report_data = {
            'report_date': today,
            'data_period_days': DAYS_BACK,
            'generated_at': datetime.now().isoformat(),
            'pipeline_health': pipeline_health,
            'signal_diagnostics': diagnostics,
            'benchmark_comparison': {
                ticker: {
                    'strategy_cumulative_return': backtest_metrics[ticker]['cumulative_return'],
                    'buy_hold_cumulative_return': benchmark.get(ticker, 0.0),
                    'alpha_vs_benchmark': round(backtest_metrics[ticker]['cumulative_return'] - benchmark.get(ticker, 0.0), 4)
                } for ticker in backtest_metrics
            },
            'top_signal_explanations': explanations,
            'backtesting_metrics': backtest_metrics,
            'summary': {
                'total_tickers': len(backtest_metrics),
                'avg_cumulative_return': round(np.mean([m['cumulative_return'] for m in backtest_metrics.values()]), 4) if backtest_metrics else 0,
                'avg_sharpe_ratio': round(np.mean([m['sharpe_ratio'] for m in backtest_metrics.values()]), 4) if backtest_metrics else 0,
                'avg_max_drawdown': round(np.mean([m['max_drawdown'] for m in backtest_metrics.values()]), 4) if backtest_metrics else 0,
                'total_closed_trades': int(sum(item['trades_closed'] for item in diagnostics.values())) if diagnostics else 0
            },
            'backtesting_config': {
                'initial_capital':   10000.0,
                'risk_free_rate':    0.02,
                'period_days':       DAYS_BACK,
                'strategy_type':     'Long/Short',
                'sharpe_annualized': True,
                'limitation': 'El backtesting usa precios de cierre del dia de la senal. No considera slippage ni comisiones.'
            },
            'trace_artifact': f'results/{today}/bayesian_trace.json',
        }
        report_key = save_report_to_s3(report_data, today)
        upsert_pipeline_kpi(connection, today, 'report', {
            'tickers_reported': len(backtest_metrics),
            'total_closed_trades': int(sum(item['trades_closed'] for item in diagnostics.values())) if diagnostics else 0
        })

        cursor = connection.cursor()
        cursor.execute("UPDATE batch_log SET status = %s WHERE batch_date = %s", ('COMPLETED', today))
        connection.commit()
        cursor.close()
        connection.close()

        return {'statusCode': 200, 'body': json.dumps({'message': 'Success', 'report': report_key})}
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
