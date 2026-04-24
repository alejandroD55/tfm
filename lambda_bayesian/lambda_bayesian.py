import sys
from unittest.mock import MagicMock

# --- EL TRUCO FANTASMA (AGUJERO NEGRO) ---
class MockImporter:
    def find_module(self, fullname, path=None):
        # Si pgmpy pide CUALQUIER sub-módulo de estas 3, lo interceptamos
        if fullname.startswith(('sklearn', 'statsmodels', 'patsy')):
            return self
        return None
        
    def load_module(self, fullname):
        # Le devolvemos un fantasma que finge ser una carpeta
        mock = MagicMock()
        mock.__path__ = []
        sys.modules[fullname] = mock
        return mock

sys.meta_path.insert(0, MockImporter())
# ----------------------------------------

import json
import boto3
import psycopg2
from datetime import datetime
import logging
import numpy as np

# Ahora pgmpy se creerá que tiene todo lo de Machine Learning instalado
from pgmpy.models import BayesianNetwork
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client('secretsmanager')

def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response: return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        logger.error(f"Error retrieving secret: {str(e)}")
        raise

def discretize_sentiment(sentiment):
    if sentiment == 'bullish': return 'bullish'
    elif sentiment == 'bearish': return 'bearish'
    else: return 'neutral'

def discretize_rsi(rsi_value):
    if rsi_value < 30: return 'oversold'
    elif rsi_value > 70: return 'overbought'
    else: return 'neutral'

def discretize_trend(sma_20, sma_50):
    if sma_20 > sma_50: return 'uptrend'
    else: return 'downtrend'

def discretize_volatility(bb_upper, bb_lower, close_price):
    if bb_upper is None or bb_lower is None or np.isnan(bb_upper) or np.isnan(bb_lower): return 'low'
    band_width = bb_upper - bb_lower
    relative_width = band_width / close_price if close_price > 0 else 0
    if relative_width > 0.05: return 'high'
    else: return 'low'

def create_bayesian_network():
    try:
        model = BayesianNetwork([
            ('Sentiment', 'MarketDirection'),
            ('RSI', 'MarketDirection'),
            ('Trend', 'MarketDirection'),
            ('Volatility', 'MarketDirection')
        ])

        cpd_sentiment = TabularCPD(variable='Sentiment', variable_card=3, values=[[0.3], [0.3], [0.4]], 
                                   state_names={'Sentiment': ['bullish', 'bearish', 'neutral']})
        cpd_rsi = TabularCPD(variable='RSI', variable_card=3, values=[[0.2], [0.6], [0.2]], 
                             state_names={'RSI': ['oversold', 'neutral', 'overbought']})
        cpd_trend = TabularCPD(variable='Trend', variable_card=2, values=[[0.5], [0.5]], 
                               state_names={'Trend': ['uptrend', 'downtrend']})
        cpd_volatility = TabularCPD(variable='Volatility', variable_card=2, values=[[0.6], [0.4]], 
                                    state_names={'Volatility': ['low', 'high']})

        cpd_direction = TabularCPD(
            variable='MarketDirection',
            variable_card=2,
            values=[
                [0.15, 0.25, 0.30, 0.20, 0.30, 0.35, 0.30, 0.40, 0.45, 0.35, 0.45, 0.50,
                 0.70, 0.75, 0.80, 0.75, 0.80, 0.85, 0.80, 0.85, 0.90, 0.85, 0.90, 0.95,
                 0.45, 0.50, 0.55, 0.50, 0.55, 0.60, 0.55, 0.60, 0.65, 0.60, 0.65, 0.70],
                [0.85, 0.75, 0.70, 0.80, 0.70, 0.65, 0.70, 0.60, 0.55, 0.65, 0.55, 0.50,
                 0.30, 0.25, 0.20, 0.25, 0.20, 0.15, 0.20, 0.15, 0.10, 0.15, 0.10, 0.05,
                 0.55, 0.50, 0.45, 0.50, 0.45, 0.40, 0.45, 0.40, 0.35, 0.40, 0.35, 0.30]
            ],
            evidence=['Sentiment', 'RSI', 'Trend', 'Volatility'],
            evidence_card=[3, 3, 2, 2],
            state_names={
                'MarketDirection': ['down', 'up'],
                'Sentiment': ['bullish', 'bearish', 'neutral'],
                'RSI': ['oversold', 'neutral', 'overbought'],
                'Trend': ['uptrend', 'downtrend'],
                'Volatility': ['low', 'high']
            }
        )

        model.add_cpds(cpd_sentiment, cpd_rsi, cpd_trend, cpd_volatility, cpd_direction)
        if not model.check_model(): raise ValueError("Invalid Bayesian Network")
        return model
    except Exception as e:
        logger.error(f"Error creating BN: {str(e)}")
        raise

def get_ticker_data(connection, batch_date, ticker):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT sentiment, confidence FROM sentiment_scores WHERE batch_date = %s AND ticker = %s ORDER BY batch_date DESC LIMIT 1", (batch_date, ticker))
        sentiment_result = cursor.fetchone()
        
        cursor.execute("SELECT rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower FROM technical_indicators WHERE batch_date = %s AND ticker = %s ORDER BY batch_date DESC LIMIT 1", (batch_date, ticker))
        indicators_result = cursor.fetchone()
        cursor.close()
        return sentiment_result, indicators_result
    except Exception as e:
        return None, None

def infer_signal(model, sentiment, rsi, trend, volatility):
    try:
        infer = VariableElimination(model)
        result = infer.query(
            variables=['MarketDirection'],
            evidence={
                'Sentiment': discretize_sentiment(sentiment),
                'RSI': discretize_rsi(rsi),
                'Trend': discretize_trend(trend[0], trend[1]),
                'Volatility': discretize_volatility(volatility[0], volatility[1], volatility[2])
            }
        )
        prob_up = float(result.values[1])
        prob_down = float(result.values[0])

        if prob_up > 0.65: signal = 'BUY'
        elif prob_up < 0.35: signal = 'SELL'
        else: signal = 'HOLD'
        return signal, prob_up, prob_down
    except Exception as e:
        raise

def handler(event, context):
    try:
        model = create_bayesian_network()
        aurora_creds = get_secret('aurora/credentials')
        connection = psycopg2.connect(**aurora_creds)
        today = datetime.now().strftime('%Y-%m-%d')

        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM sentiment_scores WHERE batch_date = %s", (today,))
        tickers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        signals_processed = 0
        for ticker in tickers:
            try:
                sentiment_result, indicators_result = get_ticker_data(connection, today, ticker)
                if not sentiment_result or not indicators_result: continue

                sentiment, confidence = sentiment_result
                rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower = indicators_result

                signal, prob_up, prob_down = infer_signal(model, sentiment, rsi_14, (sma_20, sma_50), (bb_upper, bb_lower, close_price))
                
                cursor = connection.cursor()
                cursor.execute("INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (batch_date, ticker) DO NOTHING", 
                               (today, ticker, signal, float(prob_up), float(prob_down)))
                connection.commit()
                cursor.close()
                signals_processed += 1
            except Exception as e:
                continue

        connection.close()
        return {'statusCode': 200, 'body': json.dumps({'message': 'Success', 'signals': signals_processed})}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
