import json
import boto3
import psycopg2
from datetime import datetime
import logging
import numpy as np
from pgmpy.models import BayesianNetwork
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
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


def discretize_sentiment(sentiment):
    """Convert sentiment string to discrete value"""
    if sentiment == 'bullish':
        return 'bullish'
    elif sentiment == 'bearish':
        return 'bearish'
    else:
        return 'neutral'


def discretize_rsi(rsi_value):
    """Discretize RSI into oversold, neutral, or overbought"""
    if rsi_value < 30:
        return 'oversold'
    elif rsi_value > 70:
        return 'overbought'
    else:
        return 'neutral'


def discretize_trend(sma_20, sma_50):
    """Discretize trend based on SMA crossover"""
    if sma_20 > sma_50:
        return 'uptrend'
    else:
        return 'downtrend'


def discretize_volatility(bb_upper, bb_lower, close_price):
    """Discretize volatility based on Bollinger Band width"""
    if pd.isna(bb_upper) or pd.isna(bb_lower):
        return 'low'

    band_width = bb_upper - bb_lower
    # Normalize by close price to get relative width
    relative_width = band_width / close_price if close_price > 0 else 0

    # Threshold for high volatility (e.g., > 0.05 or 5% of price)
    if relative_width > 0.05:
        return 'high'
    else:
        return 'low'


def create_bayesian_network():
    """Create and define the Bayesian Network structure"""
    try:
        # Create the DAG
        model = BayesianNetwork([
            ('Sentiment', 'MarketDirection'),
            ('RSI', 'MarketDirection'),
            ('Trend', 'MarketDirection'),
            ('Volatility', 'MarketDirection')
        ])

        # Define CPD for Sentiment node
        cpd_sentiment = TabularCPD(
            variable='Sentiment',
            variable_card=3,
            values=[[0.3], [0.3], [0.4]],
            evidence=None
        )

        # Define CPD for RSI node
        cpd_rsi = TabularCPD(
            variable='RSI',
            variable_card=3,
            values=[[0.2], [0.6], [0.2]],
            evidence=None
        )

        # Define CPD for Trend node
        cpd_trend = TabularCPD(
            variable='Trend',
            variable_card=2,
            values=[[0.5], [0.5]],
            evidence=None
        )

        # Define CPD for Volatility node
        cpd_volatility = TabularCPD(
            variable='Volatility',
            variable_card=2,
            values=[[0.6], [0.4]],
            evidence=None
        )

        # Define CPD for MarketDirection - given all parent nodes
        # MarketDirection has 2 states: 'up', 'down'
        # Parents: Sentiment (3 states), RSI (3 states), Trend (2 states), Volatility (2 states)
        # Total combinations: 3 * 3 * 2 * 2 = 36

        cpd_direction = TabularCPD(
            variable='MarketDirection',
            variable_card=2,
            values=[
                # P(MarketDirection='up') for all combinations
                [
                    # Sentiment=bullish
                    0.85, 0.75, 0.70,  # RSI=oversold, neutral, overbought | Trend=uptrend, Volatility=low
                    0.80, 0.70, 0.65,  # RSI=oversold, neutral, overbought | Trend=uptrend, Volatility=high
                    0.70, 0.60, 0.55,  # RSI=oversold, neutral, overbought | Trend=downtrend, Volatility=low
                    0.65, 0.55, 0.50,  # RSI=oversold, neutral, overbought | Trend=downtrend, Volatility=high
                    # Sentiment=bearish
                    0.30, 0.25, 0.20,  # RSI=oversold, neutral, overbought | Trend=uptrend, Volatility=low
                    0.25, 0.20, 0.15,  # RSI=oversold, neutral, overbought | Trend=uptrend, Volatility=high
                    0.20, 0.15, 0.10,  # RSI=oversold, neutral, overbought | Trend=downtrend, Volatility=low
                    0.15, 0.10, 0.05,  # RSI=oversold, neutral, overbought | Trend=downtrend, Volatility=high
                    # Sentiment=neutral
                    0.55, 0.50, 0.45,  # RSI=oversold, neutral, overbought | Trend=uptrend, Volatility=low
                    0.50, 0.45, 0.40,  # RSI=oversold, neutral, overbought | Trend=uptrend, Volatility=high
                    0.45, 0.40, 0.35,  # RSI=oversold, neutral, overbought | Trend=downtrend, Volatility=low
                    0.40, 0.35, 0.30,  # RSI=oversold, neutral, overbought | Trend=downtrend, Volatility=high
                ]
            ],
            evidence=['Sentiment', 'RSI', 'Trend', 'Volatility'],
            evidence_card=[3, 3, 2, 2]
        )

        # Add CPDs to model
        model.add_cpds(cpd_sentiment, cpd_rsi, cpd_trend, cpd_volatility, cpd_direction)

        # Check if CPDs are valid
        if not model.check_model():
            logger.error("Bayesian Network model is invalid")
            raise ValueError("Invalid Bayesian Network")

        logger.info("Bayesian Network created successfully")
        return model

    except Exception as e:
        logger.error(f"Error creating Bayesian Network: {str(e)}")
        raise


def get_ticker_data(connection, batch_date, ticker):
    """Retrieve sentiment and technical indicator data for a ticker"""
    try:
        cursor = connection.cursor()

        # Get latest sentiment
        sentiment_query = """
            SELECT sentiment, confidence
            FROM sentiment_scores
            WHERE batch_date = %s AND ticker = %s
            ORDER BY batch_date DESC
            LIMIT 1
        """
        cursor.execute(sentiment_query, (batch_date, ticker))
        sentiment_result = cursor.fetchone()

        # Get latest technical indicators
        indicators_query = """
            SELECT rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower
            FROM technical_indicators
            WHERE batch_date = %s AND ticker = %s
            ORDER BY batch_date DESC
            LIMIT 1
        """
        cursor.execute(indicators_query, (batch_date, ticker))
        indicators_result = cursor.fetchone()

        cursor.close()

        return sentiment_result, indicators_result

    except Exception as e:
        logger.error(f"Error retrieving data for {ticker}: {str(e)}")
        return None, None


def infer_signal(model, sentiment, rsi, trend, volatility):
    """Use Variable Elimination to infer market direction"""
    try:
        infer = VariableElimination(model)

        # Map discrete values
        sentiment_state = discretize_sentiment(sentiment) if isinstance(sentiment, str) else sentiment
        rsi_state = discretize_rsi(rsi) if isinstance(rsi, (int, float)) else rsi
        trend_state = discretize_trend(trend[0], trend[1]) if isinstance(trend, tuple) else trend
        volatility_state = discretize_volatility(volatility[0], volatility[1], volatility[2]) if isinstance(volatility, tuple) else volatility

        # Query the network
        result = infer.query(
            variables=['MarketDirection'],
            evidence={
                'Sentiment': sentiment_state,
                'RSI': rsi_state,
                'Trend': trend_state,
                'Volatility': volatility_state
            }
        )

        # Extract probabilities
        prob_up = float(result.values[1])  # Index 1 for 'up'
        prob_down = float(result.values[0])  # Index 0 for 'down'

        # Determine signal
        if prob_up > 0.65:
            signal = 'BUY'
        elif prob_up < 0.35:
            signal = 'SELL'
        else:
            signal = 'HOLD'

        return signal, prob_up, prob_down

    except Exception as e:
        logger.error(f"Error in inference: {str(e)}")
        raise


def insert_trading_signal(connection, batch_date, ticker, signal, prob_up, prob_down):
    """Insert trading signal into Aurora PostgreSQL"""
    try:
        cursor = connection.cursor()

        query = """
            INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down)
            VALUES (%s, %s, %s, %s, %s)
        """

        cursor.execute(query, (batch_date, ticker, signal, float(prob_up), float(prob_down)))
        connection.commit()
        cursor.close()

    except Exception as e:
        logger.error(f"Error inserting trading signal: {str(e)}")
        raise


def handler(event, context):
    """Main Lambda handler"""
    try:
        logger.info("Lambda Bayesian inference started")

        # Create Bayesian Network
        model = create_bayesian_network()

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

        # Get all tickers from sentiment_scores table
        cursor = connection.cursor()
        cursor.execute(
            "SELECT DISTINCT ticker FROM sentiment_scores WHERE batch_date = %s",
            (today,)
        )
        tickers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        logger.info(f"Processing {len(tickers)} tickers")

        signals_processed = 0

        # Process each ticker
        for ticker in tickers:
            try:
                # Get data for this ticker
                sentiment_result, indicators_result = get_ticker_data(connection, today, ticker)

                if sentiment_result is None or indicators_result is None:
                    logger.warning(f"Missing data for {ticker}")
                    continue

                sentiment, confidence = sentiment_result
                rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower = indicators_result

                # Discretize values for Bayesian inference
                sentiment_discrete = discretize_sentiment(sentiment)
                rsi_discrete = discretize_rsi(rsi_14)
                trend_discrete = discretize_trend(sma_20, sma_50)
                volatility_discrete = discretize_volatility(bb_upper, bb_lower, close_price)

                # Infer market direction
                signal, prob_up, prob_down = infer_signal(
                    model,
                    sentiment_discrete,
                    rsi_discrete,
                    trend_discrete,
                    volatility_discrete
                )

                # Insert signal into database
                insert_trading_signal(connection, today, ticker, signal, prob_up, prob_down)

                signals_processed += 1
                logger.info(f"Trading signal for {ticker}: {signal} (P(up)={prob_up:.3f})")

            except Exception as e:
                logger.error(f"Error processing {ticker}: {str(e)}")
                continue

        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Bayesian inference completed',
                'signals_processed': signals_processed
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
