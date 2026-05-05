import yfinance as yf
import requests
import pandas as pd
import pandas_ta as ta
import psycopg2
from datetime import datetime, timedelta
import time
import numpy as np
import logging
from tqdm import tqdm 
from huggingface_hub import InferenceClient

# Configuramos el logger que daba error
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# El truco para la nueva versión de pgmpy en tu ordenador local:
from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination

import os
from dotenv import load_dotenv

# Carga las variables de tu archivo .env secreto
load_dotenv() 

# =================================================================
# 1. CONFIGURACIÓN (Obtenemos credenciales de forma segura)
# =================================================================
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

TICKERS = ['SPY', 'QQQ', 'SMH', 'XLE', 'XLP', 'XLF', 'XLV', 'IWM', 'TLT', 'GLD']
DAYS_BACK = 90
MODEL_ID = "ProsusAI/finbert"

# Inicializamos clientes
hf_client = InferenceClient(token=HUGGINGFACE_API_KEY)

# funciones de discretización y creación de red bayesiana
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
        logger.error(f"Inference math error: {e}")
        raise

# =================================================================
# 2. CONEXIÓN A BASE DE DATOS
# =================================================================
def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME)

# =================================================================
# 3. LÓGICA PRINCIPAL DEL BACKFILL
# =================================================================
def run_bootstrap():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_BACK)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    model = create_bayesian_network() # Tu función que crea el DAG
    
    print(f"🚀 Iniciando viaje en el tiempo: desde {start_date.date()} hasta {end_date.date()}")
    
    for ticker in TICKERS:
        print(f"\n--- Procesando {ticker} ---")
        
        # 3.1: Descargar OHLCV
        hist_start = start_date - timedelta(days=60) 
        df = yf.download(ticker, start=hist_start, end=end_date, progress=False)
        
        if df.empty:
            continue
            
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
            
        # 3.2 Calcular indicadores técnicos
        df['rsi_14'] = ta.rsi(df['Close'], length=14)
        df['sma_20'] = ta.sma(df['Close'], length=20)
        df['sma_50'] = ta.sma(df['Close'], length=50)
        bbands = ta.bbands(df['Close'], length=20, std=2)
        if bbands is not None and not bbands.empty:
            df['bb_lower'] = bbands.iloc[:, 0]
            df['bb_upper'] = bbands.iloc[:, 2]
        else:
            df['bb_lower'] = None
            df['bb_upper'] = None
            
        # Filtramos solo los últimos 90 días
        df = df[df.index >= pd.to_datetime(start_date)]
        
        # 3.3 Iteramos día a día
        for date, row in tqdm(df.iterrows(), total=len(df), desc=f"Simulando días para {ticker}"):
            current_date_str = date.strftime('%Y-%m-%d')
            
            # --- 1. GUARDAR INDICADORES ---
            try:
                # Quitamos el ON CONFLICT DO NOTHING genérico que a veces falla en Aurora Serverless
                # Y en su lugar usamos la cláusula exacta: ON CONFLICT (batch_date, ticker)
                cursor.execute("""
                    INSERT INTO technical_indicators (batch_date, ticker, close_price, rsi_14, sma_20, sma_50, bb_upper, bb_lower)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (batch_date, ticker) DO NOTHING
                """, (current_date_str, ticker, float(row['Close']), float(row['rsi_14']) if pd.notna(row['rsi_14']) else None,
                      float(row['sma_20']) if pd.notna(row['sma_20']) else None, float(row['sma_50']) if pd.notna(row['sma_50']) else None,
                      float(row['bb_upper']) if pd.notna(row['bb_upper']) else None, float(row['bb_lower']) if pd.notna(row['bb_lower']) else None))
                conn.commit()
            except Exception as e:
                conn.rollback() # <--- LA MAGIA ESTÁ AQUÍ
                logger.error(f"DB Error (Indicators) on {current_date_str}: {e}")

            # --- 2. DESCARGAR Y GUARDAR NOTICIAS ---
            url = 'https://finnhub.io/api/v1/company-news'
            params = {'symbol': ticker, 'from': current_date_str, 'to': current_date_str, 'token': FINNHUB_API_KEY}
            try:
                news_resp = requests.get(url, params=params).json()
            except:
                news_resp = []
                
            daily_sentiment = 'neutral' 
            
            if news_resp and isinstance(news_resp, list):
                top_article = news_resp[0].get('headline', '')
                if top_article:
                    try:
                        res = hf_client.text_classification(top_article, model=MODEL_ID)
                        label = max(res, key=lambda x: x.score).label.lower() if hasattr(res[0], 'score') else max(res, key=lambda x: x.get('score', 0)).get('label', '').lower()
                        sentiment_map = {'positive': 'bullish', 'negative': 'bearish', 'neutral': 'neutral'}
                        daily_sentiment = sentiment_map.get(label, 'neutral')
                        
                        try:
                            cursor.execute("""
                                INSERT INTO sentiment_scores (batch_date, ticker, headline, sentiment)
                                VALUES (%s, %s, %s, %s) 
                                ON CONFLICT (batch_date, ticker, headline) DO NOTHING
                            """, (current_date_str, ticker, top_article, daily_sentiment))
                            conn.commit()
                        except Exception as e:
                            conn.rollback() # <--- LA MAGIA ESTÁ AQUÍ
                            
                        time.sleep(0.5) 
                    except Exception as e:
                        pass 
                        
            # --- 3. INFERENCIA BAYESIANA ---
            if pd.notna(row['rsi_14']) and pd.notna(row['sma_20']) and pd.notna(row['bb_upper']):
                try:
                    signal, prob_up, prob_down = infer_signal(model, daily_sentiment, row['rsi_14'], 
                                                              (row['sma_20'], row['sma_50']), 
                                                              (row['bb_upper'], row['bb_lower'], row['Close']))
                    
                    cursor.execute("""
                        INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down)
                        VALUES (%s, %s, %s, %s, %s) 
                        ON CONFLICT (batch_date, ticker) DO UPDATE SET
                            signal = EXCLUDED.signal,
                            prob_up = EXCLUDED.prob_up,
                            prob_down = EXCLUDED.prob_down
                    """, (current_date_str, ticker, signal, float(prob_up), float(prob_down)))
                    conn.commit()
                except Exception as e:
                    conn.rollback() # <--- LA MAGIA ESTÁ AQUÍ
                    logger.error(f"DB Error (Inference) on {current_date_str}: {e}")
                
        time.sleep(2)

    conn.close()
    print("✅ BACKFILL COMPLETADO. ¡Tu base de datos ahora tiene historia!")

if __name__ == "__main__":
    run_bootstrap()