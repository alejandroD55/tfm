#!/usr/bin/env python3
"""
local_backtest_runner.py — Pipeline TFM local: 365 días de backtesting
=======================================================================
Produce EXACTAMENTE los mismos documentos MongoDB y filas Aurora que el
pipeline de producción (Lambdas + Step Functions), de modo que el
dashboard Angular funcione sin cambios sobre los datos históricos.

Colecciones MongoDB escritas (misma estructura que las Lambdas):
  raw_news          — artículos Finnhub por (batch_date, ticker)
  ohlcv             — datos OHLCV por (batch_date, ticker)
  news              — artículos con scoring FinBERT
  news_filtered     — titulares pre-procesados (sin Bedrock → titulares crudos)
  bayesian_reports  — traza por (batch_date, ticker)   [lambda_bayesian]
  bayesian_traces   — traza completa del día            [lambda_bayesian]
  macro_context     — MacroSentiment + RiskRegime       [lambda_macro_context]
  reports           — reporte diario con backtesting    [lambda_report]

Tablas Aurora escritas (mismo schema que database_schema.sql):
  batch_log, pipeline_kpis, technical_indicators, sentiment_scores,
  trading_signals, signal_explanations, macro_sentiment_scores,
  market_regime_state, signal_outcomes

Sustituciones respecto al pipeline de producción:
  AWS Secrets Manager  →  variables de entorno (.env)
  AWS Bedrock (Haiku)  →  Ollama local (opcional; sin él se usan titulares crudos)
  HuggingFace API      →  FinBERT local (transformers, sin token)
  Aurora remota        →  PostgreSQL local (docker-compose)
  Step Functions       →  loop Python

Requisitos previos:
  1. docker compose up postgres   # PostgreSQL local en :5432
  2. pip install -r requirements_local.txt
  3. cp .env.example .env  →  rellenar FINNHUB_API_KEY y MONGODB_URI
  4. (Opcional) ollama pull llama3.2:3b

Variables .env necesarias:
  FINNHUB_API_KEY        → noticias históricas por mes (OBLIGATORIO)
  MONGODB_URI            → conexión Atlas (igual que producción)
  POSTGRES_HOST/PORT/USER/PASSWORD/DB  → default docker-compose
  OLLAMA_BASE_URL / OLLAMA_MODEL       → default localhost:11434 / llama3.2:3b
"""

# =============================================================================
# 0. IMPORTS & CONFIGURACIÓN
# =============================================================================

import os
import sys
import json
import time
import logging
import warnings
import uuid
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import Counter

import requests
import pandas as pd
import numpy as np
import yfinance as yf
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

warnings.filterwarnings("ignore")

# ── Añadir shared/ al path para importar mongo_utils ─────────────────────────
_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_REPO_ROOT, "shared"))

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("backtest.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Variables de entorno ───────────────────────────────────────────────────────
load_dotenv()

TICKERS       = ["SPY", "IWM", "GLD"]
DAYS_BACK     = 365
INITIAL_CAP   = 10_000.0
RISK_FREE_RATE = 0.02

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
MONGODB_URI     = os.getenv("MONGODB_URI", "")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "user":     os.getenv("POSTGRES_USER",     "tfmadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "localpassword123"),
    "database": os.getenv("POSTGRES_DB",       "tfm"),
}

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL",    "llama3.2:3b")

CACHE_DIR = Path("cache")
(CACHE_DIR / "news").mkdir(parents=True, exist_ok=True)
(CACHE_DIR / "ohlcv").mkdir(parents=True, exist_ok=True)

# =============================================================================
# 1. MODEL CONFIG — idéntico a lambda_bayesian.py MODEL_CONFIG
# =============================================================================

MODEL_CONFIG = {
    "version": "1.1.0",
    "description": "Red bayesiana naive con Momentum: Sentiment, RSI, Trend, Volatility -> MarketDirection",
    "discretization": {
        "rsi": {
            "oversold_below": 30,
            "overbought_above": 70,
            "neutral_range": [30, 70],
            "rationale": "RSI < 30 sugiere sobrevendido; RSI > 70 evalúa momentum si hay tendencia",
        },
        "trend": {
            "rule": "SMA20 > SMA50 = uptrend",
            "rationale": "Golden cross simple: media corta por encima de media larga",
        },
        "volatility": {
            "high_if_band_width_ratio_above": 0.05,
            "formula": "(BB_upper - BB_lower) / close_price",
            "rationale": "Bandas que representan >5% del precio indican alta volatilidad",
        },
    },
    "signal_thresholds": {
        "BUY":  {"prob_up_above": 0.58, "rationale": "Confianza alcista moderada-alta"},
        "SELL": {"prob_up_below": 0.42, "rationale": "Confianza bajista moderada-alta / Salir a Cash"},
        "HOLD": {"range": [0.42, 0.58], "rationale": "Zona de incertidumbre — mantener posición"},
    },
    "priors": {
        "Sentiment": {"bullish": 0.30, "bearish": 0.30, "neutral": 0.40,
                      "rationale": "Prior levemente favorable a neutral en mercados eficientes"},
        "RSI": {"oversold": 0.20, "neutral": 0.60, "overbought": 0.20,
                "rationale": "La mayoria del tiempo el RSI esta en zona neutral"},
        "Trend": {"uptrend": 0.50, "downtrend": 0.50,
                  "rationale": "Prior uniforme: no hay sesgo a priori sobre la tendencia"},
        "Volatility": {"low": 0.60, "high": 0.40,
                       "rationale": "Los mercados suelen tener baja volatilidad mas frecuentemente"},
    },
    "cpt_market_direction": {
        "variable": "MarketDirection", "states": ["down", "up"],
        "evidence_order": ["Sentiment", "RSI", "Trend", "Volatility"],
        "rationale": {
            "momentum_logic": "RSI sobrecomprado + Tendencia alcista = Fuerte Momentum comprador",
            "bearish+overbought+downtrend+high": "Maxima confluencia bajista -> P(up)=0.05",
        },
        "values_P_down": [0.15,0.25,0.30,0.20,0.30,0.35,0.30,0.40,0.10,0.15,0.45,0.50,
                          0.70,0.75,0.80,0.75,0.80,0.85,0.80,0.85,0.50,0.55,0.90,0.95,
                          0.45,0.50,0.55,0.50,0.55,0.60,0.55,0.60,0.25,0.30,0.65,0.70],
        "values_P_up":   [0.85,0.75,0.70,0.80,0.70,0.65,0.70,0.60,0.90,0.85,0.55,0.50,
                          0.30,0.25,0.20,0.25,0.20,0.15,0.20,0.15,0.50,0.45,0.10,0.05,
                          0.55,0.50,0.45,0.50,0.45,0.40,0.45,0.40,0.75,0.70,0.35,0.30],
    },
    "known_limitations": [
        "El confidence score de FinBERT no entra en la inferencia (solo se guarda)",
        "Se usa voto mayoritario de los titulares",
        "Estrategia Momentum ajustada para capturar subidas fuertes en sobrecompra",
    ],
}

BUY_THRESHOLD  = MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"]
SELL_THRESHOLD = MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"]

# =============================================================================
# 2. FINBERT LOCAL
# =============================================================================

_finbert_pipeline = None
MIN_HEADLINE_LEN  = 20
MIN_CONFIDENCE    = 0.55


def get_finbert():
    global _finbert_pipeline
    if _finbert_pipeline is None:
        logger.info("Cargando FinBERT local (primera carga ~30 s)…")
        from transformers import pipeline as hf_pipeline
        _finbert_pipeline = hf_pipeline(
            "text-classification", model="ProsusAI/finbert",
            top_k=None, truncation=True, max_length=512,
        )
        logger.info("FinBERT listo.")
    return _finbert_pipeline


def analyze_sentiment_local(headline: str) -> Optional[Dict]:
    if not headline or len(headline.strip()) < MIN_HEADLINE_LEN:
        return None
    try:
        results = get_finbert()(headline)[0]
        top = max(results, key=lambda x: x["score"])
        label, score = top["label"].lower(), float(top["score"])
        if score < MIN_CONFIDENCE:
            return None
        lmap = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
        return {
            "sentiment":     lmap.get(label, "neutral"),
            "confidence":    round(score, 4),
            "justification": f"FinBERT local: {label} ({score*100:.1f}%)",
        }
    except Exception as exc:
        logger.debug(f"FinBERT error: {exc}")
        return None

# =============================================================================
# 3. OLLAMA (reemplaza AWS Bedrock)
# =============================================================================

def _ollama_available() -> bool:
    try:
        return requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=3).status_code == 200
    except Exception:
        return False


def summarize_with_ollama(ticker: str, headline: str) -> str:
    prompt = (
        f"Ticker: {ticker}\nHeadline: {headline}\n\n"
        "In 1-2 sentences, summarize this financial headline objectively, "
        "preserving its original tone. Reply with ONLY the summary."
    )
    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("response", headline).strip() or headline
    except Exception:
        pass
    return headline

# =============================================================================
# 4. MONGODB  (reutiliza mongo_utils.py de shared/)
# =============================================================================

try:
    from mongo_utils import (
        upsert_raw_news       as _mg_upsert_raw_news,
        upsert_ohlcv_bulk     as _mg_upsert_ohlcv,
        upsert_news           as _mg_upsert_news,
        upsert_filtered_news  as _mg_upsert_filtered,
        upsert_bayesian_report as _mg_upsert_br,
        upsert_bayesian_trace  as _mg_upsert_bt,
        upsert_macro_context   as _mg_upsert_macro,
        upsert_report          as _mg_upsert_report,
    )
    MONGO_OK = True
    logger.info("mongo_utils importado correctamente desde shared/")
except ImportError as _e:
    MONGO_OK = False
    logger.warning(f"mongo_utils no disponible ({_e}). Los datos solo se escribirán en Aurora.")
    def _mg_noop(*a, **kw): pass
    _mg_upsert_raw_news = _mg_upsert_ohlcv = _mg_upsert_news = _mg_upsert_filtered = _mg_noop
    _mg_upsert_br = _mg_upsert_bt = _mg_upsert_macro = _mg_upsert_report = _mg_noop

# =============================================================================
# 5. AURORA POSTGRES — TODAS LAS TABLAS
# =============================================================================

def _sf(v) -> Optional[float]:
    """safe_float: None si NaN o None, float en otro caso."""
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except Exception:
        return None


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_connection(conn):
    try:
        with conn.cursor() as c:
            c.execute("SELECT 1")
        return conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        return get_db_connection()


def pg_upsert_batch_log(conn, date_str: str, run_id: str, tickers: List[str],
                         status: str = "STARTED"):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO batch_log
                (batch_date, run_id, trigger_type, execution_name, requested_tickers,
                 status, tickers_processed)
            VALUES (%s,%s,'scheduled',%s,%s::jsonb,%s,%s)
            ON CONFLICT (run_id) DO UPDATE SET
                status             = EXCLUDED.status,
                tickers_processed  = EXCLUDED.tickers_processed,
                updated_at         = CURRENT_TIMESTAMP
            """,
            (date_str, run_id, f"backtest-{date_str}",
             json.dumps(tickers), status, len(tickers)),
        )
    conn.commit()


def pg_upsert_kpi(conn, date_str: str, run_id: str, stage: str, metrics: Dict):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO pipeline_kpis
                (batch_date, run_id, trigger_type, stage, metrics)
            VALUES (%s,%s,'scheduled',%s,%s::jsonb)
            ON CONFLICT (run_id, stage) DO UPDATE SET
                metrics = EXCLUDED.metrics, updated_at = CURRENT_TIMESTAMP
            """,
            (date_str, run_id, stage, json.dumps(metrics)),
        )
    conn.commit()


def pg_upsert_indicators(conn, date_str: str, ticker: str, ind: Dict):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO technical_indicators
                (batch_date, ticker, close_price, rsi_14, sma_20, sma_50,
                 bb_upper, bb_middle, bb_lower)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                close_price=EXCLUDED.close_price, rsi_14=EXCLUDED.rsi_14,
                sma_20=EXCLUDED.sma_20,          sma_50=EXCLUDED.sma_50,
                bb_upper=EXCLUDED.bb_upper,       bb_lower=EXCLUDED.bb_lower,
                bb_middle=EXCLUDED.bb_middle
            """,
            (date_str, ticker,
             _sf(ind.get("close")),    _sf(ind.get("rsi_14")),
             _sf(ind.get("sma_20")),   _sf(ind.get("sma_50")),
             _sf(ind.get("bb_upper")), _sf(ind.get("bb_middle")),
             _sf(ind.get("bb_lower"))),
        )
    conn.commit()


def pg_upsert_sentiment(conn, date_str: str, ticker: str,
                         headline: str, sdata: Dict):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO sentiment_scores
                (batch_date, ticker, headline, sentiment, confidence, justification)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker, headline) DO NOTHING
            """,
            (date_str, ticker, headline[:500],
             sdata["sentiment"], sdata["confidence"],
             sdata.get("justification", "")),
        )
    conn.commit()


def pg_upsert_signal(conn, date_str: str, ticker: str,
                      signal: str, prob_up: float, prob_down: float):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                signal=EXCLUDED.signal, prob_up=EXCLUDED.prob_up, prob_down=EXCLUDED.prob_down
            """,
            (date_str, ticker, signal, float(prob_up), float(prob_down)),
        )
    conn.commit()


def pg_upsert_signal_explanation(conn, date_str: str, ticker: str, states: Dict):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO signal_explanations
                (batch_date, ticker, sentiment_state, rsi_state, trend_state, volatility_state)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                sentiment_state=EXCLUDED.sentiment_state, rsi_state=EXCLUDED.rsi_state,
                trend_state=EXCLUDED.trend_state, volatility_state=EXCLUDED.volatility_state
            """,
            (date_str, ticker,
             states["Sentiment"], states["RSI"], states["Trend"], states["Volatility"]),
        )
    conn.commit()


def pg_upsert_macro_scores(conn, date_str: str, run_id: str,
                            sentiment: str, score: float, n_articles: int):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO macro_sentiment_scores
                (batch_date, run_id, macro_sentiment, score, n_articles)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date) DO UPDATE SET
                macro_sentiment=EXCLUDED.macro_sentiment, score=EXCLUDED.score,
                n_articles=EXCLUDED.n_articles, updated_at=CURRENT_TIMESTAMP
            """,
            (date_str, run_id, sentiment, score, n_articles),
        )
    conn.commit()


def pg_upsert_market_regime(conn, date_str: str, run_id: str,
                             regime: str, adj: float, vix: Optional[float]):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO market_regime_state
                (batch_date, run_id, risk_regime, macro_adjustment, vix)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date) DO UPDATE SET
                risk_regime=EXCLUDED.risk_regime, macro_adjustment=EXCLUDED.macro_adjustment,
                vix=EXCLUDED.vix, updated_at=CURRENT_TIMESTAMP
            """,
            (date_str, run_id, regime, adj, vix),
        )
    conn.commit()


def pg_upsert_signal_outcome(conn, date_str: str, ticker: str, run_id: str,
                              signal: str, prob_up: float, prob_down: float,
                              states: Dict, macro_ctx: Dict, price_d0: Optional[float]):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO signal_outcomes
                (batch_date, ticker, run_id, signal, prob_up, prob_down,
                 sentiment_state, rsi_state, trend_state, volatility_state,
                 macro_sentiment, risk_regime, macro_adjustment, price_d0)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                signal=EXCLUDED.signal, prob_up=EXCLUDED.prob_up,
                prob_down=EXCLUDED.prob_down,
                sentiment_state=EXCLUDED.sentiment_state, rsi_state=EXCLUDED.rsi_state,
                trend_state=EXCLUDED.trend_state, volatility_state=EXCLUDED.volatility_state,
                macro_sentiment=EXCLUDED.macro_sentiment, risk_regime=EXCLUDED.risk_regime,
                macro_adjustment=EXCLUDED.macro_adjustment, price_d0=EXCLUDED.price_d0,
                updated_at=CURRENT_TIMESTAMP
            """,
            (date_str, ticker, run_id, signal, float(prob_up), float(prob_down),
             states["Sentiment"], states["RSI"], states["Trend"], states["Volatility"],
             macro_ctx.get("macro_sentiment", "neutral"),
             macro_ctx.get("risk_regime", "NEUTRAL"),
             float(macro_ctx.get("macro_adjustment", 0.0)),
             price_d0),
        )
    conn.commit()


def pg_update_signal_outcome_dn(conn, batch_date: str, ticker: str,
                                 col_price: str, col_outcome: str,
                                 col_correct: str, price_dn: float,
                                 signal: str):
    """Rellena price_dN, outcome_dN, correct_dN en signal_outcomes."""
    # Obtener price_d0 para calcular el outcome
    with conn.cursor() as c:
        c.execute(
            f"SELECT price_d0, signal FROM signal_outcomes "
            f"WHERE batch_date=%s AND ticker=%s AND {col_outcome} IS NULL",
            (batch_date, ticker),
        )
        row = c.fetchone()
    if not row:
        return
    price_d0, sig = row
    if price_d0 is None or price_d0 == 0:
        return
    change = (price_dn - price_d0) / price_d0
    if change > 0.005:
        outcome = "UP"
    elif change < -0.005:
        outcome = "DOWN"
    else:
        outcome = "FLAT"
    correct = (sig == "BUY" and outcome == "UP") or \
              (sig == "SELL" and outcome == "DOWN") or \
              (sig == "HOLD" and outcome == "FLAT")
    with conn.cursor() as c:
        c.execute(
            f"UPDATE signal_outcomes SET {col_price}=%s, {col_outcome}=%s, "
            f"{col_correct}=%s, updated_at=CURRENT_TIMESTAMP "
            f"WHERE batch_date=%s AND ticker=%s",
            (price_dn, outcome, correct, batch_date, ticker),
        )
    conn.commit()

# =============================================================================
# 6. DESCARGA DE DATOS HISTÓRICOS
# =============================================================================

def fetch_ohlcv_all(tickers: List[str], days_back: int) -> Dict[str, pd.DataFrame]:
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=days_back + 70)
    result   = {}
    for ticker in tickers:
        logger.info(f"  OHLCV {ticker}…")
        try:
            df = yf.download(ticker, start=start_dt, end=end_dt, progress=False)
            if df.empty:
                logger.warning(f"  Sin datos OHLCV para {ticker}")
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df.index = pd.to_datetime(df.index)
            result[ticker] = df
        except Exception as exc:
            logger.error(f"  OHLCV error {ticker}: {exc}")
        time.sleep(0.5)
    return result


def fetch_news_historical(ticker: str, start_d: date, end_d: date) -> Dict[str, List]:
    """Descarga noticias Finnhub por mes y cachea en disco."""
    tag        = f"{ticker}_{start_d.strftime('%Y%m')}_{end_d.strftime('%Y%m')}"
    cache_file = CACHE_DIR / "news" / f"{tag}.json"

    if cache_file.exists():
        logger.info(f"  {ticker}: desde caché ({cache_file.name})")
        with open(cache_file, encoding="utf-8") as fh:
            return json.load(fh)

    if not FINNHUB_API_KEY:
        logger.warning(f"  FINNHUB_API_KEY no configurado — sin noticias para {ticker}")
        return {}

    news_by_date: Dict[str, List] = {}
    current = start_d.replace(day=1)

    while current <= end_d:
        next_m     = date(current.year + (current.month == 12),
                          (current.month % 12) + 1, 1)
        month_end  = min(next_m - timedelta(days=1), end_d)
        try:
            resp = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol": ticker, "from": str(current), "to": str(month_end),
                        "token": FINNHUB_API_KEY},
                timeout=12,
            )
            if resp.status_code == 200:
                for art in (resp.json() or []):
                    ts       = art.get("datetime", 0)
                    art_date = (datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                                if ts else str(current))
                    if art.get("headline"):
                        news_by_date.setdefault(art_date, []).append({
                            "headline": art.get("headline", ""),
                            "url":      art.get("url", ""),
                            "source":   art.get("source", "finnhub"),
                            "datetime": art_date,
                            "summary":  art.get("summary", ""),
                        })
                total = sum(len(v) for v in news_by_date.values())
                logger.info(f"  {ticker} {current.strftime('%Y-%m')}: acum {total} artículos")
        except Exception as exc:
            logger.error(f"  Finnhub error {ticker} {current.strftime('%Y-%m')}: {exc}")
        time.sleep(1.2)
        current = next_m

    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(news_by_date, fh, ensure_ascii=False)

    total = sum(len(v) for v in news_by_date.values())
    logger.info(f"  {ticker}: {total} artículos en {len(news_by_date)} fechas → caché guardada")
    return news_by_date


def fetch_vix_historical(start_d: date, end_d: date) -> pd.Series:
    try:
        df = yf.download("^VIX",
                         start=str(start_d - timedelta(days=5)),
                         end=str(end_d + timedelta(days=1)),
                         progress=False)
        if df.empty:
            return pd.Series(dtype=float)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df.index = pd.to_datetime(df.index)
        return df["Close"]
    except Exception as exc:
        logger.warning(f"  VIX no disponible: {exc}")
        return pd.Series(dtype=float)

# =============================================================================
# 7. INDICADORES TÉCNICOS (idéntico a lambda_indicators)
# =============================================================================

def calculate_indicators_for_date(ohlcv_df: pd.DataFrame,
                                   target_date: str) -> Optional[Dict]:
    try:
        import pandas_ta_classic as ta
    except ImportError:
        import pandas_ta as ta  # fallback

    target_dt = pd.to_datetime(target_date)
    df = ohlcv_df[ohlcv_df.index <= target_dt].copy()
    if len(df) < 50:
        return None

    close  = df["Close"]
    rsi    = ta.rsi(close, length=14)
    sma_20 = ta.sma(close, length=20)
    sma_50 = ta.sma(close, length=50)
    bbands = ta.bbands(close, length=20, std=2)

    def last(s):
        return _sf(s.iloc[-1]) if s is not None and len(s) > 0 else None

    bb_upper = bb_mid = bb_lower = None
    if bbands is not None and not bbands.empty and len(bbands.columns) >= 3:
        bb_lower = _sf(bbands.iloc[-1, 0])
        bb_mid   = _sf(bbands.iloc[-1, 1])
        bb_upper = _sf(bbands.iloc[-1, 2])

    cl = _sf(close.iloc[-1])
    s20, s50 = last(sma_20), last(sma_50)
    sma_spread = round(float(s20) - float(s50), 4) if s20 and s50 else None
    bb_width = None
    if bb_upper and bb_lower and cl:
        bb_width = round((float(bb_upper) - float(bb_lower)) / float(cl), 6)

    return {
        "close":      cl,
        "rsi_14":     last(rsi),
        "sma_20":     s20,
        "sma_50":     s50,
        "sma_spread": sma_spread,
        "bb_upper":   bb_upper,
        "bb_middle":  bb_mid,
        "bb_lower":   bb_lower,
        "bb_width":   bb_width,
    }

# =============================================================================
# 8. CONTEXTO MACRO (VIX-based — sin NewsAPI histórico)
# =============================================================================

MACRO_ADJUSTMENTS = {
    ("bullish", "RISK_ON"):  +0.12, ("bullish", "NEUTRAL"):  +0.06,
    ("bullish", "RISK_OFF"): +0.02, ("neutral", "RISK_ON"):  +0.04,
    ("neutral", "NEUTRAL"):   0.00, ("neutral", "RISK_OFF"): -0.04,
    ("bearish", "RISK_ON"):  -0.02, ("bearish", "NEUTRAL"):  -0.06,
    ("bearish", "RISK_OFF"): -0.12,
}


def calculate_macro_context(date_str: str, vix_series: pd.Series) -> Dict:
    """
    MacroSentiment = 'neutral' (sin NewsAPI histórico en plan gratuito).
    RiskRegime     = RISK_OFF si VIX>25 | RISK_ON si VIX<18 | NEUTRAL si entre 18-25.
    """
    vix = None
    try:
        before = vix_series[vix_series.index <= pd.to_datetime(date_str)]
        if not before.empty:
            vix = _sf(before.iloc[-1])
    except Exception:
        pass

    macro_sentiment = "neutral"
    if vix is not None and vix > 25:
        risk_regime = "RISK_OFF"
    elif vix is not None and vix < 18:
        risk_regime = "RISK_ON"
    else:
        risk_regime = "NEUTRAL"

    macro_adj = MACRO_ADJUSTMENTS.get((macro_sentiment, risk_regime), 0.0)

    return {
        "macro_sentiment":  macro_sentiment,
        "risk_regime":      risk_regime,
        "macro_adjustment": macro_adj,
        "vix":              vix,
    }

# =============================================================================
# 9. RED BAYESIANA (idéntica a lambda_bayesian — CPTs y lógica exactas)
# =============================================================================

_bn_model = None


def get_bn_model():
    global _bn_model
    if _bn_model is not None:
        return _bn_model
    from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork
    from pgmpy.factors.discrete import TabularCPD
    cfg    = MODEL_CONFIG["cpt_market_direction"]
    priors = MODEL_CONFIG["priors"]
    state_names = {
        "MarketDirection": ["down", "up"],
        "Sentiment":       ["bullish", "bearish", "neutral"],
        "RSI":             ["oversold", "neutral", "overbought"],
        "Trend":           ["uptrend", "downtrend"],
        "Volatility":      ["low", "high"],
    }
    model = BayesianNetwork([
        ("Sentiment","MarketDirection"), ("RSI","MarketDirection"),
        ("Trend","MarketDirection"),     ("Volatility","MarketDirection"),
    ])
    model.add_cpds(
        TabularCPD("Sentiment", 3, [[priors["Sentiment"]["bullish"]],
                                    [priors["Sentiment"]["bearish"]],
                                    [priors["Sentiment"]["neutral"]]],
                   state_names={"Sentiment": state_names["Sentiment"]}),
        TabularCPD("RSI", 3, [[priors["RSI"]["oversold"]],
                               [priors["RSI"]["neutral"]],
                               [priors["RSI"]["overbought"]]],
                   state_names={"RSI": state_names["RSI"]}),
        TabularCPD("Trend", 2, [[priors["Trend"]["uptrend"]],
                                 [priors["Trend"]["downtrend"]]],
                   state_names={"Trend": state_names["Trend"]}),
        TabularCPD("Volatility", 2, [[priors["Volatility"]["low"]],
                                      [priors["Volatility"]["high"]]],
                   state_names={"Volatility": state_names["Volatility"]}),
        TabularCPD(
            variable="MarketDirection", variable_card=2,
            values=[cfg["values_P_down"], cfg["values_P_up"]],
            evidence=["Sentiment","RSI","Trend","Volatility"], evidence_card=[3,3,2,2],
            state_names=state_names,
        ),
    )
    if not model.check_model():
        raise ValueError("Red bayesiana inválida")
    _bn_model = model
    return model


def discretize_rsi(rsi: float) -> str:
    if rsi < 30: return "oversold"
    if rsi > 70: return "overbought"
    return "neutral"


def discretize_trend(sma_20: float, sma_50: float) -> str:
    return "uptrend" if sma_20 > sma_50 else "downtrend"


def discretize_volatility(bb_upper: Optional[float],
                           bb_lower: Optional[float], close: float) -> Tuple[str, float]:
    if bb_upper is None or bb_lower is None:
        return "low", 0.0
    try:
        ratio = (float(bb_upper) - float(bb_lower)) / float(close) if float(close) > 0 else 0.0
    except Exception:
        return "low", 0.0
    return ("high" if ratio > 0.05 else "low"), round(ratio, 6)


def aggregate_sentiment(sentiments: List[Tuple]) -> Tuple[Optional[str], float, Dict]:
    """
    Replica EXACTA de lambda_bayesian.aggregate_sentiment.
    sentiments = [(sentiment, confidence, headline, justification), ...]
    ordenados por confidence DESC (para que best=sentiments[0] sea el top).
    """
    if not sentiments:
        return None, 0.0, {}
    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    for s, c, h, j in sentiments:
        if s in dist:
            dist[s] += 1
    total = len(sentiments)
    distribution = {k: {"count": v, "pct": round(v/total*100, 1)} for k, v in dist.items()}
    # best = el de mayor confidence (sentiments ya está ordenado DESC)
    best = sentiments[0]
    dominant_sentiment  = max(dist, key=dist.get)
    dominant_confidence = round(float(best[1]), 4)
    headlines_sample = [
        {"headline": h[:120] + "..." if len(h) > 120 else h,
         "sentiment": s, "confidence": round(float(c), 4)}
        for s, c, h, j in sentiments[:10]
    ]
    return dominant_sentiment, dominant_confidence, {
        "total_headlines":    total,
        "aggregation_method": "max_confidence",
        "distribution":       distribution,
        "dominant":           {"sentiment": dominant_sentiment, "confidence": dominant_confidence},
        "headlines_sample":   headlines_sample,
        "limitation": "Se utiliza Voto Mayoritario de todos los titulares del día para decidir el sentimiento.",
    }


def build_reasoning(evidence_states: Dict, prob_up: float, signal: str) -> str:
    """Replica EXACTA de lambda_bayesian.build_reasoning."""
    parts = []
    s = evidence_states.get("Sentiment")
    r = evidence_states.get("RSI")
    t = evidence_states.get("Trend")
    v = evidence_states.get("Volatility")
    if s == "bullish":   parts.append("sentimiento positivo")
    elif s == "bearish": parts.append("sentimiento negativo")
    if r == "overbought" and t == "uptrend":
        parts.append("Fuerte Momentum Alcista (RSI>70 + Tendencia)")
    elif r == "oversold":    parts.append("RSI sobrevendido -> presion compradora")
    elif r == "overbought":  parts.append("RSI sobrecomprado -> posible correccion")
    if t == "uptrend" and r != "overbought":
        parts.append("tendencia alcista (SMA20>SMA50)")
    elif t == "downtrend":   parts.append("tendencia bajista (SMA20<SMA50)")
    if v == "high":          parts.append("alta volatilidad")
    cfg = MODEL_CONFIG["signal_thresholds"]
    th  = cfg["BUY"]["prob_up_above"] if signal == "BUY" else (
          cfg["SELL"]["prob_up_below"] if signal == "SELL" else cfg["HOLD"]["range"])
    return (
        f"Evidencias: {', '.join(parts) if parts else 'mixtas'}. "
        f"P(subida)={prob_up:.2%} -> senal {signal} (umbral: {th})."
    )


def run_bayesian_inference(evidence: Dict, macro_ctx: Dict
                           ) -> Tuple[str, float, float, Dict]:
    """
    Replica EXACTA de lambda_bayesian.infer_signal.
    Retorna (signal, prob_up_adj, prob_down_adj, macro_info_dict).
    """
    from pgmpy.inference import VariableElimination
    infer      = VariableElimination(get_bn_model())
    result     = infer.query(variables=["MarketDirection"],
                             evidence=evidence, show_progress=False)
    prob_up_r  = round(float(result.values[1]), 4)
    prob_dn_r  = round(float(result.values[0]), 4)

    macro_adj      = float(macro_ctx.get("macro_adjustment", 0.0))
    macro_sent     = macro_ctx.get("macro_sentiment", "neutral")
    risk_regime    = macro_ctx.get("risk_regime", "NEUTRAL")
    prob_up_adj    = round(max(0.0, min(1.0, prob_up_r + macro_adj)), 4)
    prob_down_adj  = round(1.0 - prob_up_adj, 4)

    if prob_up_adj >= BUY_THRESHOLD:    signal = "BUY"
    elif prob_up_adj <= SELL_THRESHOLD: signal = "SELL"
    else:                               signal = "HOLD"

    macro_info = {
        "prob_up_raw":    prob_up_r,
        "prob_down_raw":  prob_dn_r,
        "macro_adjustment": macro_adj,
        "macro_sentiment":  macro_sent,
        "risk_regime":      risk_regime,
    }
    return signal, prob_up_adj, prob_down_adj, macro_info

# =============================================================================
# 10. GENERACIÓN DEL REPORT (idéntica a lambda_report.py)
# =============================================================================

def _get_trading_data(conn, report_date: str, days_back: int) -> pd.DataFrame:
    end_d   = pd.to_datetime(report_date).date()
    start_d = end_d - timedelta(days=days_back)
    with conn.cursor() as c:
        c.execute(
            """
            SELECT ts.batch_date, ts.ticker, ts.signal, ti.close_price
            FROM trading_signals ts
            JOIN technical_indicators ti
              ON ts.batch_date = ti.batch_date AND ts.ticker = ti.ticker
            WHERE ts.batch_date >= %s AND ts.batch_date <= %s
            ORDER BY ts.batch_date, ts.ticker
            """,
            (start_d, end_d),
        )
        return pd.DataFrame(c.fetchall(),
                            columns=["batch_date","ticker","signal","close_price"])


def _calc_backtesting(signals_df: pd.DataFrame) -> Tuple[Dict, Dict]:
    """Replica EXACTA de lambda_report.calculate_backtesting_metrics."""
    metrics     = {}
    diagnostics = {}
    for ticker in signals_df["ticker"].unique():
        ts = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        capital    = INITIAL_CAP
        equity     = [capital]
        position   = 0
        entry_p    = 0.0
        trade_rets = []
        sig_cnt    = ts["signal"].value_counts().to_dict()

        for _, row in ts.iterrows():
            price = float(row["close_price"]) if row["close_price"] else 0.0
            if price == 0:
                continue
            sig = row["signal"]
            if sig == "BUY":
                if position == 0:
                    position = 1
                    entry_p  = price
            elif sig in ("SELL", "HOLD"):
                if position == 1:
                    ret     = (price - entry_p) / entry_p
                    capital *= 1 + ret
                    trade_rets.append(float(ret))
                    position = 0
            equity.append(capital)

        final_eq = capital
        if position == 1 and entry_p > 0:
            last_p    = float(ts.iloc[-1]["close_price"])
            final_eq  = capital * (1 + (last_p - entry_p) / entry_p)

        cum_ret = (final_eq - INITIAL_CAP) / INITIAL_CAP
        if len(equity) > 2:
            dr      = np.diff(equity) / np.array(equity[:-1])
            excess  = dr - (RISK_FREE_RATE / 252)
            std     = np.std(excess)
            sharpe  = float(np.mean(excess) / std * np.sqrt(252)) if std > 1e-6 else 0.0
            peak    = np.maximum.accumulate(equity)
            max_dd  = float(np.min((np.array(equity) - peak) / peak))
        else:
            sharpe = max_dd = 0.0

        metrics[ticker] = {
            "cumulative_return": round(float(cum_ret), 4),
            "sharpe_ratio":      round(float(sharpe),  4),
            "max_drawdown":      round(float(max_dd),  4),
            "final_equity":      round(float(final_eq), 2),
        }
        wins        = sum(1 for v in trade_rets if v > 0)
        gross_p     = sum(v for v in trade_rets if v > 0)
        gross_l     = abs(sum(v for v in trade_rets if v < 0))
        pf          = (gross_p/gross_l) if gross_l > 1e-9 else (gross_p or 0.0)
        diagnostics[ticker] = {
            "signals": {
                "BUY":  int(sig_cnt.get("BUY",  0)),
                "SELL": int(sig_cnt.get("SELL", 0)),
                "HOLD": int(sig_cnt.get("HOLD", 0)),
            },
            "trades_closed":        len(trade_rets),
            "win_rate":             round(float(wins/len(trade_rets)), 4) if trade_rets else 0.0,
            "avg_trade_return":     round(float(np.mean(trade_rets)),  4) if trade_rets else 0.0,
            "profit_factor":        round(float(pf), 4),
            "time_in_market_ratio": round(float(sig_cnt.get("BUY",0)/max(len(ts),1)), 4),
        }
    return metrics, diagnostics


def _compute_benchmark(signals_df: pd.DataFrame) -> Dict:
    bench = {}
    for ticker in signals_df["ticker"].unique():
        tf = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        if tf.empty:
            continue
        fp = float(tf.iloc[0]["close_price"])  if tf.iloc[0]["close_price"]  else 0.0
        lp = float(tf.iloc[-1]["close_price"]) if tf.iloc[-1]["close_price"] else 0.0
        bench[ticker] = round((lp - fp) / fp, 4) if fp > 0 else 0.0
    return bench


def _get_pipeline_health(conn, report_date: str, run_id: str) -> Dict:
    with conn.cursor() as c:
        c.execute(
            "SELECT tickers_processed, status FROM batch_log WHERE run_id=%s LIMIT 1",
            (run_id,),
        )
        batch_row = c.fetchone()
        if not batch_row:
            c.execute(
                "SELECT tickers_processed, status FROM batch_log "
                "WHERE batch_date=%s ORDER BY updated_at DESC LIMIT 1",
                (report_date,),
            )
            batch_row = c.fetchone()
        c.execute(
            "SELECT COUNT(DISTINCT ticker) FROM technical_indicators WHERE batch_date=%s",
            (report_date,),
        )
        ind_tickers = c.fetchone()[0]
        c.execute(
            "SELECT COUNT(DISTINCT ticker) FROM trading_signals WHERE batch_date=%s",
            (report_date,),
        )
        sig_tickers = c.fetchone()[0]
        c.execute(
            "SELECT COUNT(*) FROM sentiment_scores WHERE batch_date=%s",
            (report_date,),
        )
        headlines = c.fetchone()[0]
        c.execute(
            "SELECT stage, metrics FROM pipeline_kpis WHERE batch_date=%s",
            (report_date,),
        )
        stage_metrics = {row[0]: row[1] for row in c.fetchall()}
    tickers_exp = int(batch_row[0]) if batch_row and batch_row[0] else 0
    return {
        "batch_status":            batch_row[1] if batch_row else "UNKNOWN",
        "tickers_expected":        tickers_exp,
        "tickers_with_indicators": int(ind_tickers or 0),
        "tickers_with_signals":    int(sig_tickers or 0),
        "headlines_scored":        int(headlines or 0),
        "coverage_ratio": round(float((sig_tickers or 0) / tickers_exp), 4)
                          if tickers_exp else 0.0,
        "stage_kpis": stage_metrics,
    }


def _get_top_explanations(conn, report_date: str, limit: int = 10) -> List[Dict]:
    with conn.cursor() as c:
        c.execute(
            """
            SELECT e.ticker, ts.signal, ts.prob_up, ts.prob_down,
                   e.sentiment_state, e.rsi_state, e.trend_state, e.volatility_state
            FROM signal_explanations e
            JOIN trading_signals ts ON ts.batch_date=e.batch_date AND ts.ticker=e.ticker
            WHERE e.batch_date=%s ORDER BY ts.prob_up DESC LIMIT %s
            """,
            (report_date, limit),
        )
        return [
            {"ticker": r[0], "signal": r[1],
             "prob_up": round(float(r[2]),4) if r[2] else None,
             "prob_down": round(float(r[3]),4) if r[3] else None,
             "evidence": {"sentiment": r[4], "rsi": r[5], "trend": r[6], "volatility": r[7]}}
            for r in c.fetchall()
        ]


def generate_and_save_report(report_date: str, conn, run_id: str) -> Dict:
    """
    Replica EXACTA del handler de lambda_report.
    Lee de Aurora y escribe en MongoDB (reports) y Aurora (pipeline_kpis, batch_log).
    """
    signals_df = _get_trading_data(conn, report_date, DAYS_BACK)
    if signals_df.empty:
        logger.warning(f"  Sin señales en Aurora para el reporte de {report_date}")
        return {}

    bt_metrics, diagnostics = _calc_backtesting(signals_df)
    health       = _get_pipeline_health(conn, report_date, run_id)
    explanations = _get_top_explanations(conn, report_date)
    benchmark    = _compute_benchmark(signals_df)

    report_data = {
        "report_date":     report_date,
        "data_period_days": DAYS_BACK,
        "generated_at":    datetime.now().isoformat(),
        "pipeline_health": health,
        "signal_diagnostics": diagnostics,
        "benchmark_comparison": {
            t: {
                "strategy_cumulative_return": bt_metrics[t]["cumulative_return"],
                "buy_hold_cumulative_return": benchmark.get(t, 0.0),
                "alpha_vs_benchmark":         round(
                    bt_metrics[t]["cumulative_return"] - benchmark.get(t, 0.0), 4),
            }
            for t in bt_metrics
        },
        "top_signal_explanations": explanations,
        "backtesting_metrics":     bt_metrics,
        "summary": {
            "total_tickers":        len(bt_metrics),
            "avg_cumulative_return": round(float(np.mean([m["cumulative_return"]
                                                          for m in bt_metrics.values()])), 4)
                                     if bt_metrics else 0,
            "avg_sharpe_ratio":     round(float(np.mean([m["sharpe_ratio"]
                                                          for m in bt_metrics.values()])), 4)
                                     if bt_metrics else 0,
            "avg_max_drawdown":     round(float(np.mean([m["max_drawdown"]
                                                          for m in bt_metrics.values()])), 4)
                                     if bt_metrics else 0,
            "total_closed_trades":  int(sum(d["trades_closed"]
                                             for d in diagnostics.values()))
                                     if diagnostics else 0,
        },
        "backtesting_config": {
            "initial_capital":   INITIAL_CAP,
            "risk_free_rate":    RISK_FREE_RATE,
            "period_days":       DAYS_BACK,
            "strategy_type":     "Long/Cash",
            "sharpe_annualized": True,
            "limitation":        "El backtesting asume ejecucion al cierre. Estrategia de conservacion de capital (Long/Cash).",
        },
        "trace_artifact": f"mongo:bayesian_traces/{report_date}",
    }

    _mg_upsert_report(report_data)
    pg_upsert_kpi(conn, report_date, run_id, "report", {
        "tickers_reported":    len(bt_metrics),
        "total_closed_trades": int(sum(d["trades_closed"] for d in diagnostics.values()))
                               if diagnostics else 0,
        "trigger_type":        "scheduled",
    })
    pg_upsert_batch_log(conn, report_date, run_id, TICKERS, status="COMPLETED")
    logger.info(f"  Reporte {report_date}: {len(bt_metrics)} tickers, "
                f"guardado en MongoDB reports y Aurora actualizado.")
    return report_data

# =============================================================================
# 11. LOOP PRINCIPAL
# =============================================================================

def run_pipeline():
    end_d   = datetime.now().date()
    start_d = end_d - timedelta(days=DAYS_BACK)

    logger.info("=" * 65)
    logger.info("🚀  TFM — Pipeline Local de Backtesting (output fiel a Lambdas)")
    logger.info(f"    Tickers  : {TICKERS}")
    logger.info(f"    Período  : {start_d}  →  {end_d}")
    logger.info(f"    FinBERT  : local (transformers)")
    logger.info(f"    Ollama   : {'✓' if _ollama_available() else '✗ no disponible'}")
    logger.info(f"    Finnhub  : {'✓ configurado' if FINNHUB_API_KEY else '✗ sin clave'}")
    logger.info(f"    MongoDB  : {'✓ mongo_utils disponible' if MONGO_OK else '✗ solo Aurora'}")
    logger.info("=" * 65)

    # ── Fase 1: Pre-fetch ─────────────────────────────────────────────────────
    logger.info("\n📥 Fase 1: Datos históricos…")
    ohlcv_all  = fetch_ohlcv_all(TICKERS, DAYS_BACK)
    vix_series = fetch_vix_historical(start_d, end_d)
    logger.info("→ Noticias Finnhub (caché por mes)…")
    news_all: Dict[str, Dict[str, List]] = {}
    for t in TICKERS:
        news_all[t] = fetch_news_historical(t, start_d, end_d)

    # ── Fase 2: Inicialización ────────────────────────────────────────────────
    logger.info("\n🔧 Fase 2: Inicialización…")
    conn = None
    try:
        conn = get_db_connection()
        logger.info("  PostgreSQL: ✓")
    except Exception as exc:
        logger.warning(f"  PostgreSQL no disponible ({exc}). Sin escritura en Aurora.")

    get_bn_model()     # valida la BN al inicio
    get_finbert()      # pre-carga FinBERT en RAM
    ollama_ok = _ollama_available()

    # ── Fase 3: Loop diario ───────────────────────────────────────────────────
    logger.info(f"\n⚙️  Fase 3: Procesando {DAYS_BACK} días…")
    business_days = pd.bdate_range(start=str(start_d), end=str(end_d))
    all_signals   = []

    for bd in tqdm(business_days, desc="Días", unit="día"):
        date_str = bd.strftime("%Y-%m-%d")
        run_id   = f"backtest-{date_str}"
        t_start  = datetime.now(timezone.utc)

        # Contexto macro del día
        macro_ctx = calculate_macro_context(date_str, vix_series)

        # Registrar batch_log STARTED
        if conn:
            conn = ensure_connection(conn)
            try:
                pg_upsert_batch_log(conn, date_str, run_id, TICKERS, "STARTED")
            except Exception as exc:
                conn.rollback()
                logger.debug(f"batch_log error {date_str}: {exc}")

        # ── OHLCV en MongoDB — una snapshot por batch_date (últimos 90 días) ─
        for ticker in TICKERS:
            ohlcv_df = ohlcv_all.get(ticker)
            if ohlcv_df is None:
                continue
            target_dt    = pd.to_datetime(date_str)
            ohlcv_window = ohlcv_df[ohlcv_df.index <= target_dt].tail(90)
            ohlcv_rows   = []
            for idx, row in ohlcv_window.iterrows():
                ohlcv_rows.append({
                    "date":   idx.strftime("%Y-%m-%d"),
                    "open":   float(row.get("Open",   0) or 0),
                    "high":   float(row.get("High",   0) or 0),
                    "low":    float(row.get("Low",    0) or 0),
                    "close":  float(row.get("Close",  0) or 0),
                    "volume": float(row.get("Volume", 0) or 0),
                })
            _mg_upsert_ohlcv(date_str, ticker, ohlcv_rows)

        # ── Noticias en MongoDB (raw_news) ────────────────────────────────────
        for ticker in TICKERS:
            articles = news_all.get(ticker, {}).get(date_str, [])
            if articles:
                _mg_upsert_raw_news(date_str, ticker, articles)

        # KPI ingestion
        ingestion_kpi = {
            "tickers_expected":   len(TICKERS),
            "tickers_with_ohlcv": sum(1 for t in TICKERS if ohlcv_all.get(t) is not None),
            "tickers_with_news":  sum(1 for t in TICKERS
                                      if news_all.get(t, {}).get(date_str)),
            "headlines_total":    sum(len(news_all.get(t, {}).get(date_str, []))
                                      for t in TICKERS),
            "trigger_type": "scheduled",
        }
        if conn:
            conn = ensure_connection(conn)
            try:
                pg_upsert_kpi(conn, date_str, run_id, "ingestion", ingestion_kpi)
            except Exception as exc:
                conn.rollback()

        # ── Macro context → Aurora ────────────────────────────────────────────
        _mg_upsert_macro(
            date_str,
            macro_ctx["macro_sentiment"],
            macro_ctx["risk_regime"],
            macro_ctx["macro_adjustment"],
            {"vix": macro_ctx.get("vix"),
             "n_articles": 0,
             "distribution": {},
             "events": {"geopolitical": False, "hawkish_fed": False,
                        "dovish_fed": False, "inflation_shock": False},
             "regime_reasoning": {"vix": macro_ctx.get("vix"),
                                  "regime_triggers": ["vix_only_backtest"]}},
        )
        if conn:
            conn = ensure_connection(conn)
            try:
                pg_upsert_macro_scores(conn, date_str, run_id,
                                       macro_ctx["macro_sentiment"], 0.0, 0)
                pg_upsert_market_regime(conn, date_str, run_id,
                                        macro_ctx["risk_regime"],
                                        macro_ctx["macro_adjustment"],
                                        macro_ctx.get("vix"))
            except Exception as exc:
                conn.rollback()
                logger.debug(f"macro Aurora error {date_str}: {exc}")

        # ── Por ticker: sentimiento + indicadores + Bayesian ──────────────────
        tickers_trace   = {}   # {ticker: ticker_trace_dict}  → bayesian_traces
        sentiment_kpi   = {"headlines_total": 0, "headlines_processed": 0,
                           "headlines_skipped": 0, "tickers_in_news": 0}
        indicator_kpi   = {"tickers_with_indicators": 0, "trigger_type": "scheduled"}
        bayesian_kpi    = {"tickers_attempted": len(TICKERS), "signals_generated": 0,
                           "tickers_skipped": 0}
        skipped_detail  = []

        for ticker in TICKERS:
            try:
                # ── Indicadores técnicos ──────────────────────────────────────
                ohlcv_df  = ohlcv_all.get(ticker)
                ind = calculate_indicators_for_date(ohlcv_df, date_str) if ohlcv_df is not None else None
                if ind is None:
                    skipped_detail.append({"ticker": ticker, "reason": "insufficient_ohlcv"})
                    bayesian_kpi["tickers_skipped"] += 1
                    continue

                if conn:
                    conn = ensure_connection(conn)
                    try:
                        pg_upsert_indicators(conn, date_str, ticker, ind)
                        indicator_kpi["tickers_with_indicators"] += 1
                    except Exception as exc:
                        conn.rollback()
                        logger.debug(f"indicators error {date_str}/{ticker}: {exc}")

                # ── Sentimiento FinBERT ───────────────────────────────────────
                articles      = news_all.get(ticker, {}).get(date_str, [])
                raw_sentiments: List[Tuple] = []   # (sentiment, confidence, headline, justification)
                filtered_hlns: List[str]    = []

                for art in articles[:10]:
                    headline = art.get("headline", "")
                    if not headline:
                        continue
                    sentiment_kpi["headlines_total"] += 1

                    # Ollama (opcional, solo últimos 30 días)
                    days_ago = (end_d - bd.date()).days
                    if ollama_ok and days_ago < 30:
                        headline = summarize_with_ollama(ticker, headline)

                    sdata = analyze_sentiment_local(headline)
                    if sdata is None:
                        sentiment_kpi["headlines_skipped"] += 1
                        continue

                    raw_sentiments.append((sdata["sentiment"], sdata["confidence"],
                                           headline, sdata.get("justification", "")))
                    filtered_hlns.append(headline)
                    sentiment_kpi["headlines_processed"] += 1

                    if conn:
                        conn = ensure_connection(conn)
                        try:
                            pg_upsert_sentiment(conn, date_str, ticker, headline, sdata)
                        except Exception as exc:
                            conn.rollback()

                    # MongoDB news (artículo con scoring FinBERT)
                    _mg_upsert_news(date_str, ticker, art, sdata)

                if articles:
                    sentiment_kpi["tickers_in_news"] += 1

                # MongoDB news_filtered (titulares procesados — sin Bedrock)
                if filtered_hlns:
                    _mg_upsert_filtered(
                        date_str, ticker, filtered_hlns,
                        f"Backtest local: {len(filtered_hlns)} titulares procesados con FinBERT (sin Bedrock).",
                    )

                # Ordenar por confidence DESC (como lo leería lambda_bayesian desde Aurora)
                raw_sentiments.sort(key=lambda x: x[1], reverse=True)
                dom_sent, dom_conf, sent_detail = aggregate_sentiment(raw_sentiments)

                if dom_sent is None:
                    # Sin sentimiento → usar neutral para no bloquear la inferencia
                    dom_sent  = "neutral"
                    dom_conf  = 0.0
                    sent_detail = {
                        "total_headlines": 0,
                        "aggregation_method": "max_confidence",
                        "distribution": {"bullish":{"count":0,"pct":0.0},
                                         "bearish":{"count":0,"pct":0.0},
                                         "neutral":{"count":0,"pct":0.0}},
                        "dominant": {"sentiment": "neutral", "confidence": 0.0},
                        "headlines_sample": [],
                        "limitation": "Sin noticias para esta fecha.",
                    }

                # ── Inferencia Bayesiana ──────────────────────────────────────
                rsi   = ind.get("rsi_14")
                sma20 = ind.get("sma_20")
                sma50 = ind.get("sma_50")
                close = ind.get("close")
                if any(v is None for v in [rsi, sma20, sma50, close]):
                    skipped_detail.append({"ticker": ticker, "reason": "incomplete_indicators"})
                    bayesian_kpi["tickers_skipped"] += 1
                    continue

                vol_state, bb_width_r = discretize_volatility(
                    ind.get("bb_upper"), ind.get("bb_lower"), close)

                evidence = {
                    "Sentiment":  dom_sent,
                    "RSI":        discretize_rsi(rsi),
                    "Trend":      discretize_trend(sma20, sma50),
                    "Volatility": vol_state,
                }
                signal, prob_up, prob_down, macro_info = run_bayesian_inference(evidence, macro_ctx)
                reasoning = build_reasoning(evidence, prob_up, signal)

                if conn:
                    conn = ensure_connection(conn)
                    try:
                        pg_upsert_signal(conn, date_str, ticker, signal, prob_up, prob_down)
                        pg_upsert_signal_explanation(conn, date_str, ticker, evidence)
                    except Exception as exc:
                        conn.rollback()
                        logger.debug(f"signal error {date_str}/{ticker}: {exc}")

                # Construir ticker_trace (idéntico a lambda_bayesian.py líneas 688-728)
                sma_spread = round(float(sma20) - float(sma50), 4) if sma20 and sma50 else None
                threshold_used = (
                    MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"] if signal == "BUY"
                    else (MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"] if signal == "SELL"
                          else MODEL_CONFIG["signal_thresholds"]["HOLD"]["range"])
                )
                ticker_trace = {
                    "raw_values": {
                        "close_price":    round(float(close), 4),
                        "rsi_14":         round(float(rsi),   4),
                        "sma_20":         round(float(sma20), 4),
                        "sma_50":         round(float(sma50), 4),
                        "sma_spread":     sma_spread,
                        "bb_upper":       round(float(ind["bb_upper"]), 4) if ind.get("bb_upper") else None,
                        "bb_lower":       round(float(ind["bb_lower"]), 4) if ind.get("bb_lower") else None,
                        "bb_width_ratio": bb_width_r,
                    },
                    "discretization": {
                        "sentiment_raw":   dom_sent,
                        "sentiment_conf":  dom_conf,
                        "sentiment_state": evidence["Sentiment"],
                        "rsi_state":       evidence["RSI"],
                        "trend_state":     evidence["Trend"],
                        "volatility_state":evidence["Volatility"],
                    },
                    "sentiment_detail": sent_detail,
                    "inference": {
                        "prob_up":       prob_up,
                        "prob_down":     prob_down,
                        "signal":        signal,
                        "threshold_used": threshold_used,
                        "macro_context": macro_info,
                    },
                    "reasoning": reasoning,
                }
                tickers_trace[ticker] = ticker_trace

                # MongoDB bayesian_reports (por ticker)
                _mg_upsert_br(date_str, ticker, ticker_trace, MODEL_CONFIG["version"])

                # signal_outcomes en Aurora (precio D0 + nodos)
                if conn:
                    conn = ensure_connection(conn)
                    try:
                        pg_upsert_signal_outcome(
                            conn, date_str, ticker, run_id,
                            signal, prob_up, prob_down,
                            evidence, macro_ctx, close,
                        )
                    except Exception as exc:
                        conn.rollback()

                bayesian_kpi["signals_generated"] += 1
                all_signals.append({
                    "date": date_str, "ticker": ticker,
                    "signal": signal, "prob_up": prob_up, "prob_down": prob_down,
                    "sentiment": dom_sent, "rsi": round(rsi, 2),
                    "rsi_state": evidence["RSI"], "trend_state": evidence["Trend"],
                    "volatility_state": evidence["Volatility"],
                    "vix": macro_ctx.get("vix"), "risk_regime": macro_ctx.get("risk_regime"),
                    "macro_adj": macro_ctx.get("macro_adjustment"), "close": close,
                    "n_news": len(articles), "n_sentiments": len(raw_sentiments),
                })

            except Exception as exc:
                logger.error(f"Error {ticker} {date_str}: {exc}")
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                skipped_detail.append({"ticker": ticker, "reason": str(exc)})
                bayesian_kpi["tickers_skipped"] += 1

        # ── Fin del día: bayesian_trace completo ──────────────────────────────
        t_end = datetime.now(timezone.utc)
        execution_meta = {
            "started_at":       t_start.isoformat(),
            "finished_at":      t_end.isoformat(),
            "duration_seconds": round((t_end - t_start).total_seconds(), 2),
            "run_id":           run_id,
            "trigger_type":     "scheduled",
            "batch_date":       date_str,
            "tickers_attempted":len(TICKERS),
            "signals_generated":bayesian_kpi["signals_generated"],
            "tickers_skipped":  bayesian_kpi["tickers_skipped"],
            "skipped_detail":   skipped_detail,
        }
        full_trace = {
            "schema_version": "2.0",
            "batch_date":     date_str,
            "generated_at":   t_end.isoformat(),
            "execution":      execution_meta,
            "model_config":   MODEL_CONFIG,
            "tickers":        tickers_trace,
            "audit_notes": {
                "cpt_source":        "Parametros ajustados para capturar momentum alcista",
                "threshold_rsi":     "RSI <30 = oversold, >70 = overbought",
                "threshold_vol":     "BB width ratio >0.05 = high",
                "threshold_signal":  "P(up) >0.58 = BUY, <0.42 = SELL",
                "known_issues":      MODEL_CONFIG["known_limitations"],
                "backtest_note":     "Generado por local_backtest_runner.py — sin Bedrock, sin AWS.",
            },
        }
        _mg_upsert_bt(date_str, full_trace)

        # ── KPIs en Aurora ────────────────────────────────────────────────────
        if conn:
            conn = ensure_connection(conn)
            try:
                pg_upsert_kpi(conn, date_str, run_id, "sentiment", {
                    **sentiment_kpi, "trigger_type": "scheduled"})
                pg_upsert_kpi(conn, date_str, run_id, "indicators", {
                    **indicator_kpi, "trigger_type": "scheduled"})
                pg_upsert_kpi(conn, date_str, run_id, "bayesian", {
                    **bayesian_kpi, "trace_storage": "mongo",
                    "model_version": MODEL_CONFIG["version"], "trigger_type": "scheduled"})
            except Exception as exc:
                conn.rollback()
                logger.debug(f"KPI write error {date_str}: {exc}")

    # ── Fase 4: Signal Outcomes (D+1 / D+3 / D+5) ────────────────────────────
    logger.info("\n📐 Fase 4: Rellenando outcomes D+1/D+3/D+5…")
    if conn and all_signals:
        bdays_str = [bd.strftime("%Y-%m-%d") for bd in business_days]
        bdays_idx = {d: i for i, d in enumerate(bdays_str)}

        for sig in tqdm(all_signals, desc="Outcomes", unit="señal"):
            sig_date = sig["date"]
            ticker   = sig["ticker"]
            idx0     = bdays_idx.get(sig_date)
            if idx0 is None:
                continue
            ohlcv_df = ohlcv_all.get(ticker)
            if ohlcv_df is None:
                continue
            for days, col_p, col_o, col_c in [
                (1, "price_d1", "outcome_d1", "correct_d1"),
                (3, "price_d3", "outcome_d3", "correct_d3"),
                (5, "price_d5", "outcome_d5", "correct_d5"),
            ]:
                dn_idx = idx0 + days
                if dn_idx >= len(bdays_str):
                    continue
                dn_date = bdays_str[dn_idx]
                dn_dt   = pd.to_datetime(dn_date)
                rows    = ohlcv_df[ohlcv_df.index <= dn_dt].tail(1)
                if rows.empty:
                    continue
                price_dn = _sf(rows["Close"].iloc[0])
                if price_dn is None:
                    continue
                conn = ensure_connection(conn)
                try:
                    pg_update_signal_outcome_dn(
                        conn, sig_date, ticker, col_p, col_o, col_c,
                        price_dn, sig["signal"],
                    )
                except Exception as exc:
                    conn.rollback()
                    logger.debug(f"outcome error {sig_date}/{ticker}: {exc}")

    # ── Fase 5: Generar report para la fecha más reciente ─────────────────────
    logger.info("\n📊 Fase 5: Generando reporte final (lambda_report)…")
    final_date  = end_d.strftime("%Y-%m-%d")
    final_run_id = f"backtest-{final_date}"
    report_data  = {}
    if conn:
        conn = ensure_connection(conn)
        try:
            report_data = generate_and_save_report(final_date, conn, final_run_id)
        except Exception as exc:
            logger.error(f"Error generando reporte: {exc}")
            conn.rollback()

    if conn:
        try:
            conn.close()
        except Exception:
            pass

    # ── Fase 6: CSV de señales ────────────────────────────────────────────────
    logger.info("\n💾 Fase 6: Exportando CSV de señales…")
    out = Path("backtest_output")
    out.mkdir(exist_ok=True)
    if all_signals:
        pd.DataFrame(all_signals).to_csv(out / "signals_daily.csv", index=False)
        logger.info(f"  {len(all_signals)} señales → {out/'signals_daily.csv'}")

    # ── Resumen ───────────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 65)
    logger.info("✅  BACKTESTING COMPLETADO")
    logger.info(f"    Señales generadas : {len(all_signals)}")
    logger.info(f"    MongoDB           : {'✓ datos escritos' if MONGO_OK else '✗ no disponible'}")
    if report_data:
        logger.info("-" * 65)
        for ticker, m in report_data.get("backtesting_metrics", {}).items():
            bh  = report_data["benchmark_comparison"].get(ticker, {})
            alpha = bh.get("alpha_vs_benchmark", 0)
            emoji = "🟢" if alpha > 0 else ("⚪" if abs(alpha) < 0.01 else "🔴")
            note  = {"SPY": "→ caso negativo (esperado α≈0 o negativo)",
                     "IWM": "→ caso positivo (small caps)",
                     "GLD": "→ caso positivo (activo real)"}.get(ticker, "")
            logger.info(
                f"  {emoji} {ticker}: estrategia {m['cumulative_return']:+.1%}  "
                f"B&H {bh.get('buy_hold_cumulative_return', 0):+.1%}  "
                f"Alpha {alpha:+.1%}  Sharpe {m['sharpe_ratio']:.2f}  {note}"
            )
    logger.info("=" * 65)
    return report_data


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    run_pipeline()
