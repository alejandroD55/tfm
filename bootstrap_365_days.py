#!/usr/bin/env python3
"""
local_backtest_runner.py — Pipeline TFM local: 365 días de backtesting
=======================================================================
Orquesta el flujo de MLOps en local para que sea EXACTAMENTE IGUAL
al entorno de AWS (Lambdas + StepFunctions).
"""

import groq
import os
import sys
import json
import time
import threading
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd
import numpy as np
import yfinance as yf
import psycopg2
import trafilatura
from dotenv import load_dotenv
from tqdm import tqdm

import feedparser
import argparse

warnings.filterwarnings("ignore")

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_REPO_ROOT, "shared"))

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    # Por defecto, el bootstrap es "quiet": solo WARNING/ERROR + tqdm (loader de días).
    # Usa --verbose si quieres ver INFO.
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("trafilatura").setLevel(logging.CRITICAL)
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

load_dotenv()

def _configure_logging(verbose: bool) -> None:
    level = logging.INFO if verbose else logging.ERROR
    logging.getLogger().setLevel(level)
    logger.setLevel(level)


def get_args():
    parser = argparse.ArgumentParser(description="TFM Backtester Local")
    parser.add_argument(
        "--start", type=str, help="Fecha inicio YYYY-MM-DD", default=None
    )
    parser.add_argument("--end", type=str, help="Fecha fin YYYY-MM-DD", default=None)
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Subset de tickers separados por coma, ej: ARKK,XBI. Si no se indica, usa TICKERS global.",
    )
    parser.add_argument(
        "--debug-news",
        action="store_true",
        help="Muestra trazas detalladas de ingesta de noticias por ticker/dia/fuente.",
    )
    parser.add_argument(
        "--debug-news-headlines",
        type=int,
        default=None,
        help="Numero de titulares de ejemplo a mostrar por ticker/dia cuando --debug-news esta activo.",
    )
    parser.add_argument(
        "--refresh-news-cache",
        action="store_true",
        help="Ignora y regenera caches de noticias historicas Finnhub/NewsAPI/AlphaVantage para el rango solicitado.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Activa logs INFO (por defecto solo WARNING/ERROR + barra de progreso).",
    )
    return parser.parse_args()


# =============================================================================
# VARIABLES Y CONFIGURACIÓN
# =============================================================================
TICKERS = ["SPY", "IWM", "GLD", "XLE", "NVDA"]
DAYS_BACK = 365
INITIAL_CAP = 10_000.0
RISK_FREE_RATE = 0.02

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
MONGODB_URI = os.getenv("MONGODB_URI", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")
# Plan Developer gratuito: búsqueda hasta ~30 días atrás (newsapi.org/pricing).
# Plan de pago: histórico ampliado — define NEWSAPI_UNLIMITED_HISTORY=1 para no recortar.
NEWSAPI_HISTORY_DAYS = int(os.getenv("NEWSAPI_HISTORY_DAYS", "30"))
NEWSAPI_UNLIMITED_HISTORY = os.getenv("NEWSAPI_UNLIMITED_HISTORY", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Motor discriminativo opcional (LightGBM).
# Si no está inicializado/disponible, la inferencia cae automáticamente al camino bayesiano.
_disc_engine = None

DEBUG_NEWS = os.getenv("BOOTSTRAP_DEBUG_NEWS", "").lower() in ("1", "true", "yes", "on")
DEBUG_NEWS_HEADLINES = int(os.getenv("BOOTSTRAP_DEBUG_NEWS_HEADLINES", "3"))
REFRESH_NEWS_CACHE = os.getenv("BOOTSTRAP_REFRESH_NEWS_CACHE", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Cuando una fuente no publica exactamente en el dia objetivo, reutilizamos
# articulos de dias inmediatamente anteriores para evitar gaps artificiales.
TICKER_NEWS_LOOKBACK_DAYS = int(os.getenv("BOOTSTRAP_TICKER_NEWS_LOOKBACK_DAYS", "3"))

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.getenv("POSTGRES_USER", "tfmadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "localpassword123"),
    "database": os.getenv("POSTGRES_DB", "tfm"),
}

CACHE_DIR = Path("cache")
(CACHE_DIR / "news").mkdir(parents=True, exist_ok=True)
(CACHE_DIR / "ohlcv").mkdir(parents=True, exist_ok=True)


def _sf(v) -> Optional[float]:
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except Exception:
        return None


# Queries NewsAPI por ticker — se usan para enriquecer la ingesta histórica
# cuando Finnhub devuelve pocos artículos.
TICKER_NEWSAPI_QUERIES: Dict[str, List[str]] = {
    "SPY": ["S&P 500", "SPY ETF"],
    "IWM": ["Russell 2000", "IWM ETF small cap"],
    "GLD": ["gold price", "GLD ETF"],
    "XLE": ["energy stocks", "XLE ETF oil"],
    "NVDA": ["NVIDIA", "NVDA earnings", "DeepSeek NVIDIA"],
    "ARKK": ["ARK Innovation", "Cathie Wood ARKK"],
    "XBI": ["biotech stocks", "XBI ETF FDA"],
}

# Términos de búsqueda macro para NewsAPI
MACRO_NEWSAPI_QUERIES: Dict[str, List[str]] = {
    "monetary_policy": ["Federal Reserve interest rates", "Fed monetary policy ECB"],
    "inflation": ["CPI inflation consumer prices", "PCE inflation data"],
    "macro_economy": ["GDP growth recession", "unemployment nonfarm payrolls"],
    "geopolitical": ["geopolitical tensions war sanctions markets"],
}

RSS_FEEDS = {
    "reuters": "https://feeds.reuters.com/reuters/businessNews",
    "cnbc": "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "ft": "https://www.ft.com/world?format=rss",
}


def _fingerprint(url: str, headline: str) -> str:
    import hashlib

    key = (url or headline or "").strip().lower()
    return hashlib.md5(key.encode()).hexdigest()


def _newsapi_clamp_range(
    start_d: date, end_d: date, label: str = "NewsAPI"
) -> Optional[Tuple[date, date]]:
    """
    Ajusta from/to al ventana permitida por NewsAPI.
    Free Developer: artículos hasta ~30 días atrás, 100 req/día.
    Devuelve None si todo el rango queda fuera de ventana.
    """
    if NEWSAPI_UNLIMITED_HISTORY:
        return start_d, end_d

    today = datetime.now().date()
    earliest = today - timedelta(days=NEWSAPI_HISTORY_DAYS)
    eff_start = max(start_d, earliest)
    eff_end = min(end_d, today)
    if eff_end < eff_start:
        logger.warning(
            f"[{label}] rango {start_d}→{end_d} fuera de ventana "
            f"(últimos {NEWSAPI_HISTORY_DAYS} días en plan free). "
            f"NewsAPI no aportará para estas fechas."
        )
        return None
    if eff_start != start_d or eff_end != end_d:
        logger.warning(
            f"[{label}] rango ajustado {start_d}→{end_d} → {eff_start}→{eff_end} "
            f"(plan free: últimos {NEWSAPI_HISTORY_DAYS} días; "
            f"Finnhub/AlphaVantage cubren fechas anteriores)"
        )
    return eff_start, eff_end


def _normalize_macro(headline, url, source, dt, category, query_tag, summary=""):
    return {
        "headline": headline.strip(),
        "url": url,
        "source": source,
        "datetime": dt,
        "summary": summary,
        "category": category,
        "query_tag": query_tag,
    }


def _count_news(news_by_date: Dict[str, List]) -> Tuple[int, int]:
    return sum(len(v) for v in news_by_date.values()), len(news_by_date)


def _news_debug(message: str) -> None:
    if DEBUG_NEWS:
        logger.info(message)


def _headline_samples(articles: List[Dict], limit: Optional[int] = None) -> List[str]:
    max_items = DEBUG_NEWS_HEADLINES if limit is None else limit
    if max_items <= 0:
        return []
    return [
        (art.get("headline") or "").replace("\n", " ").strip()[:180]
        for art in articles[:max_items]
        if art.get("headline")
    ]


MODEL_CONFIG = {
    "version": "1.2.0",
    "description": "Red bayesiana v1.2: umbrales calibrados (SELL≤0.28, BUY≥0.52), priors con drift alcista historico, macro_adj amortiguado en uptrend",
    "discretization": {
        "rsi": {
            "oversold_below": 30,
            "overbought_above": 70,
            "neutral_range": [30, 70],
        },
        "trend": {"rule": "SMA20 > SMA50 = uptrend"},
        "volatility": {"high_if_band_width_ratio_above": 0.05},
    },
    "signal_thresholds": {
        "BUY": {"prob_up_above": 0.52},
        "SELL": {"prob_up_below": 0.28},
        "HOLD": {"range": [0.28, 0.52]},
    },
    "priors": {
        # Sesgo ligeramente alcista: mercados suben ~60% de los días históricamente
        "Sentiment": {"bullish": 0.35, "bearish": 0.25, "neutral": 0.40},
        "RSI": {"oversold": 0.15, "neutral": 0.60, "overbought": 0.25},
        "Trend": {"uptrend": 0.58, "downtrend": 0.42},
        "Volatility": {"low": 0.62, "high": 0.38},
    },
    "cpt_market_direction": {
        "variable": "MarketDirection",
        "states": ["down", "up"],
        # Orden: (Sentiment x RSI x Trend x Volatility)
        # Corrección clave: P_up para overbought en uptrend sube ~+0.08
        # porque en un mercado alcista, overbought tiende a continuar, no revertir
        "values_P_down": [
            0.12,
            0.22,
            0.25,
            0.18,
            0.25,
            0.30,
            0.22,
            0.35,
            0.08,
            0.12,
            0.40,
            0.45,
            0.70,
            0.75,
            0.80,
            0.75,
            0.80,
            0.85,
            0.80,
            0.85,
            0.50,
            0.55,
            0.90,
            0.95,
            0.42,
            0.48,
            0.52,
            0.47,
            0.52,
            0.58,
            0.52,
            0.58,
            0.22,
            0.28,
            0.62,
            0.68,
        ],
        "values_P_up": [
            0.88,
            0.78,
            0.75,
            0.82,
            0.75,
            0.70,
            0.78,
            0.65,
            0.92,
            0.88,
            0.60,
            0.55,
            0.30,
            0.25,
            0.20,
            0.25,
            0.20,
            0.15,
            0.20,
            0.15,
            0.50,
            0.45,
            0.10,
            0.05,
            0.58,
            0.52,
            0.48,
            0.53,
            0.48,
            0.42,
            0.48,
            0.42,
            0.78,
            0.72,
            0.38,
            0.32,
        ],
    },
    "known_limitations": [
        "El confidence score de FinBERT no entra en la inferencia",
        "Voto mayoritario",
    ],
    "hysteresis": {
        "sell_confirmation_days": 2,
        "buy_confirmation_days": 1,
        "rationale": (
            "Persistencia de señal: SELL solo actúa si se repite N días consecutivos. "
            "Evita salidas falsas por una noticia bearish puntual en tendencia alcista."
        ),
    },
}

BUY_THRESHOLD = MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"]
SELL_THRESHOLD = MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"]

# ── Hysteresis / Signal Persistence ──────────────────────────────────────────
# SELL necesita N días consecutivos para confirmarse; BUY actúa inmediatamente.
SELL_CONFIRMATION_DAYS: int = MODEL_CONFIG["hysteresis"]["sell_confirmation_days"]
BUY_CONFIRMATION_DAYS: int = MODEL_CONFIG["hysteresis"]["buy_confirmation_days"]

# =============================================================================
# IA LOCAL: FINBERT & GROQ
# =============================================================================
_finbert_pipeline = None
_groq_client = None

# =============================================================================
# THREAD-SAFETY PRIMITIVES
# =============================================================================
# FinBERT (transformers pipeline) no es thread-safe para inferencia concurrente.
# Todos los hilos deben adquirir este lock antes de llamar a FinBERT.
_finbert_lock = threading.Lock()

# Rate limiter preciso para Groq free tier (~30 rpm = 1 llamada cada 2s seguro).
_GROQ_MIN_INTERVAL_S = 2.1
_groq_last_ts: List[float] = [0.0]
_groq_ts_lock = threading.Lock()


def _groq_rate_wait() -> None:
    """Bloquea el hilo actual hasta que sea seguro lanzar otra llamada a Groq."""
    with _groq_ts_lock:
        now = time.time()
        wait = _GROQ_MIN_INTERVAL_S - (now - _groq_last_ts[0])
        if wait > 0:
            time.sleep(wait)
        _groq_last_ts[0] = time.time()



def get_finbert():
    global _finbert_pipeline
    if _finbert_pipeline is None:
        logger.info("Cargando FinBERT local...")
        from transformers import pipeline as hf_pipeline
        import torch

        if torch.backends.mps.is_available():
            device = "mps"
        elif torch.cuda.is_available():
            device = 0
        else:
            device = -1

        device_label = device if isinstance(device, str) else ("cuda:0" if device == 0 else "cpu")
        logger.info(f"FinBERT usará dispositivo: {device_label}")

        _finbert_pipeline = hf_pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            device=device,
            truncation=True,
            max_length=512,
        )
    return _finbert_pipeline


def get_groq_client():
    global _groq_client
    if _groq_client is None and GROQ_API_KEY:
        _groq_client = groq.Groq(api_key=GROQ_API_KEY)
    return _groq_client


def extract_and_summarize(ticker: str, headline: str, url: str) -> str:
    """
    Descarga el artículo completo (trafilatura), lo resume con Groq/LLaMA y
    devuelve el texto para que FinBERT clasifique sentimiento sobre contenido
    real en lugar de solo el titular.

    Thread-safe: usa _groq_rate_wait() en lugar de time.sleep() fijo.
    """
    client = get_groq_client()
    try:
        resp = requests.get(url, timeout=5)
        text = trafilatura.extract(resp.text)
        content = text[:4000] if text else headline
    except Exception:
        content = headline

    if client:
        prompt = f"Ticker: {ticker}\nHeadline: {headline}\nContent:\n{content}\n\nSummarize this financial news objectively in 1 or 2 sentences, preserving its original tone. Do not invent information. Reply ONLY with the summary in plain text without markdown."
        try:
            _groq_rate_wait()  # rate limiter preciso — reemplaza time.sleep fijo
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": "You are a financial analyst."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                max_tokens=150,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.debug(f"Groq falló: {e}")
    return headline


def analyze_sentiment_local(headline: str) -> Optional[Dict]:
    """Thread-safe: adquiere _finbert_lock antes de llamar al modelo."""
    if not headline or len(headline.strip()) < 20:
        return None
    try:
        with _finbert_lock:
            results = get_finbert()(headline)[0]
        label, score = results["label"].lower(), float(results["score"])
        if score < 0.55:
            return None
        lmap = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
        return {
            "sentiment": lmap.get(label, "neutral"),
            "confidence": round(score, 4),
            "justification": "FinBERT local",
        }
    except Exception:
        return None


def run_finbert_macro_local(articles: list) -> dict:
    if not articles:
        return {"score": 0.0, "state": "neutral", "distribution": {}, "n_articles": 0}

    weighted_sum = 0.0
    weight_total = 0.0
    scored = 0
    distribution = {"bullish": 0, "neutral": 0, "bearish": 0}

    for art in articles[:50]:
        headline = art.get("headline", "")
        if len(headline) < 20:
            continue
        try:
            with _finbert_lock:
                res = get_finbert()(headline)[0]
            score = float(res["score"])
            label = res["label"].lower()
            if score < 0.55:
                continue

            s_map = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
            sent = s_map.get(label, "neutral")
            val = 1.0 if sent == "bullish" else (-1.0 if sent == "bearish" else 0.0)

            weighted_sum += val * score
            weight_total += 1.0
            scored += 1
            distribution[sent] += 1
        except Exception:
            pass

    if weight_total == 0:
        return {
            "score": 0.0,
            "state": "neutral",
            "distribution": distribution,
            "n_articles": 0,
        }
    f_score = weighted_sum / weight_total
    state = (
        "bullish" if f_score > 0.20 else ("bearish" if f_score < -0.20 else "neutral")
    )
    return {
        "score": round(f_score, 4),
        "state": state,
        "distribution": distribution,
        "n_articles": scored,
    }


def aggregate_sentiment_local(samples: list) -> Tuple[str, float, dict]:
    if not samples:
        return "neutral", 0.0, {}
    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    for s in samples:
        dist[s["sentiment"]] += 1
    total = len(samples)
    distribution = {
        k: {"count": v, "pct": round(v / total * 100, 1)} for k, v in dist.items()
    }
    dominant_sentiment = max(dist, key=dist.get) if dist else "neutral"

    best_conf = 0.0
    for s in samples:
        if s["sentiment"] == dominant_sentiment and s["confidence"] > best_conf:
            best_conf = s["confidence"]

    detail = {
        "total_headlines": total,
        "aggregation_method": "max_confidence",
        "distribution": distribution,
        "dominant": {"sentiment": dominant_sentiment, "confidence": best_conf},
        "headlines_sample": samples,
        "limitation": "Se utiliza Voto Mayoritario de todos los titulares del día.",
    }
    return dominant_sentiment, best_conf, detail


def build_reasoning_local(evidence_states, prob_up, signal):
    parts = []
    s, r, t, v = (
        evidence_states.get(k) for k in ("Sentiment", "RSI", "Trend", "Volatility")
    )
    if s == "bullish":
        parts.append("sentimiento positivo")
    elif s == "bearish":
        parts.append("sentimiento negativo")

    if r == "overbought" and t == "uptrend":
        parts.append("Fuerte Momentum Alcista (RSI>70 + Tendencia)")
    elif r == "oversold":
        parts.append("RSI sobrevendido -> presion compradora")
    elif r == "overbought":
        parts.append("RSI sobrecomprado -> posible correccion")

    if t == "uptrend" and r != "overbought":
        parts.append("tendencia alcista (SMA20>SMA50)")
    elif t == "downtrend":
        parts.append("tendencia bajista (SMA20<SMA50)")

    if v == "high":
        parts.append("alta volatilidad")

    th = (
        MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"]
        if signal == "BUY"
        else (
            MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"]
            if signal == "SELL"
            else MODEL_CONFIG["signal_thresholds"]["HOLD"]["range"]
        )
    )
    return f"Evidencias: {', '.join(parts) if parts else 'mixtas'}. P(subida)={prob_up:.2%} -> senal {signal} (umbral: {th})."


# =============================================================================
# MONGODB & AURORA HELPERS
# =============================================================================
from mongo_utils import (
    upsert_raw_news,
    upsert_ohlcv_bulk,
    upsert_news,
    upsert_filtered_news,
    upsert_bayesian_report,
    upsert_bayesian_trace,
    upsert_macro_context,
    upsert_report,
    upsert_macro_news,
    read_macro_news,
    upsert_quant_audit_report,
)
from quant_observability import (
    compute_contribution_analysis,
    compute_quant_audit_report,
)


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        sslmode="prefer",
    )


def pg_upsert_signal(conn, date_str, ticker, signal, prob_up, prob_down):
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down) VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET signal=EXCLUDED.signal, prob_up=EXCLUDED.prob_up, prob_down=EXCLUDED.prob_down
        """,
            (date_str, ticker, signal, float(prob_up), float(prob_down)),
        )
    conn.commit()


def pg_upsert_batch_log(conn, batch_date, run_id, tickers, status):
    try:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO batch_log (batch_date, run_id, trigger_type, status, tickers_processed)
                VALUES (%s, %s, 'manual', %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP
            """,
                (batch_date, run_id, status, len(tickers)),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"Error batch_log: {e}")


def pg_upsert_pipeline_kpi(
    connection, batch_date, run_id, trigger_type, stage, metrics
):
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO pipeline_kpis (batch_date, run_id, trigger_type, stage, metrics) 
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (run_id, stage) DO UPDATE SET metrics = EXCLUDED.metrics, updated_at = CURRENT_TIMESTAMP
    """,
        (batch_date, run_id, trigger_type, stage, json.dumps(metrics)),
    )
    connection.commit()
    cursor.close()


def pg_upsert_position_state(
    conn,
    date_str: str,
    ticker: str,
    prob_up: float,
    regime: str,
    target_exposure: float,
    smoothed_exposure: float,
    exposure_delta: float,
):
    """Persiste el estado de exposición diario en la tabla position_state."""
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO position_state
                (batch_date, ticker, prob_up, market_regime,
                 target_exposure, smoothed_exposure, exposure_delta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                prob_up           = EXCLUDED.prob_up,
                market_regime     = EXCLUDED.market_regime,
                target_exposure   = EXCLUDED.target_exposure,
                smoothed_exposure = EXCLUDED.smoothed_exposure,
                exposure_delta    = EXCLUDED.exposure_delta
            """,
            (date_str, ticker, float(prob_up), regime,
             float(target_exposure), float(smoothed_exposure), float(exposure_delta)),
        )
    conn.commit()


# NUEVO: EXTRAER HISTÓRICO DE AURORA (Para clonar AWS lambda_report.py)
def get_trading_data(
    connection,
    report_date,
    days_back=DAYS_BACK,
    tickers: Optional[List[str]] = None,
    pipeline_start: Optional[date] = None,
):
    try:
        cursor = connection.cursor()
        end_date = pd.to_datetime(report_date).date()
        start_date = end_date - timedelta(days=days_back)
        if pipeline_start is not None:
            start_date = max(start_date, pipeline_start)
        query = """
            SELECT ts.batch_date, ts.ticker, ts.signal, ts.prob_up, ts.prob_down,
                   ti.close_price, ti.rsi_14, ti.sma_20, ti.sma_50,
                   ti.bb_upper, ti.bb_middle, ti.bb_lower
            FROM trading_signals ts
            JOIN technical_indicators ti ON ts.batch_date = ti.batch_date AND ts.ticker = ti.ticker
            WHERE ts.batch_date >= %s AND ts.batch_date <= %s
        """
        params = [start_date, end_date]
        if tickers:
            query += " AND ts.ticker = ANY(%s)"
            params.append(tickers)
        query += " ORDER BY ts.batch_date, ts.ticker"
        cursor.execute(query, params)
        signals_df = pd.DataFrame(
            cursor.fetchall(),
            columns=[
                "batch_date",
                "ticker",
                "signal",
                "prob_up",
                "prob_down",
                "close_price",
                "rsi_14",
                "sma_20",
                "sma_50",
                "bb_upper",
                "bb_middle",
                "bb_lower",
            ],
        )
        cursor.close()
        return signals_df
    except Exception:
        raise


def get_signal_outcomes(
    connection,
    report_date,
    days_back=DAYS_BACK,
    tickers: Optional[List[str]] = None,
    pipeline_start: Optional[date] = None,
):
    try:
        cursor = connection.cursor()
        end_date = pd.to_datetime(report_date).date()
        start_date = end_date - timedelta(days=days_back)
        if pipeline_start is not None:
            start_date = max(start_date, pipeline_start)
        query = """
            SELECT batch_date, ticker, signal, prob_up, outcome_d1, outcome_d3,
                   outcome_d5, correct_d1, correct_d3, correct_d5
            FROM signal_outcomes
            WHERE batch_date >= %s AND batch_date <= %s
        """
        params = [start_date, end_date]
        if tickers:
            query += " AND ticker = ANY(%s)"
            params.append(tickers)
        query += " ORDER BY batch_date, ticker"
        cursor.execute(query, params)
        cols = [
            "batch_date",
            "ticker",
            "signal",
            "prob_up",
            "outcome_d1",
            "outcome_d3",
            "outcome_d5",
            "correct_d1",
            "correct_d3",
            "correct_d5",
        ]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows
    except Exception as exc:
        logger.warning(f"No se pudieron leer signal_outcomes para auditoria: {exc}")
        return []


# =============================================================================
# DATOS Y LÓGICA DE BACKTESTING
# =============================================================================
def fetch_ohlcv_all(
    tickers: List[str], start_date: date, end_date: date
) -> Dict[str, pd.DataFrame]:
    # Lookback de 350 días calendario ≈ 245 días hábiles, garantiza SMA200 desde el primer día del rango
    download_start = start_date - timedelta(days=350)
    result = {}
    for ticker in tickers:
        df = yf.download(
            ticker, start=str(download_start), end=str(end_date), progress=False, repair=False
        )
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df.index = pd.to_datetime(df.index)
            result[ticker] = df
    return result


def fetch_news_historical(ticker: str, start_d: date, end_d: date) -> Dict[str, List]:
    cache_file = (
        CACHE_DIR
        / "news"
        / f"{ticker}_{start_d.strftime('%Y%m')}_{end_d.strftime('%Y%m')}.json"
    )
    if cache_file.exists() and not REFRESH_NEWS_CACHE:
        with open(cache_file, encoding="utf-8") as fh:
            cached = json.load(fh)
        total, days = _count_news(cached)
        _news_debug(
            f"[news-cache] Finnhub {ticker}: {total} articulos en {days} dias desde {cache_file}"
        )
        return cached
    if cache_file.exists() and REFRESH_NEWS_CACHE:
        _news_debug(f"[news-cache] Finnhub {ticker}: ignorando cache por --refresh-news-cache")
    if not FINNHUB_API_KEY:
        _news_debug(f"[Finnhub] {ticker}: sin FINNHUB_API_KEY, fuente omitida")
        return {}

    news_by_date, current = {}, start_d.replace(day=1)
    while current <= end_d:
        next_m = date(current.year + (current.month == 12), (current.month % 12) + 1, 1)
        month_end = min(next_m - timedelta(days=1), end_d)
        resp = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={
                "symbol": ticker,
                "from": str(current),
                "to": str(month_end),
                "token": FINNHUB_API_KEY,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            for art in resp.json() or []:
                dt = datetime.utcfromtimestamp(art.get("datetime", 0)).strftime(
                    "%Y-%m-%d"
                )
                if art.get("headline"):
                    news_by_date.setdefault(dt, []).append(
                        {
                            "headline": art["headline"],
                            "url": art.get("url", ""),
                            "source": art.get("source", "finnhub"),
                            "datetime": datetime.utcfromtimestamp(
                                art.get("datetime", 0)
                            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "summary": art.get("summary", ""),
                        }
                    )
        else:
            logger.warning(
                f"[Finnhub] {ticker} {current:%Y-%m}: HTTP {resp.status_code} - {resp.text[:160]}"
            )
        time.sleep(1.2)
        current = next_m
    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(news_by_date, fh)
    total, days = _count_news(news_by_date)
    logger.info(f"[Finnhub] {ticker}: {total} articulos en {days} dias -> cacheado")
    return news_by_date


def fetch_alpha_vantage_news_historical(
    ticker: str, start_d: date, end_d: date
) -> Dict[str, List]:
    """
    Fuente alternativa para eventos historicos por ticker.
    Alpha Vantage NEWS_SENTIMENT permite consultar rango completo en una llamada.
    """
    cache_file = (
        CACHE_DIR
        / "news"
        / f"alphavantage_{ticker}_{start_d.strftime('%Y%m')}_{end_d.strftime('%Y%m')}.json"
    )
    if cache_file.exists() and not REFRESH_NEWS_CACHE:
        with open(cache_file, encoding="utf-8") as fh:
            cached = json.load(fh)
        total, days = _count_news(cached)
        _news_debug(
            f"[news-cache] AlphaVantage {ticker}: {total} articulos en {days} dias desde {cache_file}"
        )
        return cached
    if cache_file.exists() and REFRESH_NEWS_CACHE:
        _news_debug(
            f"[news-cache] AlphaVantage {ticker}: ignorando cache por --refresh-news-cache"
        )
    if not ALPHAVANTAGE_API_KEY:
        _news_debug(f"[AlphaVantage] {ticker}: sin ALPHAVANTAGE_API_KEY, fuente omitida")
        return {}

    start_ts = start_d.strftime("%Y%m%dT0000")
    end_ts = (end_d + timedelta(days=1)).strftime("%Y%m%dT0000")
    try:
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "NEWS_SENTIMENT",
                "tickers": ticker,
                "time_from": start_ts,
                "time_to": end_ts,
                "sort": "RELEVANCE",
                "limit": 1000,
                "apikey": ALPHAVANTAGE_API_KEY,
            },
            timeout=30,
        )
        data = resp.json() if resp.text.strip() else {}
    except Exception as exc:
        logger.warning(f"[AlphaVantage] {ticker}: error consultando noticias: {exc}")
        return {}

    if "Information" in data or "Note" in data:
        logger.warning(
            f"[AlphaVantage] {ticker}: limite/API respuesta={data.get('Information') or data.get('Note')}"
        )
        return {}

    news_by_date: Dict[str, List] = {}
    for item in data.get("feed", []) or []:
        headline = (item.get("title") or "").strip()
        if not headline:
            continue
        raw_time = item.get("time_published", "")
        try:
            dt = datetime.strptime(raw_time[:13], "%Y%m%dT%H%M")
            date_str = dt.strftime("%Y-%m-%d")
            dt_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            date_str = start_d.isoformat()
            dt_str = raw_time or date_str
        source_name = item.get("source") or "alpha_vantage"
        news_by_date.setdefault(date_str, []).append(
            {
                "headline": headline,
                "url": item.get("url", ""),
                "source": f"alphavantage:{source_name}",
                "datetime": dt_str,
                "summary": item.get("summary", ""),
            }
        )

    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(news_by_date, fh)
    total, days = _count_news(news_by_date)
    logger.info(f"[AlphaVantage] {ticker}: {total} articulos en {days} dias -> cacheado")
    if DEBUG_NEWS:
        for d in sorted(news_by_date):
            logger.info(
                f"[AlphaVantage] {ticker} {d}: {len(news_by_date[d])} "
                f"samples={_headline_samples(news_by_date[d])}"
            )
    return news_by_date


def _fetch_alpha_vantage_macro_month(year: int, month: int) -> Dict[str, List[Dict]]:
    """
    Descarga noticias macro de Alpha Vantage para un mes completo y las indexa por día.
    Una sola llamada mensual: 1 request/mes en lugar de 1 request/día.
    Cache: alphavantage_macro_YYYY-MM.json  (formato {date_str: [articles]}).
    """
    month_key = f"{year:04d}-{month:02d}"
    cache_file = CACHE_DIR / "news" / f"alphavantage_macro_{month_key}.json"

    if cache_file.exists() and not REFRESH_NEWS_CACHE:
        with open(cache_file, encoding="utf-8") as fh:
            cached = json.load(fh)
        total = sum(len(v) for v in cached.values())
        _news_debug(
            f"[news-cache] AlphaVantage macro {month_key}: {total} articulos desde cache"
        )
        return cached
    if cache_file.exists() and REFRESH_NEWS_CACHE:
        _news_debug(f"[news-cache] AlphaVantage macro {month_key}: ignorando cache")
    if not ALPHAVANTAGE_API_KEY:
        _news_debug(f"[AlphaVantage macro] {month_key}: sin ALPHAVANTAGE_API_KEY")
        return {}

    import calendar as _cal
    last_day = _cal.monthrange(year, month)[1]
    start_ts = f"{year:04d}{month:02d}01T0000"
    end_ts = f"{year:04d}{month:02d}{last_day:02d}T2359"

    try:
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "NEWS_SENTIMENT",
                "topics": "economy_macro,economy_monetary,financial_markets",
                "time_from": start_ts,
                "time_to": end_ts,
                "sort": "LATEST",
                "limit": 1000,
                "apikey": ALPHAVANTAGE_API_KEY,
            },
            timeout=30,
        )
        data = resp.json() if resp.text.strip() else {}
    except Exception as exc:
        logger.warning(f"[AlphaVantage macro] {month_key}: error: {exc}")
        return {}

    if "Information" in data or "Note" in data:
        logger.warning(
            f"[AlphaVantage macro] {month_key}: rate limit/API: "
            f"{data.get('Information') or data.get('Note')}"
        )
        return {}

    by_day: Dict[str, List[Dict]] = {}
    seen: set = set()
    for item in data.get("feed", []) or []:
        headline = (item.get("title") or "").strip()
        url = item.get("url", "")
        if not headline:
            continue
        fp = _fingerprint(url, headline)
        if fp in seen:
            continue
        seen.add(fp)
        raw_time = item.get("time_published", "")
        try:
            dt = datetime.strptime(raw_time[:13], "%Y%m%dT%H%M")
            date_str = dt.strftime("%Y-%m-%d")
            dt_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            date_str = f"{year:04d}-{month:02d}-01"
            dt_str = raw_time or date_str
        source_name = item.get("source") or "alpha_vantage"
        by_day.setdefault(date_str, []).append(
            _normalize_macro(
                headline,
                url,
                f"alphavantage:{source_name}",
                dt_str,
                "macro",
                "alphavantage_topics",
                item.get("summary", ""),
            )
        )

    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(by_day, fh)
    total = sum(len(v) for v in by_day.values())
    logger.info(
        f"[AlphaVantage macro] {month_key}: {total} articulos en {len(by_day)} dias -> cacheado"
    )
    return by_day


# Cache en memoria para el mes actual: evita re-leer disco en cada día del mismo mes.
_av_macro_month_cache: Dict[str, Dict[str, List[Dict]]] = {}


def fetch_alpha_vantage_macro_news(date_str: str) -> List[Dict]:
    """
    Devuelve noticias macro de Alpha Vantage para date_str.
    Usa cache mensual en disco (1 request/mes) más cache en memoria para el loop diario.
    """
    global _av_macro_month_cache
    target_date = pd.to_datetime(date_str).date()
    month_key = f"{target_date.year:04d}-{target_date.month:02d}"

    if month_key not in _av_macro_month_cache:
        _av_macro_month_cache[month_key] = _fetch_alpha_vantage_macro_month(
            target_date.year, target_date.month
        )

    articles = _av_macro_month_cache[month_key].get(date_str, [])
    _news_debug(
        f"[AlphaVantage macro] {date_str}: {len(articles)} articulos del cache mensual {month_key}"
    )
    if DEBUG_NEWS and articles:
        logger.info(
            f"[AlphaVantage macro] {date_str}: samples={_headline_samples(articles)}"
        )
    return articles


def fetch_newsapi_ticker_news_historical(
    ticker: str, start_d: date, end_d: date
) -> Dict[str, List]:
    """
    Pre-fetcha artículos de NewsAPI para un ticker concreto sobre todo el rango del backtest.
    Usa una sola llamada por query (con from/to del rango completo) y distribuye por fecha.
    Cacheado en disco por rango para evitar re-llamadas.

    Plan Developer gratuito: 100 req/día, búsqueda hasta ~30 días atrás.
    Plan de pago: histórico ampliado (NEWSAPI_UNLIMITED_HISTORY=1).

    Retorna {date_str: [article_dict, ...]} en el mismo formato que fetch_news_historical().
    """
    if not NEWSAPI_KEY:
        _news_debug(f"[NewsAPI ticker] {ticker}: NEWSAPI_KEY no configurada — omitiendo")
        return {}

    clamped = _newsapi_clamp_range(start_d, end_d, label=f"NewsAPI ticker {ticker}")
    if clamped is None:
        return {}
    eff_start, eff_end = clamped

    cache_file = (
        CACHE_DIR
        / "news"
        / f"newsapi_{ticker}_{start_d.strftime('%Y%m')}_{end_d.strftime('%Y%m')}.json"
    )
    if cache_file.exists() and not REFRESH_NEWS_CACHE:
        with open(cache_file, encoding="utf-8") as fh:
            cached = json.load(fh)
        total, days = _count_news(cached)
        _news_debug(
            f"[news-cache] NewsAPI {ticker}: {total} articulos en {days} dias desde {cache_file}"
        )
        return cached
    if cache_file.exists() and REFRESH_NEWS_CACHE:
        _news_debug(f"[news-cache] NewsAPI {ticker}: ignorando cache por --refresh-news-cache")

    queries = TICKER_NEWSAPI_QUERIES.get(ticker, [ticker])
    news_by_date: Dict[str, List] = {}
    seen_global: set = set()

    logger.info(
        f"[NewsAPI ticker] Pre-fetching {ticker} ({len(queries)} queries, rango {eff_start}→{eff_end})…"
    )

    for query in queries:
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": query,
                    "from": eff_start.isoformat(),
                    "to": eff_end.isoformat(),
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 100,
                    "apiKey": NEWSAPI_KEY,
                },
                timeout=20,
            )
            if resp.status_code == 426:
                logger.warning(
                    f"[NewsAPI ticker] {ticker} query='{query}': fecha fuera de ventana del plan "
                    f"(free ≈{NEWSAPI_HISTORY_DAYS} días). Usa Finnhub/AV o plan de pago NewsAPI."
                )
                continue
            if resp.status_code == 429:
                logger.warning(f"[NewsAPI ticker] {ticker}: rate limit (100 req/día en free tier)")
                break
            if resp.status_code != 200:
                logger.warning(
                    f"[NewsAPI ticker] {ticker}: HTTP {resp.status_code} query='{query}'"
                )
                continue

            data = resp.json()
            if data.get("status") != "ok":
                msg = data.get("message", "")
                if "maximumResultsReached" not in msg:
                    logger.warning(f"[NewsAPI ticker] {ticker} query='{query}': {msg}")
                continue

            for art in data.get("articles", []):
                title = (art.get("title") or "").strip()
                url = art.get("url", "")
                if not title or title == "[Removed]":
                    continue
                fp = _fingerprint(url, title)
                if fp in seen_global:
                    continue
                seen_global.add(fp)

                pub_raw = art.get("publishedAt", "")
                try:
                    pub_dt = datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
                    date_str = pub_dt.strftime("%Y-%m-%d")
                except Exception:
                    date_str = start_d.isoformat()

                entry = {
                    "headline": title,
                    "url": url,
                    "source": (art.get("source") or {}).get("name", "newsapi"),
                    "datetime": pub_raw,
                    "summary": (art.get("description") or "")[:300],
                }
                news_by_date.setdefault(date_str, []).append(entry)

        except Exception as e:
            logger.warning(f"[NewsAPI ticker] {ticker} query='{query}': {e}")

    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(news_by_date, fh)

    total_arts = sum(len(v) for v in news_by_date.values())
    logger.info(
        f"[NewsAPI ticker] {ticker}: {total_arts} artículos en {len(news_by_date)} días → cacheado"
    )
    return news_by_date


def fetch_yfinance_ticker_news(ticker: str, target_date_str: str) -> List[Dict]:
    """
    Obtiene noticias recientes de Yahoo Finance para un ticker.
    IMPORTANTE: yfinance solo devuelve noticias del feed actual (~últimos 7-14 días),
    no tiene acceso histórico real. En backtesting solo aportará artículos cuando
    target_date_str coincida con el período de ejecución (últimos días del rango).
    En producción (pipeline diario) esta función es muy valiosa.
    """
    try:
        raw = yf.Ticker(ticker).news or []
        articles = []
        for item in raw:
            # yfinance ≥0.2.x anida el contenido en item["content"]
            content = item.get("content", item)
            headline = (content.get("title") or item.get("title") or "").strip()
            if not headline:
                continue
            url_data = content.get("canonicalUrl") or {}
            url = (
                url_data.get("url") if isinstance(url_data, dict) else ""
            ) or item.get("link", "")
            provider = content.get("provider") or {}
            source = (
                provider.get("displayName") if isinstance(provider, dict) else ""
            ) or "yahoo_finance"
            pub_date = content.get("pubDate") or item.get("providerPublishTime") or ""

            # Normalizar fecha de publicación
            pub_str = ""
            if isinstance(pub_date, (int, float)):
                pub_str = datetime.utcfromtimestamp(pub_date).strftime("%Y-%m-%d")
            elif isinstance(pub_date, str) and pub_date:
                pub_str = pub_date[:10]

            # Solo incluir si la fecha coincide con el día objetivo
            if pub_str and pub_str != target_date_str:
                continue

            articles.append(
                {
                    "headline": headline,
                    "url": url,
                    "source": source,
                    "datetime": (
                        pub_date if isinstance(pub_date, str) else str(pub_date)
                    ),
                    "summary": "",
                }
            )
        return articles
    except Exception as e:
        logger.debug(f"[YFinance news] {ticker}: {e}")
        return []


def merge_ticker_articles(
    finnhub_arts: List[Dict],
    alphavantage_arts: List[Dict],
    newsapi_arts: List[Dict],
    yfinance_arts: List[Dict],
) -> List[Dict]:
    """
    Combina artículos de las 3 fuentes con deduplicación por fingerprint.
    Orden de prioridad: Finnhub → AlphaVantage → NewsAPI → YFinance.
    """
    seen_fps: set = set()
    seen_titles: set = set()
    merged: List[Dict] = []

    for art in finnhub_arts + alphavantage_arts + newsapi_arts + yfinance_arts:
        headline = art.get("headline", "").strip()
        url = art.get("url", "")
        if not headline:
            continue
        fp = _fingerprint(url, headline)
        title = headline.lower()
        if fp in seen_fps or title in seen_titles:
            continue
        seen_fps.add(fp)
        seen_titles.add(title)
        merged.append(art)

    return merged


def _get_ticker_articles_for_day(
    source_by_day: Dict[str, List[Dict]],
    target_date_str: str,
    lookback_days: int = 0,
) -> Tuple[List[Dict], int]:
    """
    Devuelve articulos para el dia objetivo; si no hay, busca hacia atras
    hasta lookback_days para mejorar cobertura en historico.
    Retorna (articles, age_days), donde age_days=0 significa fecha exacta.
    """
    if not source_by_day:
        return [], -1

    direct = source_by_day.get(target_date_str, [])
    if direct:
        return direct, 0

    if lookback_days <= 0:
        return [], -1

    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    except Exception:
        return [], -1

    for age in range(1, lookback_days + 1):
        prior = (target_date - timedelta(days=age)).strftime("%Y-%m-%d")
        arts = source_by_day.get(prior, [])
        if arts:
            return arts, age

    return [], -1


def fetch_vix_historical(start_d: date, end_d: date) -> pd.Series:
    df = yf.download(
        "^VIX",
        start=str(start_d - timedelta(days=5)),
        end=str(end_d + timedelta(days=1)),
        progress=False,
        repair=False,
    )
    if not df.empty:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df.index = pd.to_datetime(df.index)
        return df["Close"]
    return pd.Series(dtype=float)


def fetch_newsapi_macro_articles(query: str, target_date: date, n: int = 10) -> List[Dict]:
    """
    Obtiene artículos macro de NewsAPI para un query y fecha concreta (±1 día).
    Usado por ingest_macro_news; sin caché propia (la caché mensual va en _fetch_newsapi_macro_month).
    """
    if not NEWSAPI_KEY:
        return []
    from_dt = (target_date - timedelta(days=1)).isoformat()
    to_dt   = (target_date + timedelta(days=1)).isoformat()
    try:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": query,
                "from": from_dt,
                "to": to_dt,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": n,
                "apiKey": NEWSAPI_KEY,
            },
            timeout=20,
        )
        if resp.status_code == 426:
            _news_debug(
                f"[NewsAPI macro] fecha fuera de ventana free (≈{NEWSAPI_HISTORY_DAYS}d) q='{query}'"
            )
            return []
        if resp.status_code != 200:
            _news_debug(f"[NewsAPI macro] HTTP {resp.status_code} query='{query}'")
            return []
        data = resp.json()
        if data.get("status") != "ok":
            return []
        results = []
        for art in data.get("articles", []):
            title = (art.get("title") or "").strip()
            if not title or title == "[Removed]":
                continue
            results.append({
                "title": title,
                "url": art.get("url", ""),
                "domain": (art.get("source") or {}).get("name", "newsapi"),
                "seendate": art.get("publishedAt", ""),
                "summary": (art.get("description") or "")[:300],
            })
        return results
    except Exception as e:
        _news_debug(f"[NewsAPI macro] error query='{query}': {e}")
        return []


# ── Caché mensual NewsAPI macro (misma lógica que AV para respetar rate limits) ─
_newsapi_macro_month_cache: Dict[str, Dict[str, List[Dict]]] = {}


def _fetch_newsapi_macro_month(year: int, month: int) -> Dict[str, List[Dict]]:
    """
    Descarga un mes completo de noticias macro de NewsAPI en una sola llamada por query.
    Cachea el resultado en disco como newsapi_macro_YYYY-MM.json.
    """
    import calendar
    month_key = f"{year:04d}-{month:02d}"
    cache_file = CACHE_DIR / "news" / f"newsapi_macro_{month_key}.json"

    if cache_file.exists() and not REFRESH_NEWS_CACHE:
        with open(cache_file, encoding="utf-8") as fh:
            cached = json.load(fh)
        _news_debug(f"[news-cache] NewsAPI macro {month_key}: cargado desde {cache_file}")
        return cached

    if not NEWSAPI_KEY:
        _news_debug(f"[NewsAPI macro] {month_key}: NEWSAPI_KEY no configurada")
        return {}

    first_day = date(year, month, 1)
    last_day  = date(year, month, calendar.monthrange(year, month)[1])
    clamped = _newsapi_clamp_range(first_day, last_day, label=f"NewsAPI macro {month_key}")
    if clamped is None:
        return {}
    first_day, last_day = clamped

    by_date: Dict[str, List[Dict]] = {}

    for cat, queries in MACRO_NEWSAPI_QUERIES.items():
        for query in queries:
            try:
                resp = requests.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "q": query,
                        "from": first_day.isoformat(),
                        "to": last_day.isoformat(),
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 100,
                        "apiKey": NEWSAPI_KEY,
                    },
                    timeout=25,
                )
                if resp.status_code == 426:
                    logger.warning(
                        f"[NewsAPI macro] {month_key}: fuera de ventana del plan free "
                        f"(≈{NEWSAPI_HISTORY_DAYS} días). Finnhub/AV cubren el resto."
                    )
                    continue
                if resp.status_code == 429:
                    logger.warning(f"[NewsAPI macro] {month_key}: rate limit alcanzado")
                    break
                if resp.status_code != 200:
                    _news_debug(f"[NewsAPI macro] {month_key}: HTTP {resp.status_code} q='{query}'")
                    continue
                data = resp.json()
                if data.get("status") != "ok":
                    info = data.get("message", "")
                    _news_debug(f"[NewsAPI macro] {month_key} q='{query}': {info}")
                    continue
                for art in data.get("articles", []):
                    title = (art.get("title") or "").strip()
                    if not title or title == "[Removed]":
                        continue
                    pub_raw = art.get("publishedAt", "")
                    try:
                        pub_dt = datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
                        ds = pub_dt.strftime("%Y-%m-%d")
                    except Exception:
                        ds = first_day.isoformat()
                    by_date.setdefault(ds, []).append(_normalize_macro(
                        title,
                        art.get("url", ""),
                        (art.get("source") or {}).get("name", "newsapi"),
                        pub_raw,
                        cat,
                        query,
                        (art.get("description") or "")[:300],
                    ))
            except Exception as e:
                _news_debug(f"[NewsAPI macro] {month_key} q='{query}': {e}")

    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(by_date, fh)

    total = sum(len(v) for v in by_date.values())
    logger.info(f"[NewsAPI macro] {month_key}: {total} artículos en {len(by_date)} días → cacheado")
    return by_date


def fetch_newsapi_macro_news(date_str: str) -> List[Dict]:
    """Retorna artículos macro de NewsAPI para date_str usando caché mensual."""
    global _newsapi_macro_month_cache
    target_date = pd.to_datetime(date_str).date()
    month_key = f"{target_date.year:04d}-{target_date.month:02d}"

    if month_key not in _newsapi_macro_month_cache:
        _newsapi_macro_month_cache[month_key] = _fetch_newsapi_macro_month(
            target_date.year, target_date.month
        )

    articles = _newsapi_macro_month_cache[month_key].get(date_str, [])
    _news_debug(f"[NewsAPI macro] {date_str}: {len(articles)} artículos")
    return articles


def ingest_macro_news(date_str):
    articles = []
    seen = set()
    target_date = pd.to_datetime(date_str).date()

    # ── Alpha Vantage: macro histórica mensual cacheada ──────────────────────
    for art in fetch_alpha_vantage_macro_news(date_str):
        fp = _fingerprint(art.get("url", ""), art.get("headline", ""))
        if fp in seen:
            continue
        seen.add(fp)
        articles.append(art)

    # ── NewsAPI: macro mensual cacheada (requiere NEWSAPI_KEY) ────────────────
    for art in fetch_newsapi_macro_news(date_str):
        fp = _fingerprint(art.get("url", ""), art.get("headline", ""))
        if fp in seen:
            continue
        seen.add(fp)
        articles.append(art)

    # ── RSS en tiempo real (complemento para ejecuciones live del mismo día) ──
    for name, url in RSS_FEEDS.items():
        try:
            parsed = feedparser.parse(url)
            for entry in parsed.entries[:5]:
                # Filtrar por fecha RSS
                pub = entry.get("published_parsed")
                if pub:
                    pub_date = date(*pub[:3])
                    if abs((pub_date - target_date).days) > 1:
                        continue

                fp = _fingerprint(entry.get("link"), entry.get("title"))
                if fp not in seen:
                    seen.add(fp)
                    summary = entry.get("summary", "") or entry.get("description", "")
                    articles.append(
                        _normalize_macro(
                            entry.get("title"),
                            entry.get("link"),
                            name,
                            datetime.strptime(date_str, "%Y-%m-%d").isoformat(),
                            "macro",
                            name,
                            summary,
                        )
                    )
        except Exception:
            pass

    if articles:
        upsert_macro_news(date_str, articles)
    else:
        logger.info(
            f"[{date_str}] No se encontraron noticias macro en rango temporal para {date_str}."
        )


def calculate_indicators_for_date(
    ohlcv_df: pd.DataFrame, target_date: str
) -> Optional[Dict]:
    try:
        import pandas_ta_classic as ta
    except ImportError:
        import pandas_ta as ta

    target_dt = pd.to_datetime(target_date)
    df = ohlcv_df[ohlcv_df.index <= target_dt].copy()
    if len(df) < 50:
        return None

    close  = df["Close"]
    high   = df["High"]  if "High"  in df.columns else close
    low_s  = df["Low"]   if "Low"   in df.columns else close

    rsi    = ta.rsi(close, length=14)
    sma_20 = ta.sma(close, length=20)
    sma_50 = ta.sma(close, length=50)
    sma_200= ta.sma(close, length=200)
    bbands = ta.bbands(close, length=20, std=2)

    # ── Nuevos indicadores técnicos ────────────────────────────────────────────
    # EMA-55: media exponencial de 55 sesiones (feature #1 más predictiva en literatura)
    ema_55 = ta.ema(close, length=55)
    # ADX-14: fuerza de la tendencia (> 25 = tendencia real, < 15 = lateral/ruidoso)
    try:
        adx_df = ta.adx(high, low_s, close, length=14)
        adx_val = _sf(adx_df.iloc[-1, 0]) if adx_df is not None and not adx_df.empty else None
    except Exception:
        adx_val = None
    # Momentum: retorno precio en ventanas de 5 y 20 sesiones
    mom_20d = None
    mom_5d  = None
    if len(close) >= 21:
        p0_20 = float(close.iloc[-21])
        mom_20d = round((float(close.iloc[-1]) - p0_20) / p0_20, 4) if p0_20 != 0 else None
    if len(close) >= 6:
        p0_5  = float(close.iloc[-6])
        mom_5d  = round((float(close.iloc[-1]) - p0_5)  / p0_5,  4) if p0_5  != 0 else None

    def last(s):
        return _sf(s.iloc[-1]) if s is not None and len(s) > 0 else None

    bb_upper = bb_mid = bb_lower = None
    if bbands is not None and not bbands.empty and len(bbands.columns) >= 3:
        bb_lower = _sf(bbands.iloc[-1, 0])
        bb_mid   = _sf(bbands.iloc[-1, 1])
        bb_upper = _sf(bbands.iloc[-1, 2])

    cl  = _sf(close.iloc[-1])
    s20, s50  = last(sma_20), last(sma_50)
    s200      = last(sma_200)
    e55       = last(ema_55)
    sma_spread = round(float(s20) - float(s50), 4) if s20 and s50 else None
    bb_width   = (
        round((float(bb_upper) - float(bb_lower)) / float(cl), 6)
        if bb_upper and bb_lower and cl else None
    )
    # % desviación del precio respecto a EMA-55 (+ = por encima, - = por debajo)
    ema_55_pct = round((float(cl) - float(e55)) / float(e55), 4) if e55 and e55 != 0 and cl else None

    # Drawdown desde máximo histórico disponible en la ventana de datos
    ath = _sf(close.max())
    drawdown_from_ath = round((float(cl) - float(ath)) / float(ath), 4) if cl and ath and ath > 0 else None

    return {
        "close":            cl,
        "rsi_14":           last(rsi),
        "sma_20":           s20,
        "sma_50":           s50,
        "sma_200":          s200,
        "sma_spread":       sma_spread,
        "bb_upper":         bb_upper,
        "bb_middle":        bb_mid,
        "bb_lower":         bb_lower,
        "bb_width":         bb_width,
        "drawdown_from_ath":drawdown_from_ath,
        # Nuevos indicadores
        "ema_55":           e55,
        "ema_55_pct":       ema_55_pct,
        "adx_14":           adx_val,
        "momentum_20d":     mom_20d,
        "momentum_5d":      mom_5d,
    }


def get_bn_model():
    from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork
    from pgmpy.factors.discrete import TabularCPD

    model = BayesianNetwork(
        [
            ("Sentiment", "MarketDirection"),
            ("RSI", "MarketDirection"),
            ("Trend", "MarketDirection"),
            ("Volatility", "MarketDirection"),
        ]
    )
    c = MODEL_CONFIG["cpt_market_direction"]
    p = MODEL_CONFIG["priors"]
    model.add_cpds(
        TabularCPD(
            "Sentiment",
            3,
            [
                [p.get("Sentiment")["bullish"]],
                [p.get("Sentiment")["bearish"]],
                [p.get("Sentiment")["neutral"]],
            ],
            state_names={"Sentiment": ["bullish", "bearish", "neutral"]},
        ),
        TabularCPD(
            "RSI",
            3,
            [[p["RSI"]["oversold"]], [p["RSI"]["neutral"]], [p["RSI"]["overbought"]]],
            state_names={"RSI": ["oversold", "neutral", "overbought"]},
        ),
        TabularCPD(
            "Trend",
            2,
            [[p["Trend"]["uptrend"]], [p["Trend"]["downtrend"]]],
            state_names={"Trend": ["uptrend", "downtrend"]},
        ),
        TabularCPD(
            "Volatility",
            2,
            [[p["Volatility"]["low"]], [p["Volatility"]["high"]]],
            state_names={"Volatility": ["low", "high"]},
        ),
        TabularCPD(
            "MarketDirection",
            2,
            values=[c["values_P_down"], c["values_P_up"]],
            evidence=["Sentiment", "RSI", "Trend", "Volatility"],
            evidence_card=[3, 3, 2, 2],
            state_names={
                "MarketDirection": ["down", "up"],
                "Sentiment": ["bullish", "bearish", "neutral"],
                "RSI": ["oversold", "neutral", "overbought"],
                "Trend": ["uptrend", "downtrend"],
                "Volatility": ["low", "high"],
            },
        ),
    )
    return model


def run_bayesian_inference(
    evidence:      Dict,
    macro_adj:     float,
    macro_context: Optional[Dict] = None,
    extra:         Optional[Dict] = None,
) -> Tuple[str, float]:
    """
    Inferencia de señal. Usa el motor discriminativo (Camino B) si está disponible;
    en caso contrario cae a la Red Bayesiana (Camino A / fallback).

    extra puede contener features adicionales para el discriminador:
        prob_up_bn, signal_streak, rsi_continuous, adx_14, ema_55_pct,
        momentum_20d, momentum_5d, vol_20d, vol_ratio, sentiment_dispersion
    """
    # ── Camino B: LightGBM discriminativo ─────────────────────────────────────
    if _disc_engine is not None and getattr(_disc_engine, "available", False):
        try:
            mc = macro_context or {"macro_adjustment": macro_adj}
            if "macro_adjustment" not in mc:
                mc = dict(mc); mc["macro_adjustment"] = macro_adj

            from pgmpy.inference import VariableElimination as _VE
            _result_bn = _VE(get_bn_model()).query(
                variables=["MarketDirection"], evidence=evidence, show_progress=False
            )
            extra_ctx = dict(extra or {})
            extra_ctx["prob_up_bn"] = float(_result_bn.values[1])

            prob_up = _disc_engine.infer(evidence, mc, extra_ctx)

            _DISC_BUY  = 0.55
            _DISC_SELL = 0.50
            signal = (
                "BUY"  if prob_up >= _DISC_BUY  else
                "SELL" if prob_up <= _DISC_SELL else
                "HOLD"
            )
            logger.debug(
                f"[DiscEngine] prob_up={prob_up:.4f} → {signal} "
                f"(buy≥{_DISC_BUY}, sell≤{_DISC_SELL})"
            )
            return signal, prob_up
        except Exception as _exc:
            logger.debug(f"[DiscEngine] fallback a BN: {_exc}")

    # ── Fallback: Red Bayesiana (Camino A) ────────────────────────────────────
    from pgmpy.inference import VariableElimination

    infer  = VariableElimination(get_bn_model())
    result = infer.query(
        variables=["MarketDirection"], evidence=evidence, show_progress=False
    )
    prob_up_raw = float(result.values[1])

    # En tendencia alcista confirmada, amortiguamos el macro_adj negativo al 40%
    effective_macro_adj = macro_adj
    if evidence.get("Trend") == "uptrend" and macro_adj < 0:
        effective_macro_adj = macro_adj * 0.40

    prob_up_adj = round(max(0.0, min(1.0, prob_up_raw + effective_macro_adj)), 4)
    signal = (
        "BUY"  if prob_up_adj >= BUY_THRESHOLD  else
        "SELL" if prob_up_adj <= SELL_THRESHOLD else
        "HOLD"
    )
    return signal, prob_up_adj


def apply_hysteresis_signal(
    raw_signal: str,
    recent_confirmed: list,
    sell_days: int = SELL_CONFIRMATION_DAYS,
) -> Tuple[str, str]:
    """
    Filtro de persistencia (hysteresis).

    Reglas:
    - BUY  → siempre pasa directamente (entrada sin demora).
    - HOLD → siempre pasa directamente (mantener posición).
    - SELL → solo se confirma si los últimos (sell_days-1) signals confirmados
             también fueron SELL. En caso contrario devuelve HOLD para que la
             posición abierta no se cierre por un único día bajista puntual.

    Returns
    -------
    (confirmed_signal, status_str)
        status_str es útil para tracing/debug.
    """
    if raw_signal != "SELL":
        return raw_signal, "pass_through"

    # Cuenta cuántos SELLs consecutivos hay al final del historial (más reciente al final)
    consecutive = 0
    for s in reversed(recent_confirmed):
        if s == "SELL":
            consecutive += 1
        else:
            break

    if consecutive >= sell_days - 1:
        return "SELL", f"confirmed_{sell_days}d"
    else:
        return "HOLD", f"pending_{consecutive + 1}_of_{sell_days}d"


# =============================================================================
# FASE 1 — PROBABILISTIC EXPOSURE MANAGEMENT
# =============================================================================

def detect_market_regime(
    sma50: Optional[float],
    sma200: Optional[float],
    vix: Optional[float],
    drawdown_from_ath: Optional[float],
) -> str:
    """
    Clasifica el régimen estructural de mercado en 4 estados:
      BULL     → tendencia alcista confirmada (SMA50 > SMA200, sin caída > 20%)
      NEUTRAL  → sin tendencia clara o datos insuficientes
      HIGH_VOL → volatilidad elevada (VIX > 25), mercado nervioso pero no en crash
      BEAR     → caída técnica ≥ 20% desde máximos (bear market)

    El régimen condiciona el floor y ceiling de exposición en prob_to_exposure().
    El orden de prioridad: BEAR > HIGH_VOL > BULL > NEUTRAL.
    """
    # Bear market técnico tiene máxima prioridad (capital preservation)
    if drawdown_from_ath is not None and drawdown_from_ath < -0.20:
        return "BEAR"

    # Alta volatilidad: entorno de riesgo elevado aunque no sea bear market
    if vix is not None and vix > 25:
        return "HIGH_VOL"

    # Tendencia alcista estructural: golden cross SMA50 > SMA200
    if sma50 is not None and sma200 is not None and sma50 > sma200:
        return "BULL"

    # Sin señal clara: régimen neutral por defecto
    return "NEUTRAL"


def prob_to_exposure(prob_up: float, regime: str) -> float:
    """
    Mapea prob_up bayesiana (0–1) a una exposición continua de mercado (0–1)
    condicionada por el régimen estructural.

    Parámetros de régimen (floor / ceiling):
      BULL     → [0.60, 1.00]  Siempre estamos invertidos, máx inversión posible
      NEUTRAL  → [0.35, 0.80]  Rango moderado; la probabilidad mueve el needle
      HIGH_VOL → [0.20, 0.60]  Reducción defensiva; máx 60% en períodos volátiles
      BEAR     → [0.10, 0.45]  Capital preservation; mínima exposición estructural

    La interpolación lineal usa como rango de referencia [0.30, 0.75] de prob_up:
      - prob_up ≤ 0.30 → floor del régimen
      - prob_up ≥ 0.75 → ceiling del régimen
      - valores intermedios → interpolación lineal dentro de [floor, ceiling]
    """
    FLOORS   = {"BULL": 0.60, "NEUTRAL": 0.35, "HIGH_VOL": 0.20, "BEAR": 0.10}
    CEILINGS = {"BULL": 1.00, "NEUTRAL": 0.80, "HIGH_VOL": 0.60, "BEAR": 0.45}
    floor   = FLOORS.get(regime, 0.35)
    ceiling = CEILINGS.get(regime, 0.80)
    # Normalizar prob_up al intervalo [0,1] dentro de [0.30, 0.75]
    t = (prob_up - 0.30) / (0.75 - 0.30)
    t = max(0.0, min(1.0, t))
    return round(floor + t * (ceiling - floor), 3)


def smooth_exposure(target: float, previous: float, alpha: float = 0.25) -> float:
    """
    EWM (Exponential Weighted Moving Average) de la exposición objetivo.

    exposure_smooth_t = α × target_t + (1−α) × smooth_{t-1}

    alpha=0.25 → ventana efectiva ~4 días. Evita cambios bruscos de posición
    ante noticias puntuales manteniendo la dirección estratégica del régimen.
    """
    return round(alpha * target + (1.0 - alpha) * previous, 4)


def _calc_exposure_backtesting(signals_list: List[Dict]) -> Tuple[Dict, Dict]:
    """
    Backtesting de exposición continua (Probabilistic Exposure Management).

    Fórmula: portfolio_return_t = market_return_t × smoothed_exposure_t

    A diferencia del sistema binario (Long/Cash), aquí siempre tenemos cierta
    exposición estructural. El floor del régimen garantiza que nunca estamos
    100% en cash salvo en un bear market extremo.

    Requiere que cada elemento de signals_list tenga:
      - batch_date, ticker, close_price, smoothed_exposure, market_regime
    """
    exp_metrics: Dict = {}
    exp_diagnostics: Dict = {}

    tickers = list(set(r["ticker"] for r in signals_list))
    for ticker in tickers:
        ts = sorted(
            [r for r in signals_list if r["ticker"] == ticker],
            key=lambda x: x["batch_date"],
        )
        if len(ts) < 2:
            continue

        capital = INITIAL_CAP
        equity = [capital]
        daily_rets: List[float] = []
        daily_exposures: List[float] = []
        regime_days: Dict[str, int] = {"BULL": 0, "NEUTRAL": 0, "HIGH_VOL": 0, "BEAR": 0}

        for i in range(1, len(ts)):
            p0 = float(ts[i - 1].get("close_price") or 0)
            p1 = float(ts[i].get("close_price") or 0)
            if p0 == 0 or p1 == 0:
                equity.append(equity[-1])
                continue

            market_ret = (p1 - p0) / p0
            exposure = float(ts[i].get("smoothed_exposure", 0.5))
            daily_exposures.append(exposure)

            regime = ts[i].get("market_regime", "NEUTRAL")
            if regime in regime_days:
                regime_days[regime] += 1

            portfolio_ret = market_ret * exposure
            capital *= 1.0 + portfolio_ret
            equity.append(capital)
            daily_rets.append(portfolio_ret)

        final_eq = capital
        cum_ret = (final_eq - INITIAL_CAP) / INITIAL_CAP

        if len(equity) > 2:
            eq_arr = np.array(equity)
            dr = np.diff(eq_arr) / eq_arr[:-1]
            excess = dr - (RISK_FREE_RATE / 252)
            std = np.std(excess)
            sharpe = float(np.mean(excess) / std * np.sqrt(252)) if std > 1e-6 else 0.0
            peak = np.maximum.accumulate(eq_arr)
            max_dd = float(np.min((eq_arr - peak) / peak))
        else:
            sharpe = max_dd = 0.0

        exp_metrics[ticker] = {
            "cumulative_return": round(float(cum_ret), 4),
            "sharpe_ratio": round(float(sharpe), 4),
            "max_drawdown": round(float(max_dd), 4),
            "final_equity": round(float(final_eq), 2),
        }
        exp_diagnostics[ticker] = {
            "avg_exposure": round(float(np.mean(daily_exposures)), 4) if daily_exposures else 0.5,
            "min_exposure": round(float(np.min(daily_exposures)), 4) if daily_exposures else 0.0,
            "max_exposure": round(float(np.max(daily_exposures)), 4) if daily_exposures else 1.0,
            "regime_distribution": regime_days,
        }

    return exp_metrics, exp_diagnostics


def _calc_backtesting(signals_df: pd.DataFrame) -> Tuple[Dict, Dict]:
    metrics, diagnostics = {}, {}
    if signals_df.empty:
        logger.warning("No hay señales para calcular métricas de backtesting.")
        return metrics, diagnostics

    for ticker in signals_df["ticker"].unique():
        ts = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        capital = INITIAL_CAP
        equity = [capital]

        current_position = 1
        if len(ts) > 0 and pd.notna(ts.iloc[0]["close_price"]):
            entry_p = float(ts.iloc[0]["close_price"])
        else:
            entry_p = 0.0
            current_position = 0

        trades_rets = []
        days_invested = 0
        signals_count = ts["signal"].value_counts().to_dict()

        for _, row in ts.iterrows():
            price = float(row["close_price"]) if row["close_price"] else 0.0
            if price == 0:
                continue
            sig = row["signal"]

            if sig == "BUY" and current_position == 0:
                current_position = 1
                entry_p = price
            elif sig == "SELL" and current_position == 1:
                # Solo SELL cierra la posición; HOLD la mantiene abierta
                ret = (price - entry_p) / entry_p
                capital *= 1 + ret
                trades_rets.append(float(ret))
                current_position = 0
            # HOLD: no hacer nada — se mantiene la posición actual

            if current_position == 1:
                days_invested += 1

            daily_eq = (
                capital * (1 + (price - entry_p) / entry_p)
                if current_position == 1 and entry_p > 0
                else capital
            )
            equity.append(daily_eq)

        final_eq = capital
        if current_position == 1 and entry_p > 0:
            last_p = float(ts.iloc[-1]["close_price"])
            final_eq = capital * (1 + (last_p - entry_p) / entry_p)

        cum_ret = (final_eq - INITIAL_CAP) / INITIAL_CAP
        if len(equity) > 2:
            dr = np.diff(equity) / np.array(equity[:-1])
            excess = dr - (RISK_FREE_RATE / 252)
            std = np.std(excess)
            sharpe = float(np.mean(excess) / std * np.sqrt(252)) if std > 1e-6 else 0.0
            peak = np.maximum.accumulate(equity)
            max_dd = float(np.min((np.array(equity) - peak) / peak))
        else:
            sharpe = max_dd = 0.0

        metrics[ticker] = {
            "cumulative_return": round(float(cum_ret), 4),
            "sharpe_ratio": round(float(sharpe), 4),
            "max_drawdown": round(float(max_dd), 4),
            "final_equity": round(float(final_eq), 2),
        }

        wins = sum(1 for value in trades_rets if value > 0)
        gross_profit = sum(value for value in trades_rets if value > 0)
        gross_loss = abs(sum(value for value in trades_rets if value < 0))
        profit_factor = (
            (gross_profit / gross_loss)
            if gross_loss > 1e-9
            else (gross_profit if gross_profit > 0 else 0.0)
        )

        diagnostics[ticker] = {
            "signals": {
                "BUY": int(signals_count.get("BUY", 0)),
                "SELL": int(signals_count.get("SELL", 0)),
                "HOLD": int(signals_count.get("HOLD", 0)),
            },
            "trades_closed": len(trades_rets),
            "win_rate": (
                round(float(wins / len(trades_rets)), 4) if trades_rets else 0.0
            ),
            "avg_trade_return": (
                round(float(np.mean(trades_rets)), 4) if trades_rets else 0.0
            ),
            "profit_factor": round(float(profit_factor), 4),
            "time_in_market_ratio": round(float(days_invested / max(len(ts), 1)), 4),
        }
    return metrics, diagnostics


def get_close_price(ticker: str, date_str: str) -> Optional[float]:
    try:
        target = pd.to_datetime(date_str).date()
        if target > datetime.now().date():
            return None
        start = target - timedelta(days=1)
        end = target + timedelta(days=6)
        df = yf.download(ticker, start=start, end=end, progress=False, repair=False)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df.index = pd.to_datetime(df.index).date
        candidates = [d for d in df.index if d >= target]
        if not candidates:
            return None
        row = df.loc[min(candidates)]
        close = row.get("Close") or row.get("close")
        return float(close) if close else None
    except Exception:
        return None


def update_signal_outcomes_historical(conn, signals_list):
    logger.info("🔄 Procesando outcomes históricos (D0 Insert + D+1, D+3, D+5)...")
    cursor = conn.cursor()
    price_cache = {}

    for sig in tqdm(signals_list, desc="Outcomes", unit="señal"):
        ticker = sig["ticker"]
        d0_str = sig["batch_date"]
        d0 = pd.to_datetime(d0_str)
        p0 = sig["close_price"]

        cursor.execute(
            """
            INSERT INTO signal_outcomes 
                (batch_date, ticker, run_id, signal, prob_up, prob_down,
                 sentiment_state, rsi_state, trend_state, volatility_state, price_d0,
                 macro_sentiment, risk_regime, macro_adjustment) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                signal = EXCLUDED.signal, prob_up = EXCLUDED.prob_up, prob_down = EXCLUDED.prob_down,
                sentiment_state = EXCLUDED.sentiment_state, rsi_state = EXCLUDED.rsi_state,
                trend_state = EXCLUDED.trend_state, volatility_state = EXCLUDED.volatility_state,
                price_d0 = EXCLUDED.price_d0, macro_sentiment = EXCLUDED.macro_sentiment,
                risk_regime = EXCLUDED.risk_regime, macro_adjustment = EXCLUDED.macro_adjustment,
                updated_at = CURRENT_TIMESTAMP
        """,
            (
                d0_str,
                ticker,
                sig["run_id"],
                sig["signal"],
                sig["prob_up"],
                sig["prob_down"],
                sig["sentiment_state"],
                sig["rsi_state"],
                sig["trend_state"],
                sig["volatility_state"],
                float(p0) if p0 else None,
                sig["macro_sentiment"],
                sig["risk_regime"],
                sig["macro_adjustment"],
            ),
        )

        for days, col_p, col_o, col_correct in [
            (1, "price_d1", "outcome_d1", "correct_d1"),
            (3, "price_d3", "outcome_d3", "correct_d3"),
            (5, "price_d5", "outcome_d5", "correct_d5"),
        ]:
            target_date = (d0 + timedelta(days=days)).strftime("%Y-%m-%d")

            if (ticker, target_date) not in price_cache:
                price_cache[(ticker, target_date)] = get_close_price(
                    ticker, target_date
                )
            price_dn = price_cache[(ticker, target_date)]

            if price_dn and p0 and p0 > 0:
                change = (price_dn - p0) / p0
                outcome = (
                    "UP" if change > 0.005 else ("DOWN" if change < -0.005 else "FLAT")
                )
                correct = (
                    (sig["signal"] == "BUY" and outcome == "UP")
                    or (sig["signal"] == "SELL" and outcome == "DOWN")
                    or (sig["signal"] == "HOLD" and outcome == "FLAT")
                )

                cursor.execute(
                    f"""
                    UPDATE signal_outcomes 
                    SET {col_p} = %s, {col_o} = %s, {col_correct} = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE batch_date = %s AND ticker = %s
                """,
                    (float(price_dn), outcome, correct, d0_str, ticker),
                )

    conn.commit()
    cursor.close()
    logger.info("✅ Outcomes históricos actualizados.")


def get_pipeline_health(connection, report_date, run_id):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT tickers_processed, status FROM batch_log WHERE run_id = %s LIMIT 1",
        (run_id,),
    )
    batch_row = cursor.fetchone()
    if not batch_row:
        cursor.execute(
            "SELECT tickers_processed, status FROM batch_log WHERE batch_date = %s ORDER BY updated_at DESC LIMIT 1",
            (report_date,),
        )
        batch_row = cursor.fetchone()
    cursor.execute(
        "SELECT COUNT(DISTINCT ticker) FROM technical_indicators WHERE batch_date = %s",
        (report_date,),
    )
    indicator_tickers = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(DISTINCT ticker) FROM trading_signals WHERE batch_date = %s",
        (report_date,),
    )
    signal_tickers = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM sentiment_scores WHERE batch_date = %s", (report_date,)
    )
    headlines = cursor.fetchone()[0]
    cursor.execute(
        "SELECT stage, metrics FROM pipeline_kpis WHERE run_id = %s", (run_id,)
    )
    stage_metrics = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    tickers_expected = (
        int(batch_row[0]) if batch_row and batch_row[0] is not None else 0
    )
    return {
        "batch_status": batch_row[1] if batch_row else "UNKNOWN",
        "tickers_expected": tickers_expected,
        "tickers_with_indicators": int(indicator_tickers or 0),
        "tickers_with_signals": int(signal_tickers or 0),
        "headlines_scored": int(headlines or 0),
        "coverage_ratio": (
            round(float((signal_tickers or 0) / tickers_expected), 4)
            if tickers_expected
            else 0.0
        ),
        "stage_kpis": stage_metrics,
    }


def get_explanations_sample(connection, report_date, limit=10):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT e.ticker, ts.signal, ts.prob_up, ts.prob_down, e.sentiment_state, e.rsi_state, e.trend_state, e.volatility_state
        FROM signal_explanations e JOIN trading_signals ts ON ts.batch_date = e.batch_date AND ts.ticker = e.ticker
        WHERE e.batch_date = %s ORDER BY ts.prob_up DESC LIMIT %s
    """,
        (report_date, limit),
    )
    rows = cursor.fetchall()
    cursor.close()
    return [
        {
            "ticker": r[0],
            "signal": r[1],
            "prob_up": round(float(r[2]), 4) if r[2] is not None else None,
            "prob_down": round(float(r[3]), 4) if r[3] is not None else None,
            "evidence": {
                "sentiment": r[4],
                "rsi": r[5],
                "trend": r[6],
                "volatility": r[7],
            },
        }
        for r in rows
    ]


def compute_benchmark(signals_df):
    benchmark = {}
    for ticker in signals_df["ticker"].unique():
        ticker_df = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        if ticker_df.empty:
            continue
        first_price = (
            float(ticker_df.iloc[0]["close_price"])
            if ticker_df.iloc[0]["close_price"]
            else 0.0
        )
        last_price = (
            float(ticker_df.iloc[-1]["close_price"])
            if ticker_df.iloc[-1]["close_price"]
            else 0.0
        )
        buy_hold_return = (
            ((last_price - first_price) / first_price) if first_price > 0 else 0.0
        )
        benchmark[ticker] = round(float(buy_hold_return), 4)
    return benchmark


# =============================================================================
# 4b-aux. NARRATIVA DE SEÑAL
# =============================================================================

def _build_signal_narrative(
    ticker: str,
    evidence: Dict,
    rsi_state_ext: str,
    adx_state: str,
    momentum_20d: Optional[float],
    momentum_crowding: bool,
    prob_up: float,
    market_regime: str,
    smoothed_exposure: float,
    exposure_delta: float,
    exposure_recommendation: str,
    conviction_label: str,
    effects: Dict,
    macro_sentiment: str,
    risk_regime: str,
) -> str:
    """
    Genera una narrativa accionable en lenguaje natural basada en todos los indicadores.
    Sustituye el texto plano 'HOLD' por una descripción útil de la situación.
    """
    lines = []

    # ── Exposición actual ──────────────────────────────────────────────────────
    exp_pct = round(smoothed_exposure * 100, 1)
    delta_pct = round(exposure_delta * 100, 1)
    delta_str = f"+{delta_pct}%" if delta_pct > 0 else f"{delta_pct}%"
    rec_map = {
        "INCREASE_STRONG": "↑↑ Aumentar agresivamente",
        "INCREASE_MILD":   "↑  Aumentar moderadamente",
        "MAINTAIN":        "→  Mantener posición",
        "REDUCE_MILD":     "↓  Reducir moderadamente",
        "REDUCE_STRONG":   "↓↓ Reducir significativamente",
    }
    lines.append(
        f"Exposición: {exp_pct}% ({delta_str} respecto ayer) | "
        f"Régimen: {market_regime} | Convicción: {conviction_label.upper()}"
    )
    lines.append(f"Recomendación: {rec_map.get(exposure_recommendation, exposure_recommendation)}")

    # ── Indicadores técnicos ───────────────────────────────────────────────────
    tech_parts = []
    trend = evidence.get("Trend", "")
    if trend == "uptrend":
        tech_parts.append("✅ Tendencia alcista (SMA20 > SMA50)")
    else:
        tech_parts.append("⚠️ Tendencia bajista (SMA20 < SMA50)")

    rsi_desc = {
        "very_oversold":   "RSI muy sobrevendido (<20) — zona de rebote potencial",
        "oversold":        "RSI sobrevendido (20-40) — presión compradora posible",
        "neutral":         "RSI neutro (40-60) — sin señal de extremo",
        "overbought":      "RSI sobrecomprado (60-75) — cautela en nuevas entradas",
        "very_overbought": "RSI extremadamente sobrecomprado (>75) — riesgo de reversión",
    }
    tech_parts.append(rsi_desc.get(rsi_state_ext, f"RSI: {rsi_state_ext}"))

    if adx_state == "trending":
        tech_parts.append("📈 ADX >25 — tendencia confirmada y robusta")
    elif adx_state == "lateral":
        tech_parts.append("↔️ ADX <15 — mercado lateral, señales técnicas poco fiables")
    elif adx_state == "moderate":
        tech_parts.append("ADX moderado — tendencia en formación")

    if momentum_20d is not None:
        mom_pct = round(momentum_20d * 100, 1)
        if momentum_crowding:
            tech_parts.append(
                f"⚠️ CROWDING: momentum 20d = {mom_pct:+.1f}% con RSI sobrecomprado — "
                f"riesgo de reversión brusca si cambia el flujo"
            )
        elif abs(mom_pct) > 10:
            direction = "alcista" if mom_pct > 0 else "bajista"
            tech_parts.append(f"Momentum 20d: {mom_pct:+.1f}% ({direction} sostenido)")
        else:
            tech_parts.append(f"Momentum 20d: {mom_pct:+.1f}% (movimiento moderado)")

    lines.append("Técnicos: " + " | ".join(tech_parts))

    # ── Fuerzas dominantes (contribution analysis) ─────────────────────────────
    if effects:
        sorted_eff = sorted(
            [(k, v.get("delta_prob_up", 0)) for k, v in effects.items() if v.get("applicable")],
            key=lambda x: abs(x[1]), reverse=True
        )
        if sorted_eff:
            top = sorted_eff[:2]
            drivers = []
            for name, delta in top:
                if abs(delta) < 0.01:
                    continue
                direction = "empuja ↑" if delta > 0 else "arrastra ↓"
                drivers.append(f"{name} ({delta:+.2f}) {direction}")
            if drivers:
                lines.append("Drivers: " + " | ".join(drivers))

    # ── Macro ──────────────────────────────────────────────────────────────────
    if risk_regime not in ("NEUTRAL", None):
        lines.append(f"Macro: régimen {risk_regime} | sentimiento {macro_sentiment}")

    # ── Probabilidad ──────────────────────────────────────────────────────────
    lines.append(
        f"P(subida)={prob_up:.0%} | "
        f"{'Señal probabilística: mercado favorable' if prob_up > 0.55 else 'Señal probabilística: mercado adverso' if prob_up < 0.45 else 'Señal probabilística: incertidumbre elevada'}"
    )

    return "\n".join(lines)


# =============================================================================
# 4b. WORKER POR TICKER (thread-safe, conexión PG propia)
# =============================================================================
def _process_ticker_day(
    ticker: str,
    date_str: str,
    run_id: str,
    ohlcv_all: Dict,
    news_all: Dict,
    alphavantage_news: Dict,
    newsapi_ticker_news: Dict,
    macro_adj: float,
    macro_sentiment: str,
    risk_regime: str,
    signal_history: List[str],       # copia del historial del ticker (no compartida)
    previous_exposure: float = 0.5,  # exposición suavizada del día anterior
    vix: Optional[float] = None,     # VIX del día (para detect_market_regime)
) -> Optional[Dict]:
    """
    Procesa un ticker para un día concreto en un hilo independiente.

    - Crea su propia conexión PostgreSQL (psycopg2 no es thread-safe).
    - Usa _finbert_lock para inferencia FinBERT y _groq_rate_wait() para Groq.
    - Devuelve un dict con trace_data, signal_record, new_history y kpis_delta,
      o None si el ticker no tiene datos OHLCV para esa fecha.
    """
    thread_conn = None
    try:
        thread_conn = get_db_connection()

        ohlcv_df = ohlcv_all.get(ticker)
        ind = (
            calculate_indicators_for_date(ohlcv_df, date_str)
            if ohlcv_df is not None
            else None
        )
        if not ind:
            return None

        # ── OHLCV → MongoDB + Aurora ────────────────────────────────────────
        target_dt = pd.to_datetime(date_str)
        if target_dt in ohlcv_df.index:
            row_data = ohlcv_df.loc[target_dt]
            upsert_ohlcv_bulk(
                date_str,
                ticker,
                [
                    {
                        "date": date_str,
                        "close": ind["close"],
                        "open": float(row_data.get("Open", 0) or 0),
                        "high": float(row_data.get("High", 0) or 0),
                        "low": float(row_data.get("Low", 0) or 0),
                        "volume": float(row_data.get("Volume", 0) or 0),
                    }
                ],
            )

        with thread_conn.cursor() as c:
            c.execute(
                """
                INSERT INTO technical_indicators
                    (batch_date, ticker, close_price, rsi_14, sma_20, sma_50, bb_upper, bb_middle, bb_lower)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (batch_date, ticker) DO NOTHING
            """,
                (
                    date_str,
                    ticker,
                    ind["close"],
                    ind["rsi_14"],
                    ind["sma_20"],
                    ind["sma_50"],
                    ind["bb_upper"],
                    ind["bb_middle"],
                    ind["bb_lower"],
                ),
            )
        thread_conn.commit()

        # ── Ingesta multi-fuente ─────────────────────────────────────────────
        finnhub_arts, finnhub_age = _get_ticker_articles_for_day(
            news_all.get(ticker, {}),
            date_str,
            lookback_days=TICKER_NEWS_LOOKBACK_DAYS,
        )
        alphavantage_arts, alphavantage_age = _get_ticker_articles_for_day(
            alphavantage_news.get(ticker, {}),
            date_str,
            lookback_days=TICKER_NEWS_LOOKBACK_DAYS,
        )
        newsapi_arts, newsapi_age = _get_ticker_articles_for_day(
            newsapi_ticker_news.get(ticker, {}),
            date_str,
            lookback_days=TICKER_NEWS_LOOKBACK_DAYS,
        )
        yfinance_arts = fetch_yfinance_ticker_news(ticker, date_str)
        articles = merge_ticker_articles(
            finnhub_arts, alphavantage_arts, newsapi_arts, yfinance_arts
        )

        _news_debug(
            f"[news-day] {date_str} {ticker}: "
            f"Finnhub={len(finnhub_arts)} AlphaVantage={len(alphavantage_arts)} "
            f"NewsAPI={len(newsapi_arts)} "
            f"YFinance={len(yfinance_arts)} merged={len(articles)}"
        )
        if DEBUG_NEWS:
            for source_name, age in (
                ("Finnhub", finnhub_age),
                ("AlphaVantage", alphavantage_age),
                ("NewsAPI", newsapi_age),
            ):
                if age > 0:
                    logger.info(
                        f"[news-lookback] {date_str} {ticker} {source_name}: "
                        f"usando articulos de D-{age}"
                    )
        if DEBUG_NEWS:
            for source_name, source_articles in (
                ("Finnhub", finnhub_arts),
                ("AlphaVantage", alphavantage_arts),
                ("NewsAPI", newsapi_arts),
                ("YFinance", yfinance_arts),
                ("Merged", articles),
            ):
                samples = _headline_samples(source_articles)
                if samples:
                    logger.info(
                        f"[news-headlines] {date_str} {ticker} {source_name}: {samples}"
                    )

        if articles:
            upsert_raw_news(date_str, ticker, articles)
        else:
            logger.warning(
                f"[news-gap] {date_str} {ticker}: 0 noticias tras merge "
                f"(Finnhub={len(finnhub_arts)}, AlphaVantage={len(alphavantage_arts)}, "
                f"NewsAPI={len(newsapi_arts)}, YFinance={len(yfinance_arts)})"
            )

        # ── Groq (LLM) + FinBERT por artículo ───────────────────────────────
        processed_headlines: List[str] = []
        sentiment_samples: List[Dict] = []
        kpis = {"total_headlines": 0, "processed_headlines": 0}

        for art in articles[:20]:
            kpis["total_headlines"] += 1
            # extract_and_summarize usa _groq_rate_wait() → thread-safe
            summary = extract_and_summarize(
                ticker, art.get("headline", ""), art.get("url", "")
            )
            # analyze_sentiment_local usa _finbert_lock → thread-safe
            sdata = analyze_sentiment_local(summary)

            if sdata:
                kpis["processed_headlines"] += 1
                upsert_news(date_str, ticker, art, sdata)
                sentiment_samples.append(
                    {
                        "headline": summary,
                        "sentiment": sdata["sentiment"],
                        "confidence": sdata["confidence"],
                    }
                )
                with thread_conn.cursor() as c:
                    c.execute(
                        """
                        INSERT INTO sentiment_scores
                            (batch_date, ticker, headline, sentiment, confidence, justification)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (batch_date, ticker, headline) DO NOTHING
                    """,
                        (
                            date_str,
                            ticker,
                            art.get("headline", "")[:250],
                            sdata["sentiment"],
                            sdata["confidence"],
                            sdata["justification"],
                        ),
                    )
                thread_conn.commit()
                processed_headlines.append(summary)

        if processed_headlines:
            upsert_filtered_news(
                date_str, ticker, processed_headlines, "Backtest local"
            )
        _news_debug(
            f"[news-sentiment] {date_str} {ticker}: procesadas={len(processed_headlines)}/"
            f"{len(articles[:20])} con FinBERT/Groq={'on' if GROQ_API_KEY else 'off'}"
        )

        # ── Agregación de sentimiento y evidencias ───────────────────────────
        dom_sent, best_conf, sentiment_detail = aggregate_sentiment_local(
            sentiment_samples
        )

        # ══════════════════════════════════════════════════════════════════════
        # CONSTRUCCIÓN DE EVIDENCIAS PARA LA RED BAYESIANA
        # Objetivo: maximizar la calidad informativa de cada variable discreta
        # que entra a la BN, usando todos los indicadores disponibles.
        # ══════════════════════════════════════════════════════════════════════

        _rsi_val  = ind["rsi_14"]  or 50.0
        _adx      = ind.get("adx_14")
        _mom20    = ind.get("momentum_20d") or 0.0
        _mom5     = ind.get("momentum_5d")  or 0.0
        _ema55pct = ind.get("ema_55_pct")   # % desviación precio vs EMA-55

        # ── ADX state (calculado primero porque lo usan otras evidencias) ─────
        if _adx is None:
            adx_state = "unknown"
        elif _adx > 25:
            adx_state = "trending"
        elif _adx > 15:
            adx_state = "moderate"
        else:
            adx_state = "lateral"

        # ── Evidencia SENTIMENT (mejorada con fallback a momentum) ────────────
        # Problema identificado: cuando no hay noticias disponibles, FinBERT
        # devuelve "neutral" por defecto, coartando la discriminación de la BN.
        # Solución: si no hay artículos y el momentum es claro, usarlo como
        # proxy de sentimiento implícito del mercado.
        if dom_sent == "neutral" and best_conf == 0:
            # Sin noticias: inferir sentimiento implícito desde momentum
            if _mom20 > 0.05 and _mom5 > 0.01:
                dom_sent = "bullish"
                sentiment_detail = {
                    **sentiment_detail,
                    "proxy": f"momentum_20d={_mom20*100:.1f}% → bullish implícito (sin noticias)",
                }
            elif _mom20 < -0.05 and _mom5 < -0.01:
                dom_sent = "bearish"
                sentiment_detail = {
                    **sentiment_detail,
                    "proxy": f"momentum_20d={_mom20*100:.1f}% → bearish implícito (sin noticias)",
                }
            # else: mantener neutral (momentum insuficiente para señal)

        # ── RSI 5 niveles (disc engine) con umbrales adaptados al mercado real ─
        # Los umbrales estándar 30/70 dejan el 80%+ de los días en "neutral".
        # Umbrales ajustados capturan más señal sin perder rigor estadístico.
        if _rsi_val < 20:
            rsi_state_ext = "very_oversold"
        elif _rsi_val < 40:
            rsi_state_ext = "oversold"
        elif _rsi_val <= 60:
            rsi_state_ext = "neutral"
        elif _rsi_val <= 75:
            rsi_state_ext = "overbought"
        else:
            rsi_state_ext = "very_overbought"

        # RSI para la BN (3 niveles, manteniendo los umbrales originales del modelo)
        rsi_state_bn = (
            "oversold"    if _rsi_val < 30
            else "overbought" if _rsi_val > 70
            else "neutral"
        )

        # ── Evidencia TREND (mejorada con EMA-55 y validación ADX) ────────────
        # Problema: SMA20/SMA50 genera crossovers tardíos y falsos en mercado lateral.
        # Mejora: usar EMA-55 como referencia principal (más robusta, reacciona
        # más rápido que SMA-50 y es la feature #1 en literatura académica).
        # Además, si ADX < 15 (mercado lateral sin tendencia), la señal de trend
        # no es fiable — reducimos su impacto dejando que la BN use su prior.
        _sma20 = ind.get("sma_20"); _sma50 = ind.get("sma_50")
        _cl    = ind.get("close")

        if adx_state == "lateral":
            # Mercado sin tendencia: usar EMA-55 si disponible, si no SMA-based
            if _ema55pct is not None:
                # EMA-55 como árbitro cuando no hay tendencia SMA clara
                trend_state_bn = "uptrend" if _ema55pct > 0.01 else "downtrend"
                trend_quality  = "ema55_validated"
            else:
                # Sin EMA-55, mantener SMA pero marcar baja confianza
                trend_state_bn = "uptrend" if (_sma20 and _sma50 and _sma20 > _sma50) else "downtrend"
                trend_quality  = "sma_low_confidence"
        elif _ema55pct is not None and _sma20 and _sma50:
            # EMA-55 y SMA disponibles: usamos EMA-55 como señal principal
            # La coherencia entre ambas aumenta la confianza
            ema55_up  = _ema55pct > 0.0
            sma_up    = _sma20 > _sma50
            if ema55_up == sma_up:
                trend_state_bn = "uptrend" if ema55_up else "downtrend"
                trend_quality  = "ema55_sma_agree"       # máxima confianza
            else:
                # Divergencia: EMA-55 manda (más robusta y feature #1)
                trend_state_bn = "uptrend" if ema55_up else "downtrend"
                trend_quality  = "ema55_diverges_sma"    # baja confianza
        else:
            # Fallback: SMA20/SMA50 clásico
            trend_state_bn = "uptrend" if (_sma20 and _sma50 and _sma20 > _sma50) else "downtrend"
            trend_quality  = "sma_only"

        # ── Evidencia VOLATILITY (mejorada con confirmación momentum) ──────────
        # BB Width captura volatilidad de precio. Complementamos con la
        # aceleración del precio (momentum_5d) para distinguir volatilidad
        # de expansión real vs ruido lateral.
        _bbw = ind.get("bb_width")
        vol_state_bn = "high" if (_bbw and _bbw > 0.05) else "low"

        # ── Momentum crowding flag ────────────────────────────────────────────
        # Momentum > 15% en 20d con RSI overbought = riesgo de reversión brusca
        momentum_crowding = abs(_mom20) > 0.15 and rsi_state_ext in ("overbought", "very_overbought")

        # ── Evidencias finales para la BN ─────────────────────────────────────
        evidence = {
            "Sentiment":  dom_sent,
            "RSI":        rsi_state_bn,
            "Trend":      trend_state_bn,
            "Volatility": vol_state_bn,
        }

        # ── Inferencia (Camino B / Camino A) ────────────────────────────────
        _macro_ctx = {
            "macro_adjustment": macro_adj,
            "macro_sentiment":  macro_sentiment,
            "risk_regime":      risk_regime,
        }
        # Features extendidas para el motor discriminativo
        _disc_extra = {
            "rsi_continuous":      ind.get("rsi_14"),
            "adx_14":              ind.get("adx_14"),
            "ema_55_pct":          ind.get("ema_55_pct"),
            "momentum_20d":        ind.get("momentum_20d"),
            "momentum_5d":         ind.get("momentum_5d"),
            "sentiment_dispersion":(
                sentiment_detail.get("dispersion")
                if isinstance(sentiment_detail, dict) else None
            ),
            "signal_streak": len(signal_history),
        }
        raw_signal, prob_up = run_bayesian_inference(
            evidence, macro_adj, macro_context=_macro_ctx, extra=_disc_extra
        )

        try:
            contribution_analysis = compute_contribution_analysis(
                evidence,
                probability_fn=lambda ev, adj=macro_adj: run_bayesian_inference(
                    ev, adj
                )[1],
                no_macro_probability_fn=lambda ev: run_bayesian_inference(ev, 0.0)[1],
            )
            contribution_analysis["macro_context"] = {
                "macro_sentiment": macro_sentiment,
                "risk_regime": risk_regime,
                "macro_adjustment": macro_adj,
            }
        except Exception as exc:
            logger.warning(
                f"contribution_analysis fallo para {ticker} {date_str}: {exc}"
            )
            contribution_analysis = {}

        # ── Hysteresis (sobre la copia local del historial) ──────────────────
        confirmed_signal, hysteresis_status = apply_hysteresis_signal(
            raw_signal, signal_history
        )
        new_history = (signal_history + [confirmed_signal])[-SELL_CONFIRMATION_DAYS:]

        if raw_signal != confirmed_signal:
            logger.debug(
                f"[HYSTERESIS] {ticker} {date_str}: raw={raw_signal} "
                f"→ confirmed={confirmed_signal} ({hysteresis_status})"
            )

        signal = confirmed_signal
        reasoning = build_reasoning_local(evidence, prob_up, signal)

        # ── Exposure Management (Fase 1) ─────────────────────────────────────
        # El régimen usa SMA200 (estructural) + VIX + drawdown_from_ath.
        # Difiere de risk_regime (que sólo usa VIX y modula macro_adj)
        # porque mira la tendencia de largo plazo del propio activo.
        market_regime_exp = detect_market_regime(
            sma50=ind.get("sma_50"),
            sma200=ind.get("sma_200"),
            vix=vix,
            drawdown_from_ath=ind.get("drawdown_from_ath"),
        )
        target_exposure   = prob_to_exposure(prob_up, market_regime_exp)
        smoothed_exposure = smooth_exposure(target_exposure, previous_exposure)
        exposure_delta    = round(smoothed_exposure - previous_exposure, 4)

        logger.debug(
            f"[EXPOSURE] {ticker} {date_str}: regime={market_regime_exp} "
            f"prob_up={prob_up:.3f} target={target_exposure:.3f} "
            f"smooth={smoothed_exposure:.3f} Δ={exposure_delta:+.3f}"
        )

        # Persistir en position_state (conexión del hilo)
        try:
            pg_upsert_position_state(
                thread_conn, date_str, ticker, prob_up,
                market_regime_exp, target_exposure, smoothed_exposure, exposure_delta,
            )
        except Exception as exc:
            logger.warning(f"[EXPOSURE] position_state upsert falló {ticker} {date_str}: {exc}")

        # ── Conviction score ──────────────────────────────────────────────────
        # Mide si los indicadores apuntan en la misma dirección o se contradicen.
        # Alto = señal clara. Bajo = fuerzas opuestas, posición incierta.
        _effects = (contribution_analysis or {}).get("effects", {})
        _deltas = [v.get("delta_prob_up", 0) for v in _effects.values()
                   if v.get("applicable")]
        if len(_deltas) >= 2:
            _pos = sum(1 for d in _deltas if d > 0.02)
            _neg = sum(1 for d in _deltas if d < -0.02)
            _dom = max(_pos, _neg)
            conviction_score = round(_dom / len(_deltas), 2)
            conviction_label = (
                "high"   if conviction_score >= 0.75 else
                "medium" if conviction_score >= 0.50 else
                "low"
            )
        else:
            conviction_score = 0.5
            conviction_label = "unknown"

        # ── Exposure recommendation (5 niveles, sustitye señal binaria) ────────
        _exp = smoothed_exposure
        _delta_exp = exposure_delta
        if _exp >= 0.75 and _delta_exp >= -0.01:
            exposure_recommendation = "INCREASE_STRONG"
        elif _exp >= 0.60:
            exposure_recommendation = "INCREASE_MILD"
        elif _exp >= 0.45 and abs(_delta_exp) <= 0.02:
            exposure_recommendation = "MAINTAIN"
        elif _exp >= 0.30:
            exposure_recommendation = "REDUCE_MILD"
        else:
            exposure_recommendation = "REDUCE_STRONG"

        # ── Signal narrative (texto accionable en lenguaje natural) ────────────
        signal_narrative = _build_signal_narrative(
            ticker=ticker,
            evidence=evidence,
            rsi_state_ext=rsi_state_ext,
            adx_state=adx_state,
            momentum_20d=ind.get("momentum_20d"),
            momentum_crowding=momentum_crowding,
            prob_up=prob_up,
            market_regime=market_regime_exp,
            smoothed_exposure=smoothed_exposure,
            exposure_delta=exposure_delta,
            exposure_recommendation=exposure_recommendation,
            conviction_label=conviction_label,
            effects=_effects,
            macro_sentiment=macro_sentiment,
            risk_regime=risk_regime,
        )

        # ── Trace + persistencia ─────────────────────────────────────────────
        trace_data = {
            "raw_values": {
                "close_price":   ind["close"],
                "rsi_14":        ind["rsi_14"],
                "sma_20":        ind["sma_20"],
                "sma_50":        ind["sma_50"],
                "bb_upper":      ind["bb_upper"],
                "bb_lower":      ind["bb_lower"],
                "bb_width_ratio":ind["bb_width"],
                # Nuevos indicadores
                "ema_55":        ind.get("ema_55"),
                "ema_55_pct":    ind.get("ema_55_pct"),
                "adx_14":        ind.get("adx_14"),
                "momentum_20d":  ind.get("momentum_20d"),
                "momentum_5d":   ind.get("momentum_5d"),
            },
            "discretization": {
                "sentiment_raw":    dom_sent,
                "sentiment_conf":   best_conf,
                "sentiment_state":  evidence["Sentiment"],
                "rsi_state":        evidence["RSI"],        # 3 niveles (BN)
                "rsi_state_ext":    rsi_state_ext,          # 5 niveles (disc engine)
                "trend_state":      evidence["Trend"],
                "trend_quality":    trend_quality,          # calidad de la evidencia de trend
                "volatility_state": evidence["Volatility"],
                "adx_state":        adx_state,
                "momentum_crowding":momentum_crowding,
            },
            "sentiment_detail": sentiment_detail,
            "inference": {
                "signal": signal,
                "raw_signal": raw_signal,
                "hysteresis_status": hysteresis_status,
                "prob_up": prob_up,
                "prob_down": round(1 - prob_up, 4),
                "threshold_used": (
                    MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"]
                    if signal == "BUY"
                    else MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"]
                ),
                "macro_context": {
                    "macro_sentiment": macro_sentiment,
                    "risk_regime": risk_regime,
                    "macro_adjustment": macro_adj,
                },
            },
            "contribution_analysis": contribution_analysis,
            "reasoning": reasoning,
            # ── Output primario: exposición continua ──────────────────────────
            "exposure_management": {
                "market_regime":          market_regime_exp,
                "target_exposure":        target_exposure,
                "smoothed_exposure":      smoothed_exposure,
                "exposure_delta":         exposure_delta,
                "previous_exposure":      previous_exposure,
                "exposure_recommendation":exposure_recommendation,
            },
            # ── Interpretación de señal ────────────────────────────────────────
            "signal_quality": {
                "conviction_score": conviction_score,
                "conviction_label": conviction_label,
                "narrative":        signal_narrative,
            },
        }
        upsert_bayesian_report(date_str, ticker, trace_data, MODEL_CONFIG["version"])

        pg_upsert_signal(
            thread_conn, date_str, ticker, signal, prob_up, round(1 - prob_up, 4)
        )
        with thread_conn.cursor() as c:
            c.execute(
                """
                INSERT INTO signal_explanations
                    (batch_date, ticker, sentiment_state, rsi_state, trend_state, volatility_state)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (batch_date, ticker) DO UPDATE SET
                    sentiment_state=EXCLUDED.sentiment_state,
                    rsi_state=EXCLUDED.rsi_state,
                    trend_state=EXCLUDED.trend_state,
                    volatility_state=EXCLUDED.volatility_state
            """,
                (
                    date_str,
                    ticker,
                    evidence["Sentiment"],
                    evidence["RSI"],
                    evidence["Trend"],
                    evidence["Volatility"],
                ),
            )
        thread_conn.commit()

        signal_record = {
            "batch_date":   date_str,
            "ticker":       ticker,
            "run_id":       run_id,
            # ── Output primario: exposición continua (no señal binaria) ────────
            "exposure_recommendation": exposure_recommendation,
            "smoothed_exposure":       smoothed_exposure,
            "target_exposure":         target_exposure,
            "exposure_delta":          exposure_delta,
            "market_regime":           market_regime_exp,
            # ── Señal bayesiana (referencia) ────────────────────────────────────
            "signal":          signal,
            "prob_up":         prob_up,
            "prob_down":       round(1 - prob_up, 4),
            "close_price":     ind["close"],
            # ── Discretización de indicadores ───────────────────────────────────
            "sentiment_state": evidence["Sentiment"],
            "rsi_state":       evidence["RSI"],
            "rsi_state_ext":   rsi_state_ext,
            "trend_state":     evidence["Trend"],
            "volatility_state":evidence["Volatility"],
            "adx_state":       adx_state,
            # ── Contexto macro ──────────────────────────────────────────────────
            "macro_sentiment": macro_sentiment,
            "risk_regime":     risk_regime,
            "macro_adjustment":macro_adj,
            # ── Calidad de la señal ─────────────────────────────────────────────
            "conviction_score": conviction_score,
            "conviction_label": conviction_label,
            # ── Nuevos indicadores técnicos ─────────────────────────────────────
            "adx_14":           ind.get("adx_14"),
            "ema_55_pct":       ind.get("ema_55_pct"),
            "momentum_20d":     ind.get("momentum_20d"),
            "momentum_5d":      ind.get("momentum_5d"),
            "momentum_crowding":momentum_crowding,
        }

        return {
            "ticker":          ticker,
            "trace_data":      trace_data,
            "signal_record":   signal_record,
            "new_history":     new_history,
            "kpis":            kpis,
            "smoothed_exposure": smoothed_exposure,
            "market_regime":   market_regime_exp,
            "new_conf_state":  None,
        }

    except Exception as e:
        logger.error(
            f"[thread] Error procesando {ticker} {date_str}: {e}", exc_info=True
        )
        return None
    finally:
        if thread_conn:
            try:
                thread_conn.close()
            except Exception:
                pass


# =============================================================================
# 5. LOOP MAESTRO
# =============================================================================
def run_pipeline(start_date_str=None, end_date_str=None, tickers_override=None):
    # Fechas por argumento o por defecto
    end_d = (
        pd.to_datetime(end_date_str).date() if end_date_str else datetime.now().date()
    )
    start_d = (
        pd.to_datetime(start_date_str).date()
        if start_date_str
        else (end_d - timedelta(days=DAYS_BACK))
    )

    active_tickers = tickers_override if tickers_override else TICKERS
    logger.info(
        f"🚀 Iniciando Bootstrap Local TFM | Rango: {start_d} a {end_d} | Tickers: {active_tickers}"
    )
    if DEBUG_NEWS:
        logger.info(
            f"🔎 Debug noticias activo | headlines={DEBUG_NEWS_HEADLINES} | "
            f"refresh_cache={REFRESH_NEWS_CACHE}"
        )

    # ── 1. DESCARGA INICIAL DE DATOS ──
    # Para poder calcular indicadores en start_d, la lambda descarga días extra.
    ohlcv_all = fetch_ohlcv_all(active_tickers, start_d, end_d)
    vix_series = fetch_vix_historical(start_d, end_d)

    # ── Prefetch paralelo de noticias (Finnhub + AlphaVantage + NewsAPI) ────────
    # Las tres fuentes son independientes por ticker y usan caché en disco.
    def _prefetch_ticker_news(ticker: str):
        f = fetch_news_historical(ticker, start_d, end_d)
        a = fetch_alpha_vantage_news_historical(ticker, start_d, end_d)
        n = fetch_newsapi_ticker_news_historical(ticker, start_d, end_d)
        return ticker, f, a, n

    logger.info(f"⬇ Prefetching noticias en paralelo para {active_tickers}…")
    news_all: Dict[str, Dict] = {}
    alphavantage_news: Dict[str, Dict] = {}
    newsapi_ticker_news: Dict[str, Dict] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(active_tickers))) as exe:
        prefetch_futs = {
            exe.submit(_prefetch_ticker_news, t): t for t in active_tickers
        }
        for fut in as_completed(prefetch_futs):
            t, finnhub_data, alphavantage_data, newsapi_data = fut.result()
            news_all[t] = finnhub_data
            alphavantage_news[t] = alphavantage_data
            newsapi_ticker_news[t] = newsapi_data
    logger.info("✅ Prefetch completado")

    conn = get_db_connection()
    get_bn_model()
    get_finbert()

    # ── DDL: crear position_state si no existe ────────────────────────────────
    try:
        with conn.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS position_state (
                    batch_date        DATE         NOT NULL,
                    ticker            VARCHAR(10)  NOT NULL,
                    prob_up           FLOAT,
                    market_regime     VARCHAR(20),
                    target_exposure   FLOAT,
                    smoothed_exposure FLOAT,
                    exposure_delta    FLOAT,
                    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (batch_date, ticker)
                )
            """)
        conn.commit()
        logger.info("✅ Tabla position_state lista")
    except Exception as exc:
        conn.rollback()
        logger.warning(f"DDL position_state: {exc}")

    business_days = pd.bdate_range(start=str(start_d), end=str(end_d))

    # Prevenir simulación de fechas futuras
    today_date = datetime.now().date()
    business_days = [bd for bd in business_days if bd.date() <= today_date]

    # ── Hysteresis: historial de señales CONFIRMADAS por ticker (ventana deslizante) ──
    # Se mantiene entre días para detectar N SELLs consecutivos antes de salir.
    signal_history_per_ticker: dict = {t: [] for t in active_tickers}

    # ── Fase 1 — Exposure tracking ────────────────────────────────────────────
    # Exposición suavizada del día anterior por ticker (EWM).
    # Valor inicial 0.5 = exposición neutral (equivalente al prior de mercado).
    exposure_history_per_ticker: dict = {t: 0.5 for t in active_tickers}
    # Acumulado de todos los signal_records (incluye smoothed_exposure) para
    # calcular el backtesting de exposición en cada iteración del reporte.
    all_signal_records: List[Dict] = []

    # ── ThreadPoolExecutor para procesamiento paralelo por ticker ─────────────
    # Se crea UNA SOLA VEZ fuera del loop y se reutiliza en cada día.
    # max_workers = nº de tickers activos → paralelismo exacto como Step Functions.
    ticker_executor = ThreadPoolExecutor(
        max_workers=len(active_tickers),
        thread_name_prefix="ticker-worker",
    )

    # ── 2. BUCLE DIARIO ──
    for bd in tqdm(business_days, desc="Simulando días", unit="día"):
        date_str = bd.strftime("%Y-%m-%d")
        run_id = f"backtest-{date_str}"
        global_kpis = {"total_headlines": 0, "processed_headlines": 0}
        _news_debug(f"[day-start] {date_str}: iniciando simulacion diaria")

        if conn:
            pg_upsert_batch_log(conn, date_str, run_id, active_tickers, "STARTED")

        # --- A) Ingesta y Macro (Clon lambda_macro_ingestion / lambda_macro_context) ---
        ingest_macro_news(date_str)
        macro_articles = read_macro_news(date_str)
        _news_debug(
            f"[macro-news] {date_str}: articulos_macro={len(macro_articles or [])} "
            f"samples={_headline_samples(macro_articles or [])}"
        )
        macro_sentiment_data = run_finbert_macro_local(macro_articles)

        macro_sentiment = macro_sentiment_data["state"]
        macro_score = macro_sentiment_data["score"]
        n_macro_articles = macro_sentiment_data["n_articles"]

        vix = (
            _sf(vix_series[vix_series.index <= bd].iloc[-1])
            if not vix_series[vix_series.index <= bd].empty
            else None
        )
        risk_regime = (
            "RISK_OFF"
            if vix and vix > 25
            else ("RISK_ON" if vix and vix < 18 else "NEUTRAL")
        )
        macro_adj = (
            -0.04
            if risk_regime == "RISK_OFF"
            else (0.04 if risk_regime == "RISK_ON" else 0.0)
        )

        upsert_macro_context(
            date_str,
            macro_sentiment,
            risk_regime,
            macro_adj,
            {"vix": vix, "n_articles": n_macro_articles},
        )

        if conn:
            try:
                with conn.cursor() as c:
                    c.execute(
                        """
                        INSERT INTO market_regime_state (batch_date, run_id, risk_regime, macro_adjustment, vix)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (batch_date) DO UPDATE SET risk_regime=EXCLUDED.risk_regime, macro_adjustment=EXCLUDED.macro_adjustment, vix=EXCLUDED.vix
                    """,
                        (
                            date_str,
                            run_id,
                            risk_regime,
                            float(macro_adj),
                            float(vix) if vix else None,
                        ),
                    )

                    c.execute(
                        """
                        INSERT INTO macro_sentiment_scores (batch_date, run_id, macro_sentiment, score, n_articles)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (batch_date) DO UPDATE SET macro_sentiment=EXCLUDED.macro_sentiment, score=EXCLUDED.score, n_articles=EXCLUDED.n_articles
                    """,
                        (
                            date_str,
                            run_id,
                            macro_sentiment,
                            float(macro_score),
                            n_macro_articles,
                        ),
                    )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error insertando tablas macro: {e}")

        # --- B) Procesamiento por Ticker — PARALELO (Clon Map State de Step Functions) ---
        # Cada ticker corre en su propio hilo con su propia conexión PG.
        # max_workers = número de tickers activos (≤5); todos son independientes entre sí.
        tickers_trace: Dict = {}
        daily_signals_for_outcomes: List = []

        ticker_futures = {
            ticker_executor.submit(
                _process_ticker_day,
                ticker,
                date_str,
                run_id,
                ohlcv_all,
                news_all,
                alphavantage_news,
                newsapi_ticker_news,
                macro_adj,
                macro_sentiment,
                risk_regime,
                list(signal_history_per_ticker[ticker]),    # copia — evita race condition
                exposure_history_per_ticker[ticker],        # Fase 1: exposición previa
                vix,                                        # Fase 1: VIX para detect_regime
            ): ticker
            for ticker in active_tickers
        }

        for fut in as_completed(ticker_futures):
            result = fut.result()
            if result is None:
                continue
            t = result["ticker"]
            # Actualizar estado compartido desde el hilo principal (sin races)
            tickers_trace[t] = result["trace_data"]
            daily_signals_for_outcomes.append(result["signal_record"])
            all_signal_records.append(result["signal_record"])          # Fase 1
            signal_history_per_ticker[t] = result["new_history"]
            exposure_history_per_ticker[t] = result["smoothed_exposure"]  # Fase 1
            global_kpis["total_headlines"] += result["kpis"]["total_headlines"]
            global_kpis["processed_headlines"] += result["kpis"]["processed_headlines"]

        upsert_bayesian_trace(
            date_str, {"tickers": tickers_trace, "model_config": MODEL_CONFIG}
        )

        # --- C) REPORTE DIARIO E IDÉNTICO A AWS (Clon lambda_report) ---
        if conn:
            # 1. Obtenemos señales del último año desde la BBDD (Esto da memoria al sistema)
            hist_signals_df = get_trading_data(
                conn,
                date_str,
                days_back=DAYS_BACK,
                tickers=active_tickers,
                pipeline_start=start_d,
            )
            metrics, diagnostics = _calc_backtesting(hist_signals_df)
            # Fase 1: backtesting de exposición continua usando datos in-memory
            exp_metrics, exp_diagnostics = _calc_exposure_backtesting(all_signal_records)
            benchmark = (
                compute_benchmark(hist_signals_df) if not hist_signals_df.empty else {}
            )
            health = get_pipeline_health(conn, date_str, run_id) if run_id else {}
            explanations = get_explanations_sample(conn, date_str, limit=10)
            quant_audit_report = compute_quant_audit_report(
                date_str,
                hist_signals_df.to_dict("records") if not hist_signals_df.empty else [],
                outcome_rows=get_signal_outcomes(
                    conn,
                    date_str,
                    days_back=DAYS_BACK,
                    tickers=active_tickers,
                    pipeline_start=start_d,
                ),
                model_config=MODEL_CONFIG,
            )
            upsert_quant_audit_report(date_str, quant_audit_report)

            period_days = (pd.to_datetime(date_str).date() - start_d).days + 1
            report_data = {
                "report_date": date_str,
                "pipeline_start": start_d.isoformat(),
                "pipeline_end": end_d.isoformat(),
                "data_period_days": period_days,
                "generated_at": datetime.now().isoformat(),
                "inference_engine": "bayesian_network",
                "pipeline_health": health,
                "signal_diagnostics": diagnostics,
                "benchmark_comparison": {
                    t: {
                        "strategy_cumulative_return": metrics[t]["cumulative_return"],
                        "buy_hold_cumulative_return": benchmark.get(t, 0.0),
                        "alpha_vs_benchmark": round(
                            metrics[t]["cumulative_return"] - benchmark.get(t, 0.0), 4
                        ),
                    }
                    for t in metrics
                },
                "top_signal_explanations": explanations,
                "backtesting_metrics": metrics,
                "summary": {
                    "total_tickers": len(metrics),
                    "avg_cumulative_return": (
                        round(
                            np.mean([m["cumulative_return"] for m in metrics.values()]),
                            4,
                        )
                        if metrics
                        else 0
                    ),
                    "avg_sharpe_ratio": (
                        round(np.mean([m["sharpe_ratio"] for m in metrics.values()]), 4)
                        if metrics
                        else 0
                    ),
                    "avg_max_drawdown": (
                        round(np.mean([m["max_drawdown"] for m in metrics.values()]), 4)
                        if metrics
                        else 0
                    ),  # CORRECCIÓN: Añadido para el dashboard
                    "total_closed_trades": (
                        sum(
                            item.get("trades_closed", 0)
                            for item in diagnostics.values()
                        )
                        if diagnostics
                        else 0
                    ),
                },
                "backtesting_config": {
                    "initial_capital": INITIAL_CAP,
                    "risk_free_rate": RISK_FREE_RATE,
                    "period_days": DAYS_BACK,
                    "strategy_type": "Long/Cash",
                    "sharpe_annualized": True,
                    "limitation": "El backtesting asume ejecucion al cierre. Estrategia Long/Cash: BUY entra al mercado, SELL cierra posicion, HOLD mantiene posicion abierta.",
                },
                # ── Fase 1: Probabilistic Exposure Management ─────────────────
                "exposure_backtesting_metrics": exp_metrics,
                "exposure_backtesting_diagnostics": exp_diagnostics,
                "exposure_vs_binary_comparison": {
                    t: {
                        "binary_cumulative_return":   metrics.get(t, {}).get("cumulative_return", 0.0),
                        "exposure_cumulative_return":  exp_metrics.get(t, {}).get("cumulative_return", 0.0),
                        "exposure_alpha": round(
                            exp_metrics.get(t, {}).get("cumulative_return", 0.0)
                            - metrics.get(t, {}).get("cumulative_return", 0.0),
                            4,
                        ),
                        "avg_exposure":   exp_diagnostics.get(t, {}).get("avg_exposure", 0.5),
                        "regime_distribution": exp_diagnostics.get(t, {}).get("regime_distribution", {}),
                    }
                    for t in set(list(metrics.keys()) + list(exp_metrics.keys()))
                },
                "trace_artifact": f"mongo:bayesian_traces/{date_str}",
                "quant_audit_artifact": f"mongo:quant_audit_reports/{date_str}",
            }
            upsert_report(report_data)

        pg_upsert_pipeline_kpi(
            conn, date_str, run_id, "scheduled", "ingestion", global_kpis
        )
        pg_upsert_batch_log(conn, date_str, run_id, active_tickers, "COMPLETED")

        if conn and daily_signals_for_outcomes:
            update_signal_outcomes_historical(conn, daily_signals_for_outcomes)

    ticker_executor.shutdown(wait=True)
    if conn:
        conn.close()
    logger.info("✅ BACKTESTING COMPLETADO")


if __name__ == "__main__":
    args = get_args()
    _configure_logging(verbose=getattr(args, "verbose", False))
    if args.debug_news:
        DEBUG_NEWS = True
    if args.debug_news_headlines is not None:
        DEBUG_NEWS_HEADLINES = max(0, args.debug_news_headlines)
    if args.refresh_news_cache:
        REFRESH_NEWS_CACHE = True
    tickers_list = (
        [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    )
    run_pipeline(args.start, args.end, tickers_override=tickers_list)
