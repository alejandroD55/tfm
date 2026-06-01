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

# yfinance usa SQLite para caché de timezones. None rompe Ticker.history (TypeError).
# Usamos /tmp para evitar locks entre procesos y el error con set_tz_cache_location(None).
try:
    _yf_tz_cache = os.path.join(os.environ.get("TMPDIR", "/tmp"), "yfinance_tz_cache")
    os.makedirs(_yf_tz_cache, exist_ok=True)
    yf.set_tz_cache_location(_yf_tz_cache)
except Exception:
    pass
import trafilatura
from dotenv import load_dotenv
from tqdm import tqdm

import feedparser
import argparse

warnings.filterwarnings("ignore")

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_REPO_ROOT, "shared"))

# ── Camino B: motor discriminativo LightGBM (opcional) ───────────────────────
try:
    from discriminative_engine import disc_engine as _disc_engine
    _disc_engine.load()   # carga perezosa — silencioso si modelo no existe aún
except Exception:
    _disc_engine = None

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
# Silenciar los falsos positivos "possibly delisted" de yfinance.
# yfinance los loguea como ERROR cuando la ventana de descarga toca
# fines de semana / festivos y retorna menos filas de las esperadas.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
logging.getLogger("peewee").setLevel(logging.CRITICAL)

# Siempre cargar .env desde la raíz del repo (aunque ejecutes desde otro cwd).
load_dotenv(Path(_REPO_ROOT) / ".env")

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

DEBUG_NEWS = os.getenv("BOOTSTRAP_DEBUG_NEWS", "").lower() in ("1", "true", "yes", "on")
DEBUG_NEWS_HEADLINES = int(os.getenv("BOOTSTRAP_DEBUG_NEWS_HEADLINES", "3"))
REFRESH_NEWS_CACHE = os.getenv("BOOTSTRAP_REFRESH_NEWS_CACHE", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# Puerto 5433 = mapeo host en docker-compose.yml (postgres:5432 → host:5433).
# Si usas 5432 sin Docker, caerás en el Postgres de macOS (sin rol tfmadmin).
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
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


def get_db_connection(max_attempts: int = 4, base_delay: float = 0.5):
    """
    Abre una conexión psycopg2 con reintentos y backoff exponencial.

    Motivo: bajo carga paralela (6 hilos × mismo instante) Docker puede
    rechazar conexiones momentáneamente → psycopg2.OperationalError sin
    mensaje. Con 4 intentos el retraso máximo es ~3.5 s, invisible al usuario.

    Cambios respecto a la versión anterior:
    - sslmode='disable': evita la negociación SSL en localhost (más rápido
      y elimina overhead que provoca timeouts en hilos paralelos).
    - connect_timeout=5: fuerza fallo rápido en vez de esperar indefinidamente.
    - Backoff exponencial: 0.5 s → 1 s → 2 s → (falla).
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                database=DB_CONFIG["database"],
                sslmode="disable",       # localhost → sin SSL, más rápido
                connect_timeout=5,       # fallo explícito a los 5 s
            )
        except psycopg2.OperationalError as exc:
            last_exc = exc
            if attempt < max_attempts:
                delay = base_delay * (2 ** (attempt - 1))   # 0.5, 1.0, 2.0 s
                logger.warning(
                    f"[pg-connect] intento {attempt}/{max_attempts} fallido — "
                    f"reintentando en {delay:.1f}s ({exc})"
                )
                time.sleep(delay)
    hint = (
        f"Revisa POSTGRES_* en {Path(_REPO_ROOT) / '.env'} — "
        f"Docker local usa host={DB_CONFIG['host']} port=5433 user=tfmadmin db=tfm. "
        f"Actual: port={DB_CONFIG['port']} user={DB_CONFIG['user']}"
    )
    raise psycopg2.OperationalError(f"{last_exc}\n{hint}") from last_exc


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
    conn, batch_date, ticker, prob_up, market_regime, target_exposure,
    smoothed_exposure, exposure_delta,
    # Nuevos campos Fase 2A:
    confirmed_regime=None, raw_regime=None, regime_candidate=None, regime_candidate_days=None,
    vt_exposure=None, kelly_exp=None,
    vol_5d=None, vol_20d=None, vol_ratio=None, vol_percentile=None,
    sentiment_dispersion=None, vix_regime_label=None,
):
    """Persiste el estado de exposición diario en la tabla position_state."""
    with conn.cursor() as c:
        c.execute(
            """
            INSERT INTO position_state
                (batch_date, ticker, prob_up, market_regime,
                 target_exposure, smoothed_exposure, exposure_delta,
                 confirmed_regime, raw_regime, regime_candidate, regime_candidate_days,
                 vt_exposure, kelly_exposure,
                 vol_5d, vol_20d, vol_ratio, vol_percentile_1y,
                 sentiment_dispersion, vix_regime_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                prob_up               = EXCLUDED.prob_up,
                market_regime         = EXCLUDED.market_regime,
                target_exposure       = EXCLUDED.target_exposure,
                smoothed_exposure     = EXCLUDED.smoothed_exposure,
                exposure_delta        = EXCLUDED.exposure_delta,
                confirmed_regime      = EXCLUDED.confirmed_regime,
                raw_regime            = EXCLUDED.raw_regime,
                regime_candidate      = EXCLUDED.regime_candidate,
                regime_candidate_days = EXCLUDED.regime_candidate_days,
                vt_exposure           = EXCLUDED.vt_exposure,
                kelly_exposure        = EXCLUDED.kelly_exposure,
                vol_5d                = EXCLUDED.vol_5d,
                vol_20d               = EXCLUDED.vol_20d,
                vol_ratio             = EXCLUDED.vol_ratio,
                vol_percentile_1y     = EXCLUDED.vol_percentile_1y,
                sentiment_dispersion  = EXCLUDED.sentiment_dispersion,
                vix_regime_label      = EXCLUDED.vix_regime_label
            """,
            (
                batch_date, ticker, float(prob_up), market_regime,
                float(target_exposure), float(smoothed_exposure), float(exposure_delta),
                confirmed_regime, raw_regime, regime_candidate,
                int(regime_candidate_days) if regime_candidate_days is not None else None,
                float(vt_exposure) if vt_exposure is not None else None,
                float(kelly_exp) if kelly_exp is not None else None,
                float(vol_5d) if vol_5d is not None else None,
                float(vol_20d) if vol_20d is not None else None,
                float(vol_ratio) if vol_ratio is not None else None,
                float(vol_percentile) if vol_percentile is not None else None,
                float(sentiment_dispersion) if sentiment_dispersion is not None else None,
                vix_regime_label,
            ),
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
def _normalize_ohlcv_index(df: pd.DataFrame) -> pd.DataFrame:
    """Índice datetime naive (sin TZ) para comparar con date_str YYYY-MM-DD."""
    idx = pd.to_datetime(df.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert(None)
    df = df.copy()
    df.index = idx
    return df


def _download_ohlcv_ticker(
    ticker: str, download_start: date, download_end: date
) -> pd.DataFrame:
    """Descarga OHLCV con yf.download; fallback a Ticker.history si falla."""
    start_s, end_s = str(download_start), str(download_end)
    for attempt in range(3):
        df = yf.download(
            ticker, start=start_s, end=end_s, progress=False, repair=False
        )
        if not df.empty:
            return df
        if attempt < 2:
            logger.warning(
                f"[OHLCV] {ticker}: yf.download vacío (intento {attempt + 1}/3), reintentando…"
            )
            time.sleep(1.5 * (attempt + 1))

    logger.warning(f"[OHLCV] {ticker}: yf.download falló, probando Ticker.history…")
    df = yf.Ticker(ticker).history(start=start_s, end=end_s, auto_adjust=False)
    if df.empty:
        return df
    # history devuelve Open/High/Low/Close/Volume con mismo esquema
    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
    return df[keep]


def fetch_ohlcv_all(
    tickers: List[str], start_date: date, end_date: date
) -> Dict[str, pd.DataFrame]:
    # Lookback de 350 días calendario ≈ 245 días hábiles, garantiza SMA200 desde el primer día del rango
    download_start = start_date - timedelta(days=350)
    # yfinance trata end como exclusivo: +1 día para incluir el último día simulado
    download_end = end_date + timedelta(days=1)
    result = {}
    for ticker in tickers:
        df = _download_ohlcv_ticker(ticker, download_start, download_end)
        if df.empty:
            logger.error(
                f"[OHLCV] {ticker}: sin datos tras download+history "
                f"({download_start} → {download_end})"
            )
            continue
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df = _normalize_ohlcv_index(df)
        result[ticker] = df
        logger.info(
            f"[OHLCV] {ticker}: {len(df)} filas "
            f"({df.index.min().date()} → {df.index.max().date()})"
        )
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
        if total > 0:
            _news_debug(
                f"[news-cache] Finnhub {ticker}: {total} articulos en {days} dias desde {cache_file}"
            )
            return cached
        logger.warning(
            f"[news-cache] Finnhub {ticker}: cache vacio ({cache_file.name}), re-fetching"
        )
    if cache_file.exists() and REFRESH_NEWS_CACHE:
        _news_debug(f"[news-cache] Finnhub {ticker}: ignorando cache por --refresh-news-cache")
    if not FINNHUB_API_KEY:
        _news_debug(f"[Finnhub] {ticker}: sin FINNHUB_API_KEY, fuente omitida")
        return {}

    news_by_date, current = {}, start_d.replace(day=1)
    while current <= end_d:
        next_m = date(current.year + (current.month == 12), (current.month % 12) + 1, 1)
        month_end = min(next_m - timedelta(days=1), end_d)
        try:
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
        except requests.RequestException as exc:
            logger.warning(
                f"[Finnhub] {ticker} {current:%Y-%m}: error de red ({exc}), omitiendo mes"
            )
            time.sleep(1.2)
            current = next_m
            continue
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
    total, days = _count_news(news_by_date)
    if total > 0:
        with open(cache_file, "w", encoding="utf-8") as fh:
            json.dump(news_by_date, fh)
        logger.info(f"[Finnhub] {ticker}: {total} articulos en {days} dias -> cacheado")
    else:
        logger.warning(
            f"[Finnhub] {ticker}: 0 articulos en {days} dias (no se cachea respuesta vacia)"
        )
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


def _empty_vix_series() -> pd.Series:
    """Serie vacía con DatetimeIndex (evita RangeIndex en comparaciones con fechas)."""
    return pd.Series(dtype=float, index=pd.DatetimeIndex([]))


def vix_on_or_before(vix_series: pd.Series, as_of) -> Optional[float]:
    """Último cierre VIX en o antes de `as_of` (día hábil del backtest)."""
    if vix_series is None or vix_series.empty:
        return None
    if not isinstance(vix_series.index, pd.DatetimeIndex):
        vix_series = vix_series.copy()
        vix_series.index = pd.to_datetime(vix_series.index)
    cutoff = pd.Timestamp(as_of).normalize()
    idx = vix_series.index
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert(None)
        vix_series = vix_series.copy()
        vix_series.index = idx
    mask = vix_series.index.normalize() <= cutoff
    subset = vix_series.loc[mask]
    if subset.empty:
        return None
    return _sf(subset.iloc[-1])


def fetch_vix_historical(start_d: date, end_d: date) -> pd.Series:
    df = yf.download(
        "^VIX",
        start=str(start_d - timedelta(days=5)),
        end=str(end_d + timedelta(days=1)),
        progress=False,
        repair=False,
    )
    if df.empty:
        logger.warning(
            "[VIX] yfinance no devolvió datos para ^VIX — risk_regime usará vix=None"
        )
        return _empty_vix_series()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)

    close_col = "Close" if "Close" in df.columns else df.columns[0]
    series = df[close_col].copy()
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0]

    idx = pd.to_datetime(series.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert(None)
    series.index = idx

    return series.dropna()


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
    df = _normalize_ohlcv_index(ohlcv_df)
    df = df[df.index <= target_dt].copy()
    if len(df) < 50:
        return None

    close = df["Close"]
    rsi = ta.rsi(close, length=14)
    sma_20 = ta.sma(close, length=20)
    sma_50 = ta.sma(close, length=50)
    sma_200 = ta.sma(close, length=200)
    bbands = ta.bbands(close, length=20, std=2)

    def last(s):
        return _sf(s.iloc[-1]) if s is not None and len(s) > 0 else None

    bb_upper = bb_mid = bb_lower = None
    if bbands is not None and not bbands.empty and len(bbands.columns) >= 3:
        bb_lower = _sf(bbands.iloc[-1, 0])
        bb_mid = _sf(bbands.iloc[-1, 1])
        bb_upper = _sf(bbands.iloc[-1, 2])

    cl = _sf(close.iloc[-1])
    s20, s50 = last(sma_20), last(sma_50)
    s200 = last(sma_200)
    sma_spread = round(float(s20) - float(s50), 4) if s20 and s50 else None
    bb_width = (
        round((float(bb_upper) - float(bb_lower)) / float(cl), 6)
        if bb_upper and bb_lower and cl
        else None
    )

    # Drawdown desde máximo histórico disponible en la ventana de datos
    ath = _sf(close.max())
    drawdown_from_ath = round((float(cl) - float(ath)) / float(ath), 4) if cl and ath and ath > 0 else None

    return {
        "close": cl,
        "rsi_14": last(rsi),
        "sma_20": s20,
        "sma_50": s50,
        "sma_200": s200,
        "sma_spread": sma_spread,
        "bb_upper": bb_upper,
        "bb_middle": bb_mid,
        "bb_lower": bb_lower,
        "bb_width": bb_width,
        "drawdown_from_ath": drawdown_from_ath,
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
    en caso contrario, cae a la Red Bayesiana original (Camino A / fallback).

    Parámetros
    ----------
    evidence      : {Sentiment, RSI, Trend, Volatility}
    macro_adj     : float — ajuste macro del día
    macro_context : dict completo con risk_regime, macro_sentiment, etc.
    extra         : dict con features opcionales para el discriminador:
                    prob_up_bn, signal_streak, prob_up_delta, prob_up_5d_mean,
                    vol_20d, vol_ratio, sentiment_dispersion
    """
    # ── Camino B: LightGBM discriminativo ─────────────────────────────────────
    if _disc_engine is not None and getattr(_disc_engine, "available", False):
        try:
            mc = macro_context or {"macro_adjustment": macro_adj}
            if "macro_adjustment" not in mc:
                mc = dict(mc)
                mc["macro_adjustment"] = macro_adj

            # Calcular prob_up de la BN para usarla como feature del discriminador
            from pgmpy.inference import VariableElimination
            _result_bn = VariableElimination(get_bn_model()).query(
                variables=["MarketDirection"], evidence=evidence, show_progress=False
            )
            extra_ctx = dict(extra or {})
            extra_ctx["prob_up_bn"] = float(_result_bn.values[1])

            prob_up = _disc_engine.infer(evidence, mc, extra_ctx)

            # El motor discriminativo opera con retorno relativo a la mediana
            # por ticker (target balanceado 50/50).  Su distribución de salida
            # empírica es bimodal en [0.49–0.50] (bajista) y [0.55–0.61] (alcista).
            # Se usan umbrales calibrados a esa distribución, distintos a los de la BN:
            #   BUY  : prob > 0.55 → el modelo predice retorno superior a la mediana
            #   SELL : prob < 0.50 → el modelo predice retorno inferior a la mediana
            #   HOLD : zona de incertidumbre (0.50 – 0.55)
            _DISC_BUY  = 0.55
            _DISC_SELL = 0.50
            if prob_up >= _DISC_BUY:
                signal = "BUY"
            elif prob_up <= _DISC_SELL:
                signal = "SELL"
            else:
                signal = "HOLD"
            logger.debug(
                f"[DiscEngine] prob_up={prob_up:.4f} → {signal} "
                f"(buy≥{_DISC_BUY}, sell≤{_DISC_SELL})"
            )
            return signal, prob_up
        except Exception as _exc:
            logger.warning(f"[DiscEngine] fallback a BN: {_exc}")

    # ── Fallback: Red Bayesiana (Camino A / original) ─────────────────────────
    from pgmpy.inference import VariableElimination

    infer = VariableElimination(get_bn_model())
    result = infer.query(
        variables=["MarketDirection"], evidence=evidence, show_progress=False
    )
    prob_up_raw = float(result.values[1])

    # En tendencia alcista confirmada, amortiguamos el macro_adj negativo al 40%
    # para evitar que una noticia hawkish/macro saque al modelo de un uptrend válido.
    # El macro_adj positivo se aplica completo (no penalizamos la info alcista).
    effective_macro_adj = macro_adj
    if evidence.get("Trend") == "uptrend" and macro_adj < 0:
        effective_macro_adj = macro_adj * 0.40

    prob_up_adj = round(max(0.0, min(1.0, prob_up_raw + effective_macro_adj)), 4)
    if prob_up_adj >= BUY_THRESHOLD:
        signal = "BUY"
    elif prob_up_adj <= SELL_THRESHOLD:
        signal = "SELL"
    else:
        signal = "HOLD"
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
    # Floors conservadores: nunca salir del mercado salvo condición extrema.
    # "A menos que sea hipercatastrofista, mantener posición" — coste de oportunidad.
    # BULL/NEUTRAL: siempre invertido ≥ 50-70%. BEAR: reducir pero no salir (≥15%).
    # Solo PANIC (VIX > 45) permite reducir a 5%, ningún régimen llega a 0%.
    FLOORS   = {"BULL": 0.70, "NEUTRAL": 0.50, "HIGH_VOL": 0.30, "BEAR": 0.15}
    CEILINGS = {"BULL": 1.00, "NEUTRAL": 0.85, "HIGH_VOL": 0.65, "BEAR": 0.50}
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


# =============================================================================
# FASE 2A — MEJORAS DE EXPOSURE ENGINE (basadas en literatura académica)
# =============================================================================

def classify_vix_regime(vix: Optional[float]) -> Tuple[str, float]:
    """
    Clasificación multi-nivel del VIX con ajuste macro no lineal.
    Sustituye el binario RISK_OFF/RISK_ON por 6 niveles con magnitudes distintas.
    Ref: evita que VIX=26 y VIX=55 reciban el mismo ajuste de -0.04.
    """
    if vix is None:
        return "NEUTRAL", 0.0
    if vix < 14:   return "RISK_ON_STRONG",  +0.07
    if vix < 18:   return "RISK_ON",          +0.04
    if vix < 22:   return "NEUTRAL",          +0.00
    if vix < 28:   return "RISK_OFF_MILD",    -0.03
    if vix < 35:   return "RISK_OFF",         -0.06
    if vix < 45:   return "FEAR",             -0.10
    return             "PANIC",               -0.15


def smooth_exposure_v2(target: float, previous: float,
                        alpha_up: float = 0.15, alpha_down: float = 0.35) -> float:
    """
    EWM asimétrico: las reducciones de exposición son más rápidas que los aumentos.
    Principio: 'Cut risk fast, re-enter carefully' (estándar en fondos sistemáticos).
    alpha_down > alpha_up garantiza asimetría de gestión de riesgo.
    """
    alpha = alpha_down if target < previous else alpha_up
    return round(alpha * target + (1.0 - alpha) * previous, 4)


def compute_vol_regime_features(close_series, vix: Optional[float]) -> dict:
    """
    Volatilidad realizada multi-escala del propio activo.
    VIX mide vol implícita del S&P500, no del activo individual.
    vol_ratio > 1.5 = volatilidad acelerando (señal de transición de régimen).
    vol_percentile = posición de la vol actual en el histórico del activo.
    """
    try:
        if not hasattr(close_series, 'pct_change'):
            close_series = pd.Series(close_series)
        returns = close_series.pct_change().dropna()
        if len(returns) < 21:
            return {}
        ann = np.sqrt(252)
        vol_5  = float(returns.tail(5).std()  * ann)
        vol_20 = float(returns.tail(20).std() * ann)
        vol_60 = float(returns.tail(60).std() * ann) if len(returns) >= 60 else vol_20
        vol_ratio = round(vol_5 / (vol_20 + 1e-9), 3)
        # Percentil histórico (1 año)
        hist_vols = returns.tail(252).rolling(20).std().dropna() * ann
        vol_pct = float((hist_vols < vol_20).mean()) if len(hist_vols) > 10 else 0.5
        vix_vs_realized = round(vix / (vol_20 * 100 + 1e-9), 2) if vix else None
        return {
            "vol_5d": round(vol_5, 4),
            "vol_20d": round(vol_20, 4),
            "vol_60d": round(vol_60, 4),
            "vol_ratio": vol_ratio,
            "vol_percentile_1y": round(vol_pct, 3),
            "vix_vs_realized": vix_vs_realized,
        }
    except Exception:
        return {}


def compute_volatility_target_exposure(
    close_series,
    target_annual_vol: float = 0.15,
    vol_lookback_fast: int = 10,
    vol_lookback_slow: int = 60,
    blend_fast: float = 0.5,
) -> dict:
    """
    Volatility Targeting: exposure = target_vol / realized_vol_blended.
    Estándar en fondos sistemáticos (AQR, Two Sigma). No requiere parámetros por activo.
    Ref: Concretum Group (2024) — comparativa VT vs VP en 40 mercados de futuros.
    """
    try:
        if not hasattr(close_series, 'pct_change'):
            close_series = pd.Series(close_series)
        returns = close_series.pct_change().dropna()
        if len(returns) < vol_lookback_slow:
            return {"vt_exposure": 1.0, "vol_blended": None}
        ann = np.sqrt(252)
        vol_fast = float(returns.tail(vol_lookback_fast).std() * ann)
        vol_slow = float(returns.tail(vol_lookback_slow).std() * ann)
        vol_blended = blend_fast * vol_fast + (1 - blend_fast) * vol_slow
        vol_blended = max(vol_blended, 0.01)
        vt_exp = round(min(1.0, target_annual_vol / vol_blended), 3)
        return {
            "vt_exposure": vt_exp,
            "vol_blended": round(vol_blended, 4),
            "vol_fast": round(vol_fast, 4),
            "vol_slow": round(vol_slow, 4),
        }
    except Exception:
        return {"vt_exposure": 1.0, "vol_blended": None}


def compute_win_loss_stats(close_series, window: int = 252) -> dict:
    """Estadísticas de ganancia/pérdida media del activo desde datos históricos."""
    try:
        if not hasattr(close_series, 'pct_change'):
            close_series = pd.Series(close_series)
        returns = close_series.pct_change().dropna().tail(window)
        wins   = returns[returns > 0]
        losses = returns[returns < 0]
        return {
            "avg_win":  float(wins.mean())         if len(wins)   > 0 else 0.005,
            "avg_loss": float(abs(losses.mean()))  if len(losses) > 0 else 0.005,
            "win_rate": float(len(wins) / len(returns)) if len(returns) > 0 else 0.5,
        }
    except Exception:
        return {"avg_win": 0.005, "avg_loss": 0.005, "win_rate": 0.5}


def kelly_exposure(prob_up: float, avg_win: float, avg_loss: float,
                    kelly_fraction: float = 0.35) -> float:
    """
    Fractional Kelly Criterion para sizing de posición.
    f* = p/L - q/W  (Kelly completo), f = kelly_fraction * f*
    Ref: Kelly (1956), Ed Thorp. kelly_fraction=0.35 es conservador y práctico.
    Devuelve 0 si no hay edge estadístico (Kelly negativo).
    """
    p = max(0.01, min(0.99, prob_up))
    q = 1.0 - p
    W = max(0.001, avg_win)
    L = max(0.001, avg_loss)
    f_full = (p / L) - (q / W)
    return round(float(max(0.0, min(1.0, kelly_fraction * f_full))), 3)


def update_regime_confirmation(
    raw_regime: str,
    state: dict,
) -> Tuple[str, dict]:
    """
    Confirmación temporal de régimen para evitar whipsaw.
    BEAR/HIGH_VOL se confirman en 1 día (riesgo = reacción rápida).
    BULL requiere 3 días consecutivos (evita falsas rupturas).
    NEUTRAL requiere 2 días.
    Basado en: Statistical Jump Model (Shu, Yu & Mulvey, Princeton 2024).
    """
    CONFIRM_DAYS = {"BULL": 3, "NEUTRAL": 2, "HIGH_VOL": 1, "BEAR": 1}
    if raw_regime == state.get("candidate"):
        state["days"] = state.get("days", 0) + 1
    else:
        state["candidate"] = raw_regime
        state["days"] = 1
    required = CONFIRM_DAYS.get(raw_regime, 2)
    if state["days"] >= required:
        state["confirmed"] = raw_regime
    return state.get("confirmed", "NEUTRAL"), state


def compute_decayed_sentiment(
    sentiment_samples: list,
    reference_date,
    half_life_days: float = 2.5,
) -> dict:
    """
    Sentimiento ponderado por antigüedad con función de decaimiento exponencial.
    Noticias de hoy pesan más que las de hace 3 días.
    También calcula dispersión: alta dispersión = artículos contradictorios = incertidumbre.
    Ref: Kargarzadeh (2024) — decay functions mejoran Sharpe de 3.64 a 5.10.
    sentiment_samples: lista de dicts con 'sentiment', 'confidence', opcionalmente 'date'.
    """
    if not sentiment_samples:
        return {"sentiment": 0.0, "dispersion": 0.0, "n_articles": 0, "decay_applied": False}
    decay_rate = float(np.log(2) / half_life_days)
    sentiment_map = {"bullish": 1.0, "neutral": 0.0, "bearish": -1.0}
    weighted_s, weights = [], []
    ref = pd.Timestamp(str(reference_date))
    for item in sentiment_samples:
        s_val = sentiment_map.get(item.get("sentiment", "neutral"), 0.0)
        conf  = float(item.get("confidence", 1.0))
        try:
            item_date = pd.Timestamp(str(item.get("date", reference_date)))
            days_ago = max(0, (ref - item_date).days)
        except Exception:
            days_ago = 0
        weight = float(np.exp(-decay_rate * days_ago)) * conf
        weighted_s.append(s_val)
        weights.append(weight)
    weights_arr = np.array(weights)
    sents_arr   = np.array(weighted_s)
    total_w = weights_arr.sum()
    if total_w < 1e-9:
        return {"sentiment": 0.0, "dispersion": 0.0, "n_articles": len(sentiment_samples), "decay_applied": True}
    w_mean = float((sents_arr * weights_arr).sum() / total_w)
    w_var  = float(((sents_arr - w_mean) ** 2 * weights_arr).sum() / total_w)
    dispersion = float(np.sqrt(w_var))
    return {
        "sentiment":   round(w_mean, 4),
        "dispersion":  round(dispersion, 4),
        "n_articles":  len(sentiment_samples),
        "decay_applied": True,
    }


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

        # Día 0: 10.000 € en cash, exposición 0%.
        # La curva de equity refleja cómo el capital crece al ir aumentando la exposición
        # según los indicadores, sin nunca salir completamente del mercado una vez invertido.
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

        # Empezar siempre en cash (sin posición abierta).
        # La primera señal BUY abre la posición; SELL cierra pero no va a 0
        # porque en la práctica el floor de exposición siempre mantiene algo.
        current_position = 0
        entry_p = 0.0

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
    regime_confirmation_state: dict = None,  # Fase 2A: estado de confirmación temporal
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
            if ohlcv_df is None:
                logger.warning(f"[skip] {ticker} {date_str}: sin OHLCV en prefetch")
            else:
                target_dt = pd.to_datetime(date_str)
                n_rows = len(ohlcv_df[ohlcv_df.index <= target_dt])
                if n_rows < 50:
                    logger.warning(
                        f"[skip] {ticker} {date_str}: solo {n_rows} filas OHLCV (min 50)"
                    )
                else:
                    logger.warning(
                        f"[skip] {ticker} {date_str}: indicadores None con {n_rows} filas"
                    )
            return None

        # ── OHLCV → MongoDB + Aurora ────────────────────────────────────────
        target_dt = pd.to_datetime(date_str)
        ohlcv_df = _normalize_ohlcv_index(ohlcv_df)
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
        finnhub_arts = news_all.get(ticker, {}).get(date_str, [])
        alphavantage_arts = alphavantage_news.get(ticker, {}).get(date_str, [])
        newsapi_arts = newsapi_ticker_news.get(ticker, {}).get(date_str, [])
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

        rsi_val = ind.get("rsi_14")
        evidence = {
            "Sentiment": dom_sent,
            "RSI": (
                "oversold"
                if rsi_val is not None and rsi_val < 30
                else ("overbought" if rsi_val is not None and rsi_val > 70 else "neutral")
            ),
            "Trend": (
                "uptrend"
                if (ind["sma_20"] and ind["sma_50"] and ind["sma_20"] > ind["sma_50"])
                else "downtrend"
            ),
            "Volatility": (
                "high" if (ind["bb_width"] and ind["bb_width"] > 0.05) else "low"
            ),
        }

        # ── Inferencia (Camino B si disponible, fallback BN) ────────────────
        # vol_features no está disponible aquí (se calcula post-inferencia);
        # pasamos los datos de sentimiento que sí tenemos.
        _disc_extra = {
            "sentiment_dispersion": (
                sentiment_detail.get("dispersion")
                if isinstance(sentiment_detail, dict) else None
            ),
            "signal_streak": len(signal_history),
        }
        _macro_ctx = {
            "macro_adjustment": macro_adj,
            "macro_sentiment":  macro_sentiment,
            "risk_regime":      risk_regime,
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

        # ── Exposure Management (Fase 2A) ────────────────────────────────────
        # Obtener serie de precios del activo para cálculos de vol
        _close_series_exp = None
        try:
            _df_exp = ohlcv_all.get(ticker)
            if _df_exp is not None and "Close" in _df_exp.columns:
                _close_series_exp = _df_exp["Close"].dropna()
        except Exception:
            pass

        # VIX multi-nivel (reemplaza binario RISK_OFF/ON)
        vix_regime_label_exp, macro_raw_adj_v2 = classify_vix_regime(vix)

        # Régimen con confirmación temporal (anti-whipsaw)
        raw_regime_exp = detect_market_regime(
            sma50=ind.get("sma_50"),
            sma200=ind.get("sma_200"),
            vix=vix,
            drawdown_from_ath=ind.get("drawdown_from_ath"),
        )
        conf_state = regime_confirmation_state or {"candidate": "NEUTRAL", "days": 0, "confirmed": "NEUTRAL"}
        confirmed_regime_exp, new_conf_state = update_regime_confirmation(raw_regime_exp, conf_state)

        # Volatility Targeting
        vt_result = {}
        if _close_series_exp is not None and len(_close_series_exp) >= 60:
            vt_result = compute_volatility_target_exposure(_close_series_exp)
        vt_exp = vt_result.get("vt_exposure", 1.0)

        # Fractional Kelly
        wl_stats = {"avg_win": 0.005, "avg_loss": 0.005}
        if _close_series_exp is not None and len(_close_series_exp) >= 60:
            wl_stats = compute_win_loss_stats(_close_series_exp)
        k_exp = kelly_exposure(prob_up, wl_stats["avg_win"], wl_stats["avg_loss"])

        # Exposición base desde prob_up (con régimen confirmado, no raw)
        target_exposure = prob_to_exposure(prob_up, confirmed_regime_exp)

        # Combinar: VT limita el techo, Kelly y prob_up modulan dentro de ese techo
        # VT es el sizing "seguro" dado el riesgo actual del activo.
        # Kelly y target_exposure determinan si usar más o menos de ese techo.
        # exposure_combined = VT_exp * (0.5 + prob_up) clamped al régimen
        exposure_combined = round(min(vt_exp, target_exposure * (0.5 + prob_up)), 3)
        # Ajuste macro multi-nivel (amortiguado en uptrend para activos no-hedge)
        if confirmed_regime_exp == "BULL" and macro_raw_adj_v2 < 0:
            effective_macro_v2 = macro_raw_adj_v2 * 0.40
        else:
            effective_macro_v2 = macro_raw_adj_v2
        exposure_combined = round(max(0.05, min(1.0, exposure_combined + effective_macro_v2)), 3)

        # Sentimiento con decaimiento temporal
        decayed_sent = compute_decayed_sentiment(sentiment_samples, date_str)
        sentiment_dispersion = decayed_sent.get("dispersion", 0.0)

        # Suavizado asimétrico (Fase 2A — reemplaza smooth_exposure simétrico)
        smoothed_exposure = smooth_exposure_v2(
            target=exposure_combined,
            previous=previous_exposure,
            alpha_up=0.15,
            alpha_down=0.35,
        )
        exposure_delta = round(smoothed_exposure - previous_exposure, 4)

        # Vol features para diagnóstico y frontend
        vol_features = {}
        if _close_series_exp is not None:
            vol_features = compute_vol_regime_features(_close_series_exp, vix)

        logger.debug(
            f"[EXPOSURE-V2] {ticker} {date_str}: raw={raw_regime_exp} confirmed={confirmed_regime_exp} "
            f"vix_label={vix_regime_label_exp} vt={vt_exp:.3f} kelly={k_exp:.3f} "
            f"target={exposure_combined:.3f} smooth={smoothed_exposure:.3f} Δ={exposure_delta:+.3f}"
        )

        # Persistir en position_state con campos enriquecidos
        try:
            pg_upsert_position_state(
                thread_conn, date_str, ticker, prob_up,
                confirmed_regime_exp, exposure_combined, smoothed_exposure, exposure_delta,
                confirmed_regime=confirmed_regime_exp,
                raw_regime=raw_regime_exp,
                regime_candidate=new_conf_state.get("candidate"),
                regime_candidate_days=new_conf_state.get("days"),
                vt_exposure=vt_exp,
                kelly_exp=k_exp,
                vol_5d=vol_features.get("vol_5d"),
                vol_20d=vol_features.get("vol_20d"),
                vol_ratio=vol_features.get("vol_ratio"),
                vol_percentile=vol_features.get("vol_percentile_1y"),
                sentiment_dispersion=sentiment_dispersion,
                vix_regime_label=vix_regime_label_exp,
            )
        except Exception as exc:
            logger.warning(f"[EXPOSURE-V2] position_state upsert falló {ticker} {date_str}: {exc}")

        # ── Trace + persistencia ─────────────────────────────────────────────
        trace_data = {
            "raw_values": {
                "close_price": ind["close"],
                "rsi_14": ind["rsi_14"],
                "sma_20": ind["sma_20"],
                "sma_50": ind["sma_50"],
                "bb_upper": ind["bb_upper"],
                "bb_lower": ind["bb_lower"],
                "bb_width_ratio": ind["bb_width"],
            },
            "discretization": {
                "sentiment_raw": dom_sent,
                "sentiment_conf": best_conf,
                "sentiment_state": evidence["Sentiment"],
                "rsi_state": evidence["RSI"],
                "trend_state": evidence["Trend"],
                "volatility_state": evidence["Volatility"],
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
            "exposure_management": {
                "market_regime": confirmed_regime_exp,
                "raw_regime": raw_regime_exp,
                "confirmed_regime": confirmed_regime_exp,
                "vix_regime_label": vix_regime_label_exp,
                "target_exposure": exposure_combined,
                "vt_exposure": vt_exp,
                "kelly_exposure": k_exp,
                "smoothed_exposure": smoothed_exposure,
                "exposure_delta": exposure_delta,
                "previous_exposure": previous_exposure,
                "sentiment_dispersion": sentiment_dispersion,
                "vol_ratio": vol_features.get("vol_ratio"),
                "vol_20d": vol_features.get("vol_20d"),
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
            "batch_date": date_str,
            "ticker": ticker,
            "run_id": run_id,
            "signal": signal,
            "prob_up": prob_up,
            "prob_down": round(1 - prob_up, 4),
            "close_price": ind["close"],
            "sentiment_state": evidence["Sentiment"],
            "rsi_state": evidence["RSI"],
            "trend_state": evidence["Trend"],
            "volatility_state": evidence["Volatility"],
            "macro_sentiment": macro_sentiment,
            "risk_regime": risk_regime,
            "macro_adjustment": macro_adj,
            # Fase 1: exposición continua
            "market_regime": confirmed_regime_exp,
            "target_exposure": exposure_combined,
            "smoothed_exposure": smoothed_exposure,
            # Fase 2A: campos enriquecidos
            "confirmed_regime": confirmed_regime_exp,
            "raw_regime": raw_regime_exp,
            "vix_regime_label": vix_regime_label_exp,
            "vt_exposure": vt_exp,
            "kelly_exposure": k_exp,
            "vol_ratio": vol_features.get("vol_ratio"),
            "sentiment_dispersion": sentiment_dispersion,
        }

        return {
            "ticker": ticker,
            "trace_data": trace_data,
            "signal_record": signal_record,
            "new_history": new_history,
            "kpis": kpis,
            "smoothed_exposure": smoothed_exposure,   # para actualizar exposure_history_per_ticker
            "market_regime": confirmed_regime_exp,
            "new_conf_state": new_conf_state,         # Fase 2A: estado de confirmación temporal
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
    logger.info(
        f"PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']} "
        f"(user={DB_CONFIG['user']})"
    )
    if DEBUG_NEWS:
        logger.info(
            f"🔎 Debug noticias activo | headlines={DEBUG_NEWS_HEADLINES} | "
            f"refresh_cache={REFRESH_NEWS_CACHE}"
        )

    # Motor de inferencia activo (Camino B = LightGBM; Camino A = BN fallback)
    if _disc_engine is not None:
        _disc_engine.load()
    _using_lgbm = (
        _disc_engine is not None and getattr(_disc_engine, "available", False)
    )
    if _using_lgbm:
        _meta = _disc_engine.meta or {}
        logger.info(
            f"🧠 Motor de inferencia: LightGBM (discriminative_lgbm) "
            f"| AUC={_meta.get('auc_val', 'N/A')} "
            f"| entrenado {_meta.get('trained_at', '?')[:10]}"
        )
    else:
        logger.warning(
            "🧠 Motor de inferencia: Red Bayesiana (fallback) — "
            "LightGBM no disponible. Entrena o copia modelos en models/ "
            "(lgbm_booster.txt) y comprueba pip install lightgbm."
        )

    # ── 1. DESCARGA INICIAL DE DATOS ──
    # Para poder calcular indicadores en start_d, la lambda descarga días extra.
    ohlcv_all = fetch_ohlcv_all(active_tickers, start_d, end_d)
    missing_ohlcv = [t for t in active_tickers if t not in ohlcv_all]
    if missing_ohlcv:
        logger.error(
            f"Abortando pipeline: sin datos OHLCV para {missing_ohlcv}. "
            "Comprueba red/yfinance y reintenta (p. ej. --verbose)."
        )
        sys.exit(1)
    vix_series = fetch_vix_historical(start_d, end_d)
    if vix_series.empty:
        logger.warning(
            "[VIX] serie vacía tras descarga — macro risk_regime sin VIX (comprueba red/yfinance)"
        )
    elif logger.isEnabledFor(logging.INFO):
        logger.info(
            f"[VIX] {len(vix_series)} días ({vix_series.index.min().date()} → "
            f"{vix_series.index.max().date()})"
        )

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
            # Fase 2A: añadir columnas enriquecidas si no existen
            c.execute("""
                ALTER TABLE position_state
                    ADD COLUMN IF NOT EXISTS confirmed_regime     VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS raw_regime           VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS regime_candidate     VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS regime_candidate_days INTEGER,
                    ADD COLUMN IF NOT EXISTS vt_exposure          FLOAT,
                    ADD COLUMN IF NOT EXISTS kelly_exposure       FLOAT,
                    ADD COLUMN IF NOT EXISTS vol_5d               FLOAT,
                    ADD COLUMN IF NOT EXISTS vol_20d              FLOAT,
                    ADD COLUMN IF NOT EXISTS vol_ratio            FLOAT,
                    ADD COLUMN IF NOT EXISTS vol_percentile_1y    FLOAT,
                    ADD COLUMN IF NOT EXISTS sentiment_dispersion FLOAT,
                    ADD COLUMN IF NOT EXISTS vix_regime_label     VARCHAR(25)
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
    # Empezamos en 0.0 (100% cash) y el sistema rampa hacia la exposición objetivo
    # a medida que las señales se acumulan. smooth_exposure_v2 con alpha_up=0.15
    # garantiza una entrada gradual (~7 días para alcanzar el 65% del target).
    exposure_history_per_ticker: dict = {t: 0.0 for t in active_tickers}
    # Acumulado de todos los signal_records (incluye smoothed_exposure) para
    # calcular el backtesting de exposición en cada iteración del reporte.
    all_signal_records: List[Dict] = []

    # ── Fase 2A — Estado de confirmación temporal de régimen por ticker ───────
    # Evita whipsaw: BULL requiere 3 días, NEUTRAL 2, BEAR/HIGH_VOL 1.
    regime_confirmation_per_ticker: dict = {
        t: {"candidate": "NEUTRAL", "days": 0, "confirmed": "NEUTRAL"}
        for t in active_tickers
    }

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

        vix = vix_on_or_before(vix_series, bd)
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
                regime_confirmation_per_ticker.get(ticker), # Fase 2A: estado confirmación
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
            if result.get("new_conf_state"):                              # Fase 2A
                regime_confirmation_per_ticker[t] = result["new_conf_state"]
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
            _engine_name = (
                "discriminative_lgbm"
                if (_disc_engine is not None and getattr(_disc_engine, "available", False))
                else "bayesian_network"
            )
            _disc_meta = (
                _disc_engine.meta
                if (_disc_engine is not None and getattr(_disc_engine, "available", False))
                else None
            )
            report_data = {
                "report_date": date_str,
                "pipeline_start": start_d.isoformat(),
                "pipeline_end": end_d.isoformat(),
                "data_period_days": period_days,
                "generated_at": datetime.now().isoformat(),
                "inference_engine": _engine_name,
                "disc_model_meta": {
                    "auc_val":        _disc_meta.get("auc_val")        if _disc_meta else None,
                    "trained_at":     _disc_meta.get("trained_at")     if _disc_meta else None,
                    "n_obs":          _disc_meta.get("n_obs")          if _disc_meta else None,
                    "target":         _disc_meta.get("target_definition", "outcome_d3_up") if _disc_meta else None,
                    "disc_buy_th":    0.55,
                    "disc_sell_th":   0.50,
                } if _disc_meta else None,
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
                    "period_days": period_days,
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
