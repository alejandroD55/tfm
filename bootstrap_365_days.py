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
import logging
import warnings
from datetime import datetime, timedelta, timezone, date
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
# newsapi eliminado — se usa GDELT (gratuito, sin API key, acceso histórico completo)
import feedparser
from psycopg2.extensions import AsIs
import argparse

warnings.filterwarnings("ignore")

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_REPO_ROOT, "shared"))

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("trafilatura").setLevel(logging.CRITICAL)

load_dotenv()

def get_args():
    parser = argparse.ArgumentParser(description="TFM Backtester Local")
    parser.add_argument("--start", type=str, help="Fecha inicio YYYY-MM-DD", default=None)
    parser.add_argument("--end", type=str, help="Fecha fin YYYY-MM-DD", default=None)
    return parser.parse_args()

# =============================================================================
# VARIABLES Y CONFIGURACIÓN
# =============================================================================
TICKERS        = ["SPY", "IWM", "GLD", "XLE", "NVDA"]
DAYS_BACK      = 365
INITIAL_CAP    = 10_000.0
RISK_FREE_RATE = 0.02

FINNHUB_API_KEY   = os.getenv("FINNHUB_API_KEY", "")
MONGODB_URI       = os.getenv("MONGODB_URI", "")
GROQ_API_KEY      = os.getenv("GROQ_API_KEY", "")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "user":     os.getenv("POSTGRES_USER",     "tfmadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "localpassword123"),
    "database": os.getenv("POSTGRES_DB",       "tfm"),
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
    
MACRO_QUERIES = {
    "monetary_policy": ["Federal Reserve interest rates decision", "Fed hawkish dovish monetary policy", "ECB interest rates inflation Europe"],
    "inflation": ["CPI inflation data consumer prices", "PCE inflation Federal Reserve target"],
    "macro_economy": ["GDP growth recession economic outlook", "unemployment jobs report nonfarm payrolls"],
    "geopolitical": ["geopolitical tensions war conflict markets", "Russia Ukraine war sanctions economy"],
}

RSS_FEEDS = {
    "reuters": "https://feeds.reuters.com/reuters/businessNews",
    "cnbc": "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "ft": "https://www.ft.com/world?format=rss"
}

def _fingerprint(url: str, headline: str) -> str:
    import hashlib
    key = (url or headline or "").strip().lower()
    return hashlib.md5(key.encode()).hexdigest()

def _normalize_macro(headline, url, source, dt, category, query_tag, summary=""):
    return {
        "headline": headline.strip(), "url": url, "source": source,
        "datetime": dt, "summary": summary, "category": category, "query_tag": query_tag
    }

MODEL_CONFIG = {
    "version": "1.1.0",
    "description": "Red bayesiana con Momentum: Sentiment, RSI, Trend, Volatility -> MarketDirection",
    "discretization": {
        "rsi": {"oversold_below": 30, "overbought_above": 70, "neutral_range": [30, 70]},
        "trend": {"rule": "SMA20 > SMA50 = uptrend"},
        "volatility": {"high_if_band_width_ratio_above": 0.05}
    },
    "signal_thresholds": {"BUY": {"prob_up_above": 0.58}, "SELL": {"prob_up_below": 0.42}, "HOLD": {"range": [0.42, 0.58]}},
    "priors": {
        "Sentiment": {"bullish": 0.30, "bearish": 0.30, "neutral": 0.40},
        "RSI": {"oversold": 0.20, "neutral": 0.60, "overbought": 0.20},
        "Trend": {"uptrend": 0.50, "downtrend": 0.50},
        "Volatility": {"low": 0.60, "high": 0.40},
    },
    "cpt_market_direction": {
        "variable": "MarketDirection", "states": ["down", "up"],
        "values_P_down": [0.15,0.25,0.30,0.20,0.30,0.35,0.30,0.40,0.10,0.15,0.45,0.50,
                          0.70,0.75,0.80,0.75,0.80,0.85,0.80,0.85,0.50,0.55,0.90,0.95,
                          0.45,0.50,0.55,0.50,0.55,0.60,0.55,0.60,0.25,0.30,0.65,0.70],
        "values_P_up":   [0.85,0.75,0.70,0.80,0.70,0.65,0.70,0.60,0.90,0.85,0.55,0.50,
                          0.30,0.25,0.20,0.25,0.20,0.15,0.20,0.15,0.50,0.45,0.10,0.05,
                          0.55,0.50,0.45,0.50,0.45,0.40,0.45,0.40,0.75,0.70,0.35,0.30],
    },
    "known_limitations": ["El confidence score de FinBERT no entra en la inferencia", "Voto mayoritario"]
}

BUY_THRESHOLD  = MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"]
SELL_THRESHOLD = MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"]

# =============================================================================
# IA LOCAL: FINBERT & GROQ
# =============================================================================
_finbert_pipeline = None
_groq_client = None

def get_finbert():
    global _finbert_pipeline
    if _finbert_pipeline is None:
        logger.info("Cargando FinBERT local en RAM...")
        from transformers import pipeline as hf_pipeline
        _finbert_pipeline = hf_pipeline("text-classification", model="ProsusAI/finbert", truncation=True, max_length=512)
    return _finbert_pipeline

def get_groq_client():
    global _groq_client
    if _groq_client is None and GROQ_API_KEY:
        _groq_client = groq.Groq(api_key=GROQ_API_KEY)
    return _groq_client

def extract_and_summarize(ticker: str, headline: str, url: str) -> str:
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
            time.sleep(2) 
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[{"role": "system", "content": "You are a financial analyst."}, {"role": "user", "content": prompt}],
                temperature=0.0, max_tokens=150
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.debug(f"Groq falló: {e}")
    return headline

def analyze_sentiment_local(headline: str) -> Optional[Dict]:
    if not headline or len(headline.strip()) < 20: return None
    try:
        results = get_finbert()(headline)[0]
        label, score = results["label"].lower(), float(results["score"])
        if score < 0.55: return None
        lmap = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
        return {"sentiment": lmap.get(label, "neutral"), "confidence": round(score, 4), "justification": f"FinBERT local"}
    except Exception:
        return None

def run_finbert_macro_local(articles: list) -> dict:
    if not articles:
        return {"score": 0.0, "state": "neutral", "distribution": {}, "n_articles": 0}
    
    weighted_sum = 0.0; weight_total = 0.0; scored = 0
    distribution = {"bullish": 0, "neutral": 0, "bearish": 0}
    finbert = get_finbert()
    
    for art in articles[:50]:
        headline = art.get("headline", "")
        if len(headline) < 20: continue
        try:
            res = finbert(headline)[0]
            score = float(res['score'])
            label = res['label'].lower()
            if score < 0.55: continue
            
            s_map = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
            sent = s_map.get(label, "neutral")
            val = 1.0 if sent == "bullish" else (-1.0 if sent == "bearish" else 0.0)
            
            weighted_sum += val * score
            weight_total += 1.0
            scored += 1
            distribution[sent] += 1
        except Exception: pass
        
    if weight_total == 0: return {"score": 0.0, "state": "neutral", "distribution": distribution, "n_articles": 0}
    f_score = weighted_sum / weight_total
    state = "bullish" if f_score > 0.20 else ("bearish" if f_score < -0.20 else "neutral")
    return {"score": round(f_score, 4), "state": state, "distribution": distribution, "n_articles": scored}

def aggregate_sentiment_local(samples: list) -> Tuple[str, float, dict]:
    if not samples:
        return "neutral", 0.0, {}
    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    for s in samples:
        dist[s["sentiment"]] += 1
    total = len(samples)
    distribution = {k: {"count": v, "pct": round(v / total * 100, 1)} for k, v in dist.items()}
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
        "limitation": "Se utiliza Voto Mayoritario de todos los titulares del día."
    }
    return dominant_sentiment, best_conf, detail

def build_reasoning_local(evidence_states, prob_up, signal):
    parts = []
    s, r, t, v = (evidence_states.get(k) for k in ("Sentiment", "RSI", "Trend", "Volatility"))
    if s == "bullish": parts.append("sentimiento positivo")
    elif s == "bearish": parts.append("sentimiento negativo")
    
    if r == "overbought" and t == "uptrend": parts.append("Fuerte Momentum Alcista (RSI>70 + Tendencia)")
    elif r == "oversold": parts.append("RSI sobrevendido -> presion compradora")
    elif r == "overbought": parts.append("RSI sobrecomprado -> posible correccion")
    
    if t == "uptrend" and r != "overbought": parts.append("tendencia alcista (SMA20>SMA50)")
    elif t == "downtrend": parts.append("tendencia bajista (SMA20<SMA50)")
    
    if v == "high": parts.append("alta volatilidad")
    
    th = MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"] if signal == "BUY" else (MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"] if signal == "SELL" else MODEL_CONFIG["signal_thresholds"]["HOLD"]["range"])
    return f"Evidencias: {', '.join(parts) if parts else 'mixtas'}. P(subida)={prob_up:.2%} -> senal {signal} (umbral: {th})."

# =============================================================================
# MONGODB & AURORA HELPERS
# =============================================================================
from mongo_utils import (
    upsert_raw_news, upsert_ohlcv_bulk, upsert_news, upsert_filtered_news,
    upsert_bayesian_report, upsert_bayesian_trace, upsert_macro_context, 
    upsert_report, upsert_macro_news, read_macro_news
)

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"], user=DB_CONFIG["user"],
        password=DB_CONFIG["password"], database=DB_CONFIG["database"], sslmode="prefer"
    )

def pg_upsert_signal(conn, date_str, ticker, signal, prob_up, prob_down):
    with conn.cursor() as c:
        c.execute("""
            INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down) VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET signal=EXCLUDED.signal, prob_up=EXCLUDED.prob_up, prob_down=EXCLUDED.prob_down
        """, (date_str, ticker, signal, float(prob_up), float(prob_down)))
    conn.commit()

def pg_upsert_batch_log(conn, batch_date, run_id, tickers, status):
    try:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO batch_log (batch_date, run_id, trigger_type, status, tickers_processed)
                VALUES (%s, %s, 'manual', %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status, updated_at = CURRENT_TIMESTAMP
            """, (batch_date, run_id, status, len(tickers)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"Error batch_log: {e}")

def pg_upsert_pipeline_kpi(connection, batch_date, run_id, trigger_type, stage, metrics):
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO pipeline_kpis (batch_date, run_id, trigger_type, stage, metrics) 
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (run_id, stage) DO UPDATE SET metrics = EXCLUDED.metrics, updated_at = CURRENT_TIMESTAMP
    """, (batch_date, run_id, trigger_type, stage, json.dumps(metrics)))
    connection.commit()
    cursor.close()

# NUEVO: EXTRAER HISTÓRICO DE AURORA (Para clonar AWS lambda_report.py)
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
        signals_df = pd.DataFrame(
            cursor.fetchall(), columns=["batch_date", "ticker", "signal", "close_price"]
        )
        cursor.close()
        return signals_df
    except Exception:
        raise

# =============================================================================
# DATOS Y LÓGICA DE BACKTESTING
# =============================================================================
def fetch_ohlcv_all(tickers: List[str], start_date: date, end_date: date) -> Dict[str, pd.DataFrame]:
    # Descargamos con un "lookback" extra de 80 días para asegurar el cálculo de SMA50
    download_start = start_date - timedelta(days=80)
    result = {}
    for ticker in tickers:
        df = yf.download(ticker, start=str(download_start), end=str(end_date), progress=False)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
            df.index = pd.to_datetime(df.index)
            result[ticker] = df
    return result

def fetch_news_historical(ticker: str, start_d: date, end_d: date) -> Dict[str, List]:
    cache_file = CACHE_DIR / "news" / f"{ticker}_{start_d.strftime('%Y%m')}_{end_d.strftime('%Y%m')}.json"
    if cache_file.exists():
        with open(cache_file, encoding="utf-8") as fh: return json.load(fh)
    if not FINNHUB_API_KEY: return {}
    
    news_by_date, current = {}, start_d.replace(day=1)
    while current <= end_d:
        next_m = date(current.year + (current.month == 12), (current.month % 12) + 1, 1)
        month_end = min(next_m - timedelta(days=1), end_d)
        resp = requests.get("https://finnhub.io/api/v1/company-news", params={"symbol": ticker, "from": str(current), "to": str(month_end), "token": FINNHUB_API_KEY}, timeout=15)
        if resp.status_code == 200:
            for art in (resp.json() or []):
                dt = datetime.utcfromtimestamp(art.get("datetime", 0)).strftime("%Y-%m-%d")
                if art.get("headline"):
                    news_by_date.setdefault(dt, []).append({
                        "headline": art["headline"], 
                        "url": art.get("url", ""),
                        "source": art.get("source", "finnhub"),
                        "datetime": datetime.utcfromtimestamp(art.get("datetime", 0)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "summary": art.get("summary", "")
                    })
        time.sleep(1.2)
        current = next_m
    with open(cache_file, "w", encoding="utf-8") as fh: json.dump(news_by_date, fh)
    return news_by_date

def fetch_vix_historical(start_d: date, end_d: date) -> pd.Series:
    df = yf.download("^VIX", start=str(start_d - timedelta(days=5)), end=str(end_d + timedelta(days=1)), progress=False)
    if not df.empty:
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        df.index = pd.to_datetime(df.index)
        return df["Close"]
    return pd.Series(dtype=float)

def fetch_gdelt_news(query: str, target_date, n: int = 5) -> list:
    """
    Obtiene artículos de GDELT v2 para un query y fecha histórica (±1 día).
    GDELT es gratuito, no requiere API key y cubre noticias desde 2013.
    """
    start_dt = (target_date - timedelta(days=1)).strftime("%Y%m%d%H%M%S")
    end_dt   = (target_date + timedelta(days=1)).strftime("%Y%m%d%H%M%S")
    try:
        resp = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "query":         query,
                "mode":          "artlist",
                "maxrecords":    n,
                "startdatetime": start_dt,
                "enddatetime":   end_dt,
                "format":        "json",
                "sourcelang":    "english",
            },
            timeout=15
        )
        if resp.status_code == 200:
            return (resp.json() or {}).get("articles", [])
    except Exception as e:
        logger.debug(f"GDELT error para '{query}': {e}")
    return []


def ingest_macro_news(date_str):
    articles = []
    seen = set()
    target_date = pd.to_datetime(date_str).date()

    # ── GDELT: fuente histórica gratuita, sin API key, cubre todo el backtest ──
    for cat, queries in MACRO_QUERIES.items():
        for query_tag in queries:
            for art in fetch_gdelt_news(query_tag, target_date, n=5):
                url   = art.get("url", "")
                title = art.get("title", "")
                fp    = _fingerprint(url, title)
                if fp in seen or not title:
                    continue
                seen.add(fp)
                # seendate de GDELT tiene formato: YYYYMMDDTHHMMSSZ
                raw_dt = art.get("seendate", "")
                try:
                    pub_dt = datetime.strptime(raw_dt, "%Y%m%dT%H%M%SZ").isoformat()
                except Exception:
                    pub_dt = target_date.isoformat()
                articles.append(_normalize_macro(
                    title, url,
                    art.get("domain", "gdelt"),
                    pub_dt, cat, query_tag
                ))
            time.sleep(0.3)  # cortesía con la API pública de GDELT

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
                    articles.append(_normalize_macro(
                        entry.get("title"), 
                        entry.get("link"), 
                        name, 
                        datetime.strptime(date_str, "%Y-%m-%d").isoformat(), 
                        "macro", 
                        name, 
                        summary
                    ))
        except Exception: pass

    if articles:
        upsert_macro_news(date_str, articles)
    else:
        logger.info(f"[{date_str}] No se encontraron noticias macro en rango temporal para {date_str}.")

def calculate_indicators_for_date(ohlcv_df: pd.DataFrame, target_date: str) -> Optional[Dict]:
    try: import pandas_ta_classic as ta
    except ImportError: import pandas_ta as ta

    target_dt = pd.to_datetime(target_date)
    df = ohlcv_df[ohlcv_df.index <= target_dt].copy()
    if len(df) < 50: return None

    close  = df["Close"]
    rsi    = ta.rsi(close, length=14)
    sma_20 = ta.sma(close, length=20)
    sma_50 = ta.sma(close, length=50)
    bbands = ta.bbands(close, length=20, std=2)

    def last(s): return _sf(s.iloc[-1]) if s is not None and len(s) > 0 else None

    bb_upper = bb_mid = bb_lower = None
    if bbands is not None and not bbands.empty and len(bbands.columns) >= 3:
        bb_lower = _sf(bbands.iloc[-1, 0])
        bb_mid   = _sf(bbands.iloc[-1, 1])
        bb_upper = _sf(bbands.iloc[-1, 2])

    cl = _sf(close.iloc[-1])
    s20, s50 = last(sma_20), last(sma_50)
    sma_spread = round(float(s20) - float(s50), 4) if s20 and s50 else None
    bb_width = round((float(bb_upper) - float(bb_lower)) / float(cl), 6) if bb_upper and bb_lower and cl else None

    return {"close": cl, "rsi_14": last(rsi), "sma_20": s20, "sma_50": s50, "sma_spread": sma_spread, "bb_upper": bb_upper, "bb_middle": bb_mid, "bb_lower": bb_lower, "bb_width": bb_width}

def get_bn_model():
    from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork
    from pgmpy.factors.discrete import TabularCPD
    model = BayesianNetwork([("Sentiment","MarketDirection"), ("RSI","MarketDirection"), ("Trend","MarketDirection"), ("Volatility","MarketDirection")])
    c = MODEL_CONFIG["cpt_market_direction"]
    p = MODEL_CONFIG["priors"]
    model.add_cpds(
        TabularCPD("Sentiment", 3, [[p.get("Sentiment")["bullish"]], [p.get("Sentiment")["bearish"]], [p.get("Sentiment")["neutral"]]], state_names={"Sentiment": ["bullish", "bearish", "neutral"]}),
        TabularCPD("RSI", 3, [[p["RSI"]["oversold"]], [p["RSI"]["neutral"]], [p["RSI"]["overbought"]]], state_names={"RSI": ["oversold", "neutral", "overbought"]}),
        TabularCPD("Trend", 2, [[p["Trend"]["uptrend"]], [p["Trend"]["downtrend"]]], state_names={"Trend": ["uptrend", "downtrend"]}),
        TabularCPD("Volatility", 2, [[p["Volatility"]["low"]], [p["Volatility"]["high"]]], state_names={"Volatility": ["low", "high"]}),
        TabularCPD("MarketDirection", 2, values=[c["values_P_down"], c["values_P_up"]], evidence=["Sentiment","RSI","Trend","Volatility"], evidence_card=[3,3,2,2], state_names={"MarketDirection": ["down", "up"], "Sentiment": ["bullish", "bearish", "neutral"], "RSI": ["oversold", "neutral", "overbought"], "Trend": ["uptrend", "downtrend"], "Volatility": ["low", "high"]})
    )
    return model

def run_bayesian_inference(evidence: Dict, macro_adj: float) -> Tuple[str, float]:
    from pgmpy.inference import VariableElimination
    infer = VariableElimination(get_bn_model())
    result = infer.query(variables=["MarketDirection"], evidence=evidence, show_progress=False)
    prob_up_raw = float(result.values[1])
    prob_up_adj = round(max(0.0, min(1.0, prob_up_raw + macro_adj)), 4)
    if prob_up_adj >= BUY_THRESHOLD: signal = "BUY"
    elif prob_up_adj <= SELL_THRESHOLD: signal = "SELL"
    else: signal = "HOLD"
    return signal, prob_up_adj

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
            if price == 0: continue
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

            daily_eq = capital * (1 + (price - entry_p) / entry_p) if current_position == 1 and entry_p > 0 else capital
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

        metrics[ticker] = {"cumulative_return": round(float(cum_ret), 4), "sharpe_ratio": round(float(sharpe), 4), "max_drawdown": round(float(max_dd), 4), "final_equity": round(float(final_eq), 2)}
        
        wins = sum(1 for value in trades_rets if value > 0)
        gross_profit = sum(value for value in trades_rets if value > 0)
        gross_loss = abs(sum(value for value in trades_rets if value < 0))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 1e-9 else (gross_profit if gross_profit > 0 else 0.0)

        diagnostics[ticker] = {
            "signals": {"BUY": int(signals_count.get("BUY", 0)), "SELL": int(signals_count.get("SELL", 0)), "HOLD": int(signals_count.get("HOLD", 0))},
            "trades_closed": len(trades_rets),
            "win_rate": round(float(wins / len(trades_rets)), 4) if trades_rets else 0.0,
            "avg_trade_return": round(float(np.mean(trades_rets)), 4) if trades_rets else 0.0,
            "profit_factor": round(float(profit_factor), 4),
            "time_in_market_ratio": round(float(days_invested / max(len(ts), 1)), 4),
        }
    return metrics, diagnostics

def get_close_price(ticker: str, date_str: str) -> Optional[float]:
    try:
        target = pd.to_datetime(date_str).date()
        if target > datetime.now().date(): return None 
        start = target - timedelta(days=1)
        end   = target + timedelta(days=6)
        df = yf.download(ticker, start=start, end=end, progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.droplevel(1)
        df.index = pd.to_datetime(df.index).date
        candidates = [d for d in df.index if d >= target]
        if not candidates: return None
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
        
        cursor.execute("""
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
        """, (d0_str, ticker, sig["run_id"], sig["signal"], sig["prob_up"], sig["prob_down"],
              sig["sentiment_state"], sig["rsi_state"], sig["trend_state"], sig["volatility_state"], 
              float(p0) if p0 else None, 
              sig["macro_sentiment"], sig["risk_regime"], sig["macro_adjustment"]))

        for days, col_p, col_o, col_correct in [
            (1, "price_d1", "outcome_d1", "correct_d1"),
            (3, "price_d3", "outcome_d3", "correct_d3"),
            (5, "price_d5", "outcome_d5", "correct_d5"),
        ]:
            target_date = (d0 + timedelta(days=days)).strftime("%Y-%m-%d")
            
            if (ticker, target_date) not in price_cache:
                price_cache[(ticker, target_date)] = get_close_price(ticker, target_date)
            price_dn = price_cache[(ticker, target_date)]
            
            if price_dn and p0 and p0 > 0:
                change = (price_dn - p0) / p0
                outcome = "UP" if change > 0.005 else ("DOWN" if change < -0.005 else "FLAT")
                correct = (sig["signal"] == "BUY" and outcome == "UP") or \
                          (sig["signal"] == "SELL" and outcome == "DOWN") or \
                          (sig["signal"] == "HOLD" and outcome == "FLAT")
                
                cursor.execute(f"""
                    UPDATE signal_outcomes 
                    SET {col_p} = %s, {col_o} = %s, {col_correct} = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE batch_date = %s AND ticker = %s
                """, (float(price_dn), outcome, correct, d0_str, ticker))
    
    conn.commit()
    cursor.close()
    logger.info("✅ Outcomes históricos actualizados.")

def get_pipeline_health(connection, report_date, run_id):
    cursor = connection.cursor()
    cursor.execute("SELECT tickers_processed, status FROM batch_log WHERE run_id = %s LIMIT 1", (run_id,))
    batch_row = cursor.fetchone()
    if not batch_row:
        cursor.execute("SELECT tickers_processed, status FROM batch_log WHERE batch_date = %s ORDER BY updated_at DESC LIMIT 1", (report_date,))
        batch_row = cursor.fetchone()
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM technical_indicators WHERE batch_date = %s", (report_date,))
    indicator_tickers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM trading_signals WHERE batch_date = %s", (report_date,))
    signal_tickers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sentiment_scores WHERE batch_date = %s", (report_date,))
    headlines = cursor.fetchone()[0]
    cursor.execute("SELECT stage, metrics FROM pipeline_kpis WHERE run_id = %s", (run_id,))
    stage_metrics = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    tickers_expected = int(batch_row[0]) if batch_row and batch_row[0] is not None else 0
    return {
        "batch_status": batch_row[1] if batch_row else "UNKNOWN",
        "tickers_expected": tickers_expected,
        "tickers_with_indicators": int(indicator_tickers or 0),
        "tickers_with_signals": int(signal_tickers or 0),
        "headlines_scored": int(headlines or 0),
        "coverage_ratio": round(float((signal_tickers or 0) / tickers_expected), 4) if tickers_expected else 0.0,
        "stage_kpis": stage_metrics,
    }

def get_explanations_sample(connection, report_date, limit=10):
    cursor = connection.cursor()
    cursor.execute("""
        SELECT e.ticker, ts.signal, ts.prob_up, ts.prob_down, e.sentiment_state, e.rsi_state, e.trend_state, e.volatility_state
        FROM signal_explanations e JOIN trading_signals ts ON ts.batch_date = e.batch_date AND ts.ticker = e.ticker
        WHERE e.batch_date = %s ORDER BY ts.prob_up DESC LIMIT %s
    """, (report_date, limit))
    rows = cursor.fetchall()
    cursor.close()
    return [{"ticker": r[0], "signal": r[1], "prob_up": round(float(r[2]), 4) if r[2] is not None else None,
             "prob_down": round(float(r[3]), 4) if r[3] is not None else None,
             "evidence": {"sentiment": r[4], "rsi": r[5], "trend": r[6], "volatility": r[7]}} for r in rows]

def compute_benchmark(signals_df):
    benchmark = {}
    for ticker in signals_df["ticker"].unique():
        ticker_df = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        if ticker_df.empty: continue
        first_price = float(ticker_df.iloc[0]["close_price"]) if ticker_df.iloc[0]["close_price"] else 0.0
        last_price = float(ticker_df.iloc[-1]["close_price"]) if ticker_df.iloc[-1]["close_price"] else 0.0
        buy_hold_return = ((last_price - first_price) / first_price) if first_price > 0 else 0.0
        benchmark[ticker] = round(float(buy_hold_return), 4)
    return benchmark

# =============================================================================
# 5. LOOP MAESTRO
# =============================================================================
def run_pipeline(start_date_str=None, end_date_str=None):
    # Fechas por argumento o por defecto
    end_d = pd.to_datetime(end_date_str).date() if end_date_str else datetime.now().date()
    start_d = pd.to_datetime(start_date_str).date() if start_date_str else (end_d - timedelta(days=DAYS_BACK))

    logger.info(f"🚀 Iniciando Bootstrap Local TFM | Rango: {start_d} a {end_d}")
    
    # ── 1. DESCARGA INICIAL DE DATOS ──
    # Para poder calcular indicadores en start_d, la lambda descarga días extra.
    ohlcv_all = fetch_ohlcv_all(TICKERS, start_d, end_d)
    vix_series = fetch_vix_historical(start_d, end_d)
    news_all = {t: fetch_news_historical(t, start_d, end_d) for t in TICKERS}

    conn = get_db_connection()
    get_bn_model()
    get_finbert()

    business_days = pd.bdate_range(start=str(start_d), end=str(end_d))
    
    # Prevenir simulación de fechas futuras
    today_date = datetime.now().date()
    business_days = [bd for bd in business_days if bd.date() <= today_date]

    # ── 2. BUCLE DIARIO ──
    for bd in tqdm(business_days, desc="Simulando días", unit="día"):
        date_str = bd.strftime("%Y-%m-%d")
        run_id = f"backtest-{date_str}"
        global_kpis = {"total_headlines": 0, "processed_headlines": 0}

        if conn: pg_upsert_batch_log(conn, date_str, run_id, TICKERS, "STARTED")

        # --- A) Ingesta y Macro (Clon lambda_macro_ingestion / lambda_macro_context) ---
        ingest_macro_news(date_str)
        macro_articles = read_macro_news(date_str)
        macro_sentiment_data = run_finbert_macro_local(macro_articles)
        
        macro_sentiment = macro_sentiment_data["state"]
        macro_score = macro_sentiment_data["score"]
        n_macro_articles = macro_sentiment_data["n_articles"]

        vix = _sf(vix_series[vix_series.index <= bd].iloc[-1]) if not vix_series[vix_series.index <= bd].empty else None
        risk_regime = "RISK_OFF" if vix and vix > 25 else ("RISK_ON" if vix and vix < 18 else "NEUTRAL")
        macro_adj = -0.04 if risk_regime == "RISK_OFF" else (0.04 if risk_regime == "RISK_ON" else 0.0)
        
        upsert_macro_context(date_str, macro_sentiment, risk_regime, macro_adj, {"vix": vix, "n_articles": n_macro_articles})
        
        if conn:
            try:
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO market_regime_state (batch_date, run_id, risk_regime, macro_adjustment, vix)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (batch_date) DO UPDATE SET risk_regime=EXCLUDED.risk_regime, macro_adjustment=EXCLUDED.macro_adjustment, vix=EXCLUDED.vix
                    """, (date_str, run_id, risk_regime, float(macro_adj), float(vix) if vix else None))
                    
                    c.execute("""
                        INSERT INTO macro_sentiment_scores (batch_date, run_id, macro_sentiment, score, n_articles)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (batch_date) DO UPDATE SET macro_sentiment=EXCLUDED.macro_sentiment, score=EXCLUDED.score, n_articles=EXCLUDED.n_articles
                    """, (date_str, run_id, macro_sentiment, float(macro_score), n_macro_articles))
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error insertando tablas macro: {e}")

        # --- B) Procesamiento por Ticker (Clon lambda_sentiment / lambda_indicators / lambda_bayesian) ---
        tickers_trace = {}
        daily_signals_for_outcomes = []

        for ticker in TICKERS:
            ohlcv_df = ohlcv_all.get(ticker)
            ind = calculate_indicators_for_date(ohlcv_df, date_str) if ohlcv_df is not None else None
            if not ind: continue
            
            # Guardar OHLCV en BD
            target_dt = pd.to_datetime(date_str)
            if target_dt in ohlcv_df.index:
                row_data = ohlcv_df.loc[target_dt]
                upsert_ohlcv_bulk(date_str, ticker, [{
                    "date": date_str, "close": ind["close"], 
                    "open": float(row_data.get("Open", 0) or 0), 
                    "high": float(row_data.get("High", 0) or 0), 
                    "low": float(row_data.get("Low", 0) or 0), 
                    "volume": float(row_data.get("Volume", 0) or 0)
                }])

            if conn:
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO technical_indicators (batch_date, ticker, close_price, rsi_14, sma_20, sma_50, bb_upper, bb_middle, bb_lower)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (batch_date, ticker) DO NOTHING
                    """, (date_str, ticker, ind["close"], ind["rsi_14"], ind["sma_20"], ind["sma_50"], ind["bb_upper"], ind["bb_middle"], ind["bb_lower"]))
                conn.commit()

            # Noticias y Sentiment
            articles = news_all.get(ticker, {}).get(date_str, [])
            if articles: upsert_raw_news(date_str, ticker, articles)

            processed_headlines_ticker = []
            sentiment_samples = []
            
            for art in articles[:5]:
                global_kpis["total_headlines"] += 1
                summary = extract_and_summarize(ticker, art.get("headline", ""), art.get("url", ""))
                sdata = analyze_sentiment_local(summary)
                
                if sdata:
                    global_kpis["processed_headlines"] += 1
                    upsert_news(date_str, ticker, art, sdata)
                    sentiment_samples.append({"headline": summary, "sentiment": sdata["sentiment"], "confidence": sdata["confidence"]})
                    
                    if conn:
                        with conn.cursor() as c:
                            c.execute("""
                                INSERT INTO sentiment_scores (batch_date, ticker, headline, sentiment, confidence, justification)
                                VALUES (%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (batch_date, ticker, headline) DO NOTHING
                            """, (date_str, ticker, art.get("headline", "")[:250], sdata["sentiment"], sdata["confidence"], sdata["justification"]))
                        conn.commit()
                    processed_headlines_ticker.append(summary)
            
            if processed_headlines_ticker:
                upsert_filtered_news(date_str, ticker, processed_headlines_ticker, "Backtest local")
            
            dom_sent, best_conf, sentiment_detail = aggregate_sentiment_local(sentiment_samples)

            evidence = {
                "Sentiment": dom_sent,
                "RSI": "oversold" if ind["rsi_14"] < 30 else ("overbought" if ind["rsi_14"] > 70 else "neutral"),
                "Trend": "uptrend" if (ind["sma_20"] and ind["sma_50"] and ind["sma_20"] > ind["sma_50"]) else "downtrend",
                "Volatility": "high" if (ind["bb_width"] and ind["bb_width"] > 0.05) else "low"
            }
            
            signal, prob_up = run_bayesian_inference(evidence, macro_adj)
            reasoning = build_reasoning_local(evidence, prob_up, signal)
            
            # Recolectamos la señal para outcomes
            daily_signals_for_outcomes.append({
                "batch_date": date_str, "ticker": ticker, "run_id": run_id, "signal": signal, 
                "prob_up": prob_up, "prob_down": round(1 - prob_up, 4), "close_price": ind["close"],
                "sentiment_state": evidence["Sentiment"], "rsi_state": evidence["RSI"],
                "trend_state": evidence["Trend"], "volatility_state": evidence["Volatility"],
                "macro_sentiment": macro_sentiment, "risk_regime": risk_regime, "macro_adjustment": macro_adj
            })
            
            trace_data = {
                "raw_values": {
                    "close_price": ind["close"], "rsi_14": ind["rsi_14"], 
                    "sma_20": ind["sma_20"], "sma_50": ind["sma_50"], 
                    "bb_upper": ind["bb_upper"], "bb_lower": ind["bb_lower"], "bb_width_ratio": ind["bb_width"]
                },
                "discretization": {
                    "sentiment_raw": dom_sent, "sentiment_conf": best_conf,
                    "sentiment_state": evidence["Sentiment"], "rsi_state": evidence["RSI"],
                    "trend_state": evidence["Trend"], "volatility_state": evidence["Volatility"]
                },
                "sentiment_detail": sentiment_detail,
                "inference": {
                    "signal": signal, "prob_up": prob_up, "prob_down": round(1-prob_up, 4),
                    "threshold_used": MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"] if signal == "BUY" else MODEL_CONFIG["signal_thresholds"]["SELL"]["prob_up_below"],
                    "macro_context": {"macro_sentiment": macro_sentiment, "risk_regime": risk_regime, "macro_adjustment": macro_adj}
                },
                "reasoning": reasoning
            }
            upsert_bayesian_report(date_str, ticker, trace_data, MODEL_CONFIG["version"])
            tickers_trace[ticker] = trace_data

            if conn:
                pg_upsert_signal(conn, date_str, ticker, signal, prob_up, round(1-prob_up, 4))
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO signal_explanations (batch_date, ticker, sentiment_state, rsi_state, trend_state, volatility_state)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (batch_date, ticker) DO UPDATE SET sentiment_state=EXCLUDED.sentiment_state, rsi_state=EXCLUDED.rsi_state, trend_state=EXCLUDED.trend_state, volatility_state=EXCLUDED.volatility_state
                    """, (date_str, ticker, evidence["Sentiment"], evidence["RSI"], evidence["Trend"], evidence["Volatility"]))
                conn.commit()

        upsert_bayesian_trace(date_str, {"tickers": tickers_trace, "model_config": MODEL_CONFIG})
        
        # --- C) REPORTE DIARIO E IDÉNTICO A AWS (Clon lambda_report) ---
        if conn:
            # 1. Obtenemos señales del último año desde la BBDD (Esto da memoria al sistema)
            hist_signals_df = get_trading_data(conn, date_str, days_back=DAYS_BACK)
            metrics, diagnostics = _calc_backtesting(hist_signals_df)
            benchmark = compute_benchmark(hist_signals_df) if not hist_signals_df.empty else {}
            health = get_pipeline_health(conn, date_str, run_id) if run_id else {}
            explanations = get_explanations_sample(conn, date_str, limit=10)
            
            report_data = {
                "report_date": date_str,
                "data_period_days": DAYS_BACK, # Usamos DAYS_BACK, como en AWS
                "generated_at": datetime.now().isoformat(),
                "pipeline_health": health,
                "signal_diagnostics": diagnostics,
                "benchmark_comparison": {
                    t: {
                        "strategy_cumulative_return": metrics[t]["cumulative_return"],
                        "buy_hold_cumulative_return": benchmark.get(t, 0.0),
                        "alpha_vs_benchmark": round(metrics[t]["cumulative_return"] - benchmark.get(t, 0.0), 4)
                    } for t in metrics
                },
                "top_signal_explanations": explanations,
                "backtesting_metrics": metrics,
                "summary": {
                    "total_tickers": len(metrics),
                    "avg_cumulative_return": round(np.mean([m["cumulative_return"] for m in metrics.values()]), 4) if metrics else 0,
                    "avg_sharpe_ratio": round(np.mean([m["sharpe_ratio"] for m in metrics.values()]), 4) if metrics else 0,
                    "avg_max_drawdown": round(np.mean([m["max_drawdown"] for m in metrics.values()]), 4) if metrics else 0, # CORRECCIÓN: Añadido para el dashboard
                    "total_closed_trades": sum(item.get("trades_closed", 0) for item in diagnostics.values()) if diagnostics else 0,
                },
                "backtesting_config": {"initial_capital": INITIAL_CAP, "risk_free_rate": RISK_FREE_RATE, "period_days": DAYS_BACK, "strategy_type": "Long/Cash", "sharpe_annualized": True, "limitation": "El backtesting asume ejecucion al cierre. Estrategia Long/Cash: BUY entra al mercado, SELL cierra posicion, HOLD mantiene posicion abierta."},
                "trace_artifact": f"mongo:bayesian_traces/{date_str}"
            }
            upsert_report(report_data)
        
        pg_upsert_pipeline_kpi(conn, date_str, run_id, "scheduled", "ingestion", global_kpis)
        pg_upsert_batch_log(conn, date_str, run_id, TICKERS, "COMPLETED")
        
        if conn and daily_signals_for_outcomes:
            update_signal_outcomes_historical(conn, daily_signals_for_outcomes)

    if conn: conn.close()
    logger.info("✅ BACKTESTING COMPLETADO")

if __name__ == "__main__":
    args = get_args()
    run_pipeline(args.start, args.end)