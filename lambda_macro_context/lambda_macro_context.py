"""
lambda_macro_context
─────────────────────
Transforma noticias macroeconómicas en dos variables probabilísticas globales:

  MacroSentiment  → bullish / neutral / bearish
  RiskRegime      → RISK_ON / NEUTRAL / RISK_OFF

Proceso:
  1. Lee macro_news de MongoDB (generado por lambda_macro_ingestion)
  2. Ejecuta FinBERT sobre los headlines macro para calcular MacroSentiment
     ponderado por credibilidad de fuente y decaimiento temporal
  3. Calcula RiskRegime mediante reglas deterministas sobre:
     - MacroSentiment
     - VIX (yfinance ^VIX)
     - Detección de noticias geopolíticas de alto impacto
     - Señales FED hawkish/dovish
     - Datos de inflación sorpresivos
  4. Deriva macro_adjustment dinámico (±0.12 base, hasta ±0.20 en eventos extremos)
  5. Persiste en MongoDB (macro_context) y Aurora (macro_sentiment_scores, market_regime_state)

El macro_adjustment es leído por lambda_bayesian y aplicado sobre prob_up
antes de evaluar el umbral de señal BUY/SELL/HOLD.
"""

import json
import boto3
import psycopg2
import os
import time
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from huggingface_hub import InferenceClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
rds_client     = boto3.client("rds")

MODEL_ID = "ProsusAI/finbert"

# ─── Credibilidad por fuente (0–1) ────────────────────────────────────────────
SOURCE_CREDIBILITY = {
    "reuters_markets":  1.0,
    "ft_world":         0.95,
    "cnbc_markets":     0.85,
    "marketwatch":      0.80,
    "bloomberg":        1.0,
    "reuters":          1.0,
    "financial times":  0.95,
    "wsj":              0.95,
    "wall street journal": 0.95,
    "cnbc":             0.85,
    "newsapi":          0.70,
}

# ─── Ajuste dinámico MacroSentiment × RiskRegime → prob_up delta ─────────────
MACRO_ADJUSTMENTS = {
    ("bullish", "RISK_ON"):   +0.12,
    ("bullish", "NEUTRAL"):   +0.06,
    ("bullish", "RISK_OFF"):  +0.02,
    ("neutral", "RISK_ON"):   +0.04,
    ("neutral", "NEUTRAL"):    0.00,
    ("neutral", "RISK_OFF"):  -0.04,
    ("bearish", "RISK_ON"):   -0.02,
    ("bearish", "NEUTRAL"):   -0.06,
    ("bearish", "RISK_OFF"):  -0.12,
}
HIGH_IMPACT_MULTIPLIER = 1.67   # sube hasta ±0.20 en eventos extremos
MAX_ADJUSTMENT         = 0.20

# Palabras clave para detectar eventos de alto impacto
# Keywords de alto impacto — frases específicas para evitar falsos positivos
# Se requieren MÍNIMO 2 artículos distintos con el keyword para activar el evento
GEOPOLITICAL_KEYWORDS = [
    "military strike", "armed conflict", "invasion of", "nuclear threat",
    "missile attack", "war escalation", "military offensive", "coup attempt",
    "terrorist attack", "economic sanctions imposed",
]
HAWKISH_KEYWORDS = [
    "rate hike", "raises rates", "hawkish fed", "fed hikes",
    "50 basis points hike", "75 basis points hike", "aggressive tightening",
    "higher for longer", "restrictive policy",
]
DOVISH_KEYWORDS = [
    "rate cut", "cuts rates", "dovish pivot", "fed cuts",
    "quantitative easing announced", "accommodative stance",
    "pause in rate hikes", "end of tightening",
]
INFLATION_SHOCK_KEYWORDS = [
    "hotter than expected inflation", "cpi beats expectations",
    "inflation surges unexpectedly", "core inflation surprise",
    "inflation above forecast",
]

# Mínimo de artículos que deben contener el keyword para considerar evento activo
MIN_ARTICLES_FOR_EVENT = 2

try:
    from mongo_utils import (
        read_macro_news    as _read_macro_news,
        upsert_macro_context as _upsert_macro_context,
    )
    logger.info("mongo_utils (macro_context) cargado OK")
except ImportError:
    logger.warning("mongo_utils no disponible")
    _read_macro_news     = None
    _upsert_macro_context = None


# ─── Helpers de pipeline ──────────────────────────────────────────────────────

def resolve_batch_date(event):
    raw = (event or {}).get("batch_date") or (event or {}).get("date")
    return raw[:10] if raw else datetime.now().strftime("%Y-%m-%d")


def resolve_pipeline_context(event):
    ctx = (event or {}).get("pipeline_context", {}) if isinstance(event, dict) else {}
    request = ctx.get("request", {}) if isinstance(ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}
    batch_date   = resolve_batch_date(request) if request.get("batch_date") else resolve_batch_date(ctx)
    run_id       = ctx.get("run_id") or (event or {}).get("run_id") or f"legacy-{batch_date}"
    trigger_type = request.get("trigger_type")
    if trigger_type not in ("manual", "scheduled"):
        trigger_type = "manual" if request.get("ticker") or request.get("tickers") else "scheduled"
    return {"batch_date": batch_date, "run_id": run_id, "trigger_type": trigger_type}


def get_secret(secret_name):
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    if "SecretString" in resp:
        return json.loads(resp["SecretString"])
    return json.loads(resp["SecretBinary"])


def connect_to_aurora(aurora_creds):
    auth_mode = str(aurora_creds.get("auth_mode", "")).lower()
    host      = aurora_creds["host"]
    port      = int(aurora_creds.get("port", 5432))
    username  = aurora_creds["username"]
    dbname    = aurora_creds.get("dbname", "tfm")
    region    = os.getenv("AWS_REGION", "eu-north-1")
    if auth_mode == "iam":
        token = rds_client.generate_db_auth_token(
            DBHostname=host, Port=port, DBUsername=username, Region=region
        )
        return psycopg2.connect(host=host, port=port, user=username,
                                password=token, database=dbname, sslmode="require")
    return psycopg2.connect(host=host, port=port, user=username,
                            password=aurora_creds["password"], database=dbname)


# ─── FinBERT sobre noticias macro ─────────────────────────────────────────────

def _credibility(source: str) -> float:
    s = (source or "").lower()
    for key, val in SOURCE_CREDIBILITY.items():
        if key in s:
            return val
    return 0.65   # fuente desconocida → peso moderado


def _time_decay(dt_str: str, batch_date: str) -> float:
    """Decaimiento exponencial: noticias más recientes tienen más peso."""
    try:
        pub  = pd.to_datetime(dt_str, utc=True)
        ref  = pd.to_datetime(batch_date, utc=True)
        hours_old = max((ref - pub).total_seconds() / 3600, 0)
        # Semivida de 12 horas → noticias de hace 24h tienen peso 0.25
        return 0.5 ** (hours_old / 12)
    except Exception:
        return 0.5


def run_finbert_macro(articles: list, hf_client: InferenceClient) -> dict:
    """
    Ejecuta FinBERT sobre los headlines macro y calcula MacroSentiment ponderado.
    Devuelve dict con: score, state, distribution, weighted_scores.
    """
    if not articles:
        return {"score": 0.0, "state": "neutral", "distribution": {}, "n_articles": 0}

    SENTIMENT_SCORE = {"bullish": +1.0, "neutral": 0.0, "bearish": -1.0}
    weighted_sum  = 0.0
    weight_total  = 0.0
    distribution  = {"bullish": 0, "neutral": 0, "bearish": 0}
    scored        = 0

    MIN_CONFIDENCE_MACRO = 0.55  # igual que lambda_sentiment — descarta ambigüedades

    for art in articles[:50]:   # máximo 50 artículos para controlar latencia
        headline = art.get("headline", "")
        if not headline or len(headline.strip()) < 20:
            continue

        credibility = _credibility(art.get("source", ""))
        decay       = _time_decay(art.get("datetime", ""), art.get("batch_date", ""))
        weight      = credibility * decay

        for attempt in range(3):
            try:
                result = hf_client.text_classification(headline, model=MODEL_ID)
                if result:
                    top = max(result, key=lambda x: x.score if hasattr(x, "score") else x.get("score", 0))
                    label = (top.label if hasattr(top, "label") else top.get("label", "neutral")).lower()
                    conf  = top.score  if hasattr(top, "score") else top.get("score", 0.5)

                    # Descartar si FinBERT no tiene convicción suficiente
                    if conf < MIN_CONFIDENCE_MACRO:
                        break

                    FINBERT_MAP = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
                    sentiment   = FINBERT_MAP.get(label, "neutral")

                    score_val    = SENTIMENT_SCORE[sentiment] * conf
                    weighted_sum += score_val * weight
                    weight_total += weight
                    distribution[sentiment] += 1
                    scored += 1
                break
            except Exception as exc:
                if "503" in str(exc).lower() or "loading" in str(exc).lower():
                    time.sleep(5)
                else:
                    logger.warning(f"FinBERT error para '{headline[:40]}': {exc}")
                    break

    if weight_total == 0 or scored == 0:
        return {"score": 0.0, "state": "neutral", "distribution": distribution, "n_articles": 0}

    final_score = weighted_sum / weight_total

    if final_score > 0.20:
        state = "bullish"
    elif final_score < -0.20:
        state = "bearish"
    else:
        state = "neutral"

    logger.info(f"MacroSentiment: score={final_score:.3f} → {state} "
                f"(n={scored}, dist={distribution})")

    return {
        "score":        round(final_score, 4),
        "state":        state,
        "distribution": distribution,
        "n_articles":   scored,
    }


# ─── VIX ──────────────────────────────────────────────────────────────────────

def get_vix(batch_date: str) -> float | None:
    """Obtiene el cierre del VIX más reciente."""
    try:
        # Lambda tiene filesystem read-only — redirigir caché de yfinance a /tmp
        yf.set_tz_cache_location("/tmp/yfinance_tz_cache")
        end   = pd.to_datetime(batch_date).date() + timedelta(days=1)
        start = end - timedelta(days=5)
        df = yf.download("^VIX", start=str(start), end=str(end), progress=False)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        return float(df["Close"].iloc[-1])
    except Exception as exc:
        logger.warning(f"No se pudo obtener VIX: {exc}")
        return None


# ─── Detección de eventos de alto impacto ─────────────────────────────────────

def _count_articles_with_keyword(articles: list, keywords: list) -> int:
    """Cuenta cuántos artículos distintos contienen al menos un keyword."""
    count = 0
    for art in articles:
        text = (art.get("headline", "") + " " + art.get("summary", "")).lower()
        if any(kw in text for kw in keywords):
            count += 1
    return count


def detect_high_impact_events(articles: list) -> dict:
    """
    Detecta eventos de alto impacto requiriendo que al menos MIN_ARTICLES_FOR_EVENT
    artículos distintos mencionen el keyword. Evita falsos positivos por menciones
    históricas o de contexto en un único artículo.
    """
    geo   = _count_articles_with_keyword(articles, GEOPOLITICAL_KEYWORDS)
    hawk  = _count_articles_with_keyword(articles, HAWKISH_KEYWORDS)
    dove  = _count_articles_with_keyword(articles, DOVISH_KEYWORDS)
    inf   = _count_articles_with_keyword(articles, INFLATION_SHOCK_KEYWORDS)

    events = {
        "geopolitical":    geo  >= MIN_ARTICLES_FOR_EVENT,
        "hawkish_fed":     hawk >= MIN_ARTICLES_FOR_EVENT,
        "dovish_fed":      dove >= MIN_ARTICLES_FOR_EVENT,
        "inflation_shock": inf  >= MIN_ARTICLES_FOR_EVENT,
        "_counts": {"geopolitical": geo, "hawkish": hawk, "dovish": dove, "inflation": inf},
    }

    # hawkish y dovish simultáneos es contradictorio — prevalece hawkish
    if events["hawkish_fed"] and events["dovish_fed"]:
        logger.warning("hawkish y dovish detectados simultáneamente — prevalece hawkish, dovish ignorado")
        events["dovish_fed"] = False

    return events


# ─── RiskRegime ───────────────────────────────────────────────────────────────

def calculate_risk_regime(macro_sentiment: str, vix: float | None,
                           events: dict) -> tuple[str, dict]:
    """
    Calcula RiskRegime mediante reglas deterministas sobre inputs observables.
    Devuelve (regime, reasoning_dict).
    """
    reasons = {
        "vix":                vix,
        "geopolitical":       events.get("geopolitical", False),
        "hawkish_fed":        events.get("hawkish_fed", False),
        "dovish_fed":         events.get("dovish_fed", False),
        "inflation_shock":    events.get("inflation_shock", False),
        "macro_sentiment":    macro_sentiment,
    }

    # ── RISK_OFF: cualquier condición de estrés sistémico ──────────────────
    risk_off_triggers = []
    if vix is not None and vix > 25:
        risk_off_triggers.append(f"VIX={vix:.1f} > 25")
    if events.get("geopolitical"):
        risk_off_triggers.append("geopolitical_high_impact")
    if events.get("hawkish_fed") and macro_sentiment in ("bearish", "neutral"):
        risk_off_triggers.append("hawkish_fed")
    if events.get("inflation_shock") and macro_sentiment == "bearish":
        risk_off_triggers.append("inflation_shock_bearish")

    if risk_off_triggers:
        reasons["regime_triggers"] = risk_off_triggers
        logger.info(f"RiskRegime=RISK_OFF — triggers: {risk_off_triggers}")
        return "RISK_OFF", reasons

    # ── RISK_ON: condiciones favorables alineadas ──────────────────────────
    risk_on_conditions = []
    if macro_sentiment == "bullish":
        risk_on_conditions.append("macro_bullish")
    if vix is not None and vix < 18:
        risk_on_conditions.append(f"VIX={vix:.1f} < 18")
    if events.get("dovish_fed"):
        risk_on_conditions.append("dovish_fed")

    if len(risk_on_conditions) >= 2:
        reasons["regime_triggers"] = risk_on_conditions
        logger.info(f"RiskRegime=RISK_ON — conditions: {risk_on_conditions}")
        return "RISK_ON", reasons

    reasons["regime_triggers"] = ["no_dominant_signal"]
    logger.info("RiskRegime=NEUTRAL")
    return "NEUTRAL", reasons


# ─── Macro adjustment ─────────────────────────────────────────────────────────

def calculate_macro_adjustment(macro_sentiment: str, risk_regime: str,
                                vix: float | None, events: dict) -> float:
    base = MACRO_ADJUSTMENTS.get((macro_sentiment, risk_regime), 0.0)

    # Multiplicador de alta convicción en eventos extremos
    extreme = (
        (vix is not None and vix > 30) or
        events.get("geopolitical") or
        (events.get("hawkish_fed") and events.get("inflation_shock"))
    )
    if extreme:
        base = base * HIGH_IMPACT_MULTIPLIER

    # Cabecera en ±MAX_ADJUSTMENT
    adj = max(-MAX_ADJUSTMENT, min(MAX_ADJUSTMENT, base))
    logger.info(f"macro_adjustment={adj:+.3f} "
                f"(base={base:.3f}, extreme={extreme}, "
                f"sentiment={macro_sentiment}, regime={risk_regime})")
    return round(adj, 4)


# ─── Persistencia Aurora ──────────────────────────────────────────────────────

def save_to_aurora(connection, batch_date: str, run_id: str,
                   macro_sentiment: str, macro_score: float,
                   risk_regime: str, macro_adjustment: float,
                   vix: float | None, n_articles: int):
    cursor = connection.cursor()

    # macro_sentiment_scores
    cursor.execute("""
        INSERT INTO macro_sentiment_scores
            (batch_date, run_id, macro_sentiment, score, n_articles, created_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (batch_date) DO UPDATE SET
            macro_sentiment = EXCLUDED.macro_sentiment,
            score           = EXCLUDED.score,
            n_articles      = EXCLUDED.n_articles,
            updated_at      = CURRENT_TIMESTAMP
    """, (batch_date, run_id, macro_sentiment, macro_score, n_articles))

    # market_regime_state
    cursor.execute("""
        INSERT INTO market_regime_state
            (batch_date, run_id, risk_regime, macro_adjustment, vix, created_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (batch_date) DO UPDATE SET
            risk_regime      = EXCLUDED.risk_regime,
            macro_adjustment = EXCLUDED.macro_adjustment,
            vix              = EXCLUDED.vix,
            updated_at       = CURRENT_TIMESTAMP
    """, (batch_date, run_id, risk_regime, macro_adjustment, vix))

    connection.commit()
    cursor.close()
    logger.info(f"Aurora: macro_sentiment_scores + market_regime_state actualizados")


# ─── Handler ──────────────────────────────────────────────────────────────────

def handler(event, context):
    logger.info("lambda_macro_context iniciado")
    ctx   = resolve_pipeline_context(event)
    today = ctx["batch_date"]

    # Credenciales
    aurora_creds = get_secret("aurora/credentials")
    hf_creds     = get_secret("huggingface/api_key")
    hf_client    = InferenceClient(token=hf_creds["api_key"])

    # 1. Leer noticias macro
    if not _read_macro_news:
        return {"statusCode": 500, "body": json.dumps({"error": "mongo_utils no disponible"})}

    articles = _read_macro_news(today)
    logger.info(f"macro_news leídas: {len(articles)} artículos para {today}")

    if not articles:
        logger.warning("No hay noticias macro — usando contexto neutral por defecto")
        macro_result = {"score": 0.0, "state": "neutral", "distribution": {}, "n_articles": 0}
    else:
        # 2. MacroSentiment con FinBERT ponderado
        macro_result = run_finbert_macro(articles, hf_client)

    macro_sentiment = macro_result["state"]
    macro_score     = macro_result["score"]

    # 3. VIX
    vix = get_vix(today)
    logger.info(f"VIX: {vix}")

    # 4. Detección de eventos de alto impacto
    events = detect_high_impact_events(articles)
    logger.info(f"Eventos detectados: {events}")

    # 5. RiskRegime
    risk_regime, regime_reasoning = calculate_risk_regime(macro_sentiment, vix, events)

    # 6. macro_adjustment
    macro_adjustment = calculate_macro_adjustment(macro_sentiment, risk_regime, vix, events)

    # 7. Persiste en MongoDB
    detail = {
        "macro_score":       macro_score,
        "n_articles":        macro_result["n_articles"],
        "distribution":      macro_result["distribution"],
        "vix":               vix,
        "events":            events,
        "regime_reasoning":  regime_reasoning,
    }
    _upsert_macro_context(today, macro_sentiment, risk_regime, macro_adjustment, detail)

    # 8. Persiste en Aurora
    try:
        connection = connect_to_aurora(aurora_creds)
        save_to_aurora(connection, today, ctx["run_id"],
                       macro_sentiment, macro_score,
                       risk_regime, macro_adjustment, vix,
                       macro_result["n_articles"])
        connection.close()
    except Exception as exc:
        logger.warning(f"Aurora macro no actualizado (no crítico): {exc}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":          "Contexto macro calculado",
            "batch_date":       today,
            "macro_sentiment":  macro_sentiment,
            "macro_score":      macro_score,
            "risk_regime":      risk_regime,
            "macro_adjustment": macro_adjustment,
            "vix":              vix,
            "events":           events,
            "n_articles":       macro_result["n_articles"],
        }),
    }
