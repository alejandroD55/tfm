# deploy: 2026-05-12 18:03 UTC
import sys
from unittest.mock import MagicMock


# ── Mock de dependencias ML pesadas que pgmpy intenta importar ────────────────
class MockImporter:
    def find_module(self, fullname, path=None):
        if fullname.startswith(("sklearn", "statsmodels", "patsy")):
            return self
        return None

    def load_module(self, fullname):
        mock = MagicMock()
        mock.__path__ = []
        sys.modules[fullname] = mock
        return mock


sys.meta_path.insert(0, MockImporter())

import json
import boto3
import psycopg2
import os
from datetime import datetime, timezone
import logging
import numpy as np

from pgmpy.models import BayesianNetwork
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
rds_client = boto3.client("rds")

try:
    from mongo_utils import upsert_bayesian_trace  as _mongo_upsert_bayesian_trace
    from mongo_utils import upsert_bayesian_report as _mongo_upsert_bayesian_report
    from mongo_utils import read_macro_context      as _mongo_read_macro_context
    from mongo_utils import distinct_raw_news_tickers as _mongo_distinct_raw_news_tickers
    from mongo_utils import read_feature_snapshot as _mongo_read_feature_snapshot
    from mongo_utils import upsert_feature_snapshot as _mongo_upsert_feature_snapshot
    logger.info("mongo_utils (bayesian) cargado")
except ImportError:
    _mongo_upsert_bayesian_trace  = None
    _mongo_upsert_bayesian_report = None
    _mongo_read_macro_context     = None
    _mongo_distinct_raw_news_tickers = None
    _mongo_read_feature_snapshot = None
    _mongo_upsert_feature_snapshot = None
    logger.warning("mongo_utils no disponible en lambda_bayesian")

try:
    from exposure_constraints import (
        apply_exposure_constraints,
        prob_to_exposure,
        detect_market_regime_simple,
    )
except ImportError:
    apply_exposure_constraints = None
    prob_to_exposure = None
    detect_market_regime_simple = None

MODEL_ID = "bayesian_v1.2"

try:
    from quant_observability import compute_contribution_analysis
except ImportError:
    compute_contribution_analysis = None
    logger.warning("quant_observability no disponible en lambda_bayesian")

MODEL_CONFIG = {
    "version": "1.2.0",
    "description": (
        "Red bayesiana v1.2: umbrales calibrados (SELL≤0.28, BUY≥0.52), "
        "priors con drift alcista histórico, macro_adj amortiguado en uptrend, "
        "hysteresis SELL 2 días consecutivos."
    ),
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
        "BUY":  {"prob_up_above": 0.52, "rationale": "Entrada con confianza alcista moderada"},
        "SELL": {"prob_up_below": 0.28, "rationale": "Solo salida en condiciones genuinamente bajistas"},
        "HOLD": {"range": [0.28, 0.52], "rationale": "Zona de incertidumbre — mantener posición actual"},
    },
    "priors": {
        "Sentiment": {
            "bullish": 0.35,
            "bearish": 0.25,
            "neutral": 0.40,
            "rationale": "Sesgo levemente alcista: mercados suben más días de los que bajan históricamente",
        },
        "RSI": {
            "oversold":   0.15,
            "neutral":    0.60,
            "overbought": 0.25,
            "rationale": "En bull market el RSI pasa más tiempo en zona overbought que oversold",
        },
        "Trend": {
            "uptrend":   0.58,
            "downtrend": 0.42,
            "rationale": "Drift alcista histórico: los índices están en uptrend ~60% del tiempo",
        },
        "Volatility": {
            "low":  0.62,
            "high": 0.38,
            "rationale": "Los mercados suelen tener baja volatilidad más frecuentemente",
        },
    },
    "cpt_market_direction": {
        "variable": "MarketDirection",
        "states": ["down", "up"],
        "evidence_order": ["Sentiment", "RSI", "Trend", "Volatility"],
        "rationale": {
            "momentum_logic": "RSI overbought + uptrend = momentum comprador fuerte (no reversion en bull market)",
            "v1.2_change": "P_up para overbought+uptrend aumentada ~+0.08: en bull market RSI alto no implica caída",
        },
        # Corrección clave v1.2: P_up para overbought en uptrend sube ~+0.08
        "values_P_down": [
            0.12, 0.22, 0.25, 0.18, 0.25, 0.30, 0.22, 0.35,
            0.08, 0.12, 0.40, 0.45,
            0.70, 0.75, 0.80, 0.75, 0.80, 0.85, 0.80, 0.85,
            0.50, 0.55, 0.90, 0.95,
            0.42, 0.48, 0.52, 0.47, 0.52, 0.58, 0.52, 0.58,
            0.22, 0.28, 0.62, 0.68,
        ],
        "values_P_up": [
            0.88, 0.78, 0.75, 0.82, 0.75, 0.70, 0.78, 0.65,
            0.92, 0.88, 0.60, 0.55,
            0.30, 0.25, 0.20, 0.25, 0.20, 0.15, 0.20, 0.15,
            0.50, 0.45, 0.10, 0.05,
            0.58, 0.52, 0.48, 0.53, 0.48, 0.42, 0.48, 0.42,
            0.78, 0.72, 0.38, 0.32,
        ],
    },
    "hysteresis": {
        "sell_confirmation_days": 2,
        "buy_confirmation_days":  1,
        "rationale": (
            "Persistencia de señal: SELL solo actúa si se repite N días consecutivos. "
            "Evita salidas falsas por una noticia bearish puntual en tendencia alcista."
        ),
    },
    "exposure_recommendation_policy": {
        "description": (
            "Decision policy primario basado en exposición objetivo/constrain. "
            "BUY/HOLD/SELL se conserva solo como derivación legacy para compatibilidad."
        ),
        "bands_pct": {
            "INCREASE_STRONG_min": 75.0,
            "INCREASE_MILD_min": 62.0,
            "MAINTAIN_min": 52.0,
            "REDUCE_MILD_min": 50.0,
        },
    },
    "known_limitations": [
        "El confidence score de FinBERT no entra en la inferencia (solo se guarda)",
        "Se usa voto mayoritario de los titulares",
        "macro_adj amortiguado al 40% en uptrend para evitar salidas por noticias macro puntuales",
    ],
}

# ── Hysteresis: días consecutivos de SELL necesarios para confirmar salida ────
SELL_CONFIRMATION_DAYS: int = MODEL_CONFIG["hysteresis"]["sell_confirmation_days"]


def resolve_batch_date(event):
    raw = (event or {}).get("batch_date") or (event or {}).get("date")
    return raw[:10] if raw else None


def resolve_pipeline_context(event):
    pipeline_ctx = (
        (event or {}).get("pipeline_context", {}) if isinstance(event, dict) else {}
    )
    request = pipeline_ctx.get("request", {}) if isinstance(pipeline_ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}

    batch_date = (
        resolve_batch_date(request)
        if request.get("batch_date")
        else resolve_batch_date(pipeline_ctx)
    )
    run_id = (
        pipeline_ctx.get("run_id")
        or (event or {}).get("run_id")
        or f"legacy-{batch_date}"
    )
    trigger_type = request.get("trigger_type")
    if trigger_type not in ("manual", "scheduled"):
        trigger_type = (
            "manual" if request.get("ticker") or request.get("tickers") else "scheduled"
        )

    return {"batch_date": batch_date, "run_id": run_id, "trigger_type": trigger_type}


def get_secret(secret_name):
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(resp.get("SecretString", resp.get("SecretBinary")))


def connect_to_aurora(aurora_creds):
    auth_mode = str(aurora_creds.get("auth_mode", "")).lower()
    region = os.getenv("AWS_REGION", "eu-north-1")
    host = aurora_creds["host"]
    port = int(aurora_creds.get("port", 5432))
    username = aurora_creds["username"]
    dbname = aurora_creds.get("dbname", "tfm")

    if auth_mode == "iam":
        token = rds_client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=username,
            Region=region,
        )
        return psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=token,
            database=dbname,
            sslmode="require",
        )

    return psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=aurora_creds["password"],
        database=dbname,
    )


def discretize_rsi(rsi_value):
    cfg = MODEL_CONFIG["discretization"]["rsi"]
    if rsi_value < cfg["oversold_below"]:
        return "oversold"
    if rsi_value > cfg["overbought_above"]:
        return "overbought"
    return "neutral"


def discretize_trend(sma_20, sma_50):
    return "uptrend" if sma_20 > sma_50 else "downtrend"


def discretize_volatility(bb_upper, bb_lower, close_price):
    if bb_upper is None or bb_lower is None:
        return "low", 0.0
    try:
        if np.isnan(float(bb_upper)) or np.isnan(float(bb_lower)):
            return "low", 0.0
    except:
        return "low", 0.0
    band_width = float(bb_upper) - float(bb_lower)
    width_ratio = band_width / float(close_price) if float(close_price) > 0 else 0.0
    threshold = MODEL_CONFIG["discretization"]["volatility"][
        "high_if_band_width_ratio_above"
    ]
    return ("high" if width_ratio > threshold else "low"), round(width_ratio, 6)


def discretize_sentiment(sentiment):
    return sentiment if sentiment in ("bullish", "bearish", "neutral") else "neutral"


def create_bayesian_network():
    cfg = MODEL_CONFIG["cpt_market_direction"]
    priors = MODEL_CONFIG["priors"]

    model = BayesianNetwork(
        [
            ("Sentiment", "MarketDirection"),
            ("RSI", "MarketDirection"),
            ("Trend", "MarketDirection"),
            ("Volatility", "MarketDirection"),
        ]
    )

    cpd_s = TabularCPD(
        "Sentiment",
        3,
        [
            [priors["Sentiment"]["bullish"]],
            [priors["Sentiment"]["bearish"]],
            [priors["Sentiment"]["neutral"]],
        ],
        state_names={"Sentiment": ["bullish", "bearish", "neutral"]},
    )
    cpd_r = TabularCPD(
        "RSI",
        3,
        [
            [priors["RSI"]["oversold"]],
            [priors["RSI"]["neutral"]],
            [priors["RSI"]["overbought"]],
        ],
        state_names={"RSI": ["oversold", "neutral", "overbought"]},
    )
    cpd_t = TabularCPD(
        "Trend",
        2,
        [[priors["Trend"]["uptrend"]], [priors["Trend"]["downtrend"]]],
        state_names={"Trend": ["uptrend", "downtrend"]},
    )
    cpd_v = TabularCPD(
        "Volatility",
        2,
        [[priors["Volatility"]["low"]], [priors["Volatility"]["high"]]],
        state_names={"Volatility": ["low", "high"]},
    )

    cpd_d = TabularCPD(
        variable="MarketDirection",
        variable_card=2,
        values=[cfg["values_P_down"], cfg["values_P_up"]],
        evidence=["Sentiment", "RSI", "Trend", "Volatility"],
        evidence_card=[3, 3, 2, 2],
        state_names={
            "MarketDirection": ["down", "up"],
            "Sentiment": ["bullish", "bearish", "neutral"],
            "RSI": ["oversold", "neutral", "overbought"],
            "Trend": ["uptrend", "downtrend"],
            "Volatility": ["low", "high"],
        },
    )
    model.add_cpds(cpd_s, cpd_r, cpd_t, cpd_v, cpd_d)
    if not model.check_model():
        raise ValueError("Invalid Bayesian Network")
    return model


def get_macro_context(batch_date: str) -> dict:
    """Lee el contexto macro del día desde MongoDB. Vacío si no existe."""
    if not _mongo_read_macro_context:
        return {}
    try:
        return _mongo_read_macro_context(batch_date) or {}
    except Exception as exc:
        logger.warning(f"No se pudo leer macro_context: {exc}")
        return {}


def _prob_up_for_evidence(model, evidence_states: dict, macro_context: dict = None) -> float:
    """Same probability path as infer_signal, isolated for audit attribution only."""
    infer = VariableElimination(model)
    result = infer.query(
        variables=["MarketDirection"], evidence=evidence_states, show_progress=False
    )
    prob_up_raw = round(float(result.values[1]), 4)

    macro_adjustment = 0.0
    if macro_context:
        macro_adjustment = float(macro_context.get("macro_adjustment", 0.0))

    effective_macro_adj = macro_adjustment
    if evidence_states.get("Trend") == "uptrend" and macro_adjustment < 0:
        effective_macro_adj = macro_adjustment * 0.40

    return round(max(0.0, min(1.0, prob_up_raw + effective_macro_adj)), 4)


def build_contribution_analysis(model, evidence_states: dict, macro_context: dict = None) -> dict:
    if compute_contribution_analysis is None:
        return {}
    try:
        contribution = compute_contribution_analysis(
            evidence_states,
            probability_fn=lambda ev: _prob_up_for_evidence(model, ev, macro_context),
            no_macro_probability_fn=lambda ev: _prob_up_for_evidence(model, ev, {}),
        )
        contribution["macro_context"] = {
            "macro_adjustment": float((macro_context or {}).get("macro_adjustment", 0.0)),
            "macro_sentiment": (macro_context or {}).get("macro_sentiment", "neutral"),
            "risk_regime": (macro_context or {}).get("risk_regime", "NEUTRAL"),
        }
        deltas = {
            key: value.get("delta_prob_up")
            for key, value in contribution.get("effects", {}).items()
            if isinstance(value, dict)
        }
        logger.info(f"contribution_analysis: deltas={deltas}")
        return contribution
    except Exception as exc:
        logger.warning(f"contribution_analysis failed: {exc}")
        return {}


def infer_signal(model, evidence_states, macro_context: dict = None):
    infer  = VariableElimination(model)
    result = infer.query(
        variables=["MarketDirection"], evidence=evidence_states, show_progress=False
    )
    prob_up_raw   = round(float(result.values[1]), 4)
    prob_down_raw = round(float(result.values[0]), 4)

    # ── Aplicar macro_adjustment ──────────────────────────────────────────────
    macro_adjustment = 0.0
    macro_sentiment  = "neutral"
    risk_regime      = "NEUTRAL"

    if macro_context:
        macro_adjustment = float(macro_context.get("macro_adjustment", 0.0))
        macro_sentiment  = macro_context.get("macro_sentiment", "neutral")
        risk_regime      = macro_context.get("risk_regime", "NEUTRAL")

    # En tendencia alcista confirmada, amortiguamos el macro_adj negativo al 40%.
    # Un dato macro hawkish no debe sola sacar al modelo de un uptrend válido.
    # El macro_adj positivo se aplica completo (no penalizamos info alcista).
    effective_macro_adj = macro_adjustment
    if evidence_states.get("Trend") == "uptrend" and macro_adjustment < 0:
        effective_macro_adj = macro_adjustment * 0.40
        logger.info(
            f"macro_adj amortiguado en uptrend: {macro_adjustment:+.3f} → {effective_macro_adj:+.3f}"
        )

    # prob_up ajustada: capada en [0, 1]
    prob_up_adjusted = round(
        max(0.0, min(1.0, prob_up_raw + effective_macro_adj)), 4
    )
    prob_down_adjusted = round(1.0 - prob_up_adjusted, 4)

    if macro_adjustment != 0.0:
        logger.info(
            f"macro_adjustment={macro_adjustment:+.3f} effective={effective_macro_adj:+.3f} "
            f"({macro_sentiment}/{risk_regime}): "
            f"prob_up {prob_up_raw} → {prob_up_adjusted}"
        )

    cfg = MODEL_CONFIG["signal_thresholds"]
    if prob_up_adjusted >= cfg["BUY"]["prob_up_above"]:
        signal = "BUY"
    elif prob_up_adjusted <= cfg["SELL"]["prob_up_below"]:
        signal = "SELL"
    else:
        signal = "HOLD"

    return signal, prob_up_adjusted, prob_down_adjusted, {
        "prob_up_raw":         prob_up_raw,
        "prob_down_raw":       prob_down_raw,
        "macro_adjustment":    macro_adjustment,
        "effective_macro_adj": effective_macro_adj,
        "macro_sentiment":     macro_sentiment,
        "risk_regime":         risk_regime,
    }


def classify_exposure_recommendation(
    *,
    constrained_exposure: float | None = None,
    target_exposure: float | None = None,
    prob_up: float | None = None,
) -> tuple[str, float | None]:
    """
    Clasificación primaria en 5 niveles de exposición.
    Prioriza constrained_exposure; luego target_exposure; fallback a prob_up.
    """
    exposure = constrained_exposure
    if exposure is None:
        exposure = target_exposure

    if exposure is not None:
        pct = max(0.0, min(100.0, float(exposure) * 100.0))
    elif prob_up is not None:
        # Fallback consistente con frontend actual para evitar huecos sin constraints.
        t = (float(prob_up) - 0.30) / (0.75 - 0.30)
        t = max(0.0, min(1.0, t))
        pct = (0.50 + t * (0.85 - 0.50)) * 100.0
    else:
        return "MAINTAIN", None

    bands = MODEL_CONFIG["exposure_recommendation_policy"]["bands_pct"]
    if pct >= bands["INCREASE_STRONG_min"]:
        return "INCREASE_STRONG", round(pct, 2)
    if pct >= bands["INCREASE_MILD_min"]:
        return "INCREASE_MILD", round(pct, 2)
    if pct >= bands["MAINTAIN_min"] and (prob_up is None or float(prob_up) >= 0.48):
        return "MAINTAIN", round(pct, 2)
    if pct >= bands["REDUCE_MILD_min"]:
        return "REDUCE_MILD", round(pct, 2)
    return "REDUCE_STRONG", round(pct, 2)


def recommendation_to_legacy_signal(recommendation: str) -> str:
    """Compatibilidad temporal con contratos legacy en Aurora/reporting."""
    if recommendation in ("INCREASE_STRONG", "INCREASE_MILD"):
        return "BUY"
    if recommendation == "MAINTAIN":
        return "HOLD"
    return "SELL"


def build_reasoning(evidence_states, prob_up, recommendation):
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

    return (
        f"Evidencias: {', '.join(parts) if parts else 'mixtas'}. "
        f"P(subida)={prob_up:.2%} -> recomendación {recommendation}."
    )


def get_recent_signals(connection, ticker: str, batch_date: str, n_days: int) -> list:
    """
    Devuelve las últimas n_days señales CONFIRMADAS de un ticker anteriores
    a batch_date (más reciente primero).
    Se usa para determinar si hay suficientes SELLs consecutivos (hysteresis).
    """
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT signal FROM trading_signals
            WHERE ticker = %s AND batch_date < %s
            ORDER BY batch_date DESC
            LIMIT %s
            """,
            (ticker, batch_date, n_days),
        )
        rows = cursor.fetchall()
        cursor.close()
        return [row[0] for row in rows]  # más reciente primero
    except Exception as exc:
        logger.warning(f"get_recent_signals {ticker}: {exc}")
        return []


def apply_signal_hysteresis(raw_signal: str, recent_signals: list) -> tuple:
    """
    Filtro de persistencia (hysteresis).

    Reglas:
    - BUY / HOLD → pasan directamente sin modificación.
    - SELL → solo se confirma si alguno de los (SELL_CONFIRMATION_DAYS-1)
              días previos también fue SELL. En caso contrario se emite HOLD
              para no cerrar una posición por un único día bajista puntual.

    Parameters
    ----------
    raw_signal     : señal calculada por la red bayesiana para hoy.
    recent_signals : señales de días anteriores (más reciente primero).

    Returns
    -------
    (confirmed_signal, status_str)
    """
    if raw_signal != "SELL":
        return raw_signal, "pass_through"

    # Cuenta SELLs consecutivos en el historial reciente (más reciente primero)
    consecutive = 0
    for s in recent_signals:
        if s == "SELL":
            consecutive += 1
        else:
            break  # se rompe la racha

    if consecutive >= SELL_CONFIRMATION_DAYS - 1:
        return "SELL", f"confirmed_{SELL_CONFIRMATION_DAYS}d"
    else:
        return "HOLD", f"pending_{consecutive + 1}_of_{SELL_CONFIRMATION_DAYS}d"


def get_ticker_data(connection, target_date, ticker):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT sentiment, confidence, headline, justification FROM sentiment_scores WHERE batch_date = %s AND ticker = %s ORDER BY confidence DESC",
        (target_date, ticker),
    )
    all_sentiments = cursor.fetchall()
    cursor.execute(
        "SELECT rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower FROM technical_indicators WHERE batch_date = %s AND ticker = %s LIMIT 1",
        (target_date, ticker),
    )
    indicators = cursor.fetchone()
    cursor.close()
    return all_sentiments, indicators


def aggregate_sentiment(all_sentiments):
    if not all_sentiments:
        return None, None, {}
    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    for row in all_sentiments:
        if row[0] in dist:
            dist[row[0]] += 1
    total = len(all_sentiments)
    distribution = {
        k: {"count": v, "pct": round(v / total * 100, 1)} for k, v in dist.items()
    }
    best = all_sentiments[0]
    dominant_sentiment = max(dist, key=dist.get) if dist else "neutral"
    dominant_confidence = round(float(best[1]), 4)
    headlines_sample = [
        {
            "headline": row[2][:120] + "..." if len(row[2]) > 120 else row[2],
            "sentiment": row[0],
            "confidence": round(float(row[1]), 4),
        }
        for row in all_sentiments[:10]
    ]
    return (
        dominant_sentiment,
        dominant_confidence,
        {
            "total_headlines": total,
            "aggregation_method": "max_confidence",
            "distribution": distribution,
            "dominant": {
                "sentiment": dominant_sentiment,
                "confidence": dominant_confidence,
            },
            "headlines_sample": headlines_sample,
            "limitation": "Se utiliza Voto Mayoritario de todos los titulares del día para decidir el sentimiento.",
        },
    )


def save_bayesian_trace(batch_date, tickers_trace, execution_meta):
    trace = {
        "schema_version": "2.0",
        "batch_date": batch_date,
        "model_id": MODEL_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "execution": execution_meta,
        "model_config": MODEL_CONFIG,
        "tickers": tickers_trace,
        "audit_notes": {
            "cpt_source":       "v1.2: CPT overbought+uptrend corregido, priors con drift alcista",
            "threshold_rsi":    "RSI <30 = oversold, >70 = overbought",
            "threshold_vol":    "BB width ratio >0.05 = high",
            "threshold_signal": "P(up) ≥0.52 = BUY, ≤0.28 = SELL (v1.2)",
            "hysteresis":       f"SELL requiere {SELL_CONFIRMATION_DAYS} días consecutivos para confirmar",
            "macro_dampening":  "macro_adj negativo amortiguado al 40% en uptrend",
            "known_issues":     MODEL_CONFIG["known_limitations"],
        },
    }
    if _mongo_upsert_bayesian_trace:
        _mongo_upsert_bayesian_trace(batch_date, trace)
        return f"mongo:bayesian_traces/{batch_date}"
    logger.warning("mongo_utils upsert_bayesian_trace no disponible; traza no guardada")
    return None


def upsert_signal_explanation(connection, batch_date, ticker, states):
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO signal_explanations (batch_date, ticker, sentiment_state, rsi_state, trend_state, volatility_state)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_date, ticker) DO UPDATE SET sentiment_state=EXCLUDED.sentiment_state, rsi_state=EXCLUDED.rsi_state, trend_state=EXCLUDED.trend_state, volatility_state=EXCLUDED.volatility_state
    """,
        (
            batch_date,
            ticker,
            states["Sentiment"],
            states["RSI"],
            states["Trend"],
            states["Volatility"],
        ),
    )
    connection.commit()
    cursor.close()


def upsert_pipeline_kpi(connection, batch_date, run_id, trigger_type, stage, metrics):
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO pipeline_kpis (batch_date, run_id, trigger_type, stage, metrics) VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (run_id, stage) DO UPDATE SET metrics = EXCLUDED.metrics, updated_at = CURRENT_TIMESTAMP
    """,
        (batch_date, run_id, trigger_type, stage, json.dumps(metrics)),
    )
    connection.commit()
    cursor.close()


def handler(event, context):
    start_time = datetime.now(timezone.utc)
    logger.info("Lambda bayesian network started")
    try:
        model = create_bayesian_network()
        aurora_creds = get_secret("aurora/credentials")
        connection = connect_to_aurora(aurora_creds)
        cursor = connection.cursor()
        ctx = resolve_pipeline_context(event)
        latest_date = ctx["batch_date"]
        if not latest_date:
            cursor.execute("SELECT MAX(batch_date) FROM batch_log")
            latest_date = cursor.fetchone()[0]
        if not latest_date:
            return {"statusCode": 200, "body": "No data"}

        cursor.execute(
            """
            SELECT DISTINCT ticker FROM (
                SELECT ticker FROM sentiment_scores WHERE batch_date = %s
                UNION
                SELECT ticker FROM technical_indicators WHERE batch_date = %s
            ) t
            ORDER BY ticker
            """,
            (latest_date, latest_date),
        )
        tickers = [row[0] for row in cursor.fetchall()]
        cursor.close()
        if not tickers and _mongo_distinct_raw_news_tickers:
            tickers = _mongo_distinct_raw_news_tickers(latest_date)
            if tickers:
                logger.warning(
                    f"Sin filas en Aurora para {latest_date}; "
                    f"tickers desde Mongo raw_news: {tickers}"
                )

        # ── Leer contexto macro del día (genera macro_adjustment) ────────────
        macro_context = get_macro_context(latest_date)
        if macro_context:
            logger.info(
                f"Contexto macro cargado: sentiment={macro_context.get('macro_sentiment')} "
                f"regime={macro_context.get('risk_regime')} "
                f"adj={macro_context.get('macro_adjustment', 0):+.3f}"
            )
        else:
            logger.info("Sin contexto macro disponible — ajuste = 0.0")

        tickers_trace, recommendations_processed, skipped = {}, 0, []

        for ticker in tickers:
            cursor = None
            try:
                all_sentiments, indicators_result = get_ticker_data(
                    connection, latest_date, ticker
                )
                if not all_sentiments or not indicators_result:
                    skipped.append({"ticker": ticker, "reason": "incomplete_data"})
                    continue

                rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower = (
                    indicators_result
                )
                vol_state, bb_width_ratio = discretize_volatility(
                    bb_upper, bb_lower, close_price
                )
                sma_spread = (
                    round(float(sma_20) - float(sma_50), 4)
                    if sma_20 and sma_50
                    else None
                )

                dominant_sentiment, dominant_confidence, sentiment_detail = (
                    aggregate_sentiment(all_sentiments)
                )
                if dominant_sentiment is None:
                    skipped.append({"ticker": ticker, "reason": "no_sentiment"})
                    continue

                evidence_states = {
                    "Sentiment": discretize_sentiment(dominant_sentiment),
                    "RSI": discretize_rsi(float(rsi_14)),
                    "Trend": discretize_trend(float(sma_20), float(sma_50)),
                    "Volatility": vol_state,
                }
                feature_snapshot = (
                    _mongo_read_feature_snapshot(latest_date, ticker)
                    if _mongo_read_feature_snapshot
                    else None
                )
                _, prob_up, prob_down, macro_info = infer_signal(
                    model, evidence_states, macro_context
                )
                contribution_analysis = build_contribution_analysis(
                    model, evidence_states, macro_context
                )

                feature_ref = f"feature_snapshots/{latest_date}/{ticker.upper()}"
                exposure_constraints = {}
                target_exp = None
                if apply_exposure_constraints and prob_to_exposure:
                    macro_detail = (macro_context or {}).get("detail") or {}
                    vix = macro_detail.get("vix")
                    risk_reg = (macro_context or {}).get("risk_regime")
                    regime = detect_market_regime_simple(vix=vix, risk_regime=risk_reg)
                    target_exp = prob_to_exposure(prob_up, regime)
                    fund_stress = None
                    catalyst_count = 0
                    catalyst_net = 0.0
                    if feature_snapshot:
                        fund_stress = (feature_snapshot.get("fundamental") or {}).get(
                            "fundamental_stress"
                        )
                        cats = feature_snapshot.get("catalysts") or {}
                        catalyst_count = int(cats.get("catalyst_count_7d") or 0)
                        catalyst_net = float(cats.get("catalyst_sentiment_net") or 0.0)
                        feature_ref = feature_snapshot.get(
                            "feature_snapshot_ref", feature_ref
                        )
                    exposure_constraints = apply_exposure_constraints(
                        target_exp,
                        market_regime=regime,
                        risk_regime=risk_reg,
                        fundamental_stress=fund_stress,
                        catalyst_count_7d=catalyst_count,
                        catalyst_sentiment_net=catalyst_net,
                    )
                    if _mongo_upsert_feature_snapshot and feature_snapshot:
                        feature_snapshot["exposure_constraints"] = exposure_constraints
                        feature_snapshot["inference_prob_up"] = prob_up
                        _mongo_upsert_feature_snapshot(
                            latest_date, ticker, feature_snapshot
                        )

                constrained_exp = exposure_constraints.get("constrained_exposure")
                raw_recommendation, recommendation_exposure_pct = classify_exposure_recommendation(
                    constrained_exposure=constrained_exp,
                    target_exposure=target_exp,
                    prob_up=prob_up,
                )
                recommendation = raw_recommendation

                reasoning = build_reasoning(evidence_states, prob_up, recommendation)

                upsert_signal_explanation(
                    connection, latest_date, ticker, evidence_states
                )

                cursor = connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO trading_signals (batch_date, ticker, exposure_recommendation, prob_up, prob_down)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (batch_date, ticker) DO UPDATE SET
                      exposure_recommendation=EXCLUDED.exposure_recommendation,
                      prob_up=EXCLUDED.prob_up,
                      prob_down=EXCLUDED.prob_down
                """,
                    (latest_date, ticker, recommendation, prob_up, prob_down),
                )
                connection.commit()

                tickers_trace[ticker] = {
                    "raw_values": {
                        "close_price": (
                            round(float(close_price), 4) if close_price else None
                        ),
                        "rsi_14": round(float(rsi_14), 4) if rsi_14 else None,
                        "sma_20": round(float(sma_20), 4) if sma_20 else None,
                        "sma_50": round(float(sma_50), 4) if sma_50 else None,
                        "sma_spread": sma_spread,
                        "bb_upper": round(float(bb_upper), 4) if bb_upper else None,
                        "bb_lower": round(float(bb_lower), 4) if bb_lower else None,
                        "bb_width_ratio": bb_width_ratio,
                    },
                    "discretization": {
                        "sentiment_raw": dominant_sentiment,
                        "sentiment_conf": dominant_confidence,
                        "sentiment_state": evidence_states["Sentiment"],
                        "rsi_state": evidence_states["RSI"],
                        "trend_state": evidence_states["Trend"],
                        "volatility_state": evidence_states["Volatility"],
                    },
                    "sentiment_detail": sentiment_detail,
                    "inference": {
                        "prob_up":           prob_up,
                        "prob_down":         prob_down,
                        "exposure_recommendation": recommendation,
                        "raw_exposure_recommendation": raw_recommendation,
                        "recommendation_exposure_pct": recommendation_exposure_pct,
                        "macro_context": macro_info,
                    },
                    "contribution_analysis": contribution_analysis,
                    "reasoning": reasoning,
                    "model_id": MODEL_ID,
                    "feature_snapshot_ref": feature_ref,
                    "exposure_constraints": exposure_constraints,
                }
                if _mongo_upsert_bayesian_report:
                    _mongo_upsert_bayesian_report(
                        latest_date,
                        ticker,
                        tickers_trace[ticker],
                        MODEL_CONFIG["version"],
                    )
                recommendations_processed += 1
            except Exception as e:
                # Si una operación SQL falla, hay que limpiar la transacción para
                # que el siguiente ticker no herede el estado "aborted".
                connection.rollback()
                logger.error(f"Error processing {ticker}: {e}")
                skipped.append({"ticker": ticker, "reason": str(e)})
            finally:
                if cursor is not None:
                    cursor.close()

        end_time = datetime.now(timezone.utc)
        execution_meta = {
            "started_at": start_time.isoformat(),
            "finished_at": end_time.isoformat(),
            "duration_seconds": round((end_time - start_time).total_seconds(), 2),
            "run_id": ctx["run_id"],
            "trigger_type": ctx["trigger_type"],
            "batch_date": latest_date,
            "tickers_attempted": len(tickers),
            "recommendations_generated": recommendations_processed,
            "tickers_skipped": len(skipped),
            "skipped_detail": skipped,
        }
        if not tickers_trace:
            execution_meta["warning"] = "no_signals_generated"
            logger.warning(
                f"bayesian {latest_date}: 0 recomendaciones; "
                f"attempted={len(tickers)} skipped={len(skipped)}"
            )
        trace_key = save_bayesian_trace(latest_date, tickers_trace, execution_meta)
        upsert_pipeline_kpi(
            connection,
            latest_date,
            ctx["run_id"],
            ctx["trigger_type"],
            "bayesian",
            {
                "tickers_with_sentiment": len(tickers),
                "recommendations_generated": recommendations_processed,
                "tickers_skipped": len(skipped),
                "trace_storage": "mongo",
                "trace_ref": trace_key,
                "model_version": MODEL_CONFIG["version"],
                "trigger_type": ctx["trigger_type"],
            },
        )
        connection.close()
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Bayesian inference completed",
                    "recommendations": recommendations_processed,
                    "trace_key": trace_key,
                }
            ),
        }
    except Exception as e:
        logger.error(f"Critical error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
