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

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
rds_client = boto3.client("rds")

DATALAKE_BUCKET = "tfm-unir-datalake"


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
        "BUY": {"prob_up_above": 0.65, "rationale": "Alta confianza alcista"},
        "SELL": {
            "prob_up_below": 0.35,
            "rationale": "Alta confianza bajista / Salir a Cash",
        },
        "HOLD": {
            "range": [0.35, 0.65],
            "rationale": "Incertidumbre elevada / Salir a Cash",
        },
    },
    "priors": {
        "Sentiment": {
            "bullish": 0.30,
            "bearish": 0.30,
            "neutral": 0.40,
            "rationale": "Prior levemente favorable a neutral en mercados eficientes",
        },
        "RSI": {
            "oversold": 0.20,
            "neutral": 0.60,
            "overbought": 0.20,
            "rationale": "La mayoria del tiempo el RSI esta en zona neutral",
        },
        "Trend": {
            "uptrend": 0.50,
            "downtrend": 0.50,
            "rationale": "Prior uniforme: no hay sesgo a priori sobre la tendencia",
        },
        "Volatility": {
            "low": 0.60,
            "high": 0.40,
            "rationale": "Los mercados suelen tener baja volatilidad mas frecuentemente",
        },
    },
    "cpt_market_direction": {
        "variable": "MarketDirection",
        "states": ["down", "up"],
        "evidence_order": ["Sentiment", "RSI", "Trend", "Volatility"],
        "rationale": {
            "momentum_logic": "RSI sobrecomprado + Tendencia alcista = Fuerte Momentum comprador",
            "bearish+overbought+downtrend+high": "Maxima confluencia bajista -> P(up)=0.05",
        },
        "values_P_down": [
            0.15,
            0.25,
            0.30,
            0.20,
            0.30,
            0.35,
            0.30,
            0.40,
            0.10,
            0.15,
            0.45,
            0.50,
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
            0.45,
            0.50,
            0.55,
            0.50,
            0.55,
            0.60,
            0.55,
            0.60,
            0.25,
            0.30,
            0.65,
            0.70,
        ],
        "values_P_up": [
            0.85,
            0.75,
            0.70,
            0.80,
            0.70,
            0.65,
            0.70,
            0.60,
            0.90,
            0.85,
            0.55,
            0.50,
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
            0.55,
            0.50,
            0.45,
            0.50,
            0.45,
            0.40,
            0.45,
            0.40,
            0.75,
            0.70,
            0.35,
            0.30,
        ],
    },
    "known_limitations": [
        "El confidence score de FinBERT no entra en la inferencia (solo se guarda)",
        "Se usa voto mayoritario de los titulares",
        "Estrategia Momentum ajustada para capturar subidas fuertes en sobrecompra",
    ],
}


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


def infer_signal(model, evidence_states):
    infer = VariableElimination(model)
    result = infer.query(
        variables=["MarketDirection"], evidence=evidence_states, show_progress=False
    )
    prob_up = round(float(result.values[1]), 4)
    prob_down = round(float(result.values[0]), 4)

    cfg = MODEL_CONFIG["signal_thresholds"]
    if prob_up >= cfg["BUY"]["prob_up_above"]:
        signal = "BUY"
    elif prob_up <= cfg["SELL"]["prob_up_below"]:
        signal = "SELL"
    else:
        signal = "HOLD"

    return signal, prob_up, prob_down


def build_reasoning(evidence_states, prob_up, signal):
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

    return (
        f"Evidencias: {', '.join(parts) if parts else 'mixtas'}. "
        f"P(subida)={prob_up:.2%} -> senal {signal} (umbral: {th})."
    )


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
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "execution": execution_meta,
        "model_config": MODEL_CONFIG,
        "tickers": tickers_trace,
        "audit_notes": {
            "cpt_source": "Parametros ajustados para capturar momentum alcista",
            "threshold_rsi": "RSI <30 = oversold, >70 = overbought",
            "threshold_vol": "BB width ratio >0.05 = high",
            "threshold_signal": "P(up) >0.65 = BUY, <0.35 = SELL",
            "known_issues": MODEL_CONFIG["known_limitations"],
        },
    }
    key = f"results/{batch_date}/bayesian_trace.json"
    s3_client.put_object(
        Bucket=DATALAKE_BUCKET,
        Key=key,
        Body=json.dumps(trace, indent=2, default=str),
        ContentType="application/json",
    )
    return key


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
            "SELECT DISTINCT ticker FROM sentiment_scores WHERE batch_date = %s",
            (latest_date,),
        )
        tickers = [row[0] for row in cursor.fetchall()]
        cursor.close()

        tickers_trace, signals_processed, skipped = {}, 0, []

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
                signal, prob_up, prob_down = infer_signal(model, evidence_states)
                reasoning = build_reasoning(evidence_states, prob_up, signal)

                upsert_signal_explanation(
                    connection, latest_date, ticker, evidence_states
                )

                cursor = connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO trading_signals (batch_date, ticker, signal, prob_up, prob_down) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (batch_date, ticker) DO UPDATE SET signal=EXCLUDED.signal, prob_up=EXCLUDED.prob_up, prob_down=EXCLUDED.prob_down
                """,
                    (latest_date, ticker, signal, prob_up, prob_down),
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
                        "prob_up": prob_up,
                        "prob_down": prob_down,
                        "signal": signal,
                        "threshold_used": (
                            MODEL_CONFIG["signal_thresholds"]["BUY"]["prob_up_above"]
                            if signal == "BUY"
                            else (
                                MODEL_CONFIG["signal_thresholds"]["SELL"][
                                    "prob_up_below"
                                ]
                                if signal == "SELL"
                                else MODEL_CONFIG["signal_thresholds"]["HOLD"]["range"]
                            )
                        ),
                    },
                    "reasoning": reasoning,
                }
                signals_processed += 1
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
            "tickers_attempted": len(tickers),
            "signals_generated": signals_processed,
            "tickers_skipped": len(skipped),
            "skipped_detail": skipped,
        }
        trace_key = save_bayesian_trace(latest_date, tickers_trace, execution_meta)
        upsert_pipeline_kpi(
            connection,
            latest_date,
            ctx["run_id"],
            ctx["trigger_type"],
            "bayesian",
            {
                "tickers_with_sentiment": len(tickers),
                "signals_generated": signals_processed,
                "tickers_skipped": len(skipped),
                "trace_s3_key": trace_key,
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
                    "signals": signals_processed,
                    "trace_key": trace_key,
                }
            ),
        }
    except Exception as e:
        logger.error(f"Critical error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
