"""
feature_builder.py — Construye feature_snapshot por (batch_date, ticker)
=========================================================================
Agrega sentimiento, técnico, macro, catalizadores y fundamentales (con fallback).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from catalyst import analyze_headlines_for_ticker
from exposure_constraints import apply_exposure_constraints

logger = logging.getLogger(__name__)

FEATURE_SCHEMA_VERSION = "1.0"
MODEL_ID_BAYESIAN = "bayesian_v1.2"
MODEL_ID_GBM = "gbm_v1"


def _sentiment_dispersion(distribution: Dict[str, Any]) -> float:
    """1 - max(pct)/100 sobre bullish/bearish/neutral."""
    if not distribution:
        return 0.0
    pcts = []
    for key in ("bullish", "bearish", "neutral"):
        block = distribution.get(key) or {}
        if isinstance(block, dict):
            pcts.append(float(block.get("pct", 0)))
        else:
            pcts.append(0.0)
    if not pcts:
        return 0.0
    return round(1.0 - max(pcts) / 100.0, 4)


def aggregate_sentiment_rows(rows: List[Tuple]) -> Dict[str, Any]:
    """
    rows: [(sentiment, confidence, headline, justification), ...]
    Compatible con cursor Aurora sentiment_scores.
    """
    if not rows:
        return {
            "score": None,
            "state": None,
            "dispersion": 0.0,
            "n_headlines": 0,
            "detail": {},
        }

    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    confidences = []
    for row in rows:
        sent = row[0]
        if sent in dist:
            dist[sent] += 1
        try:
            confidences.append(float(row[1]))
        except (TypeError, ValueError):
            pass

    total = len(rows)
    distribution = {
        k: {"count": v, "pct": round(v / total * 100, 1)} for k, v in dist.items()
    }
    dominant = max(dist, key=dist.get) if dist else "neutral"
    best_conf = 0.0
    for row in rows:
        if row[0] == dominant:
            try:
                best_conf = max(best_conf, float(row[1]))
            except (TypeError, ValueError):
                pass

    score_map = {"bullish": 0.35, "neutral": 0.0, "bearish": -0.35}
    score = round(score_map.get(dominant, 0.0) * (best_conf or 0.5), 4)

    return {
        "score": score,
        "state": dominant,
        "dispersion": _sentiment_dispersion(distribution),
        "n_headlines": total,
        "detail": {
            "distribution": distribution,
            "dominant_confidence": round(best_conf, 4),
        },
    }


def technical_from_row(indicators: Optional[Tuple]) -> Dict[str, Any]:
    """(rsi_14, sma_20, sma_50, close_price, bb_upper, bb_lower)"""
    if not indicators:
        return {}
    rsi, sma20, sma50, close, bb_u, bb_l = indicators
    bb_width_ratio = None
    try:
        if close and bb_u and bb_l and float(close) > 0:
            bb_width_ratio = round(
                (float(bb_u) - float(bb_l)) / float(close), 6
            )
    except (TypeError, ValueError):
        pass
    return {
        "rsi_14": float(rsi) if rsi is not None else None,
        "sma_20": float(sma20) if sma20 is not None else None,
        "sma_50": float(sma50) if sma50 is not None else None,
        "close_price": float(close) if close is not None else None,
        "bb_width_ratio": bb_width_ratio,
    }


def macro_from_doc(macro_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not macro_doc:
        return {
            "macro_sentiment": None,
            "risk_regime": None,
            "macro_score": None,
            "vix": None,
            "macro_events": {},
            "macro_adjustment": 0.0,
        }
    detail = macro_doc.get("detail") or {}
    events = detail.get("events") or detail.get("macro_events") or {}
    return {
        "macro_sentiment": macro_doc.get("macro_sentiment"),
        "risk_regime": macro_doc.get("risk_regime"),
        "macro_score": detail.get("score") or detail.get("macro_score"),
        "vix": detail.get("vix"),
        "macro_events": events,
        "macro_adjustment": macro_doc.get("macro_adjustment", 0.0),
    }


def fundamental_fallback() -> Dict[str, Any]:
    return {
        "revenue_growth_yoy": None,
        "ebitda_margin": None,
        "debt_equity": None,
        "fundamental_stress": None,
        "source": "unavailable",
    }


def fetch_fundamentals_finnhub(ticker: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """Best-effort Finnhub metrics; graceful fallback."""
    key = api_key or os.getenv("FINNHUB_API_KEY")
    if not key:
        return fundamental_fallback()
    try:
        import urllib.parse
        import urllib.request

        sym = urllib.parse.quote(ticker.upper())
        url = (
            f"https://finnhub.io/api/v1/stock/metric?symbol={sym}&metric=all"
            f"&token={key}"
        )
        with urllib.request.urlopen(url, timeout=8) as resp:
            import json

            data = json.loads(resp.read().decode())
        metric = (data or {}).get("metric") or {}
        rev_growth = metric.get("revenueGrowthTTMYoy") or metric.get("revenueGrowthQuarterlyYoy")
        ebitda_m = metric.get("ebitdaMarginTTM") or metric.get("operatingMarginTTM")
        debt_eq = metric.get("totalDebt/totalEquityAnnual") or metric.get("debtEquityRatio")

        stress = None
        if rev_growth is not None and float(rev_growth) < -0.05:
            stress = 0.6
        if debt_eq is not None and float(debt_eq) > 2.0:
            stress = max(stress or 0.0, 0.7)

        return {
            "revenue_growth_yoy": float(rev_growth) if rev_growth is not None else None,
            "ebitda_margin": float(ebitda_m) if ebitda_m is not None else None,
            "debt_equity": float(debt_eq) if debt_eq is not None else None,
            "fundamental_stress": stress,
            "source": "finnhub",
        }
    except Exception as exc:
        logger.debug(f"Finnhub fundamentals {ticker}: {exc}")
        return fundamental_fallback()


def build_feature_snapshot(
    batch_date: str,
    ticker: str,
    *,
    sentiment_rows: List[Tuple],
    indicators_row: Optional[Tuple],
    macro_doc: Optional[Dict[str, Any]],
    headlines: Optional[List[Dict[str, Any]]] = None,
    fundamentals: Optional[Dict[str, Any]] = None,
    market_regime: Optional[str] = None,
    smoothed_exposure: Optional[float] = None,
    model_id: str = MODEL_ID_BAYESIAN,
) -> Dict[str, Any]:
    """Construye el documento feature_snapshot listo para MongoDB."""
    ticker_u = ticker.upper()
    sentiment = aggregate_sentiment_rows(sentiment_rows)
    technical = technical_from_row(indicators_row)
    macro = macro_from_doc(macro_doc)
    catalyst = analyze_headlines_for_ticker(headlines or [])
    fundamental = fundamentals or fundamental_fallback()

    snapshot = {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "batch_date": batch_date,
        "ticker": ticker_u,
        "model_id": model_id,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "sentiment": sentiment,
        "technical": technical,
        "macro": macro,
        "catalysts": {
            "catalyst_count_7d": catalyst.get("catalyst_count_7d", 0),
            "catalyst_next_days": catalyst.get("catalyst_next_days"),
            "catalyst_sentiment_net": catalyst.get("catalyst_sentiment_net", 0.0),
            "events_sample": catalyst.get("events", [])[:10],
        },
        "fundamental": fundamental,
    }

    if smoothed_exposure is not None:
        constraints = apply_exposure_constraints(
            smoothed_exposure,
            market_regime=market_regime,
            risk_regime=macro.get("risk_regime"),
            fundamental_stress=fundamental.get("fundamental_stress"),
            catalyst_count_7d=snapshot["catalysts"]["catalyst_count_7d"],
            catalyst_sentiment_net=snapshot["catalysts"]["catalyst_sentiment_net"],
        )
        snapshot["exposure_constraints"] = constraints

    snapshot["feature_snapshot_ref"] = f"feature_snapshots/{batch_date}/{ticker_u}"
    return snapshot
