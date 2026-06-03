"""
sentiment_scoring.py — Agregación continua de noticias y ajuste directo a prob_up
================================================================================
El voto mayoritario (bullish/bearish/neutral) colapsa señal mixta → casi siempre
neutral. Este módulo:

  1. Calcula net_score ∈ [-1, 1] ponderado por confianza FinBERT
  2. Discretiza para la BN con umbrales sobre net_score (no mayoría)
  3. Aplica sentiment_adjustment aditivo sobre prob_up (análogo a macro_adj)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def _label_sign(label: str) -> int:
    if label == "bullish":
        return 1
    if label == "bearish":
        return -1
    return 0


def compute_net_sentiment_score(
    samples: List[Dict[str, Any]],
) -> Tuple[float, float, int]:
    """
    net_score: media de sign(sentiment) × confidence, en ~[-1, 1]
    dispersion: 1 - max(bull%, bear%, neut%) — alto cuando titulares se contradicen
    """
    if not samples:
        return 0.0, 0.0, 0

    weighted: List[float] = []
    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    for s in samples:
        label = str(s.get("sentiment") or "neutral").lower()
        if label not in dist:
            label = "neutral"
        dist[label] += 1
        try:
            conf = float(s.get("confidence") or 0.5)
        except (TypeError, ValueError):
            conf = 0.5
        conf = max(0.0, min(1.0, conf))
        weighted.append(_label_sign(label) * conf)

    n = len(weighted)
    net = sum(weighted) / n
    total = float(n)
    max_pct = max(dist.values()) / total
    dispersion = round(1.0 - max_pct, 4)
    return round(net, 4), dispersion, n


def discretize_sentiment_from_net(
    net_score: float,
    *,
    bullish_above: float = 0.10,
    bearish_below: float = -0.10,
) -> str:
    """Estado para la BN a partir del score continuo (no voto mayoritario)."""
    if net_score >= bullish_above:
        return "bullish"
    if net_score <= bearish_below:
        return "bearish"
    return "neutral"


def compute_sentiment_adjustment(
    net_score: float,
    n_headlines: int,
    dispersion: float,
    *,
    max_adj: float = 0.12,
    min_headlines: int = 2,
    min_abs_net: float = 0.08,
) -> float:
    """
    Ajuste aditivo a prob_up, en el espíritu de macro_adjustment.

    - Requiere al menos min_headlines titulares con FinBERT
    - Ignora señal débil (|net| < min_abs_net)
    - Reduce peso si hay mucha contradicción entre titulares (dispersion alta)
    """
    if n_headlines < min_headlines or abs(net_score) < min_abs_net:
        return 0.0
    coherence = max(0.25, 1.0 - dispersion)
    raw = net_score * max_adj * coherence
    return round(max(-max_adj, min(max_adj, raw)), 4)


def aggregate_sentiment_samples(
    samples: List[Dict[str, Any]],
    *,
    headlines_sample: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[str, float, Dict[str, Any]]:
    """
    Reemplazo de voto mayoritario. Devuelve (dominant_label, best_conf, detail).

    detail incluye: net_score, dispersion, sentiment_adjustment (preview), n_headlines
    """
    net_score, dispersion, n = compute_net_sentiment_score(samples)
    dominant = discretize_sentiment_from_net(net_score)

    dist = {"bullish": 0, "bearish": 0, "neutral": 0}
    for s in samples:
        label = str(s.get("sentiment") or "neutral").lower()
        if label in dist:
            dist[label] += 1
    total = max(n, 1)
    distribution = {
        k: {"count": v, "pct": round(v / total * 100, 1)} for k, v in dist.items()
    }

    best_conf = 0.0
    for s in samples:
        if str(s.get("sentiment") or "").lower() == dominant:
            try:
                best_conf = max(best_conf, float(s.get("confidence") or 0))
            except (TypeError, ValueError):
                pass

    preview_adj = compute_sentiment_adjustment(net_score, n, dispersion)

    detail: Dict[str, Any] = {
        "total_headlines": n,
        "aggregation_method": "confidence_weighted_net_score",
        "distribution": distribution,
        "dominant": {"sentiment": dominant, "confidence": best_conf},
        "net_score": net_score,
        "dispersion": dispersion,
        "sentiment_adjustment_preview": preview_adj,
        "headlines_sample": headlines_sample or samples[:10],
        "limitation": (
            "Score continuo (confianza × signo) + discretización por umbrales; "
            "ajuste directo a prob_up vía sentiment_adjustment."
        ),
    }
    return dominant, best_conf, detail


def apply_sentiment_to_prob_up(
    prob_up: float,
    net_score: float,
    n_headlines: int,
    dispersion: float,
    *,
    max_adj: float = 0.12,
) -> Tuple[float, float]:
    """Devuelve (prob_up_ajustada, sentiment_adjustment)."""
    adj = compute_sentiment_adjustment(
        net_score, n_headlines, dispersion, max_adj=max_adj
    )
    adjusted = round(max(0.0, min(1.0, float(prob_up) + adj)), 4)
    return adjusted, adj
