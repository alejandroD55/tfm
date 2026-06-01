"""
exposure_constraints.py — Caps y penalizaciones post prob_up
=============================================================
Aplica después de calcular smoothed_exposure / target_exposure:
  final = min(smoothed_exposure, regime_ceiling, fundamental_cap) - catalyst_penalty
"""
from __future__ import annotations

from typing import Any, Dict, Optional


# Techo por régimen estructural (alineado con prob_to_exposure en bootstrap)
REGIME_CEILINGS: Dict[str, float] = {
    "BULL": 1.00,
    "NEUTRAL": 0.80,
    "HIGH_VOL": 0.60,
    "BEAR": 0.45,
    # alias desde macro risk_regime
    "RISK_ON": 0.90,
    "RISK_ON_STRONG": 1.00,
    "NEUTRAL_MACRO": 0.80,
    "RISK_OFF": 0.50,
    "RISK_OFF_MILD": 0.55,
    "FEAR": 0.35,
}


def compute_fundamental_cap(
    fundamental_stress: Optional[float],
    *,
    default_cap: float = 1.0,
) -> float:
    """
    fundamental_stress ∈ [0, 1]: 0=sano, 1=estrés alto.
    Cap lineal: cap = 1.0 - 0.5 * stress (mín 0.35).
    """
    if fundamental_stress is None:
        return default_cap
    try:
        stress = max(0.0, min(1.0, float(fundamental_stress)))
    except (TypeError, ValueError):
        return default_cap
    return round(max(0.35, 1.0 - 0.5 * stress), 4)


def compute_catalyst_penalty(
    catalyst_count_7d: int,
    catalyst_sentiment_net: float,
    *,
    per_event: float = 0.03,
    max_penalty: float = 0.15,
) -> float:
    """
    Penalización por catalizadores negativos recientes.
    Solo aplica si sentiment_net < 0 y hay eventos detectados.
    """
    if catalyst_count_7d <= 0 or catalyst_sentiment_net >= 0:
        return 0.0
    raw = min(max_penalty, abs(catalyst_sentiment_net) * per_event * catalyst_count_7d)
    return round(raw, 4)


def regime_ceiling(market_regime: Optional[str], risk_regime: Optional[str] = None) -> float:
    """Techo de exposición según régimen estructural o macro."""
    if market_regime and market_regime in REGIME_CEILINGS:
        return REGIME_CEILINGS[market_regime]
    if risk_regime and risk_regime in REGIME_CEILINGS:
        return REGIME_CEILINGS[risk_regime]
    return REGIME_CEILINGS.get("NEUTRAL", 0.80)


def prob_to_exposure(prob_up: float, regime: str) -> float:
    """Mapeo prob_up → exposición objetivo por régimen (Fase 1 bootstrap)."""
    floors = {"BULL": 0.60, "NEUTRAL": 0.35, "HIGH_VOL": 0.20, "BEAR": 0.10}
    ceilings = {"BULL": 1.00, "NEUTRAL": 0.80, "HIGH_VOL": 0.60, "BEAR": 0.45}
    floor = floors.get(regime, 0.35)
    ceiling = ceilings.get(regime, 0.80)
    t = (float(prob_up) - 0.30) / (0.75 - 0.30)
    t = max(0.0, min(1.0, t))
    return round(floor + t * (ceiling - floor), 3)


def detect_market_regime_simple(
    *,
    vix: Optional[float] = None,
    risk_regime: Optional[str] = None,
) -> str:
    """Régimen simplificado cuando no hay SMA200 en lambda bayesian."""
    if risk_regime in ("RISK_OFF", "FEAR", "RISK_OFF_MILD"):
        return "BEAR" if risk_regime == "FEAR" else "HIGH_VOL"
    if vix is not None and vix > 25:
        return "HIGH_VOL"
    if risk_regime in ("RISK_ON", "RISK_ON_STRONG"):
        return "BULL"
    return "NEUTRAL"


def apply_exposure_constraints(
    smoothed_exposure: float,
    *,
    market_regime: Optional[str] = None,
    risk_regime: Optional[str] = None,
    fundamental_stress: Optional[float] = None,
    catalyst_count_7d: int = 0,
    catalyst_sentiment_net: float = 0.0,
) -> Dict[str, Any]:
    """
    final_exposure = max(0, min(smoothed, regime_ceiling, fundamental_cap) - catalyst_penalty)
    """
    smoothed = max(0.0, min(1.0, float(smoothed_exposure)))
    ceil = regime_ceiling(market_regime, risk_regime)
    fcap = compute_fundamental_cap(fundamental_stress)
    penalty = compute_catalyst_penalty(catalyst_count_7d, catalyst_sentiment_net)
    capped = min(smoothed, ceil, fcap)
    final = round(max(0.0, capped - penalty), 4)

    return {
        "smoothed_exposure_input": smoothed,
        "regime_ceiling": ceil,
        "fundamental_cap": fcap,
        "catalyst_penalty": penalty,
        "constrained_exposure": final,
        "market_regime": market_regime,
        "risk_regime": risk_regime,
    }
