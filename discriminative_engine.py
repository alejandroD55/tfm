"""
discriminative_engine.py — Motor de inferencia Camino B (LightGBM)
===================================================================
Motor de inferencia discriminativa que sustituye la Red Bayesiana en
infer_signal() y run_bayesian_inference().

Características:
  - Solo depende de lightgbm (booster nativo) + numpy en runtime.
  - sklearn NO es necesario en inferencia (calibración Platt es numpy puro).
  - Carga perezosa al primer uso; warm-start compatible con Lambda.
  - Fallback silencioso a BN si el modelo no existe o falla la carga.
  - Feature set extendido: ADX, EMA-55, momentum_20d, RSI continuo.

Uso:
    from discriminative_engine import disc_engine
    if disc_engine.available:
        prob_up = disc_engine.infer(evidence_states, macro_context, extra)
    else:
        # usar BN como fallback
        ...
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)

# ── Rutas de búsqueda del modelo (en orden de prioridad) ──────────────────────
_SEARCH_DIRS = [
    Path(__file__).parent / "models",          # local: /tfm/models/
    Path("/tmp/models"),                        # Lambda warm-start
    Path(os.environ.get("DISC_MODEL_DIR", "")) if os.environ.get("DISC_MODEL_DIR") else None,
]

# ── Encodings categóricos (deben coincidir con train_discriminative_model.py) ──
LABEL_ENCODERS: Dict[str, Dict[str, int]] = {
    "sentiment_state":  {"bullish": 0, "neutral": 1, "bearish": 2},
    # RSI 5 niveles para el discriminador
    "rsi_state": {
        "very_oversold": 0, "oversold": 1, "neutral": 2,
        "overbought": 3, "very_overbought": 4,
        # Compatibilidad con 3 niveles legacy
        "oversold_3l": 1, "neutral_3l": 2, "overbought_3l": 3,
    },
    "trend_state":      {"uptrend": 0, "downtrend": 1},
    "volatility_state": {"low": 0, "high": 1},
    "risk_regime": {
        "RISK_ON_STRONG": 3, "RISK_ON": 2, "NEUTRAL": 1,
        "RISK_OFF_MILD": -1, "RISK_OFF": -2, "FEAR": -3,
    },
    "adx_state": {"lateral": 0, "moderate": 1, "trending": 2, "unknown": 1},
}

# ── Feature set extendido (13 base + 5 nuevos = 18 features) ──────────────────
FEATURE_NAMES_V2 = [
    # Features originales (13)
    "sentiment_state",
    "rsi_state",
    "trend_state",
    "volatility_state",
    "prob_up",
    "macro_adjustment",
    "risk_regime",
    "signal_streak",
    "prob_up_delta",
    "prob_up_5d_mean",
    "vol_20d",
    "vol_ratio",
    "sentiment_dispersion",
    # Nuevos features (5)
    "rsi_continuous",    # RSI valor continuo (vs discretizado)
    "adx_14",            # Fuerza de la tendencia
    "ema_55_pct",        # % desviación respecto a EMA-55
    "momentum_20d",      # Retorno de los últimos 20 días
    "momentum_5d",       # Retorno de los últimos 5 días
]

# Defaults razonables para features no disponibles en tiempo real
_FEATURE_DEFAULTS = {
    "sentiment_state":      1,    # neutral
    "rsi_state":            2,    # neutral (en escala 5 niveles)
    "trend_state":          0,    # uptrend
    "volatility_state":     1,    # high
    "prob_up":              0.50,
    "macro_adjustment":     0.0,
    "risk_regime":          1,    # NEUTRAL
    "signal_streak":        1,
    "prob_up_delta":        0.0,
    "prob_up_5d_mean":      0.50,
    "vol_20d":              0.20,
    "vol_ratio":            1.0,
    "sentiment_dispersion": 0.0,
    # Nuevos
    "rsi_continuous":       50.0,
    "adx_14":               20.0,
    "ema_55_pct":           0.0,
    "momentum_20d":         0.0,
    "momentum_5d":          0.0,
}


class DiscriminativeEngine:
    """
    Motor de inferencia discriminativa con booster nativo LightGBM.
    Thread-safe después de la carga inicial.
    Soporta feature sets v1 (13 features) y v2 (18 features) automáticamente.
    """

    def __init__(self):
        self._booster       = None
        self._feature_names: list = []
        self._platt_a: float = 1.0
        self._platt_b: float = 0.0
        self._meta: dict = {}
        self._loaded = False

    # ── Carga ──────────────────────────────────────────────────────────────────

    def _try_load(self) -> bool:
        """Intenta cargar artefactos del modelo. Devuelve True si éxito."""
        try:
            import lightgbm as lgb
        except ImportError:
            logger.debug("lightgbm no instalado — motor discriminativo no disponible")
            return False

        for base in _SEARCH_DIRS:
            if base is None or not base.exists():
                continue
            booster_path = base / "lgbm_booster.txt"
            if not booster_path.exists():
                continue
            try:
                self._booster = lgb.Booster(model_file=str(booster_path))

                features_path = base / "feature_names.json"
                with open(features_path) as f:
                    self._feature_names = json.load(f)

                platt_path = base / "platt_calibration.json"
                with open(platt_path) as f:
                    platt = json.load(f)
                    self._platt_a = float(platt["a"])
                    self._platt_b = float(platt["b"])

                meta_path = base / "lgbm_signal_meta.json"
                if meta_path.exists():
                    with open(meta_path) as f:
                        self._meta = json.load(f)

                logger.info(
                    f"✅ Motor discriminativo cargado desde {base} "
                    f"| AUC={self._meta.get('auc_val', 'N/A')} "
                    f"| features={len(self._feature_names)} "
                    f"| entrenado: {self._meta.get('trained_at', '?')[:10]}"
                )
                return True
            except Exception as exc:
                logger.warning(f"No se pudo cargar modelo discriminativo desde {base}: {exc}")

        return False

    def load(self) -> bool:
        """Carga el modelo si no está ya cargado. Devuelve True si disponible."""
        if not self._loaded:
            self._loaded = True
            self.available = self._try_load()
        return self.available

    # ── Construcción del vector de features ────────────────────────────────────

    def _encode_rsi_state(self, rsi_state_raw: str) -> int:
        """
        Codifica RSI state al espacio de la versión del modelo.
        v1 (3 niveles): oversold=0, neutral=1, overbought=2
        v2 (5 niveles): very_oversold=0, oversold=1, neutral=2, overbought=3, very_overbought=4
        """
        # Detectar versión del modelo por número de features
        is_v2 = len(self._feature_names) > 13

        if is_v2:
            return LABEL_ENCODERS["rsi_state"].get(rsi_state_raw, 2)  # default neutral=2
        else:
            # Mapeo de 5 niveles a 3 para modelos legacy
            mapping_to_3 = {
                "very_oversold": 0, "oversold": 0,
                "neutral": 1,
                "overbought": 2, "very_overbought": 2,
            }
            legacy = {"oversold": 0, "neutral": 1, "overbought": 2}
            return mapping_to_3.get(rsi_state_raw, legacy.get(rsi_state_raw, 1))

    def build_features(
        self,
        evidence_states: Dict,
        macro_context:   Optional[Dict],
        extra:           Optional[Dict],
    ) -> np.ndarray:
        """
        Construye el vector de features en el mismo orden que durante el entrenamiento.
        Soporta automáticamente v1 (13 features) y v2 (18 features).

        Parámetros
        ----------
        evidence_states : dict con claves Sentiment, RSI, Trend, Volatility
        macro_context   : dict con macro_adjustment, risk_regime, macro_sentiment
        extra           : dict con features opcionales:
                          prob_up_bn, signal_streak, prob_up_delta, prob_up_5d_mean,
                          vol_20d, vol_ratio, sentiment_dispersion,
                          rsi_continuous, adx_14, ema_55_pct, momentum_20d, momentum_5d
        """
        extra = extra or {}
        mc    = macro_context or {}

        # Encodings categóricos
        sent_raw = (evidence_states.get("Sentiment") or "neutral").lower()
        rsi_raw  = (evidence_states.get("RSI")       or "neutral").lower()
        trn_raw  = (evidence_states.get("Trend")     or "uptrend").lower()
        vol_raw  = (evidence_states.get("Volatility") or "high").lower()
        rgm_raw  = (mc.get("risk_regime") or "NEUTRAL").upper()

        sent_enc = LABEL_ENCODERS["sentiment_state"].get(sent_raw, 1)
        rsi_enc  = self._encode_rsi_state(rsi_raw)
        trn_enc  = LABEL_ENCODERS["trend_state"].get(trn_raw, 0)
        vol_enc  = LABEL_ENCODERS["volatility_state"].get(vol_raw, 1)
        rgm_enc  = LABEL_ENCODERS["risk_regime"].get(rgm_raw, 1)

        # Continuas originales
        prob_up   = float(extra.get("prob_up_bn", _FEATURE_DEFAULTS["prob_up"]))
        macro_adj = float(mc.get("macro_adjustment", _FEATURE_DEFAULTS["macro_adjustment"]))
        streak    = float(extra.get("signal_streak",    _FEATURE_DEFAULTS["signal_streak"]))
        delta     = float(extra.get("prob_up_delta",    _FEATURE_DEFAULTS["prob_up_delta"]))
        mean5     = float(extra.get("prob_up_5d_mean",  prob_up))
        vol20d    = float(extra.get("vol_20d",          _FEATURE_DEFAULTS["vol_20d"]))
        vol_ratio = float(extra.get("vol_ratio",        _FEATURE_DEFAULTS["vol_ratio"]))
        sent_disp = float(extra.get("sentiment_dispersion", _FEATURE_DEFAULTS["sentiment_dispersion"]))

        # Nuevas features continuas (v2)
        rsi_cont  = float(extra.get("rsi_continuous",  _FEATURE_DEFAULTS["rsi_continuous"]))
        adx_14    = float(extra.get("adx_14",          _FEATURE_DEFAULTS["adx_14"]) or _FEATURE_DEFAULTS["adx_14"])
        ema55_pct = float(extra.get("ema_55_pct",      _FEATURE_DEFAULTS["ema_55_pct"]) or 0.0)
        mom_20d   = float(extra.get("momentum_20d",    _FEATURE_DEFAULTS["momentum_20d"]) or 0.0)
        mom_5d    = float(extra.get("momentum_5d",     _FEATURE_DEFAULTS["momentum_5d"]) or 0.0)

        feature_map = {
            # v1 features
            "sentiment_state":      float(sent_enc),
            "rsi_state":            float(rsi_enc),
            "trend_state":          float(trn_enc),
            "volatility_state":     float(vol_enc),
            "prob_up":              prob_up,
            "macro_adjustment":     macro_adj,
            "risk_regime":          float(rgm_enc),
            "signal_streak":        streak,
            "prob_up_delta":        delta,
            "prob_up_5d_mean":      mean5,
            "vol_20d":              vol20d,
            "vol_ratio":            vol_ratio,
            "sentiment_dispersion": sent_disp,
            # v2 features
            "rsi_continuous":       rsi_cont,
            "adx_14":               adx_14,
            "ema_55_pct":           ema55_pct,
            "momentum_20d":         mom_20d,
            "momentum_5d":          mom_5d,
        }

        # Respetar el orden exacto de entrenamiento
        feat_order = self._feature_names if self._feature_names else list(feature_map.keys())
        return np.array([feature_map.get(f, 0.0) for f in feat_order], dtype=float).reshape(1, -1)

    # ── Inferencia ─────────────────────────────────────────────────────────────

    def infer(
        self,
        evidence_states: Dict,
        macro_context:   Optional[Dict] = None,
        extra:           Optional[Dict] = None,
    ) -> float:
        """
        Devuelve prob_up calibrada (Platt en log-odds) para la combinación de evidencia.

        Raises RuntimeError si el motor no está disponible (llama a load() antes).
        """
        if not self._loaded:
            self.load()
        if not self.available or self._booster is None:
            raise RuntimeError("Motor discriminativo no disponible")

        X = self.build_features(evidence_states, macro_context, extra)

        # Inferencia con booster nativo (sin sklearn)
        raw_prob = float(self._booster.predict(X)[0])

        # Calibración Platt en espacio log-odds (numpy puro, sin sklearn).
        # prob_cal = sigmoid(a * logit(raw_prob) + b)
        # Al operar en log-odds el optimizador puede expandir el rango
        # simétricamente y generar tanto BUY como SELL.
        raw_clipped = float(np.clip(raw_prob, 1e-7, 1.0 - 1e-7))
        logit_raw   = float(np.log(raw_clipped / (1.0 - raw_clipped)))
        logit_cal   = self._platt_a * logit_raw + self._platt_b
        prob_cal    = float(1.0 / (1.0 + np.exp(-logit_cal)) if logit_cal >= 0
                            else np.exp(logit_cal) / (1.0 + np.exp(logit_cal)))
        return round(float(np.clip(prob_cal, 0.0, 1.0)), 4)

    @property
    def meta(self) -> dict:
        return self._meta


# ── Singleton global ───────────────────────────────────────────────────────────
# Carga perezosa: el modelo se carga la primera vez que se llama a disc_engine.load()
# o disc_engine.infer(). Compatible con Lambda warm-start.

disc_engine = DiscriminativeEngine()
disc_engine.available = False  # se establece en la primera llamada a load()
