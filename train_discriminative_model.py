#!/usr/bin/env python3
"""
train_discriminative_model.py — Camino B: Clasificador Discriminativo LightGBM v2
==================================================================================
Entrena un clasificador LightGBM como sustituto de la Red Bayesiana para predecir
la dirección del mercado (UP/DOWN) con validación temporal walk-forward.

Feature set v2 (18 features):
  Originales (13): sentiment_state, rsi_state, trend_state, volatility_state,
                   prob_up, macro_adjustment, risk_regime, signal_streak,
                   prob_up_delta, prob_up_5d_mean, vol_20d, vol_ratio, sentiment_dispersion
  Nuevos (5):      rsi_continuous, adx_14, ema_55_pct, momentum_20d, momentum_5d

Target: retorno relativo a la mediana por ticker (split 50/50 garantizado).
        Elimina el sesgo de mercados alcistas donde UP ≫ DOWN.

Genera en /tfm/models/:
  lgbm_booster.txt          — booster nativo LightGBM (sin sklearn en runtime)
  lgbm_signal_meta.json     — AUC, Brier Score, fecha, n_obs, features
  platt_calibration.json    — parámetros de calibración Platt (a, b) en log-odds
  feature_names.json        — lista ordenada de features
  label_encoders.json       — mapas de encoding categórico
  shap_summary.png          — SHAP feature importance (si --shap activado)

Uso:
    python train_discriminative_model.py
    python train_discriminative_model.py --outcome d5 --no-shap
    python train_discriminative_model.py --min-rows 80
"""

import os
import sys
import json
import argparse
import logging
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

try:
    import lightgbm as lgb
    from lightgbm import LGBMClassifier
except ImportError:
    print("ERROR: lightgbm no instalado.  pip install lightgbm")
    sys.exit(1)

try:
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import roc_auc_score, brier_score_loss
    import joblib
except ImportError:
    print("ERROR: scikit-learn/joblib no instalados.  pip install scikit-learn joblib")
    sys.exit(1)

try:
    from scipy.optimize import minimize
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("AVISO: scipy no instalado — calibración Platt usará identidad. pip install scipy")

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv()
MODEL_DIR = Path(__file__).parent / "models"
MODEL_DIR.mkdir(exist_ok=True)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "127.0.0.1"),
    "port":     int(os.getenv("POSTGRES_PORT", 5433)),
    "database": os.getenv("POSTGRES_DB",   "tfm"),
    "user":     os.getenv("POSTGRES_USER", "tfmadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "localpassword123"),
}

# ── Feature set v2 (18 features) ──────────────────────────────────────────────
FEATURE_NAMES = [
    # Originales (13)
    "sentiment_state", "rsi_state", "trend_state", "volatility_state",
    "prob_up", "macro_adjustment", "risk_regime",
    "signal_streak", "prob_up_delta", "prob_up_5d_mean",
    "vol_20d", "vol_ratio", "sentiment_dispersion",
    # Nuevos (5)
    "rsi_continuous",   # RSI valor continuo
    "adx_14",           # Average Directional Index
    "ema_55_pct",       # % desviación sobre EMA-55
    "momentum_20d",     # Retorno 20 días
    "momentum_5d",      # Retorno 5 días
]

LABEL_ENCODERS: Dict[str, Dict[str, int]] = {
    "sentiment_state": {"bullish": 0, "neutral": 1, "bearish": 2},
    # RSI 5 niveles
    "rsi_state": {
        "very_oversold": 0, "oversold": 1, "neutral": 2,
        "overbought": 3, "very_overbought": 4,
    },
    "trend_state":      {"uptrend": 0, "downtrend": 1},
    "volatility_state": {"low": 0, "high": 1},
    "risk_regime": {
        "RISK_ON_STRONG": 3, "RISK_ON": 2, "NEUTRAL": 1,
        "RISK_OFF_MILD": -1, "RISK_OFF": -2, "FEAR": -3,
    },
}

LGBM_PARAMS = {
    "objective": "binary", "metric": "auc",
    "num_leaves": 16, "max_depth": 4, "learning_rate": 0.05,
    "n_estimators": 400, "min_child_samples": 10,
    "subsample": 0.8, "colsample_bytree": 0.8,
    "reg_alpha": 0.1, "reg_lambda": 1.0,
    "class_weight": "balanced", "random_state": 42, "verbose": -1,
}


# =============================================================================
# 1. CARGA DE DATOS
# =============================================================================

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_training_data(outcome_col: str = "outcome_d3") -> pd.DataFrame:
    """
    Carga signal_outcomes + technical_indicators para obtener los nuevos features.
    Target: retorno relativo a la mediana por ticker (50/50 garantizado).
    """
    logger.info(f"Conectando a PostgreSQL {DB_CONFIG['host']}:{DB_CONFIG['port']}…")
    conn = get_db_connection()

    _horizon = outcome_col.replace("outcome_d", "")
    price_col = f"price_d{_horizon}"

    # Incluir rsi_14_cont, adx_14, ema_55_pct, momentum_20d, momentum_5d
    # desde la tabla technical_indicators si existen, o calcularlos
    query = f"""
        SELECT
            so.batch_date,
            so.ticker,
            so.signal,
            so.prob_up,
            so.sentiment_state,
            so.rsi_state,
            so.trend_state,
            so.volatility_state,
            so.macro_adjustment,
            so.risk_regime,
            so.{outcome_col}        AS outcome,
            so.price_d0             AS price_d0,
            so.{price_col}          AS price_dn,
            COALESCE(ps.vol_20d,          0.20) AS vol_20d,
            COALESCE(ps.vol_ratio,        1.00) AS vol_ratio,
            COALESCE(ps.sentiment_dispersion, 0.0) AS sentiment_dispersion,
            -- Nuevos indicadores (pueden ser NULL si son datos anteriores al upgrade)
            COALESCE(ti.rsi_14,    50.0) AS rsi_continuous,
            COALESCE(ti.adx_14,    20.0) AS adx_14,
            COALESCE(ti.ema_55_pct, 0.0) AS ema_55_pct,
            COALESCE(ti.momentum_20d, 0.0) AS momentum_20d,
            COALESCE(ti.momentum_5d,  0.0) AS momentum_5d
        FROM signal_outcomes so
        LEFT JOIN position_state ps
            ON so.batch_date = ps.batch_date AND so.ticker = ps.ticker
        LEFT JOIN technical_indicators ti
            ON so.batch_date = ti.batch_date AND so.ticker = ti.ticker
        WHERE so.{outcome_col} IS NOT NULL
          AND so.{outcome_col} != 'FLAT'
          AND so.price_d0  IS NOT NULL
          AND so.{price_col} IS NOT NULL
        ORDER BY so.batch_date, so.ticker
    """

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(query, conn)
    finally:
        conn.close()

    logger.info(f"Filas cargadas: {len(df)}")

    # Retorno numérico real
    df["return_d"] = (df["price_dn"] - df["price_d0"]) / df["price_d0"]

    # Normalizar estados categóricos
    for col in ["sentiment_state", "rsi_state", "trend_state", "volatility_state"]:
        df[col] = df[col].str.lower().str.strip()
    df["risk_regime"] = df["risk_regime"].str.upper().str.strip().fillna("NEUTRAL")

    # Normalizar RSI 3→5 niveles para datos legacy
    rsi_3to5 = {"oversold": "oversold", "neutral": "neutral", "overbought": "overbought"}
    df["rsi_state"] = df["rsi_state"].apply(
        lambda x: x if x in LABEL_ENCODERS["rsi_state"] else rsi_3to5.get(x, "neutral")
    )

    valid_sentiment = set(LABEL_ENCODERS["sentiment_state"].keys())
    valid_trend     = set(LABEL_ENCODERS["trend_state"].keys())
    valid_vol       = set(LABEL_ENCODERS["volatility_state"].keys())
    mask = (
        df["sentiment_state"].isin(valid_sentiment) &
        df["trend_state"].isin(valid_trend) &
        df["volatility_state"].isin(valid_vol)
    )
    df = df[mask].copy()

    logger.info(f"Filas válidas: {len(df)}")
    logger.info(
        f"Retorno {_horizon}d — mediana={df['return_d'].median()*100:.2f}%  "
        f"media={df['return_d'].mean()*100:.2f}%  std={df['return_d'].std()*100:.2f}%"
    )
    return df


# =============================================================================
# 2. FEATURE ENGINEERING
# =============================================================================

def encode_categorical(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col, mapping in LABEL_ENCODERS.items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna(
                2 if col == "rsi_state" else 1
            ).astype(float)
    return df


def compute_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["prob_up_delta"]  = df.groupby("ticker")["prob_up"].diff().fillna(0.0)
    df["prob_up_5d_mean"] = (
        df.groupby("ticker")["prob_up"]
        .transform(lambda s: s.rolling(5, min_periods=1).mean())
    )
    streaks = []
    for ticker, group in df.groupby("ticker"):
        streak = 1; prev = None
        for sig in group["signal"]:
            if sig == prev: streak += 1
            else:           streak = 1
            streaks.append(streak)
            prev = sig
    df["signal_streak"] = streaks
    return df


def build_feature_matrix(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """
    Construye X (n × 18) y y (target: above-median return por ticker).
    Target balanceado 50/50 por construcción → elimina sesgo de bull market.
    """
    df = encode_categorical(df)
    df = compute_momentum_features(df)

    if "return_d" in df.columns:
        df["target"] = df.groupby("ticker")["return_d"].transform(
            lambda x: (x > x.median()).astype(int)
        )
        y = df["target"].values.astype(int)
        logger.info(
            f"Target (mediana por ticker): {y.sum()} positivos / {len(y)} total "
            f"({y.mean()*100:.1f}% above-median)"
        )
    else:
        logger.warning("Sin columna return_d — usando etiqueta UP/DOWN original")
        y = (df["outcome"] == "UP").astype(int).values

    for col in ["vol_20d", "vol_ratio", "sentiment_dispersion",
                "rsi_continuous", "adx_14", "ema_55_pct", "momentum_20d", "momentum_5d"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    # Verificar que todas las features existen
    missing = [f for f in FEATURE_NAMES if f not in df.columns]
    if missing:
        logger.warning(f"Features faltantes (se rellenan con 0): {missing}")
        for f in missing:
            df[f] = 0.0

    X = df[FEATURE_NAMES].values.astype(float)
    return X, y


# =============================================================================
# 3. CALIBRACIÓN PLATT (log-odds space)
# =============================================================================

def _sigmoid(x: np.ndarray) -> np.ndarray:
    return np.where(x >= 0, 1/(1+np.exp(-x)), np.exp(x)/(1+np.exp(x)))

def _logit(p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-7, 1-1e-7)
    return np.log(p / (1-p))

def fit_platt_calibration(raw_probs: np.ndarray, y_true: np.ndarray) -> Tuple[float, float]:
    """
    Calibración Platt en espacio log-odds:
        prob_cal = sigmoid(a * logit(raw_prob) + b)
    Permite expandir el rango simétricamente y generar BUY y SELL.
    """
    logit_raw = _logit(raw_probs)

    def neg_log_likelihood(params):
        a, b = params
        p = _sigmoid(a * logit_raw + b)
        p = np.clip(p, 1e-7, 1-1e-7)
        return -np.mean(y_true * np.log(p) + (1-y_true) * np.log(1-p))

    if SCIPY_AVAILABLE:
        res = minimize(neg_log_likelihood, [1.0, 0.0], method="L-BFGS-B",
                       bounds=[(0.01, 10.0), (-5.0, 5.0)])
        a, b = float(res.x[0]), float(res.x[1])
    else:
        a, b = 1.0, 0.0

    logger.info(f"Platt calibration (logit-space): a={a:.4f}, b={b:.4f}")
    return a, b


def apply_platt(raw_probs: np.ndarray, a: float, b: float) -> np.ndarray:
    return _sigmoid(a * _logit(raw_probs) + b)


# =============================================================================
# 4. WALK-FORWARD CROSS-VALIDATION
# =============================================================================

def walk_forward_cv(X: np.ndarray, y: np.ndarray, n_splits: int = 3) -> Dict:
    tscv = TimeSeriesSplit(n_splits=n_splits)
    fold_results = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X), start=1):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        if len(np.unique(y_tr)) < 2 or len(np.unique(y_val)) < 2:
            logger.warning(f"  Fold {fold}: clase única — saltando")
            continue
        if len(X_tr) < 30:
            logger.warning(f"  Fold {fold}: demasiado pequeño ({len(X_tr)}) — saltando")
            continue

        clf = LGBMClassifier(**LGBM_PARAMS)
        clf.fit(X_tr, y_tr, feature_name=FEATURE_NAMES)

        raw_probs = clf.predict_proba(X_val)[:, 1]
        auc_raw   = roc_auc_score(y_val, raw_probs)
        a, b      = fit_platt_calibration(raw_probs, y_val)
        cal_probs = apply_platt(raw_probs, a, b)
        auc_cal   = roc_auc_score(y_val, cal_probs)
        brier     = brier_score_loss(y_val, cal_probs)

        logger.info(
            f"  Fold {fold}: n_train={len(X_tr)} n_val={len(X_val)} "
            f"AUC_raw={auc_raw:.4f} AUC_cal={auc_cal:.4f} Brier={brier:.4f}"
        )
        fold_results.append({
            "fold": fold, "n_train": len(X_tr), "n_val": len(X_val),
            "auc_raw": round(auc_raw, 4), "auc_cal": round(auc_cal, 4),
            "brier": round(brier, 4), "platt_a": round(a, 4), "platt_b": round(b, 4),
        })

    return {"folds": fold_results}


# =============================================================================
# 5. ENTRENAMIENTO FINAL
# =============================================================================

def train_final_model(
    X: np.ndarray, y: np.ndarray
) -> Tuple[lgb.Booster, float, float, float, float]:
    clf = LGBMClassifier(**LGBM_PARAMS)
    clf.fit(X, y, feature_name=FEATURE_NAMES)

    raw_probs = clf.predict_proba(X)[:, 1]
    auc_val   = roc_auc_score(y, raw_probs)
    a, b      = fit_platt_calibration(raw_probs, y)
    cal_probs = apply_platt(raw_probs, a, b)
    brier_val = brier_score_loss(y, cal_probs)

    logger.info(f"Modelo final: AUC={auc_val:.4f} Brier={brier_val:.4f}")
    booster = clf.booster_
    return booster, auc_val, brier_val, a, b


# =============================================================================
# 6. SHAP ANALYSIS
# =============================================================================

def run_shap_analysis(booster: lgb.Booster, X: np.ndarray, output_dir: Path):
    if not SHAP_AVAILABLE:
        logger.info("SHAP no disponible — saltando análisis")
        return
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        explainer   = shap.TreeExplainer(booster)
        shap_values = explainer.shap_values(X)
        vals        = shap_values[1] if isinstance(shap_values, list) else shap_values

        importance = {
            FEATURE_NAMES[i]: round(float(np.abs(vals[:, i]).mean()), 6)
            for i in range(len(FEATURE_NAMES))
        }
        importance_sorted = dict(sorted(importance.items(), key=lambda x: x[1], reverse=True))
        with open(output_dir / "shap_importance.json", "w") as f:
            json.dump(importance_sorted, f, indent=2)

        plt.figure(figsize=(10, 7))
        shap.summary_plot(vals, X, feature_names=FEATURE_NAMES,
                          plot_type="bar", show=False)
        plt.tight_layout()
        plt.savefig(output_dir / "shap_summary.png", dpi=150, bbox_inches="tight")
        plt.close()
        logger.info("✅ SHAP analysis guardado")

        logger.info("Top 5 features por importancia SHAP:")
        for feat, val in list(importance_sorted.items())[:5]:
            logger.info(f"  {feat:30s}: {val:.6f}")
    except Exception as exc:
        logger.warning(f"SHAP analysis falló: {exc}")


# =============================================================================
# 7. GUARDAR ARTEFACTOS
# =============================================================================

def save_artifacts(
    booster: lgb.Booster,
    platt_a: float, platt_b: float,
    auc_val: float, brier_val: float,
    cv_results: Dict, n_obs: int,
    outcome_col: str, output_dir: Path,
):
    booster_path = output_dir / "lgbm_booster.txt"
    booster.save_model(str(booster_path))
    logger.info(f"Booster guardado: {booster_path}")

    with open(output_dir / "platt_calibration.json", "w") as f:
        json.dump({"a": round(platt_a, 6), "b": round(platt_b, 6)}, f, indent=2)

    with open(output_dir / "feature_names.json", "w") as f:
        json.dump(FEATURE_NAMES, f, indent=2)

    with open(output_dir / "label_encoders.json", "w") as f:
        json.dump(LABEL_ENCODERS, f, indent=2)

    meta = {
        "version":             "2.0.0",
        "engine":              "lightgbm_discriminative",
        "trained_at":          datetime.utcnow().isoformat() + "Z",
        "outcome_col":         outcome_col,
        "target_definition":   "above_ticker_median_return",
        "n_obs":               n_obs,
        "n_features":          len(FEATURE_NAMES),
        "feature_set":         "v2_extended",
        "auc_val":             round(auc_val, 4),
        "brier_val":           round(brier_val, 4),
        "platt_a":             round(platt_a, 6),
        "platt_b":             round(platt_b, 6),
        "lgbm_params":         LGBM_PARAMS,
        "cv_results":          cv_results,
        "booster_path":        str(booster_path),
        "description": (
            "Clasificador discriminativo LightGBM v2. "
            "Feature set extendido (18 features): incluye ADX, EMA-55, momentum_20d. "
            "Target: retorno relativo a mediana por ticker (50/50). "
            "Calibración Platt en log-odds space. "
            "Sin sklearn en runtime."
        ),
    }
    with open(output_dir / "lgbm_signal_meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    logger.info(f"✅ Artefactos guardados en {output_dir}")
    logger.info(f"  AUC-ROC:    {auc_val:.4f}  {'✅ Bueno' if auc_val > 0.55 else '⚠️  Bajo'}")
    logger.info(f"  Brier:      {brier_val:.4f}  {'✅ Bueno' if brier_val < 0.23 else '⚠️  Alto'}")
    logger.info(f"  Platt:      a={platt_a:.4f}  b={platt_b:.4f}")
    logger.info(f"  Features:   {len(FEATURE_NAMES)} ({', '.join(FEATURE_NAMES[-5:])} ...nuevos)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Entrenar clasificador discriminativo LightGBM v2")
    parser.add_argument("--outcome",  default="d3", choices=["d1", "d3", "d5"],
                        help="Horizonte temporal del outcome (default: d3)")
    parser.add_argument("--no-shap",  action="store_true", help="Omitir análisis SHAP")
    parser.add_argument("--min-rows", type=int, default=50,
                        help="Mínimo de filas para entrenar (default: 50)")
    args = parser.parse_args()

    outcome_col = f"outcome_{args.outcome}"
    logger.info("=" * 60)
    logger.info("Entrenamiento Discriminativo LightGBM v2 (18 features)")
    logger.info(f"  Target:    {outcome_col} (mediana por ticker)")
    logger.info(f"  Features:  {len(FEATURE_NAMES)}")
    logger.info(f"  Output:    {MODEL_DIR}")
    logger.info("=" * 60)

    df = load_training_data(outcome_col)
    if len(df) < args.min_rows:
        logger.error(f"Solo {len(df)} filas — mínimo {args.min_rows}. Abortando.")
        sys.exit(1)

    X, y = build_feature_matrix(df)
    logger.info(f"Matriz de features: {X.shape}")

    logger.info("Walk-Forward Cross-Validation…")
    cv_results = walk_forward_cv(X, y)

    logger.info("Entrenamiento modelo final…")
    booster, auc_val, brier_val, platt_a, platt_b = train_final_model(X, y)

    if not args.no_shap:
        logger.info("Análisis SHAP…")
        run_shap_analysis(booster, X, MODEL_DIR)

    save_artifacts(booster, platt_a, platt_b, auc_val, brier_val,
                   cv_results, len(df), outcome_col, MODEL_DIR)

    logger.info("✅ Entrenamiento completado.")
    logger.info(f"   Ejecuta el bootstrap para usar el nuevo modelo.")


if __name__ == "__main__":
    main()
