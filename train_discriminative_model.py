#!/usr/bin/env python3
"""
train_discriminative_model.py — Camino B: Clasificador Discriminativo LightGBM
================================================================================
Entrena un clasificador LightGBM como sustituto de la Red Bayesiana para predecir
la dirección del mercado (UP/DOWN) con validación temporal walk-forward.

Genera en /tfm/models/:
  lgbm_booster.txt          — booster nativo LightGBM (sin sklearn en runtime)
  lgbm_signal_meta.json     — AUC, Brier Score, fecha, n_obs, features
  platt_calibration.json    — parámetros de calibración Platt (a, b)
  feature_names.json        — lista ordenada de features
  label_encoders.json       — mapas de encoding categórico
  shap_summary.png          — SHAP feature importance (si --shap activado)

Uso:
    python train_discriminative_model.py
    python train_discriminative_model.py --outcome d5 --no-shap
    python train_discriminative_model.py --min-rows 80 --ess-equiv 5

Prerrequisitos:
    pip install lightgbm scikit-learn joblib shap scipy psycopg2-binary pandas python-dotenv
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

# ── Dependencias ML ────────────────────────────────────────────────────────────
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
    print("AVISO: scipy no instalado — calibración Platt usará regresión simple. pip install scipy")

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False
    print("AVISO: shap no instalado — plots SHAP omitidos.  pip install shap")

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()

# ── Rutas ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
MODEL_DIR  = SCRIPT_DIR / "models"
MODEL_DIR.mkdir(exist_ok=True)

# ── Base de datos ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5433")),
    "user":     os.getenv("DB_USER",     "tfmadmin"),
    "password": os.getenv("DB_PASSWORD", "localpassword123"),
    "database": os.getenv("DB_NAME",     "tfm"),
}

# ── Encodings categóricos (fijos, usados en entrenamiento e inferencia) ────────
LABEL_ENCODERS: Dict[str, Dict[str, int]] = {
    "sentiment_state":  {"bullish": 0, "neutral": 1, "bearish": 2},
    "rsi_state":        {"oversold": 0, "neutral": 1, "overbought": 2},
    "trend_state":      {"uptrend": 0, "downtrend": 1},
    "volatility_state": {"low": 0, "high": 1},
    "risk_regime": {
        "RISK_ON_STRONG": 3, "RISK_ON": 2, "NEUTRAL": 1,
        "RISK_OFF_MILD": -1, "RISK_OFF": -2, "FEAR": -3,
    },
}

# Features en el orden exacto de entrenamiento
FEATURE_NAMES = [
    # ── Discretizadas (codificadas como enteros ordinales) ──────────────────
    "sentiment_state",
    "rsi_state",
    "trend_state",
    "volatility_state",
    # ── Continuas disponibles directamente en signal_outcomes ──────────────
    "prob_up",           # probabilidad BN original — feature, no target
    "macro_adjustment",  # ajuste macro del día
    "risk_regime",       # régimen de riesgo codificado ordinalmente
    # ── Momentum de señal (calculadas desde la serie temporal) ────────────
    "signal_streak",     # días consecutivos con la misma señal
    "prob_up_delta",     # cambio de prob_up respecto al día anterior
    "prob_up_5d_mean",   # media móvil 5 días de prob_up
    # ── Contexto de volatilidad (JOIN con position_state) ──────────────────
    "vol_20d",           # volatilidad realizada anualizada 20 días
    "vol_ratio",         # vol_5d / vol_20d — aceleración de vol
    "sentiment_dispersion",  # varianza ponderada de scores de sentimiento
]

# Hiperparámetros conservadores para ~600 observaciones
LGBM_PARAMS = {
    "objective":          "binary",
    "metric":             "auc",
    "num_leaves":         8,
    "max_depth":          3,
    "learning_rate":      0.05,
    "n_estimators":       300,
    "min_child_samples":  15,
    "subsample":          0.8,
    "colsample_bytree":   0.8,
    "reg_alpha":          0.1,
    "reg_lambda":         1.0,
    "class_weight":       "balanced",
    "random_state":       42,
    "verbose":            -1,
}

MIN_ROWS = 100  # mínimo de observaciones para entrenar


# =============================================================================
# 1. CARGA Y LIMPIEZA DE DATOS
# =============================================================================

def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"], port=DB_CONFIG["port"],
        user=DB_CONFIG["user"], password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        sslmode="disable", connect_timeout=10,
    )


def load_training_data(outcome_col: str = "outcome_d3") -> pd.DataFrame:
    """
    Carga signal_outcomes con LEFT JOIN a position_state para features de volatilidad.
    Incluye price_d0 y price_d3 para calcular retorno real 3d.
    Retorna DataFrame con columnas necesarias para entrenamiento.
    """
    logger.info(f"Conectando a PostgreSQL {DB_CONFIG['host']}:{DB_CONFIG['port']}…")
    conn = get_db_connection()

    # Detectar el horizonte numérico (d1, d3, d5) a partir del nombre de la columna
    _horizon = outcome_col.replace("outcome_d", "")   # "3", "5", "1", …
    price_col = f"price_d{_horizon}"

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
            COALESCE(ps.vol_20d,    0.20) AS vol_20d,
            COALESCE(ps.vol_ratio,  1.00) AS vol_ratio,
            COALESCE(ps.sentiment_dispersion, 0.0) AS sentiment_dispersion
        FROM signal_outcomes so
        LEFT JOIN position_state ps
            ON so.batch_date = ps.batch_date AND so.ticker = ps.ticker
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

    # Calcular retorno numérico real (no categórico)
    df["return_d"] = (df["price_dn"] - df["price_d0"]) / df["price_d0"]

    # Normalizar estados categóricos
    for col in ["sentiment_state", "rsi_state", "trend_state", "volatility_state"]:
        df[col] = df[col].str.lower().str.strip()
    df["risk_regime"] = df["risk_regime"].str.upper().str.strip().fillna("NEUTRAL")

    # Filtrar filas con estados no reconocidos
    valid_sentiment = set(LABEL_ENCODERS["sentiment_state"].keys())
    valid_rsi       = set(LABEL_ENCODERS["rsi_state"].keys())
    valid_trend     = set(LABEL_ENCODERS["trend_state"].keys())
    valid_vol       = set(LABEL_ENCODERS["volatility_state"].keys())

    mask = (
        df["sentiment_state"].isin(valid_sentiment) &
        df["rsi_state"].isin(valid_rsi) &
        df["trend_state"].isin(valid_trend) &
        df["volatility_state"].isin(valid_vol)
    )
    df = df[mask].copy()

    logger.info(f"Filas válidas tras filtrado: {len(df)}")
    logger.info(f"Distribución outcome (etiqueta original): {df['outcome'].value_counts().to_dict()}")
    logger.info(
        f"Retorno {_horizon}d — mediana={df['return_d'].median()*100:.2f}%  "
        f"media={df['return_d'].mean()*100:.2f}%  "
        f"std={df['return_d'].std()*100:.2f}%"
    )
    return df


# =============================================================================
# 2. FEATURE ENGINEERING
# =============================================================================

def encode_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """Codificación ordinal de variables categóricas."""
    df = df.copy()
    for col, mapping in LABEL_ENCODERS.items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna(1).astype(float)
    return df


def compute_momentum_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula features de momentum por ticker en orden temporal.
    - signal_streak: días consecutivos con la misma señal
    - prob_up_delta: cambio de prob_up respecto al día anterior
    - prob_up_5d_mean: media móvil 5 días de prob_up
    """
    df = df.sort_values(["ticker", "batch_date"]).copy()

    # prob_up_delta y prob_up_5d_mean
    df["prob_up_delta"]  = df.groupby("ticker")["prob_up"].diff().fillna(0.0)
    df["prob_up_5d_mean"] = (
        df.groupby("ticker")["prob_up"]
        .transform(lambda s: s.rolling(5, min_periods=1).mean())
    )

    # signal_streak: cuántos días seguidos lleva la misma señal
    streaks = []
    for ticker, group in df.groupby("ticker"):
        streak = 1
        prev_signal = None
        for sig in group["signal"]:
            if sig == prev_signal:
                streak += 1
            else:
                streak = 1
            streaks.append(streak)
            prev_signal = sig
    df["signal_streak"] = streaks

    return df


def build_feature_matrix(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """
    Construye la matriz X (n_obs × n_features) y el vector y (0=below-median, 1=above-median).

    Target: mediana por ticker del retorno real (return_d).
    Esto garantiza exactamente 50 % de positivos y 50 % de negativos en el dataset
    de entrenamiento, eliminando el sesgo de mercados alcistas donde UP ≫ DOWN.
    La señal semántica es: "¿Supera este activo la rentabilidad mediana de los últimos
    días para su propio ticker?" — más informativo que el simple umbral ±0.5 %.
    """
    df = encode_categorical(df)
    df = compute_momentum_features(df)

    # Target: retorno relativo a la mediana por ticker (split 50/50 garantizado)
    if "return_d" in df.columns:
        df["target"] = df.groupby("ticker")["return_d"].transform(
            lambda x: (x > x.median()).astype(int)
        )
        y = df["target"].values.astype(int)
        pos_frac = y.mean()
        logger.info(
            f"Target (mediana por ticker): {y.sum()} positivos / {len(y)} total "
            f"({pos_frac*100:.1f}% above-median)"
        )
    else:
        # Fallback si no hay columna return_d (compatibilidad con datos legacy)
        logger.warning("Columna 'return_d' no encontrada — usando etiqueta UP/DOWN original")
        y = (df["outcome"] == "UP").astype(int).values

    # Rellenar NaN en features de volatilidad con medianas
    for col in ["vol_20d", "vol_ratio", "sentiment_dispersion"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    X = df[FEATURE_NAMES].values.astype(float)
    return X, y


# =============================================================================
# 3. CALIBRACIÓN PLATT MANUAL (sin sklearn en runtime)
# =============================================================================

def _sigmoid(x: np.ndarray) -> np.ndarray:
    """Sigmoid numéricamente estable."""
    return np.where(x >= 0,
                    1.0 / (1.0 + np.exp(-x)),
                    np.exp(x) / (1.0 + np.exp(x)))


def _logit(p: np.ndarray) -> np.ndarray:
    """Logit (log-odds): inversa de sigmoid. Requiere p ∈ (0,1)."""
    p = np.clip(p, 1e-7, 1.0 - 1e-7)
    return np.log(p / (1.0 - p))


def fit_platt_calibration(raw_probs: np.ndarray, y_true: np.ndarray) -> Tuple[float, float]:
    """
    Ajusta la calibración de Platt sobre log-odds:
        prob_cal = sigmoid(a * logit(raw_prob) + b)

    Al operar en espacio log-odds el optimizador puede expandir o comprimir
    el rango de predicciones simétricamente alrededor de 0.5, lo que permite
    obtener tanto BUY como SELL cuando las probabilidades crudas no alcanzan
    los umbrales en escala de probabilidad directa.

    Devuelve (a, b).
    """
    logit_raw = _logit(raw_probs)   # transforma [0,1] → (-∞,+∞)

    def neg_log_likelihood(params):
        a, b = params
        p = _sigmoid(a * logit_raw + b)
        p = np.clip(p, 1e-7, 1.0 - 1e-7)
        return -np.mean(y_true * np.log(p) + (1.0 - y_true) * np.log(1.0 - p))

    if SCIPY_AVAILABLE:
        res = minimize(neg_log_likelihood, [1.0, 0.0], method="L-BFGS-B",
                       bounds=[(0.01, 10.0), (-5.0, 5.0)])
        a, b = float(res.x[0]), float(res.x[1])
    else:
        a, b = 1.0, 0.0   # identidad en log-odds ≡ sin calibración

    logger.info(f"Platt calibration (logit-space): a={a:.4f}, b={b:.4f}")
    return a, b


def apply_platt(raw_probs: np.ndarray, a: float, b: float) -> np.ndarray:
    """Aplica la calibración Platt en espacio log-odds (vectorizada)."""
    return _sigmoid(a * _logit(raw_probs) + b)


# =============================================================================
# 4. WALK-FORWARD CROSS-VALIDATION
# =============================================================================

def walk_forward_cv(X: np.ndarray, y: np.ndarray, n_splits: int = 3) -> Dict:
    """
    Validación temporal walk-forward con TimeSeriesSplit.
    En cada fold: entrena en pasado, valida en futuro.
    Reporta AUC-ROC y Brier Score por fold.
    """
    tscv = TimeSeriesSplit(n_splits=n_splits)
    fold_results = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X), start=1):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]

        if len(np.unique(y_tr)) < 2 or len(np.unique(y_val)) < 2:
            logger.warning(f"  Fold {fold}: clase única en train o val — saltando")
            continue
        if len(X_tr) < 30:
            logger.warning(f"  Fold {fold}: train demasiado pequeño ({len(X_tr)}) — saltando")
            continue

        clf = LGBMClassifier(**LGBM_PARAMS)
        clf.fit(X_tr, y_tr,
                eval_set=[(X_val, y_val)],
                callbacks=[lgb.early_stopping(50, verbose=False),
                           lgb.log_evaluation(period=-1)])

        raw_probs = clf.predict_proba(X_val)[:, 1]
        a, b = fit_platt_calibration(raw_probs, y_val)
        cal_probs = apply_platt(raw_probs, a, b)

        auc_raw = roc_auc_score(y_val, raw_probs)
        auc_cal = roc_auc_score(y_val, cal_probs)
        brier   = brier_score_loss(y_val, cal_probs)

        fold_results.append({
            "fold":       fold,
            "n_train":    len(X_tr),
            "n_val":      len(X_val),
            "auc_raw":    round(auc_raw, 4),
            "auc_cal":    round(auc_cal, 4),
            "brier":      round(brier, 4),
            "platt_a":    round(a, 4),
            "platt_b":    round(b, 4),
        })
        logger.info(
            f"  Fold {fold}: train={len(X_tr)} val={len(X_val)} "
            f"AUC_raw={auc_raw:.4f} AUC_cal={auc_cal:.4f} Brier={brier:.4f}"
        )

    return {"folds": fold_results}


# =============================================================================
# 5. ENTRENAMIENTO FINAL (todo el dataset)
# =============================================================================

def train_final_model(X: np.ndarray, y: np.ndarray) -> Tuple[lgb.Booster, float, float]:
    """
    Entrena el modelo final sobre todo el dataset.
    Usa los últimos 20% como conjunto de calibración Platt.
    Retorna (booster_nativo, platt_a, platt_b).
    """
    # Reservar últimos 20% para calibración (orden temporal)
    cal_size  = max(20, int(len(X) * 0.20))
    X_tr, X_cal = X[:-cal_size], X[-cal_size:]
    y_tr, y_cal = y[:-cal_size], y[-cal_size:]

    logger.info(f"Train final: {len(X_tr)} obs | Calibración Platt: {len(X_cal)} obs")

    clf = LGBMClassifier(**LGBM_PARAMS)
    clf.fit(X_tr, y_tr,
            eval_set=[(X_cal, y_cal)],
            callbacks=[lgb.early_stopping(50, verbose=False),
                       lgb.log_evaluation(period=-1)])

    # Calibración Platt
    raw_probs_cal = clf.predict_proba(X_cal)[:, 1]
    a, b = fit_platt_calibration(raw_probs_cal, y_cal)
    cal_probs = apply_platt(raw_probs_cal, a, b)

    auc_val   = roc_auc_score(y_cal, cal_probs)
    brier_val = brier_score_loss(y_cal, cal_probs)
    logger.info(f"Modelo final: AUC_cal={auc_val:.4f} Brier={brier_val:.4f}")

    # Devolver el booster nativo (sin sklearn) para inferencia en Lambda
    booster = clf.booster_
    return booster, a, b, auc_val, brier_val


# =============================================================================
# 6. SHAP ANALYSIS
# =============================================================================

def generate_shap_analysis(booster: lgb.Booster, X: np.ndarray, output_dir: Path):
    """Genera SHAP summary plot y guarda valores como JSON."""
    if not SHAP_AVAILABLE:
        logger.warning("shap no disponible — omitiendo análisis SHAP")
        return

    logger.info("Calculando SHAP values…")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        explainer   = shap.TreeExplainer(booster)
        shap_values = explainer.shap_values(X)

        # Para clasificación binaria, shap_values puede ser lista [neg, pos]
        sv = shap_values[1] if isinstance(shap_values, list) else shap_values

        # ── Summary plot ──────────────────────────────────────────────────────
        plt.figure(figsize=(10, 6))
        shap.summary_plot(sv, X, feature_names=FEATURE_NAMES,
                          show=False, plot_type="bar")
        plt.title("Feature Importance (SHAP) — Camino B LightGBM", fontsize=13)
        plt.tight_layout()
        plot_path = output_dir / "shap_summary.png"
        plt.savefig(plot_path, dpi=150, bbox_inches="tight")
        plt.close()
        logger.info(f"SHAP summary guardado: {plot_path}")

        # ── Feature importance como JSON ──────────────────────────────────────
        mean_abs_shap = np.abs(sv).mean(axis=0)
        importance_dict = {
            feat: round(float(val), 6)
            for feat, val in sorted(
                zip(FEATURE_NAMES, mean_abs_shap),
                key=lambda x: x[1], reverse=True
            )
        }
        with open(output_dir / "shap_importance.json", "w") as f:
            json.dump(importance_dict, f, indent=2)
        logger.info("SHAP importance guardado: shap_importance.json")

    except Exception as exc:
        logger.warning(f"SHAP analysis falló: {exc}")


# =============================================================================
# 7. GUARDAR ARTEFACTOS
# =============================================================================

def save_artifacts(
    booster:   lgb.Booster,
    platt_a:   float,
    platt_b:   float,
    auc_val:   float,
    brier_val: float,
    cv_results: Dict,
    n_obs:     int,
    outcome_col: str,
    output_dir: Path,
):
    """Guarda todos los artefactos necesarios para inferencia en production."""

    # 1. Booster nativo (sin sklearn — compatible con lambda inference)
    booster_path = output_dir / "lgbm_booster.txt"
    booster.save_model(str(booster_path))
    logger.info(f"Booster guardado: {booster_path}")

    # 2. Calibración Platt
    platt = {"a": round(platt_a, 6), "b": round(platt_b, 6)}
    with open(output_dir / "platt_calibration.json", "w") as f:
        json.dump(platt, f, indent=2)
    logger.info("Calibración Platt guardada: platt_calibration.json")

    # 3. Feature names
    with open(output_dir / "feature_names.json", "w") as f:
        json.dump(FEATURE_NAMES, f, indent=2)

    # 4. Label encoders
    with open(output_dir / "label_encoders.json", "w") as f:
        json.dump(LABEL_ENCODERS, f, indent=2)

    # 5. Metadata del modelo
    meta = {
        "version":      "1.0.0",
        "engine":       "lightgbm_discriminative",
        "trained_at":   datetime.utcnow().isoformat() + "Z",
        "outcome_col":  outcome_col,
        "n_obs":        n_obs,
        "n_features":   len(FEATURE_NAMES),
        "auc_val":      round(auc_val, 4),
        "brier_val":    round(brier_val, 4),
        "platt_a":      round(platt_a, 6),
        "platt_b":      round(platt_b, 6),
        "lgbm_params":  LGBM_PARAMS,
        "cv_results":   cv_results,
        "booster_path": str(booster_path),
        "target_definition": "above_ticker_median_return",
        "description": (
            "Clasificador discriminativo LightGBM entrenado sobre signal_outcomes. "
            "Target: retorno relativo a la mediana por ticker (split 50/50 garantizado). "
            "Sustituye la Red Bayesiana en infer_signal(). Calibración Platt manual "
            "(numpy puro, sin sklearn en runtime)."
        ),
    }
    with open(output_dir / "lgbm_signal_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    logger.info("Metadata guardada: lgbm_signal_meta.json")


# =============================================================================
# 8. REPORTE FINAL
# =============================================================================

def print_report(cv_results: Dict, auc_val: float, brier_val: float, n_obs: int):
    folds = cv_results.get("folds", [])
    sep = "=" * 70

    print(f"\n{sep}")
    print("REPORTE DE ENTRENAMIENTO — CAMINO B LightGBM Discriminativo")
    print(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Observaciones de entrenamiento: {n_obs}")
    print(sep)

    print("\nWALK-FORWARD CROSS-VALIDATION:")
    print(f"  {'Fold':<6} {'Train':>8} {'Val':>6} {'AUC_raw':>10} {'AUC_cal':>10} {'Brier':>8}")
    print("  " + "-" * 50)
    for f in folds:
        print(f"  {f['fold']:<6} {f['n_train']:>8} {f['n_val']:>6} "
              f"{f['auc_raw']:>10.4f} {f['auc_cal']:>10.4f} {f['brier']:>8.4f}")

    if folds:
        aucs = [f["auc_cal"] for f in folds]
        briers = [f["brier"] for f in folds]
        print(f"  {'Media':<6} {'':>8} {'':>6} {'':>10} {np.mean(aucs):>10.4f} {np.mean(briers):>8.4f}")

    print(f"\nMODELO FINAL (calibrado sobre últimos 20%):")
    print(f"  AUC-ROC:    {auc_val:.4f}  {'✅ Bueno' if auc_val > 0.55 else '⚠️  Bajo'}")
    print(f"  Brier Score:{brier_val:.4f}  {'✅ Bien calibrado' if brier_val < 0.24 else '⚠️  Calibración mejorable'}")

    print(f"\nARTEFACTOS EN /tfm/models/:")
    artifacts = [
        "lgbm_booster.txt    — booster nativo (inferencia Lambda sin sklearn)",
        "lgbm_signal_meta.json",
        "platt_calibration.json",
        "feature_names.json",
        "label_encoders.json",
        "shap_summary.png    — (si shap instalado)",
        "shap_importance.json — (si shap instalado)",
    ]
    for a in artifacts:
        print(f"  {a}")

    print(f"\nPRÓXIMOS PASOS:")
    print("  1. El modelo se carga automáticamente en bootstrap y lambda")
    print("     si /tfm/models/lgbm_booster.txt existe.")
    print("  2. Ejecuta: python bootstrap_365_days.py --start 2026-01-01 --end 2026-05-27")
    print("  3. Compara ciclos generados vs. bootstrap con BN original.")
    print(sep + "\n")


# =============================================================================
# 9. MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Entrena el clasificador discriminativo LightGBM (Camino B)"
    )
    parser.add_argument(
        "--outcome", choices=["d1", "d3", "d5"], default="d3",
        help="Columna de outcome a usar como target (default: d3)"
    )
    parser.add_argument(
        "--min-rows", type=int, default=MIN_ROWS,
        help=f"Mínimo de filas válidas para entrenar (default: {MIN_ROWS})"
    )
    parser.add_argument(
        "--no-shap", action="store_true",
        help="Omitir análisis SHAP (más rápido)"
    )
    parser.add_argument(
        "--cv-splits", type=int, default=3,
        help="Número de folds en walk-forward CV (default: 3)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    outcome_col = f"outcome_{args.outcome}"
    do_shap = SHAP_AVAILABLE and not args.no_shap

    logger.info("=" * 60)
    logger.info("ENTRENAMIENTO DISCRIMINATIVO — Camino B LightGBM")
    logger.info(f"  Target:       {outcome_col}")
    logger.info(f"  CV splits:    {args.cv_splits}")
    logger.info(f"  SHAP:         {'sí' if do_shap else 'no'}")
    logger.info("=" * 60)

    # ── 1. Cargar datos ────────────────────────────────────────────────────────
    df = load_training_data(outcome_col)
    if len(df) < args.min_rows:
        logger.error(
            f"Solo {len(df)} filas válidas — mínimo requerido: {args.min_rows}. "
            f"Ejecuta el bootstrap para acumular más signal_outcomes."
        )
        sys.exit(1)

    # ── 2. Construir matriz de features ────────────────────────────────────────
    logger.info("Construyendo features…")
    X, y = build_feature_matrix(df)
    logger.info(f"  Matriz: {X.shape[0]} obs × {X.shape[1]} features")
    logger.info(f"  UP: {y.sum()} ({y.mean()*100:.1f}%)  |  DOWN: {(1-y).sum()} ({(1-y).mean()*100:.1f}%)")

    # ── 3. Walk-forward CV ─────────────────────────────────────────────────────
    logger.info(f"\nWalk-Forward CV ({args.cv_splits} folds)…")
    cv_results = walk_forward_cv(X, y, n_splits=args.cv_splits)

    # ── 4. Entrenar modelo final ───────────────────────────────────────────────
    logger.info("\nEntrenando modelo final sobre dataset completo…")
    booster, platt_a, platt_b, auc_val, brier_val = train_final_model(X, y)

    # ── 5. Guardar artefactos ──────────────────────────────────────────────────
    save_artifacts(
        booster, platt_a, platt_b, auc_val, brier_val,
        cv_results, len(df), outcome_col, MODEL_DIR
    )

    # ── 6. SHAP analysis ───────────────────────────────────────────────────────
    if do_shap:
        generate_shap_analysis(booster, X, MODEL_DIR)

    # ── 7. Reporte final ───────────────────────────────────────────────────────
    print_report(cv_results, auc_val, brier_val, len(df))
    logger.info(f"✅ Entrenamiento completado. Artefactos en: {MODEL_DIR}")


if __name__ == "__main__":
    main()
