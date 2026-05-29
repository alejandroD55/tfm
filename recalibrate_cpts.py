#!/usr/bin/env python3
"""
recalibrate_cpts.py — Camino A: Recalibración de CPTs de la Red Bayesiana
=========================================================================
Carga los signal_outcomes de PostgreSQL, aprende las tablas de probabilidad
condicionales (CPTs) del nodo MarketDirection usando pgmpy BayesianEstimator
con suavizado de Laplace, y exporta:

  1. learned_cpts.json         — CPTs listas para pegar en MODEL_CONFIG
  2. cpt_comparison_report.txt — comparativa valor a valor vs. CPTs originales

Uso:
    python recalibrate_cpts.py
    python recalibrate_cpts.py --min-rows 50   # bajar mínimo si hay pocos datos
    python recalibrate_cpts.py --outcome d5    # usar outcome_d5 en vez de d3
    python recalibrate_cpts.py --apply         # sobrescribe lambda_bayesian.py
                                               # y bootstrap_365_days.py

Prerrequisitos:
    pip install pgmpy psycopg2-binary pandas numpy python-dotenv
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import warnings
# Silenciar FutureWarnings de pgmpy antes de importar el módulo
warnings.filterwarnings("ignore", category=FutureWarning, module="pgmpy")

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# pgmpy — solo se necesita para el estimador; el resto es numpy puro
try:
    # pgmpy ≥ 0.1.25 renombró BayesianNetwork → DiscreteBayesianNetwork
    try:
        from pgmpy.models import DiscreteBayesianNetwork as BayesianNetwork
    except ImportError:
        from pgmpy.models import BayesianNetwork  # versiones antiguas
    from pgmpy.estimators import BayesianEstimator  # FutureWarning esperado — sigue funcional
    from pgmpy.factors.discrete import TabularCPD
except ImportError:
    print("ERROR: pgmpy no instalado. Ejecuta: pip install pgmpy")
    sys.exit(1)

# ── Configuración ─────────────────────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).parent
OUTPUT_DIR  = _REPO_ROOT                            # cpts y report en raíz del proyecto
CPTS_JSON   = OUTPUT_DIR / "learned_cpts.json"
REPORT_TXT  = OUTPUT_DIR / "cpt_comparison_report.txt"

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5433")),
    "user":     os.getenv("PG_USER",     "tfmadmin"),
    "password": os.getenv("PG_PASSWORD", "localpassword123"),
    "database": os.getenv("PG_DB",       "tfm"),
}

# ── CPTs originales (v1.2) para comparativa ──────────────────────────────────
# Misma estructura que MODEL_CONFIG en lambda_bayesian.py
# Orden de columnas: Sentiment(3) × RSI(3) × Trend(2) × Volatility(2)
# Variable más rápida = Volatility, más lenta = Sentiment

ORIGINAL_CPT_UP = [
    0.88, 0.78, 0.75, 0.82, 0.75, 0.70, 0.78, 0.65,
    0.92, 0.88, 0.60, 0.55,
    0.30, 0.25, 0.20, 0.25, 0.20, 0.15, 0.20, 0.15,
    0.50, 0.45, 0.10, 0.05,
    0.58, 0.52, 0.48, 0.53, 0.48, 0.42, 0.48, 0.42,
    0.78, 0.72, 0.38, 0.32,
]

# Mapping de estados tal como los guarda el bootstrap en signal_outcomes
SENTIMENT_STATES = ["bullish", "bearish", "neutral"]
RSI_STATES       = ["oversold", "neutral", "overbought"]
TREND_STATES     = ["uptrend", "downtrend"]
VOL_STATES       = ["low", "high"]

# Normalización: el bootstrap puede guardar nombres con mayúsculas o minúsculas
SENTIMENT_NORM = {
    "bullish": "bullish", "bearish": "bearish", "neutral": "neutral",
    "Bullish": "bullish", "Bearish": "bearish", "Neutral": "neutral",
    "BULLISH": "bullish", "BEARISH": "bearish", "NEUTRAL": "neutral",
    "positive": "bullish", "negative": "bearish",
}
RSI_NORM = {
    "oversold": "oversold", "neutral": "neutral", "overbought": "overbought",
    "Oversold": "oversold", "Neutral": "neutral", "Overbought": "overbought",
    "OVERSOLD": "oversold", "NEUTRAL": "neutral", "OVERBOUGHT": "overbought",
}
TREND_NORM = {
    "uptrend": "uptrend", "downtrend": "downtrend",
    "Uptrend": "uptrend", "Downtrend": "downtrend",
    "UPTREND": "uptrend", "DOWNTREND": "downtrend",
    "up": "uptrend", "down": "downtrend",
}
VOL_NORM = {
    "low": "low", "high": "high",
    "Low": "low", "High": "high",
    "LOW": "low", "HIGH": "high",
    "normal": "low",   # algunas versiones usan "normal" para baja vol
}


# =============================================================================
# 1. CARGA DE DATOS
# =============================================================================

def load_signal_outcomes(outcome_col: str = "outcome_d3") -> pd.DataFrame:
    """
    Carga signal_outcomes desde PostgreSQL y devuelve un DataFrame limpio.

    Columnas resultantes:
        sentiment_state, rsi_state, trend_state, volatility_state, outcome
    donde outcome ∈ {UP, DOWN}  (FLAT se descarta).
    """
    logger.info(f"Conectando a PostgreSQL {DB_CONFIG['host']}:{DB_CONFIG['port']}…")
    conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10, sslmode="disable")

    query = f"""
        SELECT
            sentiment_state,
            rsi_state,
            trend_state,
            volatility_state,
            {outcome_col}  AS outcome
        FROM signal_outcomes
        WHERE {outcome_col} IS NOT NULL
          AND sentiment_state  IS NOT NULL
          AND rsi_state        IS NOT NULL
          AND trend_state      IS NOT NULL
          AND volatility_state IS NOT NULL
        ORDER BY batch_date
    """
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    logger.info(f"Filas cargadas (antes de filtrar): {len(df)}")

    # Normalizar nombres de estado
    df["sentiment_state"]  = df["sentiment_state"].map(SENTIMENT_NORM)
    df["rsi_state"]        = df["rsi_state"].map(RSI_NORM)
    df["trend_state"]      = df["trend_state"].map(TREND_NORM)
    df["volatility_state"] = df["volatility_state"].map(VOL_NORM)

    # Descartar FLAT y filas con estados desconocidos
    df = df[df["outcome"].isin(["UP", "DOWN"])]
    df = df.dropna(subset=["sentiment_state", "rsi_state", "trend_state", "volatility_state"])

    logger.info(f"Filas válidas para entrenamiento: {len(df)}")
    logger.info(f"Distribución outcome: {df['outcome'].value_counts().to_dict()}")
    return df


# =============================================================================
# 2. APRENDIZAJE DE CPTs CON PGMPY
# =============================================================================

def learn_cpts(df: pd.DataFrame, equivalent_sample_size: int = 10) -> BayesianNetwork:
    """
    Aprende las CPTs del nodo MarketDirection usando BayesianEstimator con
    suavizado de Dirichlet (equivalent_sample_size actúa como prior de Laplace).

    equivalent_sample_size=10 → cada celda tiene un pseudoconteo de 10/K
    donde K = número de estados padre. Evita probabilidades 0 o 1 con pocos datos.
    """
    # Renombrar columnas al vocabulario de la BN
    df_pgmpy = df.rename(columns={
        "sentiment_state":  "Sentiment",
        "rsi_state":        "RSI",
        "trend_state":      "Trend",
        "volatility_state": "Volatility",
        "outcome":          "MarketDirection",
    }).copy()

    # pgmpy espera strings
    df_pgmpy["MarketDirection"] = df_pgmpy["MarketDirection"].map(
        {"UP": "up", "DOWN": "down"}
    )

    # Definir la estructura (idéntica a la BN actual)
    model = BayesianNetwork([
        ("Sentiment",  "MarketDirection"),
        ("RSI",        "MarketDirection"),
        ("Trend",      "MarketDirection"),
        ("Volatility", "MarketDirection"),
    ])

    # Fijar los estados posibles para cada nodo
    state_names = {
        "Sentiment":       SENTIMENT_STATES,
        "RSI":             RSI_STATES,
        "Trend":           TREND_STATES,
        "Volatility":      VOL_STATES,
        "MarketDirection": ["down", "up"],
    }

    # ── Calcular combinaciones de padres para MarketDirection ──────────────
    # Orden pgmpy: último padre varía más rápido en la CPT
    # Sentiment(3) × RSI(3) × Trend(2) × Volatility(2) = 36 combinaciones
    n_parent_combos = len(SENTIMENT_STATES) * len(RSI_STATES) * len(TREND_STATES) * len(VOL_STATES)  # 36

    # pseudo_counts: shape (n_states_child, n_parent_combos) — ESS uniforme por celda
    ess = equivalent_sample_size
    pc_md = np.full((2, n_parent_combos), ess, dtype=float)          # (2, 36)
    pc_s  = np.full((len(SENTIMENT_STATES), 1), 5, dtype=float)      # (3, 1)
    pc_r  = np.full((len(RSI_STATES),       1), 5, dtype=float)      # (3, 1)
    pc_t  = np.full((len(TREND_STATES),     1), 5, dtype=float)      # (2, 1)
    pc_v  = np.full((len(VOL_STATES),       1), 5, dtype=float)      # (2, 1)

    logger.info(f"Aprendiendo CPTs (equivalent_sample_size={ess})…")
    estimator = BayesianEstimator(model, df_pgmpy, state_names=state_names)

    # Ajustar CPT de MarketDirection
    cpd_md = estimator.estimate_cpd(
        node="MarketDirection",
        prior_type="dirichlet",
        pseudo_counts=pc_md,
    )

    # Ajustar CPTs de los nodos raíz (priors empíricos del dataset)
    cpd_s = estimator.estimate_cpd("Sentiment",  prior_type="dirichlet", pseudo_counts=pc_s)
    cpd_r = estimator.estimate_cpd("RSI",        prior_type="dirichlet", pseudo_counts=pc_r)
    cpd_t = estimator.estimate_cpd("Trend",      prior_type="dirichlet", pseudo_counts=pc_t)
    cpd_v = estimator.estimate_cpd("Volatility", prior_type="dirichlet", pseudo_counts=pc_v)

    model.add_cpds(cpd_s, cpd_r, cpd_t, cpd_v, cpd_md)

    if not model.check_model():
        raise ValueError("El modelo aprendido no es válido. Revisa los datos.")

    logger.info("✅ CPTs aprendidas y modelo validado.")
    return model


# =============================================================================
# 3. EXTRACCIÓN Y EXPORTACIÓN
# =============================================================================

def extract_cpt_arrays(model: BayesianNetwork) -> Tuple[List[float], List[float]]:
    """
    Extrae los vectores values_P_down y values_P_up del CPD de MarketDirection,
    en el mismo orden que espera lambda_bayesian.py (Volatility fastest).

    pgmpy puede almacenar cpd.values con shape N-dimensional (2, 3, 3, 2, 2)
    o como matriz 2D (2, 36) según la versión. Normalizamos con reshape.
    """
    cpd = model.get_cpds("MarketDirection")
    flat = cpd.values.reshape(2, -1)   # siempre (2, 36)
    values_p_down = [round(float(v), 4) for v in flat[0]]
    values_p_up   = [round(float(v), 4) for v in flat[1]]
    return values_p_down, values_p_up


def extract_prior_arrays(model: BayesianNetwork) -> Dict[str, Dict[str, float]]:
    """Extrae los priors empíricos de cada nodo raíz."""
    priors = {}
    for node, states in [
        ("Sentiment", SENTIMENT_STATES),
        ("RSI",       RSI_STATES),
        ("Trend",     TREND_STATES),
        ("Volatility", VOL_STATES),
    ]:
        cpd = model.get_cpds(node)
        flat = cpd.values.flatten()    # (n_states,) independientemente de la versión
        priors[node] = {
            state: round(float(flat[i]), 4)
            for i, state in enumerate(states)
        }
    return priors


def save_learned_cpts(
    values_p_down: List[float],
    values_p_up:   List[float],
    priors:        Dict,
    df_stats:      Dict,
    outcome_col:   str,
) -> None:
    """Guarda learned_cpts.json listo para pegar en MODEL_CONFIG."""
    output = {
        "generated_at":  datetime.now().isoformat(),
        "outcome_used":  outcome_col,
        "training_rows": df_stats["n_rows"],
        "up_rate":       df_stats["up_rate"],
        "version":       "learned_v1",
        "description":   (
            f"CPTs aprendidas con pgmpy BayesianEstimator desde {df_stats['n_rows']} "
            f"observaciones de signal_outcomes. Suavizado Dirichlet ESS=10."
        ),
        # ── Drop-in replacement para MODEL_CONFIG ──────────────────────
        "cpt_market_direction": {
            "variable":       "MarketDirection",
            "states":         ["down", "up"],
            "evidence_order": ["Sentiment", "RSI", "Trend", "Volatility"],
            "values_P_down":  values_p_down,
            "values_P_up":    values_p_up,
        },
        "priors": {
            "Sentiment": {
                **priors["Sentiment"],
                "rationale": "Empírico desde signal_outcomes",
            },
            "RSI": {
                **priors["RSI"],
                "rationale": "Empírico desde signal_outcomes",
            },
            "Trend": {
                **priors["Trend"],
                "rationale": "Empírico desde signal_outcomes",
            },
            "Volatility": {
                **priors["Volatility"],
                "rationale": "Empírico desde signal_outcomes",
            },
        },
    }

    with open(CPTS_JSON, "w") as f:
        json.dump(output, f, indent=2)
    logger.info(f"✅ CPTs guardadas en: {CPTS_JSON}")


# =============================================================================
# 4. REPORTE COMPARATIVO
# =============================================================================

def generate_comparison_report(
    values_p_up_original: List[float],
    values_p_up_learned:  List[float],
    df:                   pd.DataFrame,
) -> None:
    """
    Genera un informe legible que compara las CPTs aprendidas con las originales
    para cada combinación de evidencia.
    """
    lines = [
        "=" * 78,
        "REPORTE DE RECALIBRACIÓN DE CPTs — RED BAYESIANA TFM",
        f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Observaciones de entrenamiento: {len(df)}",
        f"Tasa de UP (market direction): {(df['outcome'] == 'UP').mean():.2%}",
        "=" * 78,
        "",
        "DISTRIBUCIÓN DE ESTADOS DE EVIDENCIA:",
        f"  Sentiment:  {df['sentiment_state'].value_counts().to_dict()}",
        f"  RSI:        {df['rsi_state'].value_counts().to_dict()}",
        f"  Trend:      {df['trend_state'].value_counts().to_dict()}",
        f"  Volatility: {df['volatility_state'].value_counts().to_dict()}",
        "",
        "-" * 78,
        f"{'Combinación de evidencia':<50} {'Orig':>6} {'Aprend':>7} {'Δ':>7}  {'Zona'}",
        "-" * 78,
    ]

    # Reconstruir el orden de columnas del CPD
    # Orden: Sentiment(outer) → RSI → Trend → Volatility(inner, fastest)
    col_idx = 0
    for sent in SENTIMENT_STATES:
        for rsi in RSI_STATES:
            for trend in TREND_STATES:
                for vol in VOL_STATES:
                    orig    = values_p_up_original[col_idx]
                    learned = values_p_up_learned[col_idx]
                    delta   = learned - orig

                    if learned >= 0.52:
                        zona = "BUY ↑"
                    elif learned <= 0.28:
                        zona = "SELL ↓"
                    else:
                        zona = "HOLD –"

                    arrow = "↑" if delta > 0.03 else ("↓" if delta < -0.03 else " ")
                    combo = f"{sent[:4]}/{rsi[:4]}/{trend[:2]}/{vol[:4]}"
                    lines.append(
                        f"  {combo:<48} {orig:>6.3f} {learned:>7.3f} "
                        f"{delta:>+7.3f}{arrow}  {zona}"
                    )
                    col_idx += 1

    lines += [
        "-" * 78,
        "",
        "RESUMEN DE CAMBIOS:",
    ]

    diffs = [l - o for l, o in zip(values_p_up_learned, values_p_up_original)]
    n_to_buy  = sum(1 for v in values_p_up_learned if v >= 0.52)
    n_to_sell = sum(1 for v in values_p_up_learned if v <= 0.28)
    n_to_hold = sum(1 for v in values_p_up_learned if 0.28 < v < 0.52)
    n_orig_buy  = sum(1 for v in values_p_up_original if v >= 0.52)
    n_orig_hold = sum(1 for v in values_p_up_original if 0.28 < v < 0.52)

    lines += [
        f"  Celdas en zona BUY  (≥0.52): {n_orig_buy:>2} → {n_to_buy:>2}",
        f"  Celdas en zona HOLD:         {n_orig_hold:>2} → {n_to_hold:>2}",
        f"  Celdas en zona SELL (≤0.28): {n_to_sell:>2}",
        f"  Delta medio: {np.mean(diffs):+.4f}",
        f"  Delta máx:   {max(diffs):+.4f}  |  Delta mín: {min(diffs):+.4f}",
        "",
        "INTERPRETACIÓN:",
    ]

    if n_to_buy > n_orig_buy:
        lines.append(
            f"  ✅ Las CPTs aprendidas tienen más celdas BUY ({n_to_buy} vs {n_orig_buy}). "
            "El sistema generará más señales de entrada."
        )
    if n_to_buy == 0:
        lines.append(
            "  ⚠️  Ninguna celda alcanza la zona BUY. Considera bajar el umbral "
            "BUY de 0.52 a 0.48 si el dataset es mayoritariamente alcista."
        )
    if abs(np.mean(diffs)) < 0.02:
        lines.append(
            "  ⚠️  El cambio medio es muy pequeño (<2%). Posiblemente insuficientes datos. "
            "Se recomienda ejecutar de nuevo con más observaciones."
        )
    else:
        lines.append(
            "  ✅ Las CPTs aprendidas difieren significativamente de las originales."
        )

    lines += [
        "",
        "PRÓXIMOS PASOS:",
        "  1. Revisa el reporte y verifica que los cambios tienen sentido económico.",
        "  2. Copia el bloque 'cpt_market_direction' de learned_cpts.json a MODEL_CONFIG",
        "     en lambda_bayesian.py y bootstrap_365_days.py.",
        "  3. O usa: python recalibrate_cpts.py --apply  para actualizar automáticamente.",
        "  4. Re-ejecuta el bootstrap en un subconjunto de fechas para verificar que",
        "     ahora se generan señales BUY/SELL.",
        "=" * 78,
    ]

    report_text = "\n".join(lines)
    print("\n" + report_text)

    with open(REPORT_TXT, "w") as f:
        f.write(report_text)
    logger.info(f"✅ Reporte guardado en: {REPORT_TXT}")


# =============================================================================
# 5. APLICACIÓN AUTOMÁTICA A LOS ARCHIVOS FUENTE
# =============================================================================

def _replace_cpt_in_file(filepath: Path, new_p_down: List[float], new_p_up: List[float]) -> bool:
    """
    Sustituye las listas values_P_down y values_P_up en un archivo Python.
    Busca el patrón exacto de líneas de 8+4+8+4+8+4 valores que usan los archivos actuales.
    """
    try:
        text = filepath.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning(f"Archivo no encontrado: {filepath}")
        return False

    # Formatear las nuevas listas con el mismo estilo visual (grupos de 8, 4, 8, 4, 8, 4)
    def fmt_list(vals: List[float]) -> str:
        groups = [
            vals[0:8], vals[8:12], vals[12:20],
            vals[20:24], vals[24:32], vals[32:36],
        ]
        inner = ",\n            ".join(
            ", ".join(f"{v}" for v in g) for g in groups
        )
        return f"[\n            {inner},\n        ]"

    import re

    # Reemplazar values_P_down
    new_down_str = fmt_list(new_p_down)
    text = re.sub(
        r'"values_P_down":\s*\[.*?\]',
        f'"values_P_down": {new_down_str}',
        text, flags=re.DOTALL
    )

    # Reemplazar values_P_up
    new_up_str = fmt_list(new_p_up)
    text = re.sub(
        r'"values_P_up":\s*\[.*?\]',
        f'"values_P_up": {new_up_str}',
        text, flags=re.DOTALL
    )

    filepath.write_text(text, encoding="utf-8")
    logger.info(f"  ✅ {filepath.name} actualizado.")
    return True


def apply_cpts_to_sources(values_p_down: List[float], values_p_up: List[float]) -> None:
    """Actualiza lambda_bayesian.py y bootstrap_365_days.py con las CPTs aprendidas."""
    targets = [
        _REPO_ROOT / "lambda_bayesian" / "lambda_bayesian.py",
        _REPO_ROOT / "bootstrap_365_days.py",
    ]
    logger.info("Aplicando CPTs aprendidas a archivos fuente…")
    for target in targets:
        _replace_cpt_in_file(target, values_p_down, values_p_up)
    logger.info("✅ Archivos actualizados. Verifica los cambios con git diff.")


# =============================================================================
# 6. MAIN
# =============================================================================

def get_args():
    parser = argparse.ArgumentParser(
        description="Recalibra las CPTs de la Red Bayesiana desde signal_outcomes"
    )
    parser.add_argument(
        "--outcome", choices=["d1", "d3", "d5"], default="d3",
        help="Horizonte de outcome a usar como target (default: d3 = 3 días)"
    )
    parser.add_argument(
        "--min-rows", type=int, default=50,
        help="Mínimo de filas válidas requeridas para proceder (default: 50)"
    )
    parser.add_argument(
        "--ess", type=int, default=10,
        help="Equivalent Sample Size para suavizado Dirichlet (default: 10)"
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Aplica automáticamente las CPTs a lambda_bayesian.py y bootstrap_365_days.py"
    )
    return parser.parse_args()


def main():
    args = get_args()
    outcome_col = f"outcome_{args.outcome}"

    logger.info("=" * 60)
    logger.info("RECALIBRACIÓN DE CPTs — TFM Bayesian Signal Engine")
    logger.info(f"  Outcome target: {outcome_col}")
    logger.info(f"  ESS (suavizado): {args.ess}")
    logger.info("=" * 60)

    # 1. Cargar datos
    df = load_signal_outcomes(outcome_col=outcome_col)

    if len(df) < args.min_rows:
        logger.error(
            f"Solo {len(df)} filas válidas — mínimo requerido: {args.min_rows}. "
            "Ejecuta más días de bootstrap antes de recalibrar."
        )
        sys.exit(1)

    # 2. Aprender CPTs
    model = learn_cpts(df, equivalent_sample_size=args.ess)

    # 3. Extraer arrays
    values_p_down, values_p_up = extract_cpt_arrays(model)
    priors = extract_prior_arrays(model)

    # 4. Guardar JSON
    df_stats = {
        "n_rows":  len(df),
        "up_rate": round((df["outcome"] == "UP").mean(), 4),
    }
    save_learned_cpts(values_p_down, values_p_up, priors, df_stats, outcome_col)

    # 5. Reporte comparativo
    generate_comparison_report(ORIGINAL_CPT_UP, values_p_up, df)

    # 6. Aplicar si se pidió
    if args.apply:
        confirm = input("\n⚠️  ¿Aplicar CPTs a lambda_bayesian.py y bootstrap_365_days.py? [s/N]: ")
        if confirm.strip().lower() in ("s", "si", "sí", "y", "yes"):
            apply_cpts_to_sources(values_p_down, values_p_up)
        else:
            logger.info("Cancelado. Puedes aplicar manualmente desde learned_cpts.json.")

    logger.info("\n✅ RECALIBRACIÓN COMPLETADA")
    logger.info(f"   Archivos generados:")
    logger.info(f"     {CPTS_JSON}")
    logger.info(f"     {REPORT_TXT}")
    logger.info("   Próximo paso: revisar el reporte y ejecutar --apply si los cambios son correctos.")


if __name__ == "__main__":
    main()
