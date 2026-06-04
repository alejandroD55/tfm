#!/usr/bin/env python3
"""
recompute_exposure.py — Recalcula inferencia + exposición sin re-ejecutar el pipeline
======================================================================================

Carga las evidencias ya procesadas desde PostgreSQL y MongoDB, y reejecutas solo:
    1. Inferencia BN (+ LightGBM si disponible)
    2. Conviction scaling (Gap 1)
    3. Exposure management
    4. Actualiza signal_outcomes, position_state y reports en MongoDB

NO llama a ninguna API externa. NO ejecuta FinBERT. NO descarga OHLCV.
Tiempo estimado: ~2-5 min para 370 días × 5 tickers.

Uso:
    python recompute_exposure.py
    python recompute_exposure.py --start 2025-01-01 --end 2026-06-02
    python recompute_exposure.py --dry-run          # solo muestra, no escribe
    python recompute_exposure.py --tickers SPY,GLD  # solo esos tickers
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Importar lógica del pipeline ──────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bootstrap_365_days import (
    get_bn_model,
    run_bayesian_inference,
    prob_to_exposure,
    smooth_exposure,
    detect_market_regime,
    BUY_THRESHOLD,
    SELL_THRESHOLD,
    MODEL_CONFIG,
    DB_CONFIG,
    INITIAL_CAP,
)

# ── Motor discriminativo (opcional) ───────────────────────────────────────────
try:
    from discriminative_engine import disc_engine
    disc_engine.load()
    logger.info(f"Motor discriminativo: {'disponible' if disc_engine.available else 'no disponible'}")
except Exception:
    disc_engine = None

# ── MongoDB ────────────────────────────────────────────────────────────────────
try:
    from pymongo import MongoClient
    MONGODB_URI = os.getenv("MONGODB_URI", "")
    _mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=8000)
    _mongo_db = _mongo_client["tfm"]
    _mongo_db.list_collection_names()
    MONGO_OK = True
    logger.info("MongoDB: conectado")
except Exception as e:
    logger.warning(f"MongoDB no disponible: {e}")
    MONGO_OK = False
    _mongo_db = None


# =============================================================================
# 1. CARGA DE DATOS DESDE PostgreSQL
# =============================================================================

def get_pg_conn():
    return psycopg2.connect(**DB_CONFIG)


def _ti_columns(conn) -> set:
    """Devuelve el set de columnas que existen en technical_indicators."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'technical_indicators'
    """)
    cols = {r[0] for r in cur.fetchall()}
    cur.close()
    return cols


def load_existing_signals(
    start_date: str, end_date: str, tickers: Optional[List[str]] = None
) -> List[Dict]:
    """
    Carga de signal_outcomes las evidencias ya procesadas.
    Detecta dinámicamente qué columnas existen en technical_indicators
    para no fallar si las columnas nuevas (adx_14, ema_55_pct, etc.) aún no se han añadido.
    """
    conn = get_pg_conn()
    existing_cols = _ti_columns(conn)

    def ti_col(col: str) -> str:
        """Si la columna existe en technical_indicators, seleccionarla; si no, NULL."""
        if col in existing_cols:
            return f"ti.{col}"
        return f"NULL::float AS {col}"

    ticker_clause = ""
    params = [start_date, end_date]
    if tickers:
        ticker_clause = "AND so.ticker = ANY(%s)"
        params.append(tickers)

    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            so.batch_date,
            so.ticker,
            so.sentiment_state,
            so.rsi_state,
            so.trend_state,
            so.volatility_state,
            so.macro_adjustment,
            so.risk_regime,
            so.macro_sentiment,
            so.price_d0,
            so.prob_up           AS old_prob_up,
            so.signal            AS old_signal,
            ps.smoothed_exposure AS old_exposure,
            ps.market_regime     AS old_regime,
            {ti_col('rsi_14')},
            {ti_col('sma_20')},
            {ti_col('sma_50')},
            {ti_col('sma_200')},
            {ti_col('adx_14')},
            {ti_col('ema_55_pct')},
            {ti_col('momentum_20d')},
            {ti_col('momentum_5d')},
            {ti_col('close_price')}
        FROM signal_outcomes so
        LEFT JOIN position_state ps
            ON so.batch_date = ps.batch_date AND so.ticker = ps.ticker
        LEFT JOIN technical_indicators ti
            ON so.batch_date = ti.batch_date AND so.ticker = ti.ticker
        WHERE so.batch_date BETWEEN %s AND %s
        {ticker_clause}
        ORDER BY so.batch_date, so.ticker
    """, params)

    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()

    missing = [c for c in ['adx_14','ema_55_pct','momentum_20d','momentum_5d','sma_200']
               if c not in existing_cols]
    if missing:
        logger.warning(f"Columnas no encontradas en technical_indicators (se usará NULL): {missing}")
    logger.info(f"Cargados {len(rows)} registros de signal_outcomes")
    return rows


def load_vix_series(start_date: str, end_date: str) -> Dict[str, Optional[float]]:
    """Carga el VIX diario desde macro_context en MongoDB o desde position_state."""
    vix_by_date: Dict[str, Optional[float]] = {}

    if MONGO_OK and _mongo_db is not None:
        try:
            for doc in _mongo_db["macro_context"].find(
                {"batch_date": {"$gte": start_date, "$lte": end_date}},
                {"batch_date": 1, "vix": 1}
            ):
                vix_by_date[doc["batch_date"]] = doc.get("vix")
            logger.info(f"VIX cargado desde MongoDB: {len(vix_by_date)} días")
            return vix_by_date
        except Exception as e:
            logger.warning(f"VIX desde MongoDB falló: {e}")

    # Fallback: extraer de position_state (market_regime como proxy)
    logger.warning("VIX no disponible — usando regime del position_state como proxy")
    return vix_by_date


def load_contribution_analysis(
    date_str: str, ticker: str
) -> Dict:
    """Carga el contribution_analysis ya calculado desde MongoDB bayesian_reports."""
    if not MONGO_OK or _mongo_db is None:
        return {}
    try:
        doc = _mongo_db["bayesian_reports"].find_one(
            {"batch_date": date_str, "ticker": ticker},
            {"contribution_analysis": 1}
        )
        return (doc or {}).get("contribution_analysis", {})
    except Exception:
        return {}


# =============================================================================
# 2. RECALCULAR CONVICTION DESDE CONTRIBUTION ANALYSIS
# =============================================================================

def calc_conviction_from_effects(effects: Dict) -> Tuple[float, str]:
    """
    Extrae conviction_score y conviction_label desde los efectos ya guardados.
    Mismo cálculo que en _process_ticker_day.
    """
    deltas = [v.get("delta_prob_up", 0) for v in effects.values() if v.get("applicable")]
    if len(deltas) >= 2:
        pos   = sum(1 for d in deltas if d > 0.02)
        neg   = sum(1 for d in deltas if d < -0.02)
        score = round(max(pos, neg) / len(deltas), 2)
        label = "high" if score >= 0.75 else ("medium" if score >= 0.50 else "low")
        return score, label
    return 0.5, "unknown"


# =============================================================================
# 3. RECALCULAR PARA UNA FILA
# =============================================================================

def recompute_row(
    row: Dict,
    previous_exposure: float,
    vix_by_date: Dict[str, Optional[float]],
    signal_history: List[str],
) -> Dict:
    """
    Recalcula la inferencia y la exposición para un (batch_date, ticker).
    Devuelve un dict con los nuevos valores calculados.
    """
    date_str = str(row["batch_date"])
    ticker   = row["ticker"]

    # ── Evidencias (ya discretizadas en el pipeline original) ─────────────────
    evidence = {
        "Sentiment":  row.get("sentiment_state") or "neutral",
        "RSI":        row.get("rsi_state")        or "neutral",
        "Trend":      row.get("trend_state")      or "uptrend",
        "Volatility": row.get("volatility_state") or "high",
    }

    macro_adj  = float(row.get("macro_adjustment") or 0.0)
    risk_regime = row.get("risk_regime") or "NEUTRAL"
    macro_sent  = row.get("macro_sentiment") or "neutral"

    macro_ctx = {
        "macro_adjustment": macro_adj,
        "macro_sentiment":  macro_sent,
        "risk_regime":      risk_regime,
    }

    # Features extendidos para disc engine
    disc_extra = {
        "rsi_continuous":  row.get("rsi_14"),
        "adx_14":          row.get("adx_14"),
        "ema_55_pct":      row.get("ema_55_pct"),
        "momentum_20d":    row.get("momentum_20d"),
        "momentum_5d":     row.get("momentum_5d"),
        "signal_streak":   len(signal_history),
    }

    # ── Nueva inferencia ──────────────────────────────────────────────────────
    signal, prob_up = run_bayesian_inference(
        evidence, macro_adj, macro_context=macro_ctx, extra=disc_extra
    )

    # ── Conviction desde contribution_analysis ya guardado ────────────────────
    ca       = load_contribution_analysis(date_str, ticker)
    effects  = ca.get("effects", {})
    conv_score, conv_label = calc_conviction_from_effects(effects)

    # ── Exposición con conviction scaling ─────────────────────────────────────
    vix     = vix_by_date.get(date_str)
    sma50   = row.get("sma_50")
    sma200  = row.get("sma_200")
    dd_ath  = None  # no disponible directamente, usar regime guardado si existe

    regime  = row.get("old_regime") or detect_market_regime(
        sma50=sma50, sma200=sma200, vix=vix, drawdown_from_ath=dd_ath
    )

    _CONV_MULT = {"high": 1.10, "medium": 1.00, "low": 0.85, "unknown": 0.95}
    base_exp   = prob_to_exposure(prob_up, regime)
    target_exp = round(min(1.0, max(0.0, base_exp * _CONV_MULT.get(conv_label, 1.0))), 3)
    smoothed   = smooth_exposure(target_exp, previous_exposure)
    delta      = round(smoothed - previous_exposure, 4)

    return {
        "batch_date":      date_str,
        "ticker":          ticker,
        "new_signal":      signal,
        "new_prob_up":     prob_up,
        "new_prob_down":   round(1 - prob_up, 4),
        "conviction_score": conv_score,
        "conviction_label": conv_label,
        "conviction_mult":  _CONV_MULT.get(conv_label, 1.0),
        "base_exposure":    base_exp,
        "target_exposure":  target_exp,
        "smoothed_exposure": smoothed,
        "exposure_delta":   delta,
        "market_regime":    regime,
        "old_signal":       row.get("old_signal"),
        "old_prob_up":      row.get("old_prob_up"),
        "old_exposure":     row.get("old_exposure"),
    }


# =============================================================================
# 4. PERSISTIR RESULTADOS
# =============================================================================

def save_results(results: List[Dict], dry_run: bool):
    """Actualiza signal_outcomes y position_state con los nuevos valores."""
    if dry_run:
        logger.info(f"[DRY-RUN] Se actualizarían {len(results)} filas")
        return

    conn = get_pg_conn()
    cur  = conn.cursor()
    updated = 0

    for r in results:
        # Actualizar signal_outcomes
        cur.execute("""
            UPDATE signal_outcomes
            SET signal   = %s,
                prob_up  = %s,
                prob_down = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE batch_date = %s AND ticker = %s
        """, (r["new_signal"], r["new_prob_up"], r["new_prob_down"],
               r["batch_date"], r["ticker"]))

        # Actualizar position_state
        cur.execute("""
            INSERT INTO position_state
                (batch_date, ticker, prob_up, market_regime, target_exposure,
                 smoothed_exposure, exposure_delta)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                prob_up           = EXCLUDED.prob_up,
                market_regime     = EXCLUDED.market_regime,
                target_exposure   = EXCLUDED.target_exposure,
                smoothed_exposure = EXCLUDED.smoothed_exposure,
                exposure_delta    = EXCLUDED.exposure_delta
        """, (r["batch_date"], r["ticker"], r["new_prob_up"],
               r["market_regime"], r["target_exposure"],
               r["smoothed_exposure"], r["exposure_delta"]))
        updated += 1

    conn.commit()
    cur.close(); conn.close()
    logger.info(f"✅ PostgreSQL: {updated} filas actualizadas")


# =============================================================================
# 5. RESUMEN Y COMPARATIVA
# =============================================================================

def print_summary(results: List[Dict]):
    """Muestra cambios en señales y exposición antes/después."""
    changed_signal = sum(1 for r in results if r["new_signal"] != r["old_signal"])
    changed_expo   = sum(1 for r in results
                         if r["old_exposure"] is not None
                         and abs((r["smoothed_exposure"] or 0) - (r["old_exposure"] or 0)) > 0.01)

    conv_dist = defaultdict(int)
    for r in results:
        conv_dist[r["conviction_label"]] += 1

    print()
    print("=" * 70)
    print(f"  RECOMPUTE COMPLETADO  |  {len(results)} registros")
    print("=" * 70)
    print(f"  Señales cambiadas    : {changed_signal} ({changed_signal/len(results)*100:.1f}%)")
    print(f"  Exposición cambiada  : {changed_expo} ({changed_expo/len(results)*100:.1f}%)")
    print(f"  Distribución convicción: {dict(conv_dist)}")
    print()
    print(f"  {'Fecha':<12} {'Ticker':<6} {'OldSig':<7} {'NewSig':<7} "
          f"{'OldProb':>8} {'NewProb':>8} {'Conv':<8} {'Mult':>5} "
          f"{'OldExp%':>8} {'NewExp%':>8} {'Δ':>6}")
    print("  " + "-" * 80)
    for r in sorted(results, key=lambda x: (x["batch_date"], x["ticker"])):
        sig_changed = "←" if r["new_signal"] != r["old_signal"] else ""
        old_exp = (r["old_exposure"] or 0) * 100
        new_exp = (r["smoothed_exposure"] or 0) * 100
        delta   = new_exp - old_exp
        if abs(delta) > 0.5 or r["new_signal"] != r["old_signal"]:
            print(
                f"  {r['batch_date']:<12} {r['ticker']:<6} "
                f"{r['old_signal'] or '?':<7} {r['new_signal']:<7} "
                f"{(r['old_prob_up'] or 0):>7.3f}  {r['new_prob_up']:>7.3f}  "
                f"{r['conviction_label']:<8} {r['conviction_mult']:>4.2f}x "
                f"{old_exp:>7.1f}% {new_exp:>7.1f}%  {delta:>+5.1f}% {sig_changed}"
            )
    print("=" * 70)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Recalcula exposición sin re-ejecutar pipeline")
    parser.add_argument("--start",   default="2025-01-01")
    parser.add_argument("--end",     default=date.today().isoformat())
    parser.add_argument("--tickers", default=None, help="Coma-separado ej: SPY,GLD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None

    print("=" * 70)
    print("  RECOMPUTE EXPOSURE — Sin re-ejecutar el pipeline")
    print(f"  Rango: {args.start} → {args.end}")
    print(f"  Tickers: {tickers or 'todos'}")
    print(f"  {'[DRY-RUN]' if args.dry_run else '[ESCRIBIENDO EN BD]'}")
    print("=" * 70)

    # 1. Cargar datos ya procesados
    logger.info("Cargando evidencias desde PostgreSQL...")
    rows = load_existing_signals(args.start, args.end, tickers)
    if not rows:
        logger.error("No hay datos en ese rango. Verifica que el pipeline ya se ejecutó.")
        sys.exit(1)

    logger.info("Cargando VIX desde MongoDB...")
    vix_by_date = load_vix_series(args.start, args.end)

    # 2. Recalcular por ticker manteniendo historial de señales
    logger.info("Recalculando inferencia + exposición con conviction scaling...")
    results = []
    signal_history_per_ticker: Dict[str, List[str]] = defaultdict(list)
    exposure_per_ticker: Dict[str, float] = defaultdict(float)  # empieza en 0

    for i, row in enumerate(rows):
        ticker = row["ticker"]
        prev_exp = exposure_per_ticker[ticker]

        result = recompute_row(
            row, prev_exp, vix_by_date,
            signal_history_per_ticker[ticker]
        )
        results.append(result)

        # Actualizar estado para el día siguiente
        signal_history_per_ticker[ticker].append(result["new_signal"])
        signal_history_per_ticker[ticker] = signal_history_per_ticker[ticker][-5:]
        exposure_per_ticker[ticker] = result["smoothed_exposure"]

        if (i + 1) % 50 == 0:
            logger.info(f"  {i+1}/{len(rows)} procesados...")

    # 3. Mostrar resumen
    print_summary(results)

    # 4. Guardar
    logger.info("Guardando en PostgreSQL...")
    save_results(results, dry_run=args.dry_run)

    if not args.dry_run:
        logger.info("✅ Listo. Los reports en MongoDB seguirán mostrando los valores anteriores")
        logger.info("   hasta que regeneres los reports con bootstrap_365_days.py --reports-only")
    else:
        logger.info("[DRY-RUN] Ejecuta sin --dry-run para aplicar los cambios.")


if __name__ == "__main__":
    main()
