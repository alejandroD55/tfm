#!/usr/bin/env python3
"""
regenerate_reports.py — Actualiza métricas de exposición en MongoDB reports
============================================================================
Carga reports existentes de MongoDB, recalcula exposure_backtesting_metrics
usando los nuevos valores de position_state (con conviction scaling),
y hace upsert en MongoDB.

NO ejecuta FinBERT, NO descarga noticias, NO llama APIs externas.
Tiempo estimado: ~1-2 minutos para 370 días.

Uso:
    python regenerate_reports.py
    python regenerate_reports.py --start 2025-01-01 --end 2026-06-02
    python regenerate_reports.py --dry-run
"""

import os, sys, argparse, logging
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional
import psycopg2
import numpy as np
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bootstrap_365_days import DB_CONFIG, INITIAL_CAP, RISK_FREE_RATE

try:
    from pymongo import MongoClient, UpdateOne
    _mongo_db = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)["tfm"]
    MONGO_OK = True
    logger.info("MongoDB: conectado")
except Exception as e:
    logger.error(f"MongoDB no disponible: {e}"); sys.exit(1)


# =============================================================================
# 1. Cargar datos de PostgreSQL
# =============================================================================

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_full_records(start: str, end: str) -> List[Dict]:
    """
    Carga todos los signal_records con close_price y smoothed_exposure actualizados.
    """
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT
            so.batch_date::text,
            so.ticker,
            so.signal,
            so.prob_up,
            COALESCE(ti.close_price, so.price_d0) AS close_price,
            ps.smoothed_exposure,
            ps.target_exposure,
            ps.market_regime
        FROM signal_outcomes so
        LEFT JOIN position_state ps
            ON so.batch_date = ps.batch_date AND so.ticker = ps.ticker
        LEFT JOIN technical_indicators ti
            ON so.batch_date = ti.batch_date AND so.ticker = ti.ticker
        WHERE so.batch_date BETWEEN %s AND %s
        ORDER BY so.batch_date, so.ticker
    """, (start, end))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    logger.info(f"Cargados {len(rows)} registros de PostgreSQL")
    return rows


# =============================================================================
# 2. Calcular exposure backtesting desde los records
# =============================================================================

def calc_exposure_backtesting(records: List[Dict]) -> tuple:
    """
    Réplica de _calc_exposure_backtesting usando los records del periodo.
    portfolio_return_t = market_return_t × smoothed_exposure_t
    """
    exp_metrics: Dict = {}
    exp_diagnostics: Dict = {}

    tickers = list({r["ticker"] for r in records})
    for ticker in tickers:
        ts = sorted(
            [r for r in records if r["ticker"] == ticker],
            key=lambda x: x["batch_date"]
        )
        if len(ts) < 2:
            continue

        capital = INITIAL_CAP
        equity  = [capital]
        daily_rets: List[float] = []
        daily_exps: List[float] = []
        regime_days: Dict[str, int] = {"BULL": 0, "NEUTRAL": 0, "HIGH_VOL": 0, "BEAR": 0}

        for i in range(1, len(ts)):
            p0 = float(ts[i-1].get("close_price") or 0)
            p1 = float(ts[i].get("close_price") or 0)
            if p0 == 0 or p1 == 0:
                equity.append(equity[-1]); continue

            market_ret = (p1 - p0) / p0
            exposure   = float(ts[i].get("smoothed_exposure") or 0.5)
            daily_exps.append(exposure)

            regime = ts[i].get("market_regime", "NEUTRAL") or "NEUTRAL"
            if regime in regime_days:
                regime_days[regime] += 1

            portfolio_ret = market_ret * exposure
            capital *= 1.0 + portfolio_ret
            equity.append(capital)
            daily_rets.append(portfolio_ret)

        fe      = capital
        cum_ret = (fe - INITIAL_CAP) / INITIAL_CAP

        if len(equity) > 2:
            eq_arr  = np.array(equity)
            dr      = np.diff(eq_arr) / eq_arr[:-1]
            excess  = dr - (RISK_FREE_RATE / 252)
            std     = np.std(excess)
            sharpe  = float(np.mean(excess) / std * np.sqrt(252)) if std > 1e-6 else 0.0
            peak    = np.maximum.accumulate(eq_arr)
            max_dd  = float(np.min((eq_arr - peak) / peak))
        else:
            sharpe = max_dd = 0.0

        exp_metrics[ticker] = {
            "cumulative_return": round(float(cum_ret), 6),
            "sharpe_ratio":      round(float(sharpe), 4),
            "max_drawdown":      round(float(max_dd), 4),
            "final_equity":      round(float(fe), 2),
        }
        exp_diagnostics[ticker] = {
            "avg_exposure": round(float(np.mean(daily_exps)), 4) if daily_exps else 0.5,
            "min_exposure": round(float(np.min(daily_exps)), 4) if daily_exps else 0.0,
            "max_exposure": round(float(np.max(daily_exps)), 4) if daily_exps else 1.0,
            "regime_distribution": regime_days,
        }

    return exp_metrics, exp_diagnostics


# =============================================================================
# 3. Actualizar reports en MongoDB
# =============================================================================

def regenerate(start: str, end: str, dry_run: bool):
    # Cargar todos los records con los valores actualizados de position_state
    all_records = load_full_records(start, end)
    if not all_records:
        logger.error("Sin datos. Verifica que recompute_exposure.py se ejecutó correctamente.")
        sys.exit(1)

    # Fechas únicas ordenadas
    dates = sorted({r["batch_date"] for r in all_records})
    logger.info(f"Procesando {len(dates)} fechas...")

    ops = []
    for i, date_str in enumerate(dates):
        # Records acumulados hasta esta fecha (para el backtesting correcto)
        records_to_date = [r for r in all_records if r["batch_date"] <= date_str]

        exp_metrics, exp_diagnostics = calc_exposure_backtesting(records_to_date)

        # Recalcular summary
        returns = [m.get("cumulative_return", 0) for m in exp_metrics.values()]
        sharpes = [m.get("sharpe_ratio", 0)      for m in exp_metrics.values()]
        dds     = [m.get("max_drawdown", 0)       for m in exp_metrics.values()]

        avg_ret = round(sum(returns)/len(returns), 6) if returns else 0
        avg_sh  = round(sum(sharpes)/len(sharpes), 4) if sharpes else 0
        avg_dd  = round(sum(dds)/len(dds), 4)         if dds     else 0

        if not dry_run:
            ops.append(UpdateOne(
                {"report_date": date_str},
                {"$set": {
                    "exposure_backtesting_metrics":     exp_metrics,
                    "exposure_backtesting_diagnostics": exp_diagnostics,
                    "summary.avg_cumulative_return":    avg_ret,
                    "summary.avg_sharpe_ratio":         avg_sh,
                    "summary.avg_max_drawdown":         avg_dd,
                    "updated_at": datetime.utcnow(),
                }},
                upsert=False,  # solo actualiza si ya existe el report
            ))

        if (i+1) % 50 == 0 or (i+1) == len(dates):
            sample_ticker = list(exp_metrics.keys())[0] if exp_metrics else "?"
            sample_m      = exp_metrics.get(sample_ticker, {})
            logger.info(
                f"  {i+1}/{len(dates)}: {date_str}  "
                f"[{sample_ticker}] ret={sample_m.get('cumulative_return',0)*100:+.1f}%  "
                f"sharpe={sample_m.get('sharpe_ratio',0):.2f}  "
                f"equity={sample_m.get('final_equity',10000):.0f}€"
            )

    if dry_run:
        logger.info(f"[DRY-RUN] Se actualizarían {len(dates)} reports en MongoDB")
        return

    # Bulk write
    if ops:
        result = _mongo_db["reports"].bulk_write(ops, ordered=False)
        logger.info(f"✅ MongoDB: {result.modified_count} reports actualizados / {result.upserted_count} nuevos")
    else:
        logger.info("Sin operaciones a ejecutar.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start",   default="2025-01-01")
    parser.add_argument("--end",     default="2026-06-02")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 65)
    print(f"  REGENERAR REPORTS  |  {args.start} → {args.end}")
    print(f"  {'[DRY-RUN]' if args.dry_run else '[ESCRIBIENDO EN MONGODB]'}")
    print("=" * 65)

    regenerate(args.start, args.end, dry_run=args.dry_run)

    if not args.dry_run:
        print("\n✅ Reports actualizados en MongoDB con conviction scaling.")
        print("   Los reports quedan listos para el dashboard (selector por pipeline_start/end).")


if __name__ == "__main__":
    main()
