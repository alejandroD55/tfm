#!/usr/bin/env python3
"""
fix_pipeline_metrics.py — Recalcula exposure_backtesting_metrics POR PIPELINE
==============================================================================
Recalcula exposure_backtesting_metrics por pipeline (pipeline_start/pipeline_end en reports).

Este script:
1. Para cada pipeline, calcula exposure backtesting usando SOLO sus registros
2. Escribe esas métricas de vuelta a MongoDB (solo los tickers activos)
3. Limpia tickers obsoletos de los campos de exposición
"""
import os, sys, logging
from collections import defaultdict
from typing import Dict, List
import psycopg2
import numpy as np
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bootstrap_365_days import DB_CONFIG, INITIAL_CAP, RISK_FREE_RATE, TICKERS

try:
    from pymongo import MongoClient, UpdateOne
    db = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)["tfm"]
    logger.info("MongoDB: conectado")
except Exception as e:
    logger.error(f"MongoDB: {e}"); sys.exit(1)

ACTIVE_TICKERS = set(TICKERS)  # Solo SPY, IWM, GLD, XLE, NVDA

def pg():
    return psycopg2.connect(**DB_CONFIG)

def load_pipeline_records(ps: str, pe: str) -> List[Dict]:
    """Carga registros de UN SOLO pipeline (fecha inicio → fin)."""
    conn = pg(); cur = conn.cursor()
    cur.execute("""
        SELECT so.batch_date::text, so.ticker, so.signal, so.prob_up,
               COALESCE(ti.close_price, so.price_d0) AS close_price,
               ps.smoothed_exposure, ps.market_regime
        FROM signal_outcomes so
        LEFT JOIN position_state ps ON so.batch_date = ps.batch_date AND so.ticker = ps.ticker
        LEFT JOIN technical_indicators ti ON so.batch_date = ti.batch_date AND so.ticker = ti.ticker
        WHERE so.batch_date BETWEEN %s AND %s
          AND so.ticker = ANY(%s)
        ORDER BY so.batch_date, so.ticker
    """, (ps, pe, list(ACTIVE_TICKERS)))
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

def calc_exposure_bt(records: List[Dict]) -> tuple:
    """Calcula exposure backtesting para un conjunto de registros."""
    exp_m: Dict = {}; exp_d: Dict = {}
    for ticker in ACTIVE_TICKERS:
        ts = sorted([r for r in records if r["ticker"] == ticker], key=lambda x: x["batch_date"])
        if len(ts) < 2: continue
        capital = INITIAL_CAP; equity = [capital]
        daily_rets = []; daily_exps = []
        regime_days = {"BULL": 0, "NEUTRAL": 0, "HIGH_VOL": 0, "BEAR": 0}
        for i in range(1, len(ts)):
            p0 = float(ts[i-1].get("close_price") or 0)
            p1 = float(ts[i].get("close_price") or 0)
            if p0 == 0 or p1 == 0: equity.append(equity[-1]); continue
            mret = (p1 - p0) / p0
            exp  = float(ts[i].get("smoothed_exposure") or 0.5)
            daily_exps.append(exp)
            reg = ts[i].get("market_regime","NEUTRAL") or "NEUTRAL"
            if reg in regime_days: regime_days[reg] += 1
            portfolio_ret = mret * exp
            capital *= 1 + portfolio_ret
            equity.append(capital); daily_rets.append(portfolio_ret)
        fe = capital; cum = (fe - INITIAL_CAP) / INITIAL_CAP
        if len(equity) > 2:
            ea = np.array(equity); dr = np.diff(ea)/ea[:-1]
            ex = dr - (RISK_FREE_RATE/252); std = np.std(ex)
            sharpe = float(np.mean(ex)/std*np.sqrt(252)) if std > 1e-6 else 0.0
            peak = np.maximum.accumulate(ea); max_dd = float(np.min((ea-peak)/peak))
        else: sharpe = max_dd = 0.0
        exp_m[ticker] = {"cumulative_return": round(float(cum),6), "sharpe_ratio": round(float(sharpe),4),
                         "max_drawdown": round(float(max_dd),4), "final_equity": round(float(fe),2)}
        exp_d[ticker] = {"avg_exposure": round(float(np.mean(daily_exps)),4) if daily_exps else 0.5,
                         "min_exposure": round(float(np.min(daily_exps)),4) if daily_exps else 0.0,
                         "max_exposure": round(float(np.max(daily_exps)),4) if daily_exps else 1.0,
                         "regime_distribution": regime_days}
    return exp_m, exp_d

def main():
    # Detectar pipelines desde MongoDB
    pipeline = list(db["reports"].aggregate([
        {"$group": {"_id": {"ps": "$pipeline_start", "pe": "$pipeline_end"},
                    "count": {"$sum": 1}}},
        {"$sort": {"_id.ps": 1}}
    ]))
    print("=" * 65)
    print("  FIX PIPELINE METRICS — Recalculando por pipeline")
    print("=" * 65)
    print(f"  Pipelines encontrados: {len(pipeline)}")

    ops = []
    for p in pipeline:
        ps = p["_id"]["ps"]; pe = p["_id"]["pe"]
        if not ps or not pe: continue
        logger.info(f"\n📊 Pipeline {ps} → {pe} ({p['count']} reports)")

        # Calcular con solo los registros de este pipeline
        records = load_pipeline_records(ps, pe)
        logger.info(f"  Registros cargados: {len(records)} "
                    f"(tickers: {sorted({r['ticker'] for r in records})})")
        if not records: continue

        exp_m, exp_d = calc_exposure_bt(records)

        # Mostrar resumen
        for t in sorted(exp_m.keys()):
            m = exp_m[t]
            logger.info(f"    {t}: ret={m['cumulative_return']*100:+.2f}% "
                        f"sharpe={m['sharpe_ratio']:.2f} "
                        f"equity={m['final_equity']:.0f}€ "
                        f"avg_exp={exp_d[t]['avg_exposure']*100:.1f}%")

        # Calcular summary corregido
        rets = [exp_m[t]["cumulative_return"] for t in exp_m]
        sh   = [exp_m[t]["sharpe_ratio"] for t in exp_m]
        dds  = [exp_m[t]["max_drawdown"] for t in exp_m]

        # UPDATE de todos los reports de este pipeline con las métricas correctas
        for doc in db["reports"].find(
            {"pipeline_start": ps, "pipeline_end": pe},
            {"report_date": 1, "_id": 1}
        ):
            # Para reports intermedios: calcular con registros hasta esa fecha
            date_str = doc["report_date"]
            records_partial = [r for r in records if r["batch_date"] <= date_str]
            if len(records_partial) < 2:
                continue
            em_partial, ed_partial = calc_exposure_bt(records_partial)
            if not em_partial:
                continue
            rets_p = [em_partial[t]["cumulative_return"] for t in em_partial]
            ops.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "exposure_backtesting_metrics":     em_partial,
                    "exposure_backtesting_diagnostics": ed_partial,
                    "summary.avg_cumulative_return": round(sum(rets_p)/len(rets_p),6) if rets_p else 0,
                }}
            ))

    if ops:
        logger.info(f"\n💾 Aplicando {len(ops)} updates en MongoDB...")
        result = db["reports"].bulk_write(ops, ordered=False)
        logger.info(f"✅ MongoDB: {result.modified_count} reports corregidos")
    else:
        logger.warning("Sin operaciones generadas.")

    print("\n✅ Métricas corregidas por pipeline.")

if __name__ == "__main__":
    main()
