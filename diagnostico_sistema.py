#!/usr/bin/env python3
"""
diagnostico_sistema.py — Verifica el estado completo del sistema
Ejecuta solo SELECTs y lecturas. No modifica nada.
"""
import os, sys
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import psycopg2
from bootstrap_365_days import DB_CONFIG

try:
    from pymongo import MongoClient
    db = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)["tfm"]
    MONGO_OK = True
except Exception as e:
    print(f"⚠️  MongoDB: {e}"); MONGO_OK = False; db = None

def pg():
    return psycopg2.connect(**DB_CONFIG)

SEP = "=" * 70

print(SEP)
print("  DIAGNÓSTICO DEL SISTEMA — Solo lectura, nada se modifica")
print(SEP)

# ── 1. PostgreSQL — signal_outcomes ──────────────────────────────────────────
print("\n📊 PostgreSQL: signal_outcomes")
try:
    conn = pg(); cur = conn.cursor()
    cur.execute("""
        SELECT ticker,
               COUNT(*) as total,
               MIN(batch_date) as desde,
               MAX(batch_date) as hasta,
               COUNT(CASE WHEN pipeline_start IS NOT NULL THEN 1 END) as con_pipeline_start
        FROM signal_outcomes
        GROUP BY ticker ORDER BY ticker
    """)
    rows = cur.fetchall()
    print(f"  {'Ticker':<8} {'Total':>6} {'Desde':<12} {'Hasta':<12} {'Con pipeline_start':>18}")
    print("  " + "-" * 60)
    for r in rows:
        print(f"  {r[0]:<8} {r[1]:>6} {str(r[2]):<12} {str(r[3]):<12} {r[4]:>18}")
    cur.close(); conn.close()
    ok_tickers = [r[0] for r in rows if r[0] in ['SPY','IWM','GLD','XLE','NVDA']]
    if len(ok_tickers) == 5:
        print("  ✅ Los 5 tickers activos tienen datos")
    else:
        print(f"  ⚠️  Tickers activos encontrados: {ok_tickers}")
except Exception as e:
    print(f"  ❌ Error: {e}")

# ── 2. PostgreSQL — position_state ────────────────────────────────────────────
print("\n📊 PostgreSQL: position_state (exposición)")
try:
    conn = pg(); cur = conn.cursor()
    cur.execute("""
        SELECT ticker,
               COUNT(*) as total,
               ROUND(AVG(smoothed_exposure)::numeric, 3) as avg_exp,
               ROUND(MIN(smoothed_exposure)::numeric, 3) as min_exp,
               ROUND(MAX(smoothed_exposure)::numeric, 3) as max_exp
        FROM position_state
        GROUP BY ticker ORDER BY ticker
    """)
    rows = cur.fetchall()
    print(f"  {'Ticker':<8} {'Total':>6} {'Avg Exp':>9} {'Min Exp':>9} {'Max Exp':>9}")
    print("  " + "-" * 45)
    for r in rows:
        print(f"  {r[0]:<8} {r[1]:>6} {float(r[2] or 0)*100:>8.1f}% {float(r[3] or 0)*100:>8.1f}% {float(r[4] or 0)*100:>8.1f}%")
    cur.close(); conn.close()
except Exception as e:
    print(f"  ❌ Error: {e}")

# ── 3. PostgreSQL — technical_indicators (nuevas columnas) ────────────────────
print("\n📊 PostgreSQL: technical_indicators (columnas nuevas)")
try:
    conn = pg(); cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'technical_indicators'
        ORDER BY ordinal_position
    """)
    all_cols = [r[0] for r in cur.fetchall()]
    new_cols = ['sma_200','adx_14','ema_55_pct','momentum_20d','momentum_5d']
    for col in new_cols:
        if col in all_cols:
            cur.execute(f"SELECT COUNT(*) FROM technical_indicators WHERE {col} IS NOT NULL")
            n = cur.fetchone()[0]
            print(f"  ✅ {col:<15}: {n} filas con datos")
        else:
            print(f"  ❌ {col:<15}: columna NO existe")
    cur.close(); conn.close()
except Exception as e:
    print(f"  ❌ Error: {e}")

# ── 4. MongoDB — reports ──────────────────────────────────────────────────────
if MONGO_OK:
    print("\n📊 MongoDB: reports (pipelines independientes)")
    try:
        # Contar por pipeline_start
        pipeline = db["reports"].aggregate([
            {"$group": {
                "_id": "$pipeline_start",
                "count": {"$sum": 1},
                "last_date": {"$max": "$report_date"},
                "has_exp_metrics": {"$sum": {"$cond": [
                    {"$ifNull": ["$exposure_backtesting_metrics", False]}, 1, 0
                ]}}
            }},
            {"$sort": {"_id": 1}}
        ])
        rows = list(pipeline)
        total_reports = 0
        print(f"  {'Pipeline Start':<15} {'Reports':>8} {'Último':>12} {'Con exp_metrics':>16}")
        print("  " + "-" * 55)
        for r in rows:
            ps = r["_id"] or "sin pipeline_start"
            print(f"  {str(ps):<15} {r['count']:>8} {str(r['last_date']):<12} {r['has_exp_metrics']:>16}")
            total_reports += r['count']
        print(f"  Total: {total_reports} reports")
        if total_reports > 0:
            print("  ✅ Reports originales intactos")
        else:
            print("  ❌ No hay reports en la colección 'reports'")
    except Exception as e:
        print(f"  ❌ Error: {e}")

# ── 5. Resumen y acciones recomendadas ────────────────────────────────────────
print(f"\n{SEP}")
print("  RESUMEN Y PRÓXIMOS PASOS")
print(SEP)
print("""
  Flujo correcto para tener todo actualizado:

  1. psql ... -c "ALTER TABLE technical_indicators ADD COLUMN IF NOT EXISTS ..."
  2. python backfill_indicators.py          ← indicadores ADX/EMA55/momentum
  3. python recompute_exposure.py           ← exposición con conviction scaling
  4. python regenerate_reports.py           ← actualizar MongoDB reports

  Backtest continuo (sin ingesta): bootstrap_365_days.py --interpret-only --start ... --end ...
  o: python scripts/run_interpret_pipeline.py --start ... --end ...
""")
