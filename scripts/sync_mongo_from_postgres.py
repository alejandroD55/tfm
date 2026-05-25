#!/usr/bin/env python3
"""
Rellena MongoDB Atlas desde PostgreSQL local tras un backtest.

El runner escribió Aurora (786 señales, 262 días) pero Mongo puede quedar vacío
(borrado manual, reset de cluster, etc.). Este script reconstruye lo mínimo
para que el dashboard funcione sin re-ejecutar 365 días.

Uso:
  source .venv/bin/activate
  python scripts/sync_mongo_from_postgres.py --dry-run
  python scripts/sync_mongo_from_postgres.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "shared"))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

TICKERS = ["SPY", "IWM", "GLD"]
MODEL_VERSION = "local-backtest-v1"


def pg_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        user=os.getenv("POSTGRES_USER", "tfmadmin"),
        password=os.getenv("POSTGRES_PASSWORD", "localpassword123"),
        database=os.getenv("POSTGRES_DB", "tfm"),
    )


def fetch_dates(conn) -> list[str]:
    with conn.cursor() as c:
        c.execute(
            "SELECT DISTINCT batch_date::text FROM trading_signals ORDER BY 1"
        )
        return [r[0][:10] for r in c.fetchall()]


def rebuild_bayesian_trace(conn, batch_date: str) -> dict | None:
    tickers_trace = {}
    with conn.cursor() as c:
        for ticker in TICKERS:
            c.execute(
                """
                SELECT ts.signal, ts.prob_up, ts.prob_down,
                       e.sentiment_state, e.rsi_state, e.trend_state, e.volatility_state,
                       ti.close_price, ti.rsi_14, ti.sma_20, ti.sma_50,
                       ti.bb_upper, ti.bb_lower
                FROM trading_signals ts
                LEFT JOIN signal_explanations e
                  ON e.batch_date = ts.batch_date AND e.ticker = ts.ticker
                LEFT JOIN technical_indicators ti
                  ON ti.batch_date = ts.batch_date AND ti.ticker = ts.ticker
                WHERE ts.batch_date = %s AND ts.ticker = %s
                """,
                (batch_date, ticker),
            )
            row = c.fetchone()
            if not row:
                continue
            signal, prob_up, prob_down = row[0], row[1], row[2]
            sent, rsi_s, trend_s, vol_s = row[3], row[4], row[5], row[6]
            close, rsi, sma20, sma50, bb_u, bb_l = row[7:14]
            sma_spread = None
            if sma20 is not None and sma50 is not None:
                sma_spread = round(float(sma20) - float(sma50), 4)
            bb_ratio = None
            if bb_u and bb_l and close and float(close) > 0:
                bb_ratio = round((float(bb_u) - float(bb_l)) / float(close), 4)
            tickers_trace[ticker] = {
                "raw_values": {
                    "close_price": float(close) if close else None,
                    "rsi_14": float(rsi) if rsi else None,
                    "sma_20": float(sma20) if sma20 else None,
                    "sma_50": float(sma50) if sma50 else None,
                    "sma_spread": sma_spread,
                    "bb_upper": float(bb_u) if bb_u else None,
                    "bb_lower": float(bb_l) if bb_l else None,
                    "bb_width_ratio": bb_ratio,
                },
                "discretization": {
                    "sentiment_state": sent or "neutral",
                    "rsi_state": rsi_s or "neutral",
                    "trend_state": trend_s or "uptrend",
                    "volatility_state": vol_s or "low",
                },
                "inference": {
                    "signal": signal,
                    "prob_up": float(prob_up) if prob_up is not None else None,
                    "prob_down": float(prob_down) if prob_down is not None else None,
                    "threshold_used": {"prob_up_above": 0.65},
                },
                "reasoning": f"Reconstruido desde PostgreSQL ({batch_date}).",
            }
    if not tickers_trace:
        return None
    now = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": "2.0",
        "batch_date": batch_date,
        "generated_at": now,
        "execution": {
            "run_id": f"backtest-{batch_date}",
            "trigger_type": "scheduled",
            "batch_date": batch_date,
            "tickers_attempted": len(TICKERS),
            "signals_generated": len(tickers_trace),
        },
        "model_config": {"version": MODEL_VERSION},
        "tickers": tickers_trace,
        "audit_notes": {"source": "sync_mongo_from_postgres.py"},
    }


def sync_macro_context(conn, apply: bool) -> int:
    from mongo_utils import upsert_macro_context

    n = 0
    with conn.cursor() as c:
        c.execute(
            """
            SELECT ms.batch_date::text,
                   ms.macro_sentiment,
                   mr.risk_regime,
                   COALESCE(mr.macro_adjustment, 0),
                   mr.vix
            FROM macro_sentiment_scores ms
            LEFT JOIN market_regime_state mr ON mr.batch_date = ms.batch_date
            ORDER BY ms.batch_date
            """
        )
        rows = c.fetchall()
    for batch_date, sent, regime, adj, vix in rows:
        bd = batch_date[:10]
        detail = {
            "macro_score": 0.0,
            "vix": float(vix) if vix is not None else None,
            "events": {},
            "source": "postgres_sync",
        }
        if apply:
            upsert_macro_context(bd, sent or "neutral", regime or "NEUTRAL", float(adj or 0), detail)
        n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Escribe en MongoDB (sin este flag solo muestra conteos PG)",
    )
    args = parser.parse_args()
    apply = args.apply

    print(f"Modo: {'APPLY' if apply else 'DRY-RUN'}")
    print(f"Mongo URI host: ...@{os.getenv('MONGODB_URI','').split('@')[-1].split('/')[0] if '@' in os.getenv('MONGODB_URI','') else '?'}")
    print(f"PG: {os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}")

    conn = pg_conn()
    dates = fetch_dates(conn)
    print(f"Fechas con señales en PG: {len(dates)} ({dates[0]} … {dates[-1]})")

    if not apply:
        print("Ejecuta con --apply para escribir en MongoDB.")
        conn.close()
        return

    from mongo_utils import (
        upsert_bayesian_trace,
        upsert_bayesian_report,
        upsert_watchlist,
    )

    upsert_watchlist(TICKERS, name="Universo TFM (sync desde PG)")

    trace_count = 0
    for d in dates:
        trace = rebuild_bayesian_trace(conn, d)
        if not trace:
            continue
        upsert_bayesian_trace(d, trace)
        for ticker, tdata in trace["tickers"].items():
            upsert_bayesian_report(d, ticker, tdata, MODEL_VERSION)
        trace_count += 1

    macro_n = sync_macro_context(conn, True)

    # Reporte completo (dashboard métricas) — usa lógica del runner
    sys.path.insert(0, str(ROOT))
    from local_backtest_runner import generate_and_save_report  # noqa: E402

    final_date = dates[-1]
    run_id = f"backtest-{final_date}"
    report = generate_and_save_report(final_date, conn, run_id)

    conn.close()

    # Verificación
    from pymongo import MongoClient

    client = MongoClient(os.getenv("MONGODB_URI"), serverSelectionTimeoutMS=8000)
    db = client[os.getenv("MONGODB_DB", "tfm")]
    print("\n--- Verificación MongoDB ---")
    for col in ("reports", "bayesian_traces", "bayesian_reports", "macro_context", "watchlists"):
        print(f"  {col}: {db[col].count_documents({})}")
    if report:
        print(f"  Reporte {final_date}: {list(report.get('backtesting_metrics', {}).keys())}")
    print(f"\nTrazas escritas: {trace_count}, macro_context: {macro_n}")
    client.close()


if __name__ == "__main__":
    main()
