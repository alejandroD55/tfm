#!/usr/bin/env python3
"""
Recompute exposure backtest metrics in Mongo `reports` without re-running bootstrap.

Uses PostgreSQL position_state + technical_indicators (Mongo bayesian_reports fallback).
Same formulas as bootstrap_365_days (_calc_exposure_backtesting / compute_benchmark).

Usage (from tfm/):
  python scripts/recompute_exposure_reports.py --dry-run
  python scripts/recompute_exposure_reports.py --start 2025-01-01 --end 2025-03-31
  python scripts/recompute_exposure_reports.py --start 2025-01-01 --end 2025-03-31 --pipeline-id 2024-06-03

Requires .env: POSTGRES_* (or LOCAL_PG_*), MONGODB_URI, optional MONGODB_DB (default tfm).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "shared"))

from exposure_backtest import (  # noqa: E402
    DAYS_BACK,
    build_exposure_report_patch,
    calc_binary_backtesting,
    calc_exposure_backtesting,
    compute_benchmark,
)

load_dotenv(_REPO / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", os.getenv("LOCAL_PG_HOST", "localhost")),
        port=int(os.getenv("POSTGRES_PORT", os.getenv("LOCAL_PG_PORT", "5432"))),
        user=os.getenv("POSTGRES_USER", os.getenv("LOCAL_PG_USER", "tfmadmin")),
        password=os.getenv(
            "POSTGRES_PASSWORD", os.getenv("LOCAL_PG_PASSWORD", "localpassword123")
        ),
        database=os.getenv("POSTGRES_DB", os.getenv("LOCAL_PG_DB", "tfm")),
        sslmode=os.getenv("POSTGRES_SSLMODE", os.getenv("LOCAL_PG_SSLMODE", "prefer")),
        connect_timeout=15,
    )


def _mongo_db():
    from mongo_utils import _get_db

    db = _get_db()
    if db is None:
        raise RuntimeError("MongoDB no disponible (revisa MONGODB_URI en .env)")
    return db


def _parse_date(s: str) -> date:
    return pd.to_datetime(s).date()


def _norm_batch_date(v) -> str:
    if hasattr(v, "isoformat"):
        return v.isoformat()[:10]
    return str(v)[:10]


def list_report_dates(
    db,
    start: Optional[date],
    end: Optional[date],
    pipeline_id: Optional[str],
) -> List[Tuple[str, Optional[str]]]:
    """Returns [(report_date, pipeline_start_iso), ...] sorted ascending."""
    query: Dict = {}
    if start or end:
        rd: Dict = {}
        if start:
            rd["$gte"] = start.isoformat()
        if end:
            rd["$lte"] = end.isoformat()
        query["report_date"] = rd
    if pipeline_id:
        query["pipeline_start"] = pipeline_id

    cursor = db["reports"].find(
        query,
        {"_id": 0, "report_date": 1, "pipeline_start": 1},
    ).sort("report_date", 1)

    out = []
    for doc in cursor:
        rd = doc.get("report_date")
        if not rd:
            continue
        out.append((_norm_batch_date(rd), doc.get("pipeline_start")))
    return out


def fetch_exposure_rows_pg(
    conn,
    start: date,
    end: date,
    tickers: Optional[List[str]] = None,
) -> List[Dict]:
    sql = """
        SELECT ps.batch_date, ps.ticker, ps.smoothed_exposure, ps.market_regime,
               ps.prob_up, ps.target_exposure, ti.close_price
        FROM position_state ps
        JOIN technical_indicators ti
          ON ps.batch_date = ti.batch_date AND ps.ticker = ti.ticker
        WHERE ps.batch_date >= %s AND ps.batch_date <= %s
    """
    params: list = [start, end]
    if tickers:
        sql += " AND ps.ticker = ANY(%s)"
        params.append(tickers)
    sql += " ORDER BY ps.batch_date, ps.ticker"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    records = []
    for r in rows:
        records.append(
            {
                "batch_date": _norm_batch_date(r[0]),
                "ticker": r[1],
                "smoothed_exposure": float(r[2]) if r[2] is not None else 0.0,
                "market_regime": r[3] or "NEUTRAL",
                "prob_up": float(r[4]) if r[4] is not None else None,
                "target_exposure": float(r[5]) if r[5] is not None else None,
                "close_price": float(r[6]) if r[6] is not None else 0.0,
            }
        )
    return records


def fetch_exposure_rows_mongo(
    db,
    start: date,
    end: date,
    tickers: Optional[List[str]] = None,
) -> List[Dict]:
    query: Dict = {
        "batch_date": {"$gte": start.isoformat(), "$lte": end.isoformat()},
    }
    if tickers:
        query["ticker"] = {"$in": tickers}

    records = []
    for doc in db["bayesian_reports"].find(query):
        em = doc.get("exposure_management") or {}
        inf = doc.get("inference") or {}
        raw = doc.get("raw_values") or {}
        close = raw.get("close") or raw.get("close_price")
        if close is None:
            ohlcv = db["ohlcv"].find_one(
                {"ticker": doc["ticker"], "batch_date": doc["batch_date"]},
                {"close": 1},
                sort=[("date", -1)],
            )
            if ohlcv:
                close = ohlcv.get("close")
        if close is None:
            continue
        records.append(
            {
                "batch_date": _norm_batch_date(doc["batch_date"]),
                "ticker": doc["ticker"],
                "smoothed_exposure": float(em.get("smoothed_exposure", 0.0)),
                "market_regime": em.get("market_regime") or "NEUTRAL",
                "prob_up": inf.get("prob_up"),
                "target_exposure": em.get("target_exposure"),
                "close_price": float(close),
            }
        )
    return records


def merge_exposure_sources(
    pg_rows: List[Dict], mongo_rows: List[Dict]
) -> List[Dict]:
    by_key: Dict[Tuple[str, str], Dict] = {}
    for row in mongo_rows:
        by_key[(row["batch_date"], row["ticker"])] = row
    for row in pg_rows:
        by_key[(row["batch_date"], row["ticker"])] = row
    return sorted(by_key.values(), key=lambda x: (x["batch_date"], x["ticker"]))


def fetch_trading_signals_bulk(
    conn,
    window_start: date,
    window_end: date,
    tickers: Optional[List[str]] = None,
) -> pd.DataFrame:
    sql = """
        SELECT ts.batch_date, ts.ticker, ts.signal, ts.prob_up, ts.prob_down,
               ti.close_price
        FROM trading_signals ts
        JOIN technical_indicators ti
          ON ts.batch_date = ti.batch_date AND ts.ticker = ti.ticker
        WHERE ts.batch_date >= %s AND ts.batch_date <= %s
    """
    params: list = [window_start, window_end]
    if tickers:
        sql += " AND ts.ticker = ANY(%s)"
        params.append(tickers)
    sql += " ORDER BY ts.batch_date, ts.ticker"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(
            columns=["batch_date", "ticker", "signal", "prob_up", "prob_down", "close_price"]
        )
    df = pd.DataFrame(
        rows,
        columns=["batch_date", "ticker", "signal", "prob_up", "prob_down", "close_price"],
    )
    df["batch_date"] = df["batch_date"].apply(_norm_batch_date)
    return df


def slice_trading_window(
    df: pd.DataFrame,
    report_date: str,
    pipeline_start: Optional[date],
) -> pd.DataFrame:
    end = _parse_date(report_date)
    start = end - timedelta(days=DAYS_BACK)
    if pipeline_start is not None:
        start = max(start, pipeline_start)
    mask = (df["batch_date"] >= start.isoformat()) & (df["batch_date"] <= report_date)
    return df.loc[mask].copy()


def slice_cumulative_exposure(
    all_rows: List[Dict],
    report_date: str,
    pipeline_start: Optional[date],
) -> List[Dict]:
    ps = pipeline_start.isoformat() if pipeline_start else None
    out = []
    for r in all_rows:
        bd = r["batch_date"]
        if bd > report_date:
            continue
        if ps and bd < ps:
            continue
        out.append(r)
    return out


def resolve_pipeline_start(
    report_date: str, doc_pipeline_start: Optional[str], fallback: date
) -> date:
    if doc_pipeline_start:
        return _parse_date(str(doc_pipeline_start)[:10])
    return fallback


def update_report_mongo(db, report_date: str, patch: Dict, dry_run: bool) -> bool:
    if dry_run:
        tickers = list(patch.get("exposure_backtesting_metrics", {}).keys())
        summary = patch.get("summary", {})
        logger.info(
            "[dry-run] %s tickers=%s avg_ret=%s",
            report_date,
            tickers,
            summary.get("avg_cumulative_return"),
        )
        return True
    now = datetime.now(timezone.utc)
    patch["metrics_recomputed_at"] = now.isoformat()
    result = db["reports"].update_one(
        {"report_date": report_date},
        {"$set": {**patch, "updated_at": now}},
    )
    return result.matched_count > 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recompute exposure metrics in Mongo reports from PG/Mongo state"
    )
    parser.add_argument("--start", type=str, help="YYYY-MM-DD inclusive")
    parser.add_argument("--end", type=str, help="YYYY-MM-DD inclusive")
    parser.add_argument(
        "--pipeline-id",
        type=str,
        help="Filter reports by exact pipeline_start value (ISO date string)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print updates without writing to Mongo",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        help="Comma-separated tickers subset (default: all in position_state)",
    )
    args = parser.parse_args()

    start_d = _parse_date(args.start) if args.start else None
    end_d = _parse_date(args.end) if args.end else None
    tickers = (
        [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    )

    db = _mongo_db()
    report_docs = list_report_dates(db, start_d, end_d, args.pipeline_id)
    if not report_docs:
        logger.error("No hay reportes en Mongo para el filtro indicado.")
        return 1

    logger.info("Reportes a procesar: %d", len(report_docs))

    global_min_ps = start_d
    for _, ps in report_docs:
        if ps:
            d = _parse_date(ps[:10])
            if global_min_ps is None or d < global_min_ps:
                global_min_ps = d
    data_start = global_min_ps or _parse_date(report_docs[0][0])
    data_end = end_d or _parse_date(report_docs[-1][0])

    conn = _pg_connect()
    try:
        pg_rows = fetch_exposure_rows_pg(conn, data_start, data_end, tickers)
        mongo_rows = []
        if not pg_rows:
            logger.warning(
                "position_state vacío en PG (%s..%s); intentando bayesian_reports",
                data_start,
                data_end,
            )
            mongo_rows = fetch_exposure_rows_mongo(db, data_start, data_end, tickers)
        else:
            pg_keys = {(r["batch_date"], r["ticker"]) for r in pg_rows}
            mongo_all = fetch_exposure_rows_mongo(db, data_start, data_end, tickers)
            mongo_rows = [
                r for r in mongo_all if (r["batch_date"], r["ticker"]) not in pg_keys
            ]

        all_exposure = merge_exposure_sources(pg_rows, mongo_rows)
        if not all_exposure:
            logger.error(
                "Sin filas de exposición (PG position_state y Mongo bayesian_reports vacíos)."
            )
            return 2

        if not tickers:
            tickers = sorted({r["ticker"] for r in all_exposure})

        trade_window_start = data_start - timedelta(days=DAYS_BACK + 7)
        trading_df = fetch_trading_signals_bulk(
            conn, trade_window_start, data_end, tickers
        )

        updated = 0
        skipped = 0
        for report_date, doc_ps in report_docs:
            ps = resolve_pipeline_start(report_date, doc_ps, data_start)
            cumulative = slice_cumulative_exposure(all_exposure, report_date, ps)
            if not cumulative:
                logger.warning("Skip %s: sin filas de position_state", report_date)
                skipped += 1
                continue

            exp_metrics, exp_diagnostics = calc_exposure_backtesting(cumulative)
            if not exp_metrics:
                logger.warning(
                    "Skip %s: sin métricas de exposición (%d filas)",
                    report_date,
                    len(cumulative),
                )
                skipped += 1
                continue

            hist_df = slice_trading_window(trading_df, report_date, ps)
            binary_metrics, binary_diag = calc_binary_backtesting(hist_df)
            benchmark = compute_benchmark(hist_df) if not hist_df.empty else {}

            patch = build_exposure_report_patch(
                exp_metrics,
                exp_diagnostics,
                binary_metrics,
                binary_diag,
                benchmark,
            )
            if update_report_mongo(db, report_date, patch, args.dry_run):
                updated += 1

        logger.info(
            "Listo: %d actualizados, %d omitidos, dry_run=%s",
            updated,
            skipped,
            args.dry_run,
        )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
