#!/usr/bin/env python3
"""Auditoría cuantitativa del pipeline TFM — solo lectura, imprime JSON/tablas."""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from shared.exposure_backtest import calc_exposure_backtesting, compute_benchmark

load_dotenv(os.path.join(ROOT, ".env"))

DB = dict(
    host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
    port=int(os.getenv("POSTGRES_PORT", 5433)),
    database=os.getenv("POSTGRES_DB", "tfm"),
    user=os.getenv("POSTGRES_USER", "tfmadmin"),
    password=os.getenv("POSTGRES_PASSWORD", "localpassword123"),
)

TICKERS = ["SPY", "IWM", "GLD", "XLE", "NVDA"]
GLOBAL_START = "2025-01-01"
GLOBAL_END = "2026-06-02"
INITIAL = 10_000.0
RISK_FREE = 0.02

PERIODS = [
    ("Q1_2025", "2025-01-01", "2025-03-31", "Q1 2025 — rally inicial"),
    ("TARIFF_CRASH", "2025-03-24", "2025-04-17", "Aranceles Abr 2025"),
    ("RECOVERY_Q2", "2025-04-18", "2025-06-30", "Recuperación Q2"),
    ("H2_2025", "2025-07-01", "2025-12-31", "H2 2025"),
    ("Y2026_H1", "2026-01-01", "2026-06-02", "2026 YTD"),
    ("FULL", GLOBAL_START, GLOBAL_END, "Pipeline completo"),
]


def pg_conn():
    return psycopg2.connect(**DB)


def fetch_signals_bulk(cur, start: str, end: str) -> List[Dict]:
    cur.execute(
        """
        SELECT ps.batch_date, ps.ticker, ps.smoothed_exposure, ps.market_regime,
               ps.prob_up, ps.target_exposure,
               ti.close_price,
               ts.exposure_recommendation, ts.signal, ts.prob_up AS ts_prob_up
        FROM position_state ps
        JOIN technical_indicators ti
          ON ps.batch_date = ti.batch_date AND ps.ticker = ti.ticker
        LEFT JOIN trading_signals ts
          ON ps.batch_date = ts.batch_date AND ps.ticker = ts.ticker
        WHERE ps.batch_date BETWEEN %s AND %s
          AND ps.ticker = ANY(%s)
        ORDER BY ps.ticker, ps.batch_date
        """,
        (start, end, TICKERS),
    )
    cols = [d[0] for d in cur.description]
    rows = []
    for r in cur.fetchall():
        d = dict(zip(cols, r))
        d["batch_date"] = str(d["batch_date"])[:10]
        rows.append(d)
    return rows


def period_metrics(rows: List[Dict], ticker: str) -> Dict[str, Any]:
    sub = [r for r in rows if r["ticker"] == ticker]
    if len(sub) < 2:
        return {}
    exp_m, exp_d = calc_exposure_backtesting(sub, initial_capital=INITIAL)
    df = pd.DataFrame(sub)
    bench = compute_benchmark(df)
    t = ticker
    strat = exp_m.get(t, {})
    bh = bench.get(t, 0.0)
    alpha = round(strat.get("cumulative_return", 0) - bh, 4)
    return {
        "strategy_return": strat.get("cumulative_return", 0),
        "buy_hold_return": bh,
        "alpha": alpha,
        "sharpe": strat.get("sharpe_ratio", 0),
        "max_drawdown": strat.get("max_drawdown", 0),
        "avg_exposure": exp_d.get(t, {}).get("avg_exposure", 0),
        "min_exposure": exp_d.get(t, {}).get("min_exposure", 0),
        "max_exposure": exp_d.get(t, {}).get("max_exposure", 0),
        "regime_distribution": exp_d.get(t, {}).get("regime_distribution", {}),
        "n_days": len(sub),
    }


def market_move(rows: List[Dict], ticker: str) -> float:
    sub = sorted([r for r in rows if r["ticker"] == ticker], key=lambda x: x["batch_date"])
    if len(sub) < 2:
        return 0.0
    p0 = float(sub[0].get("close_price") or 0)
    p1 = float(sub[-1].get("close_price") or 0)
    return round((p1 - p0) / p0, 4) if p0 else 0.0


def exposure_at(cur, d: str, ticker: str = "SPY") -> Optional[float]:
    cur.execute(
        "SELECT smoothed_exposure FROM position_state WHERE batch_date=%s AND ticker=%s",
        (d, ticker),
    )
    r = cur.fetchone()
    return float(r[0]) if r and r[0] is not None else None


def macro_row(cur, d: str) -> Dict:
    cur.execute(
        """SELECT risk_regime, macro_adjustment, vix FROM market_regime_state WHERE batch_date=%s""",
        (d,),
    )
    reg = cur.fetchone()
    cur.execute(
        """SELECT macro_sentiment, score, n_articles FROM macro_sentiment_scores WHERE batch_date=%s""",
        (d,),
    )
    sent = cur.fetchone()
    return {
        "risk_regime": reg[0] if reg else None,
        "macro_adjustment": float(reg[1]) if reg and reg[1] is not None else None,
        "vix": float(reg[2]) if reg and reg[2] is not None else None,
        "macro_sentiment": sent[0] if sent else None,
        "macro_score": float(sent[1]) if sent and sent[1] is not None else None,
        "n_articles": sent[2] if sent else None,
    }


def signal_accuracy(cur, start: str, end: str) -> Dict:
    cur.execute(
        """
        SELECT ticker,
               COUNT(*) FILTER (WHERE correct_d3 IS NOT NULL) AS labeled,
               COUNT(*) FILTER (WHERE correct_d3 = true) AS correct,
               AVG(prob_up) AS avg_prob_up
        FROM signal_outcomes
        WHERE batch_date BETWEEN %s AND %s AND ticker = ANY(%s)
        GROUP BY ticker
        """,
        (start, end, TICKERS),
    )
    out = {}
    for tk, labeled, correct, avg_p in cur.fetchall():
        out[tk] = {
            "labeled_d3": labeled,
            "accuracy_d3": round(correct / labeled, 4) if labeled else None,
            "avg_prob_up": round(float(avg_p), 4) if avg_p else None,
        }
    return out


def news_stats(cur, start: str, end: str) -> Dict:
    cur.execute(
        """
        SELECT ticker, sentiment, COUNT(*), AVG(confidence)
        FROM sentiment_scores
        WHERE batch_date BETWEEN %s AND %s AND ticker = ANY(%s)
        GROUP BY ticker, sentiment
        """,
        (start, end, TICKERS),
    )
    by_ticker: Dict[str, Dict] = defaultdict(lambda: {"total": 0, "bullish": 0, "bearish": 0, "neutral": 0})
    for tk, sent, cnt, avg_conf in cur.fetchall():
        by_ticker[tk]["total"] += cnt
        by_ticker[tk][sent] = cnt
        by_ticker[tk][f"avg_conf_{sent}"] = round(float(avg_conf), 3)
    return dict(by_ticker)


def lead_lag_crash(cur) -> Dict:
    """Exposición SPY vs retorno diario alrededor del crash de aranceles."""
    cur.execute(
        """
        SELECT ps.batch_date, ps.smoothed_exposure, ti.close_price
        FROM position_state ps
        JOIN technical_indicators ti ON ps.batch_date=ti.batch_date AND ps.ticker=ti.ticker
        WHERE ps.ticker='SPY' AND ps.batch_date BETWEEN '2025-03-01' AND '2025-04-30'
        ORDER BY ps.batch_date
        """
    )
    rows = cur.fetchall()
    series = []
    for i in range(1, len(rows)):
        bd, exp, p0 = rows[i - 1]
        _, exp1, p1 = rows[i]
        if p0 and p1:
            ret = (float(p1) - float(p0)) / float(p0)
            series.append({"date": str(bd), "exposure": float(exp1 or 0), "market_ret": round(ret, 4)})
    # Did exposure drop before worst days?
    worst = sorted(series, key=lambda x: x["market_ret"])[:5]
    exp_before_crash = [s for s in series if s["date"] <= "2025-04-02"]
    avg_exp_early = np.mean([s["exposure"] for s in exp_before_crash]) if exp_before_crash else 0
    avg_exp_late = np.mean([s["exposure"] for s in series if s["date"] >= "2025-04-07"]) if series else 0
    return {
        "avg_exposure_mar_apr_pre_apr2": round(float(avg_exp_early), 4),
        "avg_exposure_post_apr7": round(float(avg_exp_late), 4),
        "worst_5_days": worst,
    }


def main():
    conn = pg_conn()
    cur = conn.cursor()

    coverage = {}
    for tbl in [
        "position_state",
        "technical_indicators",
        "trading_signals",
        "signal_outcomes",
        "market_regime_state",
        "macro_sentiment_scores",
        "sentiment_scores",
        "batch_log",
    ]:
        cur.execute(f"SELECT MIN(batch_date), MAX(batch_date), COUNT(*) FROM {tbl}")
        coverage[tbl] = cur.fetchone()

    all_rows = fetch_signals_bulk(cur, GLOBAL_START, GLOBAL_END)

    period_results = {}
    for pid, start, end, label in PERIODS:
        sub = [r for r in all_rows if start <= r["batch_date"] <= end]
        period_results[pid] = {
            "label": label,
            "start": start,
            "end": end,
            "tickers": {t: period_metrics(sub, t) for t in TICKERS},
            "market_moves": {t: market_move(sub, t) for t in TICKERS},
            "signal_accuracy": signal_accuracy(cur, start, end),
        }

    full = period_results["FULL"]["tickers"]
    lead_lag = lead_lag_crash(cur)

    # Macro snapshots on key dates
    key_dates = [
        "2025-01-15",
        "2025-04-02",
        "2025-04-07",
        "2025-04-09",
        "2025-07-01",
        "2025-10-01",
        "2026-01-02",
        "2026-04-01",
    ]
    macro_snapshots = {d: macro_row(cur, d) for d in key_dates}

    exposures_key = {
        d: {t: exposure_at(cur, d, t) for t in TICKERS}
        for d in key_dates
    }

    news_full = news_stats(cur, GLOBAL_START, GLOBAL_END)

    # Quarterly alpha sign changes
    quarters = []
    for y in [2025, 2026]:
        for q in range(1, 5):
            if y == 2026 and q > 2:
                break
            import calendar
            from datetime import datetime

            m0 = (q - 1) * 3 + 1
            m1 = q * 3
            if y == 2026 and q == 2:
                end_d = "2026-06-02"
            else:
                end_d = f"{y}-{m1:02d}-{calendar.monthrange(y, m1)[1]:02d}"
            start_d = f"{y}-{m0:02d}-01"
            if end_d < GLOBAL_START or start_d > GLOBAL_END:
                continue
            start_d = max(start_d, GLOBAL_START)
            end_d = min(end_d, GLOBAL_END)
            sub = [r for r in all_rows if start_d <= r["batch_date"] <= end_d]
            quarters.append(
                {
                    "quarter": f"{y}Q{q}",
                    "start": start_d,
                    "end": end_d,
                    "alpha": {t: period_metrics(sub, t).get("alpha") for t in TICKERS},
                    "strategy": {t: period_metrics(sub, t).get("strategy_return") for t in TICKERS},
                    "bh": {t: period_metrics(sub, t).get("buy_hold_return") for t in TICKERS},
                }
            )

    out = {
        "coverage": {k: {"min": str(v[0]), "max": str(v[1]), "count": v[2]} for k, v in coverage.items()},
        "full_period_metrics": full,
        "periods": period_results,
        "quarters": quarters,
        "lead_lag_tariff": lead_lag,
        "macro_snapshots": macro_snapshots,
        "exposures_key_dates": exposures_key,
        "news_stats": news_full,
    }

    print(json.dumps(out, indent=2, default=str))
    cur.close()
    conn.close()


if __name__ == "__main__":
    from collections import defaultdict

    main()
