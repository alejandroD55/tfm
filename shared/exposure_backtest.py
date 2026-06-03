"""
Exposure backtesting helpers (shared by bootstrap, lambda_report, recompute scripts).
No heavy ML imports — safe to import from lightweight tooling.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

INITIAL_CAP = 10_000.0
RISK_FREE_RATE = 0.02
DAYS_BACK = 365


def calc_exposure_backtesting(signals_list: List[Dict]) -> Tuple[Dict, Dict]:
    """
    Backtesting de exposición continua: portfolio_return_t = market_return_t × smoothed_exposure_t.
    Cada fila requiere: batch_date, ticker, close_price, smoothed_exposure, market_regime.
    """
    exp_metrics: Dict = {}
    exp_diagnostics: Dict = {}

    if not signals_list:
        return exp_metrics, exp_diagnostics

    tickers = list({r["ticker"] for r in signals_list})
    for ticker in tickers:
        ts = sorted(
            [r for r in signals_list if r["ticker"] == ticker],
            key=lambda x: x["batch_date"],
        )
        if len(ts) == 1:
            exposure = float(ts[0].get("smoothed_exposure", 0.0))
            regime = ts[0].get("market_regime", "NEUTRAL")
            regime_days: Dict[str, int] = {
                "BULL": 0,
                "NEUTRAL": 0,
                "HIGH_VOL": 0,
                "BEAR": 0,
            }
            if regime in regime_days:
                regime_days[regime] += 1
            exp_metrics[ticker] = {
                "cumulative_return": 0.0,
                "sharpe_ratio": 0.0,
                "max_drawdown": 0.0,
                "final_equity": round(float(INITIAL_CAP), 2),
            }
            exp_diagnostics[ticker] = {
                "avg_exposure": round(exposure, 4),
                "min_exposure": round(exposure, 4),
                "max_exposure": round(exposure, 4),
                "avg_cash_pct": round(1.0 - exposure, 4),
                "min_cash_pct": round(1.0 - exposure, 4),
                "max_cash_pct": round(1.0 - exposure, 4),
                "regime_distribution": regime_days,
            }
            continue

        if len(ts) < 2:
            continue

        capital = INITIAL_CAP
        equity = [capital]
        daily_exposures: List[float] = []
        regime_days: Dict[str, int] = {"BULL": 0, "NEUTRAL": 0, "HIGH_VOL": 0, "BEAR": 0}

        for i in range(1, len(ts)):
            p0 = float(ts[i - 1].get("close_price") or 0)
            p1 = float(ts[i].get("close_price") or 0)
            if p0 == 0 or p1 == 0:
                equity.append(equity[-1])
                continue

            market_ret = (p1 - p0) / p0
            exposure = float(ts[i].get("smoothed_exposure", 0.0))
            daily_exposures.append(exposure)

            regime = ts[i].get("market_regime", "NEUTRAL")
            if regime in regime_days:
                regime_days[regime] += 1

            portfolio_ret = market_ret * exposure
            capital *= 1.0 + portfolio_ret
            equity.append(capital)

        final_eq = capital
        cum_ret = (final_eq - INITIAL_CAP) / INITIAL_CAP

        if len(equity) > 2:
            eq_arr = np.array(equity)
            dr = np.diff(eq_arr) / eq_arr[:-1]
            excess = dr - (RISK_FREE_RATE / 252)
            std = np.std(excess)
            sharpe = float(np.mean(excess) / std * np.sqrt(252)) if std > 1e-6 else 0.0
            peak = np.maximum.accumulate(eq_arr)
            max_dd = float(np.min((eq_arr - peak) / peak))
        else:
            sharpe = max_dd = 0.0

        exp_metrics[ticker] = {
            "cumulative_return": round(float(cum_ret), 4),
            "sharpe_ratio": round(float(sharpe), 4),
            "max_drawdown": round(float(max_dd), 4),
            "final_equity": round(float(final_eq), 2),
        }
        daily_cash = [1.0 - e for e in daily_exposures]
        exp_diagnostics[ticker] = {
            "avg_exposure": round(float(np.mean(daily_exposures)), 4) if daily_exposures else 0.0,
            "min_exposure": round(float(np.min(daily_exposures)), 4) if daily_exposures else 0.0,
            "max_exposure": round(float(np.max(daily_exposures)), 4) if daily_exposures else 1.0,
            "avg_cash_pct": round(float(np.mean(daily_cash)), 4) if daily_cash else 1.0,
            "min_cash_pct": round(float(np.min(daily_cash)), 4) if daily_cash else 0.0,
            "max_cash_pct": round(float(np.max(daily_cash)), 4) if daily_cash else 1.0,
            "regime_distribution": regime_days,
        }

    return exp_metrics, exp_diagnostics


def compute_benchmark(signals_df: pd.DataFrame) -> Dict[str, float]:
    benchmark: Dict[str, float] = {}
    if signals_df.empty:
        return benchmark
    for ticker in signals_df["ticker"].unique():
        ticker_df = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        if ticker_df.empty:
            continue
        first_price = (
            float(ticker_df.iloc[0]["close_price"])
            if ticker_df.iloc[0]["close_price"]
            else 0.0
        )
        last_price = (
            float(ticker_df.iloc[-1]["close_price"])
            if ticker_df.iloc[-1]["close_price"]
            else 0.0
        )
        buy_hold_return = (
            ((last_price - first_price) / first_price) if first_price > 0 else 0.0
        )
        benchmark[ticker] = round(float(buy_hold_return), 4)
    return benchmark


def calc_binary_backtesting(signals_df: pd.DataFrame) -> Tuple[Dict, Dict]:
    """Backtesting binario Long/Cash (para exposure_vs_binary_comparison)."""
    metrics, diagnostics = {}, {}
    if signals_df.empty:
        return metrics, diagnostics

    for ticker in signals_df["ticker"].unique():
        ts = signals_df[signals_df["ticker"] == ticker].sort_values("batch_date")
        capital = INITIAL_CAP
        equity = [capital]

        current_position = 1
        if len(ts) > 0 and pd.notna(ts.iloc[0]["close_price"]):
            entry_p = float(ts.iloc[0]["close_price"])
        else:
            entry_p = 0.0
            current_position = 0

        trades_rets = []
        days_invested = 0
        signals_count = ts["signal"].value_counts().to_dict()

        for _, row in ts.iterrows():
            price = float(row["close_price"]) if row["close_price"] else 0.0
            if price == 0:
                continue
            sig = row["signal"]

            if sig == "BUY" and current_position == 0:
                current_position = 1
                entry_p = price
            elif sig == "SELL" and current_position == 1:
                ret = (price - entry_p) / entry_p
                capital *= 1 + ret
                trades_rets.append(float(ret))
                current_position = 0

            if current_position == 1:
                days_invested += 1

            daily_eq = (
                capital * (1 + (price - entry_p) / entry_p)
                if current_position == 1 and entry_p > 0
                else capital
            )
            equity.append(daily_eq)

        final_eq = capital
        if current_position == 1 and entry_p > 0:
            last_p = float(ts.iloc[-1]["close_price"])
            final_eq = capital * (1 + (last_p - entry_p) / entry_p)

        cum_ret = (final_eq - INITIAL_CAP) / INITIAL_CAP
        if len(equity) > 2:
            dr = np.diff(equity) / np.array(equity[:-1])
            excess = dr - (RISK_FREE_RATE / 252)
            std = np.std(excess)
            sharpe = float(np.mean(excess) / std * np.sqrt(252)) if std > 1e-6 else 0.0
            peak = np.maximum.accumulate(equity)
            max_dd = float(np.min((np.array(equity) - peak) / peak))
        else:
            sharpe = max_dd = 0.0

        metrics[ticker] = {
            "cumulative_return": round(float(cum_ret), 4),
            "sharpe_ratio": round(float(sharpe), 4),
            "max_drawdown": round(float(max_dd), 4),
            "final_equity": round(float(final_eq), 2),
        }

        wins = sum(1 for value in trades_rets if value > 0)
        gross_profit = sum(value for value in trades_rets if value > 0)
        gross_loss = abs(sum(value for value in trades_rets if value < 0))
        profit_factor = (
            (gross_profit / gross_loss)
            if gross_loss > 1e-9
            else (gross_profit if gross_profit > 0 else 0.0)
        )

        diagnostics[ticker] = {
            "signals": {
                "BUY": int(signals_count.get("BUY", 0)),
                "SELL": int(signals_count.get("SELL", 0)),
                "HOLD": int(signals_count.get("HOLD", 0)),
            },
            "trades_closed": len(trades_rets),
            "win_rate": (
                round(float(wins / len(trades_rets)), 4) if trades_rets else 0.0
            ),
            "avg_trade_return": (
                round(float(np.mean(trades_rets)), 4) if trades_rets else 0.0
            ),
            "profit_factor": round(float(profit_factor), 4),
            "time_in_market_ratio": round(float(days_invested / max(len(ts), 1)), 4),
        }
    return metrics, diagnostics


def build_exposure_report_patch(
    exp_metrics: Dict,
    exp_diagnostics: Dict,
    binary_metrics: Dict,
    binary_diagnostics: Dict,
    benchmark: Dict[str, float],
) -> Dict:
    """Campos a $set en Mongo reports (alineado con bootstrap_365_days)."""
    benchmark_comparison = {
        t: {
            "strategy_cumulative_return": exp_metrics.get(t, {}).get("cumulative_return", 0.0),
            "buy_hold_cumulative_return": benchmark.get(t, 0.0),
            "alpha_vs_benchmark": round(
                exp_metrics.get(t, {}).get("cumulative_return", 0.0) - benchmark.get(t, 0.0),
                4,
            ),
        }
        for t in exp_metrics
    }
    exposure_vs_binary = {
        t: {
            "binary_cumulative_return": binary_metrics.get(t, {}).get("cumulative_return", 0.0),
            "exposure_cumulative_return": exp_metrics.get(t, {}).get("cumulative_return", 0.0),
            "exposure_alpha": round(
                exp_metrics.get(t, {}).get("cumulative_return", 0.0)
                - binary_metrics.get(t, {}).get("cumulative_return", 0.0),
                4,
            ),
            "avg_exposure": exp_diagnostics.get(t, {}).get("avg_exposure", 0.0),
            "regime_distribution": exp_diagnostics.get(t, {}).get("regime_distribution", {}),
        }
        for t in set(list(binary_metrics.keys()) + list(exp_metrics.keys()))
    }
    summary = {
        "total_tickers": len(exp_metrics),
        "avg_cumulative_return": (
            round(np.mean([m["cumulative_return"] for m in exp_metrics.values()]), 4)
            if exp_metrics
            else 0
        ),
        "avg_sharpe_ratio": (
            round(np.mean([m["sharpe_ratio"] for m in exp_metrics.values()]), 4)
            if exp_metrics
            else 0
        ),
        "avg_max_drawdown": (
            round(np.mean([m["max_drawdown"] for m in exp_metrics.values()]), 4)
            if exp_metrics
            else 0
        ),
        "total_closed_trades": (
            sum(item.get("trades_closed", 0) for item in binary_diagnostics.values())
            if binary_diagnostics
            else 0
        ),
    }
    backtesting_config = {
        "initial_capital": INITIAL_CAP,
        "risk_free_rate": RISK_FREE_RATE,
        "period_days": DAYS_BACK,
        "strategy_type": "Probabilistic Exposure",
        "sharpe_annualized": True,
        "limitation": (
            "El backtesting asume ejecución al cierre. Estrategia de exposición continua: "
            "portfolio_return = market_return × smoothed_exposure. Arranca en 0% invertido."
        ),
    }
    return {
        "exposure_backtesting_metrics": exp_metrics,
        "exposure_backtesting_diagnostics": exp_diagnostics,
        "benchmark_comparison": benchmark_comparison,
        "exposure_vs_binary_comparison": exposure_vs_binary,
        "summary": summary,
        "backtesting_config": backtesting_config,
    }
