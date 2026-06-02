"""
Decision-neutral quantitative observability helpers for the Bayesian engine.

The functions in this module only measure persisted or freshly computed
behavior. They do not change thresholds, signals, positions, or model inputs.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional


EVIDENCE_FEATURES = ("Sentiment", "RSI", "Trend", "Volatility")
SIGNALS = ("BUY", "HOLD", "SELL")
EXPOSURE_ACTIONS = ("INCREASE", "MAINTAIN", "DECREASE")


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        return None if math.isnan(out) else out
    except Exception:
        return None


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _round(value: Any, ndigits: int = 4) -> Optional[float]:
    value_f = _float_or_none(value)
    return round(value_f, ndigits) if value_f is not None else None


def _date_str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value)[:10]


def _month_key(value: Any) -> str:
    value_s = _date_str(value)
    return value_s[:7] if len(value_s) >= 7 else "unknown"


def _coerce_probability(value: Any) -> Optional[float]:
    if isinstance(value, dict):
        value = _first_not_none(value.get("prob_up"), value.get("final_prob_up"))
    elif isinstance(value, (tuple, list)) and value:
        value = value[0]
    value_f = _float_or_none(value)
    if value_f is None:
        return None
    return max(0.0, min(1.0, value_f))


def _summary(values: Iterable[float]) -> Dict[str, Any]:
    vals = [float(v) for v in values if _float_or_none(v) is not None]
    if not vals:
        return {"count": 0, "avg": None, "min": None, "max": None}
    return {
        "count": len(vals),
        "avg": round(sum(vals) / len(vals), 4),
        "min": round(min(vals), 4),
        "max": round(max(vals), 4),
    }


def _runs(values: List[str]) -> List[Dict[str, Any]]:
    if not values:
        return []
    out = []
    current = values[0]
    length = 1
    for value in values[1:]:
        if value == current:
            length += 1
        else:
            out.append({"signal": current, "duration": length})
            current = value
            length = 1
    out.append({"signal": current, "duration": length})
    return out


def compute_contribution_analysis(
    evidence_states: Dict[str, Any],
    probability_fn: Callable[[Dict[str, Any]], Any],
    no_macro_probability_fn: Optional[Callable[[Dict[str, Any]], Any]] = None,
    feature_names: Iterable[str] = EVIDENCE_FEATURES,
) -> Dict[str, Any]:
    """
    Leave-one-evidence-out decomposition around the current inference.

    probability_fn must return the same adjusted prob_up that the production
    inference path uses. The helper calls it with reduced evidence only for
    attribution, never for signal decisions.
    """
    clean_evidence = {
        k: v for k, v in (evidence_states or {}).items() if v is not None
    }
    final_prob = _coerce_probability(probability_fn(dict(clean_evidence)))
    prior_prob = _coerce_probability(probability_fn({}))
    effects: Dict[str, Any] = {}

    for feature in feature_names:
        key = str(feature).lower()
        if feature not in clean_evidence:
            effects[key] = {
                "applicable": False,
                "reason": "evidence_not_present",
            }
            continue
        reduced = dict(clean_evidence)
        evidence_value = reduced.pop(feature, None)
        without_prob = _coerce_probability(probability_fn(reduced))
        effects[key] = {
            "applicable": True,
            "evidence": evidence_value,
            "without_prob_up": _round(without_prob),
            "delta_prob_up": (
                _round(final_prob - without_prob)
                if final_prob is not None and without_prob is not None
                else None
            ),
        }

    if no_macro_probability_fn is not None:
        without_macro_prob = _coerce_probability(
            no_macro_probability_fn(dict(clean_evidence))
        )
        effects["macro"] = {
            "applicable": True,
            "without_prob_up": _round(without_macro_prob),
            "delta_prob_up": (
                _round(final_prob - without_macro_prob)
                if final_prob is not None and without_macro_prob is not None
                else None
            ),
        }

    return {
        "method": "leave_one_evidence_out",
        "base_prob": _round(final_prob),
        "prior_prob_up": _round(prior_prob),
        "effects": effects,
        "final_prob_up": _round(final_prob),
        "notes": (
            "Attribution is an approximation computed by rerunning inference "
            "with one evidence variable omitted. It is not used for decisions."
        ),
    }


def normalize_signal_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize rows from Aurora queries or Mongo bayesian_reports."""
    normalized = []
    for row in rows or []:
        raw_values = row.get("raw_values") or {}
        inference = row.get("inference") or {}
        discretization = row.get("discretization") or {}
        close_price = (
            row.get("close_price")
            if row.get("close_price") is not None
            else raw_values.get("close_price")
        )
        bb_width = raw_values.get("bb_width_ratio")
        if bb_width is None:
            upper = row.get("bb_upper")
            lower = row.get("bb_lower")
            close = _float_or_none(close_price)
            if upper is not None and lower is not None and close:
                bb_width = (float(upper) - float(lower)) / close
        prob_up = row.get("prob_up")
        if prob_up is None:
            prob_up = inference.get("prob_up")
        recommendation = (
            row.get("exposure_recommendation")
            or inference.get("exposure_recommendation")
            or "MAINTAIN"
        )
        signal = (
            "BUY" if recommendation in ("INCREASE_STRONG", "INCREASE_MILD")
            else "SELL" if recommendation in ("REDUCE_STRONG", "REDUCE_MILD")
            else "HOLD"
        )
        ticker = row.get("ticker")
        if not ticker:
            continue
        if str(signal).upper() == "BUY":
            exposure_action = "INCREASE"
        elif str(signal).upper() == "SELL":
            exposure_action = "DECREASE"
        else:
            exposure_action = "MAINTAIN"

        exposure_constraints = row.get("exposure_constraints") or {}
        normalized.append(
            {
                "batch_date": _date_str(row.get("batch_date") or row.get("date")),
                "ticker": str(ticker).upper(),
                "signal": str(signal).upper(),
                "prob_up": _float_or_none(prob_up),
                "prob_down": _float_or_none(_first_not_none(row.get("prob_down"), inference.get("prob_down"))),
                "close_price": _float_or_none(close_price),
                "rsi_14": _float_or_none(_first_not_none(row.get("rsi_14"), raw_values.get("rsi_14"))),
                "sma_20": _float_or_none(_first_not_none(row.get("sma_20"), raw_values.get("sma_20"))),
                "sma_50": _float_or_none(_first_not_none(row.get("sma_50"), raw_values.get("sma_50"))),
                "sma_200": _float_or_none(_first_not_none(row.get("sma_200"), raw_values.get("sma_200"))),
                "bb_width": _float_or_none(_first_not_none(row.get("bb_width"), bb_width)),
                "sentiment_state": row.get("sentiment_state") or discretization.get("sentiment_state"),
                "rsi_state": row.get("rsi_state") or discretization.get("rsi_state"),
                "trend_state": row.get("trend_state") or discretization.get("trend_state"),
                "volatility_state": row.get("volatility_state") or discretization.get("volatility_state"),
                "exposure_action": exposure_action,
                "smoothed_exposure_input": _float_or_none(
                    _first_not_none(
                        row.get("smoothed_exposure_input"),
                        exposure_constraints.get("smoothed_exposure_input"),
                    )
                ),
                "constrained_exposure": _float_or_none(
                    _first_not_none(
                        row.get("constrained_exposure"),
                        exposure_constraints.get("constrained_exposure"),
                    )
                ),
                "regime_ceiling": _float_or_none(
                    _first_not_none(
                        row.get("regime_ceiling"),
                        exposure_constraints.get("regime_ceiling"),
                    )
                ),
                "fundamental_cap": _float_or_none(
                    _first_not_none(
                        row.get("fundamental_cap"),
                        exposure_constraints.get("fundamental_cap"),
                    )
                ),
                "catalyst_penalty": _float_or_none(
                    _first_not_none(
                        row.get("catalyst_penalty"),
                        exposure_constraints.get("catalyst_penalty"),
                    )
                ),
            }
        )
    normalized.sort(key=lambda x: (x["ticker"], x["batch_date"]))
    return normalized


def normalize_outcome_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in rows or []:
        ticker = row.get("ticker")
        if not ticker:
            continue
        recommendation = (
            row.get("exposure_recommendation")
            or "MAINTAIN"
        )
        signal = (
            "BUY" if recommendation in ("INCREASE_STRONG", "INCREASE_MILD")
            else "SELL" if recommendation in ("REDUCE_STRONG", "REDUCE_MILD")
            else "HOLD"
        )
        out.append(
            {
                "batch_date": _date_str(row.get("batch_date")),
                "ticker": str(ticker).upper(),
                "signal": signal,
                "exposure_recommendation": recommendation,
                "prob_up": _float_or_none(row.get("prob_up")),
                "outcome_d1": row.get("outcome_d1"),
                "outcome_d3": row.get("outcome_d3"),
                "outcome_d5": row.get("outcome_d5"),
                "correct_d1": row.get("correct_d1"),
                "correct_d3": row.get("correct_d3"),
                "correct_d5": row.get("correct_d5"),
            }
        )
    return out


def _group_by_ticker(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["ticker"]].append(row)
    for ticker in grouped:
        grouped[ticker].sort(key=lambda x: x["batch_date"])
    return dict(grouped)


def compute_calibration_report(
    outcome_rows: Iterable[Dict[str, Any]],
    bucket_size: float = 0.05,
    outcome_field: str = "outcome_d1",
) -> Dict[str, Any]:
    rows = normalize_outcome_rows(outcome_rows)
    usable = [
        r for r in rows
        if r.get(outcome_field) in ("UP", "DOWN", "FLAT")
        and _float_or_none(r.get("prob_up")) is not None
    ]
    if not usable:
        return {
            "status": "insufficient_data",
            "horizon": outcome_field,
            "bucket_size": bucket_size,
            "sample_size": 0,
            "reliability_table": [],
            "brier_score": None,
            "expected_calibration_error": None,
        }

    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    brier_terms = []
    for row in usable:
        prob = max(0.0, min(1.0, float(row["prob_up"])))
        observed = 1.0 if row.get(outcome_field) == "UP" else 0.0
        brier_terms.append((prob - observed) ** 2)
        lo = min(1.0 - bucket_size, math.floor(prob / bucket_size) * bucket_size)
        hi = min(1.0, lo + bucket_size)
        label = f"{lo:.2f}-{hi:.2f}"
        buckets[label].append({"prob": prob, "observed": observed})

    reliability_table = []
    ece = 0.0
    total = len(usable)
    for label in sorted(buckets):
        vals = buckets[label]
        avg_pred = sum(v["prob"] for v in vals) / len(vals)
        realized = sum(v["observed"] for v in vals) / len(vals)
        abs_error = abs(avg_pred - realized)
        ece += (len(vals) / total) * abs_error
        reliability_table.append(
            {
                "bucket": label,
                "count": len(vals),
                "avg_predicted_prob": round(avg_pred, 4),
                "realized_up_frequency": round(realized, 4),
                "abs_calibration_error": round(abs_error, 4),
            }
        )

    return {
        "status": "ok",
        "horizon": outcome_field,
        "bucket_size": bucket_size,
        "sample_size": total,
        "reliability_table": reliability_table,
        "brier_score": round(sum(brier_terms) / total, 6),
        "expected_calibration_error": round(ece, 6),
    }


def compute_transition_report(signal_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = normalize_signal_rows(signal_rows)
    grouped = _group_by_ticker(rows)
    by_ticker = {}
    all_holding_periods: List[int] = []
    all_turnovers = []
    total_buy_sell = 0
    total_hold_followed_by_hold = 0
    total_hold_with_next = 0
    run_lengths_by_signal: Dict[str, List[int]] = {s: [] for s in SIGNALS}
    run_lengths_by_action: Dict[str, List[int]] = {a: [] for a in EXPOSURE_ACTIONS}
    all_exposure_changes: List[float] = []

    for ticker, ticker_rows in grouped.items():
        signals = [r["signal"] for r in ticker_rows]
        changes = sum(1 for i in range(1, len(signals)) if signals[i] != signals[i - 1])
        intervals = max(len(signals) - 1, 0)
        turnover = changes / intervals if intervals else 0.0
        buy_sell = sum(
            1 for i in range(1, len(signals))
            if signals[i - 1] == "BUY" and signals[i] == "SELL"
        )
        hold_next = sum(1 for i in range(0, max(len(signals) - 1, 0)) if signals[i] == "HOLD")
        hold_hold = sum(
            1 for i in range(1, len(signals))
            if signals[i - 1] == "HOLD" and signals[i] == "HOLD"
        )

        position = 1
        current_hold = 0
        holding_periods: List[int] = []
        exposure_days = 0
        for row in ticker_rows:
            sig = row["signal"]
            if sig == "BUY":
                if position == 0:
                    current_hold = 0
                position = 1
            elif sig == "SELL":
                if position == 1 and current_hold > 0:
                    holding_periods.append(current_hold)
                position = 0
                current_hold = 0
            if position == 1:
                exposure_days += 1
                current_hold += 1
        if position == 1 and current_hold > 0:
            holding_periods.append(current_hold)

        runs = _runs(signals)
        avg_consecutive = {}
        for sig in SIGNALS:
            lens = [r["duration"] for r in runs if r["signal"] == sig]
            run_lengths_by_signal[sig].extend(lens)
            avg_consecutive[sig] = _summary(lens)

        actions = [str(r.get("exposure_action") or "MAINTAIN").upper() for r in ticker_rows]
        action_runs = _runs(actions)
        for action in EXPOSURE_ACTIONS:
            run_lengths_by_action[action].extend(
                [r["duration"] for r in action_runs if r["signal"] == action]
            )

        constrained = [
            _float_or_none(r.get("constrained_exposure")) for r in ticker_rows
        ]
        constrained = [float(v) for v in constrained if v is not None]
        ticker_exposure_changes: List[float] = []
        if len(constrained) >= 2:
            ticker_exposure_changes = [
                constrained[i] - constrained[i - 1] for i in range(1, len(constrained))
            ]
            all_exposure_changes.extend(ticker_exposure_changes)

        all_holding_periods.extend(holding_periods)
        all_turnovers.append(turnover)
        total_buy_sell += buy_sell
        total_hold_followed_by_hold += hold_hold
        total_hold_with_next += hold_next
        by_ticker[ticker] = {
            "observations": len(ticker_rows),
            "average_holding_period_days": _summary(holding_periods)["avg"],
            "exposure_ratio": round(exposure_days / len(ticker_rows), 4) if ticker_rows else 0.0,
            "signal_turnover": round(turnover, 4),
            "recommendation_turnover": (
                round(
                    sum(1 for i in range(1, len(actions)) if actions[i] != actions[i - 1])
                    / max(len(actions) - 1, 1),
                    4,
                )
                if actions
                else 0.0
            ),
            "buy_to_sell_transitions": buy_sell,
            "hold_persistence": round(hold_hold / hold_next, 4) if hold_next else 0.0,
            "average_consecutive_signal_durations": avg_consecutive,
            "avg_exposure_change_step": _summary(ticker_exposure_changes)["avg"],
        }

    return {
        "status": "ok" if rows else "insufficient_data",
        "sample_size": len(rows),
        "average_holding_period_days": _summary(all_holding_periods)["avg"],
        "exposure_ratio": (
            round(sum(v["exposure_ratio"] for v in by_ticker.values()) / len(by_ticker), 4)
            if by_ticker else 0.0
        ),
        "signal_turnover": round(sum(all_turnovers) / len(all_turnovers), 4) if all_turnovers else 0.0,
        "recommendation_action_durations": {
            action: _summary(run_lengths_by_action[action]) for action in EXPOSURE_ACTIONS
        },
        "avg_exposure_change_step": _summary(all_exposure_changes)["avg"],
        "abs_exposure_change_step": _summary([abs(x) for x in all_exposure_changes])["avg"],
        "buy_to_sell_transitions": total_buy_sell,
        "hold_persistence": (
            round(total_hold_followed_by_hold / total_hold_with_next, 4)
            if total_hold_with_next else 0.0
        ),
        "average_consecutive_signal_durations": {
            sig: _summary(run_lengths_by_signal[sig]) for sig in SIGNALS
        },
        "by_ticker": by_ticker,
    }


def _percentile(values: List[float], pct: float) -> Optional[float]:
    vals = sorted(values)
    if not vals:
        return None
    idx = (len(vals) - 1) * pct
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return vals[int(idx)]
    return vals[lo] * (hi - idx) + vals[hi] * (idx - lo)


def _regime_metrics(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    returns = [x["strategy_return"] for x in items]
    bench = [x["benchmark_return"] for x in items]
    if not items:
        return {
            "observations": 0,
            "cumulative_return": 0.0,
            "sharpe": 0.0,
            "drawdown": 0.0,
            "alpha": 0.0,
            "win_rate": 0.0,
            "exposure_ratio": 0.0,
        }
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    for ret in returns:
        equity *= 1.0 + ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0)
    bench_equity = 1.0
    for ret in bench:
        bench_equity *= 1.0 + ret
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = math.sqrt(variance)
    sharpe = (mean / std * math.sqrt(252)) if std > 1e-9 else 0.0
    wins = sum(1 for r in returns if r > 0)
    return {
        "observations": len(items),
        "cumulative_return": round(equity - 1.0, 4),
        "sharpe": round(sharpe, 4),
        "drawdown": round(max_dd, 4),
        "alpha": round((equity - 1.0) - (bench_equity - 1.0), 4),
        "win_rate": round(wins / len(items), 4),
        "exposure_ratio": round(sum(x["position"] for x in items) / len(items), 4),
    }


def compute_market_regime_report(signal_rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = normalize_signal_rows(signal_rows)
    grouped = _group_by_ticker(rows)
    regime_items: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    by_ticker: Dict[str, Any] = {}

    for ticker, ticker_rows in grouped.items():
        closes = [r["close_price"] for r in ticker_rows]
        returns = []
        for i in range(len(ticker_rows)):
            if i == 0 or not closes[i] or not closes[i - 1]:
                returns.append(0.0)
            else:
                returns.append((closes[i] / closes[i - 1]) - 1.0)
        rolling_vols = []
        for i in range(len(returns)):
            if i < 20:
                rolling_vols.append(None)
                continue
            window = returns[i - 19:i + 1]
            mean = sum(window) / len(window)
            rolling_vols.append(math.sqrt(sum((r - mean) ** 2 for r in window) / len(window)))
        vol_threshold = _percentile([v for v in rolling_vols if v is not None], 0.75)

        position = 1
        ticker_regime_items: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for i, row in enumerate(ticker_rows):
            if i > 0:
                prev_signal = ticker_rows[i - 1]["signal"]
                if prev_signal == "BUY":
                    position = 1
                elif prev_signal == "SELL":
                    position = 0

            close_window_50 = [c for c in closes[max(0, i - 49):i + 1] if c is not None]
            close_window_200 = [c for c in closes[max(0, i - 199):i + 1] if c is not None]
            sma_50 = row.get("sma_50") or (sum(close_window_50) / 50 if len(close_window_50) == 50 else None)
            sma_200 = row.get("sma_200") or (sum(close_window_200) / 200 if len(close_window_200) == 200 else None)
            bb_width = row.get("bb_width")
            high_vol = bool(bb_width is not None and bb_width > 0.05)
            if not high_vol and vol_threshold is not None and rolling_vols[i] is not None:
                high_vol = rolling_vols[i] >= vol_threshold
            if high_vol:
                regime = "HIGH_VOLATILITY"
            elif sma_50 is not None and sma_200 is not None and sma_50 > sma_200:
                regime = "BULL"
            elif sma_50 is not None and sma_200 is not None and sma_50 < sma_200:
                regime = "BEAR"
            else:
                regime = "SIDEWAYS"

            item = {
                "strategy_return": returns[i] * position,
                "benchmark_return": returns[i],
                "position": position,
            }
            regime_items[regime].append(item)
            ticker_regime_items[regime].append(item)

        by_ticker[ticker] = {
            regime: _regime_metrics(items)
            for regime, items in sorted(ticker_regime_items.items())
        }

    return {
        "status": "ok" if rows else "insufficient_data",
        "classification": {
            "trend": "SMA50 > SMA200 => BULL; SMA50 < SMA200 => BEAR; otherwise SIDEWAYS",
            "volatility": "BB width > 0.05 or top-quartile rolling close volatility => HIGH_VOLATILITY",
            "usage": "Observability only; not fed into trading decisions.",
        },
        "sample_size": len(rows),
        "performance_by_regime": {
            regime: _regime_metrics(items)
            for regime, items in sorted(regime_items.items())
        },
        "by_ticker": by_ticker,
    }


def compute_signal_stability_report(
    signal_rows: Iterable[Dict[str, Any]],
    buy_threshold: float = 0.52,
    sell_threshold: float = 0.28,
) -> Dict[str, Any]:
    rows = normalize_signal_rows(signal_rows)
    grouped = _group_by_ticker(rows)
    changes_per_month: Counter[str] = Counter()
    all_run_lengths: List[int] = []
    threshold_distances: List[float] = []
    exposure_edge_distances: List[float] = []
    whipsaws = 0

    by_ticker = {}
    for ticker, ticker_rows in grouped.items():
        signals = [r["signal"] for r in ticker_rows]
        ticker_changes = 0
        ticker_whipsaws = 0
        for i in range(1, len(signals)):
            if signals[i] != signals[i - 1]:
                ticker_changes += 1
                changes_per_month[_month_key(ticker_rows[i]["batch_date"])] += 1
        for i in range(2, len(signals)):
            if (
                signals[i] == signals[i - 2]
                and signals[i] != signals[i - 1]
                and signals[i] in ("BUY", "SELL")
            ):
                ticker_whipsaws += 1
        runs = _runs(signals)
        run_lengths = [r["duration"] for r in runs]
        all_run_lengths.extend(run_lengths)
        probs = [r["prob_up"] for r in ticker_rows if r.get("prob_up") is not None]
        distances = [
            min(abs(float(p) - buy_threshold), abs(float(p) - sell_threshold))
            for p in probs
        ]
        exposure_distances = [
            min(abs(float(p) - 0.30), abs(float(p) - 0.75))
            for p in probs
        ]
        threshold_distances.extend(distances)
        exposure_edge_distances.extend(exposure_distances)
        whipsaws += ticker_whipsaws
        by_ticker[ticker] = {
            "signal_changes": ticker_changes,
            "signal_changes_per_month": dict(
                Counter(
                    _month_key(ticker_rows[i]["batch_date"])
                    for i in range(1, len(ticker_rows))
                    if ticker_rows[i]["signal"] != ticker_rows[i - 1]["signal"]
                )
            ),
            "signal_duration_summary": _summary(run_lengths),
            "whipsaw_count": ticker_whipsaws,
            "whipsaw_frequency": round(ticker_whipsaws / max(len(ticker_rows), 1), 4),
            "distance_to_thresholds": _summary(distances),
            "distance_to_exposure_edges": _summary(exposure_distances),
            "near_threshold_2pct": round(sum(1 for d in distances if d <= 0.02) / len(distances), 4) if distances else 0.0,
        }

    return {
        "status": "ok" if rows else "insufficient_data",
        "sample_size": len(rows),
        "signal_changes_per_month": dict(sorted(changes_per_month.items())),
        "signal_duration_summary": _summary(all_run_lengths),
        "whipsaw_frequency": round(whipsaws / max(len(rows), 1), 4) if rows else 0.0,
        "whipsaw_count": whipsaws,
        "distance_to_thresholds": _summary(threshold_distances),
        "distance_to_exposure_edges": _summary(exposure_edge_distances),
        "near_threshold_2pct": (
            round(sum(1 for d in threshold_distances if d <= 0.02) / len(threshold_distances), 4)
            if threshold_distances else 0.0
        ),
        "near_threshold_5pct": (
            round(sum(1 for d in threshold_distances if d <= 0.05) / len(threshold_distances), 4)
            if threshold_distances else 0.0
        ),
        "by_ticker": by_ticker,
    }


def compute_probability_distribution_report(
    signal_rows: Iterable[Dict[str, Any]],
    bin_size: float = 0.05,
) -> Dict[str, Any]:
    rows = normalize_signal_rows(signal_rows)
    probs = [float(r["prob_up"]) for r in rows if r.get("prob_up") is not None]
    if not probs:
        return {
            "status": "insufficient_data",
            "sample_size": 0,
            "histogram": [],
            "concentration_near_0_5": {},
            "extremes": {},
            "skewness": None,
            "approximate_entropy": None,
        }

    bucket_count = int(round(1.0 / bin_size))
    counts = [0 for _ in range(bucket_count)]
    for prob in probs:
        idx = min(bucket_count - 1, int(max(0.0, min(1.0, prob)) / bin_size))
        counts[idx] += 1
    histogram = []
    for i, count in enumerate(counts):
        lo = i * bin_size
        hi = min(1.0, lo + bin_size)
        histogram.append(
            {
                "bucket": f"{lo:.2f}-{hi:.2f}",
                "count": count,
                "frequency": round(count / len(probs), 4),
            }
        )

    mean = sum(probs) / len(probs)
    variance = sum((p - mean) ** 2 for p in probs) / len(probs)
    std = math.sqrt(variance)
    skewness = (
        sum(((p - mean) / std) ** 3 for p in probs) / len(probs)
        if std > 1e-12 else 0.0
    )
    entropy = 0.0
    non_empty = 0
    for count in counts:
        if count:
            non_empty += 1
            p = count / len(probs)
            entropy -= p * math.log(p)
    normalized_entropy = entropy / math.log(bucket_count) if bucket_count > 1 else 0.0

    by_signal = Counter(r["signal"] for r in rows)
    by_action = Counter(r.get("exposure_action", "MAINTAIN") for r in rows)
    return {
        "status": "ok",
        "sample_size": len(probs),
        "histogram": histogram,
        "concentration_near_0_5": {
            "within_2pct": round(sum(1 for p in probs if abs(p - 0.5) <= 0.02) / len(probs), 4),
            "within_5pct": round(sum(1 for p in probs if abs(p - 0.5) <= 0.05) / len(probs), 4),
        },
        "extremes": {
            "prob_le_0_10_or_ge_0_90": round(sum(1 for p in probs if p <= 0.10 or p >= 0.90) / len(probs), 4),
            "prob_le_0_20_or_ge_0_80": round(sum(1 for p in probs if p <= 0.20 or p >= 0.80) / len(probs), 4),
        },
        "prob_up_summary": _summary(probs),
        "signal_distribution": dict(by_signal),
        "recommendation_distribution": dict(by_action),
        "skewness": round(skewness, 6),
        "approximate_entropy": round(entropy, 6),
        "normalized_entropy": round(normalized_entropy, 6),
        "non_empty_bins": non_empty,
    }


def compute_quant_audit_report(
    report_date: str,
    signal_rows: Iterable[Dict[str, Any]],
    outcome_rows: Optional[Iterable[Dict[str, Any]]] = None,
    model_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rows = normalize_signal_rows(signal_rows)
    outcomes = normalize_outcome_rows(outcome_rows or [])
    thresholds = (model_config or {}).get("signal_thresholds", {})
    buy_threshold = float((thresholds.get("BUY") or {}).get("prob_up_above", 0.52))
    sell_threshold = float((thresholds.get("SELL") or {}).get("prob_up_below", 0.28))
    transition_report = compute_transition_report(rows)
    stability_report = compute_signal_stability_report(
        rows,
        buy_threshold=buy_threshold,
        sell_threshold=sell_threshold,
    )
    probability_distribution_report = compute_probability_distribution_report(rows)
    return {
        "schema_version": "1.1",
        "report_date": report_date,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source": "observability_only",
        "decision_policy": (
            "Observability only: CPT and thresholds are unchanged. "
            "BUY/HOLD/SELL are interpreted as exposure recommendations "
            "(increase/maintain/decrease) over portfolio risk allocation."
        ),
        "signal_to_exposure_mapping": {
            "BUY": "INCREASE_EXPOSURE",
            "HOLD": "MAINTAIN_EXPOSURE",
            "SELL": "DECREASE_EXPOSURE",
            "probability_edges_reference": {"low_edge": 0.30, "high_edge": 0.75},
        },
        "sample_size": len(rows),
        "calibration_report": compute_calibration_report(outcomes),
        "transition_report": transition_report,
        "recommendation_transition_report": transition_report,
        "market_regime_report": compute_market_regime_report(rows),
        "signal_stability_report": stability_report,
        "recommendation_stability_report": stability_report,
        "probability_distribution_report": probability_distribution_report,
        "recommendation_distribution_report": probability_distribution_report,
    }
