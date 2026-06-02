# deploy: 2026-05-12 18:03 UTC
import json
import boto3
import psycopg2
import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secrets_client = boto3.client("secretsmanager")
rds_client = boto3.client("rds")

try:
    from mongo_utils import upsert_report as _mongo_upsert_report

    logger.info("mongo_utils (report) cargado")
except ImportError:
    _mongo_upsert_report = None
    logger.warning("mongo_utils no disponible en lambda_report")

try:
    from mongo_utils import upsert_quant_audit_report as _mongo_upsert_quant_audit_report
except ImportError:
    _mongo_upsert_quant_audit_report = None
    logger.warning("mongo_utils quant_audit no disponible en lambda_report")

try:
    from quant_observability import compute_quant_audit_report
except ImportError:
    compute_quant_audit_report = None

# --- CONFIGURACIÓN GLOBAL ---
DAYS_BACK = 365
MODEL_AUDIT_CONFIG = {
    "signal_thresholds": {
        "BUY": {"prob_up_above": 0.52},
        "SELL": {"prob_up_below": 0.28},
    }
}


def classify_exposure_recommendation_from_prob(prob_up: float) -> str:
    """Clasificación en 5 niveles de exposición a partir de prob_up."""
    t = (float(prob_up) - 0.30) / (0.75 - 0.30)
    t = max(0.0, min(1.0, t))
    pct = (0.50 + t * (0.85 - 0.50)) * 100.0
    if pct >= 75:
        return "INCREASE_STRONG"
    if pct >= 62:
        return "INCREASE_MILD"
    if pct >= 52 and float(prob_up) >= 0.48:
        return "MAINTAIN"
    if pct >= 50:
        return "REDUCE_MILD"
    return "REDUCE_STRONG"


def resolve_batch_date(event):
    raw_date = (event or {}).get("batch_date") or (event or {}).get("date")
    if raw_date:
        return raw_date[:10]
    return datetime.now().strftime("%Y-%m-%d")


def resolve_pipeline_context(event):
    pipeline_ctx = (
        (event or {}).get("pipeline_context", {}) if isinstance(event, dict) else {}
    )
    request = pipeline_ctx.get("request", {}) if isinstance(pipeline_ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}
    batch_date = (
        resolve_batch_date(request)
        if request.get("batch_date")
        else resolve_batch_date(pipeline_ctx)
    )
    run_id = (
        pipeline_ctx.get("run_id")
        or (event or {}).get("run_id")
        or f"legacy-{batch_date}"
    )
    trigger_type = request.get("trigger_type")
    if trigger_type not in ("manual", "scheduled"):
        trigger_type = (
            "manual" if request.get("ticker") or request.get("tickers") else "scheduled"
        )
    return {"batch_date": batch_date, "run_id": run_id, "trigger_type": trigger_type}


def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        return json.loads(response["SecretBinary"])
    except Exception:
        raise


def connect_to_aurora(aurora_creds):
    auth_mode = str(aurora_creds.get("auth_mode", "")).lower()
    region = os.getenv("AWS_REGION", "eu-north-1")
    host = aurora_creds["host"]
    port = int(aurora_creds.get("port", 5432))
    username = aurora_creds["username"]
    dbname = aurora_creds.get("dbname", "tfm")

    if auth_mode == "iam":
        token = rds_client.generate_db_auth_token(
            DBHostname=host, Port=port, DBUsername=username, Region=region
        )
        return psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=token,
            database=dbname,
            sslmode="require",
        )

    return psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=aurora_creds["password"],
        database=dbname,
    )


def get_trading_data(connection, report_date, days_back=DAYS_BACK):
    try:
        cursor = connection.cursor()
        end_date = pd.to_datetime(report_date).date()
        start_date = end_date - timedelta(days=days_back)
        query = """
            SELECT ts.batch_date, ts.ticker, ts.exposure_recommendation, ts.prob_up, ts.prob_down,
                   ti.close_price, ti.rsi_14, ti.sma_20, ti.sma_50,
                   ti.bb_upper, ti.bb_middle, ti.bb_lower
            FROM trading_signals ts
            JOIN technical_indicators ti ON ts.batch_date = ti.batch_date AND ts.ticker = ti.ticker
            WHERE ts.batch_date >= %s AND ts.batch_date <= %s 
            ORDER BY ts.batch_date, ts.ticker
        """
        cursor.execute(query, (start_date, end_date))
        signals_df = pd.DataFrame(
            cursor.fetchall(),
            columns=[
                "batch_date", "ticker", "exposure_recommendation", "prob_up", "prob_down",
                "close_price", "rsi_14", "sma_20", "sma_50",
                "bb_upper", "bb_middle", "bb_lower"
            ],
        )
        cursor.close()
        return signals_df
    except Exception:
        raise


def get_signal_outcomes(connection, report_date, days_back=DAYS_BACK):
    try:
        cursor = connection.cursor()
        end_date = pd.to_datetime(report_date).date()
        start_date = end_date - timedelta(days=days_back)
        cursor.execute(
            """
            SELECT batch_date, ticker, exposure_recommendation, prob_up, outcome_d1, outcome_d3,
                   outcome_d5, correct_d1, correct_d3, correct_d5
            FROM signal_outcomes
            WHERE batch_date >= %s AND batch_date <= %s
            ORDER BY batch_date, ticker
            """,
            (start_date, end_date),
        )
        cols = [
            "batch_date", "ticker", "exposure_recommendation", "prob_up", "outcome_d1",
            "outcome_d3", "outcome_d5", "correct_d1", "correct_d3", "correct_d5",
        ]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        cursor.close()
        return rows
    except Exception as exc:
        logger.warning(f"No se pudieron leer signal_outcomes para auditoria: {exc}")
        return []


def calculate_backtesting_metrics(signals_df):
    try:
        metrics = {}
        diagnostics = {}
        for ticker in signals_df["ticker"].unique():
            ticker_signals = signals_df[signals_df["ticker"] == ticker].sort_values(
                "batch_date"
            )
            starting_capital = 10000.0
            current_capital = starting_capital
            equity_curve = [starting_capital]

            # --- NUEVA LÓGICA LONG-ONLY (El sistema empieza invertido) ---
            # current_position: 1 = Long (Invertido), 0 = Liquidez (Cash)
            current_position = 1 
            
            # Cogemos el precio de cierre del primer día como nuestro precio de entrada inicial
            # Aseguramos que haya datos antes de extraer el precio
            if len(ticker_signals) > 0 and pd.notna(ticker_signals.iloc[0]["close_price"]):
                entry_price = float(ticker_signals.iloc[0]["close_price"])
            else:
                entry_price = 0.0
                current_position = 0 # Si no hay precio el día 1, forzamos cash por seguridad
                
            trades_returns = []
            days_invested = 0

            recommendations_count = ticker_signals["exposure_recommendation"].value_counts().to_dict()

            for idx, row in ticker_signals.iterrows():
                current_price = float(row["close_price"]) if row["close_price"] else 0.0
                if current_price == 0:
                    continue

                recommendation = classify_exposure_recommendation_from_prob(
                    float(row["prob_up"]) if row["prob_up"] is not None else 0.5
                )

                if recommendation in ("INCREASE_STRONG", "INCREASE_MILD"):
                    if current_position == 0:  # Entrar en el mercado (Comprar)
                        current_position = 1
                        entry_price = current_price
                elif recommendation in ("REDUCE_STRONG", "REDUCE_MILD"):
                    if current_position == 1:  # Salir del mercado (Vender a liquidez)
                        trade_return = (current_price - entry_price) / entry_price
                        current_capital *= 1 + trade_return
                        trades_returns.append(float(trade_return))
                        current_position = 0
                # MAINTAIN: no opera. Mantiene la posición actual (LONG o CASH).

                # Para dibujar la curva de capital diaria de forma realista, calculamos el valor 
                # de mercado (Mark-to-Market) si estamos invertidos.
                if current_position == 1 and entry_price > 0:
                    days_invested += 1
                    unrealized_return = (current_price - entry_price) / entry_price
                    daily_equity = current_capital * (1 + unrealized_return)
                else:
                    daily_equity = current_capital
                    
                equity_curve.append(daily_equity)

            # Valoración Final MTM (Mark-to-Market)
            final_equity = current_capital
            if current_position == 1 and entry_price > 0:
                final_price = float(ticker_signals.iloc[-1]["close_price"])
                unrealized_return = (final_price - entry_price) / entry_price
                final_equity = current_capital * (1 + unrealized_return)

            cumulative_return = (final_equity - starting_capital) / starting_capital

            if len(equity_curve) > 2:
                equity_arr = np.array(equity_curve)
                daily_returns = np.diff(equity_arr) / equity_arr[:-1]
                excess_returns = daily_returns - (0.02 / 252)
                std_dev = np.std(excess_returns)
                sharpe_ratio = (
                    (np.mean(excess_returns) / std_dev * np.sqrt(252))
                    if std_dev > 1e-6
                    else 0.0
                )
                peak = np.maximum.accumulate(equity_arr)
                drawdown = (equity_arr - peak) / peak
                max_drawdown = np.min(drawdown)
            else:
                sharpe_ratio = 0.0
                max_drawdown = 0.0

            metrics[ticker] = {
                "cumulative_return": round(float(cumulative_return), 4),
                "sharpe_ratio": round(float(sharpe_ratio), 4),
                "max_drawdown": round(float(max_drawdown), 4),
                "final_equity": round(float(final_equity), 2),
            }

            wins = sum(1 for value in trades_returns if value > 0)
            gross_profit = sum(value for value in trades_returns if value > 0)
            gross_loss = abs(sum(value for value in trades_returns if value < 0))
            profit_factor = (
                (gross_profit / gross_loss)
                if gross_loss > 1e-9
                else (gross_profit if gross_profit > 0 else 0.0)
            )

            diagnostics[ticker] = {
                "recommendations": {
                    "INCREASE_STRONG": int(recommendations_count.get("INCREASE_STRONG", 0)),
                    "INCREASE_MILD": int(recommendations_count.get("INCREASE_MILD", 0)),
                    "MAINTAIN": int(recommendations_count.get("MAINTAIN", 0)),
                    "REDUCE_MILD": int(recommendations_count.get("REDUCE_MILD", 0)),
                    "REDUCE_STRONG": int(recommendations_count.get("REDUCE_STRONG", 0)),
                },
                "trades_closed": len(trades_returns),
                "win_rate": (
                    round(float(wins / len(trades_returns)), 4)
                    if trades_returns
                    else 0.0
                ),
                "avg_trade_return": (
                    round(float(np.mean(trades_returns)), 4) if trades_returns else 0.0
                ),
                "profit_factor": round(float(profit_factor), 4),
                "time_in_market_ratio": round(
                    float(days_invested / max(len(ticker_signals), 1)), 4
                ),
            }

        return metrics, diagnostics
    except Exception as e:
        logger.error(f"Error in math: {e}")
        raise


def get_pipeline_health(connection, report_date, run_id):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT tickers_processed, status FROM batch_log WHERE run_id = %s LIMIT 1",
        (run_id,),
    )
    batch_row = cursor.fetchone()
    if not batch_row:
        cursor.execute(
            "SELECT tickers_processed, status FROM batch_log WHERE batch_date = %s ORDER BY updated_at DESC LIMIT 1",
            (report_date,),
        )
        batch_row = cursor.fetchone()
    cursor.execute(
        "SELECT COUNT(DISTINCT ticker) FROM technical_indicators WHERE batch_date = %s",
        (report_date,),
    )
    indicator_tickers = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(DISTINCT ticker) FROM trading_signals WHERE batch_date = %s",
        (report_date,),
    )
    recommendation_tickers = cursor.fetchone()[0]
    cursor.execute(
        "SELECT COUNT(*) FROM sentiment_scores WHERE batch_date = %s", (report_date,)
    )
    headlines = cursor.fetchone()[0]
    cursor.execute(
        "SELECT stage, metrics FROM pipeline_kpis WHERE run_id = %s", (run_id,)
    )
    stage_metrics = {row[0]: row[1] for row in cursor.fetchall()}
    if not stage_metrics:
        cursor.execute(
            "SELECT stage, metrics FROM pipeline_kpis WHERE batch_date = %s",
            (report_date,),
        )
        stage_metrics = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    tickers_expected = (
        int(batch_row[0]) if batch_row and batch_row[0] is not None else 0
    )
    return {
        "batch_status": batch_row[1] if batch_row else "UNKNOWN",
        "tickers_expected": tickers_expected,
        "tickers_with_indicators": int(indicator_tickers or 0),
        "tickers_with_recommendations": int(recommendation_tickers or 0),
        "headlines_scored": int(headlines or 0),
        "coverage_ratio": (
            round(float((recommendation_tickers or 0) / tickers_expected), 4)
            if tickers_expected
            else 0.0
        ),
        "stage_kpis": stage_metrics,
    }


def get_explanations_sample(connection, report_date, limit=10):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT e.ticker, ts.exposure_recommendation, ts.prob_up, ts.prob_down, e.sentiment_state, e.rsi_state, e.trend_state, e.volatility_state
        FROM signal_explanations e JOIN trading_signals ts ON ts.batch_date = e.batch_date AND ts.ticker = e.ticker
        WHERE e.batch_date = %s ORDER BY ts.prob_up DESC LIMIT %s
    """,
        (report_date, limit),
    )
    rows = cursor.fetchall()
    cursor.close()
    return [
        {
            "ticker": r[0],
            "exposure_recommendation": r[1],
            "prob_up": round(float(r[2]), 4) if r[2] is not None else None,
            "prob_down": round(float(r[3]), 4) if r[3] is not None else None,
            "evidence": {
                "sentiment": r[4],
                "rsi": r[5],
                "trend": r[6],
                "volatility": r[7],
            },
        }
        for r in rows
    ]


def compute_benchmark(signals_df):
    benchmark = {}
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


def get_close_price(ticker: str, date_str: str) -> float | None:
    """Obtiene el precio de cierre de un ticker en una fecha concreta vía yfinance."""
    try:
        target = pd.to_datetime(date_str).date()
        # Descarga un rango de 7 días para asegurar que captura el cierre
        # aunque caiga en fin de semana o festivo
        start = target - timedelta(days=1)
        end   = target + timedelta(days=6)
        df = yf.download(ticker, start=start, end=end, progress=False)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        # Buscar la fila más próxima a la fecha objetivo (sin pasarse)
        df.index = pd.to_datetime(df.index).date
        candidates = [d for d in df.index if d >= target]
        if not candidates:
            return None
        row = df.loc[min(candidates)]
        close = row.get("Close") or row.get("close")
        return float(close) if close else None
    except Exception as exc:
        logger.warning(f"yfinance no pudo obtener precio para {ticker} en {date_str}: {exc}")
        return None


def _outcome(price_d0: float, price_dn: float | None) -> str | None:
    """Clasifica el movimiento de precio como UP / DOWN / FLAT."""
    if price_d0 is None or price_dn is None or price_d0 == 0:
        return None
    change = (price_dn - price_d0) / price_d0
    if change > 0.005:   # +0.5%
        return "UP"
    if change < -0.005:  # -0.5%
        return "DOWN"
    return "FLAT"


def _is_correct(recommendation: str, outcome: str | None) -> bool | None:
    """Devuelve True si la recomendación predijo correctamente la dirección."""
    if outcome is None:
        return None
    if recommendation in ("INCREASE_STRONG", "INCREASE_MILD") and outcome == "UP":
        return True
    if recommendation in ("REDUCE_STRONG", "REDUCE_MILD") and outcome == "DOWN":
        return True
    if recommendation == "MAINTAIN" and outcome == "FLAT":
        return True
    return False


def upsert_signal_outcomes(connection, batch_date: str, signals_df: pd.DataFrame,
                           explanations_raw: list, run_id: str):
    """
    Persiste recomendaciones del día en signal_outcomes (paso 1: precio D0, nodos Y MACRO).
    Además actualiza outcomes de días anteriores (D+1, D+3, D+5).
    """
    cursor = connection.cursor()

    # ── Paso 1: insertar señales de hoy (AHORA INCLUYE JOIN MACRO) ────────────
    cursor.execute("""
        SELECT ts.ticker, ts.exposure_recommendation, ts.prob_up, ts.prob_down,
               se.sentiment_state, se.rsi_state, se.trend_state, se.volatility_state,
               ms.macro_sentiment, mr.risk_regime, mr.macro_adjustment
        FROM trading_signals ts
        LEFT JOIN signal_explanations se ON ts.batch_date = se.batch_date AND ts.ticker = se.ticker
        LEFT JOIN macro_sentiment_scores ms ON ts.batch_date = ms.batch_date
        LEFT JOIN market_regime_state mr ON ts.batch_date = mr.batch_date
        WHERE ts.batch_date = %s
    """, (batch_date,))
    rows = cursor.fetchall()

    for row in rows:
        ticker, rec, prob_up, prob_down, sent, rsi, trend, vol, macro_sent, risk_reg, macro_adj = row
        price_d0 = get_close_price(ticker, batch_date)
        cursor.execute("""
            INSERT INTO signal_outcomes
                (batch_date, ticker, run_id, exposure_recommendation, prob_up, prob_down,
                 sentiment_state, rsi_state, trend_state, volatility_state, price_d0,
                 macro_sentiment, risk_regime, macro_adjustment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batch_date, ticker) DO UPDATE SET
                exposure_recommendation = EXCLUDED.exposure_recommendation,
                prob_up          = EXCLUDED.prob_up,
                prob_down        = EXCLUDED.prob_down,
                sentiment_state  = EXCLUDED.sentiment_state,
                rsi_state        = EXCLUDED.rsi_state,
                trend_state      = EXCLUDED.trend_state,
                volatility_state = EXCLUDED.volatility_state,
                price_d0         = EXCLUDED.price_d0,
                macro_sentiment  = EXCLUDED.macro_sentiment,
                risk_regime      = EXCLUDED.risk_regime,
                macro_adjustment = EXCLUDED.macro_adjustment,
                updated_at       = CURRENT_TIMESTAMP
        """, (batch_date, ticker, run_id, rec,
              float(prob_up) if prob_up else None,
              float(prob_down) if prob_down else None,
              sent, rsi, trend, vol, price_d0, 
              macro_sent, risk_reg, float(macro_adj) if macro_adj else 0.0))

    # ── Paso 2: actualizar outcomes de días anteriores ─────────────────────────
    today = pd.to_datetime(batch_date).date()
    for days_ago, col_price, col_outcome, col_correct in [
        (1, "price_d1", "outcome_d1", "correct_d1"),
        (3, "price_d3", "outcome_d3", "correct_d3"),
        (5, "price_d5", "outcome_d5", "correct_d5"),
    ]:
        target_date = str(today - timedelta(days=days_ago))
        cursor.execute(f"""
            SELECT ticker, exposure_recommendation, price_d0
            FROM signal_outcomes
            WHERE batch_date = %s AND price_d0 IS NOT NULL AND {col_outcome} IS NULL
        """, (target_date,))
        pending = cursor.fetchall()

        for ticker, rec, price_d0 in pending:
            price_dn = get_close_price(ticker, batch_date)
            outcome  = _outcome(price_d0, price_dn)
            correct  = _is_correct(rec, outcome)
            if outcome:
                cursor.execute(f"""
                    UPDATE signal_outcomes
                    SET {col_price} = %s, {col_outcome} = %s, {col_correct} = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE batch_date = %s AND ticker = %s
                """, (price_dn, outcome, correct, target_date, ticker))

    connection.commit()
    cursor.close()
    logger.info(f"signal_outcomes: {len(rows)} recomendaciones de hoy insertadas, históricos actualizados.")


def upsert_pipeline_kpi(connection, batch_date, run_id, trigger_type, stage, metrics):
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO pipeline_kpis (batch_date, run_id, trigger_type, stage, metrics) VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (run_id, stage) DO UPDATE SET metrics = EXCLUDED.metrics, updated_at = CURRENT_TIMESTAMP
    """,
        (batch_date, run_id, trigger_type, stage, json.dumps(metrics)),
    )
    connection.commit()
    cursor.close()


def handler(event, context):
    try:
        logger.info("Lambda report generation started (Long/Cash Strategy)")
        aurora_creds = get_secret("aurora/credentials")
        connection = connect_to_aurora(aurora_creds)
        ctx = resolve_pipeline_context(event)
        today = ctx["batch_date"]

        signals_df = get_trading_data(connection, today, days_back=DAYS_BACK)
        backtest_metrics, diagnostics = (
            calculate_backtesting_metrics(signals_df)
            if not signals_df.empty
            else ({}, {})
        )
        pipeline_health = get_pipeline_health(connection, today, ctx["run_id"])
        explanations = get_explanations_sample(connection, today, limit=10)
        benchmark = compute_benchmark(signals_df) if not signals_df.empty else {}
        quant_audit_report = None
        if compute_quant_audit_report and _mongo_upsert_quant_audit_report:
            try:
                quant_audit_report = compute_quant_audit_report(
                    today,
                    signals_df.to_dict("records") if not signals_df.empty else [],
                    outcome_rows=get_signal_outcomes(connection, today, days_back=DAYS_BACK),
                    model_config=MODEL_AUDIT_CONFIG,
                )
                _mongo_upsert_quant_audit_report(today, quant_audit_report)
            except Exception as exc:
                logger.warning(f"quant_audit_report no actualizado: {exc}")

        report_data = {
            "report_date": today,
            "data_period_days": DAYS_BACK,
            "generated_at": datetime.now().isoformat(),
            "pipeline_health": pipeline_health,
            "recommendation_diagnostics": diagnostics,
            "benchmark_comparison": {
                ticker: {
                    "strategy_cumulative_return": backtest_metrics[ticker][
                        "cumulative_return"
                    ],
                    "buy_hold_cumulative_return": benchmark.get(ticker, 0.0),
                    "alpha_vs_benchmark": round(
                        backtest_metrics[ticker]["cumulative_return"]
                        - benchmark.get(ticker, 0.0),
                        4,
                    ),
                }
                for ticker in backtest_metrics
            },
            "top_recommendation_explanations": explanations,
            "backtesting_metrics": backtest_metrics,
            "summary": {
                "total_tickers": len(backtest_metrics),
                "avg_cumulative_return": (
                    round(
                        np.mean(
                            [m["cumulative_return"] for m in backtest_metrics.values()]
                        ),
                        4,
                    )
                    if backtest_metrics
                    else 0
                ),
                "avg_sharpe_ratio": (
                    round(
                        np.mean([m["sharpe_ratio"] for m in backtest_metrics.values()]),
                        4,
                    )
                    if backtest_metrics
                    else 0
                ),
                "avg_max_drawdown": (
                    round(
                        np.mean([m["max_drawdown"] for m in backtest_metrics.values()]),
                        4,
                    )
                    if backtest_metrics
                    else 0
                ),
                "total_closed_trades": (
                    int(sum(item["trades_closed"] for item in diagnostics.values()))
                    if diagnostics
                    else 0
                ),
            },
            "backtesting_config": {
                "initial_capital": 10000.0,
                "risk_free_rate": 0.02,
                "period_days": DAYS_BACK,
                "strategy_type": "Long/Cash",
                "sharpe_annualized": True,
                "limitation": "El backtesting asume ejecucion al cierre. Estrategia Long/Cash: recomendaciones de incremento abren posición, reducción cierra posición y mantener conserva el estado actual.",
            },
            "trace_artifact": f"mongo:bayesian_traces/{today}",
            "quant_audit_artifact": f"mongo:quant_audit_reports/{today}",
        }
        if not _mongo_upsert_report:
            raise RuntimeError(
                "mongo_utils no disponible: la imagen debe incluir mongo_utils con upsert_report."
            )
        _mongo_upsert_report(report_data)
        report_key = f"mongo:reports/{today}"
        upsert_pipeline_kpi(
            connection,
            today,
            ctx["run_id"],
            ctx["trigger_type"],
            "report",
            {
                "tickers_reported": len(backtest_metrics),
                "total_closed_trades": (
                    int(sum(item["trades_closed"] for item in diagnostics.values()))
                    if diagnostics
                    else 0
                ),
                "trigger_type": ctx["trigger_type"],
            },
        )

        # ── Registrar señales en signal_outcomes para calibración futura ────────
        try:
            upsert_signal_outcomes(
                connection, today, signals_df, explanations, ctx["run_id"]
            )
        except Exception as exc:
            # No crítico: si falla no interrumpe el pipeline
            logger.warning(f"signal_outcomes no actualizado: {exc}")

        cursor = connection.cursor()
        cursor.execute(
            "UPDATE batch_log SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE run_id = %s",
            ("COMPLETED", ctx["run_id"]),
        )
        connection.commit()
        cursor.close()
        connection.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Success", "report": report_key}),
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
