#!/usr/bin/env python3
"""
deepseek_event_study.py — Estudio de evento: impacto DeepSeek en NVDA
======================================================================
Analiza cómo reaccionó el sistema bayesiano ante el anuncio de DeepSeek
el 27-ene-2025 (NVDA cayó ~17% en la sesión del 27-ene).

PREREQUISITO: haber ejecutado primero el bootstrap para NVDA en el rango:
  python bootstrap_365_days.py --tickers NVDA --start 2025-01-13 --end 2025-02-28

RESULTADO: genera deepseek_event_study_report.html con:
  - Timeline de prob_up y señal alrededor del evento
  - Exposición bayesiana continua vs sistema binario
  - Noticias detectadas por GDELT en el rango del evento
  - Métricas de velocidad de reacción del modelo
"""

import os
import sys
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.getenv("POSTGRES_USER", "tfmadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", "localpassword123"),
    "database": os.getenv("POSTGRES_DB", "tfm"),
}

EVENT_DATE   = date(2025, 1, 27)   # Día del anuncio DeepSeek (NVDA -17%)
STUDY_START  = date(2025, 1, 13)   # 2 semanas antes del evento
STUDY_END    = date(2025, 2, 28)   # 1 mes después del evento
TICKER       = "NVDA"
CACHE_DIR    = Path("cache/news")

EVENT_WINDOW = {
    "pre_event":  (date(2025, 1, 13), date(2025, 1, 24)),   # semanas previas (baseline)
    "event_day":  (date(2025, 1, 27), date(2025, 1, 27)),   # día del shock
    "post_short": (date(2025, 1, 28), date(2025, 2, 7)),    # rebote inmediato
    "post_long":  (date(2025, 2, 10), date(2025, 2, 28)),   # recuperación
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_signals(conn) -> pd.DataFrame:
    """Carga señales + indicadores + exposición de NVDA en el período del evento."""
    query = """
        SELECT
            ts.batch_date,
            ts.ticker,
            ts.signal,
            ts.prob_up,
            ts.prob_down,
            ti.close_price,
            ti.rsi_14,
            ti.sma_20,
            ti.sma_50,
            se.sentiment_state,
            se.rsi_state,
            se.trend_state,
            se.volatility_state,
            ps.market_regime,
            ps.target_exposure,
            ps.smoothed_exposure,
            ps.exposure_delta
        FROM trading_signals ts
        LEFT JOIN technical_indicators  ti ON ti.batch_date = ts.batch_date AND ti.ticker = ts.ticker
        LEFT JOIN signal_explanations   se ON se.batch_date = ts.batch_date AND se.ticker = ts.ticker
        LEFT JOIN position_state        ps ON ps.batch_date = ts.batch_date AND ps.ticker = ts.ticker
        WHERE ts.ticker = %s
          AND ts.batch_date BETWEEN %s AND %s
        ORDER BY ts.batch_date
    """
    df = pd.read_sql(query, conn, params=(TICKER, STUDY_START, STUDY_END))
    df["batch_date"] = pd.to_datetime(df["batch_date"])
    return df


def load_cached_gdelt_news() -> dict:
    """Lee el caché GDELT de NVDA para el período del evento."""
    results = {}
    for f in CACHE_DIR.glob(f"gdelt_{TICKER}_*.json"):
        with open(f) as fh:
            data = json.load(fh)
        for date_str, arts in data.items():
            d = pd.to_datetime(date_str).date()
            if STUDY_START <= d <= STUDY_END:
                if date_str not in results:
                    results[date_str] = []
                results[date_str].extend(arts)
    return results


def assign_window(d: date) -> str:
    for name, (s, e) in EVENT_WINDOW.items():
        if s <= d <= e:
            return name
    return "other"


def compute_event_metrics(df: pd.DataFrame) -> dict:
    """Métricas clave del estudio de evento."""
    df = df.copy()
    df["date_only"] = df["batch_date"].dt.date

    # Día del evento
    event_row = df[df["date_only"] == EVENT_DATE]
    pre_rows   = df[df["date_only"] < EVENT_DATE]

    metrics = {}

    # prob_up baseline (media 5 días pre-evento)
    if not pre_rows.empty:
        metrics["prob_up_baseline"] = round(float(pre_rows.tail(5)["prob_up"].mean()), 4)
    else:
        metrics["prob_up_baseline"] = None

    # prob_up en el día del evento
    if not event_row.empty:
        metrics["prob_up_event_day"]  = round(float(event_row.iloc[0]["prob_up"]), 4)
        metrics["signal_event_day"]   = event_row.iloc[0]["signal"]
        metrics["exposure_event_day"] = round(float(event_row.iloc[0]["smoothed_exposure"] or 0.5), 4)
        metrics["regime_event_day"]   = event_row.iloc[0]["market_regime"]
    else:
        metrics["prob_up_event_day"] = None
        metrics["signal_event_day"]  = "N/A (ejecuta el bootstrap primero)"

    # Caída de prob_up el día del evento vs baseline
    if metrics.get("prob_up_baseline") and metrics.get("prob_up_event_day"):
        metrics["prob_up_drop"] = round(
            metrics["prob_up_event_day"] - metrics["prob_up_baseline"], 4
        )

    # ¿Cuántos días tardó el sistema en emitir SELL?
    post_event = df[df["date_only"] >= EVENT_DATE].copy()
    first_sell = post_event[post_event["signal"] == "SELL"]
    metrics["days_to_first_sell"] = int(
        (first_sell.iloc[0]["batch_date"] - pd.Timestamp(EVENT_DATE)).days
    ) if not first_sell.empty else None

    # ¿Cuántos días tardó en recuperar prob_up ≥ baseline?
    if metrics.get("prob_up_baseline"):
        recovery = post_event[post_event["prob_up"] >= metrics["prob_up_baseline"]]
        metrics["days_to_recovery"] = int(
            (recovery.iloc[0]["batch_date"] - pd.Timestamp(EVENT_DATE)).days
        ) if not recovery.empty else None

    # Retorno acumulado del período
    if not df.empty and df["close_price"].notna().any():
        prices = df["close_price"].dropna()
        p0, p1 = float(prices.iloc[0]), float(prices.iloc[-1])
        metrics["period_return_nvda"] = round((p1 - p0) / p0, 4) if p0 > 0 else None

    # Exposición promedio por ventana
    df["window"] = df["date_only"].apply(assign_window)
    metrics["avg_exposure_by_window"] = (
        df.groupby("window")["smoothed_exposure"].mean().round(4).to_dict()
    )

    return metrics


def build_html_report(df: pd.DataFrame, metrics: dict, gdelt_news: dict) -> str:
    """Genera un HTML autocontenido con los gráficos del estudio."""

    # Preparar datos para Chart.js
    dates_js     = json.dumps([str(d.date()) for d in df["batch_date"]])
    prob_up_js   = json.dumps([round(float(v), 4) if pd.notna(v) else None for v in df["prob_up"]])
    exposure_js  = json.dumps([round(float(v), 4) if pd.notna(v) else None for v in df["smoothed_exposure"]])
    close_js     = json.dumps([round(float(v), 2) if pd.notna(v) else None for v in df["close_price"]])
    signals_js   = json.dumps(list(df["signal"]))
    regimes_js   = json.dumps(list(df["market_regime"].fillna("NEUTRAL")))

    # Señal → color
    signal_colors = {"BUY": "#22c55e", "SELL": "#ef4444", "HOLD": "#f59e0b"}

    # Noticias del día del evento y adyacentes
    event_news_html = ""
    for d_offset in range(-1, 4):
        d = EVENT_DATE + timedelta(days=d_offset)
        arts = gdelt_news.get(d.isoformat(), [])
        if arts:
            label = f"🗓️ {d} {'← EVENTO DeepSeek' if d == EVENT_DATE else ''}"
            event_news_html += f"<h4 style='color:#94a3b8;margin:12px 0 4px'>{label}</h4><ul style='margin:0;padding-left:20px'>"
            for a in arts[:6]:
                hl = a.get("headline", "")[:140]
                src = a.get("source", "")
                event_news_html += f"<li style='margin-bottom:4px;color:#cbd5e1'>{hl} <span style='color:#64748b;font-size:11px'>— {src}</span></li>"
            event_news_html += "</ul>"

    # Métricas como tarjetas
    def card(title, value, color="#38bdf8"):
        return f"""
        <div style="background:#1e293b;border-radius:8px;padding:16px;border-left:4px solid {color}">
            <div style="color:#94a3b8;font-size:12px;margin-bottom:4px">{title}</div>
            <div style="color:#f1f5f9;font-size:22px;font-weight:700">{value}</div>
        </div>"""

    prob_drop_str = f"{metrics.get('prob_up_drop', 0)*100:+.1f}pp" if metrics.get('prob_up_drop') is not None else "N/A"
    drop_color    = "#ef4444" if (metrics.get('prob_up_drop') or 0) < 0 else "#22c55e"

    cards_html = f"""
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:24px">
        {card("prob_up baseline (5d pre)", f"{(metrics.get('prob_up_baseline') or 0)*100:.1f}%")}
        {card("prob_up día evento", f"{(metrics.get('prob_up_event_day') or 0)*100:.1f}%", "#f59e0b")}
        {card("Caída prob_up", prob_drop_str, drop_color)}
        {card("Señal día evento", metrics.get('signal_event_day','N/A'), "#a78bfa")}
        {card("Exposición día evento", f"{(metrics.get('exposure_event_day') or 0.5)*100:.1f}%", "#34d399")}
        {card("Régimen día evento", metrics.get('regime_event_day','N/A'), "#fb923c")}
        {card("Días hasta primer SELL", str(metrics.get('days_to_first_sell','N/A')), "#ef4444")}
        {card("Días hasta recuperación prob_up", str(metrics.get('days_to_recovery','N/A')), "#22c55e")}
        {card("Retorno NVDA período", f"{(metrics.get('period_return_nvda') or 0)*100:+.1f}%",
              "#22c55e" if (metrics.get('period_return_nvda') or 0) > 0 else "#ef4444")}
    </div>"""

    # Exposición promedio por ventana
    exp_by_window = metrics.get("avg_exposure_by_window", {})
    window_order  = ["pre_event", "event_day", "post_short", "post_long"]
    window_labels = ["Pre-evento (13-24 ene)", "Día evento (27 ene)", "Post corto (28 ene - 7 feb)", "Post largo (10-28 feb)"]
    exp_vals = [round(float(exp_by_window.get(w, 0.5)) * 100, 1) for w in window_order]
    exp_colors = ["#38bdf8", "#ef4444", "#f59e0b", "#22c55e"]
    exp_bars_html = "".join(
        f"""<div style="margin-bottom:10px">
            <div style="display:flex;justify-content:space-between;margin-bottom:2px">
                <span style="color:#94a3b8;font-size:13px">{lbl}</span>
                <span style="color:#f1f5f9;font-weight:700">{val}%</span>
            </div>
            <div style="background:#334155;border-radius:4px;height:8px">
                <div style="background:{col};width:{val}%;height:8px;border-radius:4px;transition:width 0.5s"></div>
            </div>
        </div>"""
        for lbl, val, col in zip(window_labels, exp_vals, exp_colors)
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Event Study: DeepSeek / NVDA — Enero 2025</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0f172a; color: #f1f5f9; font-family: system-ui, sans-serif; padding: 24px; }}
  h1 {{ font-size: 24px; font-weight: 700; margin-bottom: 4px; }}
  h2 {{ font-size: 16px; color: #94a3b8; margin: 24px 0 12px; font-weight: 600; text-transform: uppercase; letter-spacing: .05em; }}
  .subtitle {{ color: #64748b; font-size: 14px; margin-bottom: 24px; }}
  .event-badge {{ display:inline-block;background:#ef4444;color:white;font-size:12px;font-weight:700;padding:2px 10px;border-radius:999px;margin-left:8px;vertical-align:middle; }}
  canvas {{ background: #1e293b; border-radius: 8px; padding: 12px; }}
  .chart-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }}
  .chart-full {{ margin-bottom: 24px; }}
  .news-box {{ background: #1e293b; border-radius: 8px; padding: 16px; margin-bottom: 24px; max-height: 340px; overflow-y: auto; }}
  .exposure-panel {{ background: #1e293b; border-radius: 8px; padding: 16px; }}
  .footer {{ color: #475569; font-size: 12px; margin-top: 24px; text-align: center; }}
  @media(max-width:700px) {{ .chart-grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>

<h1>Event Study: DeepSeek / NVDA <span class="event-badge">27-Ene-2025</span></h1>
<p class="subtitle">Análisis de la reacción del sistema bayesiano ante el shock de DeepSeek — caída histórica de NVDA ≈ −17% en sesión</p>

<h2>📊 Métricas clave del evento</h2>
{cards_html}

<h2>📈 Precio, Probabilidad y Señal</h2>
<div class="chart-full">
  <canvas id="priceChart" height="90"></canvas>
</div>

<div class="chart-grid">
  <div>
    <canvas id="probChart" height="160"></canvas>
  </div>
  <div>
    <canvas id="exposureChart" height="160"></canvas>
  </div>
</div>

<h2>📰 Noticias detectadas (GDELT) en ventana del evento</h2>
<div class="news-box">
  {event_news_html if event_news_html else '<p style="color:#475569">No hay datos de GDELT en caché. Ejecuta el bootstrap primero.</p>'}
</div>

<h2>🎯 Exposición media por ventana temporal</h2>
<div class="exposure-panel">
  {exp_bars_html}
</div>

<p class="footer">Generado por deepseek_event_study.py · TFM Sentiment Analysis System · {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}</p>

<script>
const dates    = {dates_js};
const probUp   = {prob_up_js};
const exposure = {exposure_js};
const close    = {close_js};
const signals  = {signals_js};
const regimes  = {regimes_js};
const EVENT    = "2025-01-27";

const eventIdx = dates.indexOf(EVENT);
const eventAnnotation = (idx) => idx >= 0 ? [{{
  type: 'line', xMin: idx, xMax: idx,
  borderColor: '#ef4444', borderWidth: 2, borderDash: [6,3],
  label: {{ content: '⚡ DeepSeek', enabled: true, position: 'start', color: '#ef4444', font: {{size:11}} }}
}}] : [];

const sigColors = signals.map(s => s==='BUY'?'#22c55e': s==='SELL'?'#ef4444':'#f59e0b');

// ── Precio NVDA ───────────────────────────────────────────────────────────
new Chart(document.getElementById('priceChart'), {{
  type: 'line',
  data: {{
    labels: dates,
    datasets: [{{
      label: 'NVDA Close ($)',
      data: close,
      borderColor: '#38bdf8',
      backgroundColor: 'rgba(56,189,248,0.08)',
      borderWidth: 2,
      pointRadius: 4,
      pointBackgroundColor: sigColors,
      pointBorderColor: sigColors,
      pointRadius: 5,
      tension: 0.2,
      fill: true,
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ labels: {{ color: '#94a3b8' }} }},
      tooltip: {{
        callbacks: {{
          afterBody: (items) => {{
            const i = items[0].dataIndex;
            return [`Señal: ${{signals[i]}}`, `Régimen: ${{regimes[i]}}`];
          }}
        }}
      }}
    }},
    scales: {{
      x: {{ ticks: {{ color:'#64748b', maxTicksLimit:12 }}, grid: {{ color:'#1e293b' }} }},
      y: {{ ticks: {{ color:'#64748b' }}, grid: {{ color:'#334155' }} }}
    }}
  }}
}});

// ── prob_up ───────────────────────────────────────────────────────────────
new Chart(document.getElementById('probChart'), {{
  type: 'line',
  data: {{
    labels: dates,
    datasets: [
      {{ label:'prob_up', data:probUp, borderColor:'#a78bfa', backgroundColor:'rgba(167,139,250,0.1)', borderWidth:2, tension:0.3, fill:true, pointRadius:3 }},
      {{ label:'BUY threshold (0.52)', data:dates.map(()=>0.52), borderColor:'rgba(34,197,94,0.4)', borderDash:[4,4], borderWidth:1, pointRadius:0 }},
      {{ label:'SELL threshold (0.28)', data:dates.map(()=>0.28), borderColor:'rgba(239,68,68,0.4)', borderDash:[4,4], borderWidth:1, pointRadius:0 }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ labels: {{ color:'#94a3b8', font:{{size:11}} }} }} }},
    scales: {{
      x: {{ ticks: {{ color:'#64748b', maxTicksLimit:8 }}, grid: {{ color:'#1e293b' }} }},
      y: {{ min:0, max:1, ticks: {{ color:'#64748b', callback: v => (v*100).toFixed(0)+'%' }}, grid: {{ color:'#334155' }} }}
    }}
  }}
}});

// ── Exposición continua ───────────────────────────────────────────────────
new Chart(document.getElementById('exposureChart'), {{
  type: 'line',
  data: {{
    labels: dates,
    datasets: [
      {{ label:'smoothed_exposure', data:exposure, borderColor:'#34d399', backgroundColor:'rgba(52,211,153,0.1)', borderWidth:2, tension:0.3, fill:true, pointRadius:3 }},
      {{ label:'Floor BULL (60%)', data:dates.map(()=>0.6), borderColor:'rgba(52,211,153,0.3)', borderDash:[4,4], borderWidth:1, pointRadius:0 }},
      {{ label:'Floor NEUTRAL (35%)', data:dates.map(()=>0.35), borderColor:'rgba(248,188,56,0.3)', borderDash:[4,4], borderWidth:1, pointRadius:0 }},
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ labels: {{ color:'#94a3b8', font:{{size:11}} }} }} }},
    scales: {{
      x: {{ ticks: {{ color:'#64748b', maxTicksLimit:8 }}, grid: {{ color:'#1e293b' }} }},
      y: {{ min:0, max:1, ticks: {{ color:'#64748b', callback: v => (v*100).toFixed(0)+'%' }}, grid: {{ color:'#334155' }} }}
    }}
  }}
}});
</script>
</body>
</html>"""


def main():
    logger.info(f"🔍 Event Study: DeepSeek / {TICKER} — {STUDY_START} a {STUDY_END}")

    try:
        conn = get_conn()
    except Exception as e:
        logger.error(f"No se puede conectar a PostgreSQL: {e}")
        logger.info("💡 Asegúrate de que el docker-compose está corriendo: docker-compose up -d")
        sys.exit(1)

    df = load_signals(conn)
    conn.close()

    if df.empty:
        logger.warning("⚠️  No hay datos de NVDA en la BD para el rango del evento.")
        logger.info("▶️  Ejecuta primero:")
        logger.info("    python bootstrap_365_days.py --tickers NVDA --start 2025-01-13 --end 2025-02-28")
        # Generar reporte vacío igualmente para que el HTML existe
        metrics = {"signal_event_day": "N/A (ejecuta el bootstrap primero)"}
        gdelt_news = load_cached_gdelt_news()
        html = build_html_report(df, metrics, gdelt_news)
    else:
        logger.info(f"✅ Cargados {len(df)} días de datos para {TICKER}")
        metrics    = compute_event_metrics(df)
        gdelt_news = load_cached_gdelt_news()

        logger.info("\n=== MÉTRICAS DEL EVENTO ===")
        for k, v in metrics.items():
            logger.info(f"  {k}: {v}")

        html = build_html_report(df, metrics, gdelt_news)

    out = Path("deepseek_event_study_report.html")
    out.write_text(html, encoding="utf-8")
    logger.info(f"\n📄 Reporte generado: {out.resolve()}")
    logger.info("   Ábrelo en el navegador para ver los gráficos interactivos.")


if __name__ == "__main__":
    main()
