#!/usr/bin/env python3
"""
backfill_indicators.py — Rellena columnas nuevas en technical_indicators
=========================================================================
Calcula adx_14, ema_55_pct, momentum_20d, momentum_5d, sma_200 para todos
los registros existentes en technical_indicators usando el OHLCV ya cacheado.

NO llama a APIs externas. Usa yfinance con caché local.
Tiempo estimado: ~1-2 minutos para 5 tickers × 370 días.

Uso:
    python backfill_indicators.py
    python backfill_indicators.py --dry-run
    python backfill_indicators.py --tickers SPY,GLD
"""

import os, sys, argparse, logging
from datetime import date, timedelta
from typing import Dict, List, Optional
import psycopg2
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bootstrap_365_days import DB_CONFIG, TICKERS as DEFAULT_TICKERS

try:
    import pandas_ta_classic as ta
except ImportError:
    import pandas_ta as ta


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# =============================================================================
# 1. Cargar fechas existentes por ticker
# =============================================================================

def load_existing_dates(tickers: Optional[List[str]] = None) -> Dict[str, List[str]]:
    """Devuelve {ticker: [dates]} de lo que ya existe en technical_indicators."""
    conn = get_conn()
    cur  = conn.cursor()
    if tickers:
        cur.execute(
            "SELECT ticker, batch_date FROM technical_indicators WHERE ticker = ANY(%s) ORDER BY ticker, batch_date",
            (tickers,)
        )
    else:
        cur.execute("SELECT ticker, batch_date FROM technical_indicators ORDER BY ticker, batch_date")

    result: Dict[str, List[str]] = {}
    for tk, bd in cur.fetchall():
        result.setdefault(tk, []).append(str(bd))
    cur.close(); conn.close()
    logger.info(f"Tickers en BD: {list(result.keys())}, total filas: {sum(len(v) for v in result.values())}")
    return result


# =============================================================================
# 2. Descargar OHLCV (usa caché yfinance)
# =============================================================================

def fetch_ohlcv(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    import yfinance as yf
    # Lookback extra para que SMA200 y EMA55 estén inicializadas desde el primer día
    start_dt  = pd.to_datetime(start_date) - timedelta(days=400)
    df = yf.download(ticker, start=str(start_dt.date()), end=end_date,
                     progress=False, repair=False)
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    df.index = pd.to_datetime(df.index)
    if hasattr(df.index, 'tz') and df.index.tz is not None:
        df.index = df.index.tz_convert(None)
    return df


# =============================================================================
# 3. Calcular nuevos indicadores para una fecha
# =============================================================================

def _sf(v) -> Optional[float]:
    try:
        f = float(v)
        return None if (pd.isna(f) or not np.isfinite(f)) else round(f, 6)
    except Exception:
        return None


def calc_new_indicators(ohlcv_df: pd.DataFrame, target_date: str) -> Optional[Dict]:
    """Calcula solo las columnas nuevas para una fecha concreta."""
    target_dt = pd.to_datetime(target_date)
    df = ohlcv_df[ohlcv_df.index <= target_dt].copy()
    if len(df) < 50:
        return None

    close = df["Close"]
    high  = df["High"]  if "High"  in df.columns else close
    low_s = df["Low"]   if "Low"   in df.columns else close

    # SMA 200
    sma200 = ta.sma(close, length=200)
    s200   = _sf(sma200.iloc[-1]) if sma200 is not None and len(sma200) > 0 else None

    # EMA 55
    ema55  = ta.ema(close, length=55)
    e55    = _sf(ema55.iloc[-1]) if ema55 is not None and len(ema55) > 0 else None
    cl     = _sf(close.iloc[-1])
    ema55_pct = round((float(cl) - float(e55)) / float(e55), 4) \
        if e55 and e55 != 0 and cl else None

    # ADX 14
    try:
        adx_df = ta.adx(high, low_s, close, length=14)
        adx_val = _sf(adx_df.iloc[-1, 0]) if adx_df is not None and not adx_df.empty else None
    except Exception:
        adx_val = None

    # Momentum 20d y 5d
    mom20 = None
    mom5  = None
    if len(close) >= 21:
        p0 = float(close.iloc[-21])
        mom20 = round((float(close.iloc[-1]) - p0) / p0, 4) if p0 != 0 else None
    if len(close) >= 6:
        p0 = float(close.iloc[-6])
        mom5  = round((float(close.iloc[-1]) - p0) / p0, 4) if p0 != 0 else None

    return {
        "sma_200":      s200,
        "adx_14":       adx_val,
        "ema_55_pct":   ema55_pct,
        "momentum_20d": mom20,
        "momentum_5d":  mom5,
    }


# =============================================================================
# 4. UPDATE masivo en PostgreSQL
# =============================================================================

def update_indicators(ticker: str, updates: List[Dict], dry_run: bool):
    """Hace UPDATE de las filas existentes con los nuevos indicadores."""
    if not updates:
        return 0

    if dry_run:
        logger.info(f"  [DRY-RUN] {ticker}: {len(updates)} filas a actualizar")
        return len(updates)

    conn = get_conn()
    cur  = conn.cursor()
    updated = 0
    for u in updates:
        cur.execute("""
            UPDATE technical_indicators
            SET sma_200      = %s,
                adx_14       = %s,
                ema_55_pct   = %s,
                momentum_20d = %s,
                momentum_5d  = %s
            WHERE batch_date = %s AND ticker = %s
        """, (
            u["sma_200"], u["adx_14"], u["ema_55_pct"],
            u["momentum_20d"], u["momentum_5d"],
            u["batch_date"], ticker
        ))
        updated += cur.rowcount

    conn.commit()
    cur.close(); conn.close()
    return updated


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tickers",  default=None, help="Coma-separado ej: SPY,GLD")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    # Por defecto usa los tickers activos del sistema (no procesa ARKK, XBI ni otros legacy)
    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else DEFAULT_TICKERS
    logger.info(f"Tickers a procesar: {tickers}")

    print("=" * 65)
    print("  BACKFILL INDICADORES — SMA200, ADX14, EMA55, Momentum")
    print(f"  {'[DRY-RUN]' if args.dry_run else '[ESCRIBIENDO EN BD]'}")
    print("=" * 65)

    # 1. Qué fechas ya existen en BD
    existing = load_existing_dates(tickers)
    if not existing:
        logger.error("No hay datos en technical_indicators. Ejecuta el bootstrap primero.")
        sys.exit(1)

    total_updated = 0

    for ticker, dates in existing.items():
        if not dates:
            continue

        logger.info(f"\n📊 {ticker}: calculando indicadores para {len(dates)} fechas...")

        start_dt = min(dates)
        end_dt   = max(dates)

        # 2. Descargar OHLCV (caché local yfinance)
        logger.info(f"  Descargando OHLCV {ticker} ({start_dt} → {end_dt})...")
        ohlcv = fetch_ohlcv(ticker, start_dt, end_dt)
        if ohlcv.empty:
            logger.warning(f"  ⚠️  Sin datos OHLCV para {ticker} — saltando")
            continue
        logger.info(f"  OHLCV: {len(ohlcv)} filas")

        # 3. Calcular indicadores por fecha
        updates = []
        skipped = 0
        for i, d in enumerate(dates):
            ind = calc_new_indicators(ohlcv, d)
            if ind is None:
                skipped += 1
                continue
            ind["batch_date"] = d
            updates.append(ind)

            if (i + 1) % 100 == 0:
                logger.info(f"  {i+1}/{len(dates)} procesados...")

        if skipped:
            logger.warning(f"  {skipped} fechas saltadas (datos insuficientes)")

        # 4. Mostrar muestra
        if updates:
            sample = updates[len(updates)//2]
            fmt = lambda v, d=2: f"{v:.{d}f}" if v is not None else "N/A"
            logger.info(
                f"  Muestra [{sample['batch_date']}]: "
                f"sma200={fmt(sample['sma_200'])}  "
                f"adx={fmt(sample['adx_14'])}  "
                f"ema55_pct={fmt(sample['ema_55_pct'], 4)}  "
                f"mom20d={fmt(sample['momentum_20d'], 4)}"
            )

        # 5. Guardar
        n = update_indicators(ticker, updates, dry_run=args.dry_run)
        total_updated += n
        logger.info(f"  ✅ {ticker}: {n} filas {'procesadas' if args.dry_run else 'actualizadas'}")

    print()
    print("=" * 65)
    action = "procesarían" if args.dry_run else "actualizadas"
    print(f"  ✅ COMPLETADO — {total_updated} filas {action}")
    if args.dry_run:
        print("  Ejecuta sin --dry-run para aplicar los cambios.")
    else:
        print("  Ahora puedes ejecutar: python recompute_exposure.py --dry-run")
    print("=" * 65)


if __name__ == "__main__":
    main()
