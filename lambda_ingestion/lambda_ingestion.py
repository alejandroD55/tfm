# deploy: 2026-05-12 18:03 UTC
import json
import boto3
import yfinance as yf
import requests
import psycopg2
import os
import time
import hashlib
from datetime import datetime, timedelta
from newsapi import NewsApiClient
import pandas as pd
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
secrets_client = boto3.client('secretsmanager')
rds_client = boto3.client('rds')

# ── MongoDB helper ────────────────────────────────────────────────────────────
try:
    from mongo_utils import upsert_raw_news as _mongo_upsert_raw_news
    from mongo_utils import upsert_ohlcv_bulk as _mongo_upsert_ohlcv
    from mongo_utils import get_etf_tickers as _mongo_get_etf_tickers
    logger.info("mongo_utils (ingestion) cargado")
except ImportError:
    _mongo_upsert_raw_news = None
    _mongo_upsert_ohlcv = None
    _mongo_get_etf_tickers = None


def connect_to_aurora(aurora_creds):
    auth_mode = str(aurora_creds.get('auth_mode', '')).lower()
    region = os.getenv('AWS_REGION', 'eu-north-1')
    host = aurora_creds['host']
    port = int(aurora_creds.get('port', 5432))
    username = aurora_creds['username']
    dbname = aurora_creds.get('dbname', 'tfm')

    if auth_mode == 'iam':
        token = rds_client.generate_db_auth_token(
            DBHostname=host,
            Port=port,
            DBUsername=username,
            Region=region,
        )
        return psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=token,
            database=dbname,
            sslmode='require',
        )

    return psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=aurora_creds['password'],
        database=dbname,
    )


def resolve_batch_date(event):
    """Resolve a consistent batch date from Step Functions payload."""
    raw_date = (event or {}).get('batch_date') or (event or {}).get('date')
    if raw_date:
        return raw_date[:10]
    return datetime.now().strftime('%Y-%m-%d')


def resolve_pipeline_context(event):
    """Normalize execution metadata for manual/scheduled runs."""
    pipeline_ctx = (event or {}).get('pipeline_context', {}) if isinstance(event, dict) else {}
    request = pipeline_ctx.get('request', {}) if isinstance(pipeline_ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}

    batch_date = resolve_batch_date(request) if request.get('batch_date') else resolve_batch_date(pipeline_ctx)
    run_id = pipeline_ctx.get('run_id') or (event or {}).get('run_id') or f"legacy-{batch_date}"
    execution_name = pipeline_ctx.get('execution_name')

    requested_tickers = []
    if request.get('ticker'):
        requested_tickers = [str(request['ticker']).upper()]
    elif request.get('tickers'):
        requested_tickers = [str(t).upper() for t in request['tickers'] if t]

    trigger_type = request.get('trigger_type')
    if trigger_type not in ('manual', 'scheduled'):
        trigger_type = 'manual' if requested_tickers else 'scheduled'

    return {
        'batch_date': batch_date,
        'run_id': run_id,
        'execution_name': execution_name,
        'trigger_type': trigger_type,
        'requested_tickers': requested_tickers,
    }


def get_secret(secret_name):
    """Retrieve secret from AWS Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        return json.loads(response['SecretBinary'])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def read_etf_config():
    """Lee el universo ETF desde MongoDB (coleccion etf_universe, documento default)."""
    if not _mongo_get_etf_tickers:
        raise RuntimeError(
            "mongo_utils no disponible: la imagen Lambda debe incluir mongo_utils.py"
        )
    tickers = _mongo_get_etf_tickers()
    if not tickers:
        raise ValueError(
            "etf_universe vacio en MongoDB. Crea el documento con la API "
            "(POST /mongo/etf-universe) o inserta en la coleccion etf_universe "
            '({_id: "default", tickers: ["SPY", "QQQ", ...]}).'
        )
    return tickers


def download_ohlcv_data(tickers):
    """Download OHLCV data for the last 90 days using yfinance"""
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90) # <-- 90 DÍAS

        all_data = {}
        for ticker in tickers:
            try:
                data = yf.download(ticker, start=start_date, end=end_date, progress=False)
                if not data.empty:
                    # --- EL FIX SALVAVIDAS: Quitar la cabecera doble de yfinance ---
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.droplevel(1)
                    # ---------------------------------------------------------------
                    
                    data['Ticker'] = ticker
                    all_data[ticker] = data
                    logger.info(f"Downloaded OHLCV data for {ticker}")
                else:
                    logger.warning(f"No data found for ticker {ticker}")
            except Exception as e:
                logger.error(f"Error downloading data for {ticker}: {str(e)}")
                continue

        return all_data
    except Exception as e:
        logger.error(f"Error in download_ohlcv_data: {str(e)}")
        raise


# ─── Keywords por ticker para mejorar la búsqueda en NewsAPI ─────────────────
# Para ETFs genéricos se usa el símbolo directamente. Para los más conocidos
# añadimos términos adicionales para maximizar cobertura.
ETF_SEARCH_TERMS = {
    "SPY":  "SPY S&P 500 ETF",
    "QQQ":  "QQQ Nasdaq 100 ETF",
    "GLD":  "GLD gold ETF",
    "SLV":  "SLV silver ETF",
    "TLT":  "TLT treasury bonds ETF",
    "IWM":  "IWM Russell 2000 ETF",
    "EEM":  "EEM emerging markets ETF",
    "XLF":  "XLF financial sector ETF",
    "XLE":  "XLE energy sector ETF",
    "XLK":  "XLK technology sector ETF",
    "VNQ":  "VNQ real estate REIT ETF",
    "USO":  "USO oil ETF crude",
    "DIA":  "DIA Dow Jones ETF",
    "IAU":  "IAU gold ETF",
    "AGG":  "AGG bond ETF fixed income",
}


def _article_fingerprint(headline: str, url: str) -> str:
    """Hash único para deduplicar artículos entre fuentes."""
    key = (url or headline or "").strip().lower()
    return hashlib.md5(key.encode()).hexdigest()


def _normalize_article(headline: str, url: str, source: str,
                        published_at, summary: str = "") -> dict:
    """Convierte cualquier artículo al formato estándar del pipeline."""
    if isinstance(published_at, (int, float)):
        dt_str = datetime.utcfromtimestamp(published_at).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif isinstance(published_at, datetime):
        dt_str = published_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        dt_str = str(published_at or "")

    return {
        "headline": headline.strip() if headline else "",
        "url":      url or "",
        "source":   source or "unknown",
        "datetime": dt_str,
        "summary":  summary or "",
    }


# ─── Fuente 1: Finnhub ────────────────────────────────────────────────────────

def _news_from_finnhub(tickers: list, finnhub_key: str) -> dict:
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=1)
    result     = {}

    for ticker in tickers:
        try:
            resp = requests.get(
                "https://finnhub.io/api/v1/company-news",
                params={
                    "symbol": ticker,
                    "from":   start_date.strftime("%Y-%m-%d"),
                    "to":     end_date.strftime("%Y-%m-%d"),
                    "token":  finnhub_key,
                },
                timeout=10,
            )
            if resp.status_code == 200:
                articles = []
                for item in resp.json():
                    art = _normalize_article(
                        headline    = item.get("headline", ""),
                        url         = item.get("url", ""),
                        source      = item.get("source", "finnhub"),
                        published_at= item.get("datetime"),
                        summary     = item.get("summary", ""),
                    )
                    if art["headline"]:
                        articles.append(art)
                result[ticker] = articles
                logger.info(f"Finnhub: {len(articles)} artículos para {ticker}")
            else:
                logger.warning(f"Finnhub HTTP {resp.status_code} para {ticker}")
                result[ticker] = []
        except Exception as exc:
            logger.error(f"Finnhub error para {ticker}: {exc}")
            result[ticker] = []

    return result


# ─── Fuente 2: Yahoo Finance ──────────────────────────────────────────────────

def _news_from_yfinance(tickers: list) -> dict:
    result = {}
    for ticker in tickers:
        try:
            raw = yf.Ticker(ticker).news or []
            articles = []
            for item in raw:
                # yfinance ≥0.2.x anida el contenido en item["content"]
                content  = item.get("content", item)
                headline = (content.get("title") or item.get("title") or "").strip()
                url_data = content.get("canonicalUrl") or {}
                url      = url_data.get("url") if isinstance(url_data, dict) else content.get("url", "")
                provider = content.get("provider") or {}
                source   = provider.get("displayName") if isinstance(provider, dict) else "yahoo_finance"
                pub_date = content.get("pubDate") or item.get("providerPublishTime") or ""

                art = _normalize_article(
                    headline    = headline,
                    url         = url or item.get("link", ""),
                    source      = source or "yahoo_finance",
                    published_at= pub_date,
                )
                if art["headline"]:
                    articles.append(art)

            result[ticker] = articles
            logger.info(f"YFinance: {len(articles)} artículos para {ticker}")
        except Exception as exc:
            logger.error(f"YFinance error para {ticker}: {exc}")
            result[ticker] = []

    return result


# ─── Fuente 3: NewsAPI ────────────────────────────────────────────────────────

def _news_from_newsapi(tickers: list, newsapi_key: str) -> dict:
    client = NewsApiClient(api_key=newsapi_key)
    result = {}
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=1)

    for ticker in tickers:
        try:
            query = ETF_SEARCH_TERMS.get(ticker, ticker)
            resp  = client.get_everything(
                q          = query,
                from_param = start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                to         = end_date.strftime("%Y-%m-%dT%H:%M:%S"),
                language   = "en",
                sort_by    = "relevancy",
                page_size  = 20,
            )
            articles = []
            for item in (resp.get("articles") or []):
                art = _normalize_article(
                    headline    = item.get("title", ""),
                    url         = item.get("url", ""),
                    source      = (item.get("source") or {}).get("name", "newsapi"),
                    published_at= item.get("publishedAt", ""),
                    summary     = item.get("description", ""),
                )
                # NewsAPI incluye títulos genéricos "[Removed]" cuando el artículo
                # ya no está disponible — los filtramos
                if art["headline"] and art["headline"] != "[Removed]":
                    articles.append(art)

            result[ticker] = articles
            logger.info(f"NewsAPI: {len(articles)} artículos para {ticker}")
            time.sleep(0.2)   # respetar rate limit (max 1 req/seg en plan gratuito)

        except Exception as exc:
            logger.error(f"NewsAPI error para {ticker}: {exc}")
            result[ticker] = []

    return result


# ─── Agregador con deduplicación ──────────────────────────────────────────────

def download_news(tickers: list, finnhub_key: str, newsapi_key: str = "") -> dict:
    """
    Agrega noticias de Finnhub + Yahoo Finance + NewsAPI para cada ticker.
    Deduplica por URL (primero) y por titular exacto (segundo).
    Devuelve {ticker: [article_dict, ...]} en formato normalizado.
    """
    logger.info(f"Descargando noticias — fuentes: Finnhub, YFinance"
                + (", NewsAPI" if newsapi_key else ""))

    finnhub_news  = _news_from_finnhub(tickers, finnhub_key)
    yfinance_news = _news_from_yfinance(tickers)
    newsapi_news  = _news_from_newsapi(tickers, newsapi_key) if newsapi_key else {}

    merged = {}
    for ticker in tickers:
        seen_fps  = set()
        seen_titles = set()
        combined  = []

        # Orden de prioridad: Finnhub (más financiero) → YFinance → NewsAPI
        all_articles = (
            finnhub_news.get(ticker, []) +
            yfinance_news.get(ticker, []) +
            newsapi_news.get(ticker, [])
        )

        for art in all_articles:
            fp    = _article_fingerprint(art["headline"], art["url"])
            title = art["headline"].lower().strip()

            if fp in seen_fps or title in seen_titles:
                continue   # duplicado — saltar

            seen_fps.add(fp)
            seen_titles.add(title)
            combined.append(art)

        merged[ticker] = combined
        logger.info(
            f"{ticker}: {len(combined)} artículos únicos "
            f"(Finnhub={len(finnhub_news.get(ticker,[]))}, "
            f"YFinance={len(yfinance_news.get(ticker,[]))}, "
            f"NewsAPI={len(newsapi_news.get(ticker,[]))})"
        )

    return merged


def insert_batch_log(connection, batch_date, run_id, trigger_type, execution_name, requested_tickers, status, tickers_processed):
    """Insert batch log entry to Aurora PostgreSQL"""
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO batch_log (batch_date, run_id, trigger_type, execution_name, requested_tickers, status, tickers_processed)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
            ON CONFLICT (run_id) DO UPDATE
            SET updated_at = CURRENT_TIMESTAMP, 
                batch_date = EXCLUDED.batch_date,
                status = EXCLUDED.status, 
                tickers_processed = EXCLUDED.tickers_processed
        """
        cursor.execute(
            query,
            (
                batch_date,
                run_id,
                trigger_type,
                execution_name,
                json.dumps(requested_tickers),
                status,
                tickers_processed,
            ),
        )
        connection.commit()
        cursor.close()
        logger.info(f"Batch log upserted: run_id={run_id}, date={batch_date}, status={status}, tickers={tickers_processed}")
    except Exception as e:
        logger.error(f"Error inserting batch log: {str(e)}")
        raise


def upsert_pipeline_kpi(connection, batch_date, run_id, trigger_type, stage, metrics):
    """Persist stage KPIs for observability."""
    try:
        cursor = connection.cursor()
        query = """
            INSERT INTO pipeline_kpis (batch_date, run_id, trigger_type, stage, metrics)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id, stage) DO UPDATE
            SET metrics = EXCLUDED.metrics,
                updated_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (batch_date, run_id, trigger_type, stage, json.dumps(metrics)))
        connection.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error upserting pipeline KPI: {str(e)}")
        raise


def handler(event, context):
    """Main Lambda handler.

    Acepta un parametro opcional 'ticker' (o 'tickers') en el evento para
    ejecutar el pipeline solo para un subconjunto de ETFs.

    Ejemplos de evento:
      {}                          -> procesa todos los tickers del universo
      {"ticker": "SPY"}           -> procesa solo SPY
      {"tickers": ["SPY","QQQ"]}  -> procesa SPY y QQQ
      {"batch_date": "2024-01-15","ticker": "SPY"}
    """
    try:
        logger.info("Lambda ingestion started")
        logger.info(f"Event received: {json.dumps(event or {})}")

        # Get configurations
        aurora_creds = get_secret('aurora/credentials')
        finnhub_key  = get_secret('finnhub/api_key')['api_key']

        # NewsAPI key — opcional: si no existe el secreto, se omite esa fuente
        newsapi_key = ""
        try:
            newsapi_key = get_secret('newsapi/api_key')['api_key']
            logger.info("NewsAPI key cargada correctamente")
        except Exception:
            logger.warning("Secreto newsapi/api_key no encontrado — fuente NewsAPI desactivada")

        # Read ETF configuration (universe completo)
        all_tickers = read_etf_config()
        ctx = resolve_pipeline_context(event)
        batch_date = ctx['batch_date']

        # ── Filtrar por ticker si se especifica en el evento ──────────────────
        if len(ctx['requested_tickers']) == 1:
            tickers = ctx['requested_tickers']
            logger.info(f"Single-ticker mode: {tickers[0]}")
        elif len(ctx['requested_tickers']) > 1:
            tickers = ctx['requested_tickers']
            logger.info(f"Multi-ticker mode: {tickers}")
        else:
            tickers = all_tickers
            logger.info(f"Full-universe mode: {len(tickers)} tickers")
        # ─────────────────────────────────────────────────────────────────────

        logger.info(f"Processing {len(tickers)} tickers for batch_date={batch_date}")

        # Download OHLCV data
        ohlcv_data = download_ohlcv_data(tickers)

        # Download news (Finnhub + YFinance + NewsAPI)
        news_data = download_news(tickers, finnhub_key, newsapi_key)

        # Combine all OHLCV data into a single DataFrame
        if not ohlcv_data:
            raise ValueError("No OHLCV data downloaded for any ticker")
        combined_ohlcv = pd.concat([df for df in ohlcv_data.values()])

        today = batch_date

        # ── MongoDB: fuente unica de raw OHLCV y noticias ─────────────────────
        if not (_mongo_upsert_ohlcv and _mongo_upsert_raw_news):
            raise RuntimeError(
                "MongoDB helpers no cargados: la imagen debe incluir mongo_utils "
                "con upsert_ohlcv_bulk y upsert_raw_news."
            )
        for ticker_sym, ticker_df in ohlcv_data.items():
            rows = []
            for idx, row in ticker_df.iterrows():
                rows.append({
                    "date":   str(idx.date()) if hasattr(idx, 'date') else str(idx),
                    "open":   float(row.get("Open", 0) or 0),
                    "high":   float(row.get("High", 0) or 0),
                    "low":    float(row.get("Low",  0) or 0),
                    "close":  float(row.get("Close",0) or 0),
                    "volume": float(row.get("Volume",0) or 0),
                })
            _mongo_upsert_ohlcv(today, ticker_sym, rows)
        logger.info(f"MongoDB: OHLCV guardado para {len(ohlcv_data)} tickers")

        for ticker_sym, articles in news_data.items():
            if articles:
                _mongo_upsert_raw_news(today, ticker_sym, articles)
        logger.info(f"MongoDB: noticias guardadas para {sum(1 for a in news_data.values() if a)} tickers")

        ohlcv_key = f"mongo:ohlcv/{today}"
        news_key = f"mongo:raw_news/{today}"

        # Connect to Aurora and insert batch log
        connection = connect_to_aurora(aurora_creds)

        insert_batch_log(
            connection,
            today,
            ctx['run_id'],
            ctx['trigger_type'],
            ctx['execution_name'],
            tickers,
            'STARTED',
            len(tickers),
        )
        upsert_pipeline_kpi(connection, today, ctx['run_id'], ctx['trigger_type'], 'ingestion', {
            'tickers_expected': len(tickers),
            'tickers_with_ohlcv': len(ohlcv_data),
            'tickers_with_news': sum(1 for _, items in news_data.items() if items),
            'headlines_total': sum(len(items) for _, items in news_data.items()),
            'ohlcv_rows_total': int(len(combined_ohlcv)),
            'trigger_type': ctx['trigger_type'],
        })

        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Ingestion completed successfully',
                'tickers_processed': len(tickers),
                'ohlcv_saved': ohlcv_key,
                'news_saved': news_key
            })
        }

    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
