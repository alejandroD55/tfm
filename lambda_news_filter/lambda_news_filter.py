"""
lambda_news_filter
──────────────────
Paso de preproceso entre ingestion y FinBERT.

Para cada artículo de raw_news:
  1. Descarga el contenido completo desde la URL original
  2. Extrae el texto principal (sin anuncios, nav, etc.) con trafilatura
  3. Envía el texto a Claude Haiku (Bedrock) para obtener un resumen
     objetivo e imparcial listo para análisis de sentimiento
  4. Guarda los resúmenes en la colección news_filtered de MongoDB

Si la URL no es accesible (paywall, timeout, bloqueo), cae automáticamente
al titular original para ese artículo — el pipeline nunca se interrumpe.
"""

import json
import boto3
import os
import logging
import random
import threading
import time

from botocore.config import Config
from botocore.exceptions import ClientError
import requests
import trafilatura
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Reintentos ante ThrottlingException; "adaptive" reduce presión bajo límites de cuenta.
_bedrock_retries = int(os.getenv("BEDROCK_MAX_ATTEMPTS", "12"))
bedrock = boto3.client(
    "bedrock-runtime",
    region_name=os.getenv("AWS_REGION", "eu-north-1"),
    config=Config(
        retries={"max_attempts": _bedrock_retries, "mode": "adaptive"},
        read_timeout=int(os.getenv("BEDROCK_READ_TIMEOUT_S", "120")),
        connect_timeout=10,
    ),
)
secrets_client = boto3.client("secretsmanager")


def _default_bedrock_model_id() -> str:
    """
    Haiku 4.5 in many regions is only available via system inference profiles
    (eu./us./global.*), not the legacy on-demand ID anthropic.claude-3-haiku-*.
    """
    r = os.getenv("AWS_REGION") or "eu-north-1"
    if r.startswith("us-"):
        return "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    if r.startswith("eu-"):
        return "eu.anthropic.claude-haiku-4-5-20251001-v1:0"
    return "global.anthropic.claude-haiku-4-5-20251001-v1:0"


BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", _default_bedrock_model_id())
FETCH_TIMEOUT_S = int(os.getenv("FETCH_TIMEOUT_S", "8"))  # seg. máx. por petición HTTP
MAX_CHARS_TO_MODEL = int(
    os.getenv("MAX_CHARS_TO_MODEL", "6000")
)  # tokens aprox. → ~1500 tokens
# 1 = una llamada Bedrock a la vez por invocación Lambda (recomendado en cuentas con cuota baja).
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))
# Pausa mínima entre invoke_model (compartida entre hilos de la misma invocación).
BEDROCK_MIN_INTERVAL_S = float(os.getenv("BEDROCK_MIN_INTERVAL_S", "1.25"))
BEDROCK_THROTTLE_RETRIES = int(os.getenv("BEDROCK_THROTTLE_RETRIES", "8"))
BEDROCK_THROTTLE_BASE_DELAY_S = float(os.getenv("BEDROCK_THROTTLE_BASE_DELAY_S", "2.5"))

_bedrock_lock = threading.Lock()
_last_bedrock_call = 0.0
# 0 = sin límite. Útil si el universo ETF trae decenas de noticias por ticker y se agota el timeout.
NEWS_FILTER_MAX_ARTICLES_PER_TICKER = int(
    os.getenv("NEWS_FILTER_MAX_ARTICLES_PER_TICKER", "0")
)
# Si queda menos tiempo (ms), no se encolan más tickers (evita timeout duro sin guardar el resto).
NEWS_FILTER_MIN_REMAINING_MS = int(os.getenv("NEWS_FILTER_MIN_REMAINING_MS", "90000"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; TFM-NewsBot/1.0; +https://github.com/tfm-trading)"
    ),
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
}

try:
    from mongo_utils import (
        read_raw_news as _mongo_read_raw,
        upsert_filtered_news as _mongo_upsert_filtered,
    )

    logger.info("mongo_utils (news_filter) cargado OK")
except ImportError:
    logger.warning("mongo_utils no disponible")
    _mongo_read_raw = None
    _mongo_upsert_filtered = None


# ─── Contexto del pipeline ────────────────────────────────────────────────────


def resolve_batch_date(event):
    raw = (event or {}).get("batch_date") or (event or {}).get("date")
    return raw[:10] if raw else datetime.now().strftime("%Y-%m-%d")


def resolve_pipeline_context(event):
    ctx = (event or {}).get("pipeline_context", {}) if isinstance(event, dict) else {}
    request = ctx.get("request", {}) if isinstance(ctx, dict) else {}
    if not isinstance(request, dict):
        request = {}
    batch_date = (
        resolve_batch_date(request)
        if request.get("batch_date")
        else resolve_batch_date(ctx)
    )
    run_id = ctx.get("run_id") or (event or {}).get("run_id") or f"legacy-{batch_date}"
    trigger_type = request.get("trigger_type")
    if trigger_type not in ("manual", "scheduled"):
        trigger_type = (
            "manual" if request.get("ticker") or request.get("tickers") else "scheduled"
        )
    return {"batch_date": batch_date, "run_id": run_id, "trigger_type": trigger_type}


# ─── Descarga y extracción del artículo ──────────────────────────────────────


def fetch_article_text(url: str) -> tuple[str, str]:
    """
    Descarga la URL y extrae el texto principal del artículo.
    Devuelve (texto_extraído, método_usado).
    Si falla, devuelve ("", "failed").
    """
    if not url or not url.startswith("http"):
        return "", "no_url"

    try:
        resp = requests.get(
            url, headers=HEADERS, timeout=FETCH_TIMEOUT_S, allow_redirects=True
        )
        resp.raise_for_status()

        # trafilatura extrae el texto principal ignorando nav, ads, footers, etc.
        text = trafilatura.extract(
            resp.text,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,  # prioriza precisión sobre cobertura
        )

        if text and len(text.strip()) > 100:
            return text.strip(), "trafilatura"

        # Fallback: si trafilatura devuelve muy poco texto, usar el HTML crudo truncado
        # (podría ser un artículo muy corto o con estructura inusual)
        return resp.text[:2000], "raw_html_fallback"

    except requests.exceptions.Timeout:
        logger.debug(f"Timeout fetching {url}")
        return "", "timeout"
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response else "?"
        if status in (401, 403, 429):
            logger.debug(f"Acceso denegado ({status}): {url}")
            return "", f"blocked_{status}"
        logger.debug(f"HTTP {status}: {url}")
        return "", f"http_{status}"
    except Exception as exc:
        logger.debug(f"Error fetching {url}: {type(exc).__name__}")
        return "", "error"


# ─── Prompt de resumen imparcial ──────────────────────────────────────────────

SYSTEM_PROMPT = """Eres un asistente especializado en resumir artículos financieros.
Tu única función es condensar fielmente lo que dice el artículo, sin añadir ni quitar nada.
Responde SIEMPRE con un JSON válido y nada más. Sin markdown, sin explicaciones."""


def build_summary_prompt(
    ticker: str, headline: str, article_text: str, source: str
) -> str:
    content = article_text[:MAX_CHARS_TO_MODEL] if article_text else headline
    content_type = (
        "artículo completo" if article_text else "titular (artículo no accesible)"
    )

    return f"""Activo: {ticker}
Fuente: {source}
Titular original: {headline}
Tipo de contenido: {content_type}

Contenido del artículo:
{content}

---
Tarea: Resume el artículo anterior siguiendo estas reglas ESTRICTAMENTE:

1. El resumen debe reflejar con fidelidad lo que dice el artículo, incluyendo el tono y el lenguaje con el que está escrito. Si el artículo es optimista, el resumen debe serlo. Si es alarmista o negativo, el resumen debe reflejarlo.
2. NO suavices, NO neutralices y NO elimines el sentimiento del texto original. El tono del artículo es información valiosa.
3. NO inventes datos, cifras, ni afirmaciones que no aparezcan explícitamente en el texto.
4. NO interpretes ni añadas contexto externo que no esté en el artículo.
5. NO omitas información relevante que aparezca en el texto original.
6. El resumen debe tener entre 2 y 4 frases en inglés, conservando el mismo registro emocional del original.

Devuelve ÚNICAMENTE este JSON:
{{
  "summary": "resumen fiel en inglés de 2-4 frases, preservando el tono original",
  "relevance": "high" | "medium" | "low",
  "key_facts": ["dato o afirmación concreta 1", "dato o afirmación concreta 2"],
  "content_source": "{('full_article' if article_text else 'headline_only')}"
}}"""


# ─── Llamada a Bedrock ────────────────────────────────────────────────────────


def _throttle_bedrock() -> None:
    """Espacia llamadas Bedrock para reducir ThrottlingException."""
    global _last_bedrock_call
    with _bedrock_lock:
        now = time.monotonic()
        wait = BEDROCK_MIN_INTERVAL_S - (now - _last_bedrock_call)
        if wait > 0:
            time.sleep(wait)
        _last_bedrock_call = time.monotonic()


def _is_bedrock_throttling(exc: BaseException) -> bool:
    if type(exc).__name__ in ("ThrottlingException", "TooManyRequestsException"):
        return True
    if isinstance(exc, ClientError):
        code = (exc.response.get("Error") or {}).get("Code", "")
        return code in (
            "ThrottlingException",
            "TooManyRequestsException",
            "ServiceUnavailableException",
        )
    msg = str(exc).lower()
    return "throttl" in msg or "too many requests" in msg


def _bedrock_fallback(headline: str, reason: str) -> dict:
    return {
        "summary": headline,
        "relevance": "medium",
        "key_facts": [],
        "content_source": reason,
    }


def summarize_with_bedrock(
    ticker: str, headline: str, article_text: str, source: str
) -> dict:
    """Llama a Claude Haiku y devuelve el JSON parseado."""
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "temperature": 0.0,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": build_summary_prompt(ticker, headline, article_text, source),
            }
        ],
    }
    last_exc: BaseException | None = None
    for attempt in range(BEDROCK_THROTTLE_RETRIES):
        try:
            _throttle_bedrock()
            resp = bedrock.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body),
            )
            raw = json.loads(resp["body"].read())
            text = raw["content"][0]["text"].strip()

            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]

            return json.loads(text)

        except json.JSONDecodeError as exc:
            logger.warning(
                f"Bedrock devolvió JSON inválido para {ticker}/{headline[:40]}: {exc}"
            )
            return _bedrock_fallback(headline, "fallback_parse_error")

        except Exception as exc:
            last_exc = exc
            if _is_bedrock_throttling(exc) and attempt < BEDROCK_THROTTLE_RETRIES - 1:
                delay = BEDROCK_THROTTLE_BASE_DELAY_S * (2**attempt) + random.uniform(
                    0.0, 1.5
                )
                logger.info(
                    "Bedrock throttle %s (intento %s/%s), esperando %.1fs",
                    ticker,
                    attempt + 1,
                    BEDROCK_THROTTLE_RETRIES,
                    delay,
                )
                time.sleep(delay)
                continue
            break

    detail = str(last_exc) if last_exc else "unknown"
    if isinstance(last_exc, ClientError):
        detail = (last_exc.response.get("Error") or {}).get("Message", detail)
    logger.warning(
        "Error Bedrock para %s: %s — %s",
        ticker,
        type(last_exc).__name__ if last_exc else "Error",
        detail,
    )
    return _bedrock_fallback(headline, "fallback_bedrock_error")


# ─── Procesamiento de un artículo individual ─────────────────────────────────


def process_article(ticker: str, article: dict) -> dict:
    """
    Descarga el artículo, extrae texto y genera resumen con Bedrock.
    Siempre devuelve un dict con el resultado, nunca lanza excepción.
    """
    headline = article.get("headline") or article.get("title") or ""
    url = article.get("url") or ""
    source = article.get("source") or "unknown"

    # 1. Intentar descargar el artículo completo
    article_text, fetch_method = fetch_article_text(url)

    if article_text:
        logger.info(f"  [{ticker}] Artículo descargado ({fetch_method}): {url[:60]}...")
    else:
        logger.info(
            f"  [{ticker}] No accesible ({fetch_method}), usando titular: {headline[:60]}..."
        )

    # 2. Resumir con Claude Haiku
    result = summarize_with_bedrock(ticker, headline, article_text, source)

    return {
        "original_headline": headline,
        "url": url,
        "source": source,
        "summary": result.get("summary", headline),
        "relevance": result.get("relevance", "medium"),
        "key_facts": result.get("key_facts", []),
        "content_source": result.get("content_source", "unknown"),
        "fetch_method": fetch_method,
        "datetime": article.get("datetime", ""),
    }


# ─── Leer noticias ─────────────────────────────────────────────────────


def read_raw_news(batch_date: str) -> dict:
    if not _mongo_read_raw:
        raise RuntimeError("mongo_utils no disponible — se requiere MongoDB.")
    try:
        raw = _mongo_read_raw(batch_date)
        logger.info(f"raw_news cargado: {len(raw)} tickers, batch_date={batch_date}")
        return raw
    except Exception as exc:
        logger.error(f"Error leyendo raw_news: {exc}")
        raise


# ─── Handler principal ────────────────────────────────────────────────────────


def handler(event, context):
    req_id = getattr(context, "aws_request_id", None) if context else None
    logger.info(
        "lambda_news_filter iniciado request_id=%s MAX_WORKERS=%s "
        "bedrock_interval=%.2fs model=%s",
        req_id,
        MAX_WORKERS,
        BEDROCK_MIN_INTERVAL_S,
        BEDROCK_MODEL_ID,
    )
    ctx = resolve_pipeline_context(event)
    today = ctx["batch_date"]

    try:
        raw_news = read_raw_news(today)
    except Exception as exc:
        return {"statusCode": 500, "body": json.dumps({"error": str(exc)})}

    if not raw_news:
        logger.warning(f"No hay noticias crudas para {today}")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Sin noticias que procesar", "batch_date": today}
            ),
        }

    total_articles = 0
    full_article_count = 0
    headline_fallback = 0
    per_ticker_stats = {}
    errors = []
    partial_due_to_time = False
    tickers_not_started_due_to_deadline: list[str] = []

    for ticker, articles in raw_news.items():
        if not articles:
            continue

        if context is not None:
            remaining = context.get_remaining_time_in_millis()
            if remaining < NEWS_FILTER_MIN_REMAINING_MS:
                logger.warning(
                    "Tiempo Lambda insuficiente (%sms restantes); no se procesan más tickers.",
                    remaining,
                )
                partial_due_to_time = True
                for t2, arts2 in raw_news.items():
                    if arts2 and t2 not in per_ticker_stats:
                        tickers_not_started_due_to_deadline.append(t2)
                break

        if NEWS_FILTER_MAX_ARTICLES_PER_TICKER > 0 and len(articles) > NEWS_FILTER_MAX_ARTICLES_PER_TICKER:
            n_orig = len(articles)
            articles = articles[:NEWS_FILTER_MAX_ARTICLES_PER_TICKER]
            logger.info(
                "%s: limitando artículos de %s a %s (NEWS_FILTER_MAX_ARTICLES_PER_TICKER)",
                ticker,
                n_orig,
                len(articles),
            )

        logger.info(f"Procesando {len(articles)} artículos para {ticker}...")
        processed = []

        # Descarga en paralelo para reducir latencia total
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_article, ticker, art): art for art in articles
            }
            for future in as_completed(futures):
                try:
                    result = future.result()
                    processed.append(result)
                    total_articles += 1
                    if result["fetch_method"] in ("trafilatura", "raw_html_fallback"):
                        full_article_count += 1
                    else:
                        headline_fallback += 1
                except Exception as exc:
                    logger.error(f"Error procesando artículo de {ticker}: {exc}")
                    errors.append(ticker)

        # Filtrar artículos de baja relevancia para no contaminar FinBERT
        relevant = [p for p in processed if p.get("relevance") != "low"]
        low_relevance_removed = len(processed) - len(relevant)

        # Preparar para MongoDB: solo los summaries (texto limpio para FinBERT)
        summaries = [p["summary"] for p in relevant if p.get("summary")]
        daily_context = (
            f"{len(relevant)} relevant articles processed for {ticker} on {today}."
        )

        if _mongo_upsert_filtered:
            try:
                _mongo_upsert_filtered(today, ticker, summaries, daily_context)
            except Exception as exc:
                logger.error(f"Error guardando news_filtered para {ticker}: {exc}")
                errors.append(ticker)

        per_ticker_stats[ticker] = {
            "articles_in": len(articles),
            "articles_processed": len(processed),
            "full_article_read": sum(
                1 for p in processed if p["fetch_method"] in ("trafilatura",)
            ),
            "headline_fallback": sum(
                1 for p in processed if p["fetch_method"] not in ("trafilatura",)
            ),
            "low_relevance_removed": low_relevance_removed,
            "summaries_to_finbert": len(summaries),
        }
        logger.info(
            f"{ticker}: {len(articles)} artículos → "
            f"{per_ticker_stats[ticker]['full_article_read']} leídos completos, "
            f"{per_ticker_stats[ticker]['headline_fallback']} fallback a titular, "
            f"{low_relevance_removed} descartados por baja relevancia, "
            f"{len(summaries)} resúmenes enviados a FinBERT"
        )

    read_pct = (
        round(full_article_count / total_articles * 100, 1) if total_articles else 0
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Procesamiento de artículos completado",
                "batch_date": today,
                "tickers_processed": len(per_ticker_stats),
                "total_articles": total_articles,
                "full_article_read": full_article_count,
                "headline_fallback": headline_fallback,
                "full_read_pct": read_pct,
                "per_ticker": per_ticker_stats,
                "errors": errors,
                "partial_due_to_time": partial_due_to_time,
                "tickers_not_started_due_to_deadline": tickers_not_started_due_to_deadline,
            }
        ),
    }
