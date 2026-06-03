"""
news_relevance.py — Filtrar noticias que no mencionan el activo antes de FinBERT / agregación.

Yahoo y otros feeds etiquetan artículos genéricos bajo el ticker (p. ej. cripto en NVDA).
FinBERT solo mide tono; sin este filtro, titulares irrelevantes pero «alcistas» sesgan prob_up.
"""
from __future__ import annotations

import os
import re
from typing import Dict, List, Optional, Tuple

# Desactivar solo para depuración: NEWS_RELEVANCE_FILTER=0
RELEVANCE_FILTER_ENABLED = os.getenv("NEWS_RELEVANCE_FILTER", "1").lower() not in (
    "0",
    "false",
    "no",
    "off",
)

# Símbolo + nombres / productos que cuentan como «mención del activo»
TICKER_ENTITIES: Dict[str, List[str]] = {
    "NVDA": [
        "nvidia",
        "nvda",
        "jensen huang",
        "geforce",
        "cuda",
        "h100",
        "h200",
        "blackwell",
        "dgx",
        "tensor core",
    ],
    "SPY": [
        "s&p 500",
        "s&p500",
        "sp500",
        "s and p 500",
        "spy",
        "standard & poor",
        "standard and poor",
    ],
    "IWM": [
        "russell 2000",
        "russell2000",
        "iwm",
        "small cap",
        "small-cap",
        "small caps",
    ],
    "GLD": [
        "gold",
        "gld",
        "bullion",
        "precious metal",
        "xau",
        "gold price",
        "gold etf",
    ],
    "XLE": [
        "xle",
        "energy sector",
        "oil price",
        "crude oil",
        "wti",
        "brent",
        "natural gas",
        "opec",
        "exxon",
        "chevron",
        "conocophillips",
    ],
}

# ETFs amplios: además del símbolo, aceptar contexto índice / mercado USA
BROAD_MARKET_EXTRA: Dict[str, List[str]] = {
    "SPY": [
        "stock market",
        "wall street",
        "equities",
        "s&p",
        "fed ",
        "federal reserve",
        "treasury",
        "nasdaq composite",
        "dow jones",
        "risk-on",
        "risk on",
    ],
    "IWM": [
        "stock market",
        "wall street",
        "equities",
        "fed ",
        "federal reserve",
    ],
}

# Temas que casi nunca aplican a acciones/ETF del universo si no hay mención del activo
OFF_TOPIC_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in (
        r"\baltcoin\b",
        r"\bcardano\b",
        r"\bsolana\b",
        r"\bethereum\b",
        r"\bbitcoin\b",
        r"\bcrypto vs\b",
        r"\bbetter altcoin\b",
        r"\bdogecoin\b",
        r"\bxrp\b",
        r"\bdefi\b",
        r"\bnft\b",
    )
]


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower()).strip()


def _entity_patterns(ticker: str) -> List[re.Pattern]:
    t = ticker.upper()
    terms = list(TICKER_ENTITIES.get(t, []))
    if t not in terms:
        terms.insert(0, t.lower())
    if t in BROAD_MARKET_EXTRA:
        terms.extend(BROAD_MARKET_EXTRA[t])
    patterns = []
    for term in terms:
        term = term.strip().lower()
        if not term:
            continue
        if term.isalpha() and len(term) <= 5 and " " not in term:
            # Símbolos cortos: límite de palabra; permitir prefijo $
            patterns.append(re.compile(rf"(?:\$)?\b{re.escape(term)}\b", re.IGNORECASE))
        else:
            patterns.append(re.compile(re.escape(term), re.IGNORECASE))
    return patterns


def mentions_ticker_entity(ticker: str, text: str) -> bool:
    if not text:
        return False
    return any(p.search(text) for p in _entity_patterns(ticker))


def is_off_topic_without_entity(ticker: str, text: str) -> bool:
    """True si el texto parece de otro universo (cripto, etc.) y no nombra el activo."""
    if not text:
        return False
    if mentions_ticker_entity(ticker, text):
        return False
    return any(p.search(text) for p in OFF_TOPIC_PATTERNS)


def is_article_relevant_to_ticker(
    ticker: str,
    headline: str = "",
    summary: str = "",
    *,
    url: str = "",
) -> Tuple[bool, str]:
    """
    Devuelve (relevante, motivo).
    Regla principal: el titular o resumen debe mencionar el activo (símbolo o alias).
    Excepción off-topic: bloquear cripto/comparativas sin mención aunque el feed las asigne.
    """
    if not RELEVANCE_FILTER_ENABLED:
        return True, "filter_disabled"

    t = (ticker or "").upper().strip()
    if not t:
        return False, "empty_ticker"

    combined = _normalize(f"{headline} {summary}")
    if len(combined) < 8:
        return False, "text_too_short"

    if is_off_topic_without_entity(t, combined):
        return False, "off_topic_no_entity_mention"

    if mentions_ticker_entity(t, combined):
        return True, "entity_mention"

    return False, "no_entity_mention"


def filter_articles_for_ticker(ticker: str, articles: List[dict]) -> Tuple[List[dict], int]:
    """Filtra lista de artículos {headline, summary?, url?}."""
    kept: List[dict] = []
    skipped = 0
    for art in articles or []:
        ok, _ = is_article_relevant_to_ticker(
            ticker,
            art.get("headline") or art.get("title") or "",
            art.get("summary") or "",
            url=art.get("url") or "",
        )
        if ok:
            kept.append(art)
        else:
            skipped += 1
    return kept, skipped


def filter_sentiment_samples(
    ticker: str, samples: List[dict]
) -> Tuple[List[dict], int]:
    """Filtra filas {headline, sentiment, confidence} ya en PG (recompute / lambda)."""
    kept: List[dict] = []
    skipped = 0
    for s in samples or []:
        ok, _ = is_article_relevant_to_ticker(
            ticker, s.get("headline") or "", s.get("summary") or ""
        )
        if ok:
            kept.append(s)
        else:
            skipped += 1
    return kept, skipped
