"""
catalyst.py — Detección de catalizadores por keywords (port FinRobot CatalystAnalyzer)
======================================================================================
Analiza titulares de noticias y devuelve métricas agregadas para feature_snapshot.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


CATALYST_KEYWORDS: Dict[str, List[str]] = {
    "product_launch": [
        "launch", "release", "unveil", "introduce", "new product",
        "announcement", "debut", "rollout",
    ],
    "earnings": [
        "earnings", "quarterly results", "financial results", "revenue",
        "profit", "guidance", "forecast", "outlook",
    ],
    "regulatory": [
        "fda", "approval", "regulation", "compliance", "lawsuit",
        "investigation", "antitrust", "settlement",
    ],
    "acquisition": [
        "acquire", "merger", "acquisition", "buyout", "deal",
        "partnership", "joint venture", "stake",
    ],
    "management": [
        "ceo", "cfo", "executive", "leadership", "board",
        "appointment", "resignation", "restructuring",
    ],
    "market": [
        "market share", "expansion", "growth", "competition",
        "pricing", "demand", "supply",
    ],
}

SENTIMENT_KEYWORDS: Dict[str, List[str]] = {
    "positive": [
        "growth", "increase", "beat", "exceed", "strong", "positive",
        "upgrade", "success", "win", "gain", "improve", "record",
        "initiate", "initiates", "overweight", "outperform",
        "accumulate", "bullish", "optimistic", "top pick",
    ],
    "negative": [
        "decline", "decrease", "miss", "weak", "negative", "downgrade",
        "loss", "fail", "drop", "concern", "risk", "challenge",
        "underweight", "underperform", "bearish", "pessimistic",
        "recall", "investigation", "warning",
    ],
}

ANALYST_POSITIVE_PATTERNS = [
    "initiates coverage", "initiate coverage", "initiates with",
    "starts coverage", "begins coverage", "upgrades to",
    "price target raised", "raises target", "overweight",
    "buy rating", "outperform rating", "top pick", "conviction buy",
    "maintains buy", "reiterate buy",
]

ANALYST_NEGATIVE_PATTERNS = [
    "downgrades to", "cuts to", "lowers to", "underweight",
    "sell rating", "underperform rating", "removes from",
    "drops coverage", "cuts target", "lowers target", "reduces target",
]

HIGH_IMPACT_TYPES = {"earnings", "acquisition", "regulatory"}
MEDIUM_IMPACT_TYPES = {"product_launch", "management"}


@dataclass
class CatalystEvent:
    event_type: str
    description: str
    headline: str
    impact_level: str
    sentiment: str
    source: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _headline_text(article: Dict[str, Any]) -> str:
    return (
        article.get("headline")
        or article.get("title")
        or article.get("summary")
        or ""
    ).strip()


def classify_event_type(text: str) -> str:
    text_lower = text.lower()
    for event_type, keywords in CATALYST_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return event_type
    return "other"


def analyze_sentiment(text: str) -> str:
    text_lower = text.lower()
    for pattern in ANALYST_POSITIVE_PATTERNS:
        if pattern in text_lower:
            return "positive"
    for pattern in ANALYST_NEGATIVE_PATTERNS:
        if pattern in text_lower:
            return "negative"

    pos = sum(1 for kw in SENTIMENT_KEYWORDS["positive"] if kw in text_lower)
    neg = sum(1 for kw in SENTIMENT_KEYWORDS["negative"] if kw in text_lower)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def assess_impact_level(event_type: str) -> str:
    if event_type in HIGH_IMPACT_TYPES:
        return "high"
    if event_type in MEDIUM_IMPACT_TYPES:
        return "medium"
    return "low"


def extract_catalysts_from_headlines(
    headlines: List[Dict[str, Any]],
    *,
    min_headline_len: int = 12,
) -> List[CatalystEvent]:
    """Identifica eventos catalizadores en una lista de artículos/titulares."""
    events: List[CatalystEvent] = []
    seen: set = set()

    for article in headlines or []:
        headline = _headline_text(article)
        if len(headline) < min_headline_len:
            continue
        key = headline.lower()[:120]
        if key in seen:
            continue
        seen.add(key)

        event_type = classify_event_type(headline)
        if event_type == "other":
            continue

        sentiment = analyze_sentiment(headline)
        events.append(
            CatalystEvent(
                event_type=event_type,
                description=headline[:200],
                headline=headline,
                impact_level=assess_impact_level(event_type),
                sentiment=sentiment,
                source=str(article.get("source") or article.get("site") or ""),
            )
        )
    return events


def catalyst_summary(events: List[CatalystEvent], window_days: int = 7) -> Dict[str, Any]:
    """
    Métricas agregadas para feature_snapshot:
      - catalyst_count_7d
      - catalyst_next_days (placeholder: días hasta próximo earnings si detectado)
      - catalyst_sentiment_net (-1..1)
    """
    if not events:
        return {
            "catalyst_count_7d": 0,
            "catalyst_next_days": None,
            "catalyst_sentiment_net": 0.0,
            "events": [],
        }

    pos = sum(1 for e in events if e.sentiment == "positive")
    neg = sum(1 for e in events if e.sentiment == "negative")
    total = len(events)
    net = round((pos - neg) / max(total, 1), 4)

    next_days = None
    for e in events:
        if e.event_type == "earnings":
            next_days = 0
            break

    return {
        "catalyst_count_7d": total,
        "catalyst_next_days": next_days,
        "catalyst_sentiment_net": net,
        "events": [e.to_dict() for e in events[:20]],
    }


def analyze_headlines_for_ticker(
    headlines: List[Dict[str, Any]],
    *,
    window_days: int = 7,
) -> Dict[str, Any]:
    """API principal: headlines → resumen de catalizadores."""
    events = extract_catalysts_from_headlines(headlines)
    summary = catalyst_summary(events, window_days=window_days)
    summary["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    return summary
