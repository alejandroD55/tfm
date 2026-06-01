"""Unit tests for shared/catalyst.py"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))

from catalyst import (  # noqa: E402
    analyze_headlines_for_ticker,
    classify_event_type,
    analyze_sentiment,
    extract_catalysts_from_headlines,
)


class TestCatalyst(unittest.TestCase):
    def test_classify_earnings(self):
        self.assertEqual(
            classify_event_type("Apple reports quarterly earnings beat"), "earnings"
        )

    def test_analyze_sentiment_positive_analyst(self):
        self.assertEqual(
            analyze_sentiment("Analyst initiates coverage with overweight rating"),
            "positive",
        )

    def test_analyze_sentiment_negative(self):
        self.assertEqual(
            analyze_sentiment("Company misses revenue guidance, shares drop"),
            "negative",
        )

    def test_extract_catalysts_from_headlines(self):
        articles = [
            {
                "headline": "NVDA beats quarterly earnings expectations with strong revenue growth"
            },
            {"headline": "Random market wrap without catalyst keywords here"},
        ]
        events = extract_catalysts_from_headlines(articles)
        self.assertGreaterEqual(len(events), 1)
        self.assertEqual(events[0].event_type, "earnings")

    def test_analyze_headlines_summary(self):
        summary = analyze_headlines_for_ticker(
            [{"headline": "FDA approval for new drug candidate boosts outlook"}]
        )
        self.assertGreaterEqual(summary["catalyst_count_7d"], 1)
        self.assertIn("catalyst_sentiment_net", summary)


if __name__ == "__main__":
    unittest.main()
