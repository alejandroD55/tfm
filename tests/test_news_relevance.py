import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))

from news_relevance import (  # noqa: E402
    filter_sentiment_samples,
    is_article_relevant_to_ticker,
    mentions_ticker_entity,
)


class TestNewsRelevance(unittest.TestCase):
    def test_nvda_rejects_cardano_solana_headline(self):
        ok, reason = is_article_relevant_to_ticker(
            "NVDA",
            "Better Altcoin: Cardano vs. Solana",
            "",
        )
        self.assertFalse(ok)
        self.assertIn("off_topic", reason)

    def test_nvda_accepts_nvidia_earnings(self):
        ok, _ = is_article_relevant_to_ticker(
            "NVDA",
            "NVIDIA beats earnings expectations on data center demand",
            "",
        )
        self.assertTrue(ok)

    def test_nvda_rejects_micron_without_nvidia(self):
        ok, reason = is_article_relevant_to_ticker(
            "NVDA",
            "Micron and SK Hynix Cross $1 Trillion Valuations as Chip Shortage Fuels AI Boom",
            "",
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "no_entity_mention")

    def test_spy_accepts_sp500(self):
        ok, _ = is_article_relevant_to_ticker(
            "SPY", "S&P 500 closes at record high amid soft landing hopes", ""
        )
        self.assertTrue(ok)

    def test_spy_rejects_pure_crypto(self):
        ok, _ = is_article_relevant_to_ticker(
            "SPY", "Better Altcoin: Cardano vs. Solana", ""
        )
        self.assertFalse(ok)

    def test_nvda_symbol_in_headline(self):
        self.assertTrue(mentions_ticker_entity("NVDA", "Why $NVDA could rally 20%"))

    def test_filter_samples(self):
        samples = [
            {"headline": "Better Altcoin: Cardano vs. Solana", "sentiment": "bullish", "confidence": 0.94},
            {"headline": "NVIDIA unveils new AI chip", "sentiment": "bullish", "confidence": 0.8},
        ]
        kept, skipped = filter_sentiment_samples("NVDA", samples)
        self.assertEqual(skipped, 1)
        self.assertEqual(len(kept), 1)
        self.assertIn("NVIDIA", kept[0]["headline"])


if __name__ == "__main__":
    unittest.main()
