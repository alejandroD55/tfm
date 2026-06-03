"""Tests for shared/sentiment_scoring.py"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))

from sentiment_scoring import (  # noqa: E402
    aggregate_sentiment_samples,
    apply_sentiment_to_prob_up,
    compute_sentiment_adjustment,
    discretize_sentiment_from_net,
)


class TestSentimentScoring(unittest.TestCase):
    def test_mixed_headlines_not_always_neutral(self):
        samples = [
            {"sentiment": "bullish", "confidence": 0.9},
            {"sentiment": "bullish", "confidence": 0.8},
            {"sentiment": "bearish", "confidence": 0.6},
        ]
        dom, _, detail = aggregate_sentiment_samples(samples)
        self.assertEqual(dom, "bullish")
        self.assertGreater(detail["net_score"], 0.1)

    def test_majority_bearish_with_confidence(self):
        samples = [
            {"sentiment": "bearish", "confidence": 0.85},
            {"sentiment": "bearish", "confidence": 0.75},
            {"sentiment": "neutral", "confidence": 0.5},
        ]
        dom, _, detail = aggregate_sentiment_samples(samples)
        self.assertEqual(dom, "bearish")
        self.assertLess(detail["net_score"], -0.1)

    def test_adjustment_requires_headlines_and_signal(self):
        self.assertEqual(compute_sentiment_adjustment(0.5, 1, 0.2), 0.0)
        self.assertEqual(compute_sentiment_adjustment(0.05, 5, 0.1), 0.0)
        adj = compute_sentiment_adjustment(0.5, 4, 0.1)
        self.assertGreater(adj, 0.04)

    def test_apply_to_prob_up_caps(self):
        prob, adj = apply_sentiment_to_prob_up(0.95, 0.8, 5, 0.1)
        self.assertLessEqual(prob, 1.0)
        self.assertGreater(adj, 0)

    def test_discretize_thresholds(self):
        self.assertEqual(discretize_sentiment_from_net(0.15), "bullish")
        self.assertEqual(discretize_sentiment_from_net(-0.2), "bearish")
        self.assertEqual(discretize_sentiment_from_net(0.02), "neutral")


if __name__ == "__main__":
    unittest.main()
