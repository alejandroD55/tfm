"""Unit tests for shared/feature_builder.py"""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))

from feature_builder import (  # noqa: E402
    aggregate_sentiment_rows,
    technical_from_row,
    build_feature_snapshot,
    MODEL_ID_BAYESIAN,
)


class TestFeatureBuilder(unittest.TestCase):
    def test_aggregate_sentiment_empty(self):
        out = aggregate_sentiment_rows([])
        self.assertEqual(out["n_headlines"], 0)
        self.assertIsNone(out["state"])

    def test_aggregate_sentiment_rows(self):
        rows = [
            ("bullish", 0.9, "Stock rises on strong demand", ""),
            ("bullish", 0.7, "Analyst upgrade", ""),
            ("neutral", 0.5, "Market steady", ""),
        ]
        out = aggregate_sentiment_rows(rows)
        self.assertEqual(out["n_headlines"], 3)
        self.assertEqual(out["state"], "bullish")
        self.assertGreater(out["dispersion"], 0)

    def test_technical_from_row(self):
        tech = technical_from_row((55.0, 100.0, 95.0, 100.0, 105.0, 95.0))
        self.assertEqual(tech["rsi_14"], 55.0)
        self.assertAlmostEqual(tech["bb_width_ratio"], 0.1, places=3)

    def test_build_feature_snapshot_minimal(self):
        snap = build_feature_snapshot(
            "2025-03-01",
            "SPY",
            sentiment_rows=[("neutral", 0.6, "Markets flat", "")],
            indicators_row=(50.0, 500.0, 490.0, 500.0, 510.0, 490.0),
            macro_doc={
                "macro_sentiment": "neutral",
                "risk_regime": "NEUTRAL",
                "macro_adjustment": 0.0,
                "detail": {"vix": 18.0, "score": 0.1},
            },
            headlines=[],
            model_id=MODEL_ID_BAYESIAN,
        )
        self.assertEqual(snap["ticker"], "SPY")
        self.assertEqual(snap["schema_version"], "1.0")
        self.assertEqual(snap["technical"]["close_price"], 500.0)
        self.assertEqual(snap["macro"]["vix"], 18.0)


if __name__ == "__main__":
    unittest.main()
