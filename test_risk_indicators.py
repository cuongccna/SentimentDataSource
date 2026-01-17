"""
Unit Tests for Risk Indicators Module
"""

import unittest
from risk_indicators import (
    validate_input,
    compute_social_overheat,
    compute_panic_risk,
    compute_fomo_risk,
    compute_fear_greed_zone,
    compute_sentiment_reliability,
    compute_risk_indicators,
    process_batch,
    Mentions
)


class TestValidation(unittest.TestCase):
    """Test input validation."""
    
    def get_valid_input(self):
        return {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "sentiment": {
                "final": {
                    "label": 1,
                    "confidence": 0.85
                }
            },
            "mentions": {
                "count_1h": 1000,
                "velocity": 1.5,
                "anomaly": False
            },
            "fear_greed_index": 50
        }
    
    def test_valid_input(self):
        data = self.get_valid_input()
        is_valid, parsed, error = validate_input(data)
        self.assertTrue(is_valid)
        self.assertIsNotNone(parsed)
        self.assertIsNone(error)
    
    def test_valid_input_null_fear_greed(self):
        data = self.get_valid_input()
        data["fear_greed_index"] = None
        is_valid, parsed, error = validate_input(data)
        self.assertTrue(is_valid)
        self.assertIsNone(parsed.fear_greed_index)
    
    def test_missing_asset(self):
        data = self.get_valid_input()
        del data["asset"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("asset", error)
    
    def test_missing_timestamp(self):
        data = self.get_valid_input()
        del data["timestamp"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("timestamp", error)
    
    def test_missing_sentiment(self):
        data = self.get_valid_input()
        del data["sentiment"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("sentiment", error)
    
    def test_missing_sentiment_final(self):
        data = self.get_valid_input()
        del data["sentiment"]["final"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("sentiment.final", error)
    
    def test_missing_sentiment_label(self):
        data = self.get_valid_input()
        del data["sentiment"]["final"]["label"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("sentiment.final.label", error)
    
    def test_missing_sentiment_confidence(self):
        data = self.get_valid_input()
        del data["sentiment"]["final"]["confidence"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("sentiment.final.confidence", error)
    
    def test_missing_mentions(self):
        data = self.get_valid_input()
        del data["mentions"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("mentions", error)
    
    def test_missing_mentions_count_1h(self):
        data = self.get_valid_input()
        del data["mentions"]["count_1h"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("mentions.count_1h", error)
    
    def test_missing_mentions_velocity(self):
        data = self.get_valid_input()
        del data["mentions"]["velocity"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("mentions.velocity", error)
    
    def test_missing_mentions_anomaly(self):
        data = self.get_valid_input()
        del data["mentions"]["anomaly"]
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("mentions.anomaly", error)
    
    def test_invalid_label_value(self):
        data = self.get_valid_input()
        data["sentiment"]["final"]["label"] = 5  # Invalid
        is_valid, parsed, error = validate_input(data)
        self.assertFalse(is_valid)
        self.assertIn("Invalid", error)


class TestSocialOverheat(unittest.TestCase):
    """Test Social Overheat Indicator."""
    
    def test_overheat_true(self):
        # velocity >= 3.0 AND anomaly == true
        mentions = Mentions(count_1h=2000, velocity=3.0, anomaly=True)
        result = compute_social_overheat(mentions)
        self.assertTrue(result)
    
    def test_overheat_high_velocity(self):
        mentions = Mentions(count_1h=2000, velocity=5.0, anomaly=True)
        result = compute_social_overheat(mentions)
        self.assertTrue(result)
    
    def test_overheat_false_low_velocity(self):
        mentions = Mentions(count_1h=2000, velocity=2.9, anomaly=True)
        result = compute_social_overheat(mentions)
        self.assertFalse(result)
    
    def test_overheat_false_no_anomaly(self):
        mentions = Mentions(count_1h=2000, velocity=3.0, anomaly=False)
        result = compute_social_overheat(mentions)
        self.assertFalse(result)
    
    def test_overheat_false_both_conditions(self):
        mentions = Mentions(count_1h=500, velocity=1.0, anomaly=False)
        result = compute_social_overheat(mentions)
        self.assertFalse(result)


class TestPanicRisk(unittest.TestCase):
    """Test Panic Risk Indicator."""
    
    def test_panic_true(self):
        # label == -1 AND velocity >= 2.0
        result = compute_panic_risk(sentiment_label=-1, velocity=2.0)
        self.assertTrue(result)
    
    def test_panic_true_high_velocity(self):
        result = compute_panic_risk(sentiment_label=-1, velocity=5.0)
        self.assertTrue(result)
    
    def test_panic_false_wrong_label(self):
        result = compute_panic_risk(sentiment_label=1, velocity=2.0)
        self.assertFalse(result)
    
    def test_panic_false_neutral_label(self):
        result = compute_panic_risk(sentiment_label=0, velocity=2.0)
        self.assertFalse(result)
    
    def test_panic_false_low_velocity(self):
        result = compute_panic_risk(sentiment_label=-1, velocity=1.9)
        self.assertFalse(result)


class TestFomoRisk(unittest.TestCase):
    """Test FOMO Risk Indicator."""
    
    def test_fomo_true(self):
        # label == +1 AND fear_greed_index IS NOT null AND fear_greed_index >= 70
        result = compute_fomo_risk(sentiment_label=1, fear_greed_index=70)
        self.assertTrue(result)
    
    def test_fomo_true_high_index(self):
        result = compute_fomo_risk(sentiment_label=1, fear_greed_index=90)
        self.assertTrue(result)
    
    def test_fomo_false_wrong_label(self):
        result = compute_fomo_risk(sentiment_label=-1, fear_greed_index=70)
        self.assertFalse(result)
    
    def test_fomo_false_neutral_label(self):
        result = compute_fomo_risk(sentiment_label=0, fear_greed_index=70)
        self.assertFalse(result)
    
    def test_fomo_false_null_index(self):
        result = compute_fomo_risk(sentiment_label=1, fear_greed_index=None)
        self.assertFalse(result)
    
    def test_fomo_false_low_index(self):
        result = compute_fomo_risk(sentiment_label=1, fear_greed_index=69)
        self.assertFalse(result)


class TestFearGreedZone(unittest.TestCase):
    """Test Fear/Greed Zone (DESCRIPTIVE ONLY)."""
    
    def test_zone_unknown(self):
        result = compute_fear_greed_zone(None)
        self.assertEqual(result, "unknown")
    
    def test_zone_extreme_fear_boundary(self):
        result = compute_fear_greed_zone(20)
        self.assertEqual(result, "extreme_fear")
    
    def test_zone_extreme_fear_low(self):
        result = compute_fear_greed_zone(5)
        self.assertEqual(result, "extreme_fear")
    
    def test_zone_extreme_greed_boundary(self):
        result = compute_fear_greed_zone(80)
        self.assertEqual(result, "extreme_greed")
    
    def test_zone_extreme_greed_high(self):
        result = compute_fear_greed_zone(95)
        self.assertEqual(result, "extreme_greed")
    
    def test_zone_normal_low(self):
        result = compute_fear_greed_zone(21)
        self.assertEqual(result, "normal")
    
    def test_zone_normal_mid(self):
        result = compute_fear_greed_zone(50)
        self.assertEqual(result, "normal")
    
    def test_zone_normal_high(self):
        result = compute_fear_greed_zone(79)
        self.assertEqual(result, "normal")


class TestSentimentReliability(unittest.TestCase):
    """Test Sentiment Reliability Indicator."""
    
    def test_reliability_low(self):
        result = compute_sentiment_reliability(0.59)
        self.assertEqual(result, "low")
    
    def test_reliability_low_boundary(self):
        result = compute_sentiment_reliability(0.5)
        self.assertEqual(result, "low")
    
    def test_reliability_normal_boundary(self):
        result = compute_sentiment_reliability(0.6)
        self.assertEqual(result, "normal")
    
    def test_reliability_normal_high(self):
        result = compute_sentiment_reliability(0.95)
        self.assertEqual(result, "normal")


class TestComputeRiskIndicators(unittest.TestCase):
    """Test full risk indicator computation."""
    
    def get_valid_input(self):
        return {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "sentiment": {
                "final": {
                    "label": 1,
                    "confidence": 0.85
                }
            },
            "mentions": {
                "count_1h": 1000,
                "velocity": 1.5,
                "anomaly": False
            },
            "fear_greed_index": 50
        }
    
    def test_output_structure(self):
        data = self.get_valid_input()
        result = compute_risk_indicators(data)
        
        # Check top-level fields
        self.assertIn("asset", result)
        self.assertIn("timestamp", result)
        self.assertIn("risk_indicators", result)
        self.assertIn("data_quality", result)
        
        # Check risk_indicators fields
        ri = result["risk_indicators"]
        self.assertIn("sentiment_label", ri)
        self.assertIn("sentiment_confidence", ri)
        self.assertIn("sentiment_reliability", ri)
        self.assertIn("fear_greed_index", ri)
        self.assertIn("fear_greed_zone", ri)
        self.assertIn("social_overheat", ri)
        self.assertIn("panic_risk", ri)
        self.assertIn("fomo_risk", ri)
        
        # Check data_quality fields
        dq = result["data_quality"]
        self.assertIn("status", dq)
        self.assertIn("notes", dq)
    
    def test_valid_input_ok_status(self):
        data = self.get_valid_input()
        result = compute_risk_indicators(data)
        self.assertEqual(result["data_quality"]["status"], "ok")
        self.assertIsNone(result["data_quality"]["notes"])
    
    def test_invalid_input_insufficient_status(self):
        data = {"asset": "BTC"}  # Missing fields
        result = compute_risk_indicators(data)
        self.assertEqual(result["data_quality"]["status"], "insufficient")
        self.assertIsNotNone(result["data_quality"]["notes"])
    
    def test_panic_scenario(self):
        data = self.get_valid_input()
        data["sentiment"]["final"]["label"] = -1
        data["mentions"]["velocity"] = 2.5
        
        result = compute_risk_indicators(data)
        self.assertTrue(result["risk_indicators"]["panic_risk"])
    
    def test_fomo_scenario(self):
        data = self.get_valid_input()
        data["sentiment"]["final"]["label"] = 1
        data["fear_greed_index"] = 75
        
        result = compute_risk_indicators(data)
        self.assertTrue(result["risk_indicators"]["fomo_risk"])
    
    def test_overheat_scenario(self):
        data = self.get_valid_input()
        data["mentions"]["velocity"] = 3.5
        data["mentions"]["anomaly"] = True
        
        result = compute_risk_indicators(data)
        self.assertTrue(result["risk_indicators"]["social_overheat"])
    
    def test_extreme_fear_zone(self):
        data = self.get_valid_input()
        data["fear_greed_index"] = 15
        
        result = compute_risk_indicators(data)
        self.assertEqual(result["risk_indicators"]["fear_greed_zone"], "extreme_fear")
    
    def test_extreme_greed_zone(self):
        data = self.get_valid_input()
        data["fear_greed_index"] = 85
        
        result = compute_risk_indicators(data)
        self.assertEqual(result["risk_indicators"]["fear_greed_zone"], "extreme_greed")
    
    def test_low_reliability(self):
        data = self.get_valid_input()
        data["sentiment"]["final"]["confidence"] = 0.4
        
        result = compute_risk_indicators(data)
        self.assertEqual(result["risk_indicators"]["sentiment_reliability"], "low")
    
    def test_null_fear_greed_index(self):
        data = self.get_valid_input()
        data["fear_greed_index"] = None
        
        result = compute_risk_indicators(data)
        self.assertIsNone(result["risk_indicators"]["fear_greed_index"])
        self.assertEqual(result["risk_indicators"]["fear_greed_zone"], "unknown")
        self.assertFalse(result["risk_indicators"]["fomo_risk"])  # Can't be FOMO without index


class TestOutputDeterminism(unittest.TestCase):
    """Test that output is deterministic."""
    
    def test_same_input_same_output(self):
        data = {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "sentiment": {
                "final": {
                    "label": 1,
                    "confidence": 0.85
                }
            },
            "mentions": {
                "count_1h": 1000,
                "velocity": 1.5,
                "anomaly": False
            },
            "fear_greed_index": 50
        }
        
        result1 = compute_risk_indicators(data)
        result2 = compute_risk_indicators(data)
        
        self.assertEqual(result1, result2)


class TestBatchProcessing(unittest.TestCase):
    """Test batch processing."""
    
    def test_batch_multiple_records(self):
        records = [
            {
                "asset": "BTC",
                "timestamp": "2026-01-17T10:30:00Z",
                "sentiment": {"final": {"label": 1, "confidence": 0.85}},
                "mentions": {"count_1h": 1000, "velocity": 1.5, "anomaly": False},
                "fear_greed_index": 50
            },
            {
                "asset": "ETH",
                "timestamp": "2026-01-17T10:31:00Z",
                "sentiment": {"final": {"label": -1, "confidence": 0.9}},
                "mentions": {"count_1h": 500, "velocity": 2.5, "anomaly": True},
                "fear_greed_index": 25
            }
        ]
        
        results = process_batch(records)
        
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["asset"], "BTC")
        self.assertEqual(results[1]["asset"], "ETH")


class TestNoExtraFields(unittest.TestCase):
    """Test that no extra fields are added."""
    
    def test_no_extra_top_level_fields(self):
        data = {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "sentiment": {"final": {"label": 1, "confidence": 0.85}},
            "mentions": {"count_1h": 1000, "velocity": 1.5, "anomaly": False},
            "fear_greed_index": 50
        }
        
        result = compute_risk_indicators(data)
        expected_keys = {"asset", "timestamp", "risk_indicators", "data_quality"}
        self.assertEqual(set(result.keys()), expected_keys)
    
    def test_no_extra_risk_indicator_fields(self):
        data = {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "sentiment": {"final": {"label": 1, "confidence": 0.85}},
            "mentions": {"count_1h": 1000, "velocity": 1.5, "anomaly": False},
            "fear_greed_index": 50
        }
        
        result = compute_risk_indicators(data)
        expected_keys = {
            "sentiment_label", "sentiment_confidence", "sentiment_reliability",
            "fear_greed_index", "fear_greed_zone",
            "social_overheat", "panic_risk", "fomo_risk"
        }
        self.assertEqual(set(result["risk_indicators"].keys()), expected_keys)
    
    def test_no_extra_data_quality_fields(self):
        data = {
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "sentiment": {"final": {"label": 1, "confidence": 0.85}},
            "mentions": {"count_1h": 1000, "velocity": 1.5, "anomaly": False},
            "fear_greed_index": 50
        }
        
        result = compute_risk_indicators(data)
        expected_keys = {"status", "notes"}
        self.assertEqual(set(result["data_quality"].keys()), expected_keys)


if __name__ == "__main__":
    unittest.main()
