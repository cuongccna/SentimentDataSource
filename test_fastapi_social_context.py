"""
Unit tests for FastAPI Social Context Service.
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

from fastapi_social_context import (
    app,
    SocialDataStorage,
    SocialContextService,
    SocialContextRequest,
    SocialContextResponse,
    parse_timestamp,
    format_timestamp,
    get_storage,
    get_service,
    create_app,
    MIN_WINDOW_SECONDS,
    MAX_WINDOW_SECONDS,
    SOURCE_RELIABILITY,
    VALID_SOURCES
)


class TestConstants(unittest.TestCase):
    """Tests for constants."""
    
    def test_min_window_seconds(self):
        self.assertEqual(MIN_WINDOW_SECONDS, 30)
    
    def test_max_window_seconds(self):
        self.assertEqual(MAX_WINDOW_SECONDS, 300)
    
    def test_source_reliability_twitter(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY["twitter"], 0.5, places=5)
    
    def test_source_reliability_reddit(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY["reddit"], 0.7, places=5)
    
    def test_source_reliability_telegram(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY["telegram"], 0.3, places=5)
    
    def test_valid_sources(self):
        self.assertEqual(VALID_SOURCES, {"twitter", "reddit", "telegram"})


class TestHelperFunctions(unittest.TestCase):
    """Tests for helper functions."""
    
    def test_parse_timestamp_z_suffix(self):
        dt = parse_timestamp("2026-01-17T10:00:00Z")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 17)
        self.assertEqual(dt.hour, 10)
    
    def test_parse_timestamp_offset(self):
        dt = parse_timestamp("2026-01-17T10:00:00+00:00")
        self.assertEqual(dt.hour, 10)
    
    def test_format_timestamp(self):
        dt = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        formatted = format_timestamp(dt)
        self.assertEqual(formatted, "2026-01-17T10:00:00Z")


class TestSocialDataStorage(unittest.TestCase):
    """Tests for SocialDataStorage."""
    
    def setUp(self):
        self.storage = SocialDataStorage()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.storage.clear()
    
    def test_add_record(self):
        record = {
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now)
        }
        self.storage.add_record(record)
        self.assertEqual(len(self.storage._records), 1)
    
    def test_query_by_asset(self):
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now)
        })
        self.storage.add_record({
            "asset": "ETH",
            "source": "twitter",
            "timestamp": format_timestamp(self.now)
        })
        
        results = self.storage.query_records(
            asset="BTC",
            since=self.now - timedelta(minutes=1),
            until=self.now + timedelta(minutes=1),
            sources=["twitter"]
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["asset"], "BTC")
    
    def test_query_by_source(self):
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now)
        })
        self.storage.add_record({
            "asset": "BTC",
            "source": "reddit",
            "timestamp": format_timestamp(self.now)
        })
        
        results = self.storage.query_records(
            asset="BTC",
            since=self.now - timedelta(minutes=1),
            until=self.now + timedelta(minutes=1),
            sources=["twitter"]
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["source"], "twitter")
    
    def test_query_by_time_window(self):
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now - timedelta(minutes=5))
        })
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now)
        })
        
        results = self.storage.query_records(
            asset="BTC",
            since=self.now - timedelta(minutes=1),
            until=self.now + timedelta(minutes=1),
            sources=["twitter"]
        )
        self.assertEqual(len(results), 1)
    
    def test_query_empty_results(self):
        results = self.storage.query_records(
            asset="BTC",
            since=self.now - timedelta(minutes=1),
            until=self.now + timedelta(minutes=1),
            sources=["twitter"]
        )
        self.assertEqual(len(results), 0)
    
    def test_clear(self):
        self.storage.add_record({"asset": "BTC", "timestamp": format_timestamp(self.now)})
        self.storage.clear()
        self.assertEqual(len(self.storage._records), 0)


class TestSocialContextService(unittest.TestCase):
    """Tests for SocialContextService."""
    
    def setUp(self):
        self.storage = SocialDataStorage()
        self.service = SocialContextService(self.storage)
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.storage.clear()
    
    def test_get_context_no_records(self):
        result = self.service.get_social_context(
            asset="BTC",
            since=self.now - timedelta(minutes=1),
            until=self.now + timedelta(minutes=1),
            sources=["twitter"]
        )
        self.assertIsNone(result)
    
    def test_get_context_with_records(self):
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now),
            "sentiment": {"label": 1, "confidence": 0.8},
            "risk_indicators": {},
            "data_quality": {}
        })
        
        result = self.service.get_social_context(
            asset="BTC",
            since=self.now - timedelta(minutes=1),
            until=self.now + timedelta(minutes=1),
            sources=["twitter"]
        )
        
        self.assertIsNotNone(result)
        self.assertIn("sentiment", result)
        self.assertIn("risk_indicators", result)
        self.assertIn("data_quality", result)
    
    def test_aggregate_sentiment_single(self):
        records = [{
            "source": "twitter",
            "sentiment": {"label": 1, "confidence": 0.8}
        }]
        
        result = self.service._aggregate_sentiment(records)
        self.assertEqual(result["label"], 1)
        self.assertAlmostEqual(result["confidence"], 0.8, places=4)
    
    def test_aggregate_sentiment_weighted_majority(self):
        # Reddit has higher reliability (0.7) than Twitter (0.5)
        records = [
            {"source": "twitter", "sentiment": {"label": 1, "confidence": 0.8}},
            {"source": "reddit", "sentiment": {"label": -1, "confidence": 0.9}}
        ]
        
        result = self.service._aggregate_sentiment(records)
        # Reddit's weight: 0.7 * 0.9 = 0.63 for bearish
        # Twitter's weight: 0.5 * 0.8 = 0.40 for bullish
        self.assertEqual(result["label"], -1)
    
    def test_aggregate_sentiment_average_confidence(self):
        records = [
            {"source": "twitter", "sentiment": {"label": 1, "confidence": 0.8}},
            {"source": "reddit", "sentiment": {"label": 1, "confidence": 0.6}}
        ]
        
        result = self.service._aggregate_sentiment(records)
        self.assertAlmostEqual(result["confidence"], 0.7, places=4)
    
    def test_aggregate_risk_boolean_or(self):
        records = [
            {"timestamp": "2026-01-17T10:00:00Z", "risk_indicators": {"social_overheat": False, "panic_risk": True}},
            {"timestamp": "2026-01-17T10:00:01Z", "risk_indicators": {"social_overheat": True, "panic_risk": False}}
        ]
        
        result = self.service._aggregate_risk_indicators(records)
        self.assertTrue(result["social_overheat"])
        self.assertTrue(result["panic_risk"])
    
    def test_aggregate_risk_most_recent(self):
        records = [
            {"timestamp": "2026-01-17T10:00:00Z", "risk_indicators": {"fear_greed_index": 50}},
            {"timestamp": "2026-01-17T10:00:01Z", "risk_indicators": {"fear_greed_index": 70}}
        ]
        
        result = self.service._aggregate_risk_indicators(records)
        self.assertEqual(result["fear_greed_index"], 70)
    
    def test_aggregate_quality_worst_overall(self):
        records = [
            {"data_quality": {"overall": "healthy"}},
            {"data_quality": {"overall": "degraded"}},
            {"data_quality": {"overall": "healthy"}}
        ]
        
        result = self.service._aggregate_data_quality(records)
        self.assertEqual(result["overall"], "degraded")
    
    def test_aggregate_quality_most_frequent(self):
        records = [
            {"data_quality": {"volume": "normal"}},
            {"data_quality": {"volume": "normal"}},
            {"data_quality": {"volume": "abnormally_low"}}
        ]
        
        result = self.service._aggregate_data_quality(records)
        self.assertEqual(result["volume"], "normal")


class TestFastAPIEndpoint(unittest.TestCase):
    """Tests for FastAPI endpoint."""
    
    def setUp(self):
        self.client = TestClient(app)
        self.storage = get_storage()
        self.now = datetime.now(timezone.utc)
        self.storage.clear()
    
    def tearDown(self):
        self.storage.clear()
    
    def test_health_check(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "healthy")
    
    def test_valid_request_no_content(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=2)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 204)
    
    def test_valid_request_with_data(self):
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now - timedelta(seconds=30)),
            "sentiment": {"label": 1, "confidence": 0.8},
            "risk_indicators": {
                "social_overheat": False,
                "panic_risk": False,
                "fomo_risk": False,
                "fear_greed_index": 65,
                "fear_greed_zone": "normal"
            },
            "data_quality": {
                "overall": "healthy",
                "availability": "ok",
                "time_integrity": "ok",
                "volume": "normal",
                "source_balance": "normal",
                "anomaly_frequency": "normal"
            }
        })
        
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("meta", data)
        self.assertIn("social_context", data)
    
    def test_response_format(self):
        self.storage.add_record({
            "asset": "BTC",
            "source": "twitter",
            "timestamp": format_timestamp(self.now - timedelta(seconds=30)),
            "sentiment": {"label": 1, "confidence": 0.8},
            "risk_indicators": {
                "social_overheat": False,
                "panic_risk": False,
                "fomo_risk": False,
                "fear_greed_index": 65,
                "fear_greed_zone": "normal"
            },
            "data_quality": {
                "overall": "healthy",
                "availability": "ok",
                "time_integrity": "ok",
                "volume": "normal",
                "source_balance": "normal",
                "anomaly_frequency": "normal"
            }
        })
        
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        
        data = response.json()
        
        # Check meta
        self.assertEqual(data["meta"]["asset"], "BTC")
        self.assertIn("window", data["meta"])
        self.assertIn("generated_at", data["meta"])
        
        # Check social_context
        self.assertIn("sentiment", data["social_context"])
        self.assertIn("risk_indicators", data["social_context"])
        self.assertIn("data_quality", data["social_context"])
        
        # Check sentiment structure
        self.assertIn("label", data["social_context"]["sentiment"])
        self.assertIn("confidence", data["social_context"]["sentiment"])
        
        # Check risk_indicators structure
        self.assertIn("sentiment_reliability", data["social_context"]["risk_indicators"])
        self.assertIn("fear_greed_index", data["social_context"]["risk_indicators"])
        self.assertIn("fear_greed_zone", data["social_context"]["risk_indicators"])
        self.assertIn("social_overheat", data["social_context"]["risk_indicators"])
        self.assertIn("panic_risk", data["social_context"]["risk_indicators"])
        self.assertIn("fomo_risk", data["social_context"]["risk_indicators"])
        
        # Check data_quality structure
        self.assertIn("overall", data["social_context"]["data_quality"])
        self.assertIn("availability", data["social_context"]["data_quality"])
        self.assertIn("time_integrity", data["social_context"]["data_quality"])
        self.assertIn("volume", data["social_context"]["data_quality"])
        self.assertIn("source_balance", data["social_context"]["data_quality"])
        self.assertIn("anomaly_frequency", data["social_context"]["data_quality"])


class TestRequestValidation(unittest.TestCase):
    """Tests for request validation."""
    
    def setUp(self):
        self.client = TestClient(app)
        self.now = datetime.now(timezone.utc)
    
    def test_missing_asset(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_missing_since(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_missing_until(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_empty_sources(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": []
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_invalid_source(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": ["invalid_source"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_since_after_until(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now),
                "until": format_timestamp(self.now - timedelta(minutes=1)),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_window_too_small(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(seconds=20)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_window_too_large(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=10)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        self.assertEqual(response.status_code, 422)
    
    def test_valid_window_30_seconds(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(seconds=30)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        # Should not be a validation error (can be 204 if no data)
        self.assertIn(response.status_code, [200, 204])
    
    def test_valid_window_5_minutes(self):
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=5)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        self.assertIn(response.status_code, [200, 204])


class TestMultipleSourceAggregation(unittest.TestCase):
    """Tests for aggregation across multiple sources."""
    
    def setUp(self):
        self.client = TestClient(app)
        self.storage = get_storage()
        self.now = datetime.now(timezone.utc)
        self.storage.clear()
    
    def tearDown(self):
        self.storage.clear()
    
    def test_aggregate_all_sources(self):
        # Add records from all sources
        for source in ["twitter", "reddit", "telegram"]:
            self.storage.add_record({
                "asset": "BTC",
                "source": source,
                "timestamp": format_timestamp(self.now - timedelta(seconds=30)),
                "sentiment": {"label": 1, "confidence": 0.8},
                "risk_indicators": {
                    "social_overheat": False,
                    "panic_risk": False,
                    "fomo_risk": False,
                    "fear_greed_index": 65,
                    "fear_greed_zone": "normal"
                },
                "data_quality": {
                    "overall": "healthy",
                    "availability": "ok",
                    "time_integrity": "ok",
                    "volume": "normal",
                    "source_balance": "normal",
                    "anomaly_frequency": "normal"
                }
            })
        
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": ["twitter", "reddit", "telegram"]
            }
        )
        
        self.assertEqual(response.status_code, 200)
    
    def test_filter_by_sources(self):
        # Add records from all sources
        for source in ["twitter", "reddit", "telegram"]:
            self.storage.add_record({
                "asset": "BTC",
                "source": source,
                "timestamp": format_timestamp(self.now - timedelta(seconds=30)),
                "sentiment": {"label": 1, "confidence": 0.8},
                "risk_indicators": {},
                "data_quality": {}
            })
        
        # Query only twitter
        response = self.client.post(
            "/api/v1/social/context",
            json={
                "asset": "BTC",
                "since": format_timestamp(self.now - timedelta(minutes=1)),
                "until": format_timestamp(self.now),
                "sources": ["twitter"]
            }
        )
        
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
