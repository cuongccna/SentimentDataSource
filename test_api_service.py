"""
Unit Tests for Social Sentiment Analysis API
"""

import unittest
import json
from api_service import app, validate_request_structure, build_fallback_response


class TestAPIValidation(unittest.TestCase):
    """Test request validation."""
    
    def test_valid_request(self):
        data = {"records": [{"source": "twitter", "asset": "BTC", "text": "test"}]}
        is_valid, error = validate_request_structure(data)
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_missing_records(self):
        data = {}
        is_valid, error = validate_request_structure(data)
        self.assertFalse(is_valid)
        self.assertIn("records", error)
    
    def test_records_not_array(self):
        data = {"records": "not an array"}
        is_valid, error = validate_request_structure(data)
        self.assertFalse(is_valid)
        self.assertIn("array", error)
    
    def test_empty_records_array(self):
        data = {"records": []}
        is_valid, error = validate_request_structure(data)
        self.assertFalse(is_valid)
        self.assertIn("empty", error)
    
    def test_null_data(self):
        is_valid, error = validate_request_structure(None)
        self.assertFalse(is_valid)


class TestFallbackResponse(unittest.TestCase):
    """Test fallback response structure."""
    
    def test_fallback_structure(self):
        response = build_fallback_response(5, 5, "BTC")
        
        self.assertIn("meta", response)
        self.assertIn("results", response)
        self.assertIn("risk_flag", response)
        
        self.assertEqual(response["results"], [])
        self.assertEqual(response["meta"]["record_processed"], 0)
        self.assertEqual(response["meta"]["record_dropped"], 5)
        
        self.assertTrue(response["risk_flag"]["sentiment_unavailable"])
        self.assertEqual(response["risk_flag"]["action"], "BLOCK_TRADING")


class TestAPIEndpoint(unittest.TestCase):
    """Test API endpoint responses."""
    
    def setUp(self):
        self.client = app.test_client()
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def get_valid_record(self, text="moon breakout"):
        return {
            "source": "twitter",
            "asset": "BTC",
            "text": text,
            "timestamp": "2026-01-17T10:30:00Z",
            "engagement": {
                "like": 100,
                "reply": 10,
                "share": 5
            },
            "author": {
                "followers": 1000,
                "reputation_score": 0.8
            }
        }
    
    def test_successful_request(self):
        """Test 200 response with valid records."""
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        self.assertEqual(response.status_code, 200)
        
        result = json.loads(response.data)
        self.assertIn("meta", result)
        self.assertIn("results", result)
        self.assertEqual(result["meta"]["record_processed"], 1)
        self.assertEqual(len(result["results"]), 1)
    
    def test_missing_content_type(self):
        """Test 400 response when Content-Type is missing."""
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers={"Accept": "application/json"}
        )
        
        self.assertEqual(response.status_code, 400)
    
    def test_invalid_json(self):
        """Test 400 response for invalid JSON."""
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data="not valid json{",
            headers=self.headers
        )
        
        self.assertEqual(response.status_code, 400)
    
    def test_missing_records_field(self):
        """Test 400 response when records field is missing."""
        data = {"data": []}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        self.assertEqual(response.status_code, 400)
    
    def test_empty_records_array(self):
        """Test 422 response for empty records array."""
        data = {"records": []}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        self.assertEqual(response.status_code, 422)
    
    def test_all_records_dropped(self):
        """Test 422 response when all records are invalid."""
        data = {
            "records": [
                {"source": "twitter"},  # Missing fields
                {"asset": "BTC"},  # Missing fields
            ]
        }
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        self.assertEqual(response.status_code, 422)
        
        result = json.loads(response.data)
        self.assertIn("risk_flag", result)
        self.assertTrue(result["risk_flag"]["sentiment_unavailable"])
        self.assertEqual(result["risk_flag"]["action"], "BLOCK_TRADING")
    
    def test_partial_valid_records(self):
        """Test 200 response when some records are valid."""
        data = {
            "records": [
                self.get_valid_record(),  # Valid
                {"source": "twitter"},  # Invalid - will be dropped
            ]
        }
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        self.assertEqual(response.status_code, 200)
        
        result = json.loads(response.data)
        self.assertEqual(result["meta"]["record_received"], 2)
        self.assertEqual(result["meta"]["record_processed"], 1)
        self.assertEqual(result["meta"]["record_dropped"], 1)
    
    def test_response_structure_meta(self):
        """Test meta structure in response."""
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        result = json.loads(response.data)
        meta = result["meta"]
        
        self.assertIn("asset", meta)
        self.assertIn("record_received", meta)
        self.assertIn("record_processed", meta)
        self.assertIn("record_dropped", meta)
        self.assertIn("timestamp", meta)
    
    def test_response_structure_results(self):
        """Test results structure in response."""
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        result = json.loads(response.data)
        self.assertIsInstance(result["results"], list)
        
        if result["results"]:
            item = result["results"][0]
            self.assertIn("asset", item)
            self.assertIn("timestamp", item)
            self.assertIn("source", item)
            self.assertIn("sentiment", item)
            
            sentiment = item["sentiment"]
            self.assertIn("rule_based", sentiment)
            self.assertIn("llm", sentiment)
            self.assertIn("final", sentiment)
    
    def test_bullish_sentiment(self):
        """Test bullish sentiment processing."""
        data = {"records": [self.get_valid_record("BTC to the moon! Breakout!")]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        result = json.loads(response.data)
        sentiment = result["results"][0]["sentiment"]
        
        self.assertGreater(sentiment["rule_based"]["bullish"], 0)
        self.assertEqual(sentiment["final"]["label"], 1)
    
    def test_bearish_sentiment(self):
        """Test bearish sentiment processing."""
        data = {"records": [self.get_valid_record("This is a dump! Rug pull!")]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        result = json.loads(response.data)
        sentiment = result["results"][0]["sentiment"]
        
        self.assertGreater(sentiment["rule_based"]["bearish"], 0)
        self.assertEqual(sentiment["final"]["label"], -1)
    
    def test_health_endpoint(self):
        """Test health check endpoint."""
        response = self.client.get("/health")
        
        self.assertEqual(response.status_code, 200)
        
        result = json.loads(response.data)
        self.assertEqual(result["status"], "healthy")
    
    def test_method_not_allowed(self):
        """Test 405 for wrong HTTP method."""
        response = self.client.get("/api/v1/sentiment/analyze")
        self.assertEqual(response.status_code, 405)
    
    def test_not_found(self):
        """Test 404 for unknown endpoint."""
        response = self.client.get("/api/v1/unknown")
        self.assertEqual(response.status_code, 404)


class TestResponseGuarantees(unittest.TestCase):
    """Test response guarantees for BotTrading."""
    
    def setUp(self):
        self.client = app.test_client()
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def get_valid_record(self):
        return {
            "source": "twitter",
            "asset": "BTC",
            "text": "moon breakout",
            "timestamp": "2026-01-17T10:30:00Z",
            "engagement": {"like": 100, "reply": 10, "share": 5},
            "author": {"followers": 1000, "reputation_score": 0.8}
        }
    
    def test_root_json_not_null(self):
        """Root JSON object is NEVER null."""
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        
        result = json.loads(response.data)
        self.assertIsNotNone(result)
    
    def test_meta_always_exists(self):
        """'meta' ALWAYS exists."""
        # Test with valid records
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        result = json.loads(response.data)
        self.assertIn("meta", result)
        
        # Test with all invalid records (fallback)
        data = {"records": [{"invalid": "record"}]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        result = json.loads(response.data)
        self.assertIn("meta", result)
    
    def test_results_always_array(self):
        """'results' is ALWAYS an array."""
        # Test with valid records
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        result = json.loads(response.data)
        self.assertIsInstance(result["results"], list)
        
        # Test with all invalid records (fallback)
        data = {"records": [{"invalid": "record"}]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        result = json.loads(response.data)
        self.assertIsInstance(result["results"], list)
    
    def test_no_unexpected_fields_success(self):
        """No unexpected fields in success response."""
        data = {"records": [self.get_valid_record()]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        result = json.loads(response.data)
        
        # Check top-level keys
        expected_keys = {"meta", "results"}
        self.assertEqual(set(result.keys()), expected_keys)
    
    def test_no_unexpected_fields_fallback(self):
        """No unexpected fields in fallback response."""
        data = {"records": [{"invalid": "record"}]}
        response = self.client.post(
            "/api/v1/sentiment/analyze",
            data=json.dumps(data),
            headers=self.headers
        )
        result = json.loads(response.data)
        
        # Check top-level keys
        expected_keys = {"meta", "results", "risk_flag"}
        self.assertEqual(set(result.keys()), expected_keys)


if __name__ == "__main__":
    unittest.main()
