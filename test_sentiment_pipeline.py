"""
Unit Tests for Social Sentiment Pipeline
"""

import unittest
import json
from sentiment_pipeline import (
    validate_record,
    preprocess_text,
    count_lexicon_matches,
    count_regex_matches,
    calculate_raw_score,
    normalize_score,
    assign_label,
    process_record,
    process_batch,
    LEXICON,
    SCORE_WEIGHTS
)


class TestValidation(unittest.TestCase):
    """Test input validation - drop records with missing fields."""
    
    def get_valid_record(self):
        return {
            "source": "twitter",
            "asset": "BTC",
            "text": "Test message",
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
    
    def test_valid_record(self):
        record = self.get_valid_record()
        result = validate_record(record)
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "twitter")
        self.assertEqual(result.asset, "BTC")
    
    def test_missing_source(self):
        record = self.get_valid_record()
        del record["source"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_asset(self):
        record = self.get_valid_record()
        del record["asset"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_text(self):
        record = self.get_valid_record()
        del record["text"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_timestamp(self):
        record = self.get_valid_record()
        del record["timestamp"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_engagement(self):
        record = self.get_valid_record()
        del record["engagement"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_engagement_field(self):
        record = self.get_valid_record()
        del record["engagement"]["like"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_author(self):
        record = self.get_valid_record()
        del record["author"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_missing_author_field(self):
        record = self.get_valid_record()
        del record["author"]["followers"]
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_null_field(self):
        record = self.get_valid_record()
        record["text"] = None
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_invalid_source(self):
        record = self.get_valid_record()
        record["source"] = "facebook"  # Not in allowed list
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_empty_text(self):
        record = self.get_valid_record()
        record["text"] = ""
        result = validate_record(record)
        self.assertIsNone(result)
    
    def test_whitespace_text(self):
        record = self.get_valid_record()
        record["text"] = "   "
        result = validate_record(record)
        self.assertIsNone(result)


class TestPreprocessing(unittest.TestCase):
    """Test text preprocessing."""
    
    def test_lowercase(self):
        processed, original = preprocess_text("BTC MOON BREAKOUT")
        self.assertEqual(processed, "btc moon breakout")
        self.assertEqual(original, "BTC MOON BREAKOUT")
    
    def test_url_removal(self):
        processed, _ = preprocess_text("Check this https://example.com/btc")
        self.assertNotIn("https://", processed)
        self.assertNotIn("example.com", processed)
    
    def test_emoji_removal(self):
        processed, _ = preprocess_text("BTC to the moon 🚀🌙")
        self.assertNotIn("🚀", processed)
        self.assertNotIn("🌙", processed)
    
    def test_preserves_original(self):
        original_text = "BTC MOON 🚀 https://example.com"
        processed, original = preprocess_text(original_text)
        self.assertEqual(original, original_text)


class TestLexiconMatching(unittest.TestCase):
    """Test lexicon term counting."""
    
    def test_bullish_terms(self):
        counts = count_lexicon_matches("btc is going to the moon, breakout incoming")
        self.assertEqual(counts["bullish"], 2)  # moon, breakout
    
    def test_bearish_terms(self):
        counts = count_lexicon_matches("this is a dump, possible hack exploit")
        self.assertEqual(counts["bearish"], 3)  # dump, hack, exploit
    
    def test_fear_terms(self):
        counts = count_lexicon_matches("panic selling, exit now, collapse imminent")
        self.assertEqual(counts["fear"], 3)  # panic, exit, collapse
    
    def test_greed_terms(self):
        counts = count_lexicon_matches("100x gains, lambo soon, yolo all in")
        self.assertEqual(counts["greed"], 4)  # 100x, lambo, all in, yolo
    
    def test_no_matches(self):
        counts = count_lexicon_matches("just a regular message about crypto")
        self.assertEqual(counts["bullish"], 0)
        self.assertEqual(counts["bearish"], 0)
        self.assertEqual(counts["fear"], 0)
        self.assertEqual(counts["greed"], 0)
    
    def test_case_insensitive(self):
        counts = count_lexicon_matches("MOON breakout ACCUMULATION")
        self.assertEqual(counts["bullish"], 3)


class TestRegexMatching(unittest.TestCase):
    """Test regex pattern matching."""
    
    def test_dump_pattern(self):
        counts = count_regex_matches("they are dumping, price dumped hard")
        self.assertEqual(counts["bearish"], 2)  # dumping, dumped
    
    def test_rug_pattern(self):
        counts = count_regex_matches("this is a rug, rugpull incoming")
        self.assertEqual(counts["bearish"], 2)  # rug, rugpull
    
    def test_multiplier_pattern(self):
        counts = count_regex_matches("this will 10x or even 100x")
        self.assertEqual(counts["greed"], 2)  # 10x, 100x
    
    def test_exclamation_pattern(self):
        # Exclamation marks are matched but not assigned to a category
        text = "amazing news!! this is huge!!!"
        counts = count_regex_matches(text)
        # These don't add to any category per REGEX_CATEGORY_MAP
        self.assertEqual(counts["bullish"], 0)


class TestScoring(unittest.TestCase):
    """Test scoring formula."""
    
    def test_raw_score_bullish(self):
        counts = {"bullish": 2, "bearish": 0, "fear": 0, "greed": 0}
        score = calculate_raw_score(counts)
        self.assertEqual(score, 2.0)  # 2 * 1.0
    
    def test_raw_score_bearish(self):
        counts = {"bullish": 0, "bearish": 2, "fear": 0, "greed": 0}
        score = calculate_raw_score(counts)
        self.assertEqual(score, -2.4)  # 2 * -1.2
    
    def test_raw_score_fear(self):
        counts = {"bullish": 0, "bearish": 0, "fear": 2, "greed": 0}
        score = calculate_raw_score(counts)
        self.assertEqual(score, -3.0)  # 2 * -1.5
    
    def test_raw_score_greed(self):
        counts = {"bullish": 0, "bearish": 0, "fear": 0, "greed": 2}
        score = calculate_raw_score(counts)
        self.assertEqual(score, 1.0)  # 2 * 0.5
    
    def test_raw_score_mixed(self):
        counts = {"bullish": 1, "bearish": 1, "fear": 0, "greed": 0}
        score = calculate_raw_score(counts)
        self.assertAlmostEqual(score, -0.2, places=5)  # 1.0 - 1.2


class TestNormalization(unittest.TestCase):
    """Test score normalization."""
    
    def test_normalize_positive(self):
        score = normalize_score(2.0, 2)
        self.assertEqual(score, 1.0)
    
    def test_normalize_negative(self):
        score = normalize_score(-2.0, 2)
        self.assertEqual(score, -1.0)
    
    def test_normalize_clamp_upper(self):
        score = normalize_score(10.0, 2)
        self.assertEqual(score, 1.0)  # Clamped
    
    def test_normalize_clamp_lower(self):
        score = normalize_score(-10.0, 2)
        self.assertEqual(score, -1.0)  # Clamped
    
    def test_normalize_zero_terms(self):
        score = normalize_score(5.0, 0)
        self.assertEqual(score, 0.0)


class TestLabelAssignment(unittest.TestCase):
    """Test label assignment thresholds."""
    
    def test_positive_label(self):
        self.assertEqual(assign_label(0.2), 1)
        self.assertEqual(assign_label(0.5), 1)
        self.assertEqual(assign_label(1.0), 1)
    
    def test_negative_label(self):
        self.assertEqual(assign_label(-0.2), -1)
        self.assertEqual(assign_label(-0.5), -1)
        self.assertEqual(assign_label(-1.0), -1)
    
    def test_neutral_label(self):
        self.assertEqual(assign_label(0.0), 0)
        self.assertEqual(assign_label(0.1), 0)
        self.assertEqual(assign_label(-0.1), 0)
        self.assertEqual(assign_label(0.19), 0)
        self.assertEqual(assign_label(-0.19), 0)


class TestProcessRecord(unittest.TestCase):
    """Test full record processing."""
    
    def get_base_record(self, text="Test message"):
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
    
    def test_bullish_sentiment(self):
        record = self.get_base_record("BTC to the moon! Breakout confirmed!")
        result = process_record(record)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["asset"], "BTC")
        self.assertEqual(result["source"], "twitter")
        self.assertGreater(result["sentiment"]["rule_based"]["bullish"], 0)
        self.assertEqual(result["sentiment"]["final"]["label"], 1)
    
    def test_bearish_sentiment(self):
        record = self.get_base_record("This is a dump! Rug pull! Exploit detected!")
        result = process_record(record)
        
        self.assertIsNotNone(result)
        self.assertGreater(result["sentiment"]["rule_based"]["bearish"], 0)
        self.assertEqual(result["sentiment"]["final"]["label"], -1)
    
    def test_output_format(self):
        record = self.get_base_record("moon breakout")
        result = process_record(record)
        
        # Verify all required fields exist
        self.assertIn("asset", result)
        self.assertIn("timestamp", result)
        self.assertIn("source", result)
        self.assertIn("sentiment", result)
        
        sentiment = result["sentiment"]
        self.assertIn("rule_based", sentiment)
        self.assertIn("llm", sentiment)
        self.assertIn("final", sentiment)
        
        rule_based = sentiment["rule_based"]
        self.assertIn("bullish", rule_based)
        self.assertIn("bearish", rule_based)
        self.assertIn("fear", rule_based)
        self.assertIn("greed", rule_based)
        self.assertIn("score", rule_based)
        self.assertIn("label", rule_based)
        
        llm = sentiment["llm"]
        self.assertIn("used", llm)
        self.assertIn("label", llm)
        self.assertIn("confidence", llm)
        
        final = sentiment["final"]
        self.assertIn("label", final)
        self.assertIn("confidence", final)
    
    def test_invalid_record_dropped(self):
        record = self.get_base_record("Test")
        del record["source"]
        result = process_record(record)
        self.assertIsNone(result)
    
    def test_llm_not_used_when_matches_exist(self):
        record = self.get_base_record("moon breakout")
        result = process_record(record)
        
        self.assertFalse(result["sentiment"]["llm"]["used"])


class TestBatchProcessing(unittest.TestCase):
    """Test batch record processing."""
    
    def test_batch_processing(self):
        records = [
            {
                "source": "twitter",
                "asset": "BTC",
                "text": "moon breakout",
                "timestamp": "2026-01-17T10:30:00Z",
                "engagement": {"like": 100, "reply": 10, "share": 5},
                "author": {"followers": 1000, "reputation_score": 0.8}
            },
            {
                "source": "reddit",
                "asset": "ETH",
                "text": "dump incoming",
                "timestamp": "2026-01-17T10:31:00Z",
                "engagement": {"like": 50, "reply": 5, "share": 2},
                "author": {"followers": 500, "reputation_score": 0.6}
            }
        ]
        
        results = process_batch(records)
        self.assertEqual(len(results), 2)
    
    def test_batch_drops_invalid(self):
        records = [
            {
                "source": "twitter",
                "asset": "BTC",
                "text": "valid record",
                "timestamp": "2026-01-17T10:30:00Z",
                "engagement": {"like": 100, "reply": 10, "share": 5},
                "author": {"followers": 1000, "reputation_score": 0.8}
            },
            {
                "source": "twitter",
                # Missing asset - invalid
                "text": "invalid record",
                "timestamp": "2026-01-17T10:31:00Z",
                "engagement": {"like": 50, "reply": 5, "share": 2},
                "author": {"followers": 500, "reputation_score": 0.6}
            }
        ]
        
        results = process_batch(records)
        self.assertEqual(len(results), 1)  # Only valid record processed


class TestCoefficientsNotModified(unittest.TestCase):
    """Verify scoring coefficients are exactly as specified."""
    
    def test_coefficient_values(self):
        self.assertEqual(SCORE_WEIGHTS["bullish"], 1.0)
        self.assertEqual(SCORE_WEIGHTS["greed"], 0.5)
        self.assertEqual(SCORE_WEIGHTS["bearish"], -1.2)
        self.assertEqual(SCORE_WEIGHTS["fear"], -1.5)


class TestLexiconNotModified(unittest.TestCase):
    """Verify lexicon terms are exactly as specified."""
    
    def test_bullish_terms(self):
        expected = ["moon", "breakout", "accumulation", "buy wall", "whale buying"]
        self.assertEqual(LEXICON["bullish"], expected)
    
    def test_bearish_terms(self):
        expected = ["dump", "rug", "hack", "exploit", "sell wall"]
        self.assertEqual(LEXICON["bearish"], expected)
    
    def test_fear_terms(self):
        expected = ["panic", "exit", "collapse", "bankruptcy"]
        self.assertEqual(LEXICON["fear"], expected)
    
    def test_greed_terms(self):
        expected = ["100x", "lambo", "all in", "yolo"]
        self.assertEqual(LEXICON["greed"], expected)


if __name__ == "__main__":
    unittest.main()
