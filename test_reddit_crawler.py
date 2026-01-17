"""
Unit Tests for Reddit Social Data Crawler
"""

import unittest
import math
from datetime import datetime, timezone, timedelta
from reddit_crawler import (
    contains_asset_keyword,
    safe_log,
    compute_engagement_weight,
    compute_author_weight,
    timestamp_to_iso,
    RedditCrawler,
    MentionTracker,
    SOURCE_RELIABILITY,
    DEFAULT_SUBREDDITS
)


class TestAssetFilter(unittest.TestCase):
    """Test asset keyword filtering."""
    
    def test_dollar_btc(self):
        self.assertTrue(contains_asset_keyword("Buy $BTC now!"))
    
    def test_hash_btc(self):
        self.assertTrue(contains_asset_keyword("Trending #BTC"))
    
    def test_bitcoin_lowercase(self):
        self.assertTrue(contains_asset_keyword("bitcoin is rising"))
    
    def test_bitcoin_uppercase(self):
        self.assertTrue(contains_asset_keyword("BITCOIN to the moon"))
    
    def test_bitcoin_mixed_case(self):
        self.assertTrue(contains_asset_keyword("Bitcoin breaking out"))
    
    def test_no_match(self):
        self.assertFalse(contains_asset_keyword("Ethereum is great"))
    
    def test_empty_string(self):
        self.assertFalse(contains_asset_keyword(""))
    
    def test_none(self):
        self.assertFalse(contains_asset_keyword(None))


class TestSourceReliability(unittest.TestCase):
    """Test source reliability is fixed."""
    
    def test_reliability_value(self):
        self.assertEqual(SOURCE_RELIABILITY, 0.7)


class TestDefaultSubreddits(unittest.TestCase):
    """Test default subreddits are configured."""
    
    def test_has_cryptocurrency(self):
        self.assertIn("CryptoCurrency", DEFAULT_SUBREDDITS)
    
    def test_has_bitcoin(self):
        self.assertIn("Bitcoin", DEFAULT_SUBREDDITS)
    
    def test_has_cryptomarkets(self):
        self.assertIn("CryptoMarkets", DEFAULT_SUBREDDITS)
    
    def test_has_bitcoinmarkets(self):
        self.assertIn("BitcoinMarkets", DEFAULT_SUBREDDITS)


class TestSafeLog(unittest.TestCase):
    """Test safe logarithm function."""
    
    def test_zero(self):
        self.assertEqual(safe_log(0), math.log(1))
    
    def test_positive(self):
        self.assertEqual(safe_log(10), math.log(11))
    
    def test_negative(self):
        self.assertEqual(safe_log(-5), math.log(1))


class TestEngagementWeight(unittest.TestCase):
    """Test engagement weight calculation."""
    
    def test_basic_calculation(self):
        # engagement_weight = log(1 + upvotes + number_of_comments)
        result = compute_engagement_weight(100, 50)
        expected = math.log(1 + 100 + 50)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_zero_values(self):
        result = compute_engagement_weight(0, 0)
        expected = math.log(1)
        self.assertAlmostEqual(result, expected, places=5)


class TestAuthorWeight(unittest.TestCase):
    """Test author weight calculation."""
    
    def test_basic_calculation(self):
        # author_weight = log(1 + author_karma)
        result = compute_author_weight(15000)
        expected = math.log(1 + 15000)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_zero_karma(self):
        result = compute_author_weight(0)
        expected = math.log(1)
        self.assertAlmostEqual(result, expected, places=5)


class TestTimestampConversion(unittest.TestCase):
    """Test timestamp conversion."""
    
    def test_to_iso(self):
        # Unix timestamp for 2026-01-17T10:30:00Z = 1768645800
        result = timestamp_to_iso(1768645800.0)
        self.assertEqual(result, "2026-01-17T10:30:00Z")


class TestMentionTracker(unittest.TestCase):
    """Test mention tracking for velocity calculation."""
    
    def test_add_mention(self):
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        tracker.add_mention(now)
        self.assertEqual(len(tracker.mentions), 1)
    
    def test_mentions_in_window(self):
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        # Add mentions within last 6 hours
        for i in range(5):
            tracker.add_mention(now - timedelta(hours=i))
        
        # Add mention outside 6 hour window
        tracker.add_mention(now - timedelta(hours=10))
        
        count = tracker.get_mentions_in_window(6)
        self.assertEqual(count, 5)
    
    def test_velocity_calculation(self):
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        # Add 10 mentions in last 6 hours
        for i in range(10):
            tracker.add_mention(now - timedelta(hours=i % 6))
        
        velocity = tracker.compute_velocity()
        # Should be > 0
        self.assertGreater(velocity, 0)


class TestRedditCrawlerValidation(unittest.TestCase):
    """Test Reddit crawler validation logic."""
    
    def setUp(self):
        self.crawler = RedditCrawler()
    
    def get_valid_post(self):
        return {
            "id": "abc123",
            "title": "Bitcoin breaking out! $BTC to 100k",
            "selftext": "Great analysis here.",
            "author": "crypto_user",
            "score": 100,
            "num_comments": 25,
            "created_utc": 1737107400.0,
            "is_self": True
        }
    
    def get_valid_comment(self):
        return {
            "id": "comment1",
            "body": "Bitcoin is amazing! $BTC moon!",
            "author": "btc_hodler",
            "score": 50,
            "depth": 1,
            "created_utc": 1737107400.0
        }
    
    def test_valid_post(self):
        post = self.get_valid_post()
        self.assertTrue(self.crawler._validate_post(post))
    
    def test_deleted_author(self):
        post = self.get_valid_post()
        post["author"] = "[deleted]"
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_removed_author(self):
        post = self.get_valid_post()
        post["author"] = "[removed]"
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_zero_score(self):
        post = self.get_valid_post()
        post["score"] = 0
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_negative_score(self):
        post = self.get_valid_post()
        post["score"] = -5
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_no_comments(self):
        post = self.get_valid_post()
        post["num_comments"] = 0
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_no_asset_keyword(self):
        post = self.get_valid_post()
        post["title"] = "Ethereum is great!"
        post["selftext"] = "ETH to the moon"
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_empty_text(self):
        post = self.get_valid_post()
        post["title"] = ""
        post["selftext"] = ""
        self.assertFalse(self.crawler._validate_post(post))
    
    def test_valid_comment(self):
        comment = self.get_valid_comment()
        self.assertTrue(self.crawler._validate_comment(comment))
    
    def test_deleted_comment_author(self):
        comment = self.get_valid_comment()
        comment["author"] = "[deleted]"
        self.assertFalse(self.crawler._validate_comment(comment))
    
    def test_removed_comment_body(self):
        comment = self.get_valid_comment()
        comment["body"] = "[removed]"
        self.assertFalse(self.crawler._validate_comment(comment))
    
    def test_empty_comment_body(self):
        comment = self.get_valid_comment()
        comment["body"] = ""
        self.assertFalse(self.crawler._validate_comment(comment))
    
    def test_comment_no_asset_keyword(self):
        comment = self.get_valid_comment()
        comment["body"] = "Ethereum is better"
        self.assertFalse(self.crawler._validate_comment(comment))


class TestNormalization(unittest.TestCase):
    """Test record normalization."""
    
    def setUp(self):
        self.crawler = RedditCrawler()
    
    def get_valid_post(self):
        return {
            "id": "abc123",
            "title": "Bitcoin breaking out! $BTC",
            "selftext": "Great analysis.",
            "author": "crypto_user",
            "score": 100,
            "num_comments": 25,
            "created_utc": 1737107400.0,
            "is_self": True
        }
    
    def get_valid_comment(self):
        return {
            "id": "comment1",
            "body": "Bitcoin is amazing! $BTC",
            "author": "btc_hodler",
            "score": 50,
            "depth": 2,
            "created_utc": 1737107400.0
        }
    
    def test_normalize_post(self):
        post = self.get_valid_post()
        result = self.crawler.normalize_post(post, velocity=1.5)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "reddit")
        self.assertEqual(result.asset, "BTC")
        self.assertEqual(result.velocity, 1.5)
        self.assertEqual(result.source_reliability, 0.7)
    
    def test_normalize_post_output_format(self):
        post = self.get_valid_post()
        result = self.crawler.normalize_post(post, velocity=1.5)
        output = result.to_dict()
        
        # Check structure
        self.assertIn("source", output)
        self.assertIn("asset", output)
        self.assertIn("timestamp", output)
        self.assertIn("text", output)
        self.assertIn("metrics", output)
        self.assertIn("source_reliability", output)
        
        # Check metrics
        metrics = output["metrics"]
        self.assertIn("engagement_weight", metrics)
        self.assertIn("author_weight", metrics)
        self.assertIn("velocity", metrics)
    
    def test_normalize_comment(self):
        comment = self.get_valid_comment()
        result = self.crawler.normalize_comment(comment, velocity=2.0)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "reddit")
        self.assertEqual(result.asset, "BTC")
        self.assertEqual(result.velocity, 2.0)
    
    def test_normalize_post_missing_score(self):
        post = self.get_valid_post()
        del post["score"]
        result = self.crawler.normalize_post(post, velocity=1.0)
        self.assertIsNone(result)
    
    def test_normalize_post_missing_timestamp(self):
        post = self.get_valid_post()
        del post["created_utc"]
        result = self.crawler.normalize_post(post, velocity=1.0)
        self.assertIsNone(result)
    
    def test_normalize_comment_missing_score(self):
        comment = self.get_valid_comment()
        del comment["score"]
        result = self.crawler.normalize_comment(comment, velocity=1.0)
        self.assertIsNone(result)


class TestProcessRawData(unittest.TestCase):
    """Test processing of raw data."""
    
    def setUp(self):
        self.crawler = RedditCrawler()
    
    def test_process_valid_data(self):
        posts = [{
            "id": "abc123",
            "title": "Bitcoin $BTC breaking out!",
            "selftext": "Analysis here.",
            "author": "user1",
            "score": 100,
            "num_comments": 25,
            "created_utc": 1737107400.0,
            "is_self": True
        }]
        
        comments = [{
            "id": "comment1",
            "body": "Bitcoin is great! $BTC",
            "author": "user2",
            "score": 50,
            "depth": 1,
            "created_utc": 1737107460.0
        }]
        
        results = self.crawler.process_raw_data(posts, comments)
        
        self.assertEqual(len(results), 2)
    
    def test_process_drops_invalid(self):
        posts = [
            {
                "id": "abc123",
                "title": "Bitcoin $BTC!",
                "selftext": "",
                "author": "user1",
                "score": 100,
                "num_comments": 25,
                "created_utc": 1737107400.0,
                "is_self": True
            },
            {
                "id": "def456",
                "title": "Ethereum ETH!",  # No BTC keyword
                "selftext": "",
                "author": "user2",
                "score": 50,
                "num_comments": 10,
                "created_utc": 1737107460.0,
                "is_self": True
            }
        ]
        
        results = self.crawler.process_raw_data(posts, [])
        
        self.assertEqual(len(results), 1)


class TestOutputFormat(unittest.TestCase):
    """Test output format matches specification."""
    
    def test_exact_output_structure(self):
        crawler = RedditCrawler()
        
        post = {
            "id": "abc123",
            "title": "Bitcoin $BTC breaking out!",
            "selftext": "Analysis.",
            "author": "user1",
            "score": 100,
            "num_comments": 25,
            "created_utc": 1737107400.0,
            "is_self": True
        }
        
        result = crawler.normalize_post(post, velocity=1.5)
        output = result.to_dict()
        
        # Verify exact structure
        expected_keys = {"source", "asset", "timestamp", "text", "metrics", "source_reliability"}
        self.assertEqual(set(output.keys()), expected_keys)
        
        expected_metric_keys = {"engagement_weight", "author_weight", "velocity"}
        self.assertEqual(set(output["metrics"].keys()), expected_metric_keys)
        
        # Verify values
        self.assertEqual(output["source"], "reddit")
        self.assertEqual(output["asset"], "BTC")
        self.assertEqual(output["source_reliability"], 0.7)


class TestDropRules(unittest.TestCase):
    """Test DROP rules are enforced."""
    
    def setUp(self):
        self.crawler = RedditCrawler()
    
    def test_drop_missing_engagement(self):
        post = {
            "id": "abc123",
            "title": "Bitcoin $BTC",
            "selftext": "",
            "author": "user1",
            # Missing score and num_comments
            "created_utc": 1737107400.0,
            "is_self": True
        }
        
        result = self.crawler.normalize_post(post, velocity=1.0)
        self.assertIsNone(result)
    
    def test_drop_missing_asset_keyword(self):
        post = {
            "id": "abc123",
            "title": "Ethereum is great!",
            "selftext": "ETH analysis",
            "author": "user1",
            "score": 100,
            "num_comments": 25,
            "created_utc": 1737107400.0,
            "is_self": True
        }
        
        result = self.crawler.normalize_post(post, velocity=1.0)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
