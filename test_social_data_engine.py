"""
Unit Tests for Social Data Collection & Normalization Engine
"""

import unittest
import math
from social_data_engine import (
    contains_asset_keyword,
    safe_log,
    TwitterCollector,
    RedditCollector,
    TelegramCollector,
    VelocityCalculator,
    SocialDataEngine,
    SOURCE_RELIABILITY,
    get_engine
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
    """Test source reliability values are fixed."""
    
    def test_twitter_reliability(self):
        self.assertEqual(SOURCE_RELIABILITY["twitter"], 0.5)
    
    def test_reddit_reliability(self):
        self.assertEqual(SOURCE_RELIABILITY["reddit"], 0.7)
    
    def test_telegram_reliability(self):
        self.assertEqual(SOURCE_RELIABILITY["telegram"], 0.3)


class TestSafeLog(unittest.TestCase):
    """Test safe logarithm function."""
    
    def test_zero(self):
        self.assertEqual(safe_log(0), math.log(1))
    
    def test_positive(self):
        self.assertEqual(safe_log(10), math.log(11))
    
    def test_negative(self):
        # Negative should be treated as 0
        self.assertEqual(safe_log(-5), math.log(1))


class TestTwitterCollector(unittest.TestCase):
    """Test Twitter data collection and normalization."""
    
    def setUp(self):
        self.collector = TwitterCollector()
    
    def get_valid_record(self):
        return {
            "text": "Bitcoin is going to the moon! $BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 100,
            "retweet_count": 50,
            "reply_count": 25,
            "is_ad": False,
            "author": {
                "followers_count": 5000,
                "is_private": False
            }
        }
    
    def test_valid_record(self):
        record = self.get_valid_record()
        self.assertTrue(self.collector.validate_record(record))
    
    def test_missing_text(self):
        record = self.get_valid_record()
        del record["text"]
        self.assertFalse(self.collector.validate_record(record))
    
    def test_no_asset_keyword(self):
        record = self.get_valid_record()
        record["text"] = "Ethereum is great"
        self.assertFalse(self.collector.validate_record(record))
    
    def test_missing_timestamp(self):
        record = self.get_valid_record()
        del record["timestamp"]
        self.assertFalse(self.collector.validate_record(record))
    
    def test_zero_engagement(self):
        record = self.get_valid_record()
        record["like_count"] = 0
        record["retweet_count"] = 0
        record["reply_count"] = 0
        self.assertFalse(self.collector.validate_record(record))
    
    def test_private_account(self):
        record = self.get_valid_record()
        record["author"]["is_private"] = True
        self.assertFalse(self.collector.validate_record(record))
    
    def test_is_ad(self):
        record = self.get_valid_record()
        record["is_ad"] = True
        self.assertFalse(self.collector.validate_record(record))
    
    def test_missing_followers(self):
        record = self.get_valid_record()
        del record["author"]["followers_count"]
        self.assertFalse(self.collector.validate_record(record))
    
    def test_engagement_weight_calculation(self):
        record = self.get_valid_record()
        # engagement_weight = log(1 + like + 2 * retweet + reply)
        # = log(1 + 100 + 2*50 + 25) = log(226)
        expected = math.log(226)
        result = self.collector.compute_engagement_weight(record)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_author_weight_calculation(self):
        record = self.get_valid_record()
        # author_weight = log(1 + followers_count)
        # = log(1 + 5000) = log(5001)
        expected = math.log(5001)
        result = self.collector.compute_author_weight(record)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_normalize_output(self):
        record = self.get_valid_record()
        result = self.collector.normalize(record, velocity=2.0)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "twitter")
        self.assertEqual(result.asset, "BTC")
        self.assertEqual(result.velocity, 2.0)
        self.assertEqual(result.source_reliability, 0.5)
        self.assertFalse(result.manipulation_flag)


class TestRedditCollector(unittest.TestCase):
    """Test Reddit data collection and normalization."""
    
    def setUp(self):
        self.collector = RedditCollector()
    
    def get_valid_record(self):
        return {
            "title": "Bitcoin breaking out!",
            "text": "Just saw massive buy walls forming. $BTC ready for liftoff.",
            "timestamp": "2026-01-17T10:35:00Z",
            "upvotes": 250,
            "comment_count": 45,
            "comment_depth": 3,
            "author": {
                "karma": 15000
            }
        }
    
    def test_valid_record(self):
        record = self.get_valid_record()
        self.assertTrue(self.collector.validate_record(record))
    
    def test_missing_text(self):
        record = self.get_valid_record()
        del record["text"]
        del record["title"]  # Both must be gone
        self.assertFalse(self.collector.validate_record(record))
    
    def test_title_only(self):
        record = self.get_valid_record()
        record["title"] = "Bitcoin is amazing #BTC"
        del record["text"]
        self.assertTrue(self.collector.validate_record(record))
    
    def test_zero_upvotes(self):
        record = self.get_valid_record()
        record["upvotes"] = 0
        self.assertFalse(self.collector.validate_record(record))
    
    def test_negative_upvotes(self):
        record = self.get_valid_record()
        record["upvotes"] = -5
        self.assertFalse(self.collector.validate_record(record))
    
    def test_zero_comment_depth(self):
        record = self.get_valid_record()
        record["comment_depth"] = 0
        self.assertFalse(self.collector.validate_record(record))
    
    def test_missing_karma(self):
        record = self.get_valid_record()
        del record["author"]["karma"]
        self.assertFalse(self.collector.validate_record(record))
    
    def test_engagement_weight_calculation(self):
        record = self.get_valid_record()
        # engagement_weight = log(1 + upvotes + comments)
        # = log(1 + 250 + 45) = log(296)
        expected = math.log(296)
        result = self.collector.compute_engagement_weight(record)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_author_weight_calculation(self):
        record = self.get_valid_record()
        # author_weight = log(1 + author_karma)
        # = log(1 + 15000) = log(15001)
        expected = math.log(15001)
        result = self.collector.compute_author_weight(record)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_normalize_output(self):
        record = self.get_valid_record()
        result = self.collector.normalize(record, velocity=1.5)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "reddit")
        self.assertEqual(result.asset, "BTC")
        self.assertEqual(result.velocity, 1.5)
        self.assertEqual(result.source_reliability, 0.7)


class TestTelegramCollector(unittest.TestCase):
    """Test Telegram data collection and normalization."""
    
    def setUp(self):
        self.collector = TelegramCollector()
    
    def get_valid_record(self):
        return {
            "text": "PANIC! Bitcoin dumping hard! Exit now!",
            "timestamp": "2026-01-17T10:40:00Z",
            "is_public": True,
            "channel_id": "btc_signals"
        }
    
    def test_valid_record(self):
        record = self.get_valid_record()
        self.assertTrue(self.collector.validate_record(record))
    
    def test_private_chat(self):
        record = self.get_valid_record()
        record["is_public"] = False
        self.assertFalse(self.collector.validate_record(record))
    
    def test_missing_channel_and_group(self):
        record = self.get_valid_record()
        del record["channel_id"]
        self.assertFalse(self.collector.validate_record(record))
    
    def test_group_id_valid(self):
        record = self.get_valid_record()
        del record["channel_id"]
        record["group_id"] = "btc_group"
        self.assertTrue(self.collector.validate_record(record))
    
    def test_no_engagement_weight(self):
        record = self.get_valid_record()
        result = self.collector.compute_engagement_weight(record)
        self.assertIsNone(result)
    
    def test_no_author_weight(self):
        record = self.get_valid_record()
        result = self.collector.compute_author_weight(record)
        self.assertIsNone(result)
    
    def test_manipulation_detection(self):
        # Same message repeated N times should trigger manipulation flag
        self.collector.phrase_tracker.clear()
        
        for i in range(3):
            is_manipulation = self.collector.check_manipulation(
                "Buy now! Bitcoin to 100k!",
                "test_channel"
            )
        
        self.assertTrue(is_manipulation)
    
    def test_no_manipulation_below_threshold(self):
        self.collector.phrase_tracker.clear()
        
        for i in range(2):
            is_manipulation = self.collector.check_manipulation(
                "Unique message",
                "test_channel"
            )
        
        self.assertFalse(is_manipulation)
    
    def test_normalize_output(self):
        record = self.get_valid_record()
        self.collector.phrase_tracker.clear()
        result = self.collector.normalize(record, velocity=5.0)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "telegram")
        self.assertEqual(result.asset, "BTC")
        self.assertEqual(result.velocity, 5.0)
        self.assertEqual(result.source_reliability, 0.3)
        self.assertIsNone(result.engagement_weight)
        self.assertIsNone(result.author_weight)


class TestVelocityCalculator(unittest.TestCase):
    """Test velocity calculations."""
    
    def setUp(self):
        self.calc = VelocityCalculator()
    
    def test_twitter_velocity(self):
        # mentions_1h / avg_mentions_24h
        result = self.calc.compute_twitter_velocity(100, 50)
        self.assertEqual(result, 2.0)
    
    def test_twitter_velocity_zero_avg(self):
        result = self.calc.compute_twitter_velocity(100, 0)
        self.assertIsNone(result)
    
    def test_reddit_velocity(self):
        # mentions_6h / avg_mentions_48h
        result = self.calc.compute_reddit_velocity(200, 100)
        self.assertEqual(result, 2.0)
    
    def test_reddit_velocity_zero_avg(self):
        result = self.calc.compute_reddit_velocity(200, 0)
        self.assertIsNone(result)
    
    def test_telegram_velocity(self):
        # messages_10min / avg_messages_1h
        result = self.calc.compute_telegram_velocity(50, 10)
        self.assertEqual(result, 5.0)
    
    def test_telegram_velocity_zero_avg(self):
        result = self.calc.compute_telegram_velocity(50, 0)
        self.assertIsNone(result)


class TestSocialDataEngine(unittest.TestCase):
    """Test main engine processing."""
    
    def setUp(self):
        self.engine = SocialDataEngine()
    
    def test_process_twitter_record(self):
        record = {
            "text": "Bitcoin is going to the moon! $BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 100,
            "retweet_count": 50,
            "reply_count": 25,
            "is_ad": False,
            "author": {
                "followers_count": 5000,
                "is_private": False
            }
        }
        
        result = self.engine.process_twitter_record(record, 100, 50)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "twitter")
        self.assertEqual(result["asset"], "BTC")
        self.assertEqual(result["metrics"]["velocity"], 2.0)
        self.assertEqual(result["source_reliability"], 0.5)
    
    def test_process_twitter_drop_zero_velocity(self):
        record = {
            "text": "Bitcoin $BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 1,
            "retweet_count": 0,
            "reply_count": 0,
            "is_ad": False,
            "author": {"followers_count": 100, "is_private": False}
        }
        
        result = self.engine.process_twitter_record(record, 100, 0)  # Zero avg
        self.assertIsNone(result)
    
    def test_process_reddit_record(self):
        record = {
            "text": "Bitcoin breaking out! $BTC",
            "timestamp": "2026-01-17T10:35:00Z",
            "upvotes": 250,
            "comment_count": 45,
            "comment_depth": 3,
            "author": {"karma": 15000}
        }
        
        result = self.engine.process_reddit_record(record, 200, 100)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "reddit")
        self.assertEqual(result["metrics"]["velocity"], 2.0)
        self.assertEqual(result["source_reliability"], 0.7)
    
    def test_process_telegram_record(self):
        record = {
            "text": "PANIC! Bitcoin dumping hard!",
            "timestamp": "2026-01-17T10:40:00Z",
            "is_public": True,
            "channel_id": "btc_signals"
        }
        
        result = self.engine.process_telegram_record(record, 50, 10)
        
        self.assertIsNotNone(result)
        self.assertEqual(result["source"], "telegram")
        self.assertEqual(result["metrics"]["velocity"], 5.0)
        self.assertEqual(result["source_reliability"], 0.3)
        self.assertIsNone(result["metrics"]["engagement_weight"])
        self.assertIsNone(result["metrics"]["author_weight"])
    
    def test_batch_processing(self):
        records = [
            {
                "text": "Bitcoin $BTC moon!",
                "timestamp": "2026-01-17T10:30:00Z",
                "like_count": 100,
                "retweet_count": 50,
                "reply_count": 25,
                "is_ad": False,
                "author": {"followers_count": 5000, "is_private": False}
            },
            {
                "text": "No asset keyword here",  # Should be dropped
                "timestamp": "2026-01-17T10:31:00Z",
                "like_count": 50,
                "retweet_count": 10,
                "reply_count": 5,
                "is_ad": False,
                "author": {"followers_count": 1000, "is_private": False}
            }
        ]
        
        velocity_data = {
            "mentions_1h": 100,
            "avg_mentions_24h": 50
        }
        
        results = self.engine.process_batch(records, "twitter", velocity_data)
        
        self.assertEqual(len(results), 1)  # Only one valid record


class TestOutputFormat(unittest.TestCase):
    """Test output format matches specification."""
    
    def test_output_structure(self):
        engine = get_engine()
        
        record = {
            "text": "Bitcoin $BTC going up!",
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 100,
            "retweet_count": 50,
            "reply_count": 25,
            "is_ad": False,
            "author": {"followers_count": 5000, "is_private": False}
        }
        
        result = engine.process_twitter_record(record, 100, 50)
        
        # Check top-level fields
        self.assertIn("source", result)
        self.assertIn("asset", result)
        self.assertIn("timestamp", result)
        self.assertIn("text", result)
        self.assertIn("metrics", result)
        self.assertIn("source_reliability", result)
        
        # Check metrics fields
        metrics = result["metrics"]
        self.assertIn("engagement_weight", metrics)
        self.assertIn("author_weight", metrics)
        self.assertIn("velocity", metrics)
        self.assertIn("manipulation_flag", metrics)


class TestDropRules(unittest.TestCase):
    """Test records are dropped correctly."""
    
    def setUp(self):
        self.engine = SocialDataEngine()
    
    def test_drop_missing_author_data(self):
        record = {
            "text": "Bitcoin $BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 100,
            "retweet_count": 50,
            "reply_count": 25,
            "is_ad": False,
            "author": {}  # Missing followers_count
        }
        
        result = self.engine.process_twitter_record(record, 100, 50)
        self.assertIsNone(result)
    
    def test_drop_missing_engagement_data(self):
        record = {
            "text": "Bitcoin $BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            # Missing like_count, retweet_count, reply_count
            "is_ad": False,
            "author": {"followers_count": 5000, "is_private": False}
        }
        
        result = self.engine.process_twitter_record(record, 100, 50)
        self.assertIsNone(result)
    
    def test_drop_velocity_cannot_compute(self):
        record = {
            "text": "Bitcoin $BTC",
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 100,
            "retweet_count": 50,
            "reply_count": 25,
            "is_ad": False,
            "author": {"followers_count": 5000, "is_private": False}
        }
        
        result = self.engine.process_twitter_record(record, 100, 0)  # avg=0
        self.assertIsNone(result)
    
    def test_drop_asset_keyword_missing(self):
        record = {
            "text": "Ethereum is great!",  # No BTC keyword
            "timestamp": "2026-01-17T10:30:00Z",
            "like_count": 100,
            "retweet_count": 50,
            "reply_count": 25,
            "is_ad": False,
            "author": {"followers_count": 5000, "is_private": False}
        }
        
        result = self.engine.process_twitter_record(record, 100, 50)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
