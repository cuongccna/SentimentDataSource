"""
Unit tests for Twitter/X Social Data Crawler.

Tests cover:
- Asset keyword filtering
- Timestamp parsing and precision
- Engagement weight calculation
- Author weight calculation
- Velocity calculation (event-time based)
- Drop rules enforcement
- Record normalization
"""

import unittest
import math
from datetime import datetime, timezone, timedelta
from twitter_crawler import (
    TwitterCrawler,
    Tweet,
    TweetAuthor,
    NormalizedTwitterRecord,
    MentionTracker,
    contains_asset_keyword,
    calculate_engagement_weight,
    calculate_author_weight,
    get_total_engagement,
    parse_twitter_timestamp,
    parse_unix_timestamp,
    timestamp_to_iso,
    validate_timestamp_precision,
    create_crawler,
    SOURCE_RELIABILITY,
    VELOCITY_SHORT_WINDOW_SECONDS,
    VELOCITY_LONG_WINDOW_SECONDS
)


class TestAssetKeywordFilter(unittest.TestCase):
    """Test asset keyword filtering."""
    
    def test_dollar_btc(self):
        """$BTC should match."""
        self.assertTrue(contains_asset_keyword("Breaking: $BTC is pumping!"))
    
    def test_hashtag_btc(self):
        """#BTC should match."""
        self.assertTrue(contains_asset_keyword("Trending: #BTC breakout"))
    
    def test_bitcoin_lowercase(self):
        """bitcoin should match (case-insensitive)."""
        self.assertTrue(contains_asset_keyword("bitcoin is rallying today"))
    
    def test_bitcoin_uppercase(self):
        """BITCOIN should match."""
        self.assertTrue(contains_asset_keyword("BITCOIN TO THE MOON"))
    
    def test_bitcoin_mixed_case(self):
        """Bitcoin should match."""
        self.assertTrue(contains_asset_keyword("Bitcoin price analysis"))
    
    def test_btc_without_prefix(self):
        """BTC without $ or # should NOT match."""
        self.assertFalse(contains_asset_keyword("BTC looking bullish"))
    
    def test_no_keyword(self):
        """No keyword should not match."""
        self.assertFalse(contains_asset_keyword("crypto market update"))
    
    def test_empty_text(self):
        """Empty text should not match."""
        self.assertFalse(contains_asset_keyword(""))
    
    def test_none_text(self):
        """None text should not match."""
        self.assertFalse(contains_asset_keyword(None))
    
    def test_multiple_keywords(self):
        """Multiple keywords should match."""
        self.assertTrue(contains_asset_keyword("$BTC and Bitcoin #BTC"))


class TestEngagementWeight(unittest.TestCase):
    """Test engagement weight calculation."""
    
    def test_engagement_weight_formula(self):
        """engagement_weight = log(1 + likes + 2*retweets + replies)"""
        like = 100
        retweet = 50
        reply = 25
        expected = math.log(1 + 100 + 2*50 + 25)  # log(226)
        
        result = calculate_engagement_weight(like, retweet, reply)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_engagement_weight_zero(self):
        """Zero engagement should give log(1) = 0."""
        result = calculate_engagement_weight(0, 0, 0)
        self.assertEqual(result, 0.0)
    
    def test_engagement_weight_retweet_double(self):
        """Retweets count double."""
        # 10 retweets = 20 weight
        result_retweets = calculate_engagement_weight(0, 10, 0)
        # 20 likes = 20 weight
        result_likes = calculate_engagement_weight(20, 0, 0)
        
        self.assertAlmostEqual(result_retweets, result_likes, places=5)
    
    def test_engagement_weight_all_components(self):
        """All engagement components contribute."""
        likes_only = calculate_engagement_weight(10, 0, 0)
        retweets_only = calculate_engagement_weight(0, 10, 0)
        replies_only = calculate_engagement_weight(0, 0, 10)
        all_combined = calculate_engagement_weight(10, 10, 10)
        
        # Combined should be greater than any individual
        self.assertGreater(all_combined, likes_only)
        self.assertGreater(all_combined, retweets_only)
        self.assertGreater(all_combined, replies_only)


class TestAuthorWeight(unittest.TestCase):
    """Test author weight calculation."""
    
    def test_author_weight_formula(self):
        """author_weight = log(1 + followers_count)"""
        followers = 10000
        expected = math.log(1 + 10000)
        
        result = calculate_author_weight(followers)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_author_weight_zero_followers(self):
        """Zero followers should give log(1) = 0."""
        result = calculate_author_weight(0)
        self.assertEqual(result, 0.0)
    
    def test_author_weight_increases_with_followers(self):
        """More followers = higher weight."""
        weight_100 = calculate_author_weight(100)
        weight_1000 = calculate_author_weight(1000)
        weight_10000 = calculate_author_weight(10000)
        
        self.assertLess(weight_100, weight_1000)
        self.assertLess(weight_1000, weight_10000)


class TestTotalEngagement(unittest.TestCase):
    """Test total engagement calculation."""
    
    def test_total_engagement(self):
        """Total = likes + retweets + replies."""
        total = get_total_engagement(100, 50, 25)
        self.assertEqual(total, 175)
    
    def test_total_engagement_zero(self):
        """Zero engagement."""
        total = get_total_engagement(0, 0, 0)
        self.assertEqual(total, 0)


class TestTimestampParsing(unittest.TestCase):
    """Test timestamp parsing."""
    
    def test_iso_format_z(self):
        """Parse ISO-8601 with Z suffix."""
        dt = parse_twitter_timestamp("2026-01-17T10:30:00Z")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 17)
        self.assertEqual(dt.hour, 10)
        self.assertEqual(dt.minute, 30)
        self.assertEqual(dt.second, 0)
        self.assertEqual(dt.tzinfo, timezone.utc)
    
    def test_iso_format_offset(self):
        """Parse ISO-8601 with timezone offset."""
        dt = parse_twitter_timestamp("2026-01-17T12:30:00+02:00")
        # Should be converted to UTC
        self.assertEqual(dt.hour, 10)  # 12:00 + 2 = 10:00 UTC
        self.assertEqual(dt.tzinfo, timezone.utc)
    
    def test_twitter_format(self):
        """Parse Twitter API format."""
        dt = parse_twitter_timestamp("Fri Jan 17 10:30:00 +0000 2026")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 17)
        self.assertEqual(dt.hour, 10)
        self.assertEqual(dt.minute, 30)
    
    def test_unix_timestamp(self):
        """Parse Unix timestamp."""
        dt = parse_unix_timestamp(1768645800)
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 17)
    
    def test_invalid_format(self):
        """Invalid format returns None."""
        dt = parse_twitter_timestamp("not a timestamp")
        self.assertIsNone(dt)
    
    def test_empty_string(self):
        """Empty string returns None."""
        dt = parse_twitter_timestamp("")
        self.assertIsNone(dt)
    
    def test_none_input(self):
        """None input returns None."""
        dt = parse_twitter_timestamp(None)
        self.assertIsNone(dt)


class TestTimestampToISO(unittest.TestCase):
    """Test timestamp to ISO conversion."""
    
    def test_to_iso_format(self):
        """Convert to ISO-8601 format."""
        dt = datetime(2026, 1, 17, 10, 30, 45, tzinfo=timezone.utc)
        iso = timestamp_to_iso(dt)
        self.assertEqual(iso, "2026-01-17T10:30:45Z")
    
    def test_to_iso_naive_datetime(self):
        """Naive datetime treated as UTC."""
        dt = datetime(2026, 1, 17, 10, 30, 45)
        iso = timestamp_to_iso(dt)
        self.assertEqual(iso, "2026-01-17T10:30:45Z")
    
    def test_second_precision(self):
        """Output has second precision."""
        dt = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        iso = timestamp_to_iso(dt)
        self.assertRegex(iso, r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z')


class TestMentionTracker(unittest.TestCase):
    """Test mention tracking and velocity calculation."""
    
    def test_add_mention(self):
        """Add mention to tracker."""
        tracker = MentionTracker()
        ts = datetime.now(timezone.utc)
        tracker.add_mention(ts)
        
        count = tracker.get_mentions_in_window(ts, 60)
        self.assertEqual(count, 1)
    
    def test_mentions_in_window(self):
        """Count mentions in time window."""
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        # Add 5 mentions in last minute
        for i in range(5):
            tracker.add_mention(now - timedelta(seconds=i*10))
        
        count = tracker.get_mentions_in_window(now, 60)
        self.assertEqual(count, 5)
    
    def test_mentions_outside_window(self):
        """Mentions outside window not counted."""
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        # Add mention 2 minutes ago (outside 1-min window)
        tracker.add_mention(now - timedelta(minutes=2))
        
        count = tracker.get_mentions_in_window(now, 60)
        self.assertEqual(count, 0)
    
    def test_velocity_calculation(self):
        """Compute velocity: mentions_1min / avg_mentions_60min."""
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        # Add 60 mentions in last hour (1 per minute average)
        for i in range(60):
            tracker.add_mention(now - timedelta(minutes=i))
        
        # Add 6 more in last minute (spike)
        for i in range(6):
            tracker.add_mention(now - timedelta(seconds=i*5))
        
        velocity = tracker.compute_velocity(now)
        # 7 mentions in last minute / (66 total / 60) = 7 / 1.1 ≈ 6.36
        self.assertGreater(velocity, 5.0)
    
    def test_velocity_no_history(self):
        """Velocity with no history defaults to 1.0."""
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        velocity = tracker.compute_velocity(now)
        self.assertEqual(velocity, 1.0)
    
    def test_velocity_uses_event_time(self):
        """Velocity uses tweet timestamp (event time)."""
        tracker = MentionTracker()
        
        # Historical tweets with their event times
        base = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        
        tracker.add_mention(base)
        tracker.add_mention(base + timedelta(seconds=30))
        
        # Velocity at base + 1 minute should count both
        velocity = tracker.compute_velocity(base + timedelta(minutes=1))
        self.assertGreater(velocity, 0)
    
    def test_clear_tracker(self):
        """Clear all mentions."""
        tracker = MentionTracker()
        now = datetime.now(timezone.utc)
        
        tracker.add_mention(now)
        tracker.clear()
        
        count = tracker.get_mentions_in_window(now, 60)
        self.assertEqual(count, 0)


class TestNormalizedRecord(unittest.TestCase):
    """Test normalized record output."""
    
    def test_to_dict(self):
        """Convert record to dictionary format."""
        record = NormalizedTwitterRecord(
            source="twitter",
            asset="BTC",
            timestamp="2026-01-17T10:30:00Z",
            text="$BTC is pumping!",
            engagement_weight=3.5,
            author_weight=8.2,
            velocity=2.0,
            source_reliability=0.5
        )
        
        result = record.to_dict()
        
        self.assertEqual(result["source"], "twitter")
        self.assertEqual(result["asset"], "BTC")
        self.assertEqual(result["timestamp"], "2026-01-17T10:30:00Z")
        self.assertEqual(result["text"], "$BTC is pumping!")
        self.assertEqual(result["metrics"]["engagement_weight"], 3.5)
        self.assertEqual(result["metrics"]["author_weight"], 8.2)
        self.assertEqual(result["metrics"]["velocity"], 2.0)
        self.assertEqual(result["source_reliability"], 0.5)
    
    def test_source_reliability_fixed(self):
        """Source reliability must be 0.5."""
        self.assertEqual(SOURCE_RELIABILITY, 0.5)


class TestTwitterCrawlerValidation(unittest.TestCase):
    """Test tweet validation in crawler."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TwitterCrawler()
    
    def test_valid_tweet(self):
        """Valid tweet should normalize."""
        tweet = {
            "text": "$BTC breaking resistance!",
            "created_at": datetime.now(timezone.utc),
            "author": {
                "followers_count": 5000,
                "account_created_at": datetime(2020, 1, 1, tzinfo=timezone.utc)
            },
            "like_count": 50,
            "retweet_count": 10,
            "reply_count": 5
        }
        
        result = self.crawler.normalize_tweet(tweet)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "twitter")
        self.assertEqual(result.asset, "BTC")
    
    def test_drop_missing_text(self):
        """Missing text should drop."""
        tweet = {
            "text": "",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_missing_timestamp(self):
        """Missing timestamp should drop."""
        tweet = {
            "text": "$BTC alert",
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_no_asset_keyword(self):
        """Missing asset keyword should drop."""
        tweet = {
            "text": "crypto market update",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_missing_followers_count(self):
        """Missing followers_count should drop."""
        tweet = {
            "text": "$BTC alert",
            "created_at": datetime.now(timezone.utc),
            "author": {"account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_missing_account_created_at(self):
        """Missing account_created_at should drop."""
        tweet = {
            "text": "$BTC alert",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_zero_engagement(self):
        """Zero engagement should drop."""
        tweet = {
            "text": "$BTC alert",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 0,
            "retweet_count": 0,
            "reply_count": 0
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_protected_account(self):
        """Protected account tweets should drop."""
        tweet = {
            "text": "$BTC insider info",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 100,
            "is_protected": True
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_promoted_tweet(self):
        """Promoted/ad tweets should drop."""
        tweet = {
            "text": "$BTC sponsored content",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 100,
            "is_promoted": True
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_retweet_without_original(self):
        """Retweet without original text should drop."""
        tweet = {
            "text": "RT @someone: $BTC is great",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10,
            "is_retweet": True,
            "original_text": None
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_accept_retweet_with_original(self):
        """Retweet with original text should normalize."""
        tweet = {
            "text": "RT @someone: $BTC is great",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10,
            "is_retweet": True,
            "original_text": "$BTC is great"
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNotNone(result)
    
    def test_drop_reply_zero_engagement(self):
        """Reply with zero engagement should drop."""
        tweet = {
            "text": "$BTC reply",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 0,
            "retweet_count": 0,
            "reply_count": 0,
            "is_reply": True
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)


class TestTwitterCrawlerTimestampParsing(unittest.TestCase):
    """Test timestamp parsing in crawler."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TwitterCrawler()
    
    def test_datetime_object(self):
        """Parse datetime object."""
        ts = datetime(2026, 1, 17, 10, 30, 45, tzinfo=timezone.utc)
        tweet = {
            "text": "$BTC alert",
            "created_at": ts,
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertEqual(result.timestamp, "2026-01-17T10:30:45Z")
    
    def test_unix_timestamp(self):
        """Parse Unix timestamp."""
        tweet = {
            "text": "$BTC alert",
            "created_at": 1768645800,
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIn("2026-01-17", result.timestamp)
    
    def test_iso_string(self):
        """Parse ISO-8601 string."""
        tweet = {
            "text": "$BTC alert",
            "created_at": "2026-01-17T10:30:45Z",
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertEqual(result.timestamp, "2026-01-17T10:30:45Z")
    
    def test_twitter_format(self):
        """Parse Twitter API format."""
        tweet = {
            "text": "$BTC alert",
            "created_at": "Fri Jan 17 10:30:45 +0000 2026",
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIn("10:30:45", result.timestamp)
    
    def test_timestamp_field_fallback(self):
        """Use 'timestamp' field if 'created_at' missing."""
        tweet = {
            "text": "$BTC alert",
            "timestamp": datetime(2026, 1, 17, 10, 30, 45, tzinfo=timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertEqual(result.timestamp, "2026-01-17T10:30:45Z")


class TestTwitterCrawlerProcessing(unittest.TestCase):
    """Test batch tweet processing."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TwitterCrawler()
    
    def test_process_valid_tweets(self):
        """Process batch of valid tweets."""
        now = datetime.now(timezone.utc)
        tweets = [
            {
                "text": "$BTC breaking out!",
                "created_at": now - timedelta(minutes=2),
                "author": {"followers_count": 5000, "account_created_at": datetime(2020, 1, 1, tzinfo=timezone.utc)},
                "like_count": 100,
                "retweet_count": 25,
                "reply_count": 10
            },
            {
                "text": "Bitcoin rally continues!",
                "created_at": now - timedelta(minutes=1),
                "author": {"followers_count": 3000, "account_created_at": datetime(2019, 6, 1, tzinfo=timezone.utc)},
                "like_count": 50,
                "retweet_count": 10,
                "reply_count": 5
            }
        ]
        
        results = self.crawler.process_tweets(tweets)
        self.assertEqual(len(results), 2)
    
    def test_process_filters_invalid(self):
        """Invalid tweets should be filtered out."""
        now = datetime.now(timezone.utc)
        tweets = [
            {
                "text": "$BTC valid tweet",
                "created_at": now,
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            },
            {
                "text": "",  # Empty - should drop
                "created_at": now,
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            },
            {
                "text": "No keywords",  # No asset - should drop
                "created_at": now,
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            }
        ]
        
        results = self.crawler.process_tweets(tweets)
        self.assertEqual(len(results), 1)
    
    def test_output_format(self):
        """Verify output format."""
        now = datetime.now(timezone.utc)
        tweets = [
            {
                "text": "$BTC price update",
                "created_at": now,
                "author": {"followers_count": 10000, "account_created_at": datetime(2018, 1, 1, tzinfo=timezone.utc)},
                "like_count": 200,
                "retweet_count": 50,
                "reply_count": 25
            }
        ]
        
        results = self.crawler.process_tweets(tweets)
        
        self.assertEqual(len(results), 1)
        record = results[0]
        
        self.assertEqual(record["source"], "twitter")
        self.assertEqual(record["asset"], "BTC")
        self.assertIn("timestamp", record)
        self.assertEqual(record["text"], "$BTC price update")
        self.assertIn("metrics", record)
        self.assertIn("engagement_weight", record["metrics"])
        self.assertIn("author_weight", record["metrics"])
        self.assertIn("velocity", record["metrics"])
        self.assertEqual(record["source_reliability"], 0.5)
    
    def test_sorted_by_timestamp(self):
        """Tweets should be processed in timestamp order."""
        base = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        
        # Out of order tweets
        tweets = [
            {
                "text": "$BTC third",
                "created_at": base + timedelta(minutes=2),
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            },
            {
                "text": "$BTC first",
                "created_at": base,
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            },
            {
                "text": "$BTC second",
                "created_at": base + timedelta(minutes=1),
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            }
        ]
        
        results = self.crawler.process_tweets(tweets)
        
        # Verify order by checking timestamps
        self.assertEqual(len(results), 3)
        ts1 = results[0]["timestamp"]
        ts2 = results[1]["timestamp"]
        ts3 = results[2]["timestamp"]
        
        self.assertLess(ts1, ts2)
        self.assertLess(ts2, ts3)


class TestVelocityIntegration(unittest.TestCase):
    """Test velocity calculation in normalized output."""
    
    def test_velocity_in_output(self):
        """Velocity should be in output."""
        crawler = TwitterCrawler()
        now = datetime.now(timezone.utc)
        
        tweet = {
            "text": "$BTC alert",
            "created_at": now,
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        
        results = crawler.process_tweets([tweet])
        
        self.assertIn("velocity", results[0]["metrics"])
        self.assertIsInstance(results[0]["metrics"]["velocity"], float)
    
    def test_velocity_increases_with_activity(self):
        """Velocity should increase with more recent activity."""
        crawler = TwitterCrawler()
        now = datetime.now(timezone.utc)
        
        # Simulate historical activity
        for i in range(30):
            crawler.mention_tracker.add_mention(now - timedelta(minutes=i+5))
        
        # Add burst of recent activity
        tweets = []
        for i in range(10):
            tweets.append({
                "text": "$BTC spike!",
                "created_at": now - timedelta(seconds=i*5),
                "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
                "like_count": 10
            })
        
        results = crawler.process_tweets(tweets)
        
        # Later tweets should have higher velocity
        # (as more mentions accumulate in the 1-min window)
        velocities = [r["metrics"]["velocity"] for r in results]
        
        # The last processed tweet should have high velocity
        self.assertGreater(velocities[-1], 1.0)


class TestCrawlerFactory(unittest.TestCase):
    """Test crawler factory function."""
    
    def test_create_crawler(self):
        """Factory creates crawler."""
        crawler = create_crawler()
        self.assertIsInstance(crawler, TwitterCrawler)
    
    def test_reset_tracker(self):
        """Reset tracker clears history."""
        crawler = create_crawler()
        now = datetime.now(timezone.utc)
        
        crawler.mention_tracker.add_mention(now)
        crawler.reset_tracker()
        
        count = crawler.mention_tracker.get_mentions_in_window(now, 60)
        self.assertEqual(count, 0)


class TestConstants(unittest.TestCase):
    """Test fixed constants."""
    
    def test_source_reliability(self):
        """Source reliability must be 0.5."""
        self.assertEqual(SOURCE_RELIABILITY, 0.5)
    
    def test_velocity_short_window(self):
        """Short window must be 60 seconds (1 minute)."""
        self.assertEqual(VELOCITY_SHORT_WINDOW_SECONDS, 60)
    
    def test_velocity_long_window(self):
        """Long window must be 3600 seconds (60 minutes)."""
        self.assertEqual(VELOCITY_LONG_WINDOW_SECONDS, 3600)


class TestDropRulesEnforcement(unittest.TestCase):
    """Verify all drop rules are enforced."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TwitterCrawler()
    
    def test_drop_rule_missing_timestamp(self):
        """DROP if timestamp missing."""
        tweet = {
            "text": "$BTC alert",
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_rule_missing_followers(self):
        """DROP if followers_count missing."""
        tweet = {
            "text": "$BTC alert",
            "created_at": datetime.now(timezone.utc),
            "author": {"account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_rule_asset_keyword_missing(self):
        """DROP if asset keyword missing."""
        tweet = {
            "text": "crypto update",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)
    
    def test_drop_rule_whitespace_only_text(self):
        """DROP if text is whitespace only."""
        tweet = {
            "text": "   \n\t   ",
            "created_at": datetime.now(timezone.utc),
            "author": {"followers_count": 1000, "account_created_at": datetime.now(timezone.utc)},
            "like_count": 10
        }
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNone(result)


class TestDirectAuthorFields(unittest.TestCase):
    """Test author data in direct fields (not nested)."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TwitterCrawler()
    
    def test_direct_followers_count(self):
        """Accept followers_count as direct field."""
        tweet = {
            "text": "$BTC alert",
            "created_at": datetime.now(timezone.utc),
            "followers_count": 5000,
            "account_created_at": datetime.now(timezone.utc),
            "like_count": 10
        }
        
        result = self.crawler.normalize_tweet(tweet)
        self.assertIsNotNone(result)
        # author_weight = log(1 + 5000)
        expected_weight = math.log(1 + 5000)
        self.assertAlmostEqual(result.author_weight, expected_weight, places=5)


if __name__ == "__main__":
    unittest.main()
