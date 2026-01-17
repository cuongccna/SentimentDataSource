"""
Unit tests for Telegram Social Data Crawler.

Tests cover:
- Asset keyword filtering
- Message validation
- Timestamp parsing
- Velocity calculation
- Manipulation detection
- Record normalization
- Drop rules enforcement
"""

import unittest
from datetime import datetime, timezone, timedelta
from telegram_crawler import (
    TelegramCrawler,
    TelegramMessage,
    NormalizedTelegramRecord,
    MessageTracker,
    ManipulationDetector,
    contains_asset_keyword,
    timestamp_to_iso,
    parse_unix_timestamp,
    create_crawler,
    SOURCE_RELIABILITY,
    DEFAULT_MANIPULATION_THRESHOLD
)


class TestAssetKeywordFilter(unittest.TestCase):
    """Test asset keyword filtering."""
    
    def test_dollar_btc(self):
        """$BTC should match."""
        self.assertTrue(contains_asset_keyword("Price alert: $BTC is moving!"))
    
    def test_hashtag_btc(self):
        """#BTC should match."""
        self.assertTrue(contains_asset_keyword("Trending now: #BTC breakout"))
    
    def test_bitcoin_lowercase(self):
        """bitcoin should match (case-insensitive)."""
        self.assertTrue(contains_asset_keyword("bitcoin is crashing hard"))
    
    def test_bitcoin_uppercase(self):
        """BITCOIN should match."""
        self.assertTrue(contains_asset_keyword("BITCOIN TO THE MOON"))
    
    def test_bitcoin_mixed_case(self):
        """Bitcoin should match."""
        self.assertTrue(contains_asset_keyword("Bitcoin price update"))
    
    def test_btc_without_prefix(self):
        """BTC without $ or # should NOT match."""
        self.assertFalse(contains_asset_keyword("BTC pumping today"))
    
    def test_no_keyword(self):
        """No keyword should not match."""
        self.assertFalse(contains_asset_keyword("crypto is the future"))
    
    def test_empty_text(self):
        """Empty text should not match."""
        self.assertFalse(contains_asset_keyword(""))
    
    def test_none_text(self):
        """None text should not match."""
        self.assertFalse(contains_asset_keyword(None))
    
    def test_multiple_keywords(self):
        """Multiple keywords should match."""
        self.assertTrue(contains_asset_keyword("$BTC and Bitcoin are #BTC trending"))
    
    def test_btc_in_word(self):
        """BTC embedded in word should NOT match without prefix."""
        self.assertFalse(contains_asset_keyword("The WBTC token is wrapped"))
    
    def test_dollar_btc_no_space_after(self):
        """$BTC at end of text should match."""
        self.assertTrue(contains_asset_keyword("Buy $BTC"))


class TestTimestampParsing(unittest.TestCase):
    """Test timestamp parsing."""
    
    def test_unix_timestamp(self):
        """Parse Unix timestamp."""
        dt = parse_unix_timestamp(1768645800)
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 17)
    
    def test_timestamp_to_iso(self):
        """Convert datetime to ISO-8601."""
        dt = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        iso = timestamp_to_iso(dt)
        self.assertEqual(iso, "2026-01-17T10:30:00Z")
    
    def test_timestamp_to_iso_naive(self):
        """Naive datetime should be treated as UTC."""
        dt = datetime(2026, 1, 17, 10, 30, 0)
        iso = timestamp_to_iso(dt)
        self.assertEqual(iso, "2026-01-17T10:30:00Z")


class TestMessageTracker(unittest.TestCase):
    """Test message velocity tracking."""
    
    def test_add_message(self):
        """Add message to tracker."""
        tracker = MessageTracker()
        ts = datetime.now(timezone.utc)
        tracker.add_message(ts, "test_channel")
        
        count = tracker.get_messages_in_window("test_channel", 10)
        self.assertEqual(count, 1)
    
    def test_messages_in_window(self):
        """Count messages in time window."""
        tracker = MessageTracker()
        now = datetime.now(timezone.utc)
        
        # Add 5 messages in last 10 minutes
        for i in range(5):
            tracker.add_message(now - timedelta(minutes=i), "channel1")
        
        count = tracker.get_messages_in_window("channel1", 10)
        self.assertEqual(count, 5)
    
    def test_messages_outside_window(self):
        """Messages outside window should not count."""
        tracker = MessageTracker()
        now = datetime.now(timezone.utc)
        
        # Add message 15 minutes ago (outside 10-min window)
        tracker.add_message(now - timedelta(minutes=15), "channel1")
        
        count = tracker.get_messages_in_window("channel1", 10)
        self.assertEqual(count, 0)
    
    def test_separate_channels(self):
        """Messages counted separately per channel."""
        tracker = MessageTracker()
        now = datetime.now(timezone.utc)
        
        tracker.add_message(now, "channel1")
        tracker.add_message(now, "channel1")
        tracker.add_message(now, "channel2")
        
        self.assertEqual(tracker.get_messages_in_window("channel1", 10), 2)
        self.assertEqual(tracker.get_messages_in_window("channel2", 10), 1)
    
    def test_velocity_calculation(self):
        """Compute velocity: messages_10min / avg_messages_1h."""
        tracker = MessageTracker()
        now = datetime.now(timezone.utc)
        
        # Add 6 messages in last 10 minutes (high activity)
        for i in range(6):
            tracker.add_message(now - timedelta(minutes=i), "channel1")
        
        # 6 windows in 1 hour, 6 messages = 1 avg per window
        # velocity = 6 / 1 = 6.0
        velocity = tracker.compute_velocity("channel1")
        self.assertAlmostEqual(velocity, 6.0, places=1)
    
    def test_velocity_no_messages(self):
        """Velocity with no messages defaults to 1.0."""
        tracker = MessageTracker()
        velocity = tracker.compute_velocity("empty_channel")
        self.assertEqual(velocity, 1.0)


class TestManipulationDetector(unittest.TestCase):
    """Test manipulation detection via phrase repetition."""
    
    def test_no_manipulation_single_message(self):
        """Single message is not manipulation."""
        detector = ManipulationDetector(threshold=3)
        ts = datetime.now(timezone.utc)
        
        result = detector.check_manipulation("Buy Bitcoin now!", "channel1", ts)
        self.assertFalse(result)
    
    def test_no_manipulation_different_messages(self):
        """Different messages are not manipulation."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("Buy Bitcoin now!", "channel1", now)
        detector.check_manipulation("Sell Bitcoin fast!", "channel1", now + timedelta(seconds=10))
        result = detector.check_manipulation("Hold your Bitcoin!", "channel1", now + timedelta(seconds=20))
        
        self.assertFalse(result)
    
    def test_manipulation_threshold_reached(self):
        """Same message repeated >= threshold times triggers flag."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("BUY BITCOIN NOW!", "channel1", now)
        detector.check_manipulation("BUY BITCOIN NOW!", "channel1", now + timedelta(seconds=30))
        result = detector.check_manipulation("BUY BITCOIN NOW!", "channel1", now + timedelta(seconds=60))
        
        self.assertTrue(result)  # Third repetition triggers flag
    
    def test_manipulation_fingerprint_ignores_case(self):
        """Fingerprint should ignore case."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("BUY BITCOIN NOW!", "channel1", now)
        detector.check_manipulation("buy bitcoin now!", "channel1", now + timedelta(seconds=30))
        result = detector.check_manipulation("Buy Bitcoin Now!", "channel1", now + timedelta(seconds=60))
        
        self.assertTrue(result)
    
    def test_manipulation_fingerprint_ignores_numbers(self):
        """Fingerprint should ignore numbers."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("BTC to 100k!", "channel1", now)
        detector.check_manipulation("BTC to 200k!", "channel1", now + timedelta(seconds=30))
        result = detector.check_manipulation("BTC to 50k!", "channel1", now + timedelta(seconds=60))
        
        self.assertTrue(result)  # "btc to k" is same fingerprint
    
    def test_manipulation_fingerprint_ignores_punctuation(self):
        """Fingerprint should ignore punctuation."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("BUY BITCOIN!!!", "channel1", now)
        detector.check_manipulation("BUY BITCOIN...", "channel1", now + timedelta(seconds=30))
        result = detector.check_manipulation("BUY BITCOIN???", "channel1", now + timedelta(seconds=60))
        
        self.assertTrue(result)
    
    def test_manipulation_separate_channels(self):
        """Manipulation detected per channel."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("SPAM MESSAGE", "channel1", now)
        detector.check_manipulation("SPAM MESSAGE", "channel2", now + timedelta(seconds=30))
        result = detector.check_manipulation("SPAM MESSAGE", "channel1", now + timedelta(seconds=60))
        
        self.assertFalse(result)  # Only 2 in channel1
    
    def test_manipulation_outside_window(self):
        """Messages outside time window don't count."""
        detector = ManipulationDetector(threshold=3, window_minutes=5)
        now = datetime.now(timezone.utc)
        
        # Message 10 minutes ago (outside 5-min window)
        detector.check_manipulation("SPAM", "channel1", now - timedelta(minutes=10))
        detector.check_manipulation("SPAM", "channel1", now - timedelta(minutes=8))
        result = detector.check_manipulation("SPAM", "channel1", now)
        
        self.assertFalse(result)  # Only 1 message in window
    
    def test_configurable_threshold(self):
        """Threshold is configurable."""
        detector = ManipulationDetector(threshold=5)
        now = datetime.now(timezone.utc)
        
        for i in range(4):
            result = detector.check_manipulation("SPAM", "channel1", now + timedelta(seconds=i*10))
        
        self.assertFalse(result)  # 4 < 5
        
        result = detector.check_manipulation("SPAM", "channel1", now + timedelta(seconds=50))
        self.assertTrue(result)  # 5 >= 5
    
    def test_clear_channel(self):
        """Clear channel history."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        detector.check_manipulation("SPAM", "channel1", now)
        detector.check_manipulation("SPAM", "channel1", now + timedelta(seconds=10))
        
        detector.clear_channel("channel1")
        
        result = detector.check_manipulation("SPAM", "channel1", now + timedelta(seconds=20))
        self.assertFalse(result)  # History cleared, only 1 message
    
    def test_empty_text(self):
        """Empty text should not trigger manipulation."""
        detector = ManipulationDetector(threshold=3)
        now = datetime.now(timezone.utc)
        
        for i in range(5):
            result = detector.check_manipulation("", "channel1", now + timedelta(seconds=i*10))
        
        self.assertFalse(result)


class TestNormalizedRecord(unittest.TestCase):
    """Test normalized record output."""
    
    def test_to_dict(self):
        """Convert record to dictionary format."""
        record = NormalizedTelegramRecord(
            source="telegram",
            asset="BTC",
            timestamp="2026-01-17T10:30:00Z",
            text="Bitcoin is crashing!",
            velocity=2.5,
            manipulation_flag=False,
            source_reliability=0.3
        )
        
        result = record.to_dict()
        
        self.assertEqual(result["source"], "telegram")
        self.assertEqual(result["asset"], "BTC")
        self.assertEqual(result["timestamp"], "2026-01-17T10:30:00Z")
        self.assertEqual(result["text"], "Bitcoin is crashing!")
        self.assertEqual(result["metrics"]["velocity"], 2.5)
        self.assertEqual(result["metrics"]["manipulation_flag"], False)
        self.assertEqual(result["source_reliability"], 0.3)
    
    def test_source_reliability_fixed(self):
        """Source reliability must be 0.3."""
        self.assertEqual(SOURCE_RELIABILITY, 0.3)


class TestTelegramCrawlerValidation(unittest.TestCase):
    """Test message validation in crawler."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TelegramCrawler()
    
    def test_valid_message(self):
        """Valid message should normalize."""
        message = {
            "text": "Bitcoin is moving $BTC",
            "timestamp": datetime.now(timezone.utc),
            "is_public": True,
            "is_forwarded": False,
            "is_bot": False
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "telegram")
        self.assertEqual(result.asset, "BTC")
    
    def test_drop_missing_text(self):
        """Missing text should drop."""
        message = {
            "text": "",
            "timestamp": datetime.now(timezone.utc)
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_missing_timestamp(self):
        """Missing timestamp should drop."""
        message = {
            "text": "Bitcoin alert $BTC"
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_no_asset_keyword(self):
        """Missing asset keyword should drop."""
        message = {
            "text": "Crypto is volatile",
            "timestamp": datetime.now(timezone.utc)
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_forwarded_unknown_source(self):
        """Forwarded message with unknown source should drop."""
        message = {
            "text": "Bitcoin pump! $BTC",
            "timestamp": datetime.now(timezone.utc),
            "is_forwarded": True,
            "forward_source": None
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_accept_forwarded_known_source(self):
        """Forwarded message with known source should normalize."""
        message = {
            "text": "Bitcoin pump! $BTC",
            "timestamp": datetime.now(timezone.utc),
            "is_forwarded": True,
            "forward_source": "trusted_channel"
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNotNone(result)
    
    def test_drop_bot_message(self):
        """Bot message should drop."""
        message = {
            "text": "Bitcoin price: $BTC = 50000",
            "timestamp": datetime.now(timezone.utc),
            "is_bot": True
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_private_message(self):
        """Private message should drop."""
        message = {
            "text": "Bitcoin insider tip $BTC",
            "timestamp": datetime.now(timezone.utc),
            "is_public": False
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)


class TestTelegramCrawlerTimestampParsing(unittest.TestCase):
    """Test timestamp parsing in crawler."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TelegramCrawler()
    
    def test_datetime_object(self):
        """Parse datetime object."""
        ts = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        message = {
            "text": "Bitcoin alert $BTC",
            "timestamp": ts
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertEqual(result.timestamp, "2026-01-17T10:30:00Z")
    
    def test_unix_timestamp(self):
        """Parse Unix timestamp."""
        message = {
            "text": "Bitcoin alert $BTC",
            "timestamp": 1768645800
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIn("2026-01-17", result.timestamp)
    
    def test_iso_string(self):
        """Parse ISO-8601 string."""
        message = {
            "text": "Bitcoin alert $BTC",
            "timestamp": "2026-01-17T10:30:00Z"
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertEqual(result.timestamp, "2026-01-17T10:30:00Z")
    
    def test_date_field_unix(self):
        """Parse 'date' field with Unix timestamp (Telegram API format)."""
        message = {
            "text": "Bitcoin alert $BTC",
            "date": 1768645800
        }
        
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIn("2026-01-17", result.timestamp)


class TestTelegramCrawlerProcessing(unittest.TestCase):
    """Test batch message processing."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TelegramCrawler()
    
    def test_process_valid_messages(self):
        """Process batch of valid messages."""
        messages = [
            {
                "text": "Bitcoin crash! $BTC down",
                "timestamp": datetime.now(timezone.utc)
            },
            {
                "text": "Alert: Bitcoin dropping fast",
                "timestamp": datetime.now(timezone.utc)
            }
        ]
        
        results = self.crawler.process_messages(messages, "channel1")
        self.assertEqual(len(results), 2)
    
    def test_process_filters_invalid(self):
        """Invalid messages should be filtered out."""
        messages = [
            {
                "text": "Bitcoin crash! $BTC",
                "timestamp": datetime.now(timezone.utc)
            },
            {
                "text": "",  # Empty - should drop
                "timestamp": datetime.now(timezone.utc)
            },
            {
                "text": "No keywords here",  # No asset - should drop
                "timestamp": datetime.now(timezone.utc)
            }
        ]
        
        results = self.crawler.process_messages(messages, "channel1")
        self.assertEqual(len(results), 1)
    
    def test_output_format(self):
        """Verify output format."""
        messages = [
            {
                "text": "$BTC is moving fast",
                "timestamp": datetime.now(timezone.utc)
            }
        ]
        
        results = self.crawler.process_messages(messages, "channel1")
        
        self.assertEqual(len(results), 1)
        record = results[0]
        
        self.assertEqual(record["source"], "telegram")
        self.assertEqual(record["asset"], "BTC")
        self.assertIn("timestamp", record)
        self.assertEqual(record["text"], "$BTC is moving fast")
        self.assertIn("metrics", record)
        self.assertIn("velocity", record["metrics"])
        self.assertIn("manipulation_flag", record["metrics"])
        self.assertEqual(record["source_reliability"], 0.3)


class TestManipulationFlagIntegration(unittest.TestCase):
    """Test manipulation flag in normalized output."""
    
    def test_manipulation_flag_false(self):
        """Unique messages should not have manipulation flag."""
        crawler = TelegramCrawler()
        now = datetime.now(timezone.utc)
        
        messages = [
            {"text": "Bitcoin news $BTC", "timestamp": now},
            {"text": "Different message about Bitcoin", "timestamp": now + timedelta(seconds=10)}
        ]
        
        results = crawler.process_messages(messages, "channel1")
        
        for record in results:
            self.assertFalse(record["metrics"]["manipulation_flag"])
    
    def test_manipulation_flag_true(self):
        """Repeated messages should have manipulation flag."""
        crawler = TelegramCrawler(manipulation_threshold=3)
        now = datetime.now(timezone.utc)
        
        messages = [
            {"text": "BUY BITCOIN NOW! $BTC", "timestamp": now},
            {"text": "BUY BITCOIN NOW! $BTC", "timestamp": now + timedelta(seconds=10)},
            {"text": "BUY BITCOIN NOW! $BTC", "timestamp": now + timedelta(seconds=20)}
        ]
        
        results = crawler.process_messages(messages, "channel1")
        
        # Third message should have manipulation flag
        self.assertFalse(results[0]["metrics"]["manipulation_flag"])
        self.assertFalse(results[1]["metrics"]["manipulation_flag"])
        self.assertTrue(results[2]["metrics"]["manipulation_flag"])
    
    def test_set_manipulation_threshold(self):
        """Manipulation threshold can be updated."""
        crawler = TelegramCrawler(manipulation_threshold=3)
        crawler.set_manipulation_threshold(5)
        
        self.assertEqual(crawler.manipulation_detector.threshold, 5)


class TestVelocityIntegration(unittest.TestCase):
    """Test velocity calculation in normalized output."""
    
    def test_velocity_in_output(self):
        """Velocity should be in output."""
        crawler = TelegramCrawler()
        now = datetime.now(timezone.utc)
        
        message = {"text": "Bitcoin alert $BTC", "timestamp": now}
        results = crawler.process_messages([message], "channel1")
        
        self.assertIn("velocity", results[0]["metrics"])
        self.assertIsInstance(results[0]["metrics"]["velocity"], float)


class TestCrawlerFactory(unittest.TestCase):
    """Test crawler factory function."""
    
    def test_create_crawler(self):
        """Factory creates crawler with channels."""
        crawler = create_crawler(channels=["channel1", "channel2"])
        
        self.assertEqual(len(crawler.channels), 2)
        self.assertIn("channel1", crawler.channels)
        self.assertIn("channel2", crawler.channels)
    
    def test_create_crawler_with_threshold(self):
        """Factory creates crawler with custom threshold."""
        crawler = create_crawler(manipulation_threshold=5)
        
        self.assertEqual(crawler.manipulation_detector.threshold, 5)
    
    def test_create_crawler_defaults(self):
        """Factory creates crawler with defaults."""
        crawler = create_crawler()
        
        self.assertEqual(len(crawler.channels), 0)
        self.assertEqual(crawler.manipulation_detector.threshold, DEFAULT_MANIPULATION_THRESHOLD)


class TestConstants(unittest.TestCase):
    """Test fixed constants."""
    
    def test_source_reliability(self):
        """Source reliability must be 0.3."""
        self.assertEqual(SOURCE_RELIABILITY, 0.3)
    
    def test_default_manipulation_threshold(self):
        """Default manipulation threshold must be 3."""
        self.assertEqual(DEFAULT_MANIPULATION_THRESHOLD, 3)


class TestDropRulesEnforcement(unittest.TestCase):
    """Verify all drop rules are enforced."""
    
    def setUp(self):
        """Create crawler instance."""
        self.crawler = TelegramCrawler()
    
    def test_drop_rule_missing_timestamp(self):
        """DROP if timestamp missing."""
        message = {"text": "Bitcoin $BTC alert"}
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_rule_missing_text(self):
        """DROP if text missing."""
        message = {"timestamp": datetime.now(timezone.utc)}
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_rule_asset_keyword_missing(self):
        """DROP if asset keyword missing."""
        message = {
            "text": "Crypto market is volatile",
            "timestamp": datetime.now(timezone.utc)
        }
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_rule_whitespace_only_text(self):
        """DROP if text is whitespace only."""
        message = {
            "text": "   \n\t   ",
            "timestamp": datetime.now(timezone.utc)
        }
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)
    
    def test_drop_rule_none_text(self):
        """DROP if text is None."""
        message = {
            "text": None,
            "timestamp": datetime.now(timezone.utc)
        }
        result = self.crawler.normalize_message(message, "channel1")
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
