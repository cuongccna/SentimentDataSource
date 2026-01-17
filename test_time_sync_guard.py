"""
Unit tests for Time Synchronization & Validation Guard.

Tests cover:
- Timestamp format validation
- Future event detection
- Late event detection (by source)
- Out-of-order event detection
- Duplicate/replay detection
- Passthrough behavior
- Metrics tracking
"""

import unittest
from datetime import datetime, timezone, timedelta
from time_sync_guard import (
    TimeSyncGuard,
    TimeQualityMetrics,
    EventTracker,
    DuplicateDetector,
    parse_timestamp,
    format_timestamp,
    create_guard,
    MAX_EVENT_DELAY,
    OUT_OF_ORDER_TOLERANCE,
    DUPLICATE_WINDOW
)


class TestTimestampParsing(unittest.TestCase):
    """Test timestamp parsing and validation."""
    
    def test_valid_iso8601_utc(self):
        """Parse valid ISO-8601 UTC timestamp."""
        dt = parse_timestamp("2026-01-17T10:30:45Z")
        self.assertIsNotNone(dt)
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 17)
        self.assertEqual(dt.hour, 10)
        self.assertEqual(dt.minute, 30)
        self.assertEqual(dt.second, 45)
        self.assertEqual(dt.tzinfo, timezone.utc)
    
    def test_valid_iso8601_with_offset(self):
        """Parse ISO-8601 with timezone offset, convert to UTC."""
        dt = parse_timestamp("2026-01-17T12:30:45+02:00")
        self.assertIsNotNone(dt)
        self.assertEqual(dt.hour, 10)  # 12:00 + 2:00 offset = 10:00 UTC
        self.assertEqual(dt.tzinfo, timezone.utc)
    
    def test_invalid_format(self):
        """Invalid format returns None."""
        self.assertIsNone(parse_timestamp("not a timestamp"))
        self.assertIsNone(parse_timestamp("2026-01-17"))
        self.assertIsNone(parse_timestamp("10:30:45"))
    
    def test_missing_timezone(self):
        """Timestamp without timezone returns None."""
        self.assertIsNone(parse_timestamp("2026-01-17T10:30:45"))
    
    def test_empty_string(self):
        """Empty string returns None."""
        self.assertIsNone(parse_timestamp(""))
    
    def test_none_input(self):
        """None input returns None."""
        self.assertIsNone(parse_timestamp(None))
    
    def test_minute_precision_only(self):
        """Minute-only precision (no seconds) returns None."""
        self.assertIsNone(parse_timestamp("2026-01-17T10:30Z"))


class TestFormatTimestamp(unittest.TestCase):
    """Test timestamp formatting."""
    
    def test_format_utc(self):
        """Format datetime to ISO-8601 UTC."""
        dt = datetime(2026, 1, 17, 10, 30, 45, tzinfo=timezone.utc)
        result = format_timestamp(dt)
        self.assertEqual(result, "2026-01-17T10:30:45Z")
    
    def test_format_naive_datetime(self):
        """Naive datetime treated as UTC."""
        dt = datetime(2026, 1, 17, 10, 30, 45)
        result = format_timestamp(dt)
        self.assertEqual(result, "2026-01-17T10:30:45Z")
    
    def test_second_precision(self):
        """Output has second precision."""
        dt = datetime(2026, 1, 17, 10, 30, 45, 123456, tzinfo=timezone.utc)
        result = format_timestamp(dt)
        # Microseconds should be stripped
        self.assertEqual(result, "2026-01-17T10:30:45Z")


class TestEventTracker(unittest.TestCase):
    """Test last seen event tracking."""
    
    def test_get_last_seen_empty(self):
        """No history returns None."""
        tracker = EventTracker()
        result = tracker.get_last_seen("twitter", "BTC")
        self.assertIsNone(result)
    
    def test_update_and_get(self):
        """Update and retrieve last seen time."""
        tracker = EventTracker()
        ts = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        
        tracker.update("twitter", "BTC", ts)
        result = tracker.get_last_seen("twitter", "BTC")
        
        self.assertEqual(result, ts)
    
    def test_update_newer_time(self):
        """Update with newer time replaces old."""
        tracker = EventTracker()
        ts1 = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 1, 17, 10, 35, 0, tzinfo=timezone.utc)
        
        tracker.update("twitter", "BTC", ts1)
        tracker.update("twitter", "BTC", ts2)
        result = tracker.get_last_seen("twitter", "BTC")
        
        self.assertEqual(result, ts2)
    
    def test_update_older_time_ignored(self):
        """Update with older time is ignored."""
        tracker = EventTracker()
        ts1 = datetime(2026, 1, 17, 10, 35, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        
        tracker.update("twitter", "BTC", ts1)
        tracker.update("twitter", "BTC", ts2)
        result = tracker.get_last_seen("twitter", "BTC")
        
        self.assertEqual(result, ts1)  # Newer time kept
    
    def test_separate_sources(self):
        """Separate tracking per source."""
        tracker = EventTracker()
        ts1 = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 1, 17, 10, 35, 0, tzinfo=timezone.utc)
        
        tracker.update("twitter", "BTC", ts1)
        tracker.update("reddit", "BTC", ts2)
        
        self.assertEqual(tracker.get_last_seen("twitter", "BTC"), ts1)
        self.assertEqual(tracker.get_last_seen("reddit", "BTC"), ts2)
    
    def test_separate_assets(self):
        """Separate tracking per asset."""
        tracker = EventTracker()
        ts1 = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        ts2 = datetime(2026, 1, 17, 10, 35, 0, tzinfo=timezone.utc)
        
        tracker.update("twitter", "BTC", ts1)
        tracker.update("twitter", "ETH", ts2)
        
        self.assertEqual(tracker.get_last_seen("twitter", "BTC"), ts1)
        self.assertEqual(tracker.get_last_seen("twitter", "ETH"), ts2)
    
    def test_clear(self):
        """Clear all tracking."""
        tracker = EventTracker()
        ts = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        
        tracker.update("twitter", "BTC", ts)
        tracker.clear()
        
        self.assertIsNone(tracker.get_last_seen("twitter", "BTC"))


class TestDuplicateDetector(unittest.TestCase):
    """Test duplicate/replay detection."""
    
    def test_not_duplicate_first_time(self):
        """First occurrence is not duplicate."""
        detector = DuplicateDetector()
        now = datetime.now(timezone.utc)
        
        result = detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:30:00Z",
            "$BTC is pumping", now
        )
        
        self.assertFalse(result)
    
    def test_duplicate_same_fingerprint(self):
        """Same fingerprint is duplicate."""
        detector = DuplicateDetector()
        now = datetime.now(timezone.utc)
        
        detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:30:00Z",
            "$BTC is pumping", now
        )
        
        result = detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:30:00Z",
            "$BTC is pumping", now + timedelta(seconds=10)
        )
        
        self.assertTrue(result)
    
    def test_not_duplicate_different_text(self):
        """Different text is not duplicate."""
        detector = DuplicateDetector()
        now = datetime.now(timezone.utc)
        
        detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:30:00Z",
            "$BTC is pumping", now
        )
        
        result = detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:30:00Z",
            "$BTC is dumping", now + timedelta(seconds=10)
        )
        
        self.assertFalse(result)
    
    def test_not_duplicate_different_timestamp(self):
        """Different timestamp is not duplicate."""
        detector = DuplicateDetector()
        now = datetime.now(timezone.utc)
        
        detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:30:00Z",
            "$BTC is pumping", now
        )
        
        result = detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:31:00Z",
            "$BTC is pumping", now + timedelta(seconds=10)
        )
        
        self.assertFalse(result)
    
    def test_duplicate_window_twitter(self):
        """Twitter duplicate window is 5 minutes."""
        self.assertEqual(DUPLICATE_WINDOW["twitter"], 300)
    
    def test_duplicate_window_telegram(self):
        """Telegram duplicate window is 10 minutes."""
        self.assertEqual(DUPLICATE_WINDOW["telegram"], 600)
    
    def test_duplicate_window_reddit(self):
        """Reddit duplicate window is 30 minutes."""
        self.assertEqual(DUPLICATE_WINDOW["reddit"], 1800)
    
    def test_expires_after_window(self):
        """Duplicate expires after rolling window."""
        detector = DuplicateDetector()
        base = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        
        # First occurrence
        detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:00:00Z",
            "$BTC is pumping", base
        )
        
        # After 6 minutes (beyond 5-min Twitter window)
        result = detector.is_duplicate(
            "twitter", "BTC", "2026-01-17T10:00:00Z",
            "$BTC is pumping", base + timedelta(minutes=6)
        )
        
        self.assertFalse(result)  # No longer duplicate
    
    def test_clear_source(self):
        """Clear specific source."""
        detector = DuplicateDetector()
        now = datetime.now(timezone.utc)
        
        detector.is_duplicate("twitter", "BTC", "ts1", "text1", now)
        detector.is_duplicate("reddit", "BTC", "ts2", "text2", now)
        
        detector.clear("twitter")
        
        # Twitter should not be duplicate after clear
        result1 = detector.is_duplicate("twitter", "BTC", "ts1", "text1", now)
        # Reddit should still be duplicate
        result2 = detector.is_duplicate("reddit", "BTC", "ts2", "text2", now)
        
        self.assertFalse(result1)
        self.assertTrue(result2)


class TestMaxEventDelay(unittest.TestCase):
    """Test max event delay constants."""
    
    def test_twitter_delay(self):
        """Twitter max delay is 15 seconds."""
        self.assertEqual(MAX_EVENT_DELAY["twitter"], 15)
    
    def test_telegram_delay(self):
        """Telegram max delay is 30 seconds."""
        self.assertEqual(MAX_EVENT_DELAY["telegram"], 30)
    
    def test_reddit_delay(self):
        """Reddit max delay is 120 seconds."""
        self.assertEqual(MAX_EVENT_DELAY["reddit"], 120)


class TestOutOfOrderTolerance(unittest.TestCase):
    """Test out-of-order tolerance constants."""
    
    def test_twitter_tolerance(self):
        """Twitter tolerance is 5 seconds."""
        self.assertEqual(OUT_OF_ORDER_TOLERANCE["twitter"], 5)
    
    def test_telegram_tolerance(self):
        """Telegram tolerance is 10 seconds."""
        self.assertEqual(OUT_OF_ORDER_TOLERANCE["telegram"], 10)
    
    def test_reddit_tolerance(self):
        """Reddit tolerance is 60 seconds."""
        self.assertEqual(OUT_OF_ORDER_TOLERANCE["reddit"], 60)


class TestTimeSyncGuardValidation(unittest.TestCase):
    """Test record validation in guard."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_valid_twitter_record(self):
        """Valid Twitter record passes through."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",  # 10 seconds ago
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)
        self.assertEqual(result, record)  # Passthrough - no modification
    
    def test_valid_reddit_record(self):
        """Valid Reddit record passes through."""
        record = {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:28:00Z",  # 2 minutes ago (within 120s)
            "text": "Bitcoin discussion",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.7
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)
    
    def test_valid_telegram_record(self):
        """Valid Telegram record passes through."""
        record = {
            "source": "telegram",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:35Z",  # 25 seconds ago (within 30s)
            "text": "Bitcoin alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.3
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)
    
    def test_drop_missing_timestamp(self):
        """Missing timestamp drops record."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0}
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_missing_timestamp, 1)
    
    def test_drop_empty_timestamp(self):
        """Empty timestamp drops record."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "",
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0}
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
    
    def test_drop_invalid_format(self):
        """Invalid timestamp format drops record."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "not a timestamp",
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0}
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_invalid_format, 1)
    
    def test_drop_unknown_source(self):
        """Unknown source drops record."""
        record = {
            "source": "facebook",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "Alert",
            "metrics": {"velocity": 1.0}
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)


class TestFutureEventDetection(unittest.TestCase):
    """Test future event detection."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_drop_future_event(self):
        """Future event is dropped."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:10Z",  # 10 seconds in future
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_future_events, 1)
    
    def test_accept_current_time(self):
        """Event at current time is accepted."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:30:00Z",  # Exactly current time
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)


class TestLateEventDetection(unittest.TestCase):
    """Test late event detection by source."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_drop_late_twitter(self):
        """Twitter event > 15 seconds old is dropped."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:40Z",  # 20 seconds ago
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_late_events, 1)
    
    def test_accept_twitter_within_limit(self):
        """Twitter event within 15 seconds is accepted."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",  # 10 seconds ago
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)
    
    def test_drop_late_telegram(self):
        """Telegram event > 30 seconds old is dropped."""
        record = {
            "source": "telegram",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:20Z",  # 40 seconds ago
            "text": "Bitcoin alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.3
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_late_events, 1)
    
    def test_accept_telegram_within_limit(self):
        """Telegram event within 30 seconds is accepted."""
        record = {
            "source": "telegram",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:35Z",  # 25 seconds ago
            "text": "Bitcoin alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.3
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)
    
    def test_drop_late_reddit(self):
        """Reddit event > 120 seconds old is dropped."""
        record = {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:27:30Z",  # 2.5 minutes ago
            "text": "Bitcoin discussion",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.7
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_late_events, 1)
    
    def test_accept_reddit_within_limit(self):
        """Reddit event within 120 seconds is accepted."""
        record = {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:28:30Z",  # 90 seconds ago
            "text": "Bitcoin discussion",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.7
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)


class TestOutOfOrderDetection(unittest.TestCase):
    """Test out-of-order event detection."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_first_event_accepted(self):
        """First event is always accepted (no history)."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC first",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result = self.guard.validate_record(record)
        
        self.assertIsNotNone(result)
    
    def test_drop_out_of_order_twitter(self):
        """Twitter event out of order beyond 5s tolerance is dropped."""
        # First event at 10:29:55
        record1 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:55Z",
            "text": "$BTC first",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        self.guard.validate_record(record1)
        
        # Second event at 10:29:45 (10 seconds earlier, beyond 5s tolerance)
        record2 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:45Z",
            "text": "$BTC second",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        result = self.guard.validate_record(record2)
        
        self.assertIsNone(result)
        self.assertEqual(self.guard.metrics.dropped_out_of_order, 1)
    
    def test_accept_within_tolerance_twitter(self):
        """Twitter event within 5s tolerance is accepted."""
        # First event at 10:29:55
        record1 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:55Z",
            "text": "$BTC first",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        self.guard.validate_record(record1)
        
        # Second event at 10:29:52 (3 seconds earlier, within 5s tolerance)
        record2 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:52Z",
            "text": "$BTC second",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        result = self.guard.validate_record(record2)
        
        self.assertIsNotNone(result)
    
    def test_reddit_larger_tolerance(self):
        """Reddit has 60 second tolerance for out-of-order."""
        # First event at 10:29:00
        record1 = {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:00Z",
            "text": "Reddit first",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.7
        }
        self.guard.validate_record(record1)
        
        # Second event at 10:28:10 (50 seconds earlier, within 60s tolerance)
        record2 = {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:28:10Z",
            "text": "Reddit second",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.7
        }
        result = self.guard.validate_record(record2)
        
        self.assertIsNotNone(result)


class TestDuplicateDetectionIntegration(unittest.TestCase):
    """Test duplicate detection in guard."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_drop_duplicate(self):
        """Duplicate record is dropped."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC alert",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        # First occurrence
        result1 = self.guard.validate_record(record)
        # Duplicate
        result2 = self.guard.validate_record(record)
        
        self.assertIsNotNone(result1)
        self.assertIsNone(result2)
        self.assertEqual(self.guard.metrics.dropped_duplicates, 1)
    
    def test_accept_different_text(self):
        """Same timestamp but different text is not duplicate."""
        record1 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC alert one",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        record2 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC alert two",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
        
        result1 = self.guard.validate_record(record1)
        result2 = self.guard.validate_record(record2)
        
        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)


class TestBatchValidation(unittest.TestCase):
    """Test batch record validation."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_batch_filters_invalid(self):
        """Batch validation filters out invalid records."""
        records = [
            {
                "source": "twitter",
                "asset": "BTC",
                "timestamp": "2026-01-17T10:29:50Z",  # Valid
                "text": "$BTC valid",
                "metrics": {"velocity": 1.0},
                "source_reliability": 0.5
            },
            {
                "source": "twitter",
                "asset": "BTC",
                "timestamp": "2026-01-17T10:30:10Z",  # Future - invalid
                "text": "$BTC future",
                "metrics": {"velocity": 1.0},
                "source_reliability": 0.5
            },
            {
                "source": "twitter",
                "asset": "BTC",
                # Missing timestamp - invalid
                "text": "$BTC no timestamp",
                "metrics": {"velocity": 1.0},
                "source_reliability": 0.5
            }
        ]
        
        valid = self.guard.validate_batch(records)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0]["text"], "$BTC valid")
    
    def test_batch_returns_passthrough(self):
        """Batch returns records unchanged (passthrough)."""
        records = [
            {
                "source": "twitter",
                "asset": "BTC",
                "timestamp": "2026-01-17T10:29:50Z",
                "text": "$BTC alert",
                "metrics": {"velocity": 1.0, "custom_field": 123},
                "source_reliability": 0.5,
                "extra_field": "preserved"
            }
        ]
        
        valid = self.guard.validate_batch(records)
        
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0], records[0])  # Exact same object


class TestMetrics(unittest.TestCase):
    """Test metrics tracking."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_metrics_initial(self):
        """Initial metrics are zero."""
        metrics = self.guard.get_metrics()
        
        self.assertEqual(metrics["total_processed"], 0)
        self.assertEqual(metrics["total_passed"], 0)
        self.assertEqual(metrics["dropped_future_events"], 0)
    
    def test_metrics_count_processed(self):
        """Total processed is counted."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC",
            "metrics": {}
        }
        
        self.guard.validate_record(record)
        self.guard.validate_record(record)
        
        metrics = self.guard.get_metrics()
        self.assertEqual(metrics["total_processed"], 2)
    
    def test_metrics_count_passed(self):
        """Total passed is counted (excluding duplicates)."""
        record1 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC one",
            "metrics": {}
        }
        record2 = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:55Z",
            "text": "$BTC two",
            "metrics": {}
        }
        
        self.guard.validate_record(record1)
        self.guard.validate_record(record2)
        
        metrics = self.guard.get_metrics()
        self.assertEqual(metrics["total_passed"], 2)
    
    def test_reset_metrics(self):
        """Reset metrics to zero."""
        record = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC",
            "metrics": {}
        }
        
        self.guard.validate_record(record)
        self.guard.reset_metrics()
        
        metrics = self.guard.get_metrics()
        self.assertEqual(metrics["total_processed"], 0)


class TestPassthrough(unittest.TestCase):
    """Test that records pass through unmodified."""
    
    def setUp(self):
        """Create guard with fixed time."""
        self.fixed_time = datetime(2026, 1, 17, 10, 30, 0, tzinfo=timezone.utc)
        self.guard = TimeSyncGuard(time_provider=lambda: self.fixed_time)
    
    def test_no_modification(self):
        """Records are not modified."""
        original = {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": "2026-01-17T10:29:50Z",
            "text": "$BTC alert",
            "metrics": {
                "engagement_weight": 5.2,
                "author_weight": 8.1,
                "velocity": 2.5
            },
            "source_reliability": 0.5,
            "extra_field": "should be preserved"
        }
        
        result = self.guard.validate_record(original)
        
        self.assertEqual(result, original)
        self.assertIs(result, original)  # Same object reference


class TestFactoryFunction(unittest.TestCase):
    """Test factory function."""
    
    def test_create_guard(self):
        """Factory creates guard instance."""
        guard = create_guard()
        self.assertIsInstance(guard, TimeSyncGuard)
    
    def test_create_guard_with_time_provider(self):
        """Factory accepts custom time provider."""
        fixed_time = datetime(2026, 1, 17, 12, 0, 0, tzinfo=timezone.utc)
        guard = create_guard(time_provider=lambda: fixed_time)
        
        self.assertEqual(guard._get_current_time(), fixed_time)
    
    def test_reset_tracking(self):
        """Reset tracking clears all state."""
        guard = create_guard()
        
        # Add some state
        guard.event_tracker.update("twitter", "BTC", datetime.now(timezone.utc))
        guard.metrics.total_processed = 10
        
        guard.reset_tracking()
        
        self.assertIsNone(guard.event_tracker.get_last_seen("twitter", "BTC"))
        self.assertEqual(guard.metrics.total_processed, 0)


if __name__ == "__main__":
    unittest.main()
