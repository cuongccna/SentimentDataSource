"""
Unit tests for Telegram Ingestion Worker.
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
import time
from datetime import datetime, timezone, timedelta

from telegram_ingestion import (
    # Constants
    SOURCE_RELIABILITY_TELEGRAM,
    DEFAULT_GLOBAL_RATE_LIMIT,
    DEFAULT_GROUP_RATE_LIMIT,
    RATE_LIMIT_WINDOW_SECONDS,
    MANIPULATION_SIMILARITY_THRESHOLD,
    MANIPULATION_WINDOW_SECONDS,
    ASSET_KEYWORDS_BTC,
    
    # Enums
    SourceType,
    SourceRole,
    MessageDropReason,
    
    # Data classes
    TelegramSource,
    TelegramMessage,
    ProcessedMessage,
    IngestionMetrics,
    
    # Classes
    TelegramSourceRegistry,
    MessageRateLimiter,
    ManipulationDetector,
    VelocityCalculator,
    TelegramMessageFilter,
    TelegramIngestionWorker,
    
    # Factory functions
    create_source_registry,
    create_ingestion_worker,
    create_test_worker,
    get_create_telegram_sources_sql
)


class TestConstants(unittest.TestCase):
    """Tests for constants."""
    
    def test_source_reliability_telegram(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY_TELEGRAM, 0.3, places=5)
    
    def test_default_global_rate_limit(self):
        self.assertEqual(DEFAULT_GLOBAL_RATE_LIMIT, 100)
    
    def test_default_group_rate_limit(self):
        self.assertEqual(DEFAULT_GROUP_RATE_LIMIT, 30)
    
    def test_rate_limit_window(self):
        self.assertEqual(RATE_LIMIT_WINDOW_SECONDS, 60)
    
    def test_manipulation_threshold(self):
        self.assertEqual(MANIPULATION_SIMILARITY_THRESHOLD, 3)
    
    def test_manipulation_window(self):
        self.assertEqual(MANIPULATION_WINDOW_SECONDS, 300)
    
    def test_btc_keywords(self):
        self.assertIn("btc", ASSET_KEYWORDS_BTC)
        self.assertIn("bitcoin", ASSET_KEYWORDS_BTC)
        self.assertIn("$btc", ASSET_KEYWORDS_BTC)


class TestSourceTypeEnum(unittest.TestCase):
    """Tests for SourceType enum."""
    
    def test_channel(self):
        self.assertEqual(SourceType.CHANNEL.value, "channel")
    
    def test_group(self):
        self.assertEqual(SourceType.GROUP.value, "group")


class TestSourceRoleEnum(unittest.TestCase):
    """Tests for SourceRole enum."""
    
    def test_news(self):
        self.assertEqual(SourceRole.NEWS.value, "news")
    
    def test_panic(self):
        self.assertEqual(SourceRole.PANIC.value, "panic")
    
    def test_community(self):
        self.assertEqual(SourceRole.COMMUNITY.value, "community")


class TestMessageDropReasonEnum(unittest.TestCase):
    """Tests for MessageDropReason enum."""
    
    def test_not_whitelisted(self):
        self.assertEqual(MessageDropReason.NOT_WHITELISTED.value, "not_whitelisted")
    
    def test_source_disabled(self):
        self.assertEqual(MessageDropReason.SOURCE_DISABLED.value, "source_disabled")
    
    def test_group_rate_exceeded(self):
        self.assertEqual(MessageDropReason.GROUP_RATE_EXCEEDED.value, "group_rate_exceeded")
    
    def test_global_rate_exceeded(self):
        self.assertEqual(MessageDropReason.GLOBAL_RATE_EXCEEDED.value, "global_rate_exceeded")
    
    def test_empty_text(self):
        self.assertEqual(MessageDropReason.EMPTY_TEXT.value, "empty_text")
    
    def test_no_asset_keyword(self):
        self.assertEqual(MessageDropReason.NO_ASSET_KEYWORD.value, "no_asset_keyword")


class TestTelegramSource(unittest.TestCase):
    """Tests for TelegramSource dataclass."""
    
    def test_create_source(self):
        source = TelegramSource(
            tg_id=123456789,
            source_type=SourceType.CHANNEL,
            role=SourceRole.NEWS,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=30
        )
        self.assertEqual(source.tg_id, 123456789)
        self.assertEqual(source.source_type, SourceType.CHANNEL)
        self.assertEqual(source.role, SourceRole.NEWS)
    
    def test_to_dict(self):
        source = TelegramSource(
            tg_id=123456789,
            source_type=SourceType.GROUP,
            role=SourceRole.PANIC,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=20
        )
        d = source.to_dict()
        self.assertEqual(d["tg_id"], 123456789)
        self.assertEqual(d["type"], "group")
        self.assertEqual(d["role"], "panic")


class TestTelegramMessage(unittest.TestCase):
    """Tests for TelegramMessage dataclass."""
    
    def test_create_message(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123456789,
            text="Bitcoin is pumping!",
            timestamp=now
        )
        self.assertEqual(msg.chat_id, 123456789)
        self.assertEqual(msg.text, "Bitcoin is pumping!")
    
    def test_to_dict(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123456789,
            text="BTC alert",
            timestamp=now,
            message_id=100,
            sender_id=999
        )
        d = msg.to_dict()
        self.assertEqual(d["chat_id"], 123456789)
        self.assertEqual(d["message_id"], 100)


class TestProcessedMessage(unittest.TestCase):
    """Tests for ProcessedMessage dataclass."""
    
    def test_create_processed(self):
        msg = ProcessedMessage(
            chat_id=123456789,
            text="BTC news",
            event_time="2026-01-17T10:00:00Z",
            asset="BTC"
        )
        self.assertEqual(msg.source, "telegram")
        self.assertAlmostEqual(msg.source_reliability, 0.3, places=5)
    
    def test_to_dict(self):
        msg = ProcessedMessage(
            chat_id=123456789,
            text="BTC news",
            event_time="2026-01-17T10:00:00Z",
            asset="BTC",
            velocity=1.5,
            manipulation_flag=True
        )
        d = msg.to_dict()
        self.assertEqual(d["source"], "telegram")
        self.assertEqual(d["velocity"], 1.5)
        self.assertTrue(d["manipulation_flag"])


class TestIngestionMetrics(unittest.TestCase):
    """Tests for IngestionMetrics dataclass."""
    
    def test_default_values(self):
        metrics = IngestionMetrics()
        self.assertEqual(metrics.received, 0)
        self.assertEqual(metrics.accepted, 0)
    
    def test_to_dict(self):
        metrics = IngestionMetrics()
        metrics.received = 10
        metrics.accepted = 5
        d = metrics.to_dict()
        self.assertEqual(d["received"], 10)
        self.assertEqual(d["accepted"], 5)
    
    def test_reset(self):
        metrics = IngestionMetrics()
        metrics.received = 100
        metrics.accepted = 50
        metrics.reset()
        self.assertEqual(metrics.received, 0)
        self.assertEqual(metrics.accepted, 0)


class TestTelegramSourceRegistry(unittest.TestCase):
    """Tests for TelegramSourceRegistry."""
    
    def setUp(self):
        self.registry = TelegramSourceRegistry()
    
    def tearDown(self):
        self.registry.clear()
    
    def test_empty_registry(self):
        self.assertEqual(self.registry.count(), 0)
    
    def test_load_from_list(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        count = self.registry.load_from_list(sources)
        self.assertEqual(count, 1)
    
    def test_is_whitelisted_true(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        self.registry.load_from_list(sources)
        self.assertTrue(self.registry.is_whitelisted(123))
    
    def test_is_whitelisted_false(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        self.registry.load_from_list(sources)
        self.assertFalse(self.registry.is_whitelisted(999))
    
    def test_get_source(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        self.registry.load_from_list(sources)
        source = self.registry.get_source(123)
        self.assertIsNotNone(source)
        self.assertEqual(source.tg_id, 123)
    
    def test_get_source_not_found(self):
        source = self.registry.get_source(999)
        self.assertIsNone(source)
    
    def test_get_all_whitelisted_ids(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            ),
            TelegramSource(
                tg_id=456,
                source_type=SourceType.GROUP,
                role=SourceRole.PANIC,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=20
            )
        ]
        self.registry.load_from_list(sources)
        ids = self.registry.get_all_whitelisted_ids()
        self.assertEqual(len(ids), 2)
        self.assertIn(123, ids)
        self.assertIn(456, ids)
    
    def test_get_enabled_sources(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            ),
            TelegramSource(
                tg_id=456,
                source_type=SourceType.GROUP,
                role=SourceRole.PANIC,
                asset="BTC",
                enabled=False,
                max_msgs_per_min=20
            )
        ]
        self.registry.load_from_list(sources)
        enabled = self.registry.get_enabled_sources()
        self.assertEqual(len(enabled), 1)
        self.assertEqual(enabled[0].tg_id, 123)
    
    def test_clear(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        self.registry.load_from_list(sources)
        self.assertEqual(self.registry.count(), 1)
        self.registry.clear()
        self.assertEqual(self.registry.count(), 0)


class TestMessageRateLimiter(unittest.TestCase):
    """Tests for MessageRateLimiter."""
    
    def setUp(self):
        self.limiter = MessageRateLimiter(global_limit=10, window_seconds=60)
    
    def tearDown(self):
        self.limiter.clear()
    
    def test_check_group_rate_allowed(self):
        now = time.time()
        self.assertTrue(self.limiter.check_group_rate(123, 5, now))
    
    def test_check_group_rate_exceeded(self):
        now = time.time()
        for _ in range(5):
            self.limiter.record_message(123, now)
        self.assertFalse(self.limiter.check_group_rate(123, 5, now))
    
    def test_check_global_rate_allowed(self):
        now = time.time()
        self.assertTrue(self.limiter.check_global_rate(now))
    
    def test_check_global_rate_exceeded(self):
        now = time.time()
        for i in range(10):
            self.limiter.record_message(i, now)
        self.assertFalse(self.limiter.check_global_rate(now))
    
    def test_get_group_count(self):
        now = time.time()
        self.limiter.record_message(123, now)
        self.limiter.record_message(123, now)
        self.assertEqual(self.limiter.get_group_count(123), 2)
    
    def test_get_global_count(self):
        now = time.time()
        self.limiter.record_message(123, now)
        self.limiter.record_message(456, now)
        self.assertEqual(self.limiter.get_global_count(), 2)
    
    def test_clear(self):
        now = time.time()
        self.limiter.record_message(123, now)
        self.limiter.clear()
        self.assertEqual(self.limiter.get_group_count(123), 0)
        self.assertEqual(self.limiter.get_global_count(), 0)


class TestManipulationDetector(unittest.TestCase):
    """Tests for ManipulationDetector."""
    
    def setUp(self):
        self.detector = ManipulationDetector(threshold=3, window_seconds=300)
    
    def tearDown(self):
        self.detector.clear()
    
    def test_no_manipulation_single_message(self):
        now = time.time()
        is_manip, fingerprint = self.detector.check_manipulation(
            "Bitcoin is great", 123, now
        )
        self.assertFalse(is_manip)
        self.assertEqual(len(fingerprint), 16)
    
    def test_no_manipulation_different_messages(self):
        now = time.time()
        is_manip1, _ = self.detector.check_manipulation("Bitcoin up", 123, now)
        is_manip2, _ = self.detector.check_manipulation("Ethereum down", 456, now)
        self.assertFalse(is_manip1)
        self.assertFalse(is_manip2)
    
    def test_manipulation_detected(self):
        now = time.time()
        text = "BTC to the moon!"
        
        # Same message in different chats
        self.detector.check_manipulation(text, 111, now)
        self.detector.check_manipulation(text, 222, now)
        is_manip, _ = self.detector.check_manipulation(text, 333, now)
        
        self.assertTrue(is_manip)
    
    def test_fingerprint_ignores_case(self):
        now = time.time()
        text1 = "Bitcoin is GREAT"
        text2 = "bitcoin is great"
        
        _, fp1 = self.detector.check_manipulation(text1, 123, now)
        _, fp2 = self.detector.check_manipulation(text2, 456, now)
        
        self.assertEqual(fp1, fp2)
    
    def test_fingerprint_ignores_numbers(self):
        now = time.time()
        text1 = "Bitcoin price 50000"
        text2 = "Bitcoin price 60000"
        
        _, fp1 = self.detector.check_manipulation(text1, 123, now)
        _, fp2 = self.detector.check_manipulation(text2, 456, now)
        
        self.assertEqual(fp1, fp2)
    
    def test_clear(self):
        now = time.time()
        self.detector.check_manipulation("test", 123, now)
        self.detector.clear()
        # After clear, same message should not trigger manipulation
        is_manip, _ = self.detector.check_manipulation("test", 456, now)
        self.assertFalse(is_manip)


class TestVelocityCalculator(unittest.TestCase):
    """Tests for VelocityCalculator."""
    
    def setUp(self):
        self.calculator = VelocityCalculator(short_window=60, long_window=300)
    
    def tearDown(self):
        self.calculator.clear()
    
    def test_no_messages_zero_velocity(self):
        velocity = self.calculator.get_velocity(123)
        self.assertAlmostEqual(velocity, 0.0, places=2)
    
    def test_record_and_get_velocity(self):
        now = time.time()
        
        # Record some messages
        for i in range(5):
            self.calculator.record_message(123, now - i)
        
        velocity = self.calculator.get_velocity(123, now)
        self.assertGreater(velocity, 0)
    
    def test_velocity_increases_with_burst(self):
        now = time.time()
        
        # Baseline: spread messages over long window
        for i in range(5):
            self.calculator.record_message(123, now - 200 + i * 40)
        
        v1 = self.calculator.get_velocity(123, now)
        
        # Add burst in short window
        for i in range(10):
            self.calculator.record_message(123, now - i)
        
        v2 = self.calculator.get_velocity(123, now)
        
        self.assertGreater(v2, v1)
    
    def test_clear(self):
        now = time.time()
        self.calculator.record_message(123, now)
        self.calculator.clear()
        velocity = self.calculator.get_velocity(123, now)
        self.assertAlmostEqual(velocity, 0.0, places=2)


class TestTelegramMessageFilter(unittest.TestCase):
    """Tests for TelegramMessageFilter."""
    
    def setUp(self):
        self.registry = TelegramSourceRegistry()
        self.rate_limiter = MessageRateLimiter(global_limit=100)
        
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            ),
            TelegramSource(
                tg_id=456,
                source_type=SourceType.GROUP,
                role=SourceRole.PANIC,
                asset="BTC",
                enabled=False,
                max_msgs_per_min=20
            )
        ]
        self.registry.load_from_list(sources)
        
        self.filter = TelegramMessageFilter(
            registry=self.registry,
            rate_limiter=self.rate_limiter
        )
    
    def tearDown(self):
        self.registry.clear()
        self.rate_limiter.clear()
    
    def test_accept_valid_message(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin is pumping!",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertTrue(accepted)
        self.assertIsNone(reason)
        self.assertIsNotNone(source)
    
    def test_drop_not_whitelisted(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=999,
            text="Bitcoin news",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertFalse(accepted)
        self.assertEqual(reason, MessageDropReason.NOT_WHITELISTED)
    
    def test_drop_disabled_source(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=456,  # This source is disabled
            text="Bitcoin alert",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertFalse(accepted)
        self.assertEqual(reason, MessageDropReason.SOURCE_DISABLED)
    
    def test_drop_empty_text(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertFalse(accepted)
        self.assertEqual(reason, MessageDropReason.EMPTY_TEXT)
    
    def test_drop_whitespace_text(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="   \n\t   ",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertFalse(accepted)
        self.assertEqual(reason, MessageDropReason.EMPTY_TEXT)
    
    def test_drop_no_asset_keyword(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Hello world!",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertFalse(accepted)
        self.assertEqual(reason, MessageDropReason.NO_ASSET_KEYWORD)
    
    def test_drop_invalid_timestamp(self):
        msg = TelegramMessage(
            chat_id=123,
            text="BTC alert",
            timestamp=None
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertFalse(accepted)
        self.assertEqual(reason, MessageDropReason.INVALID_TIMESTAMP)
    
    def test_asset_keyword_btc(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="BTC is up 10%",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertTrue(accepted)
    
    def test_asset_keyword_bitcoin(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin price rising",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertTrue(accepted)
    
    def test_asset_keyword_dollar_btc(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="$BTC looking good",
            timestamp=now
        )
        accepted, reason, source = self.filter.filter_message(msg)
        self.assertTrue(accepted)


class TestTelegramIngestionWorker(unittest.TestCase):
    """Tests for TelegramIngestionWorker."""
    
    def setUp(self):
        self.sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            ),
            TelegramSource(
                tg_id=456,
                source_type=SourceType.GROUP,
                role=SourceRole.PANIC,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=20
            )
        ]
        self.worker = create_test_worker(sources=self.sources)
    
    def test_start_stop(self):
        self.assertFalse(self.worker.is_running)
        self.worker.start()
        self.assertTrue(self.worker.is_running)
        self.worker.stop()
        self.assertFalse(self.worker.is_running)
    
    def test_handle_valid_message(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin alert!",
            timestamp=now
        )
        
        result = self.worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.chat_id, 123)
        self.assertEqual(result.source, "telegram")
        self.assertEqual(result.asset, "BTC")
    
    def test_handle_not_whitelisted(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=999,  # Not in whitelist
            text="Bitcoin news",
            timestamp=now
        )
        
        result = self.worker.handle_message(msg)
        
        self.assertIsNone(result)
    
    def test_metrics_received(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="BTC update",
            timestamp=now
        )
        
        self.worker.handle_message(msg)
        
        metrics = self.worker.get_metrics()
        self.assertEqual(metrics["received"], 1)
        self.assertEqual(metrics["accepted"], 1)
    
    def test_metrics_dropped(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=999,  # Not whitelisted
            text="BTC news",
            timestamp=now
        )
        
        self.worker.handle_message(msg)
        
        metrics = self.worker.get_metrics()
        self.assertEqual(metrics["received"], 1)
        self.assertEqual(metrics["dropped_not_whitelisted"], 1)
    
    def test_reset_metrics(self):
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin news",
            timestamp=now
        )
        
        self.worker.handle_message(msg)
        self.worker.reset_metrics()
        
        metrics = self.worker.get_metrics()
        self.assertEqual(metrics["received"], 0)
    
    def test_get_whitelisted_ids(self):
        ids = self.worker.get_whitelisted_ids()
        self.assertEqual(len(ids), 2)
        self.assertIn(123, ids)
        self.assertIn(456, ids)
    
    def test_callback_invoked(self):
        results = []
        
        def on_message(msg):
            results.append(msg)
        
        worker = create_test_worker(sources=self.sources)
        worker.on_message = on_message
        
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="BTC alert",
            timestamp=now
        )
        
        worker.handle_message(msg)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].chat_id, 123)
    
    def test_velocity_computed(self):
        now = datetime.now(timezone.utc)
        
        # Send multiple messages
        for i in range(5):
            msg = TelegramMessage(
                chat_id=123,
                text=f"BTC update {i}",
                timestamp=now
            )
            result = self.worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertGreaterEqual(result.velocity, 0)
    
    def test_manipulation_flag_computed(self):
        now = datetime.now(timezone.utc)
        
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin is pumping!",
            timestamp=now
        )
        
        result = self.worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result.manipulation_flag, bool)
    
    def test_fingerprint_computed(self):
        now = datetime.now(timezone.utc)
        
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin news update",
            timestamp=now
        )
        
        result = self.worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.fingerprint)
        self.assertEqual(len(result.fingerprint), 16)


class TestWhitelistEnforcement(unittest.TestCase):
    """Tests to verify whitelist enforcement contract."""
    
    def test_non_whitelisted_completely_ignored(self):
        """CRITICAL: Non-whitelisted chats must be completely ignored."""
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        
        # Message from non-whitelisted chat
        msg = TelegramMessage(
            chat_id=999999,  # Not in whitelist
            text="Bitcoin news",
            timestamp=now
        )
        
        result = worker.handle_message(msg)
        
        # Must be completely ignored
        self.assertIsNone(result)
    
    def test_empty_whitelist_all_ignored(self):
        """With empty whitelist, all messages must be ignored."""
        worker = create_test_worker(sources=[])
        
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin alert",
            timestamp=now
        )
        
        result = worker.handle_message(msg)
        
        self.assertIsNone(result)
    
    def test_disabled_source_ignored(self):
        """Disabled sources must be ignored."""
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=False,  # Disabled
                max_msgs_per_min=30
            )
        ]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin news",
            timestamp=now
        )
        
        result = worker.handle_message(msg)
        
        self.assertIsNone(result)


class TestRateLimitEnforcement(unittest.TestCase):
    """Tests for rate limit enforcement."""
    
    def test_group_rate_limit_enforced(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=3  # Low limit for testing
            )
        ]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        accepted = 0
        
        for i in range(5):
            msg = TelegramMessage(
                chat_id=123,
                text=f"BTC news {i}",
                timestamp=now
            )
            if worker.handle_message(msg):
                accepted += 1
        
        # Should only accept up to rate limit
        self.assertEqual(accepted, 3)
    
    def test_global_rate_limit_enforced(self):
        sources = [
            TelegramSource(
                tg_id=i,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=100  # High per-group limit
            )
            for i in range(20)
        ]
        worker = create_test_worker(sources=sources, global_rate_limit=5)
        
        now = datetime.now(timezone.utc)
        accepted = 0
        
        for i in range(10):
            msg = TelegramMessage(
                chat_id=i,  # Different chats
                text=f"BTC update",
                timestamp=now
            )
            if worker.handle_message(msg):
                accepted += 1
        
        # Should only accept up to global limit
        self.assertEqual(accepted, 5)


class TestFactoryFunctions(unittest.TestCase):
    """Tests for factory functions."""
    
    def test_create_source_registry(self):
        registry = create_source_registry()
        self.assertIsInstance(registry, TelegramSourceRegistry)
        self.assertEqual(registry.count(), 0)
    
    def test_create_ingestion_worker(self):
        worker = create_ingestion_worker()
        self.assertIsInstance(worker, TelegramIngestionWorker)
    
    def test_create_test_worker(self):
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        worker = create_test_worker(sources=sources)
        self.assertIsInstance(worker, TelegramIngestionWorker)
        self.assertEqual(len(worker.get_whitelisted_ids()), 1)
    
    def test_get_create_sql(self):
        sql = get_create_telegram_sources_sql()
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("telegram_sources", sql)
        self.assertIn("tg_id", sql)
        self.assertIn("BIGINT", sql)


class TestContractCompliance(unittest.TestCase):
    """Tests to verify contract compliance."""
    
    def test_source_reliability_telegram(self):
        """Telegram source reliability must be 0.3."""
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin news",
            timestamp=now
        )
        
        result = worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result.source_reliability, 0.3, places=5)
    
    def test_source_field_telegram(self):
        """Source field must be 'telegram'."""
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.NEWS,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="BTC update",
            timestamp=now
        )
        
        result = worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "telegram")
    
    def test_role_preserved(self):
        """Source role must be preserved in output."""
        sources = [
            TelegramSource(
                tg_id=123,
                source_type=SourceType.CHANNEL,
                role=SourceRole.PANIC,
                asset="BTC",
                enabled=True,
                max_msgs_per_min=30
            )
        ]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        msg = TelegramMessage(
            chat_id=123,
            text="Bitcoin panic!",
            timestamp=now
        )
        
        result = worker.handle_message(msg)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.role, "panic")


if __name__ == "__main__":
    unittest.main()
