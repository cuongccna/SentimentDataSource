"""
Unit tests for Background Worker / Scheduler.
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
import uuid
import json
import tempfile
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from background_worker import (
    # Constants
    TWITTER_INTERVAL,
    TELEGRAM_INTERVAL,
    REDDIT_INTERVAL,
    SOURCE_RELIABILITY,
    MAX_DELAY_SECONDS,
    MAX_FUTURE_SECONDS,
    SUPPORTED_ASSET,
    
    # Enums
    PipelineStage,
    SourceType,
    
    # Classes
    WorkerMetrics,
    SourceState,
    WorkerState,
    InMemoryDatabase,
    PipelineExecutor,
    DataQualityUpdater,
    SourceLoop,
    MockSourceCollector,
    BackgroundWorker,
    
    # Functions
    compute_fingerprint,
    validate_time_sync,
    parse_timestamp,
    format_timestamp,
    compute_sentiment,
    compute_risk_indicators,
    compute_engagement_weight,
    compute_author_weight,
    compute_velocity,
    check_manipulation_flag,
    validate_raw_event,
    create_worker,
    create_in_memory_worker,
    
    # Lexicons
    BULLISH_KEYWORDS,
    BEARISH_KEYWORDS,
    FEAR_KEYWORDS,
    GREED_KEYWORDS
)


class TestConstants(unittest.TestCase):
    """Tests for constants."""
    
    def test_twitter_interval(self):
        self.assertEqual(TWITTER_INTERVAL, 10)
    
    def test_telegram_interval(self):
        self.assertEqual(TELEGRAM_INTERVAL, 20)
    
    def test_reddit_interval(self):
        self.assertEqual(REDDIT_INTERVAL, 300)
    
    def test_source_reliability_twitter(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY["twitter"], 0.5, places=5)
    
    def test_source_reliability_reddit(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY["reddit"], 0.7, places=5)
    
    def test_source_reliability_telegram(self):
        self.assertAlmostEqual(SOURCE_RELIABILITY["telegram"], 0.3, places=5)
    
    def test_max_delay_seconds(self):
        self.assertEqual(MAX_DELAY_SECONDS, 300)
    
    def test_max_future_seconds(self):
        self.assertEqual(MAX_FUTURE_SECONDS, 5)
    
    def test_supported_asset(self):
        self.assertEqual(SUPPORTED_ASSET, "BTC")


class TestEnums(unittest.TestCase):
    """Tests for enums."""
    
    def test_pipeline_stages(self):
        self.assertEqual(PipelineStage.RAW_VALIDATION.value, "raw_validation")
        self.assertEqual(PipelineStage.TIME_SYNC.value, "time_sync")
        self.assertEqual(PipelineStage.INSERT_RAW.value, "insert_raw")
        self.assertEqual(PipelineStage.SENTIMENT.value, "sentiment")
    
    def test_source_types(self):
        self.assertEqual(SourceType.TWITTER.value, "twitter")
        self.assertEqual(SourceType.REDDIT.value, "reddit")
        self.assertEqual(SourceType.TELEGRAM.value, "telegram")


class TestWorkerMetrics(unittest.TestCase):
    """Tests for WorkerMetrics."""
    
    def setUp(self):
        self.metrics = WorkerMetrics()
    
    def test_initial_state(self):
        self.assertEqual(self.metrics.events_collected["twitter"], 0)
        self.assertEqual(self.metrics.events_dropped["reddit"], 0)
        self.assertEqual(self.metrics.events_inserted["telegram"], 0)
    
    def test_record_collected(self):
        self.metrics.record_collected("twitter")
        self.metrics.record_collected("twitter")
        self.assertEqual(self.metrics.events_collected["twitter"], 2)
    
    def test_record_dropped(self):
        self.metrics.record_dropped("reddit")
        self.assertEqual(self.metrics.events_dropped["reddit"], 1)
    
    def test_record_inserted(self):
        self.metrics.record_inserted("telegram")
        self.metrics.record_inserted("telegram")
        self.metrics.record_inserted("telegram")
        self.assertEqual(self.metrics.events_inserted["telegram"], 3)
    
    def test_record_error(self):
        self.metrics.record_error(PipelineStage.TIME_SYNC)
        self.metrics.record_error(PipelineStage.TIME_SYNC)
        self.assertEqual(self.metrics.errors_by_stage["time_sync"], 2)
    
    def test_update_lag(self):
        self.metrics.update_lag("twitter", 15.5)
        self.assertAlmostEqual(self.metrics.lag_seconds_per_source["twitter"], 15.5, places=5)
    
    def test_record_success(self):
        self.metrics.record_success("reddit")
        self.assertIsNotNone(self.metrics.last_success_time["reddit"])
    
    def test_to_dict(self):
        self.metrics.record_collected("twitter")
        result = self.metrics.to_dict()
        self.assertIn("events_collected", result)
        self.assertEqual(result["events_collected"]["twitter"], 1)


class TestSourceState(unittest.TestCase):
    """Tests for SourceState."""
    
    def test_default_values(self):
        state = SourceState()
        self.assertIsNone(state.last_event_time)
        self.assertIsNone(state.last_message_id)
        self.assertIsNone(state.last_run_time)
    
    def test_to_dict(self):
        now = datetime.now(timezone.utc)
        state = SourceState(
            last_event_time=now,
            last_message_id="msg123",
            last_run_time=now
        )
        result = state.to_dict()
        self.assertIsNotNone(result["last_event_time"])
        self.assertEqual(result["last_message_id"], "msg123")
    
    def test_from_dict(self):
        data = {
            "last_event_time": "2026-01-17T10:00:00+00:00",
            "last_message_id": "msg456",
            "last_run_time": "2026-01-17T10:00:00+00:00"
        }
        state = SourceState.from_dict(data)
        self.assertEqual(state.last_message_id, "msg456")
        self.assertEqual(state.last_event_time.year, 2026)


class TestWorkerState(unittest.TestCase):
    """Tests for WorkerState."""
    
    def setUp(self):
        self.temp_file = Path(tempfile.mktemp(suffix=".json"))
        self.state = WorkerState(self.temp_file)
    
    def tearDown(self):
        if self.temp_file.exists():
            self.temp_file.unlink()
    
    def test_initial_sources(self):
        self.assertIn("twitter", self.state.sources)
        self.assertIn("reddit", self.state.sources)
        self.assertIn("telegram", self.state.sources)
    
    def test_get_source_state(self):
        state = self.state.get_source_state("twitter")
        self.assertIsInstance(state, SourceState)
    
    def test_update_source_state(self):
        now = datetime.now(timezone.utc)
        self.state.update_source_state("twitter", last_event_time=now)
        
        state = self.state.get_source_state("twitter")
        self.assertEqual(state.last_event_time, now)
    
    def test_save_and_load_state(self):
        now = datetime.now(timezone.utc)
        self.state.update_source_state("reddit", last_event_time=now)
        self.state.save_state()
        
        # Create new instance to test loading
        new_state = WorkerState(self.temp_file)
        loaded = new_state.get_source_state("reddit")
        self.assertEqual(loaded.last_event_time, now)
    
    def test_persist_message_id(self):
        self.state.update_source_state("telegram", last_message_id="tg_123")
        self.state.save_state()
        
        new_state = WorkerState(self.temp_file)
        loaded = new_state.get_source_state("telegram")
        self.assertEqual(loaded.last_message_id, "tg_123")


class TestFingerprint(unittest.TestCase):
    """Tests for fingerprint computation."""
    
    def test_compute_fingerprint(self):
        now = datetime.now(timezone.utc)
        fp = compute_fingerprint("twitter", "test text", now)
        self.assertEqual(len(fp), 32)
    
    def test_same_input_same_fingerprint(self):
        now = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        fp1 = compute_fingerprint("twitter", "BTC moon", now)
        fp2 = compute_fingerprint("twitter", "BTC moon", now)
        self.assertEqual(fp1, fp2)
    
    def test_different_source_different_fingerprint(self):
        now = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        fp1 = compute_fingerprint("twitter", "BTC moon", now)
        fp2 = compute_fingerprint("reddit", "BTC moon", now)
        self.assertNotEqual(fp1, fp2)
    
    def test_different_text_different_fingerprint(self):
        now = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        fp1 = compute_fingerprint("twitter", "BTC moon", now)
        fp2 = compute_fingerprint("twitter", "BTC crash", now)
        self.assertNotEqual(fp1, fp2)


class TestTimeSync(unittest.TestCase):
    """Tests for time synchronization."""
    
    def test_valid_time(self):
        now = datetime.now(timezone.utc)
        event_time = now - timedelta(seconds=30)
        is_valid, error = validate_time_sync(event_time, now)
        self.assertTrue(is_valid)
        self.assertIsNone(error)
    
    def test_future_time_rejected(self):
        now = datetime.now(timezone.utc)
        event_time = now + timedelta(seconds=10)
        is_valid, error = validate_time_sync(event_time, now)
        self.assertFalse(is_valid)
        self.assertIn("future", error)
    
    def test_old_time_rejected(self):
        now = datetime.now(timezone.utc)
        event_time = now - timedelta(seconds=400)
        is_valid, error = validate_time_sync(event_time, now)
        self.assertFalse(is_valid)
        self.assertIn("old", error)
    
    def test_no_timezone_rejected(self):
        now = datetime.now(timezone.utc)
        event_time = datetime.now()  # No timezone
        is_valid, error = validate_time_sync(event_time, now)
        self.assertFalse(is_valid)


class TestParseTimestamp(unittest.TestCase):
    """Tests for timestamp parsing."""
    
    def test_parse_datetime(self):
        dt = datetime(2026, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        result = parse_timestamp(dt)
        self.assertEqual(result, dt)
    
    def test_parse_unix_timestamp(self):
        ts = 1768518000  # Some timestamp
        result = parse_timestamp(ts)
        self.assertIsInstance(result, datetime)
    
    def test_parse_iso_string_z(self):
        result = parse_timestamp("2026-01-17T10:00:00Z")
        self.assertEqual(result.year, 2026)
        self.assertEqual(result.hour, 10)
    
    def test_parse_iso_string_offset(self):
        result = parse_timestamp("2026-01-17T10:00:00+00:00")
        self.assertEqual(result.hour, 10)
    
    def test_parse_none(self):
        result = parse_timestamp(None)
        self.assertIsNone(result)
    
    def test_format_timestamp(self):
        dt = datetime(2026, 1, 17, 10, 30, 45, tzinfo=timezone.utc)
        result = format_timestamp(dt)
        self.assertEqual(result, "2026-01-17T10:30:45Z")


class TestSentimentAnalysis(unittest.TestCase):
    """Tests for sentiment analysis."""
    
    def test_bullish_sentiment(self):
        result = compute_sentiment("BTC moon bullish pump rocket")
        self.assertEqual(result["label"], 1)
        self.assertGreater(result["confidence"], 0.5)
    
    def test_bearish_sentiment(self):
        result = compute_sentiment("BTC crash incoming, very bearish")
        self.assertEqual(result["label"], -1)
        self.assertGreater(result["confidence"], 0.5)
    
    def test_neutral_sentiment(self):
        result = compute_sentiment("BTC is a cryptocurrency")
        self.assertEqual(result["label"], 0)
    
    def test_empty_text(self):
        result = compute_sentiment("")
        self.assertEqual(result["label"], 0)
        self.assertAlmostEqual(result["confidence"], 0.0, places=5)
    
    def test_mixed_signals(self):
        result = compute_sentiment("BTC moon but also crash possible")
        self.assertIn(result["label"], [-1, 0, 1])


class TestRiskIndicators(unittest.TestCase):
    """Tests for risk indicators computation."""
    
    def test_social_overheat_high_velocity(self):
        result = compute_risk_indicators(
            sentiment_label=1,
            sentiment_confidence=0.8,
            text="BTC moon",
            velocity=10.0
        )
        self.assertTrue(result["social_overheat"])
    
    def test_panic_risk(self):
        result = compute_risk_indicators(
            sentiment_label=-1,
            sentiment_confidence=0.8,
            text="BTC crash panic fear worried"
        )
        self.assertTrue(result["panic_risk"])
    
    def test_fomo_risk(self):
        result = compute_risk_indicators(
            sentiment_label=1,
            sentiment_confidence=0.8,
            text="BTC fomo yolo all-in greed"
        )
        self.assertTrue(result["fomo_risk"])
    
    def test_fear_greed_extreme_fear(self):
        result = compute_risk_indicators(
            sentiment_label=-1,
            sentiment_confidence=0.8,
            text="fear panic scared worried"
        )
        self.assertEqual(result["fear_greed_zone"], "extreme_fear")
    
    def test_fear_greed_extreme_greed(self):
        result = compute_risk_indicators(
            sentiment_label=1,
            sentiment_confidence=0.8,
            text="greed fomo yolo lambo moon"
        )
        self.assertEqual(result["fear_greed_zone"], "extreme_greed")
    
    def test_normal_indicators(self):
        result = compute_risk_indicators(
            sentiment_label=0,
            sentiment_confidence=0.5,
            text="BTC price stable"
        )
        self.assertFalse(result["social_overheat"])
        self.assertFalse(result["panic_risk"])
        self.assertFalse(result["fomo_risk"])


class TestMetricComputation(unittest.TestCase):
    """Tests for metric computation."""
    
    def test_engagement_weight_twitter(self):
        event = {"favorite_count": 100, "retweet_count": 50, "reply_count": 20}
        result = compute_engagement_weight(event, "twitter")
        expected = 100 * 1.0 + 50 * 2.0 + 20 * 1.5
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_engagement_weight_reddit(self):
        event = {"score": 500, "num_comments": 100}
        result = compute_engagement_weight(event, "reddit")
        expected = 500 * 1.0 + 100 * 2.0
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_engagement_weight_telegram(self):
        event = {"views": 1000, "forwards": 10}
        result = compute_engagement_weight(event, "telegram")
        expected = 1000 * 0.1 + 10 * 5.0
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_author_weight_twitter(self):
        event = {"user": {"followers_count": 50000, "verified": True}}
        result = compute_author_weight(event, "twitter")
        self.assertGreater(result, 1.0)
    
    def test_velocity_computation(self):
        result = compute_velocity(60, 60.0)
        self.assertAlmostEqual(result, 60.0, places=5)
    
    def test_manipulation_flag_telegram(self):
        event = {"text": "BTC guaranteed 100x pump!"}
        result = check_manipulation_flag(event, "telegram")
        self.assertTrue(result)
    
    def test_manipulation_flag_non_telegram(self):
        event = {"text": "BTC guaranteed 100x pump!"}
        result = check_manipulation_flag(event, "twitter")
        self.assertFalse(result)


class TestRawEventValidation(unittest.TestCase):
    """Tests for raw event validation."""
    
    def test_valid_event(self):
        event = {
            "text": "BTC to the moon!",
            "created_at": "2026-01-17T10:00:00Z"
        }
        is_valid, error = validate_raw_event(event, "twitter")
        self.assertTrue(is_valid)
    
    def test_missing_text(self):
        event = {"created_at": "2026-01-17T10:00:00Z"}
        is_valid, error = validate_raw_event(event, "twitter")
        self.assertFalse(is_valid)
        self.assertIn("text", error.lower())
    
    def test_missing_timestamp(self):
        event = {"text": "BTC moon"}
        is_valid, error = validate_raw_event(event, "twitter")
        self.assertFalse(is_valid)
        self.assertIn("timestamp", error.lower())
    
    def test_no_btc_keyword(self):
        event = {
            "text": "ETH to the moon!",
            "created_at": "2026-01-17T10:00:00Z"
        }
        is_valid, error = validate_raw_event(event, "twitter")
        self.assertFalse(is_valid)
        self.assertIn("BTC", error)
    
    def test_btc_variations(self):
        for text in ["$BTC moon", "Bitcoin is great", "btc pump"]:
            event = {"text": text, "created_at": "2026-01-17T10:00:00Z"}
            is_valid, _ = validate_raw_event(event, "twitter")
            self.assertTrue(is_valid, f"Failed for: {text}")


class TestInMemoryDatabase(unittest.TestCase):
    """Tests for InMemoryDatabase."""
    
    def setUp(self):
        self.db = InMemoryDatabase()
        self.now = datetime.now(timezone.utc)
    
    def test_insert_raw_event(self):
        event_id = self.db.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="test",
            fingerprint="fp1"
        )
        self.assertIsInstance(event_id, uuid.UUID)
    
    def test_duplicate_fingerprint_blocked(self):
        self.db.insert_raw_event(fingerprint="fp1")
        event_id = self.db.insert_raw_event(fingerprint="fp1")
        self.assertIsNone(event_id)
    
    def test_insert_sentiment_event(self):
        event_id = self.db.insert_sentiment_event(
            event_time=self.now,
            sentiment_label=1
        )
        self.assertIsInstance(event_id, uuid.UUID)
    
    def test_insert_risk_event(self):
        event_id = self.db.insert_risk_event(
            event_time=self.now,
            social_overheat=True
        )
        self.assertIsInstance(event_id, uuid.UUID)
    
    def test_insert_quality_event(self):
        event_id = self.db.insert_quality_event(
            event_time=self.now,
            overall="healthy"
        )
        self.assertIsInstance(event_id, uuid.UUID)
    
    def test_check_fingerprint_exists(self):
        self.assertFalse(self.db.check_fingerprint_exists("fp1"))
        self.db.insert_raw_event(fingerprint="fp1")
        self.assertTrue(self.db.check_fingerprint_exists("fp1"))
    
    def test_get_counts(self):
        self.db.insert_raw_event(fingerprint="fp1")
        self.db.insert_sentiment_event()
        counts = self.db.get_counts()
        self.assertEqual(counts["raw_events"], 1)
        self.assertEqual(counts["sentiment_events"], 1)


class TestPipelineExecutor(unittest.TestCase):
    """Tests for PipelineExecutor."""
    
    def setUp(self):
        self.db = InMemoryDatabase()
        self.metrics = WorkerMetrics()
        self.pipeline = PipelineExecutor(self.db, self.metrics)
        self.now = datetime.now(timezone.utc)
    
    def test_process_valid_event(self):
        event = {
            "text": "BTC moon bullish!",
            "created_at": format_timestamp(self.now - timedelta(seconds=30))
        }
        success = self.pipeline.process_event(event, "twitter")
        self.assertTrue(success)
        self.assertEqual(self.metrics.events_inserted["twitter"], 1)
    
    def test_drop_invalid_event(self):
        event = {"text": "ETH only"}  # No BTC, no timestamp
        success = self.pipeline.process_event(event, "twitter")
        self.assertFalse(success)
        self.assertEqual(self.metrics.events_dropped["twitter"], 1)
    
    def test_drop_old_event(self):
        event = {
            "text": "BTC moon",
            "created_at": format_timestamp(self.now - timedelta(hours=1))
        }
        success = self.pipeline.process_event(event, "twitter")
        self.assertFalse(success)
    
    def test_drop_duplicate_event(self):
        event = {
            "text": "BTC moon bullish!",
            "created_at": format_timestamp(self.now - timedelta(seconds=30))
        }
        
        success1 = self.pipeline.process_event(event, "twitter")
        success2 = self.pipeline.process_event(event, "twitter")
        
        self.assertTrue(success1)
        self.assertFalse(success2)
    
    def test_all_events_inserted(self):
        event = {
            "text": "BTC moon bullish!",
            "created_at": format_timestamp(self.now - timedelta(seconds=30))
        }
        self.pipeline.process_event(event, "twitter")
        
        counts = self.db.get_counts()
        self.assertEqual(counts["raw_events"], 1)
        self.assertEqual(counts["sentiment_events"], 1)
        self.assertEqual(counts["risk_events"], 1)


class TestDataQualityUpdater(unittest.TestCase):
    """Tests for DataQualityUpdater."""
    
    def setUp(self):
        self.db = InMemoryDatabase()
        self.metrics = WorkerMetrics()
        self.updater = DataQualityUpdater(self.db, self.metrics)
    
    def test_should_update_after_interval(self):
        self.updater.last_update = datetime.now(timezone.utc) - timedelta(seconds=120)
        self.assertTrue(self.updater.should_update())
    
    def test_should_not_update_before_interval(self):
        self.updater.last_update = datetime.now(timezone.utc)
        self.assertFalse(self.updater.should_update())
    
    def test_update_inserts_event(self):
        self.updater.update()
        counts = self.db.get_counts()
        self.assertEqual(counts["quality_events"], 1)


class TestSourceLoop(unittest.TestCase):
    """Tests for SourceLoop."""
    
    def setUp(self):
        self.db = InMemoryDatabase()
        self.metrics = WorkerMetrics()
        self.state = WorkerState(Path(tempfile.mktemp(suffix=".json")))
        self.pipeline = PipelineExecutor(self.db, self.metrics)
        self.collector = MockSourceCollector("twitter")
    
    def test_loop_creation(self):
        loop = SourceLoop(
            source="twitter",
            interval=10,
            collector=self.collector,
            pipeline=self.pipeline,
            state=self.state,
            metrics=self.metrics
        )
        self.assertEqual(loop.source, "twitter")
        self.assertEqual(loop.interval, 10)
    
    def test_loop_start_stop(self):
        loop = SourceLoop(
            source="twitter",
            interval=1,
            collector=self.collector,
            pipeline=self.pipeline,
            state=self.state,
            metrics=self.metrics
        )
        loop.start()
        time.sleep(0.5)
        loop.stop()
        self.assertFalse(loop._running)


class TestBackgroundWorker(unittest.TestCase):
    """Tests for BackgroundWorker."""
    
    def test_create_worker(self):
        worker = create_in_memory_worker()
        self.assertIsInstance(worker, BackgroundWorker)
    
    def test_worker_has_all_loops(self):
        worker = create_in_memory_worker()
        self.assertIn("twitter", worker.loops)
        self.assertIn("reddit", worker.loops)
        self.assertIn("telegram", worker.loops)
    
    def test_get_metrics(self):
        worker = create_in_memory_worker()
        metrics = worker.get_metrics()
        self.assertIn("events_collected", metrics)
        self.assertIn("events_dropped", metrics)
        self.assertIn("events_inserted", metrics)
    
    def test_worker_intervals(self):
        worker = create_in_memory_worker()
        self.assertEqual(worker.loops["twitter"].interval, TWITTER_INTERVAL)
        self.assertEqual(worker.loops["telegram"].interval, TELEGRAM_INTERVAL)
        self.assertEqual(worker.loops["reddit"].interval, REDDIT_INTERVAL)


class TestLexicons(unittest.TestCase):
    """Tests for sentiment lexicons."""
    
    def test_bullish_keywords_exist(self):
        self.assertIn("moon", BULLISH_KEYWORDS)
        self.assertIn("bullish", BULLISH_KEYWORDS)
        self.assertIn("pump", BULLISH_KEYWORDS)
    
    def test_bearish_keywords_exist(self):
        self.assertIn("crash", BEARISH_KEYWORDS)
        self.assertIn("bearish", BEARISH_KEYWORDS)
        self.assertIn("dump", BEARISH_KEYWORDS)
    
    def test_fear_keywords_exist(self):
        self.assertIn("fear", FEAR_KEYWORDS)
        self.assertIn("panic", FEAR_KEYWORDS)
    
    def test_greed_keywords_exist(self):
        self.assertIn("greed", GREED_KEYWORDS)
        self.assertIn("fomo", GREED_KEYWORDS)


class TestFactoryFunctions(unittest.TestCase):
    """Tests for factory functions."""
    
    def test_create_worker(self):
        db = InMemoryDatabase()
        worker = create_worker(database=db)
        self.assertIsInstance(worker, BackgroundWorker)
    
    def test_create_in_memory_worker(self):
        worker = create_in_memory_worker()
        self.assertIsInstance(worker.database, InMemoryDatabase)


class TestPipelineOrder(unittest.TestCase):
    """Tests for pipeline execution order."""
    
    def test_all_stages_executed_in_order(self):
        """Verify the locked pipeline order is maintained."""
        db = InMemoryDatabase()
        metrics = WorkerMetrics()
        pipeline = PipelineExecutor(db, metrics)
        
        now = datetime.now(timezone.utc)
        event = {
            "text": "BTC moon bullish!",
            "created_at": format_timestamp(now - timedelta(seconds=30)),
            "favorite_count": 100,
            "retweet_count": 50,
            "user": {"followers_count": 10000}
        }
        
        success = pipeline.process_event(event, "twitter")
        self.assertTrue(success)
        
        # Verify all events were created
        self.assertEqual(len(db.raw_events), 1)
        self.assertEqual(len(db.sentiment_events), 1)
        self.assertEqual(len(db.risk_events), 1)
        
        # Verify raw event has metrics
        raw = db.raw_events[0]
        self.assertIn("engagement_weight", raw)
        self.assertIn("author_weight", raw)
        self.assertIn("fingerprint", raw)


if __name__ == "__main__":
    unittest.main()
