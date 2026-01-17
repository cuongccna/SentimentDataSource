"""
Unit tests for PostgreSQL Event Store.
All tests use assertAlmostEqual where needed for float comparisons.

NO MOCKING. NO HALLUCINATION.
"""

import unittest
import uuid
from datetime import datetime, timezone, timedelta

from event_store import (
    EventStore,
    InMemoryEventStore,
    DatabaseConnection,
    RawEventRecord,
    SentimentEventRecord,
    RiskIndicatorRecord,
    DataQualityRecord,
    create_event_store,
    create_in_memory_store,
    get_create_tables_sql,
    CREATE_TABLES_SQL
)


class TestDatabaseConnection(unittest.TestCase):
    """Tests for DatabaseConnection interface."""
    
    def test_init_without_connection(self):
        conn = DatabaseConnection()
        self.assertIsNone(conn.connection)
    
    def test_execute_without_connection(self):
        conn = DatabaseConnection()
        result = conn.execute("SELECT 1")
        self.assertIsNone(result)
    
    def test_commit_without_connection(self):
        conn = DatabaseConnection()
        # Should not raise
        conn.commit()
    
    def test_rollback_without_connection(self):
        conn = DatabaseConnection()
        # Should not raise
        conn.rollback()


class TestRawEventRecord(unittest.TestCase):
    """Tests for RawEventRecord dataclass."""
    
    def test_required_fields(self):
        event_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        record = RawEventRecord(
            id=event_id,
            source="twitter",
            asset="BTC",
            event_time=now,
            ingest_time=now,
            text="Test tweet",
            source_reliability=0.5
        )
        
        self.assertEqual(record.id, event_id)
        self.assertEqual(record.source, "twitter")
        self.assertEqual(record.asset, "BTC")
        self.assertEqual(record.text, "Test tweet")
        self.assertAlmostEqual(record.source_reliability, 0.5, places=5)
    
    def test_optional_fields_defaults(self):
        event_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        record = RawEventRecord(
            id=event_id,
            source="twitter",
            asset="BTC",
            event_time=now,
            ingest_time=now,
            text="Test",
            source_reliability=0.5
        )
        
        self.assertIsNone(record.engagement_weight)
        self.assertIsNone(record.author_weight)
        self.assertIsNone(record.velocity)
        self.assertIsNone(record.manipulation_flag)
        self.assertIsNone(record.fingerprint)
        self.assertFalse(record.dropped)


class TestSentimentEventRecord(unittest.TestCase):
    """Tests for SentimentEventRecord dataclass."""
    
    def test_required_fields(self):
        event_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        record = SentimentEventRecord(
            id=event_id,
            event_time=now
        )
        
        self.assertEqual(record.id, event_id)
        self.assertEqual(record.event_time, now)
    
    def test_optional_fields(self):
        event_id = uuid.uuid4()
        raw_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        record = SentimentEventRecord(
            id=event_id,
            event_time=now,
            raw_event_id=raw_id,
            bullish_count=3,
            bearish_count=1,
            sentiment_score=2.0,
            sentiment_label=1,
            confidence=0.85
        )
        
        self.assertEqual(record.raw_event_id, raw_id)
        self.assertEqual(record.bullish_count, 3)
        self.assertEqual(record.sentiment_label, 1)
        self.assertAlmostEqual(record.confidence, 0.85, places=5)


class TestRiskIndicatorRecord(unittest.TestCase):
    """Tests for RiskIndicatorRecord dataclass."""
    
    def test_fields(self):
        event_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        record = RiskIndicatorRecord(
            id=event_id,
            event_time=now,
            sentiment_label=1,
            sentiment_confidence=0.85,
            social_overheat=True,
            panic_risk=False,
            fomo_risk=True,
            fear_greed_index=75,
            fear_greed_zone="extreme_greed"
        )
        
        self.assertEqual(record.sentiment_label, 1)
        self.assertTrue(record.social_overheat)
        self.assertFalse(record.panic_risk)
        self.assertEqual(record.fear_greed_index, 75)
        self.assertEqual(record.fear_greed_zone, "extreme_greed")


class TestDataQualityRecord(unittest.TestCase):
    """Tests for DataQualityRecord dataclass."""
    
    def test_fields(self):
        event_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        record = DataQualityRecord(
            id=event_id,
            event_time=now,
            overall="healthy",
            availability="ok",
            time_integrity="ok",
            volume="normal",
            source_balance="normal",
            anomaly_frequency="normal"
        )
        
        self.assertEqual(record.overall, "healthy")
        self.assertEqual(record.availability, "ok")
        self.assertEqual(record.volume, "normal")


class TestInMemoryEventStoreRawEvents(unittest.TestCase):
    """Tests for InMemoryEventStore raw events."""
    
    def setUp(self):
        self.store = create_in_memory_store()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.store.clear()
    
    def test_insert_raw_event(self):
        event_id = self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Test tweet"
        )
        
        self.assertIsNotNone(event_id)
        self.assertIsInstance(event_id, uuid.UUID)
        self.assertEqual(len(self.store.raw_events), 1)
    
    def test_source_reliability_twitter(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Test"
        )
        
        record = self.store.raw_events[0]
        self.assertAlmostEqual(record.source_reliability, 0.5, places=5)
    
    def test_source_reliability_reddit(self):
        self.store.insert_raw_event(
            source="reddit",
            asset="BTC",
            event_time=self.now,
            text="Test"
        )
        
        record = self.store.raw_events[0]
        self.assertAlmostEqual(record.source_reliability, 0.7, places=5)
    
    def test_source_reliability_telegram(self):
        self.store.insert_raw_event(
            source="telegram",
            asset="BTC",
            event_time=self.now,
            text="Test"
        )
        
        record = self.store.raw_events[0]
        self.assertAlmostEqual(record.source_reliability, 0.3, places=5)
    
    def test_duplicate_fingerprint_blocked(self):
        fp = "test_fingerprint_123"
        
        id1 = self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="First",
            fingerprint=fp
        )
        
        id2 = self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Duplicate",
            fingerprint=fp
        )
        
        self.assertIsNotNone(id1)
        self.assertIsNone(id2)
        self.assertEqual(len(self.store.raw_events), 1)
    
    def test_different_fingerprints_allowed(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="First",
            fingerprint="fp1"
        )
        
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Second",
            fingerprint="fp2"
        )
        
        self.assertEqual(len(self.store.raw_events), 2)
    
    def test_null_fingerprint_allowed(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="First",
            fingerprint=None
        )
        
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Second",
            fingerprint=None
        )
        
        self.assertEqual(len(self.store.raw_events), 2)
    
    def test_insert_with_all_fields(self):
        event_id = self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="$BTC bullish!",
            engagement_weight=5.2,
            author_weight=8.1,
            velocity=2.5,
            manipulation_flag=False,
            fingerprint="twitter_123",
            dropped=False
        )
        
        record = self.store.raw_events[0]
        self.assertAlmostEqual(record.engagement_weight, 5.2, places=5)
        self.assertAlmostEqual(record.author_weight, 8.1, places=5)
        self.assertAlmostEqual(record.velocity, 2.5, places=5)
        self.assertFalse(record.manipulation_flag)


class TestInMemoryEventStoreSentimentEvents(unittest.TestCase):
    """Tests for InMemoryEventStore sentiment events."""
    
    def setUp(self):
        self.store = create_in_memory_store()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.store.clear()
    
    def test_insert_sentiment_event(self):
        event_id = self.store.insert_sentiment_event(
            event_time=self.now,
            sentiment_label=1,
            confidence=0.85
        )
        
        self.assertIsNotNone(event_id)
        self.assertEqual(len(self.store.sentiment_events), 1)
    
    def test_insert_with_raw_reference(self):
        raw_id = self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Test"
        )
        
        sentiment_id = self.store.insert_sentiment_event(
            event_time=self.now,
            raw_event_id=raw_id,
            sentiment_label=1,
            confidence=0.85
        )
        
        record = self.store.sentiment_events[0]
        self.assertEqual(record.raw_event_id, raw_id)
    
    def test_insert_with_counts(self):
        self.store.insert_sentiment_event(
            event_time=self.now,
            bullish_count=3,
            bearish_count=1,
            fear_count=0,
            greed_count=1,
            sentiment_score=2.0,
            sentiment_label=1,
            confidence=0.85
        )
        
        record = self.store.sentiment_events[0]
        self.assertEqual(record.bullish_count, 3)
        self.assertEqual(record.bearish_count, 1)
        self.assertAlmostEqual(record.sentiment_score, 2.0, places=5)


class TestInMemoryEventStoreRiskEvents(unittest.TestCase):
    """Tests for InMemoryEventStore risk events."""
    
    def setUp(self):
        self.store = create_in_memory_store()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.store.clear()
    
    def test_insert_risk_event(self):
        event_id = self.store.insert_risk_event(
            event_time=self.now,
            sentiment_label=1,
            sentiment_confidence=0.85,
            social_overheat=False,
            panic_risk=False,
            fomo_risk=True,
            fear_greed_index=65,
            fear_greed_zone="normal"
        )
        
        self.assertIsNotNone(event_id)
        self.assertEqual(len(self.store.risk_events), 1)
    
    def test_risk_fields_stored(self):
        self.store.insert_risk_event(
            event_time=self.now,
            social_overheat=True,
            panic_risk=True,
            fomo_risk=False,
            fear_greed_index=25,
            fear_greed_zone="extreme_fear"
        )
        
        record = self.store.risk_events[0]
        self.assertTrue(record.social_overheat)
        self.assertTrue(record.panic_risk)
        self.assertFalse(record.fomo_risk)
        self.assertEqual(record.fear_greed_index, 25)
        self.assertEqual(record.fear_greed_zone, "extreme_fear")


class TestInMemoryEventStoreQualityEvents(unittest.TestCase):
    """Tests for InMemoryEventStore quality events."""
    
    def setUp(self):
        self.store = create_in_memory_store()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.store.clear()
    
    def test_insert_quality_event(self):
        event_id = self.store.insert_quality_event(
            event_time=self.now,
            overall="healthy",
            availability="ok",
            time_integrity="ok",
            volume="normal",
            source_balance="normal",
            anomaly_frequency="normal"
        )
        
        self.assertIsNotNone(event_id)
        self.assertEqual(len(self.store.quality_events), 1)
    
    def test_quality_fields_stored(self):
        self.store.insert_quality_event(
            event_time=self.now,
            overall="degraded",
            availability="degraded",
            time_integrity="unstable",
            volume="abnormally_low",
            source_balance="imbalanced",
            anomaly_frequency="persistent"
        )
        
        record = self.store.quality_events[0]
        self.assertEqual(record.overall, "degraded")
        self.assertEqual(record.availability, "degraded")
        self.assertEqual(record.time_integrity, "unstable")
        self.assertEqual(record.volume, "abnormally_low")


class TestInMemoryEventStoreQueries(unittest.TestCase):
    """Tests for InMemoryEventStore query methods."""
    
    def setUp(self):
        self.store = create_in_memory_store()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.store.clear()
    
    def test_query_raw_events_empty(self):
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now
        )
        self.assertEqual(len(results), 0)
    
    def test_query_raw_events_by_asset(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="BTC tweet"
        )
        self.store.insert_raw_event(
            source="twitter",
            asset="ETH",
            event_time=self.now,
            text="ETH tweet"
        )
        
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].asset, "BTC")
    
    def test_query_raw_events_by_source(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Twitter"
        )
        self.store.insert_raw_event(
            source="reddit",
            asset="BTC",
            event_time=self.now,
            text="Reddit"
        )
        
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1),
            source="twitter"
        )
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source, "twitter")
    
    def test_query_raw_events_by_time_window(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now - timedelta(hours=2),
            text="Old"
        )
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Current"
        )
        
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(minutes=30),
            end_time=self.now + timedelta(minutes=30)
        )
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].text, "Current")
    
    def test_query_raw_events_excludes_dropped(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Valid",
            dropped=False
        )
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Dropped",
            dropped=True
        )
        
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1),
            include_dropped=False
        )
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].text, "Valid")
    
    def test_query_raw_events_includes_dropped(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Valid",
            dropped=False
        )
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="Dropped",
            dropped=True
        )
        
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1),
            include_dropped=True
        )
        
        self.assertEqual(len(results), 2)
    
    def test_query_raw_events_ordered_by_time(self):
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now + timedelta(minutes=1),
            text="Second"
        )
        self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="First"
        )
        
        results = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        self.assertEqual(results[0].text, "First")
        self.assertEqual(results[1].text, "Second")
    
    def test_query_sentiment_events(self):
        self.store.insert_sentiment_event(
            event_time=self.now,
            sentiment_label=1
        )
        
        results = self.store.query_sentiment_events(
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        self.assertEqual(len(results), 1)
    
    def test_query_risk_events(self):
        self.store.insert_risk_event(
            event_time=self.now,
            social_overheat=True
        )
        
        results = self.store.query_risk_events(
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        self.assertEqual(len(results), 1)
    
    def test_query_quality_events(self):
        self.store.insert_quality_event(
            event_time=self.now,
            overall="healthy"
        )
        
        results = self.store.query_quality_events(
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        self.assertEqual(len(results), 1)


class TestFactoryFunctions(unittest.TestCase):
    """Tests for factory functions."""
    
    def test_create_in_memory_store(self):
        store = create_in_memory_store()
        self.assertIsInstance(store, InMemoryEventStore)
    
    def test_create_event_store(self):
        db = DatabaseConnection()
        store = create_event_store(db)
        self.assertIsInstance(store, EventStore)
    
    def test_get_create_tables_sql(self):
        sql = get_create_tables_sql()
        self.assertIn("social_raw_events", sql)
        self.assertIn("social_sentiment_events", sql)
        self.assertIn("risk_indicator_events", sql)
        self.assertIn("data_quality_events", sql)


class TestCreateTablesSql(unittest.TestCase):
    """Tests for CREATE_TABLES_SQL schema."""
    
    def test_raw_events_table_exists(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS social_raw_events", CREATE_TABLES_SQL)
    
    def test_sentiment_events_table_exists(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS social_sentiment_events", CREATE_TABLES_SQL)
    
    def test_risk_events_table_exists(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS risk_indicator_events", CREATE_TABLES_SQL)
    
    def test_quality_events_table_exists(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS data_quality_events", CREATE_TABLES_SQL)
    
    def test_uuid_primary_keys(self):
        self.assertIn("id UUID PRIMARY KEY", CREATE_TABLES_SQL)
    
    def test_timestamptz_columns(self):
        self.assertIn("event_time TIMESTAMPTZ NOT NULL", CREATE_TABLES_SQL)
    
    def test_raw_events_indexes(self):
        self.assertIn("idx_raw_asset_time", CREATE_TABLES_SQL)
        self.assertIn("idx_raw_source_time", CREATE_TABLES_SQL)
    
    def test_fingerprint_unique(self):
        self.assertIn("fingerprint TEXT UNIQUE", CREATE_TABLES_SQL)
    
    def test_foreign_key_reference(self):
        self.assertIn("REFERENCES social_raw_events(id)", CREATE_TABLES_SQL)


class TestPipelineFlow(unittest.TestCase):
    """Tests for complete pipeline flow through Event Store."""
    
    def setUp(self):
        self.store = create_in_memory_store()
        self.now = datetime.now(timezone.utc)
    
    def tearDown(self):
        self.store.clear()
    
    def test_complete_pipeline_flow(self):
        """Test storing data in correct order through pipeline."""
        
        # Step 1: After Time Sync Guard
        raw_id = self.store.insert_raw_event(
            source="twitter",
            asset="BTC",
            event_time=self.now,
            text="$BTC bullish momentum!",
            engagement_weight=5.2,
            author_weight=8.1,
            velocity=2.5,
            manipulation_flag=False,
            fingerprint="twitter_12345"
        )
        self.assertIsNotNone(raw_id)
        
        # Step 2: After Sentiment Pipeline
        sentiment_id = self.store.insert_sentiment_event(
            event_time=self.now,
            raw_event_id=raw_id,
            bullish_count=2,
            bearish_count=0,
            sentiment_score=2.0,
            sentiment_label=1,
            confidence=0.85
        )
        self.assertIsNotNone(sentiment_id)
        
        # Step 3: After Risk Indicators
        risk_id = self.store.insert_risk_event(
            event_time=self.now,
            sentiment_label=1,
            sentiment_confidence=0.85,
            social_overheat=False,
            panic_risk=False,
            fomo_risk=False,
            fear_greed_index=65,
            fear_greed_zone="normal"
        )
        self.assertIsNotNone(risk_id)
        
        # Step 4: After DQM
        quality_id = self.store.insert_quality_event(
            event_time=self.now,
            overall="healthy",
            availability="ok",
            time_integrity="ok",
            volume="normal",
            source_balance="normal",
            anomaly_frequency="normal"
        )
        self.assertIsNotNone(quality_id)
        
        # Verify all events stored
        self.assertEqual(len(self.store.raw_events), 1)
        self.assertEqual(len(self.store.sentiment_events), 1)
        self.assertEqual(len(self.store.risk_events), 1)
        self.assertEqual(len(self.store.quality_events), 1)
    
    def test_backtest_replay(self):
        """Test querying for backtest replay."""
        
        # Insert multiple events over time
        for i in range(5):
            event_time = self.now - timedelta(minutes=i)
            
            raw_id = self.store.insert_raw_event(
                source="twitter",
                asset="BTC",
                event_time=event_time,
                text=f"Tweet {i}",
                fingerprint=f"fp_{i}"
            )
            
            self.store.insert_sentiment_event(
                event_time=event_time,
                raw_event_id=raw_id,
                sentiment_label=1 if i % 2 == 0 else -1
            )
        
        # Query for backtest
        raw_events = self.store.query_raw_events(
            asset="BTC",
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        sentiment_events = self.store.query_sentiment_events(
            start_time=self.now - timedelta(hours=1),
            end_time=self.now + timedelta(hours=1)
        )
        
        self.assertEqual(len(raw_events), 5)
        self.assertEqual(len(sentiment_events), 5)
        
        # Verify ordering (ascending by event_time)
        for i in range(len(raw_events) - 1):
            self.assertLessEqual(
                raw_events[i].event_time,
                raw_events[i + 1].event_time
            )


class TestEventStoreSourceReliability(unittest.TestCase):
    """Tests for source reliability constants in EventStore."""
    
    def test_twitter_reliability(self):
        self.assertAlmostEqual(
            InMemoryEventStore.SOURCE_RELIABILITY["twitter"],
            0.5,
            places=5
        )
    
    def test_reddit_reliability(self):
        self.assertAlmostEqual(
            InMemoryEventStore.SOURCE_RELIABILITY["reddit"],
            0.7,
            places=5
        )
    
    def test_telegram_reliability(self):
        self.assertAlmostEqual(
            InMemoryEventStore.SOURCE_RELIABILITY["telegram"],
            0.3,
            places=5
        )


if __name__ == "__main__":
    unittest.main()
