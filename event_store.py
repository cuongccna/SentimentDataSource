"""
PostgreSQL Event Store for Crypto BotTrading Social Data Pipeline
Purpose: Persist ALL social data events and derived results for
backtest and replay.

THIS EVENT STORE IS THE SINGLE SOURCE OF TRUTH FOR BACKTESTING.

NO MOCKING. NO HALLUCINATION. NO DATA MODIFICATION.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass

# Note: psycopg2 or asyncpg would be used in production
# This implementation uses standard interfaces


# =============================================================================
# SQL SCHEMA DEFINITIONS (STRICT - DO NOT CHANGE)
# =============================================================================

CREATE_TABLES_SQL = """
-- TABLE 1: social_raw_events
CREATE TABLE IF NOT EXISTS social_raw_events (
    id UUID PRIMARY KEY,
    source TEXT NOT NULL,                 -- twitter | reddit | telegram
    asset TEXT NOT NULL,                  -- BTC
    event_time TIMESTAMPTZ NOT NULL,      -- EVENT TIME (UTC)
    ingest_time TIMESTAMPTZ NOT NULL,     -- DB insert time
    text TEXT NOT NULL,

    engagement_weight DOUBLE PRECISION,
    author_weight DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    manipulation_flag BOOLEAN,

    source_reliability DOUBLE PRECISION NOT NULL,

    fingerprint TEXT UNIQUE,
    dropped BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_raw_asset_time
ON social_raw_events (asset, event_time);

CREATE INDEX IF NOT EXISTS idx_raw_source_time
ON social_raw_events (source, event_time);

-- TABLE 2: social_sentiment_events
CREATE TABLE IF NOT EXISTS social_sentiment_events (
    id UUID PRIMARY KEY,
    raw_event_id UUID REFERENCES social_raw_events(id),
    event_time TIMESTAMPTZ NOT NULL,

    bullish_count INT,
    bearish_count INT,
    fear_count INT,
    greed_count INT,

    sentiment_score DOUBLE PRECISION,
    sentiment_label SMALLINT,          -- -1, 0, +1
    confidence DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_sentiment_time
ON social_sentiment_events (event_time);

-- TABLE 3: risk_indicator_events
CREATE TABLE IF NOT EXISTS risk_indicator_events (
    id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,

    sentiment_label SMALLINT,
    sentiment_confidence DOUBLE PRECISION,

    social_overheat BOOLEAN,
    panic_risk BOOLEAN,
    fomo_risk BOOLEAN,

    fear_greed_index INT,
    fear_greed_zone TEXT
);

CREATE INDEX IF NOT EXISTS idx_risk_time
ON risk_indicator_events (event_time);

-- TABLE 4: data_quality_events
CREATE TABLE IF NOT EXISTS data_quality_events (
    id UUID PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,

    overall TEXT,
    availability TEXT,
    time_integrity TEXT,
    volume TEXT,
    source_balance TEXT,
    anomaly_frequency TEXT
);

CREATE INDEX IF NOT EXISTS idx_quality_time
ON data_quality_events (event_time);
"""


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RawEventRecord:
    """Record for social_raw_events table."""
    id: uuid.UUID
    source: str
    asset: str
    event_time: datetime
    ingest_time: datetime
    text: str
    source_reliability: float
    engagement_weight: Optional[float] = None
    author_weight: Optional[float] = None
    velocity: Optional[float] = None
    manipulation_flag: Optional[bool] = None
    fingerprint: Optional[str] = None
    dropped: bool = False


@dataclass
class SentimentEventRecord:
    """Record for social_sentiment_events table."""
    id: uuid.UUID
    event_time: datetime
    raw_event_id: Optional[uuid.UUID] = None
    bullish_count: Optional[int] = None
    bearish_count: Optional[int] = None
    fear_count: Optional[int] = None
    greed_count: Optional[int] = None
    sentiment_score: Optional[float] = None
    sentiment_label: Optional[int] = None
    confidence: Optional[float] = None


@dataclass
class RiskIndicatorRecord:
    """Record for risk_indicator_events table."""
    id: uuid.UUID
    event_time: datetime
    sentiment_label: Optional[int] = None
    sentiment_confidence: Optional[float] = None
    social_overheat: Optional[bool] = None
    panic_risk: Optional[bool] = None
    fomo_risk: Optional[bool] = None
    fear_greed_index: Optional[int] = None
    fear_greed_zone: Optional[str] = None


@dataclass
class DataQualityRecord:
    """Record for data_quality_events table."""
    id: uuid.UUID
    event_time: datetime
    overall: Optional[str] = None
    availability: Optional[str] = None
    time_integrity: Optional[str] = None
    volume: Optional[str] = None
    source_balance: Optional[str] = None
    anomaly_frequency: Optional[str] = None


# =============================================================================
# DATABASE CONNECTION INTERFACE
# =============================================================================

class DatabaseConnection:
    """
    Abstract database connection interface.
    
    In production, this would wrap psycopg2 or asyncpg connection.
    """
    
    def __init__(self, connection=None):
        self.connection = connection
        self._cursor = None
    
    def execute(self, sql: str, params: tuple = None):
        """Execute a SQL statement."""
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(sql, params)
            return cursor
        return None
    
    def commit(self):
        """Commit the transaction."""
        if self.connection:
            self.connection.commit()
    
    def rollback(self):
        """Rollback the transaction."""
        if self.connection:
            self.connection.rollback()


# =============================================================================
# EVENT STORE IMPLEMENTATION
# =============================================================================

class EventStore:
    """
    PostgreSQL Event Store for Social Data Pipeline.
    
    Persists ALL social data events and derived results
    for backtest and replay.
    
    Write order (CRITICAL):
    1. AFTER Time Sync Guard passes → INSERT into social_raw_events
    2. AFTER Sentiment Pipeline processes → INSERT into social_sentiment_events
    3. AFTER Risk Indicators computed → INSERT into risk_indicator_events
    4. AFTER DQM updates → INSERT into data_quality_events
    
    NO BATCH AGGREGATION BEFORE STORAGE.
    STORE EVENT-LEVEL DATA ONLY.
    """
    
    # Source reliability constants
    SOURCE_RELIABILITY = {
        "twitter": 0.5,
        "reddit": 0.7,
        "telegram": 0.3
    }
    
    def __init__(self, db: DatabaseConnection):
        """
        Initialize Event Store with database connection.
        
        Args:
            db: DatabaseConnection instance
        """
        self.db = db
        self._fingerprints: set[str] = set()  # In-memory duplicate tracking
    
    def initialize_schema(self):
        """Create tables if they don't exist."""
        self.db.execute(CREATE_TABLES_SQL)
        self.db.commit()
    
    def _generate_uuid(self) -> uuid.UUID:
        """Generate a new UUID for primary key."""
        return uuid.uuid4()
    
    def _get_ingest_time(self) -> datetime:
        """Get current UTC time for ingest_time."""
        return datetime.now(timezone.utc)
    
    def _is_duplicate_fingerprint(self, fingerprint: Optional[str]) -> bool:
        """Check if fingerprint already exists."""
        if fingerprint is None:
            return False
        return fingerprint in self._fingerprints
    
    def _record_fingerprint(self, fingerprint: Optional[str]):
        """Record fingerprint for duplicate detection."""
        if fingerprint:
            self._fingerprints.add(fingerprint)
    
    # =========================================================================
    # INSERT FUNCTIONS
    # =========================================================================
    
    def insert_raw_event(
        self,
        source: str,
        asset: str,
        event_time: datetime,
        text: str,
        engagement_weight: Optional[float] = None,
        author_weight: Optional[float] = None,
        velocity: Optional[float] = None,
        manipulation_flag: Optional[bool] = None,
        fingerprint: Optional[str] = None,
        dropped: bool = False
    ) -> Optional[uuid.UUID]:
        """
        Insert a raw social event into social_raw_events.
        
        Called AFTER Time Sync Guard passes an event.
        
        Args:
            source: twitter | reddit | telegram
            asset: Asset symbol (e.g., BTC)
            event_time: Original event timestamp (UTC)
            text: Raw text content
            engagement_weight: Engagement metric weight
            author_weight: Author reputation weight
            velocity: Event velocity
            manipulation_flag: Manipulation detection flag
            fingerprint: Unique fingerprint for deduplication
            dropped: Whether event was dropped by Time Sync Guard
        
        Returns:
            UUID of inserted record, or None if duplicate
        """
        # Check for duplicate fingerprint
        if self._is_duplicate_fingerprint(fingerprint):
            return None
        
        event_id = self._generate_uuid()
        ingest_time = self._get_ingest_time()
        source_reliability = self.SOURCE_RELIABILITY.get(source, 0.5)
        
        sql = """
        INSERT INTO social_raw_events (
            id, source, asset, event_time, ingest_time, text,
            engagement_weight, author_weight, velocity, manipulation_flag,
            source_reliability, fingerprint, dropped
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (fingerprint) DO NOTHING
        """
        
        params = (
            str(event_id),
            source,
            asset,
            event_time,
            ingest_time,
            text,
            engagement_weight,
            author_weight,
            velocity,
            manipulation_flag,
            source_reliability,
            fingerprint,
            dropped
        )
        
        self.db.execute(sql, params)
        self.db.commit()
        
        # Record fingerprint
        self._record_fingerprint(fingerprint)
        
        return event_id
    
    def insert_sentiment_event(
        self,
        event_time: datetime,
        raw_event_id: Optional[uuid.UUID] = None,
        bullish_count: Optional[int] = None,
        bearish_count: Optional[int] = None,
        fear_count: Optional[int] = None,
        greed_count: Optional[int] = None,
        sentiment_score: Optional[float] = None,
        sentiment_label: Optional[int] = None,
        confidence: Optional[float] = None
    ) -> uuid.UUID:
        """
        Insert a sentiment analysis result into social_sentiment_events.
        
        Called AFTER Sentiment Pipeline processes the event.
        
        Args:
            event_time: Original event timestamp (UTC)
            raw_event_id: Reference to social_raw_events.id
            bullish_count: Count of bullish keywords
            bearish_count: Count of bearish keywords
            fear_count: Count of fear keywords
            greed_count: Count of greed keywords
            sentiment_score: Computed sentiment score
            sentiment_label: -1 (bearish), 0 (neutral), +1 (bullish)
            confidence: Confidence level (0-1)
        
        Returns:
            UUID of inserted record
        """
        event_id = self._generate_uuid()
        
        sql = """
        INSERT INTO social_sentiment_events (
            id, raw_event_id, event_time,
            bullish_count, bearish_count, fear_count, greed_count,
            sentiment_score, sentiment_label, confidence
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params = (
            str(event_id),
            str(raw_event_id) if raw_event_id else None,
            event_time,
            bullish_count,
            bearish_count,
            fear_count,
            greed_count,
            sentiment_score,
            sentiment_label,
            confidence
        )
        
        self.db.execute(sql, params)
        self.db.commit()
        
        return event_id
    
    def insert_risk_event(
        self,
        event_time: datetime,
        sentiment_label: Optional[int] = None,
        sentiment_confidence: Optional[float] = None,
        social_overheat: Optional[bool] = None,
        panic_risk: Optional[bool] = None,
        fomo_risk: Optional[bool] = None,
        fear_greed_index: Optional[int] = None,
        fear_greed_zone: Optional[str] = None
    ) -> uuid.UUID:
        """
        Insert a risk indicator result into risk_indicator_events.
        
        Called AFTER Risk Indicators are computed.
        
        Args:
            event_time: Original event timestamp (UTC)
            sentiment_label: -1, 0, +1
            sentiment_confidence: Confidence level
            social_overheat: Social activity overheat flag
            panic_risk: Panic risk flag
            fomo_risk: FOMO risk flag
            fear_greed_index: Fear/Greed index value
            fear_greed_zone: Zone classification
        
        Returns:
            UUID of inserted record
        """
        event_id = self._generate_uuid()
        
        sql = """
        INSERT INTO risk_indicator_events (
            id, event_time,
            sentiment_label, sentiment_confidence,
            social_overheat, panic_risk, fomo_risk,
            fear_greed_index, fear_greed_zone
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params = (
            str(event_id),
            event_time,
            sentiment_label,
            sentiment_confidence,
            social_overheat,
            panic_risk,
            fomo_risk,
            fear_greed_index,
            fear_greed_zone
        )
        
        self.db.execute(sql, params)
        self.db.commit()
        
        return event_id
    
    def insert_quality_event(
        self,
        event_time: datetime,
        overall: Optional[str] = None,
        availability: Optional[str] = None,
        time_integrity: Optional[str] = None,
        volume: Optional[str] = None,
        source_balance: Optional[str] = None,
        anomaly_frequency: Optional[str] = None
    ) -> uuid.UUID:
        """
        Insert a data quality snapshot into data_quality_events.
        
        Called AFTER Data Quality Monitor updates.
        
        Args:
            event_time: Timestamp of the quality check (UTC)
            overall: Overall quality status
            availability: Availability status
            time_integrity: Time integrity status
            volume: Volume status
            source_balance: Source balance status
            anomaly_frequency: Anomaly frequency status
        
        Returns:
            UUID of inserted record
        """
        event_id = self._generate_uuid()
        
        sql = """
        INSERT INTO data_quality_events (
            id, event_time,
            overall, availability, time_integrity,
            volume, source_balance, anomaly_frequency
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        params = (
            str(event_id),
            event_time,
            overall,
            availability,
            time_integrity,
            volume,
            source_balance,
            anomaly_frequency
        )
        
        self.db.execute(sql, params)
        self.db.commit()
        
        return event_id
    
    # =========================================================================
    # QUERY FUNCTIONS (FOR BACKTEST/REPLAY)
    # =========================================================================
    
    def query_raw_events(
        self,
        asset: str,
        start_time: datetime,
        end_time: datetime,
        source: Optional[str] = None,
        include_dropped: bool = False
    ) -> list[RawEventRecord]:
        """
        Query raw events for backtest/replay.
        
        Args:
            asset: Asset symbol
            start_time: Start of time window (inclusive)
            end_time: End of time window (inclusive)
            source: Optional source filter
            include_dropped: Whether to include dropped events
        
        Returns:
            List of RawEventRecord ordered by event_time
        """
        sql = """
        SELECT id, source, asset, event_time, ingest_time, text,
               engagement_weight, author_weight, velocity, manipulation_flag,
               source_reliability, fingerprint, dropped
        FROM social_raw_events
        WHERE asset = %s
          AND event_time >= %s
          AND event_time <= %s
        """
        
        params = [asset, start_time, end_time]
        
        if source:
            sql += " AND source = %s"
            params.append(source)
        
        if not include_dropped:
            sql += " AND (dropped = FALSE OR dropped IS NULL)"
        
        sql += " ORDER BY event_time ASC"
        
        cursor = self.db.execute(sql, tuple(params))
        
        if cursor is None:
            return []
        
        results = []
        for row in cursor.fetchall():
            results.append(RawEventRecord(
                id=uuid.UUID(row[0]),
                source=row[1],
                asset=row[2],
                event_time=row[3],
                ingest_time=row[4],
                text=row[5],
                engagement_weight=row[6],
                author_weight=row[7],
                velocity=row[8],
                manipulation_flag=row[9],
                source_reliability=row[10],
                fingerprint=row[11],
                dropped=row[12]
            ))
        
        return results
    
    def query_sentiment_events(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[SentimentEventRecord]:
        """
        Query sentiment events for backtest/replay.
        
        Args:
            start_time: Start of time window (inclusive)
            end_time: End of time window (inclusive)
        
        Returns:
            List of SentimentEventRecord ordered by event_time
        """
        sql = """
        SELECT id, raw_event_id, event_time,
               bullish_count, bearish_count, fear_count, greed_count,
               sentiment_score, sentiment_label, confidence
        FROM social_sentiment_events
        WHERE event_time >= %s
          AND event_time <= %s
        ORDER BY event_time ASC
        """
        
        cursor = self.db.execute(sql, (start_time, end_time))
        
        if cursor is None:
            return []
        
        results = []
        for row in cursor.fetchall():
            results.append(SentimentEventRecord(
                id=uuid.UUID(row[0]),
                raw_event_id=uuid.UUID(row[1]) if row[1] else None,
                event_time=row[2],
                bullish_count=row[3],
                bearish_count=row[4],
                fear_count=row[5],
                greed_count=row[6],
                sentiment_score=row[7],
                sentiment_label=row[8],
                confidence=row[9]
            ))
        
        return results
    
    def query_risk_events(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[RiskIndicatorRecord]:
        """
        Query risk indicator events for backtest/replay.
        
        Args:
            start_time: Start of time window (inclusive)
            end_time: End of time window (inclusive)
        
        Returns:
            List of RiskIndicatorRecord ordered by event_time
        """
        sql = """
        SELECT id, event_time,
               sentiment_label, sentiment_confidence,
               social_overheat, panic_risk, fomo_risk,
               fear_greed_index, fear_greed_zone
        FROM risk_indicator_events
        WHERE event_time >= %s
          AND event_time <= %s
        ORDER BY event_time ASC
        """
        
        cursor = self.db.execute(sql, (start_time, end_time))
        
        if cursor is None:
            return []
        
        results = []
        for row in cursor.fetchall():
            results.append(RiskIndicatorRecord(
                id=uuid.UUID(row[0]),
                event_time=row[1],
                sentiment_label=row[2],
                sentiment_confidence=row[3],
                social_overheat=row[4],
                panic_risk=row[5],
                fomo_risk=row[6],
                fear_greed_index=row[7],
                fear_greed_zone=row[8]
            ))
        
        return results
    
    def query_quality_events(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[DataQualityRecord]:
        """
        Query data quality events for backtest/replay.
        
        Args:
            start_time: Start of time window (inclusive)
            end_time: End of time window (inclusive)
        
        Returns:
            List of DataQualityRecord ordered by event_time
        """
        sql = """
        SELECT id, event_time,
               overall, availability, time_integrity,
               volume, source_balance, anomaly_frequency
        FROM data_quality_events
        WHERE event_time >= %s
          AND event_time <= %s
        ORDER BY event_time ASC
        """
        
        cursor = self.db.execute(sql, (start_time, end_time))
        
        if cursor is None:
            return []
        
        results = []
        for row in cursor.fetchall():
            results.append(DataQualityRecord(
                id=uuid.UUID(row[0]),
                event_time=row[1],
                overall=row[2],
                availability=row[3],
                time_integrity=row[4],
                volume=row[5],
                source_balance=row[6],
                anomaly_frequency=row[7]
            ))
        
        return results
    
    def query_raw_with_sentiment(
        self,
        asset: str,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        """
        Query raw events joined with sentiment for backtest.
        
        Args:
            asset: Asset symbol
            start_time: Start of time window (inclusive)
            end_time: End of time window (inclusive)
        
        Returns:
            List of joined records ordered by event_time
        """
        sql = """
        SELECT 
            r.id, r.source, r.asset, r.event_time, r.text,
            r.engagement_weight, r.author_weight, r.velocity,
            r.manipulation_flag, r.source_reliability,
            s.sentiment_score, s.sentiment_label, s.confidence
        FROM social_raw_events r
        LEFT JOIN social_sentiment_events s ON r.id = s.raw_event_id
        WHERE r.asset = %s
          AND r.event_time >= %s
          AND r.event_time <= %s
          AND (r.dropped = FALSE OR r.dropped IS NULL)
        ORDER BY r.event_time ASC
        """
        
        cursor = self.db.execute(sql, (asset, start_time, end_time))
        
        if cursor is None:
            return []
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "id": str(row[0]),
                "source": row[1],
                "asset": row[2],
                "event_time": row[3],
                "text": row[4],
                "engagement_weight": row[5],
                "author_weight": row[6],
                "velocity": row[7],
                "manipulation_flag": row[8],
                "source_reliability": row[9],
                "sentiment_score": row[10],
                "sentiment_label": row[11],
                "confidence": row[12]
            })
        
        return results


# =============================================================================
# IN-MEMORY EVENT STORE (FOR TESTING)
# =============================================================================

class InMemoryEventStore:
    """
    In-memory implementation of Event Store for testing.
    
    Implements the same interface as EventStore but stores
    data in memory instead of PostgreSQL.
    """
    
    SOURCE_RELIABILITY = {
        "twitter": 0.5,
        "reddit": 0.7,
        "telegram": 0.3
    }
    
    def __init__(self):
        self.raw_events: list[RawEventRecord] = []
        self.sentiment_events: list[SentimentEventRecord] = []
        self.risk_events: list[RiskIndicatorRecord] = []
        self.quality_events: list[DataQualityRecord] = []
        self._fingerprints: set[str] = set()
    
    def _generate_uuid(self) -> uuid.UUID:
        return uuid.uuid4()
    
    def _get_ingest_time(self) -> datetime:
        return datetime.now(timezone.utc)
    
    def insert_raw_event(
        self,
        source: str,
        asset: str,
        event_time: datetime,
        text: str,
        engagement_weight: Optional[float] = None,
        author_weight: Optional[float] = None,
        velocity: Optional[float] = None,
        manipulation_flag: Optional[bool] = None,
        fingerprint: Optional[str] = None,
        dropped: bool = False
    ) -> Optional[uuid.UUID]:
        """Insert raw event into memory."""
        if fingerprint and fingerprint in self._fingerprints:
            return None
        
        event_id = self._generate_uuid()
        ingest_time = self._get_ingest_time()
        source_reliability = self.SOURCE_RELIABILITY.get(source, 0.5)
        
        record = RawEventRecord(
            id=event_id,
            source=source,
            asset=asset,
            event_time=event_time,
            ingest_time=ingest_time,
            text=text,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            manipulation_flag=manipulation_flag,
            source_reliability=source_reliability,
            fingerprint=fingerprint,
            dropped=dropped
        )
        
        self.raw_events.append(record)
        
        if fingerprint:
            self._fingerprints.add(fingerprint)
        
        return event_id
    
    def insert_sentiment_event(
        self,
        event_time: datetime,
        raw_event_id: Optional[uuid.UUID] = None,
        bullish_count: Optional[int] = None,
        bearish_count: Optional[int] = None,
        fear_count: Optional[int] = None,
        greed_count: Optional[int] = None,
        sentiment_score: Optional[float] = None,
        sentiment_label: Optional[int] = None,
        confidence: Optional[float] = None
    ) -> uuid.UUID:
        """Insert sentiment event into memory."""
        event_id = self._generate_uuid()
        
        record = SentimentEventRecord(
            id=event_id,
            raw_event_id=raw_event_id,
            event_time=event_time,
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            fear_count=fear_count,
            greed_count=greed_count,
            sentiment_score=sentiment_score,
            sentiment_label=sentiment_label,
            confidence=confidence
        )
        
        self.sentiment_events.append(record)
        return event_id
    
    def insert_risk_event(
        self,
        event_time: datetime,
        sentiment_label: Optional[int] = None,
        sentiment_confidence: Optional[float] = None,
        social_overheat: Optional[bool] = None,
        panic_risk: Optional[bool] = None,
        fomo_risk: Optional[bool] = None,
        fear_greed_index: Optional[int] = None,
        fear_greed_zone: Optional[str] = None
    ) -> uuid.UUID:
        """Insert risk event into memory."""
        event_id = self._generate_uuid()
        
        record = RiskIndicatorRecord(
            id=event_id,
            event_time=event_time,
            sentiment_label=sentiment_label,
            sentiment_confidence=sentiment_confidence,
            social_overheat=social_overheat,
            panic_risk=panic_risk,
            fomo_risk=fomo_risk,
            fear_greed_index=fear_greed_index,
            fear_greed_zone=fear_greed_zone
        )
        
        self.risk_events.append(record)
        return event_id
    
    def insert_quality_event(
        self,
        event_time: datetime,
        overall: Optional[str] = None,
        availability: Optional[str] = None,
        time_integrity: Optional[str] = None,
        volume: Optional[str] = None,
        source_balance: Optional[str] = None,
        anomaly_frequency: Optional[str] = None
    ) -> uuid.UUID:
        """Insert quality event into memory."""
        event_id = self._generate_uuid()
        
        record = DataQualityRecord(
            id=event_id,
            event_time=event_time,
            overall=overall,
            availability=availability,
            time_integrity=time_integrity,
            volume=volume,
            source_balance=source_balance,
            anomaly_frequency=anomaly_frequency
        )
        
        self.quality_events.append(record)
        return event_id
    
    def query_raw_events(
        self,
        asset: str,
        start_time: datetime,
        end_time: datetime,
        source: Optional[str] = None,
        include_dropped: bool = False
    ) -> list[RawEventRecord]:
        """Query raw events from memory."""
        results = []
        
        for record in self.raw_events:
            if record.asset != asset:
                continue
            if record.event_time < start_time or record.event_time > end_time:
                continue
            if source and record.source != source:
                continue
            if not include_dropped and record.dropped:
                continue
            results.append(record)
        
        return sorted(results, key=lambda r: r.event_time)
    
    def query_sentiment_events(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[SentimentEventRecord]:
        """Query sentiment events from memory."""
        results = [
            r for r in self.sentiment_events
            if start_time <= r.event_time <= end_time
        ]
        return sorted(results, key=lambda r: r.event_time)
    
    def query_risk_events(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[RiskIndicatorRecord]:
        """Query risk events from memory."""
        results = [
            r for r in self.risk_events
            if start_time <= r.event_time <= end_time
        ]
        return sorted(results, key=lambda r: r.event_time)
    
    def query_quality_events(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[DataQualityRecord]:
        """Query quality events from memory."""
        results = [
            r for r in self.quality_events
            if start_time <= r.event_time <= end_time
        ]
        return sorted(results, key=lambda r: r.event_time)
    
    def clear(self):
        """Clear all data."""
        self.raw_events.clear()
        self.sentiment_events.clear()
        self.risk_events.clear()
        self.quality_events.clear()
        self._fingerprints.clear()


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_event_store(db: DatabaseConnection) -> EventStore:
    """Create an EventStore with the given database connection."""
    return EventStore(db)


def create_in_memory_store() -> InMemoryEventStore:
    """Create an in-memory event store for testing."""
    return InMemoryEventStore()


def get_create_tables_sql() -> str:
    """Get the SQL to create all tables."""
    return CREATE_TABLES_SQL


if __name__ == "__main__":
    from datetime import timedelta
    
    print("=== PostgreSQL Event Store Demo ===\n")
    
    # Create in-memory store for demo
    store = create_in_memory_store()
    
    now = datetime.now(timezone.utc)
    
    # Simulate pipeline flow
    print("1. Inserting raw event (after Time Sync Guard)...")
    raw_id = store.insert_raw_event(
        source="twitter",
        asset="BTC",
        event_time=now - timedelta(minutes=5),
        text="$BTC breaking resistance! Bullish momentum!",
        engagement_weight=5.2,
        author_weight=8.1,
        velocity=2.5,
        manipulation_flag=False,
        fingerprint="twitter_12345_abc"
    )
    print(f"   Raw event ID: {raw_id}")
    
    print("\n2. Inserting sentiment event (after Sentiment Pipeline)...")
    sentiment_id = store.insert_sentiment_event(
        event_time=now - timedelta(minutes=5),
        raw_event_id=raw_id,
        bullish_count=2,
        bearish_count=0,
        fear_count=0,
        greed_count=0,
        sentiment_score=2.0,
        sentiment_label=1,
        confidence=0.85
    )
    print(f"   Sentiment event ID: {sentiment_id}")
    
    print("\n3. Inserting risk event (after Risk Indicators)...")
    risk_id = store.insert_risk_event(
        event_time=now - timedelta(minutes=5),
        sentiment_label=1,
        sentiment_confidence=0.85,
        social_overheat=False,
        panic_risk=False,
        fomo_risk=False,
        fear_greed_index=65,
        fear_greed_zone="normal"
    )
    print(f"   Risk event ID: {risk_id}")
    
    print("\n4. Inserting quality event (after DQM)...")
    quality_id = store.insert_quality_event(
        event_time=now,
        overall="healthy",
        availability="ok",
        time_integrity="ok",
        volume="normal",
        source_balance="normal",
        anomaly_frequency="normal"
    )
    print(f"   Quality event ID: {quality_id}")
    
    # Query for backtest
    print("\n5. Querying for backtest replay...")
    raw_events = store.query_raw_events(
        asset="BTC",
        start_time=now - timedelta(hours=1),
        end_time=now
    )
    print(f"   Found {len(raw_events)} raw events")
    
    sentiment_events = store.query_sentiment_events(
        start_time=now - timedelta(hours=1),
        end_time=now
    )
    print(f"   Found {len(sentiment_events)} sentiment events")
    
    # Test duplicate prevention
    print("\n6. Testing duplicate fingerprint prevention...")
    duplicate_id = store.insert_raw_event(
        source="twitter",
        asset="BTC",
        event_time=now - timedelta(minutes=4),
        text="Duplicate tweet",
        fingerprint="twitter_12345_abc"  # Same fingerprint
    )
    print(f"   Duplicate insert result: {duplicate_id} (None = blocked)")
    
    print("\n=== Event Store Demo Complete ===")
    print("\nSQL Schema:")
    print("-" * 50)
    print(CREATE_TABLES_SQL[:500] + "...")
