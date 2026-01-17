"""
Twitter/X Ingestion Worker for Crypto BotTrading Social Data Pipeline.

LOCKED TWITTER INGESTION CONTRACT.

This module safely ingests Twitter/X data ONLY from predefined
whitelisted sources (accounts, lists, or queries) to detect
early narrative, FOMO, and panic signals.

ABSOLUTE NON-NEGOTIABLE RULES:
- DO NOT read the global Twitter firehose
- DO NOT search all tweets containing BTC
- DO NOT scrape trending topics
- DO NOT infer which accounts or queries to read
- DO NOT use paid Twitter APIs
- DO NOT generate trading signals
- ONLY ingest from explicitly whitelisted sources
- If a source is not whitelisted → IGNORE COMPLETELY

ROLE IN BOTTRADING SYSTEM:
- Twitter provides EARLY narrative & FOMO/PANIC context
- Twitter does NOT decide sentiment or trades
"""

import hashlib
import json
import math
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# =============================================================================
# LOGGING (Simple UTC logger)
# =============================================================================

def _log(level: str, msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} [{level}] twitter_ingestion: {msg}")

class logger:
    @staticmethod
    def info(msg: str) -> None:
        _log("INFO", msg)
    
    @staticmethod
    def warning(msg: str) -> None:
        _log("WARNING", msg)
    
    @staticmethod
    def error(msg: str) -> None:
        _log("ERROR", msg)
    
    @staticmethod
    def debug(msg: str) -> None:
        _log("DEBUG", msg)


# =============================================================================
# CONSTANTS
# =============================================================================

# Source reliability for Twitter (LOCKED)
SOURCE_RELIABILITY_TWITTER = 0.5

# Global rate limit (tweets per minute across ALL sources)
GLOBAL_RATE_LIMIT = 500

# Default per-source rate limit
DEFAULT_SOURCE_RATE_LIMIT = 60

# Rate limit window in seconds
RATE_LIMIT_WINDOW_SECONDS = 60

# Asset keywords for BTC filtering
ASSET_KEYWORDS_BTC = [
    "btc", "bitcoin", "$btc", "#btc", "#bitcoin",
    "₿", "satoshi", "sats"
]

# Velocity calculation windows
VELOCITY_SHORT_WINDOW = 60   # 1 minute
VELOCITY_LONG_WINDOW = 300   # 5 minutes


# =============================================================================
# ENUMS
# =============================================================================

class TwitterSourceType(Enum):
    """Type of Twitter source."""
    ACCOUNT = "account"
    LIST = "list"
    QUERY = "query"


class SourceRole(Enum):
    """Role of the Twitter source."""
    NEWS = "news"
    ANALYST = "analyst"
    COMMUNITY = "community"


class TweetDropReason(Enum):
    """Reason for dropping a tweet."""
    NOT_WHITELISTED = "not_whitelisted"
    SOURCE_DISABLED = "source_disabled"
    SOURCE_RATE_EXCEEDED = "source_rate_exceeded"
    GLOBAL_RATE_EXCEEDED = "global_rate_exceeded"
    EMPTY_TEXT = "empty_text"
    NO_ASSET_KEYWORD = "no_asset_keyword"
    INVALID_TIMESTAMP = "invalid_timestamp"
    LOW_PRECISION_TIMESTAMP = "low_precision_timestamp"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    ALREADY_PROCESSED = "already_processed"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TwitterSource:
    """
    Registered Twitter source from the whitelist.
    
    ONLY sources with enabled = true may be ingested.
    """
    id: int
    source_type: TwitterSourceType
    value: str  # username | list_id | search_query
    asset: str
    role: SourceRole
    enabled: bool
    max_tweets_per_min: int
    priority: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source_type": self.source_type.value,
            "value": self.value,
            "asset": self.asset,
            "role": self.role.value,
            "enabled": self.enabled,
            "max_tweets_per_min": self.max_tweets_per_min,
            "priority": self.priority
        }


@dataclass
class RawTweet:
    """
    Raw tweet data before processing.
    
    REQUIRED fields per contract:
    - tweet_id
    - created_at (UTC, second-level precision)
    - text
    - like_count
    - retweet_count
    - reply_count
    - author_followers_count
    """
    tweet_id: str
    created_at: datetime
    text: str
    like_count: int
    retweet_count: int
    reply_count: int
    author_followers_count: int
    author_username: Optional[str] = None
    source_id: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tweet_id": self.tweet_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "text": self.text,
            "like_count": self.like_count,
            "retweet_count": self.retweet_count,
            "reply_count": self.reply_count,
            "author_followers_count": self.author_followers_count,
            "author_username": self.author_username,
            "source_id": self.source_id
        }


@dataclass
class ProcessedTweet:
    """
    Processed tweet ready for pipeline.
    """
    tweet_id: str
    text: str
    event_time: str  # ISO 8601 UTC
    asset: str
    source: str = "twitter"
    source_reliability: float = SOURCE_RELIABILITY_TWITTER
    
    # Engagement metrics
    like_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    total_engagement: int = 0
    engagement_weight: float = 0.0
    
    # Author metrics
    author_followers_count: int = 0
    author_weight: float = 0.0
    author_username: Optional[str] = None
    
    # Computed metrics
    velocity: float = 0.0
    role: str = ""
    fingerprint: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tweet_id": self.tweet_id,
            "text": self.text,
            "event_time": self.event_time,
            "asset": self.asset,
            "source": self.source,
            "source_reliability": self.source_reliability,
            "like_count": self.like_count,
            "retweet_count": self.retweet_count,
            "reply_count": self.reply_count,
            "total_engagement": self.total_engagement,
            "engagement_weight": self.engagement_weight,
            "author_followers_count": self.author_followers_count,
            "author_weight": self.author_weight,
            "author_username": self.author_username,
            "velocity": self.velocity,
            "role": self.role,
            "fingerprint": self.fingerprint
        }


@dataclass
class IngestionState:
    """
    State for a single Twitter source.
    
    State MUST survive restart.
    NO silent reset of cursors.
    """
    source_id: int
    last_processed_tweet_id: Optional[str] = None
    last_processed_event_time: Optional[datetime] = None
    tweets_processed: int = 0
    last_ingestion_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "last_processed_tweet_id": self.last_processed_tweet_id,
            "last_processed_event_time": (
                self.last_processed_event_time.isoformat()
                if self.last_processed_event_time else None
            ),
            "tweets_processed": self.tweets_processed,
            "last_ingestion_time": (
                self.last_ingestion_time.isoformat()
                if self.last_ingestion_time else None
            )
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IngestionState':
        last_event = data.get("last_processed_event_time")
        last_ingest = data.get("last_ingestion_time")
        
        return cls(
            source_id=data["source_id"],
            last_processed_tweet_id=data.get("last_processed_tweet_id"),
            last_processed_event_time=(
                datetime.fromisoformat(last_event) if last_event else None
            ),
            tweets_processed=data.get("tweets_processed", 0),
            last_ingestion_time=(
                datetime.fromisoformat(last_ingest) if last_ingest else None
            )
        )


@dataclass
class IngestionMetrics:
    """Metrics for the ingestion worker."""
    received: int = 0
    accepted: int = 0
    dropped_not_whitelisted: int = 0
    dropped_disabled: int = 0
    dropped_source_rate: int = 0
    dropped_global_rate: int = 0
    dropped_empty: int = 0
    dropped_no_keyword: int = 0
    dropped_invalid_time: int = 0
    dropped_low_precision: int = 0
    dropped_missing_field: int = 0
    dropped_already_processed: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "received": self.received,
            "accepted": self.accepted,
            "dropped_not_whitelisted": self.dropped_not_whitelisted,
            "dropped_disabled": self.dropped_disabled,
            "dropped_source_rate": self.dropped_source_rate,
            "dropped_global_rate": self.dropped_global_rate,
            "dropped_empty": self.dropped_empty,
            "dropped_no_keyword": self.dropped_no_keyword,
            "dropped_invalid_time": self.dropped_invalid_time,
            "dropped_low_precision": self.dropped_low_precision,
            "dropped_missing_field": self.dropped_missing_field,
            "dropped_already_processed": self.dropped_already_processed
        }
    
    def reset(self) -> None:
        self.received = 0
        self.accepted = 0
        self.dropped_not_whitelisted = 0
        self.dropped_disabled = 0
        self.dropped_source_rate = 0
        self.dropped_global_rate = 0
        self.dropped_empty = 0
        self.dropped_no_keyword = 0
        self.dropped_invalid_time = 0
        self.dropped_low_precision = 0
        self.dropped_missing_field = 0
        self.dropped_already_processed = 0


# =============================================================================
# TWITTER SOURCE REGISTRY
# =============================================================================

class TwitterSourceRegistry:
    """
    Registry of whitelisted Twitter sources.
    
    The worker MUST load Twitter sources from this registry.
    ONLY sources with enabled = true may be ingested.
    """
    
    def __init__(self):
        self._sources: Dict[int, TwitterSource] = {}
        self._lock = threading.Lock()
    
    def load_from_list(self, sources: List[TwitterSource]) -> int:
        """
        Load sources from a list.
        Returns number of sources loaded.
        """
        with self._lock:
            self._sources.clear()
            for source in sources:
                self._sources[source.id] = source
            return len(self._sources)
    
    def load_from_database(
        self,
        connection: Any,
        table_name: str = "twitter_sources"
    ) -> int:
        """
        Load sources from PostgreSQL database.
        
        Expected table schema:
        - id (SERIAL PRIMARY KEY)
        - source_type (VARCHAR): account | list | query
        - value (VARCHAR): username | list_id | search_query
        - asset (VARCHAR)
        - role (VARCHAR): news | analyst | community
        - enabled (BOOLEAN)
        - max_tweets_per_min (INTEGER)
        - priority (SMALLINT)
        
        Returns number of sources loaded.
        """
        if connection is None:
            logger.warning("No database connection, using empty registry")
            return 0
        
        try:
            cursor = connection.cursor()
            query = f"""
                SELECT id, source_type, value, asset, role, 
                       enabled, max_tweets_per_min, priority
                FROM {table_name}
                WHERE enabled = true
                ORDER BY priority DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            sources = []
            for row in rows:
                try:
                    source = TwitterSource(
                        id=int(row[0]),
                        source_type=TwitterSourceType(row[1]),
                        value=str(row[2]),
                        asset=str(row[3]),
                        role=SourceRole(row[4]),
                        enabled=bool(row[5]),
                        max_tweets_per_min=int(row[6]),
                        priority=int(row[7]) if row[7] else 0
                    )
                    sources.append(source)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Invalid source row: {row}, error: {e}")
            
            return self.load_from_list(sources)
            
        except Exception as e:
            logger.error(f"Failed to load sources from database: {e}")
            return 0
    
    def is_whitelisted(self, source_id: int) -> bool:
        """Check if a source_id is in the whitelist."""
        with self._lock:
            return source_id in self._sources
    
    def get_source(self, source_id: int) -> Optional[TwitterSource]:
        """Get source by id. Returns None if not whitelisted."""
        with self._lock:
            return self._sources.get(source_id)
    
    def get_source_by_value(self, value: str) -> Optional[TwitterSource]:
        """Get source by value (username, list_id, query)."""
        with self._lock:
            for source in self._sources.values():
                if source.value.lower() == value.lower():
                    return source
            return None
    
    def get_all_sources(self) -> List[TwitterSource]:
        """Get all sources."""
        with self._lock:
            return list(self._sources.values())
    
    def get_enabled_sources(self) -> List[TwitterSource]:
        """Get all enabled sources, sorted by priority."""
        with self._lock:
            enabled = [s for s in self._sources.values() if s.enabled]
            return sorted(enabled, key=lambda x: x.priority, reverse=True)
    
    def get_sources_by_type(
        self,
        source_type: TwitterSourceType
    ) -> List[TwitterSource]:
        """Get sources by type."""
        with self._lock:
            return [
                s for s in self._sources.values()
                if s.source_type == source_type and s.enabled
            ]
    
    def count(self) -> int:
        """Get number of registered sources."""
        with self._lock:
            return len(self._sources)
    
    def clear(self) -> None:
        """Clear all sources."""
        with self._lock:
            self._sources.clear()


# =============================================================================
# INGESTION STATE MANAGER
# =============================================================================

class IngestionStateManager:
    """
    Manages ingestion state per source.
    
    State MUST survive restart.
    NO silent reset of cursors.
    """
    
    def __init__(self, state_file: Optional[str] = None):
        self._states: Dict[int, IngestionState] = {}
        self._state_file = state_file
        self._lock = threading.Lock()
        
        if state_file:
            self._load_state_from_file()
    
    def _load_state_from_file(self) -> None:
        """Load state from JSON file."""
        if not self._state_file or not os.path.exists(self._state_file):
            return
        
        try:
            with open(self._state_file, 'r') as f:
                data = json.load(f)
                for item in data:
                    state = IngestionState.from_dict(item)
                    self._states[state.source_id] = state
            logger.info(f"Loaded {len(self._states)} source states from file")
        except Exception as e:
            logger.error(f"Failed to load state file: {e}")
    
    def _save_state_to_file(self) -> None:
        """Save state to JSON file."""
        if not self._state_file:
            return
        
        try:
            data = [s.to_dict() for s in self._states.values()]
            with open(self._state_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")
    
    def get_state(self, source_id: int) -> IngestionState:
        """Get or create state for a source."""
        with self._lock:
            if source_id not in self._states:
                self._states[source_id] = IngestionState(source_id=source_id)
            return self._states[source_id]
    
    def update_state(
        self,
        source_id: int,
        tweet_id: str,
        event_time: datetime
    ) -> None:
        """Update state after processing a tweet."""
        with self._lock:
            state = self._states.get(source_id)
            if state is None:
                state = IngestionState(source_id=source_id)
                self._states[source_id] = state
            
            state.last_processed_tweet_id = tweet_id
            state.last_processed_event_time = event_time
            state.tweets_processed += 1
            state.last_ingestion_time = datetime.now(timezone.utc)
            
            self._save_state_to_file()
    
    def get_last_event_time(self, source_id: int) -> Optional[datetime]:
        """Get last processed event time for a source."""
        state = self.get_state(source_id)
        return state.last_processed_event_time
    
    def is_already_processed(self, source_id: int, tweet_id: str) -> bool:
        """Check if a tweet was already processed."""
        state = self.get_state(source_id)
        return state.last_processed_tweet_id == tweet_id
    
    def load_from_database(self, connection: Any) -> int:
        """
        Load states from database.
        
        Expected table: twitter_ingestion_state
        """
        if connection is None:
            return 0
        
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT source_id, last_processed_tweet_id, 
                       last_processed_event_time, tweets_processed,
                       last_ingestion_time
                FROM twitter_ingestion_state
            """)
            rows = cursor.fetchall()
            
            with self._lock:
                for row in rows:
                    state = IngestionState(
                        source_id=int(row[0]),
                        last_processed_tweet_id=row[1],
                        last_processed_event_time=row[2],
                        tweets_processed=int(row[3]) if row[3] else 0,
                        last_ingestion_time=row[4]
                    )
                    self._states[state.source_id] = state
            
            return len(rows)
        except Exception as e:
            logger.error(f"Failed to load states from database: {e}")
            return 0
    
    def save_to_database(self, connection: Any) -> bool:
        """Save all states to database."""
        if connection is None:
            return False
        
        try:
            cursor = connection.cursor()
            
            with self._lock:
                for state in self._states.values():
                    cursor.execute("""
                        INSERT INTO twitter_ingestion_state
                        (source_id, last_processed_tweet_id,
                         last_processed_event_time, tweets_processed,
                         last_ingestion_time)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO UPDATE SET
                            last_processed_tweet_id = EXCLUDED.last_processed_tweet_id,
                            last_processed_event_time = EXCLUDED.last_processed_event_time,
                            tweets_processed = EXCLUDED.tweets_processed,
                            last_ingestion_time = EXCLUDED.last_ingestion_time
                    """, (
                        state.source_id,
                        state.last_processed_tweet_id,
                        state.last_processed_event_time,
                        state.tweets_processed,
                        state.last_ingestion_time
                    ))
            
            connection.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save states to database: {e}")
            return False
    
    def clear(self) -> None:
        """Clear all states (use with caution)."""
        with self._lock:
            self._states.clear()


# =============================================================================
# TWEET RATE LIMITER
# =============================================================================

@dataclass
class RateLimitRecord:
    """Record for rate limiting."""
    timestamps: List[float] = field(default_factory=list)


class TweetRateLimiter:
    """
    Rate limiter for Twitter ingestion.
    
    Enforces:
    - Per-source rate limits (max_tweets_per_min from source config)
    - Global rate limit (500 tweets per minute)
    """
    
    def __init__(
        self,
        global_limit: int = GLOBAL_RATE_LIMIT,
        window_seconds: int = RATE_LIMIT_WINDOW_SECONDS
    ):
        self.global_limit = global_limit
        self.window_seconds = window_seconds
        
        self._source_records: Dict[int, RateLimitRecord] = {}
        self._global_timestamps: List[float] = []
        self._lock = threading.Lock()
    
    def _cleanup_old_timestamps(
        self,
        timestamps: List[float],
        now: float
    ) -> List[float]:
        """Remove timestamps outside the window."""
        cutoff = now - self.window_seconds
        return [t for t in timestamps if t > cutoff]
    
    def check_source_rate(
        self,
        source_id: int,
        max_per_min: int,
        now: Optional[float] = None
    ) -> bool:
        """
        Check if source rate limit allows tweet.
        Returns True if allowed, False if exceeded.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            if source_id not in self._source_records:
                self._source_records[source_id] = RateLimitRecord()
            
            record = self._source_records[source_id]
            record.timestamps = self._cleanup_old_timestamps(
                record.timestamps, now
            )
            
            return len(record.timestamps) < max_per_min
    
    def check_global_rate(self, now: Optional[float] = None) -> bool:
        """
        Check if global rate limit allows tweet.
        Returns True if allowed, False if exceeded.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            self._global_timestamps = self._cleanup_old_timestamps(
                self._global_timestamps, now
            )
            
            return len(self._global_timestamps) < self.global_limit
    
    def record_tweet(
        self,
        source_id: int,
        now: Optional[float] = None
    ) -> None:
        """Record a tweet for rate limiting."""
        if now is None:
            now = time.time()
        
        with self._lock:
            # Record for source
            if source_id not in self._source_records:
                self._source_records[source_id] = RateLimitRecord()
            self._source_records[source_id].timestamps.append(now)
            
            # Record globally
            self._global_timestamps.append(now)
    
    def get_source_count(self, source_id: int) -> int:
        """Get current tweet count for a source."""
        now = time.time()
        with self._lock:
            if source_id not in self._source_records:
                return 0
            record = self._source_records[source_id]
            record.timestamps = self._cleanup_old_timestamps(
                record.timestamps, now
            )
            return len(record.timestamps)
    
    def get_global_count(self) -> int:
        """Get current global tweet count."""
        now = time.time()
        with self._lock:
            self._global_timestamps = self._cleanup_old_timestamps(
                self._global_timestamps, now
            )
            return len(self._global_timestamps)
    
    def clear(self) -> None:
        """Clear all rate limit records."""
        with self._lock:
            self._source_records.clear()
            self._global_timestamps.clear()


# =============================================================================
# METRICS CALCULATOR
# =============================================================================

def safe_log(x: float) -> float:
    """Safe logarithm that handles zero and negative values."""
    if x <= 0:
        return 0.0
    return math.log(x + 1)


def compute_total_engagement(
    like_count: int,
    retweet_count: int,
    reply_count: int
) -> int:
    """
    Compute total engagement.
    Retweets count double per contract.
    """
    return like_count + (retweet_count * 2) + reply_count


def compute_engagement_weight(
    like_count: int,
    retweet_count: int,
    reply_count: int
) -> float:
    """
    Compute engagement weight.
    
    Formula: log(1 + likes + 2*retweets + replies)
    """
    total = compute_total_engagement(like_count, retweet_count, reply_count)
    return round(safe_log(total), 4)


def compute_author_weight(followers_count: int) -> float:
    """
    Compute author weight based on followers.
    
    Formula: log(1 + followers)
    """
    return round(safe_log(followers_count), 4)


class VelocityCalculator:
    """
    Calculates tweet velocity per source.
    
    Velocity = tweets in short window / baseline average
    """
    
    def __init__(
        self,
        short_window: int = VELOCITY_SHORT_WINDOW,
        long_window: int = VELOCITY_LONG_WINDOW
    ):
        self.short_window = short_window
        self.long_window = long_window
        
        # source_id -> list of timestamps
        self._tweets: Dict[int, List[float]] = {}
        self._lock = threading.Lock()
    
    def record_tweet(
        self,
        source_id: int,
        now: Optional[float] = None
    ) -> None:
        """Record a tweet timestamp."""
        if now is None:
            now = time.time()
        
        with self._lock:
            if source_id not in self._tweets:
                self._tweets[source_id] = []
            self._tweets[source_id].append(now)
            
            # Cleanup old tweets (keep only long window)
            cutoff = now - self.long_window
            self._tweets[source_id] = [
                t for t in self._tweets[source_id] if t > cutoff
            ]
    
    def get_velocity(
        self,
        source_id: int,
        now: Optional[float] = None
    ) -> float:
        """
        Get velocity for a source.
        
        Returns ratio of short window rate to long window rate.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            if source_id not in self._tweets:
                return 0.0
            
            timestamps = self._tweets[source_id]
            
            short_cutoff = now - self.short_window
            long_cutoff = now - self.long_window
            
            short_count = sum(1 for t in timestamps if t > short_cutoff)
            long_count = sum(1 for t in timestamps if t > long_cutoff)
            
            if long_count == 0:
                return 0.0
            
            # Normalize to per-minute rates
            short_rate = short_count / (self.short_window / 60)
            long_rate = long_count / (self.long_window / 60)
            
            if long_rate == 0:
                return 0.0
            
            return round(short_rate / long_rate, 2)
    
    def clear(self) -> None:
        """Clear all records."""
        with self._lock:
            self._tweets.clear()


# =============================================================================
# TWEET VALIDATOR
# =============================================================================

class TweetValidator:
    """
    Validates tweets according to ingestion rules.
    """
    
    def __init__(self, asset_keywords: Optional[List[str]] = None):
        self.asset_keywords = asset_keywords or ASSET_KEYWORDS_BTC
    
    def _contains_asset_keyword(self, text: str) -> bool:
        """Check if text contains any asset keyword."""
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in self.asset_keywords)
    
    def _has_second_precision(self, dt: datetime) -> bool:
        """
        Check if timestamp has second-level precision.
        
        Contract: IF timestamp precision < seconds → DROP
        """
        # Check if microseconds are all zeros (minute-only precision)
        # or if it looks like a rounded timestamp
        return dt.second != 0 or dt.microsecond != 0
    
    def validate_required_fields(self, tweet: RawTweet) -> Optional[TweetDropReason]:
        """
        Validate required fields are present.
        
        REQUIRED fields per contract:
        - tweet_id
        - created_at (UTC, second-level precision)
        - text
        - like_count
        - retweet_count
        - reply_count
        - author_followers_count
        """
        if not tweet.tweet_id:
            return TweetDropReason.MISSING_REQUIRED_FIELD
        
        if tweet.created_at is None:
            return TweetDropReason.INVALID_TIMESTAMP
        
        if not tweet.text:
            return TweetDropReason.EMPTY_TEXT
        
        if tweet.like_count is None:
            return TweetDropReason.MISSING_REQUIRED_FIELD
        
        if tweet.retweet_count is None:
            return TweetDropReason.MISSING_REQUIRED_FIELD
        
        if tweet.reply_count is None:
            return TweetDropReason.MISSING_REQUIRED_FIELD
        
        if tweet.author_followers_count is None:
            return TweetDropReason.MISSING_REQUIRED_FIELD
        
        return None
    
    def validate_tweet(
        self,
        tweet: RawTweet,
        source: Optional[TwitterSource] = None
    ) -> Tuple[bool, Optional[TweetDropReason]]:
        """
        Validate a tweet according to all rules.
        
        Returns: (is_valid, drop_reason)
        """
        # Check required fields
        field_error = self.validate_required_fields(tweet)
        if field_error:
            return False, field_error
        
        # Check empty text
        if not tweet.text.strip():
            return False, TweetDropReason.EMPTY_TEXT
        
        # Check timestamp precision
        if not self._has_second_precision(tweet.created_at):
            return False, TweetDropReason.LOW_PRECISION_TIMESTAMP
        
        # Check asset keywords
        if not self._contains_asset_keyword(tweet.text):
            return False, TweetDropReason.NO_ASSET_KEYWORD
        
        return True, None


# =============================================================================
# TWITTER INGESTION WORKER
# =============================================================================

class TwitterIngestionWorker:
    """
    Twitter/X Ingestion Worker for Crypto BotTrading Social Data Pipeline.
    
    LOCKED TWITTER INGESTION CONTRACT.
    
    Safely ingests Twitter/X data ONLY from predefined whitelisted
    sources (accounts, lists, or queries) to detect early narrative,
    FOMO, and panic signals.
    
    ABSOLUTE NON-NEGOTIABLE RULES:
    - DO NOT read the global Twitter firehose
    - DO NOT search all tweets containing BTC
    - DO NOT scrape trending topics
    - DO NOT infer which accounts or queries to read
    - DO NOT use paid Twitter APIs
    - DO NOT generate trading signals
    - ONLY ingest from explicitly whitelisted sources
    - If a source is not whitelisted → IGNORE COMPLETELY
    """
    
    def __init__(
        self,
        registry: TwitterSourceRegistry,
        state_manager: Optional[IngestionStateManager] = None,
        rate_limiter: Optional[TweetRateLimiter] = None,
        velocity_calculator: Optional[VelocityCalculator] = None,
        global_rate_limit: int = GLOBAL_RATE_LIMIT,
        on_tweet: Optional[Callable[[ProcessedTweet], None]] = None
    ):
        self.registry = registry
        self.state_manager = state_manager or IngestionStateManager()
        self.rate_limiter = rate_limiter or TweetRateLimiter(
            global_limit=global_rate_limit
        )
        self.velocity_calculator = velocity_calculator or VelocityCalculator()
        self.on_tweet = on_tweet
        
        self._validator = TweetValidator()
        self._metrics = IngestionMetrics()
        self._running = False
        self._lock = threading.Lock()
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    def start(self) -> None:
        """Start the ingestion worker."""
        with self._lock:
            if self._running:
                logger.warning("Worker already running")
                return
            self._running = True
            logger.info("Twitter Ingestion Worker started")
    
    def stop(self) -> None:
        """Stop the ingestion worker."""
        with self._lock:
            if not self._running:
                logger.warning("Worker not running")
                return
            self._running = False
            logger.info("Twitter Ingestion Worker stopped")
    
    def handle_tweet(
        self,
        tweet: RawTweet,
        source_id: Optional[int] = None,
        now: Optional[float] = None
    ) -> Optional[ProcessedTweet]:
        """
        Handle an incoming tweet.
        
        This is the main entry point for tweet processing.
        
        Returns ProcessedTweet if accepted, None if dropped.
        """
        if now is None:
            now = time.time()
        
        self._metrics.received += 1
        
        # Resolve source
        if source_id is None:
            source_id = tweet.source_id
        
        if source_id is None:
            self._metrics.dropped_not_whitelisted += 1
            return None
        
        # Check whitelist
        if not self.registry.is_whitelisted(source_id):
            self._metrics.dropped_not_whitelisted += 1
            return None
        
        source = self.registry.get_source(source_id)
        
        # Check if source is enabled
        if source is None or not source.enabled:
            self._metrics.dropped_disabled += 1
            return None
        
        # Check if already processed
        if self.state_manager.is_already_processed(source_id, tweet.tweet_id):
            self._metrics.dropped_already_processed += 1
            return None
        
        # Validate tweet
        is_valid, drop_reason = self._validator.validate_tweet(tweet, source)
        if not is_valid:
            self._record_drop(drop_reason)
            return None
        
        # Check source rate limit
        if not self.rate_limiter.check_source_rate(
            source_id, source.max_tweets_per_min, now
        ):
            self._metrics.dropped_source_rate += 1
            return None
        
        # Check global rate limit
        if not self.rate_limiter.check_global_rate(now):
            self._metrics.dropped_global_rate += 1
            return None
        
        # Record for rate limiting
        self.rate_limiter.record_tweet(source_id, now)
        
        # Record for velocity
        self.velocity_calculator.record_tweet(source_id, now)
        velocity = self.velocity_calculator.get_velocity(source_id, now)
        
        # Compute metrics
        total_engagement = compute_total_engagement(
            tweet.like_count, tweet.retweet_count, tweet.reply_count
        )
        engagement_weight = compute_engagement_weight(
            tweet.like_count, tweet.retweet_count, tweet.reply_count
        )
        author_weight = compute_author_weight(tweet.author_followers_count)
        
        # Compute fingerprint
        fingerprint = hashlib.md5(
            f"{tweet.tweet_id}".encode()
        ).hexdigest()[:16]
        
        # Format timestamp
        event_time = tweet.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Create processed tweet
        processed = ProcessedTweet(
            tweet_id=tweet.tweet_id,
            text=tweet.text,
            event_time=event_time,
            asset=source.asset,
            like_count=tweet.like_count,
            retweet_count=tweet.retweet_count,
            reply_count=tweet.reply_count,
            total_engagement=total_engagement,
            engagement_weight=engagement_weight,
            author_followers_count=tweet.author_followers_count,
            author_weight=author_weight,
            author_username=tweet.author_username,
            velocity=velocity,
            role=source.role.value,
            fingerprint=fingerprint
        )
        
        # Update state
        self.state_manager.update_state(
            source_id, tweet.tweet_id, tweet.created_at
        )
        
        self._metrics.accepted += 1
        
        # Invoke callback if set
        if self.on_tweet:
            try:
                self.on_tweet(processed)
            except Exception as e:
                logger.error(f"Callback error: {e}")
        
        return processed
    
    def process_batch(
        self,
        tweets: List[RawTweet],
        source_id: int
    ) -> List[ProcessedTweet]:
        """
        Process a batch of tweets from a source.
        
        Filters tweets created AFTER last_processed_event_time.
        """
        last_time = self.state_manager.get_last_event_time(source_id)
        
        results = []
        for tweet in tweets:
            # Filter by last processed time
            if last_time and tweet.created_at <= last_time:
                continue
            
            tweet.source_id = source_id
            processed = self.handle_tweet(tweet, source_id)
            
            if processed:
                results.append(processed)
        
        return results
    
    def _record_drop(self, reason: Optional[TweetDropReason]) -> None:
        """Record a dropped tweet in metrics."""
        if reason == TweetDropReason.NOT_WHITELISTED:
            self._metrics.dropped_not_whitelisted += 1
        elif reason == TweetDropReason.SOURCE_DISABLED:
            self._metrics.dropped_disabled += 1
        elif reason == TweetDropReason.SOURCE_RATE_EXCEEDED:
            self._metrics.dropped_source_rate += 1
        elif reason == TweetDropReason.GLOBAL_RATE_EXCEEDED:
            self._metrics.dropped_global_rate += 1
        elif reason == TweetDropReason.EMPTY_TEXT:
            self._metrics.dropped_empty += 1
        elif reason == TweetDropReason.NO_ASSET_KEYWORD:
            self._metrics.dropped_no_keyword += 1
        elif reason == TweetDropReason.INVALID_TIMESTAMP:
            self._metrics.dropped_invalid_time += 1
        elif reason == TweetDropReason.LOW_PRECISION_TIMESTAMP:
            self._metrics.dropped_low_precision += 1
        elif reason == TweetDropReason.MISSING_REQUIRED_FIELD:
            self._metrics.dropped_missing_field += 1
        elif reason == TweetDropReason.ALREADY_PROCESSED:
            self._metrics.dropped_already_processed += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current ingestion metrics."""
        return self._metrics.to_dict()
    
    def reset_metrics(self) -> None:
        """Reset ingestion metrics."""
        self._metrics.reset()
    
    def get_enabled_sources(self) -> List[TwitterSource]:
        """Get all enabled sources."""
        return self.registry.get_enabled_sources()
    
    def get_source_state(self, source_id: int) -> IngestionState:
        """Get ingestion state for a source."""
        return self.state_manager.get_state(source_id)


# =============================================================================
# TWITTER CLIENT ADAPTER (Interface)
# =============================================================================

class TwitterClientAdapter:
    """
    Adapter interface for Twitter/X data fetching.
    
    USE public endpoints or scraping techniques only.
    DO NOT use paid Twitter APIs.
    
    This is an abstract interface - implement with actual
    Twitter client library or scraper.
    """
    
    def __init__(self, worker: TwitterIngestionWorker):
        self.worker = worker
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def fetch_account_tweets(
        self,
        username: str,
        since: Optional[datetime] = None,
        max_results: int = 100
    ) -> List[RawTweet]:
        """
        Fetch tweets from an account.
        Override with actual implementation.
        """
        raise NotImplementedError("Implement with Twitter client/scraper")
    
    def fetch_list_tweets(
        self,
        list_id: str,
        since: Optional[datetime] = None,
        max_results: int = 100
    ) -> List[RawTweet]:
        """
        Fetch tweets from a list.
        Override with actual implementation.
        """
        raise NotImplementedError("Implement with Twitter client/scraper")
    
    def fetch_search_tweets(
        self,
        query: str,
        since: Optional[datetime] = None,
        max_results: int = 100
    ) -> List[RawTweet]:
        """
        Fetch tweets matching a search query.
        Override with actual implementation.
        """
        raise NotImplementedError("Implement with Twitter client/scraper")


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_source_registry() -> TwitterSourceRegistry:
    """Create a new source registry."""
    return TwitterSourceRegistry()


def create_ingestion_worker(
    registry: Optional[TwitterSourceRegistry] = None,
    state_file: Optional[str] = None,
    global_rate_limit: int = GLOBAL_RATE_LIMIT,
    on_tweet: Optional[Callable[[ProcessedTweet], None]] = None
) -> TwitterIngestionWorker:
    """
    Create a Twitter Ingestion Worker.
    
    Args:
        registry: Source registry (created if not provided)
        state_file: Path to state persistence file
        global_rate_limit: Maximum tweets per minute globally
        on_tweet: Callback for processed tweets
    
    Returns:
        Configured TwitterIngestionWorker
    """
    if registry is None:
        registry = create_source_registry()
    
    state_manager = IngestionStateManager(state_file=state_file)
    
    return TwitterIngestionWorker(
        registry=registry,
        state_manager=state_manager,
        global_rate_limit=global_rate_limit,
        on_tweet=on_tweet
    )


def create_test_worker(
    sources: Optional[List[TwitterSource]] = None,
    global_rate_limit: int = GLOBAL_RATE_LIMIT
) -> TwitterIngestionWorker:
    """
    Create a test worker with optional predefined sources.
    
    For testing only.
    """
    registry = create_source_registry()
    
    if sources:
        registry.load_from_list(sources)
    
    return TwitterIngestionWorker(
        registry=registry,
        global_rate_limit=global_rate_limit
    )


def get_create_twitter_sources_sql() -> str:
    """
    Get SQL to create the twitter_sources table.
    
    PostgreSQL schema for whitelist registry.
    """
    return """
        CREATE TABLE IF NOT EXISTS twitter_sources (
            id SERIAL PRIMARY KEY,
            source_type VARCHAR(20) NOT NULL 
                CHECK (source_type IN ('account', 'list', 'query')),
            value VARCHAR(255) NOT NULL,
            asset VARCHAR(10) NOT NULL DEFAULT 'BTC',
            role VARCHAR(20) NOT NULL 
                CHECK (role IN ('news', 'analyst', 'community')),
            enabled BOOLEAN NOT NULL DEFAULT true,
            max_tweets_per_min INTEGER NOT NULL DEFAULT 60,
            priority SMALLINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_type, value)
        );
        
        CREATE INDEX IF NOT EXISTS idx_twitter_sources_enabled 
        ON twitter_sources(enabled);
        
        CREATE INDEX IF NOT EXISTS idx_twitter_sources_type 
        ON twitter_sources(source_type);
        
        CREATE TABLE IF NOT EXISTS twitter_ingestion_state (
            source_id INTEGER PRIMARY KEY REFERENCES twitter_sources(id),
            last_processed_tweet_id VARCHAR(50),
            last_processed_event_time TIMESTAMPTZ,
            tweets_processed BIGINT NOT NULL DEFAULT 0,
            last_ingestion_time TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """
