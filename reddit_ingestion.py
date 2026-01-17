"""
Reddit Ingestion Worker for Crypto BotTrading Social Data Pipeline.

LOCKED REDDIT INGESTION CONTRACT.

This module safely ingests Reddit data ONLY from predefined whitelisted
subreddits to capture slower-moving crowd sentiment and confirmation
signals without unnecessary noise.

ABSOLUTE NON-NEGOTIABLE RULES:
- DO NOT crawl all subreddits
- DO NOT read r/all or global comment streams
- DO NOT infer which subreddits to monitor
- DO NOT use paid Reddit APIs
- DO NOT generate trading signals
- ONLY ingest from explicitly whitelisted subreddits
- If subreddit is not whitelisted → IGNORE COMPLETELY

ROLE IN BOTTRADING SYSTEM:
- Reddit provides CROWD CONFIRMATION
- Reddit does NOT provide early signals
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
    print(f"{ts} [{level}] reddit_ingestion: {msg}")

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

# Source reliability for Reddit (LOCKED)
SOURCE_RELIABILITY_REDDIT = 0.7

# Global rate limit (items per run across ALL subreddits)
GLOBAL_RATE_LIMIT = 500

# Default per-subreddit limit
DEFAULT_SUBREDDIT_LIMIT = 100

# Minimum score for acceptance
MIN_SCORE = 1  # score > 0

# Asset keywords for BTC filtering
ASSET_KEYWORDS_BTC = [
    "btc", "bitcoin", "$btc", "#btc", "#bitcoin",
    "₿", "satoshi", "sats"
]

# Velocity calculation windows (in seconds)
VELOCITY_SHORT_WINDOW = 300   # 5 minutes
VELOCITY_LONG_WINDOW = 1800   # 30 minutes


# =============================================================================
# ENUMS
# =============================================================================

class RedditItemType(Enum):
    """Type of Reddit item."""
    POST = "post"
    COMMENT = "comment"


class SubredditRole(Enum):
    """Role of the subreddit in the pipeline."""
    DISCUSSION = "discussion"
    MARKET = "market"
    DEV = "dev"


class RedditDropReason(Enum):
    """Reason for dropping a Reddit item."""
    NOT_WHITELISTED = "not_whitelisted"
    SUBREDDIT_DISABLED = "subreddit_disabled"
    SUBREDDIT_RATE_EXCEEDED = "subreddit_rate_exceeded"
    GLOBAL_RATE_EXCEEDED = "global_rate_exceeded"
    EMPTY_TEXT = "empty_text"
    NO_ASSET_KEYWORD = "no_asset_keyword"
    INVALID_TIMESTAMP = "invalid_timestamp"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    LOW_SCORE = "low_score"
    ALREADY_PROCESSED = "already_processed"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RedditSource:
    """
    Registered Reddit source (subreddit) from the whitelist.
    
    ONLY sources with enabled = true may be ingested.
    """
    id: int
    subreddit: str  # e.g., "Bitcoin", "CryptoCurrency"
    asset: str
    role: SubredditRole
    enabled: bool
    max_posts_per_run: int
    priority: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "subreddit": self.subreddit,
            "asset": self.asset,
            "role": self.role.value,
            "enabled": self.enabled,
            "max_posts_per_run": self.max_posts_per_run,
            "priority": self.priority
        }


@dataclass
class RawRedditItem:
    """
    Raw Reddit item (post or comment) before processing.
    
    REQUIRED fields per contract:
    - item_id (unique identifier)
    - created_utc (Unix timestamp)
    - text/body
    - score (upvotes)
    - num_comments (for posts)
    - author_karma
    """
    item_id: str
    item_type: RedditItemType
    subreddit: str
    created_utc: float  # Unix timestamp
    text: str
    score: int
    num_comments: int = 0
    author_karma: int = 0
    author_name: Optional[str] = None
    title: Optional[str] = None  # For posts
    permalink: Optional[str] = None
    source_id: Optional[int] = None
    
    @property
    def created_at(self) -> datetime:
        """Convert created_utc to datetime."""
        return datetime.fromtimestamp(self.created_utc, tz=timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "item_type": self.item_type.value,
            "subreddit": self.subreddit,
            "created_utc": self.created_utc,
            "created_at": self.created_at.isoformat() if self.created_utc else None,
            "text": self.text,
            "score": self.score,
            "num_comments": self.num_comments,
            "author_karma": self.author_karma,
            "author_name": self.author_name,
            "title": self.title,
            "permalink": self.permalink,
            "source_id": self.source_id
        }


@dataclass
class ProcessedRedditItem:
    """
    Processed Reddit item ready for pipeline.
    """
    item_id: str
    item_type: str
    subreddit: str
    text: str
    event_time: str  # ISO 8601 UTC
    asset: str
    source: str = "reddit"
    source_reliability: float = SOURCE_RELIABILITY_REDDIT
    
    # Engagement metrics
    score: int = 0
    num_comments: int = 0
    engagement_weight: float = 0.0
    
    # Author metrics
    author_karma: int = 0
    author_weight: float = 0.0
    author_name: Optional[str] = None
    
    # Computed metrics
    velocity: float = 0.0
    role: str = ""
    fingerprint: Optional[str] = None
    title: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "item_type": self.item_type,
            "subreddit": self.subreddit,
            "text": self.text,
            "event_time": self.event_time,
            "asset": self.asset,
            "source": self.source,
            "source_reliability": self.source_reliability,
            "score": self.score,
            "num_comments": self.num_comments,
            "engagement_weight": self.engagement_weight,
            "author_karma": self.author_karma,
            "author_weight": self.author_weight,
            "author_name": self.author_name,
            "velocity": self.velocity,
            "role": self.role,
            "fingerprint": self.fingerprint,
            "title": self.title
        }


@dataclass
class IngestionState:
    """
    State for a single Reddit source (subreddit).
    
    State MUST survive restart.
    NO historical backfill beyond cursor.
    """
    source_id: int
    subreddit: str
    last_processed_item_id: Optional[str] = None
    last_processed_timestamp: Optional[float] = None  # Unix timestamp
    items_processed: int = 0
    last_ingestion_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_id": self.source_id,
            "subreddit": self.subreddit,
            "last_processed_item_id": self.last_processed_item_id,
            "last_processed_timestamp": self.last_processed_timestamp,
            "items_processed": self.items_processed,
            "last_ingestion_time": (
                self.last_ingestion_time.isoformat()
                if self.last_ingestion_time else None
            )
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IngestionState':
        last_ingest = data.get("last_ingestion_time")
        
        return cls(
            source_id=data["source_id"],
            subreddit=data.get("subreddit", ""),
            last_processed_item_id=data.get("last_processed_item_id"),
            last_processed_timestamp=data.get("last_processed_timestamp"),
            items_processed=data.get("items_processed", 0),
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
    dropped_subreddit_rate: int = 0
    dropped_global_rate: int = 0
    dropped_empty: int = 0
    dropped_no_keyword: int = 0
    dropped_invalid_time: int = 0
    dropped_missing_field: int = 0
    dropped_low_score: int = 0
    dropped_already_processed: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "received": self.received,
            "accepted": self.accepted,
            "dropped_not_whitelisted": self.dropped_not_whitelisted,
            "dropped_disabled": self.dropped_disabled,
            "dropped_subreddit_rate": self.dropped_subreddit_rate,
            "dropped_global_rate": self.dropped_global_rate,
            "dropped_empty": self.dropped_empty,
            "dropped_no_keyword": self.dropped_no_keyword,
            "dropped_invalid_time": self.dropped_invalid_time,
            "dropped_missing_field": self.dropped_missing_field,
            "dropped_low_score": self.dropped_low_score,
            "dropped_already_processed": self.dropped_already_processed
        }
    
    def reset(self) -> None:
        self.received = 0
        self.accepted = 0
        self.dropped_not_whitelisted = 0
        self.dropped_disabled = 0
        self.dropped_subreddit_rate = 0
        self.dropped_global_rate = 0
        self.dropped_empty = 0
        self.dropped_no_keyword = 0
        self.dropped_invalid_time = 0
        self.dropped_missing_field = 0
        self.dropped_low_score = 0
        self.dropped_already_processed = 0


# =============================================================================
# REDDIT SOURCE REGISTRY
# =============================================================================

class RedditSourceRegistry:
    """
    Registry of whitelisted Reddit sources (subreddits).
    
    The worker MUST load Reddit sources from this registry.
    ONLY sources with enabled = true may be ingested.
    """
    
    def __init__(self):
        self._sources: Dict[int, RedditSource] = {}
        self._by_subreddit: Dict[str, RedditSource] = {}
        self._lock = threading.Lock()
    
    def load_from_list(self, sources: List[RedditSource]) -> int:
        """
        Load sources from a list.
        Returns number of sources loaded.
        """
        with self._lock:
            self._sources.clear()
            self._by_subreddit.clear()
            for source in sources:
                self._sources[source.id] = source
                self._by_subreddit[source.subreddit.lower()] = source
            return len(self._sources)
    
    def load_from_database(
        self,
        connection: Any,
        table_name: str = "reddit_sources"
    ) -> int:
        """
        Load sources from PostgreSQL database.
        
        Expected table schema:
        - id (SERIAL PRIMARY KEY)
        - subreddit (VARCHAR)
        - asset (VARCHAR)
        - role (VARCHAR): discussion | market | dev
        - enabled (BOOLEAN)
        - max_posts_per_run (INTEGER)
        - priority (SMALLINT)
        
        Returns number of sources loaded.
        """
        if connection is None:
            logger.warning("No database connection, using empty registry")
            return 0
        
        try:
            cursor = connection.cursor()
            query = f"""
                SELECT id, subreddit, asset, role, enabled, 
                       max_posts_per_run, priority
                FROM {table_name}
                WHERE enabled = true
                ORDER BY priority DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            sources = []
            for row in rows:
                try:
                    source = RedditSource(
                        id=int(row[0]),
                        subreddit=str(row[1]),
                        asset=str(row[2]),
                        role=SubredditRole(row[3]),
                        enabled=bool(row[4]),
                        max_posts_per_run=int(row[5]),
                        priority=int(row[6]) if row[6] else 0
                    )
                    sources.append(source)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Invalid source row: {row}, error: {e}")
            
            return self.load_from_list(sources)
            
        except Exception as e:
            logger.error(f"Failed to load sources from database: {e}")
            return 0
    
    def is_whitelisted(self, subreddit: str) -> bool:
        """Check if a subreddit is in the whitelist."""
        with self._lock:
            return subreddit.lower() in self._by_subreddit
    
    def is_whitelisted_by_id(self, source_id: int) -> bool:
        """Check if a source_id is in the whitelist."""
        with self._lock:
            return source_id in self._sources
    
    def get_source(self, source_id: int) -> Optional[RedditSource]:
        """Get source by id. Returns None if not whitelisted."""
        with self._lock:
            return self._sources.get(source_id)
    
    def get_source_by_subreddit(self, subreddit: str) -> Optional[RedditSource]:
        """Get source by subreddit name (case insensitive)."""
        with self._lock:
            return self._by_subreddit.get(subreddit.lower())
    
    def get_all_sources(self) -> List[RedditSource]:
        """Get all sources."""
        with self._lock:
            return list(self._sources.values())
    
    def get_enabled_sources(self) -> List[RedditSource]:
        """Get all enabled sources, sorted by priority."""
        with self._lock:
            enabled = [s for s in self._sources.values() if s.enabled]
            return sorted(enabled, key=lambda x: x.priority, reverse=True)
    
    def get_sources_by_role(self, role: SubredditRole) -> List[RedditSource]:
        """Get sources by role."""
        with self._lock:
            return [
                s for s in self._sources.values()
                if s.role == role and s.enabled
            ]
    
    def count(self) -> int:
        """Get number of registered sources."""
        with self._lock:
            return len(self._sources)
    
    def clear(self) -> None:
        """Clear all sources."""
        with self._lock:
            self._sources.clear()
            self._by_subreddit.clear()


# =============================================================================
# INGESTION STATE MANAGER
# =============================================================================

class IngestionStateManager:
    """
    Manages ingestion state per subreddit.
    
    State MUST survive restart.
    NO historical backfill beyond cursor.
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
            logger.info(f"Loaded {len(self._states)} subreddit states from file")
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
    
    def get_state(self, source_id: int, subreddit: str = "") -> IngestionState:
        """Get or create state for a source."""
        with self._lock:
            if source_id not in self._states:
                self._states[source_id] = IngestionState(
                    source_id=source_id,
                    subreddit=subreddit
                )
            return self._states[source_id]
    
    def update_state(
        self,
        source_id: int,
        subreddit: str,
        item_id: str,
        timestamp: float
    ) -> None:
        """Update state after processing an item."""
        with self._lock:
            state = self._states.get(source_id)
            if state is None:
                state = IngestionState(source_id=source_id, subreddit=subreddit)
                self._states[source_id] = state
            
            state.last_processed_item_id = item_id
            state.last_processed_timestamp = timestamp
            state.items_processed += 1
            state.last_ingestion_time = datetime.now(timezone.utc)
            
            self._save_state_to_file()
    
    def get_last_timestamp(self, source_id: int) -> Optional[float]:
        """Get last processed timestamp for a source."""
        with self._lock:
            state = self._states.get(source_id)
            if state:
                return state.last_processed_timestamp
            return None
    
    def is_already_processed(self, source_id: int, item_id: str) -> bool:
        """Check if an item was already processed."""
        with self._lock:
            state = self._states.get(source_id)
            if state:
                return state.last_processed_item_id == item_id
            return False
    
    def load_from_database(self, connection: Any) -> int:
        """Load states from database."""
        if connection is None:
            return 0
        
        try:
            cursor = connection.cursor()
            cursor.execute("""
                SELECT source_id, subreddit, last_processed_item_id, 
                       last_processed_timestamp, items_processed,
                       last_ingestion_time
                FROM reddit_ingestion_state
            """)
            rows = cursor.fetchall()
            
            with self._lock:
                for row in rows:
                    state = IngestionState(
                        source_id=int(row[0]),
                        subreddit=str(row[1]) if row[1] else "",
                        last_processed_item_id=row[2],
                        last_processed_timestamp=float(row[3]) if row[3] else None,
                        items_processed=int(row[4]) if row[4] else 0,
                        last_ingestion_time=row[5]
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
                        INSERT INTO reddit_ingestion_state
                        (source_id, subreddit, last_processed_item_id,
                         last_processed_timestamp, items_processed,
                         last_ingestion_time)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO UPDATE SET
                            subreddit = EXCLUDED.subreddit,
                            last_processed_item_id = EXCLUDED.last_processed_item_id,
                            last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                            items_processed = EXCLUDED.items_processed,
                            last_ingestion_time = EXCLUDED.last_ingestion_time
                    """, (
                        state.source_id,
                        state.subreddit,
                        state.last_processed_item_id,
                        state.last_processed_timestamp,
                        state.items_processed,
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
# REDDIT RATE LIMITER
# =============================================================================

class RedditRateLimiter:
    """
    Rate limiter for Reddit ingestion.
    
    Enforces:
    - Per-subreddit limits (max_posts_per_run from source config)
    - Global limit (500 items per run)
    
    Unlike Twitter/Telegram, Reddit limits are PER RUN not per minute.
    """
    
    def __init__(self, global_limit: int = GLOBAL_RATE_LIMIT):
        self.global_limit = global_limit
        
        self._subreddit_counts: Dict[int, int] = {}
        self._global_count: int = 0
        self._lock = threading.Lock()
    
    def check_subreddit_limit(
        self,
        source_id: int,
        max_per_run: int
    ) -> bool:
        """
        Check if subreddit limit allows item.
        Returns True if allowed, False if exceeded.
        """
        with self._lock:
            current = self._subreddit_counts.get(source_id, 0)
            return current < max_per_run
    
    def check_global_limit(self) -> bool:
        """
        Check if global limit allows item.
        Returns True if allowed, False if exceeded.
        """
        with self._lock:
            return self._global_count < self.global_limit
    
    def record_item(self, source_id: int) -> None:
        """Record an item for rate limiting."""
        with self._lock:
            self._subreddit_counts[source_id] = (
                self._subreddit_counts.get(source_id, 0) + 1
            )
            self._global_count += 1
    
    def get_subreddit_count(self, source_id: int) -> int:
        """Get current count for a subreddit."""
        with self._lock:
            return self._subreddit_counts.get(source_id, 0)
    
    def get_global_count(self) -> int:
        """Get current global count."""
        with self._lock:
            return self._global_count
    
    def reset(self) -> None:
        """Reset all counters (call at start of each run)."""
        with self._lock:
            self._subreddit_counts.clear()
            self._global_count = 0


# =============================================================================
# METRICS CALCULATOR
# =============================================================================

def safe_log(x: float) -> float:
    """Safe logarithm that handles zero and negative values."""
    if x <= 0:
        return 0.0
    return math.log(x + 1)


def compute_engagement_weight(score: int, num_comments: int) -> float:
    """
    Compute engagement weight for Reddit.
    
    Formula: log(1 + score + num_comments)
    """
    total = score + num_comments
    return round(safe_log(total), 4)


def compute_author_weight(author_karma: int) -> float:
    """
    Compute author weight based on karma.
    
    Formula: log(1 + karma)
    """
    return round(safe_log(author_karma), 4)


class VelocityCalculator:
    """
    Calculates item velocity per subreddit.
    
    Velocity = items in short window / baseline average
    """
    
    def __init__(
        self,
        short_window: int = VELOCITY_SHORT_WINDOW,
        long_window: int = VELOCITY_LONG_WINDOW
    ):
        self.short_window = short_window
        self.long_window = long_window
        
        # source_id -> list of timestamps
        self._items: Dict[int, List[float]] = {}
        self._lock = threading.Lock()
    
    def record_item(
        self,
        source_id: int,
        now: Optional[float] = None
    ) -> None:
        """Record an item timestamp."""
        if now is None:
            now = time.time()
        
        with self._lock:
            if source_id not in self._items:
                self._items[source_id] = []
            self._items[source_id].append(now)
            
            # Cleanup old items (keep only long window)
            cutoff = now - self.long_window
            self._items[source_id] = [
                t for t in self._items[source_id] if t > cutoff
            ]
    
    def get_velocity(
        self,
        source_id: int,
        now: Optional[float] = None
    ) -> float:
        """
        Get velocity for a subreddit.
        
        Returns ratio of short window rate to long window rate.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            if source_id not in self._items:
                return 0.0
            
            timestamps = self._items[source_id]
            
            short_cutoff = now - self.short_window
            long_cutoff = now - self.long_window
            
            short_count = sum(1 for t in timestamps if t > short_cutoff)
            long_count = sum(1 for t in timestamps if t > long_cutoff)
            
            if long_count == 0:
                return 0.0
            
            # Normalize to comparable rates
            short_rate = short_count / (self.short_window / 60)
            long_rate = long_count / (self.long_window / 60)
            
            if long_rate == 0:
                return 0.0
            
            return round(short_rate / long_rate, 2)
    
    def clear(self) -> None:
        """Clear all records."""
        with self._lock:
            self._items.clear()


# =============================================================================
# REDDIT ITEM VALIDATOR
# =============================================================================

class RedditItemValidator:
    """
    Validates Reddit items according to ingestion rules.
    """
    
    def __init__(self, asset_keywords: Optional[List[str]] = None):
        self.asset_keywords = asset_keywords or ASSET_KEYWORDS_BTC
    
    def _contains_asset_keyword(self, text: str) -> bool:
        """Check if text contains any asset keyword."""
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in self.asset_keywords)
    
    def validate_required_fields(
        self,
        item: RawRedditItem
    ) -> Optional[RedditDropReason]:
        """
        Validate required fields are present.
        
        REQUIRED fields per contract:
        - item_id
        - created_utc
        - text/body
        - score (upvotes)
        - num_comments (for posts)
        - author_karma
        """
        if not item.item_id:
            return RedditDropReason.MISSING_REQUIRED_FIELD
        
        if item.created_utc is None or item.created_utc <= 0:
            return RedditDropReason.INVALID_TIMESTAMP
        
        if not item.text:
            return RedditDropReason.EMPTY_TEXT
        
        if item.score is None:
            return RedditDropReason.MISSING_REQUIRED_FIELD
        
        return None
    
    def validate_item(
        self,
        item: RawRedditItem,
        source: Optional[RedditSource] = None
    ) -> Tuple[bool, Optional[RedditDropReason]]:
        """
        Validate a Reddit item according to all rules.
        
        FILTER rules per contract:
        - score > 0
        - text length > 0
        - asset keyword present
        
        Returns: (is_valid, drop_reason)
        """
        # Check required fields
        field_error = self.validate_required_fields(item)
        if field_error:
            return False, field_error
        
        # Check empty text
        if not item.text.strip():
            return False, RedditDropReason.EMPTY_TEXT
        
        # Check score > 0
        if item.score < MIN_SCORE:
            return False, RedditDropReason.LOW_SCORE
        
        # Check asset keywords (in text or title)
        text_to_check = item.text
        if item.title:
            text_to_check = f"{item.title} {item.text}"
        
        if not self._contains_asset_keyword(text_to_check):
            return False, RedditDropReason.NO_ASSET_KEYWORD
        
        return True, None


# =============================================================================
# REDDIT INGESTION WORKER
# =============================================================================

class RedditIngestionWorker:
    """
    Reddit Ingestion Worker for Crypto BotTrading Social Data Pipeline.
    
    LOCKED REDDIT INGESTION CONTRACT.
    
    Safely ingests Reddit data ONLY from predefined whitelisted
    subreddits to capture slower-moving crowd sentiment and
    confirmation signals.
    
    ABSOLUTE NON-NEGOTIABLE RULES:
    - DO NOT crawl all subreddits
    - DO NOT read r/all or global comment streams
    - DO NOT infer which subreddits to monitor
    - DO NOT use paid Reddit APIs
    - DO NOT generate trading signals
    - ONLY ingest from explicitly whitelisted subreddits
    - If subreddit is not whitelisted → IGNORE COMPLETELY
    """
    
    def __init__(
        self,
        registry: RedditSourceRegistry,
        state_manager: Optional[IngestionStateManager] = None,
        rate_limiter: Optional[RedditRateLimiter] = None,
        velocity_calculator: Optional[VelocityCalculator] = None,
        global_rate_limit: int = GLOBAL_RATE_LIMIT,
        on_item: Optional[Callable[[ProcessedRedditItem], None]] = None
    ):
        self.registry = registry
        self.state_manager = state_manager or IngestionStateManager()
        self.rate_limiter = rate_limiter or RedditRateLimiter(
            global_limit=global_rate_limit
        )
        self.velocity_calculator = velocity_calculator or VelocityCalculator()
        self.on_item = on_item
        
        self._validator = RedditItemValidator()
        self._metrics = IngestionMetrics()
        self._running = False
        self._lock = threading.Lock()
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    def start_run(self) -> None:
        """Start a new ingestion run."""
        with self._lock:
            if self._running:
                logger.warning("Run already in progress")
                return
            self._running = True
            self.rate_limiter.reset()  # Reset per-run counters
            logger.info("Reddit Ingestion Run started")
    
    def end_run(self) -> None:
        """End the current ingestion run."""
        with self._lock:
            if not self._running:
                logger.warning("No run in progress")
                return
            self._running = False
            logger.info("Reddit Ingestion Run ended")
    
    def handle_item(
        self,
        item: RawRedditItem,
        source_id: Optional[int] = None,
        now: Optional[float] = None
    ) -> Optional[ProcessedRedditItem]:
        """
        Handle an incoming Reddit item.
        
        This is the main entry point for item processing.
        
        Returns ProcessedRedditItem if accepted, None if dropped.
        """
        if now is None:
            now = time.time()
        
        self._metrics.received += 1
        
        # Resolve source by subreddit name if source_id not provided
        if source_id is None:
            source_id = item.source_id
        
        if source_id is None:
            # Try to find by subreddit name
            source = self.registry.get_source_by_subreddit(item.subreddit)
            if source:
                source_id = source.id
        
        if source_id is None:
            self._metrics.dropped_not_whitelisted += 1
            return None
        
        # Check whitelist
        if not self.registry.is_whitelisted_by_id(source_id):
            self._metrics.dropped_not_whitelisted += 1
            return None
        
        source = self.registry.get_source(source_id)
        
        # Check if source is enabled
        if source is None or not source.enabled:
            self._metrics.dropped_disabled += 1
            return None
        
        # Check if already processed
        if self.state_manager.is_already_processed(source_id, item.item_id):
            self._metrics.dropped_already_processed += 1
            return None
        
        # Validate item
        is_valid, drop_reason = self._validator.validate_item(item, source)
        if not is_valid:
            self._record_drop(drop_reason)
            return None
        
        # Check subreddit limit
        if not self.rate_limiter.check_subreddit_limit(
            source_id, source.max_posts_per_run
        ):
            self._metrics.dropped_subreddit_rate += 1
            return None
        
        # Check global limit
        if not self.rate_limiter.check_global_limit():
            self._metrics.dropped_global_rate += 1
            return None
        
        # Record for rate limiting
        self.rate_limiter.record_item(source_id)
        
        # Record for velocity
        self.velocity_calculator.record_item(source_id, now)
        velocity = self.velocity_calculator.get_velocity(source_id, now)
        
        # Compute metrics
        engagement_weight = compute_engagement_weight(
            item.score, item.num_comments
        )
        author_weight = compute_author_weight(item.author_karma)
        
        # Compute fingerprint
        fingerprint = hashlib.md5(
            f"{item.item_id}".encode()
        ).hexdigest()[:16]
        
        # Format timestamp
        event_time = item.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Create processed item
        processed = ProcessedRedditItem(
            item_id=item.item_id,
            item_type=item.item_type.value,
            subreddit=item.subreddit,
            text=item.text,
            event_time=event_time,
            asset=source.asset,
            score=item.score,
            num_comments=item.num_comments,
            engagement_weight=engagement_weight,
            author_karma=item.author_karma,
            author_weight=author_weight,
            author_name=item.author_name,
            velocity=velocity,
            role=source.role.value,
            fingerprint=fingerprint,
            title=item.title
        )
        
        # Update state
        self.state_manager.update_state(
            source_id, source.subreddit, item.item_id, item.created_utc
        )
        
        self._metrics.accepted += 1
        
        # Invoke callback if set
        if self.on_item:
            try:
                self.on_item(processed)
            except Exception as e:
                logger.error(f"Callback error: {e}")
        
        return processed
    
    def process_batch(
        self,
        items: List[RawRedditItem],
        source_id: int
    ) -> List[ProcessedRedditItem]:
        """
        Process a batch of items from a subreddit.
        
        Filters items created AFTER last_processed_timestamp.
        """
        last_ts = self.state_manager.get_last_timestamp(source_id)
        
        results = []
        for item in items:
            # Filter by last processed timestamp
            if last_ts and item.created_utc <= last_ts:
                continue
            
            item.source_id = source_id
            processed = self.handle_item(item, source_id)
            
            if processed:
                results.append(processed)
        
        return results
    
    def _record_drop(self, reason: Optional[RedditDropReason]) -> None:
        """Record a dropped item in metrics."""
        if reason == RedditDropReason.NOT_WHITELISTED:
            self._metrics.dropped_not_whitelisted += 1
        elif reason == RedditDropReason.SUBREDDIT_DISABLED:
            self._metrics.dropped_disabled += 1
        elif reason == RedditDropReason.SUBREDDIT_RATE_EXCEEDED:
            self._metrics.dropped_subreddit_rate += 1
        elif reason == RedditDropReason.GLOBAL_RATE_EXCEEDED:
            self._metrics.dropped_global_rate += 1
        elif reason == RedditDropReason.EMPTY_TEXT:
            self._metrics.dropped_empty += 1
        elif reason == RedditDropReason.NO_ASSET_KEYWORD:
            self._metrics.dropped_no_keyword += 1
        elif reason == RedditDropReason.INVALID_TIMESTAMP:
            self._metrics.dropped_invalid_time += 1
        elif reason == RedditDropReason.MISSING_REQUIRED_FIELD:
            self._metrics.dropped_missing_field += 1
        elif reason == RedditDropReason.LOW_SCORE:
            self._metrics.dropped_low_score += 1
        elif reason == RedditDropReason.ALREADY_PROCESSED:
            self._metrics.dropped_already_processed += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current ingestion metrics."""
        return self._metrics.to_dict()
    
    def reset_metrics(self) -> None:
        """Reset ingestion metrics."""
        self._metrics.reset()
    
    def get_enabled_sources(self) -> List[RedditSource]:
        """Get all enabled subreddits."""
        return self.registry.get_enabled_sources()
    
    def get_source_state(self, source_id: int) -> IngestionState:
        """Get ingestion state for a subreddit."""
        source = self.registry.get_source(source_id)
        subreddit = source.subreddit if source else ""
        return self.state_manager.get_state(source_id, subreddit)


# =============================================================================
# REDDIT CLIENT ADAPTER (Interface)
# =============================================================================

class RedditClientAdapter:
    """
    Adapter interface for Reddit data fetching.
    
    USE public JSON endpoints only.
    DO NOT use paid Reddit APIs.
    
    This is an abstract interface - implement with actual
    Reddit client library or public JSON endpoints.
    
    Example endpoints:
    - https://www.reddit.com/r/{subreddit}/new.json
    - https://www.reddit.com/r/{subreddit}/comments.json
    """
    
    def __init__(self, worker: RedditIngestionWorker):
        self.worker = worker
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def fetch_subreddit_posts(
        self,
        subreddit: str,
        since: Optional[float] = None,
        limit: int = 100
    ) -> List[RawRedditItem]:
        """
        Fetch posts from a subreddit.
        Override with actual implementation.
        
        Uses: https://www.reddit.com/r/{subreddit}/new.json
        """
        raise NotImplementedError("Implement with Reddit JSON endpoint")
    
    def fetch_subreddit_comments(
        self,
        subreddit: str,
        since: Optional[float] = None,
        limit: int = 100
    ) -> List[RawRedditItem]:
        """
        Fetch comments from a subreddit.
        Override with actual implementation.
        
        Uses: https://www.reddit.com/r/{subreddit}/comments.json
        """
        raise NotImplementedError("Implement with Reddit JSON endpoint")


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_source_registry() -> RedditSourceRegistry:
    """Create a new source registry."""
    return RedditSourceRegistry()


def create_ingestion_worker(
    registry: Optional[RedditSourceRegistry] = None,
    state_file: Optional[str] = None,
    global_rate_limit: int = GLOBAL_RATE_LIMIT,
    on_item: Optional[Callable[[ProcessedRedditItem], None]] = None
) -> RedditIngestionWorker:
    """
    Create a Reddit Ingestion Worker.
    
    Args:
        registry: Source registry (created if not provided)
        state_file: Path to state persistence file
        global_rate_limit: Maximum items per run globally
        on_item: Callback for processed items
    
    Returns:
        Configured RedditIngestionWorker
    """
    if registry is None:
        registry = create_source_registry()
    
    state_manager = IngestionStateManager(state_file=state_file)
    
    return RedditIngestionWorker(
        registry=registry,
        state_manager=state_manager,
        global_rate_limit=global_rate_limit,
        on_item=on_item
    )


def create_test_worker(
    sources: Optional[List[RedditSource]] = None,
    global_rate_limit: int = GLOBAL_RATE_LIMIT
) -> RedditIngestionWorker:
    """
    Create a test worker with optional predefined sources.
    
    For testing only.
    """
    registry = create_source_registry()
    
    if sources:
        registry.load_from_list(sources)
    
    return RedditIngestionWorker(
        registry=registry,
        global_rate_limit=global_rate_limit
    )


def get_create_reddit_sources_sql() -> str:
    """
    Get SQL to create the reddit_sources table.
    
    PostgreSQL schema for whitelist registry.
    """
    return """
        CREATE TABLE IF NOT EXISTS reddit_sources (
            id SERIAL PRIMARY KEY,
            subreddit VARCHAR(100) NOT NULL UNIQUE,
            asset VARCHAR(10) NOT NULL DEFAULT 'BTC',
            role VARCHAR(20) NOT NULL 
                CHECK (role IN ('discussion', 'market', 'dev')),
            enabled BOOLEAN NOT NULL DEFAULT true,
            max_posts_per_run INTEGER NOT NULL DEFAULT 100,
            priority SMALLINT NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_reddit_sources_enabled 
        ON reddit_sources(enabled);
        
        CREATE INDEX IF NOT EXISTS idx_reddit_sources_role 
        ON reddit_sources(role);
        
        CREATE TABLE IF NOT EXISTS reddit_ingestion_state (
            source_id INTEGER PRIMARY KEY REFERENCES reddit_sources(id),
            subreddit VARCHAR(100),
            last_processed_item_id VARCHAR(50),
            last_processed_timestamp DOUBLE PRECISION,
            items_processed BIGINT NOT NULL DEFAULT 0,
            last_ingestion_time TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """
