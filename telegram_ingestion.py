"""
Telegram Ingestion Worker for Crypto BotTrading Social Data.

LOCKED TELEGRAM INGESTION CONTRACT.

This module safely ingests Telegram messages ONLY from a predefined
whitelist of public channels/groups to detect panic and abnormal
activity without overwhelming the system.

ABSOLUTE NON-NEGOTIABLE RULES:
- DO NOT read all joined Telegram groups
- DO NOT subscribe to all chats
- DO NOT infer which groups to read
- DO NOT backfill historical messages
- DO NOT use paid Telegram services
- DO NOT generate trading signals
- If chat_id is not explicitly whitelisted → IGNORE COMPLETELY

ROLE IN BOTTRADING SYSTEM:
- Telegram ingestion provides panic/manipulation context only
- It does NOT determine sentiment or trades
"""

import hashlib
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


# =============================================================================
# LOGGING (Simple UTC logger)
# =============================================================================

def _log(level: str, msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} [{level}] telegram_ingestion: {msg}")

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

# Source reliability for Telegram (LOCKED)
SOURCE_RELIABILITY_TELEGRAM = 0.3

# Default global rate limit (messages per minute)
DEFAULT_GLOBAL_RATE_LIMIT = 100

# Default per-group rate limit (messages per minute)
DEFAULT_GROUP_RATE_LIMIT = 30

# Rate limit window in seconds
RATE_LIMIT_WINDOW_SECONDS = 60

# Manipulation detection: similarity threshold for repetition
MANIPULATION_SIMILARITY_THRESHOLD = 3

# Manipulation detection window in seconds
MANIPULATION_WINDOW_SECONDS = 300

# Asset keywords for BTC filtering
ASSET_KEYWORDS_BTC = [
    "btc", "bitcoin", "$btc", "#btc", "#bitcoin",
    "₿", "satoshi", "sats"
]


# =============================================================================
# ENUMS
# =============================================================================

class SourceType(Enum):
    """Type of Telegram source."""
    CHANNEL = "channel"
    GROUP = "group"


class SourceRole(Enum):
    """Role of the Telegram source."""
    NEWS = "news"
    PANIC = "panic"
    COMMUNITY = "community"


class MessageDropReason(Enum):
    """Reason for dropping a message."""
    NOT_WHITELISTED = "not_whitelisted"
    SOURCE_DISABLED = "source_disabled"
    GROUP_RATE_EXCEEDED = "group_rate_exceeded"
    GLOBAL_RATE_EXCEEDED = "global_rate_exceeded"
    EMPTY_TEXT = "empty_text"
    NO_ASSET_KEYWORD = "no_asset_keyword"
    INVALID_TIMESTAMP = "invalid_timestamp"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TelegramSource:
    """
    Registered Telegram source from the whitelist.
    
    Only tg_id values from the registry may be processed.
    """
    tg_id: int
    source_type: SourceType
    role: SourceRole
    asset: str
    enabled: bool
    max_msgs_per_min: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tg_id": self.tg_id,
            "type": self.source_type.value,
            "role": self.role.value,
            "asset": self.asset,
            "enabled": self.enabled,
            "max_msgs_per_min": self.max_msgs_per_min
        }


@dataclass
class TelegramMessage:
    """
    Raw Telegram message before processing.
    """
    chat_id: int
    text: str
    timestamp: datetime
    message_id: Optional[int] = None
    sender_id: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "chat_id": self.chat_id,
            "text": self.text,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "message_id": self.message_id,
            "sender_id": self.sender_id
        }


@dataclass
class ProcessedMessage:
    """
    Processed Telegram message ready for pipeline.
    """
    chat_id: int
    text: str
    event_time: str  # ISO 8601 UTC
    asset: str
    source: str = "telegram"
    source_reliability: float = SOURCE_RELIABILITY_TELEGRAM
    velocity: float = 0.0
    manipulation_flag: bool = False
    role: str = ""
    fingerprint: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "chat_id": self.chat_id,
            "text": self.text,
            "event_time": self.event_time,
            "asset": self.asset,
            "source": self.source,
            "source_reliability": self.source_reliability,
            "velocity": self.velocity,
            "manipulation_flag": self.manipulation_flag,
            "role": self.role,
            "fingerprint": self.fingerprint
        }


@dataclass
class IngestionMetrics:
    """Metrics for the ingestion worker."""
    received: int = 0
    accepted: int = 0
    dropped_not_whitelisted: int = 0
    dropped_disabled: int = 0
    dropped_group_rate: int = 0
    dropped_global_rate: int = 0
    dropped_empty: int = 0
    dropped_no_keyword: int = 0
    dropped_invalid_time: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "received": self.received,
            "accepted": self.accepted,
            "dropped_not_whitelisted": self.dropped_not_whitelisted,
            "dropped_disabled": self.dropped_disabled,
            "dropped_group_rate": self.dropped_group_rate,
            "dropped_global_rate": self.dropped_global_rate,
            "dropped_empty": self.dropped_empty,
            "dropped_no_keyword": self.dropped_no_keyword,
            "dropped_invalid_time": self.dropped_invalid_time
        }
    
    def reset(self) -> None:
        self.received = 0
        self.accepted = 0
        self.dropped_not_whitelisted = 0
        self.dropped_disabled = 0
        self.dropped_group_rate = 0
        self.dropped_global_rate = 0
        self.dropped_empty = 0
        self.dropped_no_keyword = 0
        self.dropped_invalid_time = 0


# =============================================================================
# TELEGRAM SOURCE REGISTRY
# =============================================================================

class TelegramSourceRegistry:
    """
    Registry of whitelisted Telegram sources.
    
    The worker MUST load Telegram sources from this registry.
    ONLY tg_id values from this registry may be processed.
    """
    
    def __init__(self):
        self._sources: Dict[int, TelegramSource] = {}
        self._lock = threading.Lock()
    
    def load_from_list(self, sources: List[TelegramSource]) -> int:
        """
        Load sources from a list.
        Returns number of sources loaded.
        """
        with self._lock:
            self._sources.clear()
            for source in sources:
                self._sources[source.tg_id] = source
            return len(self._sources)
    
    def load_from_database(
        self,
        connection: Any,
        table_name: str = "telegram_sources"
    ) -> int:
        """
        Load sources from PostgreSQL database.
        
        Expected table schema:
        - tg_id (BIGINT)
        - type (VARCHAR): channel | group
        - role (VARCHAR): news | panic | community
        - asset (VARCHAR)
        - enabled (BOOLEAN)
        - max_msgs_per_min (INTEGER)
        
        Returns number of sources loaded.
        """
        if connection is None:
            logger.warning("No database connection, using empty registry")
            return 0
        
        try:
            cursor = connection.cursor()
            query = f"""
                SELECT tg_id, type, role, asset, enabled, max_msgs_per_min
                FROM {table_name}
                WHERE enabled = true
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            sources = []
            for row in rows:
                try:
                    source = TelegramSource(
                        tg_id=int(row[0]),
                        source_type=SourceType(row[1]),
                        role=SourceRole(row[2]),
                        asset=str(row[3]),
                        enabled=bool(row[4]),
                        max_msgs_per_min=int(row[5])
                    )
                    sources.append(source)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Invalid source row: {row}, error: {e}")
            
            return self.load_from_list(sources)
            
        except Exception as e:
            logger.error(f"Failed to load sources from database: {e}")
            return 0
    
    def is_whitelisted(self, tg_id: int) -> bool:
        """
        Check if a tg_id is in the whitelist.
        
        CRITICAL: If not whitelisted, message MUST be ignored completely.
        """
        with self._lock:
            return tg_id in self._sources
    
    def get_source(self, tg_id: int) -> Optional[TelegramSource]:
        """Get source by tg_id. Returns None if not whitelisted."""
        with self._lock:
            return self._sources.get(tg_id)
    
    def get_all_whitelisted_ids(self) -> Set[int]:
        """Get all whitelisted tg_ids."""
        with self._lock:
            return set(self._sources.keys())
    
    def get_enabled_sources(self) -> List[TelegramSource]:
        """Get all enabled sources."""
        with self._lock:
            return [s for s in self._sources.values() if s.enabled]
    
    def count(self) -> int:
        """Get number of registered sources."""
        with self._lock:
            return len(self._sources)
    
    def clear(self) -> None:
        """Clear all sources."""
        with self._lock:
            self._sources.clear()


# =============================================================================
# MESSAGE RATE LIMITER
# =============================================================================

@dataclass
class RateLimitRecord:
    """Record for rate limiting."""
    timestamps: List[float] = field(default_factory=list)


class MessageRateLimiter:
    """
    Rate limiter for Telegram messages.
    
    Enforces:
    - Per-group rate limits (max_msgs_per_min from source config)
    - Global rate limit (safety limit for entire system)
    """
    
    def __init__(
        self,
        global_limit: int = DEFAULT_GLOBAL_RATE_LIMIT,
        window_seconds: int = RATE_LIMIT_WINDOW_SECONDS
    ):
        self.global_limit = global_limit
        self.window_seconds = window_seconds
        
        self._group_records: Dict[int, RateLimitRecord] = {}
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
    
    def check_group_rate(
        self,
        tg_id: int,
        max_per_min: int,
        now: Optional[float] = None
    ) -> bool:
        """
        Check if group rate limit allows message.
        Returns True if allowed, False if exceeded.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            if tg_id not in self._group_records:
                self._group_records[tg_id] = RateLimitRecord()
            
            record = self._group_records[tg_id]
            record.timestamps = self._cleanup_old_timestamps(
                record.timestamps, now
            )
            
            return len(record.timestamps) < max_per_min
    
    def check_global_rate(self, now: Optional[float] = None) -> bool:
        """
        Check if global rate limit allows message.
        Returns True if allowed, False if exceeded.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            self._global_timestamps = self._cleanup_old_timestamps(
                self._global_timestamps, now
            )
            
            return len(self._global_timestamps) < self.global_limit
    
    def record_message(
        self,
        tg_id: int,
        now: Optional[float] = None
    ) -> None:
        """Record a message for rate limiting."""
        if now is None:
            now = time.time()
        
        with self._lock:
            # Record for group
            if tg_id not in self._group_records:
                self._group_records[tg_id] = RateLimitRecord()
            self._group_records[tg_id].timestamps.append(now)
            
            # Record globally
            self._global_timestamps.append(now)
    
    def get_group_count(self, tg_id: int) -> int:
        """Get current message count for a group."""
        now = time.time()
        with self._lock:
            if tg_id not in self._group_records:
                return 0
            record = self._group_records[tg_id]
            record.timestamps = self._cleanup_old_timestamps(
                record.timestamps, now
            )
            return len(record.timestamps)
    
    def get_global_count(self) -> int:
        """Get current global message count."""
        now = time.time()
        with self._lock:
            self._global_timestamps = self._cleanup_old_timestamps(
                self._global_timestamps, now
            )
            return len(self._global_timestamps)
    
    def clear(self) -> None:
        """Clear all rate limit records."""
        with self._lock:
            self._group_records.clear()
            self._global_timestamps.clear()


# =============================================================================
# MANIPULATION DETECTOR
# =============================================================================

class ManipulationDetector:
    """
    Detects manipulation through message repetition.
    
    Flags messages that appear multiple times with similar content
    within a short window (coordinated spam).
    """
    
    def __init__(
        self,
        threshold: int = MANIPULATION_SIMILARITY_THRESHOLD,
        window_seconds: int = MANIPULATION_WINDOW_SECONDS
    ):
        self.threshold = threshold
        self.window_seconds = window_seconds
        
        # fingerprint -> list of (timestamp, chat_id)
        self._fingerprints: Dict[str, List[tuple]] = {}
        self._lock = threading.Lock()
    
    def _compute_fingerprint(self, text: str) -> str:
        """
        Compute fingerprint for text.
        Normalizes text to detect similar messages.
        """
        # Normalize: lowercase, remove punctuation and numbers
        normalized = text.lower()
        normalized = ''.join(
            c for c in normalized
            if c.isalpha() or c.isspace()
        )
        normalized = ' '.join(normalized.split())
        
        # Hash the normalized text
        return hashlib.md5(normalized.encode()).hexdigest()[:16]
    
    def _cleanup_old_entries(self, now: float) -> None:
        """Remove entries outside the window."""
        cutoff = now - self.window_seconds
        
        keys_to_remove = []
        for fingerprint, entries in self._fingerprints.items():
            entries[:] = [(t, c) for t, c in entries if t > cutoff]
            if not entries:
                keys_to_remove.append(fingerprint)
        
        for key in keys_to_remove:
            del self._fingerprints[key]
    
    def check_manipulation(
        self,
        text: str,
        chat_id: int,
        now: Optional[float] = None
    ) -> tuple:
        """
        Check if message shows signs of manipulation.
        
        Returns: (is_manipulation: bool, fingerprint: str)
        """
        if now is None:
            now = time.time()
        
        fingerprint = self._compute_fingerprint(text)
        
        with self._lock:
            self._cleanup_old_entries(now)
            
            if fingerprint not in self._fingerprints:
                self._fingerprints[fingerprint] = []
            
            entries = self._fingerprints[fingerprint]
            
            # Count unique chats with this fingerprint
            unique_chats = set(c for _, c in entries)
            unique_chats.add(chat_id)
            
            # Record this occurrence
            entries.append((now, chat_id))
            
            # Manipulation if same message in multiple chats
            is_manipulation = len(unique_chats) >= self.threshold
            
            return is_manipulation, fingerprint
    
    def clear(self) -> None:
        """Clear all fingerprints."""
        with self._lock:
            self._fingerprints.clear()


# =============================================================================
# VELOCITY CALCULATOR
# =============================================================================

class VelocityCalculator:
    """
    Calculates message velocity per chat.
    
    Velocity = messages in short window / baseline average
    """
    
    def __init__(
        self,
        short_window: int = 60,
        long_window: int = 300
    ):
        self.short_window = short_window
        self.long_window = long_window
        
        # chat_id -> list of timestamps
        self._messages: Dict[int, List[float]] = {}
        self._lock = threading.Lock()
    
    def record_message(
        self,
        chat_id: int,
        now: Optional[float] = None
    ) -> None:
        """Record a message timestamp."""
        if now is None:
            now = time.time()
        
        with self._lock:
            if chat_id not in self._messages:
                self._messages[chat_id] = []
            self._messages[chat_id].append(now)
            
            # Cleanup old messages (keep only long window)
            cutoff = now - self.long_window
            self._messages[chat_id] = [
                t for t in self._messages[chat_id] if t > cutoff
            ]
    
    def get_velocity(
        self,
        chat_id: int,
        now: Optional[float] = None
    ) -> float:
        """
        Get velocity for a chat.
        
        Returns ratio of short window rate to long window rate.
        """
        if now is None:
            now = time.time()
        
        with self._lock:
            if chat_id not in self._messages:
                return 0.0
            
            timestamps = self._messages[chat_id]
            
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
            self._messages.clear()


# =============================================================================
# MESSAGE FILTER
# =============================================================================

class TelegramMessageFilter:
    """
    Filters Telegram messages based on ingestion rules.
    
    Message handling logic (STRICT):
    1. IF message.chat_id NOT IN whitelist → IGNORE
    2. IF source.enabled == false → IGNORE
    3. IF per-group message rate exceeds max_msgs_per_min → DROP
    4. IF global Telegram message rate exceeds safety limit → DROP
    5. IF message.text is empty → DROP
    6. IF message does not contain asset keywords → DROP
    """
    
    def __init__(
        self,
        registry: TelegramSourceRegistry,
        rate_limiter: MessageRateLimiter,
        asset_keywords: Optional[List[str]] = None
    ):
        self.registry = registry
        self.rate_limiter = rate_limiter
        self.asset_keywords = asset_keywords or ASSET_KEYWORDS_BTC
    
    def _contains_asset_keyword(self, text: str) -> bool:
        """Check if text contains any asset keyword."""
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in self.asset_keywords)
    
    def filter_message(
        self,
        message: TelegramMessage,
        now: Optional[float] = None
    ) -> tuple:
        """
        Filter a message according to ingestion rules.
        
        Returns: (accepted: bool, drop_reason: Optional[MessageDropReason], source: Optional[TelegramSource])
        """
        if now is None:
            now = time.time()
        
        chat_id = message.chat_id
        
        # Rule 1: Check whitelist
        # CRITICAL: If not whitelisted → IGNORE COMPLETELY
        if not self.registry.is_whitelisted(chat_id):
            return False, MessageDropReason.NOT_WHITELISTED, None
        
        source = self.registry.get_source(chat_id)
        
        # Rule 2: Check if enabled
        if source is None or not source.enabled:
            return False, MessageDropReason.SOURCE_DISABLED, source
        
        # Rule 3: Check per-group rate limit
        if not self.rate_limiter.check_group_rate(
            chat_id, source.max_msgs_per_min, now
        ):
            return False, MessageDropReason.GROUP_RATE_EXCEEDED, source
        
        # Rule 4: Check global rate limit
        if not self.rate_limiter.check_global_rate(now):
            return False, MessageDropReason.GLOBAL_RATE_EXCEEDED, source
        
        # Rule 5: Check empty text
        if not message.text or not message.text.strip():
            return False, MessageDropReason.EMPTY_TEXT, source
        
        # Rule 6: Check asset keywords
        if not self._contains_asset_keyword(message.text):
            return False, MessageDropReason.NO_ASSET_KEYWORD, source
        
        # Rule 7: Validate timestamp
        if message.timestamp is None:
            return False, MessageDropReason.INVALID_TIMESTAMP, source
        
        return True, None, source


# =============================================================================
# TELEGRAM INGESTION WORKER
# =============================================================================

class TelegramIngestionWorker:
    """
    Telegram Ingestion Worker for Crypto BotTrading Social Data.
    
    LOCKED TELEGRAM INGESTION CONTRACT.
    
    Safely ingests Telegram messages ONLY from a predefined whitelist
    of public channels/groups to detect panic and abnormal activity.
    
    ABSOLUTE NON-NEGOTIABLE RULES:
    - DO NOT read all joined Telegram groups
    - DO NOT subscribe to all chats
    - DO NOT infer which groups to read
    - DO NOT backfill historical messages
    - DO NOT use paid Telegram services
    - DO NOT generate trading signals
    - If chat_id is not explicitly whitelisted → IGNORE COMPLETELY
    """
    
    def __init__(
        self,
        registry: TelegramSourceRegistry,
        rate_limiter: Optional[MessageRateLimiter] = None,
        manipulation_detector: Optional[ManipulationDetector] = None,
        velocity_calculator: Optional[VelocityCalculator] = None,
        global_rate_limit: int = DEFAULT_GLOBAL_RATE_LIMIT,
        on_message: Optional[Callable[[ProcessedMessage], None]] = None
    ):
        self.registry = registry
        self.rate_limiter = rate_limiter or MessageRateLimiter(
            global_limit=global_rate_limit
        )
        self.manipulation_detector = manipulation_detector or ManipulationDetector()
        self.velocity_calculator = velocity_calculator or VelocityCalculator()
        self.on_message = on_message
        
        self._filter = TelegramMessageFilter(
            registry=self.registry,
            rate_limiter=self.rate_limiter
        )
        
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
            logger.info("Telegram Ingestion Worker started")
    
    def stop(self) -> None:
        """Stop the ingestion worker."""
        with self._lock:
            if not self._running:
                logger.warning("Worker not running")
                return
            self._running = False
            logger.info("Telegram Ingestion Worker stopped")
    
    def handle_message(
        self,
        message: TelegramMessage,
        now: Optional[float] = None
    ) -> Optional[ProcessedMessage]:
        """
        Handle an incoming Telegram message.
        
        This is the main entry point for message processing.
        
        Returns ProcessedMessage if accepted, None if dropped.
        """
        if now is None:
            now = time.time()
        
        self._metrics.received += 1
        
        # Filter message according to rules
        accepted, drop_reason, source = self._filter.filter_message(message, now)
        
        if not accepted:
            self._record_drop(drop_reason)
            return None
        
        # Record for rate limiting
        self.rate_limiter.record_message(message.chat_id, now)
        
        # Compute manipulation flag
        is_manipulation, fingerprint = self.manipulation_detector.check_manipulation(
            message.text, message.chat_id, now
        )
        
        # Record for velocity calculation
        self.velocity_calculator.record_message(message.chat_id, now)
        velocity = self.velocity_calculator.get_velocity(message.chat_id, now)
        
        # Format timestamp
        event_time = message.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Create processed message
        processed = ProcessedMessage(
            chat_id=message.chat_id,
            text=message.text,
            event_time=event_time,
            asset=source.asset,
            velocity=velocity,
            manipulation_flag=is_manipulation,
            role=source.role.value,
            fingerprint=fingerprint
        )
        
        self._metrics.accepted += 1
        
        # Invoke callback if set
        if self.on_message:
            try:
                self.on_message(processed)
            except Exception as e:
                logger.error(f"Callback error: {e}")
        
        return processed
    
    def _record_drop(self, reason: Optional[MessageDropReason]) -> None:
        """Record a dropped message in metrics."""
        if reason == MessageDropReason.NOT_WHITELISTED:
            self._metrics.dropped_not_whitelisted += 1
        elif reason == MessageDropReason.SOURCE_DISABLED:
            self._metrics.dropped_disabled += 1
        elif reason == MessageDropReason.GROUP_RATE_EXCEEDED:
            self._metrics.dropped_group_rate += 1
        elif reason == MessageDropReason.GLOBAL_RATE_EXCEEDED:
            self._metrics.dropped_global_rate += 1
        elif reason == MessageDropReason.EMPTY_TEXT:
            self._metrics.dropped_empty += 1
        elif reason == MessageDropReason.NO_ASSET_KEYWORD:
            self._metrics.dropped_no_keyword += 1
        elif reason == MessageDropReason.INVALID_TIMESTAMP:
            self._metrics.dropped_invalid_time += 1
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current ingestion metrics."""
        return self._metrics.to_dict()
    
    def reset_metrics(self) -> None:
        """Reset ingestion metrics."""
        self._metrics.reset()
    
    def get_whitelisted_ids(self) -> Set[int]:
        """Get all whitelisted chat IDs."""
        return self.registry.get_all_whitelisted_ids()


# =============================================================================
# TELEGRAM CLIENT ADAPTER (Interface)
# =============================================================================

class TelegramClientAdapter:
    """
    Adapter interface for Telegram Client API.
    
    Use Telegram Client API (user account).
    Connect as normal Telegram user.
    DO NOT use bot API for ingestion.
    Messages are received in real time only.
    
    This is an abstract interface - implement with actual
    Telegram client library (e.g., Telethon).
    """
    
    def __init__(self, worker: TelegramIngestionWorker):
        self.worker = worker
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> bool:
        """
        Connect to Telegram.
        Override this method with actual implementation.
        """
        raise NotImplementedError("Implement with Telegram client library")
    
    def disconnect(self) -> None:
        """
        Disconnect from Telegram.
        Override this method with actual implementation.
        """
        raise NotImplementedError("Implement with Telegram client library")
    
    def on_new_message(self, event: Any) -> None:
        """
        Handler for new messages from Telegram client.
        
        CRITICAL: Only process messages from whitelisted chats.
        If chat_id NOT IN whitelist → IGNORE COMPLETELY.
        """
        raise NotImplementedError("Implement with Telegram client library")


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_source_registry() -> TelegramSourceRegistry:
    """Create a new source registry."""
    return TelegramSourceRegistry()


def create_ingestion_worker(
    registry: Optional[TelegramSourceRegistry] = None,
    global_rate_limit: int = DEFAULT_GLOBAL_RATE_LIMIT,
    on_message: Optional[Callable[[ProcessedMessage], None]] = None
) -> TelegramIngestionWorker:
    """
    Create a Telegram Ingestion Worker.
    
    Args:
        registry: Source registry (created if not provided)
        global_rate_limit: Maximum messages per minute globally
        on_message: Callback for processed messages
    
    Returns:
        Configured TelegramIngestionWorker
    """
    if registry is None:
        registry = create_source_registry()
    
    return TelegramIngestionWorker(
        registry=registry,
        global_rate_limit=global_rate_limit,
        on_message=on_message
    )


def create_test_worker(
    sources: Optional[List[TelegramSource]] = None,
    global_rate_limit: int = DEFAULT_GLOBAL_RATE_LIMIT
) -> TelegramIngestionWorker:
    """
    Create a test worker with optional predefined sources.
    
    For testing only.
    """
    registry = create_source_registry()
    
    if sources:
        registry.load_from_list(sources)
    
    return TelegramIngestionWorker(
        registry=registry,
        global_rate_limit=global_rate_limit
    )


def get_create_telegram_sources_sql() -> str:
    """
    Get SQL to create the telegram_sources table.
    
    PostgreSQL schema for whitelist registry.
    """
    return """
        CREATE TABLE IF NOT EXISTS telegram_sources (
            id SERIAL PRIMARY KEY,
            tg_id BIGINT NOT NULL UNIQUE,
            type VARCHAR(20) NOT NULL CHECK (type IN ('channel', 'group')),
            role VARCHAR(20) NOT NULL CHECK (role IN ('news', 'panic', 'community')),
            asset VARCHAR(10) NOT NULL DEFAULT 'BTC',
            enabled BOOLEAN NOT NULL DEFAULT true,
            max_msgs_per_min INTEGER NOT NULL DEFAULT 30,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_telegram_sources_tg_id 
        ON telegram_sources(tg_id);
        
        CREATE INDEX IF NOT EXISTS idx_telegram_sources_enabled 
        ON telegram_sources(enabled);
    """
