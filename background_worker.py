"""
Background Worker / Scheduler for Crypto Social Sentiment Data Service.

This worker runs continuously and independently:
- Collects FREE public social data
- Processes data deterministically (time-safe)
- Computes sentiment and risk indicators
- Persists ALL event-level data into PostgreSQL

ABSOLUTE RULES:
- NO mocking
- NO hallucination
- NO paid APIs
- NO trading signals
- NO ALLOW/BLOCK decisions
- If any stage fails → DROP EVENT and continue

BotTrading MUST NEVER trigger crawling or computation.
"""

import os
import sys
import time
import signal
import threading
import logging
import hashlib
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Callable
from enum import Enum
from pathlib import Path
import uuid

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ"
)
logger = logging.getLogger("background_worker")


# =============================================================================
# CONSTANTS
# =============================================================================

# Scheduling intervals (seconds)
# Twitter: 30 min interval due to Syndication API rate limits
TWITTER_INTERVAL = 1800  # 30 minutes (rate limit sensitive)
TELEGRAM_INTERVAL = 20
REDDIT_INTERVAL = 300  # 5 minutes

# Source reliability weights
SOURCE_RELIABILITY = {
    "twitter": 0.5,
    "reddit": 0.7,
    "telegram": 0.3
}

# Time sync thresholds
MAX_DELAY_SECONDS = 300  # 5 minutes
MAX_FUTURE_SECONDS = 5   # 5 seconds tolerance

# State file path
STATE_FILE_PATH = Path("worker_state.json")

# Asset configuration - loaded dynamically from database
# Use asset_config module for multi-asset support
from asset_config import get_asset_config, detect_asset as detect_asset_from_text

def get_supported_assets() -> list:
    """Get list of currently active asset symbols."""
    return get_asset_config().get_active_symbols()

def get_asset_for_text(text: str) -> str:
    """Detect asset from text, defaults to BTC if none detected."""
    asset = detect_asset_from_text(text)
    return asset if asset else "BTC"


# =============================================================================
# ENUMS
# =============================================================================

class PipelineStage(str, Enum):
    RAW_VALIDATION = "raw_validation"
    TIME_SYNC = "time_sync"
    INSERT_RAW = "insert_raw"
    METRICS = "metrics"
    SENTIMENT = "sentiment"
    INSERT_SENTIMENT = "insert_sentiment"
    RISK_INDICATORS = "risk_indicators"
    INSERT_RISK = "insert_risk"
    DATA_QUALITY = "data_quality"


class SourceType(str, Enum):
    TWITTER = "twitter"
    REDDIT = "reddit"
    TELEGRAM = "telegram"


# =============================================================================
# METRICS
# =============================================================================

@dataclass
class WorkerMetrics:
    """Metrics for observability (internal only, NOT for BotTrading)."""
    events_collected: Dict[str, int] = field(default_factory=lambda: {
        "twitter": 0, "reddit": 0, "telegram": 0
    })
    events_dropped: Dict[str, int] = field(default_factory=lambda: {
        "twitter": 0, "reddit": 0, "telegram": 0
    })
    events_inserted: Dict[str, int] = field(default_factory=lambda: {
        "twitter": 0, "reddit": 0, "telegram": 0
    })
    errors_by_stage: Dict[str, int] = field(default_factory=lambda: {
        stage.value: 0 for stage in PipelineStage
    })
    lag_seconds_per_source: Dict[str, float] = field(default_factory=lambda: {
        "twitter": 0.0, "reddit": 0.0, "telegram": 0.0
    })
    last_success_time: Dict[str, Optional[datetime]] = field(default_factory=lambda: {
        "twitter": None, "reddit": None, "telegram": None
    })
    
    def record_collected(self, source: str):
        self.events_collected[source] = self.events_collected.get(source, 0) + 1
    
    def record_dropped(self, source: str):
        self.events_dropped[source] = self.events_dropped.get(source, 0) + 1
    
    def record_inserted(self, source: str):
        self.events_inserted[source] = self.events_inserted.get(source, 0) + 1
    
    def record_error(self, stage: PipelineStage):
        self.errors_by_stage[stage.value] = self.errors_by_stage.get(stage.value, 0) + 1
    
    def update_lag(self, source: str, lag_seconds: float):
        self.lag_seconds_per_source[source] = lag_seconds
    
    def record_success(self, source: str):
        self.last_success_time[source] = datetime.now(timezone.utc)
    
    def to_dict(self) -> dict:
        return {
            "events_collected": self.events_collected.copy(),
            "events_dropped": self.events_dropped.copy(),
            "events_inserted": self.events_inserted.copy(),
            "errors_by_stage": self.errors_by_stage.copy(),
            "lag_seconds_per_source": self.lag_seconds_per_source.copy(),
            "last_success_time": {
                k: v.isoformat() if v else None
                for k, v in self.last_success_time.items()
            }
        }


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

@dataclass
class SourceState:
    """State for a single source."""
    last_event_time: Optional[datetime] = None
    last_message_id: Optional[str] = None  # For Telegram
    last_run_time: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            "last_event_time": self.last_event_time.isoformat() if self.last_event_time else None,
            "last_message_id": self.last_message_id,
            "last_run_time": self.last_run_time.isoformat() if self.last_run_time else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "SourceState":
        return cls(
            last_event_time=datetime.fromisoformat(data["last_event_time"]) if data.get("last_event_time") else None,
            last_message_id=data.get("last_message_id"),
            last_run_time=datetime.fromisoformat(data["last_run_time"]) if data.get("last_run_time") else None
        )


class WorkerState:
    """
    Persistent state management for worker cursors.
    State survives restarts via file persistence.
    """
    
    def __init__(self, state_file: Path = STATE_FILE_PATH):
        self.state_file = state_file
        self.sources: Dict[str, SourceState] = {
            "twitter": SourceState(),
            "reddit": SourceState(),
            "telegram": SourceState()
        }
        self._lock = threading.Lock()
        self._load_state()
    
    def _load_state(self):
        """Load state from file if exists."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                
                for source, state_data in data.get("sources", {}).items():
                    if source in self.sources:
                        self.sources[source] = SourceState.from_dict(state_data)
                
                logger.info(f"Loaded worker state from {self.state_file}")
            except Exception as e:
                logger.warning(f"Failed to load state: {e}. Starting fresh.")
    
    def save_state(self):
        """Persist state to file."""
        with self._lock:
            try:
                data = {
                    "sources": {
                        source: state.to_dict()
                        for source, state in self.sources.items()
                    },
                    "saved_at": datetime.now(timezone.utc).isoformat()
                }
                
                # Atomic write
                temp_file = self.state_file.with_suffix(".tmp")
                with open(temp_file, "w") as f:
                    json.dump(data, f, indent=2)
                temp_file.replace(self.state_file)
                
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
    
    def get_source_state(self, source: str) -> SourceState:
        """Get state for a source."""
        with self._lock:
            return self.sources.get(source, SourceState())
    
    def update_source_state(
        self,
        source: str,
        last_event_time: Optional[datetime] = None,
        last_message_id: Optional[str] = None
    ):
        """Update state for a source."""
        with self._lock:
            state = self.sources.get(source, SourceState())
            
            if last_event_time:
                state.last_event_time = last_event_time
            if last_message_id:
                state.last_message_id = last_message_id
            
            state.last_run_time = datetime.now(timezone.utc)
            self.sources[source] = state


# =============================================================================
# EVENT FINGERPRINT
# =============================================================================

def compute_fingerprint(source: str, text: str, event_time: datetime) -> str:
    """
    Compute deterministic fingerprint for deduplication.
    """
    content = f"{source}|{text}|{event_time.isoformat()}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


# =============================================================================
# TIME SYNC GUARD
# =============================================================================

def validate_time_sync(
    event_time: datetime,
    ingest_time: Optional[datetime] = None
) -> tuple[bool, Optional[str]]:
    """
    Validate event time for time synchronization.
    
    Returns (is_valid, error_message)
    """
    if ingest_time is None:
        ingest_time = datetime.now(timezone.utc)
    
    # Ensure UTC
    if event_time.tzinfo is None:
        return False, "event_time must have timezone info"
    
    # Check for future events
    future_delta = (event_time - ingest_time).total_seconds()
    if future_delta > MAX_FUTURE_SECONDS:
        return False, f"event_time is {future_delta:.1f}s in future"
    
    # Check for excessive delay
    delay = (ingest_time - event_time).total_seconds()
    if delay > MAX_DELAY_SECONDS:
        return False, f"event_time is {delay:.1f}s old (max {MAX_DELAY_SECONDS}s)"
    
    return True, None


def parse_timestamp(ts: Any) -> Optional[datetime]:
    """Parse various timestamp formats to datetime."""
    if ts is None:
        return None
    
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts
    
    if isinstance(ts, (int, float)):
        # Unix timestamp
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    
    if isinstance(ts, str):
        # Try ISO format
        try:
            if ts.endswith("Z"):
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return datetime.fromisoformat(ts)
        except ValueError:
            pass
        
        # Try common formats
        formats = [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%a %b %d %H:%M:%S %z %Y",  # Twitter format
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(ts, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
    
    return None


def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO-8601 UTC."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# SENTIMENT ANALYSIS (RULE-BASED)
# =============================================================================

# Lexicon for sentiment scoring
BULLISH_KEYWORDS = {
    "moon", "bullish", "buy", "long", "pump", "rocket", "ath", "breakout",
    "lambo", "hodl", "diamond", "gains", "surge", "rally", "parabolic",
    "explosive", "skyrocket", "mooning", "bull", "up", "green"
}

BEARISH_KEYWORDS = {
    "crash", "bearish", "sell", "short", "dump", "rekt", "bear", "dip",
    "collapse", "plunge", "tank", "correction", "downfall", "panic",
    "fear", "capitulation", "bloodbath", "red", "down", "loss"
}

FEAR_KEYWORDS = {
    "fear", "panic", "scared", "worried", "concern", "risk", "warning",
    "danger", "crash", "collapse", "uncertainty"
}

GREED_KEYWORDS = {
    "greed", "fomo", "yolo", "all-in", "100x", "rich", "millionaire",
    "lambo", "moon", "guaranteed", "easy money"
}


def compute_sentiment(text: str) -> dict:
    """
    Compute sentiment using rule-based lexicon matching.
    Returns sentiment_label (-1, 0, 1) and confidence (0-1).
    """
    if not text:
        return {"label": 0, "confidence": 0.0}
    
    text_lower = text.lower()
    words = set(text_lower.split())
    
    bullish_count = len(words & BULLISH_KEYWORDS)
    bearish_count = len(words & BEARISH_KEYWORDS)
    
    total_signals = bullish_count + bearish_count
    
    if total_signals == 0:
        return {"label": 0, "confidence": 0.5}
    
    # Calculate sentiment
    score = bullish_count - bearish_count
    
    if score > 0:
        label = 1  # Bullish
    elif score < 0:
        label = -1  # Bearish
    else:
        label = 0  # Neutral
    
    # Confidence based on signal strength
    confidence = min(1.0, total_signals / 5.0) * 0.5 + 0.5
    confidence = round(confidence, 4)
    
    return {"label": label, "confidence": confidence}


# =============================================================================
# RISK INDICATORS COMPUTATION
# =============================================================================

def compute_risk_indicators(
    sentiment_label: int,
    sentiment_confidence: float,
    text: str,
    velocity: float = 0.0,
    source: str = "unknown"
) -> dict:
    """
    Compute risk indicators from sentiment and text.
    These are DATA ONLY - no trading decisions.
    """
    text_lower = text.lower() if text else ""
    words = set(text_lower.split())
    
    fear_count = len(words & FEAR_KEYWORDS)
    greed_count = len(words & GREED_KEYWORDS)
    
    # Social overheat: high velocity or excessive greed signals
    social_overheat = velocity > 5.0 or greed_count >= 3
    
    # Panic risk: bearish sentiment with fear signals
    panic_risk = sentiment_label == -1 and fear_count >= 2
    
    # FOMO risk: bullish sentiment with greed signals
    fomo_risk = sentiment_label == 1 and greed_count >= 2
    
    # Fear/Greed zone (descriptive only)
    if fear_count > greed_count + 2:
        fear_greed_zone = "extreme_fear"
        fear_greed_index = max(0, 25 - fear_count * 5)
    elif greed_count > fear_count + 2:
        fear_greed_zone = "extreme_greed"
        fear_greed_index = min(100, 75 + greed_count * 5)
    else:
        fear_greed_zone = "normal"
        fear_greed_index = 50 + (greed_count - fear_count) * 10
        fear_greed_index = max(25, min(75, fear_greed_index))
    
    return {
        "sentiment_label": sentiment_label,
        "sentiment_confidence": sentiment_confidence,
        "social_overheat": social_overheat,
        "panic_risk": panic_risk,
        "fomo_risk": fomo_risk,
        "fear_greed_index": fear_greed_index,
        "fear_greed_zone": fear_greed_zone
    }


# =============================================================================
# METRIC COMPUTATION
# =============================================================================

def compute_engagement_weight(event: dict, source: str) -> float:
    """Compute engagement weight based on source metrics."""
    if source == "twitter":
        likes = event.get("favorite_count", event.get("likes", 0)) or 0
        retweets = event.get("retweet_count", event.get("retweets", 0)) or 0
        replies = event.get("reply_count", event.get("replies", 0)) or 0
        return likes * 1.0 + retweets * 2.0 + replies * 1.5
    
    elif source == "reddit":
        score = event.get("score", event.get("ups", 0)) or 0
        comments = event.get("num_comments", 0) or 0
        return score * 1.0 + comments * 2.0
    
    elif source == "telegram":
        views = event.get("views", 0) or 0
        forwards = event.get("forwards", 0) or 0
        return views * 0.1 + forwards * 5.0
    
    return 0.0


def compute_author_weight(event: dict, source: str) -> float:
    """Compute author weight based on account metrics."""
    if source == "twitter":
        followers = event.get("user", {}).get("followers_count", 0) or 0
        verified = event.get("user", {}).get("verified", False)
        base = min(10.0, followers / 10000)
        return base * (1.5 if verified else 1.0)
    
    elif source == "reddit":
        karma = event.get("author_karma", event.get("score", 0)) or 0
        return min(10.0, karma / 10000)
    
    elif source == "telegram":
        # Telegram has limited author info
        return 1.0
    
    return 1.0


def compute_velocity(events_in_window: int, window_seconds: float = 60.0) -> float:
    """Compute event velocity (events per minute)."""
    if window_seconds <= 0:
        return 0.0
    return (events_in_window / window_seconds) * 60.0


def check_manipulation_flag(event: dict, source: str) -> bool:
    """Check for potential manipulation (Telegram only)."""
    if source != "telegram":
        return False
    
    # Simple heuristics for Telegram manipulation
    text = event.get("text", "") or ""
    
    # Check for pump signals
    pump_signals = ["pump", "guaranteed", "100x", "1000x", "insider", "secret"]
    text_lower = text.lower()
    
    for signal in pump_signals:
        if signal in text_lower:
            return True
    
    return False


# =============================================================================
# RAW EVENT VALIDATION
# =============================================================================

def validate_raw_event(event: dict, source: str) -> tuple[bool, Optional[str]]:
    """
    Validate raw event has required fields and matches asset.
    Returns (is_valid, error_message)
    """
    if not isinstance(event, dict):
        return False, "Event must be a dictionary"
    
    # Check for text field
    text = event.get("text", event.get("body", event.get("selftext", "")))
    if not text:
        return False, "Missing text content"
    
    # Check for timestamp
    ts_field = None
    for field in ["created_at", "timestamp", "date", "created_utc"]:
        if field in event:
            ts_field = event[field]
            break
    
    if ts_field is None:
        return False, "Missing timestamp"
    
    # Check for BTC asset keyword
    text_lower = text.lower()
    btc_keywords = ["btc", "bitcoin", "$btc"]
    
    has_btc = any(kw in text_lower for kw in btc_keywords)
    if not has_btc:
        return False, "No BTC keyword match"
    
    return True, None


# =============================================================================
# DATABASE INTERFACE (PostgreSQL or InMemory)
# =============================================================================

class DatabaseInterface:
    """
    Abstract database interface for event persistence.
    Implementations: PostgreSQLDatabase, InMemoryDatabase
    """
    
    def insert_raw_event(self, **kwargs) -> Optional[uuid.UUID]:
        raise NotImplementedError
    
    def insert_sentiment_event(self, **kwargs) -> Optional[uuid.UUID]:
        raise NotImplementedError
    
    def insert_risk_event(self, **kwargs) -> Optional[uuid.UUID]:
        raise NotImplementedError
    
    def insert_quality_event(self, **kwargs) -> Optional[uuid.UUID]:
        raise NotImplementedError
    
    def check_fingerprint_exists(self, fingerprint: str) -> bool:
        raise NotImplementedError
    
    def commit(self):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError


class InMemoryDatabase(DatabaseInterface):
    """In-memory database for testing (no actual PostgreSQL)."""
    
    def __init__(self):
        self.raw_events: List[dict] = []
        self.sentiment_events: List[dict] = []
        self.risk_events: List[dict] = []
        self.quality_events: List[dict] = []
        self.fingerprints: set = set()
        self._lock = threading.Lock()
    
    def insert_raw_event(self, **kwargs) -> Optional[uuid.UUID]:
        with self._lock:
            fingerprint = kwargs.get("fingerprint")
            if fingerprint and fingerprint in self.fingerprints:
                return None  # Duplicate
            
            event_id = uuid.uuid4()
            kwargs["id"] = event_id
            self.raw_events.append(kwargs)
            
            if fingerprint:
                self.fingerprints.add(fingerprint)
            
            return event_id
    
    def insert_sentiment_event(self, **kwargs) -> Optional[uuid.UUID]:
        with self._lock:
            event_id = uuid.uuid4()
            kwargs["id"] = event_id
            self.sentiment_events.append(kwargs)
            return event_id
    
    def insert_risk_event(self, **kwargs) -> Optional[uuid.UUID]:
        with self._lock:
            event_id = uuid.uuid4()
            kwargs["id"] = event_id
            self.risk_events.append(kwargs)
            return event_id
    
    def insert_quality_event(self, **kwargs) -> Optional[uuid.UUID]:
        with self._lock:
            event_id = uuid.uuid4()
            kwargs["id"] = event_id
            self.quality_events.append(kwargs)
            return event_id
    
    def check_fingerprint_exists(self, fingerprint: str) -> bool:
        with self._lock:
            return fingerprint in self.fingerprints
    
    def commit(self):
        pass  # No-op for in-memory
    
    def close(self):
        pass  # No-op for in-memory
    
    def get_counts(self) -> dict:
        with self._lock:
            return {
                "raw_events": len(self.raw_events),
                "sentiment_events": len(self.sentiment_events),
                "risk_events": len(self.risk_events),
                "quality_events": len(self.quality_events)
            }


# =============================================================================
# PIPELINE EXECUTOR
# =============================================================================

class PipelineExecutor:
    """
    Executes the locked pipeline for each event.
    
    Order:
    1. RAW_VALIDATION
    2. TIME_SYNC
    3. INSERT_RAW
    4. METRICS
    5. SENTIMENT
    6. INSERT_SENTIMENT
    7. RISK_INDICATORS
    8. INSERT_RISK
    """
    
    def __init__(self, database: DatabaseInterface, metrics: WorkerMetrics):
        self.database = database
        self.metrics = metrics
    
    def process_event(self, event: dict, source: str) -> bool:
        """
        Process a single event through the pipeline.
        Returns True if event was successfully processed.
        If any stage fails → DROP EVENT.
        """
        ingest_time = datetime.now(timezone.utc)
        
        # Stage 1: RAW VALIDATION
        try:
            is_valid, error = validate_raw_event(event, source)
            if not is_valid:
                logger.debug(f"[{source}] Raw validation failed: {error}")
                self.metrics.record_error(PipelineStage.RAW_VALIDATION)
                self.metrics.record_dropped(source)
                return False
        except Exception as e:
            logger.error(f"[{source}] Raw validation exception: {e}")
            self.metrics.record_error(PipelineStage.RAW_VALIDATION)
            self.metrics.record_dropped(source)
            return False
        
        # Extract text
        text = event.get("text", event.get("body", event.get("selftext", "")))
        
        # Extract and parse timestamp
        ts_raw = None
        for field in ["created_at", "timestamp", "date", "created_utc"]:
            if field in event:
                ts_raw = event[field]
                break
        
        event_time = parse_timestamp(ts_raw)
        if event_time is None:
            logger.debug(f"[{source}] Failed to parse timestamp: {ts_raw}")
            self.metrics.record_error(PipelineStage.TIME_SYNC)
            self.metrics.record_dropped(source)
            return False
        
        # Stage 2: TIME SYNC GUARD
        try:
            is_valid, error = validate_time_sync(event_time, ingest_time)
            if not is_valid:
                logger.debug(f"[{source}] Time sync failed: {error}")
                self.metrics.record_error(PipelineStage.TIME_SYNC)
                self.metrics.record_dropped(source)
                return False
        except Exception as e:
            logger.error(f"[{source}] Time sync exception: {e}")
            self.metrics.record_error(PipelineStage.TIME_SYNC)
            self.metrics.record_dropped(source)
            return False
        
        # Compute fingerprint for deduplication
        fingerprint = compute_fingerprint(source, text, event_time)
        
        # Check for duplicate
        if self.database.check_fingerprint_exists(fingerprint):
            logger.debug(f"[{source}] Duplicate event (fingerprint exists)")
            self.metrics.record_dropped(source)
            return False
        
        # Stage 3: INSERT RAW EVENT
        try:
            # Stage 4: METRICS (computed before insert)
            engagement_weight = compute_engagement_weight(event, source)
            author_weight = compute_author_weight(event, source)
            velocity = 0.0  # Will be updated with window context
            manipulation_flag = check_manipulation_flag(event, source)
            
            # Detect asset from text dynamically
            detected_asset = get_asset_for_text(text)
            
            raw_id = self.database.insert_raw_event(
                source=source,
                asset=detected_asset,
                event_time=event_time,
                ingest_time=ingest_time,
                text=text,
                source_reliability=SOURCE_RELIABILITY.get(source, 0.5),
                engagement_weight=engagement_weight,
                author_weight=author_weight,
                velocity=velocity,
                manipulation_flag=manipulation_flag,
                fingerprint=fingerprint,
                dropped=False
            )
            
            if raw_id is None:
                logger.debug(f"[{source}] Duplicate (fingerprint exists): {text[:50]}...")
                self.metrics.record_error(PipelineStage.INSERT_RAW)
                self.metrics.record_dropped(source)
                return False
            
            # Log successful insert with asset info
            logger.info(f"[{source}] ✅ Inserted #{raw_id} | Asset: {detected_asset} | {text[:60]}...")
                
        except Exception as e:
            logger.error(f"[{source}] Insert raw exception: {e}")
            self.metrics.record_error(PipelineStage.INSERT_RAW)
            self.metrics.record_dropped(source)
            return False
        
        # Stage 5: SENTIMENT NORMALIZATION
        try:
            sentiment = compute_sentiment(text)
            sentiment_label = sentiment["label"]
            sentiment_confidence = sentiment["confidence"]
        except Exception as e:
            logger.error(f"[{source}] Sentiment computation exception: {e}")
            self.metrics.record_error(PipelineStage.SENTIMENT)
            self.metrics.record_dropped(source)
            return False
        
        # Stage 6: INSERT SENTIMENT EVENT
        try:
            sentiment_id = self.database.insert_sentiment_event(
                event_time=event_time,
                raw_event_id=raw_id,
                sentiment_label=sentiment_label,
                confidence=sentiment_confidence
            )
            
            if sentiment_id is None:
                logger.error(f"[{source}] Insert sentiment failed")
                self.metrics.record_error(PipelineStage.INSERT_SENTIMENT)
                self.metrics.record_dropped(source)
                return False
                
        except Exception as e:
            logger.error(f"[{source}] Insert sentiment exception: {e}")
            self.metrics.record_error(PipelineStage.INSERT_SENTIMENT)
            self.metrics.record_dropped(source)
            return False
        
        # Stage 7: RISK INDICATORS COMPUTATION
        try:
            risk_indicators = compute_risk_indicators(
                sentiment_label=sentiment_label,
                sentiment_confidence=sentiment_confidence,
                text=text,
                velocity=velocity,
                source=source
            )
        except Exception as e:
            logger.error(f"[{source}] Risk computation exception: {e}")
            self.metrics.record_error(PipelineStage.RISK_INDICATORS)
            self.metrics.record_dropped(source)
            return False
        
        # Stage 8: INSERT RISK INDICATOR EVENT
        try:
            risk_id = self.database.insert_risk_event(
                event_time=event_time,
                sentiment_label=risk_indicators["sentiment_label"],
                sentiment_confidence=risk_indicators["sentiment_confidence"],
                social_overheat=risk_indicators["social_overheat"],
                panic_risk=risk_indicators["panic_risk"],
                fomo_risk=risk_indicators["fomo_risk"],
                fear_greed_index=risk_indicators["fear_greed_index"],
                fear_greed_zone=risk_indicators["fear_greed_zone"]
            )
            
            if risk_id is None:
                logger.error(f"[{source}] Insert risk failed")
                self.metrics.record_error(PipelineStage.INSERT_RISK)
                self.metrics.record_dropped(source)
                return False
                
        except Exception as e:
            logger.error(f"[{source}] Insert risk exception: {e}")
            self.metrics.record_error(PipelineStage.INSERT_RISK)
            self.metrics.record_dropped(source)
            return False
        
        # Success
        self.metrics.record_inserted(source)
        logger.debug(f"[{source}] Pipeline complete for {detected_asset}: raw={raw_id}, sentiment={sentiment_id}, risk={risk_id}")
        return True


# =============================================================================
# DATA QUALITY UPDATER
# =============================================================================

class DataQualityUpdater:
    """
    Periodically aggregates and inserts data quality events.
    """
    
    def __init__(self, database: DatabaseInterface, metrics: WorkerMetrics):
        self.database = database
        self.metrics = metrics
        self.last_update = datetime.now(timezone.utc)
        self.update_interval = 60  # seconds
    
    def should_update(self) -> bool:
        elapsed = (datetime.now(timezone.utc) - self.last_update).total_seconds()
        return elapsed >= self.update_interval
    
    def update(self):
        """Insert data quality event based on current metrics."""
        now = datetime.now(timezone.utc)
        
        # Compute quality indicators from metrics
        total_collected = sum(self.metrics.events_collected.values())
        total_dropped = sum(self.metrics.events_dropped.values())
        total_inserted = sum(self.metrics.events_inserted.values())
        
        # Overall health
        if total_collected == 0:
            overall = "degraded"
            availability = "down"
        elif total_dropped / max(1, total_collected) > 0.5:
            overall = "degraded"
            availability = "degraded"
        else:
            overall = "healthy"
            availability = "ok"
        
        # Time integrity (based on lag)
        max_lag = max(self.metrics.lag_seconds_per_source.values())
        if max_lag > 120:
            time_integrity = "critical"
        elif max_lag > 60:
            time_integrity = "unstable"
        else:
            time_integrity = "ok"
        
        # Volume assessment
        if total_inserted == 0:
            volume = "abnormally_low"
        elif total_inserted > 100:
            volume = "abnormally_high"
        else:
            volume = "normal"
        
        # Source balance
        source_counts = list(self.metrics.events_inserted.values())
        if source_counts and max(source_counts) > 3 * min(max(1, min(source_counts)), 1):
            source_balance = "imbalanced"
        else:
            source_balance = "normal"
        
        # Anomaly frequency (errors)
        total_errors = sum(self.metrics.errors_by_stage.values())
        if total_errors > 10:
            anomaly_frequency = "persistent"
        else:
            anomaly_frequency = "normal"
        
        try:
            self.database.insert_quality_event(
                event_time=now,
                overall=overall,
                availability=availability,
                time_integrity=time_integrity,
                volume=volume,
                source_balance=source_balance,
                anomaly_frequency=anomaly_frequency
            )
            self.last_update = now
            logger.debug("Data quality event inserted")
        except Exception as e:
            logger.error(f"Failed to insert data quality event: {e}")


# =============================================================================
# SOURCE COLLECTOR (Abstract)
# =============================================================================

class SourceCollector:
    """Abstract base class for source collectors."""
    
    def __init__(self, source: str):
        self.source = source
    
    def collect(self, since: Optional[datetime]) -> List[dict]:
        """Collect events since the given timestamp."""
        raise NotImplementedError


class MockSourceCollector(SourceCollector):
    """
    Mock collector that generates sample events for testing.
    In production, this would be replaced with actual crawlers.
    """
    
    def __init__(self, source: str):
        super().__init__(source)
        self._counter = 0
    
    def collect(self, since: Optional[datetime]) -> List[dict]:
        """Generate mock events (NO ACTUAL CRAWLING in this implementation)."""
        # This returns empty to avoid hallucination
        # Real implementation would use twitter_crawler, reddit_crawler, telegram_crawler
        return []


# =============================================================================
# SOURCE LOOP
# =============================================================================

class SourceLoop:
    """
    Independent loop for a single source.
    Maintains its own cursor/state and does NOT block other loops.
    """
    
    def __init__(
        self,
        source: str,
        interval: float,
        collector: SourceCollector,
        pipeline: PipelineExecutor,
        state: WorkerState,
        metrics: WorkerMetrics
    ):
        self.source = source
        self.interval = interval
        self.collector = collector
        self.pipeline = pipeline
        self.state = state
        self.metrics = metrics
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the source loop in a background thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"loop_{self.source}",
            daemon=True
        )
        self._thread.start()
        logger.info(f"[{self.source}] Loop started (interval={self.interval}s)")
    
    def stop(self):
        """Stop the source loop."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval + 5)
        logger.info(f"[{self.source}] Loop stopped")
    
    def _run_loop(self):
        """Main loop execution."""
        while self._running:
            try:
                self._run_once()
            except Exception as e:
                logger.error(f"[{self.source}] Loop error: {e}")
            
            # Sleep with interruptibility
            for _ in range(int(self.interval)):
                if not self._running:
                    break
                time.sleep(1)
    
    def _run_once(self):
        """Execute one iteration of the loop."""
        source_state = self.state.get_source_state(self.source)
        since = source_state.last_event_time
        
        # Collect events
        try:
            events = self.collector.collect(since)
            self.metrics.record_success(self.source)
        except Exception as e:
            logger.error(f"[{self.source}] Collector error: {e}")
            return
        
        if not events:
            logger.debug(f"[{self.source}] No events collected")
            return
        
        logger.info(f"[{self.source}] Collected {len(events)} events")
        
        # Track latest event time
        latest_event_time = since
        
        # Track processing stats
        inserted_count = 0
        duplicate_count = 0
        
        # Process each event
        for event in events:
            self.metrics.record_collected(self.source)
            
            success = self.pipeline.process_event(event, self.source)
            
            if success:
                inserted_count += 1
                # Update latest event time
                ts_raw = None
                for field in ["created_at", "timestamp", "date", "created_utc"]:
                    if field in event:
                        ts_raw = event[field]
                        break
                
                event_time = parse_timestamp(ts_raw)
                if event_time and (latest_event_time is None or event_time > latest_event_time):
                    latest_event_time = event_time
            else:
                duplicate_count += 1
        
        # Log summary
        logger.info(f"[{self.source}] 📊 Summary: {inserted_count} inserted, {duplicate_count} duplicates/dropped")
        
        # Update state with latest event time
        if latest_event_time and latest_event_time != since:
            self.state.update_source_state(
                self.source,
                last_event_time=latest_event_time
            )
        
        # Update lag metric
        if latest_event_time:
            lag = (datetime.now(timezone.utc) - latest_event_time).total_seconds()
            self.metrics.update_lag(self.source, lag)


# =============================================================================
# BACKGROUND WORKER
# =============================================================================

class BackgroundWorker:
    """
    Main background worker that orchestrates all source loops.
    
    Supports:
    - Graceful shutdown
    - Restart without data corruption
    - Independent source loops
    """
    
    def __init__(
        self,
        database: Optional[DatabaseInterface] = None,
        state_file: Optional[Path] = None
    ):
        self.database = database or InMemoryDatabase()
        self.state = WorkerState(state_file or STATE_FILE_PATH)
        self.metrics = WorkerMetrics()
        
        self.pipeline = PipelineExecutor(self.database, self.metrics)
        self.quality_updater = DataQualityUpdater(self.database, self.metrics)
        
        # Create source loops
        self.loops: Dict[str, SourceLoop] = {}
        self._setup_loops()
        
        self._running = False
        self._shutdown_event = threading.Event()
    
    def _setup_loops(self):
        """Setup source loops with their collectors."""
        sources = [
            ("twitter", TWITTER_INTERVAL),
            ("telegram", TELEGRAM_INTERVAL),
            ("reddit", REDDIT_INTERVAL)
        ]
        
        for source, interval in sources:
            collector = MockSourceCollector(source)
            loop = SourceLoop(
                source=source,
                interval=interval,
                collector=collector,
                pipeline=self.pipeline,
                state=self.state,
                metrics=self.metrics
            )
            self.loops[source] = loop
    
    def start(self):
        """Start the background worker."""
        logger.info("Background worker starting...")
        self._running = True
        self._shutdown_event.clear()
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Start all source loops
        for loop in self.loops.values():
            loop.start()
        
        logger.info("Background worker started")
        
        # Main loop for data quality updates
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.quality_updater.should_update():
                    self.quality_updater.update()
                
                # Periodic state save
                self.state.save_state()
                
            except Exception as e:
                logger.error(f"Main loop error: {e}")
            
            # Sleep with shutdown check
            self._shutdown_event.wait(timeout=10)
    
    def stop(self):
        """Stop the background worker gracefully."""
        logger.info("Background worker stopping...")
        self._running = False
        self._shutdown_event.set()
        
        # Stop all source loops
        for loop in self.loops.values():
            loop.stop()
        
        # Final state save
        self.state.save_state()
        
        # Flush database
        self.database.commit()
        self.database.close()
        
        logger.info("Background worker stopped")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.stop()
    
    def get_metrics(self) -> dict:
        """Get current metrics (for observability only)."""
        return self.metrics.to_dict()


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_worker(
    database: Optional[DatabaseInterface] = None,
    state_file: Optional[Path] = None
) -> BackgroundWorker:
    """Create a new background worker instance."""
    return BackgroundWorker(database=database, state_file=state_file)


def create_in_memory_worker() -> BackgroundWorker:
    """Create a worker with in-memory database (for testing)."""
    return BackgroundWorker(
        database=InMemoryDatabase(),
        state_file=Path("test_worker_state.json")
    )


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("Background Worker / Scheduler for Social Sentiment Data Service")
    print("=" * 70)
    print()
    print("ABSOLUTE RULES:")
    print("  - NO mocking data")
    print("  - NO hallucination")
    print("  - NO paid APIs")
    print("  - NO trading signals")
    print("  - If any stage fails → DROP EVENT and continue")
    print()
    print("Starting worker with in-memory database...")
    print()
    
    worker = create_in_memory_worker()
    
    try:
        worker.start()
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    finally:
        worker.stop()
        
        print()
        print("Final Metrics:")
        import json
        print(json.dumps(worker.get_metrics(), indent=2))
