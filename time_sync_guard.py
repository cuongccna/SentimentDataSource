"""
Time Synchronization & Validation Guard for Social Data
Purpose: Ensure ALL social data events have accurate, consistent,
and BotTrading-safe timestamps.

Protects BotTrading from:
- Time drift
- Lag
- Out-of-order events
- Replayed/duplicate messages

This module NEVER alters timestamps.
This module NEVER fills missing time.
This module NEVER decides trading actions.

BotTrading assumes all timestamps AFTER this guard are trustworthy.
"""

import re
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from collections import deque, defaultdict


# ============================================================================
# TIME STANDARDS (FIXED - DO NOT CHANGE)
# ============================================================================

# Maximum allowed event delay by source (seconds)
MAX_EVENT_DELAY = {
    "twitter": 15,
    "reddit": 120,
    "telegram": 30
}

# Out-of-order tolerance by source (seconds)
OUT_OF_ORDER_TOLERANCE = {
    "twitter": 5,
    "reddit": 60,
    "telegram": 10
}

# Duplicate detection rolling window by source (seconds)
DUPLICATE_WINDOW = {
    "twitter": 300,    # 5 minutes
    "reddit": 1800,    # 30 minutes
    "telegram": 600    # 10 minutes
}

# ISO-8601 UTC pattern with second precision
ISO8601_PATTERN = re.compile(
    r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'
)


@dataclass
class TimeQualityMetrics:
    """
    Internal metrics for monitoring/logging ONLY.
    NOT for BotTrading decisions.
    """
    dropped_future_events: int = 0
    dropped_late_events: int = 0
    dropped_out_of_order: int = 0
    dropped_duplicates: int = 0
    dropped_invalid_format: int = 0
    dropped_missing_timestamp: int = 0
    total_processed: int = 0
    total_passed: int = 0
    
    def to_dict(self) -> dict:
        return {
            "dropped_future_events": self.dropped_future_events,
            "dropped_late_events": self.dropped_late_events,
            "dropped_out_of_order": self.dropped_out_of_order,
            "dropped_duplicates": self.dropped_duplicates,
            "dropped_invalid_format": self.dropped_invalid_format,
            "dropped_missing_timestamp": self.dropped_missing_timestamp,
            "total_processed": self.total_processed,
            "total_passed": self.total_passed
        }


class EventTracker:
    """
    Track last seen event time per source + asset.
    Used for out-of-order detection.
    """
    
    def __init__(self):
        # Key: (source, asset) -> last_seen_timestamp
        self.last_seen: dict[tuple[str, str], datetime] = {}
    
    def get_last_seen(self, source: str, asset: str) -> Optional[datetime]:
        """Get last seen event time for source + asset."""
        return self.last_seen.get((source, asset))
    
    def update(self, source: str, asset: str, timestamp: datetime):
        """Update last seen time if newer."""
        key = (source, asset)
        current = self.last_seen.get(key)
        if current is None or timestamp > current:
            self.last_seen[key] = timestamp
    
    def clear(self):
        """Clear all tracking."""
        self.last_seen.clear()


class DuplicateDetector:
    """
    Detect duplicate/replayed events using fingerprinting.
    
    fingerprint = hash(source + asset + timestamp + normalized_text)
    """
    
    def __init__(self):
        # source -> deque of (timestamp, fingerprint)
        self.seen: dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
    
    def _compute_fingerprint(self, source: str, asset: str, timestamp: str, text: str) -> str:
        """
        Compute event fingerprint.
        
        fingerprint = hash(source + asset + timestamp + normalized_text)
        """
        # Normalize text: lowercase, strip whitespace
        normalized_text = text.lower().strip() if text else ""
        
        data = f"{source}|{asset}|{timestamp}|{normalized_text}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def _clean_old_entries(self, source: str, current_time: datetime):
        """Remove entries outside the rolling window."""
        window_seconds = DUPLICATE_WINDOW.get(source, 300)
        cutoff = current_time - timedelta(seconds=window_seconds)
        
        while self.seen[source] and self.seen[source][0][0] < cutoff:
            self.seen[source].popleft()
    
    def is_duplicate(
        self,
        source: str,
        asset: str,
        timestamp: str,
        text: str,
        current_time: datetime
    ) -> bool:
        """
        Check if event is a duplicate.
        
        Returns True if fingerprint already seen within rolling window.
        """
        # Clean old entries first
        self._clean_old_entries(source, current_time)
        
        fingerprint = self._compute_fingerprint(source, asset, timestamp, text)
        
        # Check if already seen
        for _, fp in self.seen[source]:
            if fp == fingerprint:
                return True
        
        # Not a duplicate - record it
        self.seen[source].append((current_time, fingerprint))
        return False
    
    def clear(self, source: Optional[str] = None):
        """Clear duplicate history."""
        if source:
            if source in self.seen:
                self.seen[source].clear()
        else:
            self.seen.clear()


def parse_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    Parse ISO-8601 UTC timestamp with second precision.
    
    Returns None if:
    - Missing
    - Not ISO-8601
    - Not UTC
    - Precision coarser than seconds
    """
    if not timestamp_str:
        return None
    
    # Check format: YYYY-MM-DDTHH:MM:SSZ
    if not ISO8601_PATTERN.match(timestamp_str):
        # Try to parse and convert to UTC if timezone info exists
        try:
            # Handle various ISO formats with timezone
            if timestamp_str.endswith('Z'):
                ts_clean = timestamp_str.replace('Z', '+00:00')
                # Check for second precision - must have HH:MM:SS pattern
                if not _has_second_precision(timestamp_str):
                    return None
                dt = datetime.fromisoformat(ts_clean)
            elif '+' in timestamp_str or (timestamp_str.count('-') > 2 and 'T' in timestamp_str):
                # Check for second precision
                if not _has_second_precision(timestamp_str):
                    return None
                dt = datetime.fromisoformat(timestamp_str)
            else:
                return None
            
            # Convert to UTC
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            else:
                return None  # No timezone info - cannot verify
            
            return dt
        except (ValueError, TypeError):
            return None
    
    # Parse standard format
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def _has_second_precision(timestamp_str: str) -> bool:
    """Check if timestamp has second-level precision (HH:MM:SS)."""
    # Look for time pattern with seconds: T followed by HH:MM:SS
    import re
    time_pattern = re.compile(r'T\d{2}:\d{2}:\d{2}')
    return bool(time_pattern.search(timestamp_str))


def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO-8601 UTC with second precision."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_current_utc() -> datetime:
    """Get current UTC time."""
    return datetime.now(timezone.utc)


class TimeSyncGuard:
    """
    Time Synchronization & Validation Guard.
    
    Ensures all social data events have accurate, consistent,
    and BotTrading-safe timestamps.
    
    Any event failing this guard MUST NOT reach BotTrading.
    """
    
    def __init__(self, time_provider: Optional[callable] = None):
        """
        Initialize the guard.
        
        Args:
            time_provider: Optional callable that returns current UTC time.
                          Defaults to datetime.now(timezone.utc).
                          Useful for testing.
        """
        self.event_tracker = EventTracker()
        self.duplicate_detector = DuplicateDetector()
        self.metrics = TimeQualityMetrics()
        self._get_current_time = time_provider or get_current_utc
    
    def _validate_record_structure(self, record: dict) -> bool:
        """Validate record has required fields."""
        required = ["source", "asset", "timestamp", "text"]
        return all(field in record for field in required)
    
    def _check_timestamp_format(self, timestamp: str) -> Optional[datetime]:
        """
        Validate and parse timestamp.
        
        Returns parsed datetime or None if invalid.
        """
        return parse_timestamp(timestamp)
    
    def _check_future_event(self, event_time: datetime, current_time: datetime) -> bool:
        """
        Check if event is in the future.
        
        Returns True if event is in the future (should drop).
        """
        return event_time > current_time
    
    def _check_late_event(
        self,
        event_time: datetime,
        current_time: datetime,
        source: str
    ) -> bool:
        """
        Check if event is too old (exceeds max_event_delay).
        
        event_delay = current_time - event_time
        Returns True if too late (should drop).
        """
        max_delay = MAX_EVENT_DELAY.get(source, 60)
        event_delay = (current_time - event_time).total_seconds()
        
        return event_delay > max_delay
    
    def _check_out_of_order(
        self,
        event_time: datetime,
        source: str,
        asset: str
    ) -> bool:
        """
        Check if event is out of order.
        
        Returns True if out of order (should drop).
        """
        last_seen = self.event_tracker.get_last_seen(source, asset)
        
        if last_seen is None:
            return False
        
        tolerance = OUT_OF_ORDER_TOLERANCE.get(source, 10)
        threshold = last_seen - timedelta(seconds=tolerance)
        
        return event_time < threshold
    
    def validate_record(self, record: dict) -> Optional[dict]:
        """
        Validate a single record.
        
        Returns the record (passthrough) if valid, None if dropped.
        """
        self.metrics.total_processed += 1
        
        # Check structure
        if not self._validate_record_structure(record):
            self.metrics.dropped_missing_timestamp += 1
            return None
        
        source = record.get("source", "").lower()
        asset = record.get("asset", "")
        timestamp_str = record.get("timestamp", "")
        text = record.get("text", "")
        
        # Check timestamp exists
        if not timestamp_str:
            self.metrics.dropped_missing_timestamp += 1
            return None
        
        # Validate source
        if source not in MAX_EVENT_DELAY:
            self.metrics.dropped_invalid_format += 1
            return None
        
        # Parse and validate timestamp format
        event_time = self._check_timestamp_format(timestamp_str)
        if event_time is None:
            self.metrics.dropped_invalid_format += 1
            return None
        
        # Get current time
        current_time = self._get_current_time()
        
        # Check future event
        if self._check_future_event(event_time, current_time):
            self.metrics.dropped_future_events += 1
            return None
        
        # Check late event
        if self._check_late_event(event_time, current_time, source):
            self.metrics.dropped_late_events += 1
            return None
        
        # Check out of order
        if self._check_out_of_order(event_time, source, asset):
            self.metrics.dropped_out_of_order += 1
            return None
        
        # Check duplicate
        if self.duplicate_detector.is_duplicate(
            source, asset, timestamp_str, text, current_time
        ):
            self.metrics.dropped_duplicates += 1
            return None
        
        # Update last seen time
        self.event_tracker.update(source, asset, event_time)
        
        # PASSTHROUGH - no modification
        self.metrics.total_passed += 1
        return record
    
    def validate_batch(self, records: list[dict]) -> list[dict]:
        """
        Validate a batch of records.
        
        Returns list of valid records (passthrough).
        """
        valid_records = []
        
        for record in records:
            validated = self.validate_record(record)
            if validated is not None:
                valid_records.append(validated)
        
        return valid_records
    
    def get_metrics(self) -> dict:
        """Get time quality metrics for monitoring/logging."""
        return self.metrics.to_dict()
    
    def reset_metrics(self):
        """Reset metrics counters."""
        self.metrics = TimeQualityMetrics()
    
    def reset_tracking(self):
        """Reset all tracking state."""
        self.event_tracker.clear()
        self.duplicate_detector.clear()
        self.reset_metrics()


def create_guard(time_provider: Optional[callable] = None) -> TimeSyncGuard:
    """Factory function to create a Time Sync Guard."""
    return TimeSyncGuard(time_provider)


if __name__ == "__main__":
    import json
    
    # Create guard with current time
    guard = create_guard()
    
    # Get current time for sample data
    now = get_current_utc()
    
    # Sample records from crawlers
    sample_records = [
        # Valid Twitter record (within 15 seconds)
        {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": format_timestamp(now - timedelta(seconds=5)),
            "text": "$BTC breaking resistance!",
            "metrics": {
                "engagement_weight": 5.2,
                "author_weight": 8.1,
                "velocity": 2.5
            },
            "source_reliability": 0.5
        },
        # Valid Reddit record (within 120 seconds)
        {
            "source": "reddit",
            "asset": "BTC",
            "timestamp": format_timestamp(now - timedelta(seconds=60)),
            "text": "Bitcoin discussion thread",
            "metrics": {
                "engagement_weight": 4.0,
                "author_weight": 6.5,
                "velocity": 1.2
            },
            "source_reliability": 0.7
        },
        # Late Twitter (should drop - > 15 seconds)
        {
            "source": "twitter",
            "asset": "BTC",
            "timestamp": format_timestamp(now - timedelta(seconds=30)),
            "text": "$BTC old tweet",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        },
        # Future event (should drop)
        {
            "source": "telegram",
            "asset": "BTC",
            "timestamp": format_timestamp(now + timedelta(seconds=10)),
            "text": "Future message bitcoin",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.3
        },
        # Missing timestamp (should drop)
        {
            "source": "twitter",
            "asset": "BTC",
            "text": "No timestamp $BTC",
            "metrics": {"velocity": 1.0},
            "source_reliability": 0.5
        }
    ]
    
    print("=== Time Sync Guard Demo ===\n")
    print(f"Current UTC: {format_timestamp(now)}\n")
    
    valid_records = guard.validate_batch(sample_records)
    
    print(f"Input records: {len(sample_records)}")
    print(f"Valid records: {len(valid_records)}\n")
    
    print("Valid records passed through:")
    for record in valid_records:
        print(json.dumps(record, indent=2))
        print()
    
    print("\nTime Quality Metrics:")
    print(json.dumps(guard.get_metrics(), indent=2))
