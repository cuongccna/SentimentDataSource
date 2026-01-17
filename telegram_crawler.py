"""
Telegram Social Data Crawler for Crypto BotTrading
Purpose: Collect and normalize RAW Telegram messages related to crypto assets.
Used ONLY for panic detection, abnormal activity, and manipulation warnings.

Telegram data MUST NOT be used as a bullish confirmation source.
This module is DATA PROVIDER ONLY.
BotTrading Core owns all decisions.

NO SENTIMENT. NO NLP. NO SEMANTIC GUESSING.
"""

import re
import time
import json
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from collections import deque, defaultdict
from abc import ABC, abstractmethod


# SOURCE RELIABILITY (FIXED - DO NOT CHANGE)
SOURCE_RELIABILITY = 0.3

# ASSET FILTER PATTERNS (MANDATORY)
ASSET_PATTERNS = [
    re.compile(r'\$BTC\b', re.IGNORECASE),
    re.compile(r'#BTC\b', re.IGNORECASE),
    re.compile(r'\bbitcoin\b', re.IGNORECASE)
]

# DEFAULT MANIPULATION THRESHOLD
DEFAULT_MANIPULATION_THRESHOLD = 3

# Time windows for velocity and manipulation detection
VELOCITY_SHORT_WINDOW_MINUTES = 10
VELOCITY_LONG_WINDOW_MINUTES = 60
MANIPULATION_WINDOW_MINUTES = 10


@dataclass
class TelegramMessage:
    """Raw Telegram message data."""
    message_id: int
    channel_id: str
    text: str
    timestamp: datetime
    is_forwarded: bool = False
    forward_source: Optional[str] = None
    is_bot: bool = False
    author_id: Optional[str] = None


@dataclass
class NormalizedTelegramRecord:
    """Normalized output record."""
    source: str
    asset: str
    timestamp: str
    text: str
    velocity: float
    manipulation_flag: bool
    source_reliability: float
    
    def to_dict(self) -> dict:
        """Convert to output dictionary format."""
        return {
            "source": self.source,
            "asset": self.asset,
            "timestamp": self.timestamp,
            "text": self.text,
            "metrics": {
                "velocity": self.velocity,
                "manipulation_flag": self.manipulation_flag
            },
            "source_reliability": self.source_reliability
        }


class MessageTracker:
    """
    Track messages for velocity calculation.
    
    Velocity = messages_10min / avg_messages_1h
    WHERE:
    - messages_10min = valid asset-related messages in last 10 minutes
    - avg_messages_1h = average messages per 10-min window over previous 1 hour
    """
    
    def __init__(self):
        # Store (timestamp, channel_id) tuples
        self.messages: deque = deque(maxlen=10000)
    
    def add_message(self, timestamp: datetime, channel_id: str):
        """Record a message timestamp."""
        self.messages.append((timestamp, channel_id))
    
    def get_messages_in_window(self, channel_id: str, window_minutes: int) -> int:
        """Get count of messages in a channel within the last N minutes."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=window_minutes)
        
        count = 0
        for ts, ch_id in self.messages:
            if ch_id == channel_id and ts >= cutoff:
                count += 1
        return count
    
    def compute_velocity(self, channel_id: str) -> float:
        """
        Compute velocity:
        messages_10min / avg_messages_1h
        
        WHERE:
        - messages_10min = messages in last 10 minutes
        - avg_messages_1h = average per 10-min window over previous 1 hour
          (1 hour = 6 windows of 10 minutes)
        """
        messages_10min = self.get_messages_in_window(channel_id, VELOCITY_SHORT_WINDOW_MINUTES)
        messages_1h = self.get_messages_in_window(channel_id, VELOCITY_LONG_WINDOW_MINUTES)
        
        # If no messages at all, return default velocity of 1.0
        if messages_1h == 0 and messages_10min == 0:
            return 1.0
        
        # 1 hour = 6 windows of 10 minutes
        num_windows = 6
        avg_messages_per_window = messages_1h / num_windows if messages_1h > 0 else 1
        
        if avg_messages_per_window <= 0:
            return 1.0  # Default velocity
        
        return messages_10min / avg_messages_per_window


class ManipulationDetector:
    """
    Detect potential manipulation through phrase repetition.
    
    IF same phrase_fingerprint appears >= N times within 5-10 minutes:
    → manipulation_flag = true
    """
    
    def __init__(self, threshold: int = DEFAULT_MANIPULATION_THRESHOLD, window_minutes: int = MANIPULATION_WINDOW_MINUTES):
        """
        Initialize detector.
        
        Args:
            threshold: Number of repetitions to trigger flag (default: 3)
            window_minutes: Time window for detection (default: 10 minutes)
        """
        self.threshold = threshold
        self.window_minutes = window_minutes
        # channel_id -> list of (timestamp, fingerprint)
        self.history: dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
    
    def _compute_fingerprint(self, text: str) -> str:
        """
        Compute phrase fingerprint.
        
        Normalized text:
        - lowercase
        - remove numbers
        - remove punctuation
        """
        if not text:
            return ""
        
        # Lowercase
        normalized = text.lower()
        
        # Remove numbers
        normalized = re.sub(r'\d+', '', normalized)
        
        # Remove punctuation (keep only letters and spaces)
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Normalize whitespace
        normalized = ' '.join(normalized.split())
        
        # Create hash for efficient comparison
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def _clean_old_entries(self, channel_id: str, now: datetime):
        """Remove entries outside the time window."""
        cutoff = now - timedelta(minutes=self.window_minutes)
        
        history = self.history[channel_id]
        while history and history[0][0] < cutoff:
            history.popleft()
    
    def check_manipulation(self, text: str, channel_id: str, timestamp: datetime) -> bool:
        """
        Check if message indicates potential manipulation.
        
        Returns True if same phrase appears >= threshold times in window.
        """
        fingerprint = self._compute_fingerprint(text)
        
        if not fingerprint:
            return False
        
        # Clean old entries
        self._clean_old_entries(channel_id, timestamp)
        
        # Add current message
        self.history[channel_id].append((timestamp, fingerprint))
        
        # Count occurrences of this fingerprint
        count = sum(1 for _, fp in self.history[channel_id] if fp == fingerprint)
        
        return count >= self.threshold
    
    def clear_channel(self, channel_id: str):
        """Clear history for a channel."""
        if channel_id in self.history:
            self.history[channel_id].clear()


def contains_asset_keyword(text: str) -> bool:
    """
    Check if text contains at least one of:
    - "$BTC"
    - "#BTC"
    - "bitcoin" (case-insensitive)
    """
    if not text:
        return False
    for pattern in ASSET_PATTERNS:
        if pattern.search(text):
            return True
    return False


def timestamp_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO-8601 format."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_unix_timestamp(utc_timestamp: float) -> datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.fromtimestamp(utc_timestamp, tz=timezone.utc)


class TelegramDataSource(ABC):
    """Abstract base class for Telegram data sources."""
    
    @abstractmethod
    def fetch_messages(self, channel_id: str, limit: int) -> list[dict]:
        """Fetch recent messages from a channel."""
        pass
    
    @abstractmethod
    def get_channel_info(self, channel_id: str) -> Optional[dict]:
        """Get channel information."""
        pass


class PublicChannelScraper(TelegramDataSource):
    """
    Scraper for public Telegram channels.
    
    Uses public web preview (t.me/s/<channel>) for data access.
    No API key required.
    """
    
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        self.request_delay = 2.0  # Rate limiting
        self.last_request_time = 0.0
    
    def _rate_limit(self):
        """Enforce rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        self.last_request_time = time.time()
    
    def fetch_messages(self, channel_id: str, limit: int = 50) -> list[dict]:
        """
        Fetch messages from a public channel.
        
        Note: This is a placeholder for actual scraping implementation.
        In production, implement actual HTTP scraping of t.me/s/<channel>
        """
        # Placeholder - in production, implement actual scraping
        return []
    
    def get_channel_info(self, channel_id: str) -> Optional[dict]:
        """Get public channel information."""
        return None


class TelegramCrawler:
    """
    Telegram Social Data Crawler.
    
    Collects and normalizes public Telegram messages for:
    - Panic detection
    - Abnormal social activity
    - Manipulation warnings
    
    NOT for bullish confirmation.
    """
    
    def __init__(
        self,
        channels: Optional[list[str]] = None,
        manipulation_threshold: int = DEFAULT_MANIPULATION_THRESHOLD,
        data_source: Optional[TelegramDataSource] = None
    ):
        """
        Initialize crawler.
        
        Args:
            channels: List of public channel/group IDs to monitor
            manipulation_threshold: Repetition threshold for manipulation flag
            data_source: Data source implementation (for testing)
        """
        self.channels = channels or []
        self.message_tracker = MessageTracker()
        self.manipulation_detector = ManipulationDetector(threshold=manipulation_threshold)
        self.data_source = data_source or PublicChannelScraper()
    
    def _validate_message(self, message_data: dict) -> bool:
        """
        Validate message meets criteria:
        - Has text
        - Has timestamp
        - Is public (not from private source)
        - Contains asset keyword
        - Not spam forwarded from unknown source
        - Not detectable bot message
        """
        # Check text exists and has content
        text = message_data.get("text", "")
        if not text or not text.strip():
            return False
        
        # Check timestamp exists
        if not message_data.get("timestamp") and not message_data.get("date"):
            return False
        
        # Check is public
        if not message_data.get("is_public", True):
            return False
        
        # Check for spam forwarded messages with unknown source
        if message_data.get("is_forwarded", False):
            if not message_data.get("forward_source"):
                return False
        
        # Exclude bot messages if detectable
        if message_data.get("is_bot", False):
            return False
        
        # Check contains asset keyword
        if not contains_asset_keyword(text):
            return False
        
        return True
    
    def _parse_timestamp(self, message_data: dict) -> Optional[datetime]:
        """Extract timestamp from message data."""
        # Try various timestamp formats
        ts = message_data.get("timestamp")
        if ts:
            if isinstance(ts, datetime):
                return ts
            if isinstance(ts, (int, float)):
                return parse_unix_timestamp(ts)
            if isinstance(ts, str):
                try:
                    return datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except ValueError:
                    pass
        
        # Try 'date' field (common in Telegram API)
        date = message_data.get("date")
        if date:
            if isinstance(date, (int, float)):
                return parse_unix_timestamp(date)
        
        return None
    
    def normalize_message(self, message_data: dict, channel_id: str) -> Optional[NormalizedTelegramRecord]:
        """
        Normalize a Telegram message to output format.
        
        Returns None if record should be dropped.
        """
        # Validate message
        if not self._validate_message(message_data):
            return None
        
        # Parse timestamp - DROP if missing
        timestamp = self._parse_timestamp(message_data)
        if timestamp is None:
            return None
        
        # Get text - DROP if missing
        text = message_data.get("text", "")
        if not text:
            return None
        
        # Check asset keyword - DROP if missing
        if not contains_asset_keyword(text):
            return None
        
        # Track message for velocity
        self.message_tracker.add_message(timestamp, channel_id)
        
        # Compute velocity
        velocity = self.message_tracker.compute_velocity(channel_id)
        
        # Check for manipulation
        manipulation_flag = self.manipulation_detector.check_manipulation(
            text, channel_id, timestamp
        )
        
        return NormalizedTelegramRecord(
            source="telegram",
            asset="BTC",
            timestamp=timestamp_to_iso(timestamp),
            text=text,
            velocity=velocity,
            manipulation_flag=manipulation_flag,
            source_reliability=SOURCE_RELIABILITY
        )
    
    def process_messages(self, messages: list[dict], channel_id: str) -> list[dict]:
        """
        Process a batch of messages from a channel.
        
        Args:
            messages: List of raw message data
            channel_id: Channel/group ID
        
        Returns:
            List of normalized record dictionaries
        """
        records = []
        
        for message_data in messages:
            normalized = self.normalize_message(message_data, channel_id)
            if normalized:
                records.append(normalized.to_dict())
        
        return records
    
    def crawl_channel(self, channel_id: str, limit: int = 50) -> list[dict]:
        """
        Crawl a single channel for messages.
        
        Args:
            channel_id: Channel/group ID
            limit: Maximum messages to fetch
        
        Returns:
            List of normalized record dictionaries
        """
        messages = self.data_source.fetch_messages(channel_id, limit)
        return self.process_messages(messages, channel_id)
    
    def crawl_all(self, limit: int = 50) -> list[dict]:
        """
        Crawl all configured channels.
        
        Args:
            limit: Maximum messages per channel
        
        Returns:
            List of normalized record dictionaries
        """
        all_records = []
        
        for channel_id in self.channels:
            records = self.crawl_channel(channel_id, limit)
            all_records.extend(records)
        
        return all_records
    
    def set_manipulation_threshold(self, threshold: int):
        """Update manipulation detection threshold."""
        self.manipulation_detector.threshold = threshold


def create_crawler(
    channels: Optional[list[str]] = None,
    manipulation_threshold: int = DEFAULT_MANIPULATION_THRESHOLD
) -> TelegramCrawler:
    """Factory function to create a Telegram crawler."""
    return TelegramCrawler(channels, manipulation_threshold)


if __name__ == "__main__":
    # Example usage with sample data (no live crawling)
    crawler = create_crawler(channels=["btc_signals", "crypto_news"])
    
    # Sample message data (simulating Telegram messages)
    sample_messages = [
        {
            "message_id": 1001,
            "text": "ALERT! Bitcoin dumping hard! $BTC dropping fast!",
            "timestamp": datetime.now(timezone.utc) - timedelta(minutes=2),
            "is_public": True,
            "is_forwarded": False,
            "is_bot": False
        },
        {
            "message_id": 1002,
            "text": "Panic selling Bitcoin everywhere! Exit your positions!",
            "timestamp": datetime.now(timezone.utc) - timedelta(minutes=1),
            "is_public": True,
            "is_forwarded": False,
            "is_bot": False
        },
        {
            "message_id": 1003,
            "text": "BTC crash incoming! Sell now! Bitcoin going to zero!",
            "timestamp": datetime.now(timezone.utc),
            "is_public": True,
            "is_forwarded": False,
            "is_bot": False
        },
        {
            "message_id": 1004,
            "text": "This message has no crypto keywords",  # Should be dropped
            "timestamp": datetime.now(timezone.utc),
            "is_public": True,
            "is_forwarded": False,
            "is_bot": False
        }
    ]
    
    results = crawler.process_messages(sample_messages, "btc_signals")
    
    print(f"Processed {len(results)} records:\n")
    for record in results:
        print(json.dumps(record, indent=2, default=str))
        print()
    
    # Demonstrate manipulation detection
    print("\n=== Manipulation Detection Demo ===\n")
    
    manipulation_messages = [
        {
            "message_id": 2001,
            "text": "BUY BTC NOW! Bitcoin to 100k! Don't miss out!",
            "timestamp": datetime.now(timezone.utc) - timedelta(minutes=5),
            "is_public": True
        },
        {
            "message_id": 2002,
            "text": "BUY BTC NOW! Bitcoin to 100k! Don't miss out!",  # Duplicate
            "timestamp": datetime.now(timezone.utc) - timedelta(minutes=4),
            "is_public": True
        },
        {
            "message_id": 2003,
            "text": "BUY BTC NOW! Bitcoin to 100k! Don't miss out!",  # Third duplicate - triggers flag
            "timestamp": datetime.now(timezone.utc) - timedelta(minutes=3),
            "is_public": True
        }
    ]
    
    manipulation_results = crawler.process_messages(manipulation_messages, "spam_channel")
    
    for record in manipulation_results:
        print(f"Manipulation Flag: {record['metrics']['manipulation_flag']}")
        print(json.dumps(record, indent=2, default=str))
        print()
