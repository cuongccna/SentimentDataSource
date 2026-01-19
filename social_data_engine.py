"""
Social Data Collection & Normalization Engine for Crypto BotTrading
Purpose: Collect, compute, and normalize RAW social data from Twitter/X, Reddit, and Telegram.
This engine produces STRUCTURED INPUT DATA ONLY for BotTrading.

This module is DATA PROVIDER ONLY.
BotTrading owns ALL decision logic.
"""

import re
import math
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass


# SOURCE RELIABILITY (FIXED, DO NOT CHANGE)
SOURCE_RELIABILITY = {
    "twitter": 0.5,
    "reddit": 0.7,
    "telegram": 0.3
}

# Import asset config for dynamic keyword detection
from asset_config import get_asset_config, contains_tracked_asset, detect_asset as detect_asset_from_text

# Legacy ASSET FILTER PATTERNS (fallback)
ASSET_PATTERNS = [
    re.compile(r'\$BTC\b', re.IGNORECASE),
    re.compile(r'#BTC\b', re.IGNORECASE),
    re.compile(r'\bbitcoin\b', re.IGNORECASE)
]


@dataclass
class NormalizedRecord:
    """Normalized sentiment input structure."""
    source: str
    asset: str
    timestamp: str
    text: str
    engagement_weight: Optional[float]
    author_weight: Optional[float]
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
                "engagement_weight": self.engagement_weight,
                "author_weight": self.author_weight,
                "velocity": self.velocity,
                "manipulation_flag": self.manipulation_flag
            },
            "source_reliability": self.source_reliability
        }


def contains_asset_keyword(text: str) -> bool:
    """
    Check if text contains any tracked crypto asset keyword.
    Uses dynamic asset configuration from database.
    """
    if not text:
        return False
    
    # Use dynamic asset config
    try:
        return contains_tracked_asset(text)
    except Exception:
        # Fallback to legacy patterns
        for pattern in ASSET_PATTERNS:
            if pattern.search(text):
                return True
        return False


def get_detected_asset(text: str) -> str:
    """
    Detect which asset the text is about.
    Returns the highest priority matching asset, or 'BTC' as default.
    """
    if not text:
        return "BTC"
    
    try:
        asset = detect_asset_from_text(text)
        return asset if asset else "BTC"
    except Exception:
        return "BTC"


def safe_log(value: float) -> float:
    """Compute log(1 + value) safely."""
    return math.log(1 + max(0, value))


class BaseCollector(ABC):
    """Base class for social data collectors."""
    
    @abstractmethod
    def validate_record(self, record: dict) -> bool:
        """Validate that record has all required fields."""
        pass
    
    @abstractmethod
    def compute_engagement_weight(self, record: dict) -> Optional[float]:
        """Compute engagement weight for the record."""
        pass
    
    @abstractmethod
    def compute_author_weight(self, record: dict) -> Optional[float]:
        """Compute author weight for the record."""
        pass
    
    @abstractmethod
    def normalize(self, record: dict, velocity: float, manipulation_flag: bool = False) -> Optional[NormalizedRecord]:
        """Normalize a record to standard output format."""
        pass


class TwitterCollector(BaseCollector):
    """
    Twitter/X Collection Engine
    
    SOURCE SELECTION:
    - Public tweets only
    - Exclude private accounts
    - Exclude ads
    - Exclude replies with zero engagement
    
    ENGAGEMENT FILTER:
    - like + retweet + reply > 0
    
    ROLE:
    - Early narrative detection
    - FOMO / hype detection
    - Panic onset detection
    """
    
    def validate_record(self, record: dict) -> bool:
        """
        Validate Twitter record.
        
        Required fields:
        - text
        - timestamp
        - like_count
        - retweet_count
        - reply_count
        - author.followers_count
        - author.is_private (must be False)
        - is_ad (must be False)
        """
        if not isinstance(record, dict):
            return False
        
        # Check text exists and contains asset keyword
        text = record.get("text")
        if not text or not contains_asset_keyword(text):
            return False
        
        # Check timestamp
        if not record.get("timestamp"):
            return False
        
        # Check engagement fields exist
        like = record.get("like_count")
        retweet = record.get("retweet_count")
        reply = record.get("reply_count")
        
        if like is None or retweet is None or reply is None:
            return False
        
        # Engagement filter: like + retweet + reply > 0
        if (like + retweet + reply) <= 0:
            return False
        
        # Check author data
        author = record.get("author")
        if not isinstance(author, dict):
            return False
        
        # Exclude private accounts
        if author.get("is_private", False):
            return False
        
        # Exclude ads
        if record.get("is_ad", False):
            return False
        
        # followers_count MUST exist
        if author.get("followers_count") is None:
            return False
        
        return True
    
    def compute_engagement_weight(self, record: dict) -> Optional[float]:
        """
        Compute engagement weight (FIXED):
        engagement_weight = log(1 + like + 2 * retweet + reply)
        """
        try:
            like = record.get("like_count", 0)
            retweet = record.get("retweet_count", 0)
            reply = record.get("reply_count", 0)
            
            return safe_log(like + 2 * retweet + reply)
        except (TypeError, ValueError):
            return None
    
    def compute_author_weight(self, record: dict) -> Optional[float]:
        """
        Compute author weight (FIXED):
        author_weight = log(1 + followers_count)
        """
        try:
            author = record.get("author", {})
            followers = author.get("followers_count", 0)
            
            return safe_log(followers)
        except (TypeError, ValueError):
            return None
    
    def normalize(self, record: dict, velocity: float, manipulation_flag: bool = False) -> Optional[NormalizedRecord]:
        """Normalize Twitter record to standard output format."""
        if not self.validate_record(record):
            return None
        
        engagement_weight = self.compute_engagement_weight(record)
        author_weight = self.compute_author_weight(record)
        
        # DROP if weights cannot be computed
        if engagement_weight is None or author_weight is None:
            return None
        
        # Detect asset from text dynamically
        text = record["text"]
        detected_asset = detect_asset_from_text(text) or "BTC"
        
        return NormalizedRecord(
            source="twitter",
            asset=detected_asset,
            timestamp=record["timestamp"],
            text=text,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            manipulation_flag=manipulation_flag,
            source_reliability=SOURCE_RELIABILITY["twitter"]
        )


class RedditCollector(BaseCollector):
    """
    Reddit Collection Engine
    
    SOURCE SELECTION:
    - Public subreddits only
    - Posts or comments must have:
      - upvotes > 0
      - comment_depth >= 1
    
    ROLE:
    - Crowd sentiment confirmation
    - Medium-lag emotional signal
    - Fewer bots, higher signal quality
    """
    
    def validate_record(self, record: dict) -> bool:
        """
        Validate Reddit record.
        
        Required fields:
        - text (body or title)
        - timestamp
        - upvotes > 0
        - comment_count
        - comment_depth >= 1
        - author.karma
        """
        if not isinstance(record, dict):
            return False
        
        # Check text exists and contains asset keyword
        text = record.get("text") or record.get("body") or record.get("title")
        if not text or not contains_asset_keyword(text):
            return False
        
        # Check timestamp
        if not record.get("timestamp"):
            return False
        
        # Check upvotes > 0
        upvotes = record.get("upvotes")
        if upvotes is None or upvotes <= 0:
            return False
        
        # Check comment_depth >= 1
        comment_depth = record.get("comment_depth")
        if comment_depth is None or comment_depth < 1:
            return False
        
        # Check comment_count exists
        if record.get("comment_count") is None:
            return False
        
        # Check author karma
        author = record.get("author")
        if not isinstance(author, dict):
            return False
        
        if author.get("karma") is None:
            return False
        
        return True
    
    def compute_engagement_weight(self, record: dict) -> Optional[float]:
        """
        Compute engagement weight (FIXED):
        engagement_weight = log(1 + upvotes + comments)
        """
        try:
            upvotes = record.get("upvotes", 0)
            comments = record.get("comment_count", 0)
            
            return safe_log(upvotes + comments)
        except (TypeError, ValueError):
            return None
    
    def compute_author_weight(self, record: dict) -> Optional[float]:
        """
        Compute author weight (FIXED):
        author_weight = log(1 + author_karma)
        """
        try:
            author = record.get("author", {})
            karma = author.get("karma", 0)
            
            return safe_log(karma)
        except (TypeError, ValueError):
            return None
    
    def normalize(self, record: dict, velocity: float, manipulation_flag: bool = False) -> Optional[NormalizedRecord]:
        """Normalize Reddit record to standard output format."""
        if not self.validate_record(record):
            return None
        
        engagement_weight = self.compute_engagement_weight(record)
        author_weight = self.compute_author_weight(record)
        
        # DROP if weights cannot be computed
        if engagement_weight is None or author_weight is None:
            return None
        
        # Get text from available fields
        text = record.get("text") or record.get("body") or record.get("title")
        
        # Detect asset from text dynamically
        detected_asset = detect_asset_from_text(text) or "BTC"
        
        return NormalizedRecord(
            source="reddit",
            asset=detected_asset,
            timestamp=record["timestamp"],
            text=text,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            manipulation_flag=manipulation_flag,
            source_reliability=SOURCE_RELIABILITY["reddit"]
        )


class TelegramCollector(BaseCollector):
    """
    Telegram Collection Engine
    
    SOURCE SELECTION:
    - Public groups or channels ONLY
    - Exclude private chats
    - Exclude groups with excessive spam
    
    ROLE:
    - Panic detection
    - Manipulation warning
    - NOT a bullish confirmation source
    """
    
    def __init__(self):
        self.phrase_tracker: dict[str, int] = {}
        self.manipulation_threshold = 3  # N times in short window
    
    def validate_record(self, record: dict) -> bool:
        """
        Validate Telegram record.
        
        Required fields:
        - text
        - timestamp
        - is_public (must be True)
        - channel_id or group_id
        """
        if not isinstance(record, dict):
            return False
        
        # Check text exists and contains asset keyword
        text = record.get("text")
        if not text or not contains_asset_keyword(text):
            return False
        
        # Check timestamp
        if not record.get("timestamp"):
            return False
        
        # Must be public
        if not record.get("is_public", False):
            return False
        
        # Must have channel_id or group_id
        if not record.get("channel_id") and not record.get("group_id"):
            return False
        
        return True
    
    def compute_engagement_weight(self, record: dict) -> Optional[float]:
        """
        Telegram doesn't have traditional engagement metrics.
        Returns None as per specification.
        """
        # Telegram messages don't have engagement weights
        return None
    
    def compute_author_weight(self, record: dict) -> Optional[float]:
        """
        Telegram doesn't have author karma/followers in public data.
        Returns None as per specification.
        """
        return None
    
    def check_manipulation(self, text: str, window_id: str) -> bool:
        """
        REPETITION CHECK:
        IF identical phrase appears >= N times in short window
        → manipulation_flag = true
        """
        # Create a normalized key for the phrase
        normalized = text.strip().lower()[:100]  # First 100 chars
        key = f"{window_id}:{normalized}"
        
        self.phrase_tracker[key] = self.phrase_tracker.get(key, 0) + 1
        
        return self.phrase_tracker[key] >= self.manipulation_threshold
    
    def clear_window(self, window_id: str):
        """Clear tracking for a specific window."""
        keys_to_remove = [k for k in self.phrase_tracker if k.startswith(f"{window_id}:")]
        for key in keys_to_remove:
            del self.phrase_tracker[key]
    
    def normalize(self, record: dict, velocity: float, manipulation_flag: bool = False) -> Optional[NormalizedRecord]:
        """Normalize Telegram record to standard output format."""
        if not self.validate_record(record):
            return None
        
        # Check for manipulation if not already flagged
        if not manipulation_flag:
            window_id = record.get("channel_id") or record.get("group_id")
            manipulation_flag = self.check_manipulation(record["text"], window_id)
        
        # Detect asset from text dynamically
        text = record["text"]
        detected_asset = detect_asset_from_text(text) or "BTC"
        
        return NormalizedRecord(
            source="telegram",
            asset=detected_asset,
            timestamp=record["timestamp"],
            text=text,
            engagement_weight=None,  # Telegram has no engagement metrics
            author_weight=None,  # Telegram has no author weight
            velocity=velocity,
            manipulation_flag=manipulation_flag,
            source_reliability=SOURCE_RELIABILITY["telegram"]
        )


class VelocityCalculator:
    """
    Calculate velocity metrics for each source.
    
    Twitter: mentions_1h / avg_mentions_24h
    Reddit: mentions_6h / avg_mentions_48h
    Telegram: messages_10min / avg_messages_1h
    """
    
    def __init__(self):
        self.history: dict[str, list[tuple[str, int]]] = {
            "twitter": [],
            "reddit": [],
            "telegram": []
        }
    
    def compute_twitter_velocity(self, mentions_1h: int, avg_mentions_24h: float) -> Optional[float]:
        """
        Twitter velocity_calc:
        mentions_1h / avg_mentions_24h
        """
        if avg_mentions_24h <= 0:
            return None
        return mentions_1h / avg_mentions_24h
    
    def compute_reddit_velocity(self, mentions_6h: int, avg_mentions_48h: float) -> Optional[float]:
        """
        Reddit velocity_calc:
        mentions_6h / avg_mentions_48h
        """
        if avg_mentions_48h <= 0:
            return None
        return mentions_6h / avg_mentions_48h
    
    def compute_telegram_velocity(self, messages_10min: int, avg_messages_1h: float) -> Optional[float]:
        """
        Telegram velocity_calc:
        messages_10min / avg_messages_1h
        """
        if avg_messages_1h <= 0:
            return None
        return messages_10min / avg_messages_1h


class SocialDataEngine:
    """
    Main Social Data Collection & Normalization Engine.
    
    This engine:
    1. Collects data from Twitter, Reddit, Telegram
    2. Validates and filters records
    3. Computes engagement/author weights
    4. Computes velocity metrics
    5. Normalizes output for Sentiment Pipeline
    
    This module is DATA PROVIDER ONLY.
    """
    
    def __init__(self):
        self.twitter = TwitterCollector()
        self.reddit = RedditCollector()
        self.telegram = TelegramCollector()
        self.velocity = VelocityCalculator()
    
    def process_twitter_record(
        self,
        record: dict,
        mentions_1h: int,
        avg_mentions_24h: float
    ) -> Optional[dict]:
        """Process a single Twitter record."""
        # Compute velocity
        velocity = self.velocity.compute_twitter_velocity(mentions_1h, avg_mentions_24h)
        if velocity is None:
            return None  # DROP if velocity cannot be computed
        
        # Normalize
        normalized = self.twitter.normalize(record, velocity)
        if normalized is None:
            return None
        
        return normalized.to_dict()
    
    def process_reddit_record(
        self,
        record: dict,
        mentions_6h: int,
        avg_mentions_48h: float
    ) -> Optional[dict]:
        """Process a single Reddit record."""
        # Compute velocity
        velocity = self.velocity.compute_reddit_velocity(mentions_6h, avg_mentions_48h)
        if velocity is None:
            return None  # DROP if velocity cannot be computed
        
        # Normalize
        normalized = self.reddit.normalize(record, velocity)
        if normalized is None:
            return None
        
        return normalized.to_dict()
    
    def process_telegram_record(
        self,
        record: dict,
        messages_10min: int,
        avg_messages_1h: float
    ) -> Optional[dict]:
        """Process a single Telegram record."""
        # Compute velocity
        velocity = self.velocity.compute_telegram_velocity(messages_10min, avg_messages_1h)
        if velocity is None:
            return None  # DROP if velocity cannot be computed
        
        # Normalize
        normalized = self.telegram.normalize(record, velocity)
        if normalized is None:
            return None
        
        return normalized.to_dict()
    
    def process_batch(
        self,
        records: list[dict],
        source: str,
        velocity_data: dict
    ) -> list[dict]:
        """
        Process a batch of records from a single source.
        
        velocity_data must contain:
        - For twitter: mentions_1h, avg_mentions_24h
        - For reddit: mentions_6h, avg_mentions_48h
        - For telegram: messages_10min, avg_messages_1h
        """
        results = []
        
        for record in records:
            result = None
            
            if source == "twitter":
                result = self.process_twitter_record(
                    record,
                    velocity_data.get("mentions_1h", 0),
                    velocity_data.get("avg_mentions_24h", 1)
                )
            elif source == "reddit":
                result = self.process_reddit_record(
                    record,
                    velocity_data.get("mentions_6h", 0),
                    velocity_data.get("avg_mentions_48h", 1)
                )
            elif source == "telegram":
                result = self.process_telegram_record(
                    record,
                    velocity_data.get("messages_10min", 0),
                    velocity_data.get("avg_messages_1h", 1)
                )
            
            if result is not None:
                results.append(result)
        
        return results


# Singleton instance
_engine: Optional[SocialDataEngine] = None


def get_engine() -> SocialDataEngine:
    """Get or create the global engine instance."""
    global _engine
    if _engine is None:
        _engine = SocialDataEngine()
    return _engine


if __name__ == "__main__":
    import json
    
    engine = get_engine()
    
    # Example Twitter record
    twitter_record = {
        "text": "Bitcoin is going to the moon! $BTC 🚀",
        "timestamp": "2026-01-17T10:30:00Z",
        "like_count": 150,
        "retweet_count": 50,
        "reply_count": 25,
        "is_ad": False,
        "author": {
            "followers_count": 5000,
            "is_private": False
        }
    }
    
    result = engine.process_twitter_record(
        twitter_record,
        mentions_1h=100,
        avg_mentions_24h=50
    )
    
    if result:
        print("Twitter Result:")
        print(json.dumps(result, indent=2))
    
    # Example Reddit record
    reddit_record = {
        "title": "Bitcoin breaking out! #BTC to 100k",
        "text": "Just saw massive buy walls forming. Bitcoin is ready for liftoff.",
        "timestamp": "2026-01-17T10:35:00Z",
        "upvotes": 250,
        "comment_count": 45,
        "comment_depth": 3,
        "author": {
            "karma": 15000
        }
    }
    
    result = engine.process_reddit_record(
        reddit_record,
        mentions_6h=200,
        avg_mentions_48h=100
    )
    
    if result:
        print("\nReddit Result:")
        print(json.dumps(result, indent=2))
    
    # Example Telegram record
    telegram_record = {
        "text": "PANIC! Bitcoin dumping hard! Exit now!",
        "timestamp": "2026-01-17T10:40:00Z",
        "is_public": True,
        "channel_id": "btc_signals"
    }
    
    result = engine.process_telegram_record(
        telegram_record,
        messages_10min=50,
        avg_messages_1h=10
    )
    
    if result:
        print("\nTelegram Result:")
        print(json.dumps(result, indent=2))
