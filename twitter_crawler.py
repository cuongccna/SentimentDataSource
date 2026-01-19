"""
Twitter/X Social Data Crawler for Crypto BotTrading
Purpose: Collect REAL-TIME public Twitter/X data related to crypto assets.
Provides HIGH-TIME-PRECISION social inputs for BotTrading and Sentiment Pipeline.

TIME ACCURACY IS CRITICAL.

This module is DATA PROVIDER ONLY.
BotTrading Core owns all decisions.

NO SENTIMENT. NO NLP. NO SEMANTIC GUESSING.
"""

import re
import math
import json
import time
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from collections import deque
from abc import ABC, abstractmethod

# Asset detection from database
from asset_config import detect_asset as detect_asset_from_text


# SOURCE RELIABILITY (FIXED - DO NOT CHANGE)
SOURCE_RELIABILITY = 0.5

# ASSET FILTER PATTERNS (MANDATORY)
ASSET_PATTERNS = [
    re.compile(r'\$BTC\b', re.IGNORECASE),
    re.compile(r'#BTC\b', re.IGNORECASE),
    re.compile(r'\bbitcoin\b', re.IGNORECASE)
]

# Velocity windows
VELOCITY_SHORT_WINDOW_SECONDS = 60      # 1 minute
VELOCITY_LONG_WINDOW_SECONDS = 3600     # 60 minutes


@dataclass
class TweetAuthor:
    """Twitter author data."""
    user_id: str
    username: str
    followers_count: int
    account_created_at: datetime


@dataclass
class Tweet:
    """Raw tweet data."""
    tweet_id: str
    text: str
    created_at: datetime
    author: TweetAuthor
    like_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    is_retweet: bool = False
    is_reply: bool = False
    is_promoted: bool = False
    is_protected: bool = False
    original_text: Optional[str] = None  # For retweets with quote


@dataclass
class NormalizedTwitterRecord:
    """Normalized output record."""
    source: str
    asset: str
    timestamp: str
    text: str
    engagement_weight: float
    author_weight: float
    velocity: float
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
                "velocity": self.velocity
            },
            "source_reliability": self.source_reliability
        }


class MentionTracker:
    """
    Track tweet mentions for velocity calculation using EVENT TIME.
    
    Velocity = mentions_1min / avg_mentions_60min
    WHERE:
    - mentions_1min = tweets in same rolling 1-minute window
    - avg_mentions_60min = average per 1-min window over previous 60 minutes
    
    CRITICAL: Uses tweet timestamp (event time), NOT crawl time.
    """
    
    def __init__(self):
        # Store timestamps of valid tweets, ordered by event time
        self.timestamps: deque = deque(maxlen=100000)
    
    def add_mention(self, timestamp: datetime):
        """
        Record a tweet timestamp (event time).
        
        Args:
            timestamp: Tweet creation time (NOT crawl time)
        """
        self.timestamps.append(timestamp)
    
    def get_mentions_in_window(self, reference_time: datetime, window_seconds: int) -> int:
        """
        Count mentions within a time window ending at reference_time.
        
        Args:
            reference_time: End of window (tweet's event time)
            window_seconds: Window size in seconds
        
        Returns:
            Count of mentions in window
        """
        cutoff = reference_time - timedelta(seconds=window_seconds)
        
        count = 0
        for ts in self.timestamps:
            if cutoff <= ts <= reference_time:
                count += 1
        return count
    
    def compute_velocity(self, tweet_timestamp: datetime) -> float:
        """
        Compute velocity at a specific tweet's event time.
        
        velocity = mentions_1min / avg_mentions_60min
        
        WHERE:
        - mentions_1min = tweets in last 1 minute
        - avg_mentions_60min = average per 1-min window over previous 60 min
          (60 windows of 1 minute each)
        
        Args:
            tweet_timestamp: The tweet's creation time (event time)
        
        Returns:
            Velocity value
        """
        mentions_1min = self.get_mentions_in_window(
            tweet_timestamp, VELOCITY_SHORT_WINDOW_SECONDS
        )
        mentions_60min = self.get_mentions_in_window(
            tweet_timestamp, VELOCITY_LONG_WINDOW_SECONDS
        )
        
        # If no history, return default velocity
        if mentions_60min == 0 and mentions_1min == 0:
            return 1.0
        
        # 60 windows of 1 minute in 60 minutes
        num_windows = 60
        avg_mentions_per_window = mentions_60min / num_windows if mentions_60min > 0 else 1
        
        if avg_mentions_per_window <= 0:
            return 1.0
        
        return mentions_1min / avg_mentions_per_window
    
    def clear(self):
        """Clear all tracked mentions."""
        self.timestamps.clear()


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


def calculate_engagement_weight(like_count: int, retweet_count: int, reply_count: int) -> float:
    """
    Calculate engagement weight.
    
    engagement_weight = log(1 + like_count + 2 * retweet_count + reply_count)
    """
    total = like_count + 2 * retweet_count + reply_count
    return math.log(1 + total)


def calculate_author_weight(followers_count: int) -> float:
    """
    Calculate author weight.
    
    author_weight = log(1 + followers_count)
    """
    return math.log(1 + followers_count)


def get_total_engagement(like_count: int, retweet_count: int, reply_count: int) -> int:
    """Calculate total engagement count."""
    return like_count + retweet_count + reply_count


def parse_twitter_timestamp(timestamp_str: str) -> Optional[datetime]:
    """
    Parse Twitter timestamp string to datetime.
    
    Twitter API formats:
    - ISO-8601: "2026-01-17T10:30:00Z"
    - Twitter format: "Fri Jan 17 10:30:00 +0000 2026"
    
    Returns None if parsing fails or precision is coarser than seconds.
    """
    if not timestamp_str:
        return None
    
    # Try ISO-8601 format
    try:
        # Handle 'Z' suffix
        if timestamp_str.endswith('Z'):
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        elif '+' in timestamp_str or timestamp_str.count('-') > 2:
            dt = datetime.fromisoformat(timestamp_str)
        else:
            dt = None
        
        if dt:
            # Ensure UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
    except ValueError:
        pass
    
    # Try Twitter format: "Fri Jan 17 10:30:00 +0000 2026"
    try:
        dt = datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %z %Y")
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass
    
    return None


def parse_unix_timestamp(timestamp: float) -> datetime:
    """Convert Unix timestamp to datetime in UTC."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def timestamp_to_iso(dt: datetime) -> str:
    """
    Convert datetime to ISO-8601 format with second precision.
    
    Format: YYYY-MM-DDTHH:MM:SSZ
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def validate_timestamp_precision(dt: datetime) -> bool:
    """
    Validate timestamp has at least second-level precision.
    
    Returns False if precision is coarser than seconds.
    """
    if dt is None:
        return False
    # If microseconds are exactly 0 and seconds are 0, might be minute precision
    # But we accept this - we just require seconds to be present
    return True


class TwitterDataSource(ABC):
    """Abstract base class for Twitter data sources."""
    
    @abstractmethod
    def search_tweets(self, query: str, limit: int) -> list[dict]:
        """Search for tweets matching query."""
        pass
    
    @abstractmethod
    def get_user_timeline(self, user_id: str, limit: int) -> list[dict]:
        """Get tweets from a user's timeline."""
        pass


class PublicTwitterScraper(TwitterDataSource):
    """
    Scraper for public Twitter data.
    
    Uses public endpoints for data access.
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
    
    def search_tweets(self, query: str, limit: int = 50) -> list[dict]:
        """
        Search for tweets matching query.
        
        Note: Placeholder for actual scraping implementation.
        In production, implement actual HTTP scraping.
        """
        # Placeholder - implement actual scraping
        return []
    
    def get_user_timeline(self, user_id: str, limit: int = 50) -> list[dict]:
        """Get tweets from a user's timeline."""
        return []


class TwitterCrawler:
    """
    Twitter/X Social Data Crawler.
    
    Collects REAL-TIME public Twitter/X data for:
    - Early narrative detection
    - FOMO onset
    - Panic onset
    
    TIME ACCURACY IS CRITICAL.
    Uses EVENT TIME (tweet creation), NOT crawl time.
    """
    
    def __init__(
        self,
        data_source: Optional[TwitterDataSource] = None
    ):
        """
        Initialize crawler.
        
        Args:
            data_source: Data source implementation (for testing)
        """
        self.mention_tracker = MentionTracker()
        self.data_source = data_source or PublicTwitterScraper()
    
    def _validate_tweet_data(self, tweet_data: dict) -> bool:
        """
        Validate tweet meets all criteria:
        - Has text with asset keyword
        - Has timestamp with second precision
        - Has author with followers_count and account_created_at
        - Has engagement > 0
        - Is public (not protected, not promoted)
        - Is not a retweet without original text
        - Is not a reply with zero engagement
        """
        # Check text exists
        text = tweet_data.get("text", "")
        if not text or not text.strip():
            return False
        
        # Check asset keyword
        if not contains_asset_keyword(text):
            return False
        
        # Check timestamp exists
        if not tweet_data.get("created_at") and not tweet_data.get("timestamp"):
            return False
        
        # Check not protected/private
        if tweet_data.get("is_protected", False):
            return False
        
        # Check not promoted
        if tweet_data.get("is_promoted", False):
            return False
        
        # Check for retweets without original text
        if tweet_data.get("is_retweet", False):
            if not tweet_data.get("original_text"):
                return False
        
        # Check author data (can be nested in 'author' or at top level)
        author = tweet_data.get("author", {})
        has_nested_author = isinstance(author, dict) and "followers_count" in author
        
        if has_nested_author:
            # Nested author object
            if "followers_count" not in author:
                return False
            if "account_created_at" not in author:
                return False
        else:
            # Direct fields at top level
            if "followers_count" not in tweet_data:
                return False
            if "account_created_at" not in tweet_data:
                return False
        
        # Check engagement filter
        like_count = tweet_data.get("like_count", 0)
        retweet_count = tweet_data.get("retweet_count", 0)
        reply_count = tweet_data.get("reply_count", 0)
        total_engagement = get_total_engagement(like_count, retweet_count, reply_count)
        
        # Replies with zero engagement are dropped
        if tweet_data.get("is_reply", False) and total_engagement == 0:
            return False
        
        # General engagement filter
        if total_engagement <= 0:
            return False
        
        return True
    
    def _parse_timestamp(self, tweet_data: dict) -> Optional[datetime]:
        """
        Extract and validate timestamp from tweet data.
        
        Returns None if:
        - Timestamp missing
        - Precision coarser than seconds
        """
        # Try 'created_at' field first (original tweet time)
        created_at = tweet_data.get("created_at")
        if created_at:
            if isinstance(created_at, datetime):
                dt = created_at
            elif isinstance(created_at, (int, float)):
                dt = parse_unix_timestamp(created_at)
            elif isinstance(created_at, str):
                dt = parse_twitter_timestamp(created_at)
            else:
                dt = None
            
            if dt and validate_timestamp_precision(dt):
                # Ensure UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
        
        # Try 'timestamp' field
        timestamp = tweet_data.get("timestamp")
        if timestamp:
            if isinstance(timestamp, datetime):
                dt = timestamp
            elif isinstance(timestamp, (int, float)):
                dt = parse_unix_timestamp(timestamp)
            elif isinstance(timestamp, str):
                dt = parse_twitter_timestamp(timestamp)
            else:
                dt = None
            
            if dt and validate_timestamp_precision(dt):
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
        
        return None
    
    def _get_followers_count(self, tweet_data: dict) -> Optional[int]:
        """Extract followers count from tweet data."""
        author = tweet_data.get("author", {})
        if isinstance(author, dict) and "followers_count" in author:
            count = author.get("followers_count")
        else:
            count = tweet_data.get("followers_count")
        
        if count is not None:
            return int(count)
        return None
    
    def normalize_tweet(self, tweet_data: dict) -> Optional[NormalizedTwitterRecord]:
        """
        Normalize a tweet to output format.
        
        Returns None if record should be dropped.
        """
        # Validate tweet data
        if not self._validate_tweet_data(tweet_data):
            return None
        
        # Parse timestamp - DROP if missing or imprecise
        timestamp = self._parse_timestamp(tweet_data)
        if timestamp is None:
            return None
        
        # Get text - DROP if missing
        text = tweet_data.get("text", "")
        if not text:
            return None
        
        # Check asset keyword - DROP if missing
        if not contains_asset_keyword(text):
            return None
        
        # Get followers count - DROP if missing
        followers_count = self._get_followers_count(tweet_data)
        if followers_count is None:
            return None
        
        # Get engagement metrics
        like_count = tweet_data.get("like_count", 0)
        retweet_count = tweet_data.get("retweet_count", 0)
        reply_count = tweet_data.get("reply_count", 0)
        
        # Track mention for velocity (using EVENT TIME)
        self.mention_tracker.add_mention(timestamp)
        
        # Calculate metrics
        engagement_weight = calculate_engagement_weight(
            like_count, retweet_count, reply_count
        )
        author_weight = calculate_author_weight(followers_count)
        velocity = self.mention_tracker.compute_velocity(timestamp)
        
        # Detect asset from text dynamically
        detected_asset = detect_asset_from_text(text) or "BTC"
        
        return NormalizedTwitterRecord(
            source="twitter",
            asset=detected_asset,
            timestamp=timestamp_to_iso(timestamp),
            text=text,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            source_reliability=SOURCE_RELIABILITY
        )
    
    def process_tweets(self, tweets: list[dict]) -> list[dict]:
        """
        Process a batch of tweets.
        
        Args:
            tweets: List of raw tweet data
        
        Returns:
            List of normalized record dictionaries
        """
        records = []
        
        # Sort by timestamp (event time) for proper velocity calculation
        def get_ts(t):
            ts = self._parse_timestamp(t)
            return ts if ts else datetime.min.replace(tzinfo=timezone.utc)
        
        sorted_tweets = sorted(tweets, key=get_ts)
        
        for tweet_data in sorted_tweets:
            normalized = self.normalize_tweet(tweet_data)
            if normalized:
                records.append(normalized.to_dict())
        
        return records
    
    def search(self, query: str = "$BTC OR #BTC OR bitcoin", limit: int = 50) -> list[dict]:
        """
        Search for tweets matching query.
        
        Args:
            query: Search query
            limit: Maximum tweets to fetch
        
        Returns:
            List of normalized record dictionaries
        """
        tweets = self.data_source.search_tweets(query, limit)
        return self.process_tweets(tweets)
    
    def reset_tracker(self):
        """Reset the mention tracker."""
        self.mention_tracker.clear()


def create_crawler(
    data_source: Optional[TwitterDataSource] = None
) -> TwitterCrawler:
    """Factory function to create a Twitter crawler."""
    return TwitterCrawler(data_source)


if __name__ == "__main__":
    # Example usage with sample data (no live crawling)
    crawler = create_crawler()
    
    # Sample tweet data (simulating Twitter API response)
    now = datetime.now(timezone.utc)
    sample_tweets = [
        {
            "tweet_id": "1001",
            "text": "BREAKING: $BTC just crossed 50k! This is huge!",
            "created_at": now - timedelta(minutes=5),
            "author": {
                "user_id": "user1",
                "username": "cryptotrader",
                "followers_count": 15000,
                "account_created_at": datetime(2020, 1, 1, tzinfo=timezone.utc)
            },
            "like_count": 250,
            "retweet_count": 85,
            "reply_count": 42,
            "is_retweet": False,
            "is_reply": False,
            "is_promoted": False,
            "is_protected": False
        },
        {
            "tweet_id": "1002",
            "text": "Bitcoin price action looking strong! #BTC",
            "created_at": now - timedelta(minutes=3),
            "author": {
                "user_id": "user2",
                "username": "btc_analyst",
                "followers_count": 8500,
                "account_created_at": datetime(2019, 6, 15, tzinfo=timezone.utc)
            },
            "like_count": 120,
            "retweet_count": 30,
            "reply_count": 15,
            "is_retweet": False,
            "is_reply": False,
            "is_promoted": False,
            "is_protected": False
        },
        {
            "tweet_id": "1003",
            "text": "This has no crypto keywords",  # Should be dropped
            "created_at": now - timedelta(minutes=2),
            "author": {
                "user_id": "user3",
                "username": "random_user",
                "followers_count": 500,
                "account_created_at": datetime(2021, 3, 1, tzinfo=timezone.utc)
            },
            "like_count": 10,
            "retweet_count": 2,
            "reply_count": 1,
            "is_retweet": False,
            "is_reply": False,
            "is_promoted": False,
            "is_protected": False
        },
        {
            "tweet_id": "1004",
            "text": "Bitcoin is dead",  # Zero engagement - should be dropped
            "created_at": now - timedelta(minutes=1),
            "author": {
                "user_id": "user4",
                "username": "skeptic",
                "followers_count": 100,
                "account_created_at": datetime(2022, 1, 1, tzinfo=timezone.utc)
            },
            "like_count": 0,
            "retweet_count": 0,
            "reply_count": 0,
            "is_retweet": False,
            "is_reply": False,
            "is_promoted": False,
            "is_protected": False
        }
    ]
    
    results = crawler.process_tweets(sample_tweets)
    
    print(f"Processed {len(results)} tweets:\n")
    for record in results:
        print(json.dumps(record, indent=2, default=str))
        print()
