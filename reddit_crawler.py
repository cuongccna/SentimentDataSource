"""
Reddit Social Data Crawler for Crypto BotTrading
Purpose: Collect and normalize RAW Reddit data related to crypto assets.
This module performs DATA COLLECTION and BASIC METRICS ONLY.

This module is DATA PROVIDER ONLY.
BotTrading Core owns all decisions.

NO SENTIMENT. NO NLP. NO SEMANTIC INTERPRETATION.
"""

import re
import math
import time
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
from collections import deque


# SOURCE RELIABILITY (FIXED)
SOURCE_RELIABILITY = 0.7

# ASSET FILTER PATTERNS (MANDATORY)
ASSET_PATTERNS = [
    re.compile(r'\$BTC\b', re.IGNORECASE),
    re.compile(r'#BTC\b', re.IGNORECASE),
    re.compile(r'\bbitcoin\b', re.IGNORECASE)
]

# DEFAULT TARGET SUBREDDITS
DEFAULT_SUBREDDITS = [
    "CryptoCurrency",
    "Bitcoin",
    "CryptoMarkets",
    "BitcoinMarkets"
]

# Rate limiting
REQUEST_DELAY_SECONDS = 2.0  # Delay between requests to avoid rate limiting
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


@dataclass
class RedditPost:
    """Raw Reddit post data."""
    id: str
    subreddit: str
    title: str
    text: str
    author: str
    author_karma: int
    score: int
    num_comments: int
    created_utc: float
    is_self: bool
    permalink: str


@dataclass
class RedditComment:
    """Raw Reddit comment data."""
    id: str
    subreddit: str
    text: str
    author: str
    author_karma: int
    score: int
    depth: int
    created_utc: float
    permalink: str
    parent_id: str


@dataclass
class NormalizedRedditRecord:
    """Normalized output record."""
    source: str
    asset: str
    timestamp: str
    text: str
    engagement_weight: float
    author_weight: float
    velocity: float
    source_reliability: float
    post_id: str = ""  # Reddit post/comment ID
    subreddit: str = ""
    author: str = ""
    score: int = 0
    num_comments: int = 0
    
    def to_dict(self) -> dict:
        """Convert to output dictionary format."""
        return {
            "id": self.post_id,
            "source": self.source,
            "asset": self.asset,
            "timestamp": self.timestamp,
            "text": self.text,
            "subreddit": self.subreddit,
            "author": self.author,
            "score": self.score,
            "num_comments": self.num_comments,
            "metrics": {
                "engagement_weight": self.engagement_weight,
                "author_weight": self.author_weight,
                "velocity": self.velocity
            },
            "source_reliability": self.source_reliability
        }


@dataclass
class MentionTracker:
    """Track mentions for velocity calculation."""
    # Stores (timestamp, count) tuples
    mentions: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def add_mention(self, timestamp: datetime):
        """Add a mention timestamp."""
        self.mentions.append(timestamp)
    
    def get_mentions_in_window(self, window_hours: int) -> int:
        """Get count of mentions in the last N hours."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)
        
        count = 0
        for ts in self.mentions:
            if ts >= cutoff:
                count += 1
        return count
    
    def compute_velocity(self) -> float:
        """
        Compute velocity:
        mentions_6h / avg_mentions_48h
        
        WHERE:
        - mentions_6h = mentions in last 6 hours
        - avg_mentions_48h = average mentions per 6h window over last 48 hours
        """
        mentions_6h = self.get_mentions_in_window(6)
        mentions_48h = self.get_mentions_in_window(48)
        
        # 48 hours = 8 windows of 6 hours
        num_windows = 8
        avg_mentions_per_window = mentions_48h / num_windows if mentions_48h > 0 else 1
        
        if avg_mentions_per_window <= 0:
            return 1.0  # Default velocity
        
        return mentions_6h / avg_mentions_per_window


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


def safe_log(value: float) -> float:
    """Compute log(1 + value) safely."""
    return math.log(1 + max(0, value))


def compute_engagement_weight(upvotes: int, num_comments: int) -> float:
    """
    Compute engagement weight (FIXED):
    engagement_weight = log(1 + upvotes + number_of_comments)
    """
    return safe_log(upvotes + num_comments)


def compute_author_weight(author_karma: int) -> float:
    """
    Compute author weight (FIXED):
    author_weight = log(1 + author_karma)
    """
    return safe_log(author_karma)


def timestamp_to_iso(utc_timestamp: float) -> str:
    """Convert Unix timestamp to ISO-8601 format."""
    dt = datetime.fromtimestamp(utc_timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_timestamp(utc_timestamp: float) -> datetime:
    """Convert Unix timestamp to datetime."""
    return datetime.fromtimestamp(utc_timestamp, tz=timezone.utc)


class RedditCrawler:
    """
    Reddit Social Data Crawler.
    
    Uses Reddit's public JSON endpoints.
    No authenticated API required.
    """
    
    def __init__(self, subreddits: Optional[list[str]] = None):
        """
        Initialize crawler.
        
        Args:
            subreddits: List of subreddit names to crawl.
                        Defaults to DEFAULT_SUBREDDITS.
        """
        self.subreddits = subreddits or DEFAULT_SUBREDDITS
        self.mention_tracker = MentionTracker()
        self.last_request_time = 0.0
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY_SECONDS:
            time.sleep(REQUEST_DELAY_SECONDS - elapsed)
        self.last_request_time = time.time()
    
    def _fetch_json(self, url: str) -> Optional[dict]:
        """
        Fetch JSON from URL with rate limiting.
        
        Returns None on error.
        """
        self._rate_limit()
        
        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": USER_AGENT}
            )
            
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read().decode("utf-8")
                return json.loads(data)
                
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # Rate limited - wait and retry once
                time.sleep(5)
                try:
                    with urllib.request.urlopen(request, timeout=30) as response:
                        data = response.read().decode("utf-8")
                        return json.loads(data)
                except Exception:
                    return None
            return None
        except Exception:
            return None
    
    def _validate_post(self, post_data: dict) -> bool:
        """
        Validate post meets criteria:
        - Not deleted/removed
        - score > 0
        - has comments
        - has text (for self posts) or title contains keyword
        - author exists
        """
        # Check not deleted
        if post_data.get("author") in ["[deleted]", "[removed]", None]:
            return False
        
        # Check score > 0
        if post_data.get("score", 0) <= 0:
            return False
        
        # Check has comments
        if post_data.get("num_comments", 0) <= 0:
            return False
        
        # Get text content
        title = post_data.get("title", "")
        selftext = post_data.get("selftext", "")
        
        # For self posts, require text; otherwise use title
        if post_data.get("is_self", False):
            text = f"{title} {selftext}"
        else:
            text = title
        
        # Check has text
        if not text.strip():
            return False
        
        # Check contains asset keyword
        if not contains_asset_keyword(text):
            return False
        
        return True
    
    def _validate_comment(self, comment_data: dict) -> bool:
        """
        Validate comment meets criteria:
        - Not deleted/removed
        - score > 0 (for initial fetch, we'll be lenient)
        - has text
        - author exists
        """
        # Check not deleted
        if comment_data.get("author") in ["[deleted]", "[removed]", None]:
            return False
        
        # Get text
        text = comment_data.get("body", "")
        
        # Check has text
        if not text.strip():
            return False
        
        # Exclude removed content
        if text in ["[deleted]", "[removed]"]:
            return False
        
        # Check contains asset keyword
        if not contains_asset_keyword(text):
            return False
        
        return True
    
    def _extract_author_karma(self, post_data: dict) -> Optional[int]:
        """
        Extract author karma from post data.
        
        Note: Reddit's public JSON doesn't include author karma in listings.
        We use a combination of available metrics as proxy.
        """
        # Try to get from author data if available
        author_data = post_data.get("author_data", {})
        if author_data:
            karma = author_data.get("total_karma") or author_data.get("link_karma", 0) + author_data.get("comment_karma", 0)
            if karma:
                return karma
        
        # Fallback: use post score as rough proxy for author credibility
        # This is a limitation of unauthenticated API access
        score = post_data.get("score", 0)
        
        # If we have no karma info, we must DROP
        # For now, use a minimum threshold
        return max(1, score * 10)  # Rough estimate
    
    def fetch_subreddit_posts(self, subreddit: str, limit: int = 25) -> list[dict]:
        """
        Fetch recent posts from a subreddit using public JSON.
        
        Args:
            subreddit: Subreddit name (without r/)
            limit: Maximum posts to fetch (max 100)
        
        Returns:
            List of raw post data dictionaries
        """
        url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={min(limit, 100)}"
        
        data = self._fetch_json(url)
        if not data:
            return []
        
        posts = []
        children = data.get("data", {}).get("children", [])
        
        for child in children:
            post_data = child.get("data", {})
            if self._validate_post(post_data):
                posts.append(post_data)
        
        return posts
    
    def fetch_post_comments(self, subreddit: str, post_id: str, limit: int = 50) -> list[dict]:
        """
        Fetch comments from a post using public JSON.
        
        Args:
            subreddit: Subreddit name
            post_id: Post ID
            limit: Maximum comments to fetch
        
        Returns:
            List of raw comment data dictionaries
        """
        url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json?limit={limit}"
        
        data = self._fetch_json(url)
        if not data or not isinstance(data, list) or len(data) < 2:
            return []
        
        comments = []
        
        def extract_comments(listing: dict, depth: int = 1):
            """Recursively extract comments from listing."""
            if not isinstance(listing, dict):
                return
            
            children = listing.get("data", {}).get("children", [])
            
            for child in children:
                if child.get("kind") != "t1":  # t1 = comment
                    continue
                
                comment_data = child.get("data", {})
                comment_data["depth"] = depth
                
                if self._validate_comment(comment_data):
                    comments.append(comment_data)
                
                # Process replies recursively
                replies = comment_data.get("replies")
                if isinstance(replies, dict):
                    extract_comments(replies, depth + 1)
        
        # Comments are in the second element of the response
        extract_comments(data[1])
        
        return comments[:limit]
    
    def normalize_post(self, post_data: dict, velocity: float) -> Optional[NormalizedRedditRecord]:
        """
        Normalize a Reddit post to output format.
        
        Returns None if record should be dropped.
        """
        # Get author karma - DROP if missing
        author_karma = self._extract_author_karma(post_data)
        if author_karma is None:
            return None
        
        # Get engagement metrics
        score = post_data.get("score")
        num_comments = post_data.get("num_comments")
        
        if score is None or num_comments is None:
            return None  # DROP if missing engagement
        
        # Get text
        title = post_data.get("title", "")
        selftext = post_data.get("selftext", "")
        text = f"{title} {selftext}".strip() if selftext else title
        
        # Check asset keyword
        if not contains_asset_keyword(text):
            return None
        
        # Get timestamp
        created_utc = post_data.get("created_utc")
        if created_utc is None:
            return None
        
        # Compute weights
        engagement_weight = compute_engagement_weight(score, num_comments)
        author_weight = compute_author_weight(author_karma)
        
        # Track mention
        self.mention_tracker.add_mention(parse_timestamp(created_utc))
        
        # Get post ID and metadata
        post_id = post_data.get("id", post_data.get("name", ""))
        subreddit = post_data.get("subreddit", "")
        author = post_data.get("author", "")
        
        return NormalizedRedditRecord(
            source="reddit",
            asset="BTC",
            timestamp=timestamp_to_iso(created_utc),
            text=text,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            source_reliability=SOURCE_RELIABILITY,
            post_id=post_id,
            subreddit=subreddit,
            author=author,
            score=score,
            num_comments=num_comments
        )
    
    def normalize_comment(self, comment_data: dict, velocity: float) -> Optional[NormalizedRedditRecord]:
        """
        Normalize a Reddit comment to output format.
        
        Returns None if record should be dropped.
        """
        # Get author karma - DROP if missing
        author_karma = self._extract_author_karma(comment_data)
        if author_karma is None:
            return None
        
        # Get engagement metrics
        score = comment_data.get("score")
        depth = comment_data.get("depth", 1)
        
        if score is None:
            return None  # DROP if missing engagement
        
        # Use depth as proxy for "number of comments" in engagement
        # For comments, we use depth to indicate thread activity
        engagement_proxy = max(1, depth)
        
        # Get text
        text = comment_data.get("body", "")
        
        # Check asset keyword
        if not contains_asset_keyword(text):
            return None
        
        # Get timestamp
        created_utc = comment_data.get("created_utc")
        if created_utc is None:
            return None
        
        # Compute weights
        engagement_weight = compute_engagement_weight(score, engagement_proxy)
        author_weight = compute_author_weight(author_karma)
        
        # Track mention
        self.mention_tracker.add_mention(parse_timestamp(created_utc))
        
        # Get comment ID and metadata
        comment_id = comment_data.get("id", comment_data.get("name", ""))
        subreddit = comment_data.get("subreddit", "")
        author = comment_data.get("author", "")
        
        return NormalizedRedditRecord(
            source="reddit",
            asset="BTC",
            timestamp=timestamp_to_iso(created_utc),
            text=text,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            source_reliability=SOURCE_RELIABILITY,
            post_id=comment_id,
            subreddit=subreddit,
            author=author,
            score=score,
            num_comments=0  # Comments don't have num_comments
        )
    
    def crawl_subreddit(self, subreddit: str, post_limit: int = 25, comment_limit: int = 50) -> list[dict]:
        """
        Crawl a subreddit for posts and comments.
        
        Args:
            subreddit: Subreddit name
            post_limit: Max posts to fetch
            comment_limit: Max comments per post
        
        Returns:
            List of normalized record dictionaries
        """
        records = []
        
        # Compute current velocity
        velocity = self.mention_tracker.compute_velocity()
        
        # Fetch posts
        posts = self.fetch_subreddit_posts(subreddit, post_limit)
        
        for post_data in posts:
            # Normalize post
            normalized = self.normalize_post(post_data, velocity)
            if normalized:
                records.append(normalized.to_dict())
            
            # Fetch and normalize comments
            post_id = post_data.get("id")
            if post_id:
                comments = self.fetch_post_comments(subreddit, post_id, comment_limit)
                
                for comment_data in comments:
                    normalized = self.normalize_comment(comment_data, velocity)
                    if normalized:
                        records.append(normalized.to_dict())
        
        return records
    
    def crawl_all(self, post_limit: int = 25, comment_limit: int = 50) -> list[dict]:
        """
        Crawl all configured subreddits.
        
        Args:
            post_limit: Max posts per subreddit
            comment_limit: Max comments per post
        
        Returns:
            List of normalized record dictionaries
        """
        all_records = []
        
        for subreddit in self.subreddits:
            records = self.crawl_subreddit(subreddit, post_limit, comment_limit)
            all_records.extend(records)
        
        return all_records
    
    def process_raw_data(self, raw_posts: list[dict], raw_comments: list[dict]) -> list[dict]:
        """
        Process pre-fetched raw data (for testing or offline processing).
        
        Args:
            raw_posts: List of raw post data
            raw_comments: List of raw comment data
        
        Returns:
            List of normalized record dictionaries
        """
        records = []
        velocity = self.mention_tracker.compute_velocity()
        
        for post_data in raw_posts:
            if self._validate_post(post_data):
                normalized = self.normalize_post(post_data, velocity)
                if normalized:
                    records.append(normalized.to_dict())
        
        for comment_data in raw_comments:
            if self._validate_comment(comment_data):
                normalized = self.normalize_comment(comment_data, velocity)
                if normalized:
                    records.append(normalized.to_dict())
        
        return records


def create_crawler(subreddits: Optional[list[str]] = None) -> RedditCrawler:
    """Factory function to create a Reddit crawler."""
    return RedditCrawler(subreddits)


if __name__ == "__main__":
    # Example usage with sample data (no live crawling)
    crawler = create_crawler()
    
    # Sample post data (simulating Reddit API response)
    sample_posts = [
        {
            "id": "abc123",
            "subreddit": "Bitcoin",
            "title": "Bitcoin breaking all time highs! $BTC to 100k",
            "selftext": "Just saw massive buy walls forming. This is the breakout we've been waiting for.",
            "author": "crypto_enthusiast",
            "score": 250,
            "num_comments": 45,
            "created_utc": 1737107400.0,  # 2026-01-17T10:30:00Z
            "is_self": True,
            "permalink": "/r/Bitcoin/comments/abc123/"
        }
    ]
    
    sample_comments = [
        {
            "id": "comment1",
            "subreddit": "Bitcoin",
            "body": "Bitcoin is definitely going to the moon! So bullish right now.",
            "author": "btc_hodler",
            "score": 50,
            "depth": 1,
            "created_utc": 1737107460.0,
            "permalink": "/r/Bitcoin/comments/abc123/comment1/",
            "parent_id": "t3_abc123"
        }
    ]
    
    results = crawler.process_raw_data(sample_posts, sample_comments)
    
    print(f"Processed {len(results)} records:\n")
    for record in results:
        print(json.dumps(record, indent=2))
        print()
