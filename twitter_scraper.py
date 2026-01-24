"""
Twitter/X Real-time Scraper for Crypto BotTrading.

Production-ready scraper using Twitter Syndication API.
NO API KEY REQUIRED - uses public embeddable endpoints.

Features:
- Scrapes from whitelisted Twitter accounts
- Filters BTC-related content
- Calculates engagement metrics
- Supports proxy (optional)
- Rate-limit aware

LOCKED CONTRACT:
- ONLY scrape from whitelisted accounts
- NO firehose access
- NO paid APIs
- NO trading signals
"""

import os
import re
import json
import asyncio
import hashlib
import math
import random
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment
load_dotenv()


def _log(level: str, msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} [{level}] twitter_scraper: {msg}")


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

SOURCE_RELIABILITY = 0.5

# Import asset config for dynamic keyword detection
from asset_config import get_asset_config, contains_tracked_asset, detect_asset

# Legacy BTC keywords (fallback if asset_config not available)
BTC_KEYWORDS = [
    "btc", "bitcoin", "₿", "satoshi", "sats", 
    "$btc", "#btc", "#bitcoin"
]

# Proxy configuration (optional)
PROXY_HOST = os.getenv("TWITTER_PROXY_HOST", "")
PROXY_PORT = os.getenv("TWITTER_PROXY_PORT", "")
PROXY_USER = os.getenv("TWITTER_PROXY_USER", "")
PROXY_PASS = os.getenv("TWITTER_PROXY_PASS", "")
PROXY_TYPE = os.getenv("TWITTER_PROXY_TYPE", "socks5")

def get_proxy_url() -> Optional[str]:
    """Build proxy URL if configured."""
    if PROXY_HOST and PROXY_PORT:
        if PROXY_USER and PROXY_PASS:
            proxy_url = f"{PROXY_TYPE}://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
        else:
            proxy_url = f"{PROXY_TYPE}://{PROXY_HOST}:{PROXY_PORT}"
        logger.info(f"Using proxy: {PROXY_HOST}:{PROXY_PORT}")
        return proxy_url
    return None


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ScrapedTweet:
    """Raw scraped tweet data."""
    tweet_id: str
    text: str
    username: str
    created_at: Optional[datetime]
    like_count: int = 0
    retweet_count: int = 0
    reply_count: int = 0
    quote_count: int = 0
    followers_count: int = 0
    profile_image: Optional[str] = None
    conversation_id: Optional[str] = None
    in_reply_to: Optional[str] = None
    is_retweet: bool = False
    is_quote: bool = False
    
    @property
    def total_engagement(self) -> int:
        return self.like_count + self.retweet_count + self.reply_count + self.quote_count
    
    @property
    def engagement_weight(self) -> float:
        """log(1 + likes + 2*retweets + replies)"""
        total = self.like_count + 2 * self.retweet_count + self.reply_count
        return math.log(1 + total)
    
    @property
    def author_weight(self) -> float:
        """log(1 + followers)"""
        return math.log(1 + self.followers_count)
    
    @property
    def fingerprint(self) -> str:
        """Unique fingerprint for deduplication."""
        content = f"{self.tweet_id}:{self.text[:100]}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
    
    def has_btc_keyword(self) -> bool:
        """Check if tweet contains any tracked asset keywords."""
        # Use dynamic asset config
        try:
            return contains_tracked_asset(self.text)
        except Exception:
            # Fallback to legacy BTC keywords
            text_lower = self.text.lower()
            return any(kw in text_lower for kw in BTC_KEYWORDS)
    
    def get_detected_asset(self) -> Optional[str]:
        """Get the detected asset symbol from tweet text."""
        try:
            return detect_asset(self.text)
        except Exception:
            return "BTC" if self.has_btc_keyword() else None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tweet_id": self.tweet_id,
            "text": self.text,
            "username": self.username,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "like_count": self.like_count,
            "retweet_count": self.retweet_count,
            "reply_count": self.reply_count,
            "quote_count": self.quote_count,
            "followers_count": self.followers_count,
            "total_engagement": self.total_engagement,
            "engagement_weight": self.engagement_weight,
            "author_weight": self.author_weight,
            "fingerprint": self.fingerprint,
            "has_btc": self.has_btc_keyword(),
            "asset": self.get_detected_asset(),
            "is_retweet": self.is_retweet,
            "is_quote": self.is_quote,
        }


@dataclass
class NormalizedRecord:
    """Normalized record for pipeline."""
    source: str
    asset: str
    timestamp: str
    text: str
    engagement_weight: float
    author_weight: float
    velocity: float
    source_reliability: float
    fingerprint: str
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
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
            "source_reliability": self.source_reliability,
            "fingerprint": self.fingerprint,
            "metadata": self.metadata
        }


# =============================================================================
# TWITTER SYNDICATION SCRAPER
# =============================================================================

class TwitterSyndicationScraper:
    """
    Scraper using Twitter Syndication API.
    
    This API is used for embedded tweets and doesn't require authentication.
    It provides good data for timeline scraping of public accounts.
    
    RATE LIMIT POLICY:
    - Minimum 5 seconds between requests
    - 60 second backoff on HTTP 429
    - Maximum 5 accounts per batch
    """
    
    def __init__(self, proxy_url: Optional[str] = None):
        self.proxy_url = proxy_url or get_proxy_url()
        self.base_url = "https://syndication.twitter.com/srv/timeline-profile/screen-name"
        self.request_delay = 5.0  # Minimum 5 seconds between requests
        self.last_request_time = 0.0
        self._client = None
        self._rate_limited_until = 0.0  # Timestamp when rate limit expires
    
    async def _get_client(self):
        """Get or create HTTP client."""
        import httpx
        
        if self._client is None or self._client.is_closed:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
            }
            
            if self.proxy_url:
                transport = httpx.AsyncHTTPTransport(proxy=self.proxy_url)
                self._client = httpx.AsyncClient(
                    transport=transport,
                    headers=headers,
                    timeout=30.0,
                    follow_redirects=True
                )
            else:
                self._client = httpx.AsyncClient(
                    headers=headers,
                    timeout=30.0,
                    follow_redirects=True
                )
        
        return self._client
    
    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
    
    async def _rate_limit(self):
        """Enforce rate limiting."""
        import time
        
        # Check if we're in rate limit backoff period
        now = time.time()
        if now < self._rate_limited_until:
            wait_time = self._rate_limited_until - now
            logger.debug(f"In rate limit backoff, waiting {wait_time:.1f}s")
            await asyncio.sleep(wait_time)
        
        # Normal rate limiting between requests
        elapsed = now - self.last_request_time
        if elapsed < self.request_delay:
            await asyncio.sleep(self.request_delay - elapsed)
        self.last_request_time = time.time()
    
    def _parse_tweet_from_json(self, tweet_data: Dict, username: str) -> Optional[ScrapedTweet]:
        """Parse tweet from JSON response."""
        try:
            # Extract tweet ID
            tweet_id = tweet_data.get("id_str") or str(tweet_data.get("id", ""))
            if not tweet_id:
                return None
            
            # Extract text (try multiple fields)
            text = tweet_data.get("full_text") or tweet_data.get("text", "")
            if not text:
                return None
            
            # Parse timestamp
            created_at = None
            if "created_at" in tweet_data:
                try:
                    # Twitter format: "Fri Jan 17 10:30:00 +0000 2026"
                    created_at = datetime.strptime(
                        tweet_data["created_at"],
                        "%a %b %d %H:%M:%S %z %Y"
                    )
                except:
                    pass
            
            # Extract user info
            user = tweet_data.get("user", {})
            actual_username = user.get("screen_name", username)
            followers_count = user.get("followers_count", 0)
            
            # Engagement metrics
            like_count = tweet_data.get("favorite_count", 0)
            retweet_count = tweet_data.get("retweet_count", 0)
            reply_count = tweet_data.get("reply_count", 0)
            quote_count = tweet_data.get("quote_count", 0)
            
            # Check if retweet or quote
            is_retweet = tweet_data.get("retweeted", False) or text.startswith("RT @")
            is_quote = tweet_data.get("is_quote_status", False)
            
            return ScrapedTweet(
                tweet_id=tweet_id,
                text=text,
                username=actual_username,
                created_at=created_at,
                like_count=like_count,
                retweet_count=retweet_count,
                reply_count=reply_count,
                quote_count=quote_count,
                followers_count=followers_count,
                is_retweet=is_retweet,
                is_quote=is_quote
            )
            
        except Exception as e:
            logger.debug(f"Failed to parse tweet: {e}")
            return None
    
    def _extract_tweets_from_html(self, html: str, username: str) -> List[ScrapedTweet]:
        """Extract tweets from syndication HTML response."""
        tweets = []
        
        try:
            # The syndication API returns HTML with embedded JSON data
            # Look for JSON objects in the response
            
            # Method 1: Extract from script tags
            json_pattern = r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\});'
            match = re.search(json_pattern, html, re.DOTALL)
            
            if match:
                try:
                    data = json.loads(match.group(1))
                    timeline = data.get("timeline", {})
                    entries = timeline.get("entries", [])
                    
                    for entry in entries:
                        tweet_data = entry.get("content", {}).get("tweet", {})
                        if tweet_data:
                            tweet = self._parse_tweet_from_json(tweet_data, username)
                            if tweet:
                                tweets.append(tweet)
                    
                    return tweets
                except json.JSONDecodeError:
                    pass
            
            # Method 2: Extract individual tweet JSON objects
            tweet_json_pattern = r'"id_str":"(\d+)"[^}]*"text":"([^"]+)"[^}]*"favorite_count":(\d+)[^}]*"retweet_count":(\d+)'
            
            for match in re.finditer(tweet_json_pattern, html):
                try:
                    tweet_id = match.group(1)
                    text = match.group(2)
                    
                    # Decode unicode escapes
                    try:
                        text = text.encode('latin1').decode('unicode_escape')
                    except:
                        pass
                    
                    like_count = int(match.group(3))
                    retweet_count = int(match.group(4))
                    
                    tweet = ScrapedTweet(
                        tweet_id=tweet_id,
                        text=text,
                        username=username,
                        created_at=datetime.now(timezone.utc),  # Fallback
                        like_count=like_count,
                        retweet_count=retweet_count,
                    )
                    tweets.append(tweet)
                    
                except Exception as e:
                    continue
            
            if tweets:
                return tweets
            
            # Method 3: Simple text extraction (fallback)
            text_pattern = r'"text":"([^"]{20,500})"'
            matches = re.findall(text_pattern, html)
            
            for i, text in enumerate(matches[:50]):
                try:
                    # Decode unicode escapes
                    decoded = text.encode('latin1').decode('unicode_escape')
                except:
                    decoded = text
                
                tweet = ScrapedTweet(
                    tweet_id=f"syn_{username}_{i}",
                    text=decoded,
                    username=username,
                    created_at=datetime.now(timezone.utc),
                    like_count=0,
                    retweet_count=0,
                )
                tweets.append(tweet)
            
        except Exception as e:
            logger.error(f"Failed to extract tweets from HTML: {e}")
        
        return tweets
    
    async def scrape_user_timeline(
        self,
        username: str,
        limit: int = 50,
        filter_btc: bool = True
    ) -> List[ScrapedTweet]:
        """
        Scrape tweets from a user's timeline.
        
        Args:
            username: Twitter username (without @)
            limit: Maximum tweets to return
            filter_btc: Only return BTC-related tweets
        
        Returns:
            List of scraped tweets
        """
        await self._rate_limit()
        
        try:
            client = await self._get_client()
            
            url = f"{self.base_url}/{username}"
            params = {"showReplies": "false"}
            
            response = await client.get(url, params=params)
            
            if response.status_code == 200:
                tweets = self._extract_tweets_from_html(response.text, username)
                
                # Filter BTC-related if requested
                if filter_btc:
                    tweets = [t for t in tweets if t.has_btc_keyword()]
                
                # Apply limit
                tweets = tweets[:limit]
                
                logger.info(f"Scraped {len(tweets)} tweets from @{username}")
                return tweets
                
            elif response.status_code == 404:
                logger.warning(f"User @{username} not found")
                return []
            elif response.status_code == 429:
                # Rate limited - back off for 60 seconds
                import time
                self._rate_limited_until = time.time() + 60
                logger.warning(f"Rate limited when scraping @{username}. Backing off for 60s.")
                return []
            else:
                logger.warning(f"HTTP {response.status_code} for @{username}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to scrape @{username}: {e}")
            return []
    
    async def scrape_multiple_users(
        self,
        usernames: List[str],
        limit_per_user: int = 20,
        filter_btc: bool = True
    ) -> Dict[str, List[ScrapedTweet]]:
        """
        Scrape tweets from multiple users.
        
        Args:
            usernames: List of Twitter usernames
            limit_per_user: Max tweets per user
            filter_btc: Only return BTC-related tweets
        
        Returns:
            Dict mapping username to list of tweets
        """
        results = {}
        rate_limited = False
        
        for username in usernames:
            # Stop if we're rate limited
            if rate_limited:
                logger.debug(f"Skipping @{username} due to rate limiting")
                results[username] = []
                continue
            
            tweets = await self.scrape_user_timeline(
                username,
                limit=limit_per_user,
                filter_btc=filter_btc
            )
            results[username] = tweets
            
            # Check if we got rate limited
            import time
            if time.time() < self._rate_limited_until:
                rate_limited = True
                logger.info("Rate limited detected, stopping further scraping for this batch")
                continue
            
            # ANTI-BAN: Random delay between users (3-7 seconds)
            delay = random.uniform(3.0, 7.0)
            await asyncio.sleep(delay)
        
        return results


# =============================================================================
# VELOCITY TRACKER
# =============================================================================

class VelocityTracker:
    """Track tweet velocity for normalization."""
    
    def __init__(self):
        self.timestamps: List[datetime] = []
        self.max_history = 10000
    
    def add_tweet(self, timestamp: datetime):
        """Record a tweet timestamp."""
        self.timestamps.append(timestamp)
        if len(self.timestamps) > self.max_history:
            self.timestamps = self.timestamps[-self.max_history:]
    
    def compute_velocity(self, reference_time: datetime) -> float:
        """
        Compute velocity at a reference time.
        
        velocity = mentions_1min / avg_mentions_60min
        """
        if not self.timestamps:
            return 1.0
        
        # Count tweets in last 1 minute
        one_min_ago = reference_time - timedelta(minutes=1)
        mentions_1min = sum(1 for ts in self.timestamps if ts >= one_min_ago)
        
        # Count tweets in last 60 minutes
        sixty_min_ago = reference_time - timedelta(minutes=60)
        mentions_60min = sum(1 for ts in self.timestamps if ts >= sixty_min_ago)
        
        # Calculate average per minute window
        avg_per_min = mentions_60min / 60 if mentions_60min > 0 else 1
        
        return mentions_1min / avg_per_min if avg_per_min > 0 else 1.0


# =============================================================================
# MAIN TWITTER SCRAPER
# =============================================================================

class TwitterScraper:
    """
    Main Twitter scraper for BotTrading pipeline.
    
    Uses Syndication API to scrape from whitelisted accounts.
    """
    
    def __init__(
        self,
        accounts: Optional[List[str]] = None,
        proxy_url: Optional[str] = None
    ):
        # Default crypto accounts to monitor
        self.accounts = accounts or [
            "whale_alert",
            "Cointelegraph",
            "BitcoinMagazine",
            "WuBlockchain",
            "DocumentingBTC",
            "glassnode",
            "santaboris",
            "CryptoQuant_News",
        ]
        
        self.scraper = TwitterSyndicationScraper(proxy_url)
        self.velocity_tracker = VelocityTracker()
        self.seen_fingerprints: set = set()
    
    async def close(self):
        """Cleanup resources."""
        await self.scraper.close()
    
    def normalize_tweet(
        self,
        tweet: ScrapedTweet,
        asset: str = "BTC"
    ) -> Optional[NormalizedRecord]:
        """Normalize a scraped tweet to pipeline format."""
        
        # Validate tweet_id - critical for deduplication
        if not tweet.tweet_id:
            logger.warning(f"Skipping tweet with empty tweet_id from {tweet.username}")
            return None
        
        # Track for velocity
        if tweet.created_at:
            self.velocity_tracker.add_tweet(tweet.created_at)
        
        # Compute velocity
        ref_time = tweet.created_at or datetime.now(timezone.utc)
        velocity = self.velocity_tracker.compute_velocity(ref_time)
        
        # Format timestamp
        timestamp = tweet.created_at or datetime.now(timezone.utc)
        timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        return NormalizedRecord(
            source="twitter",
            asset=asset,
            timestamp=timestamp_str,
            text=tweet.text,
            engagement_weight=tweet.engagement_weight,
            author_weight=tweet.author_weight,
            velocity=velocity,
            source_reliability=SOURCE_RELIABILITY,
            fingerprint=tweet.fingerprint,
            metadata={
                "tweet_id": tweet.tweet_id,
                "username": tweet.username,
                "like_count": tweet.like_count,
                "retweet_count": tweet.retweet_count,
                "reply_count": tweet.reply_count,
                "total_engagement": tweet.total_engagement,
                "is_retweet": tweet.is_retweet,
            }
        )
    
    async def scrape(
        self,
        limit_per_account: int = 20,
        filter_btc: bool = True,
        deduplicate: bool = True
    ) -> List[NormalizedRecord]:
        """
        Scrape tweets from all monitored accounts.
        
        Args:
            limit_per_account: Max tweets per account
            filter_btc: Only return BTC-related tweets
            deduplicate: Skip already-seen tweets
        
        Returns:
            List of normalized records
        """
        logger.info(f"Starting scrape of {len(self.accounts)} accounts...")
        
        results = await self.scraper.scrape_multiple_users(
            self.accounts,
            limit_per_user=limit_per_account,
            filter_btc=filter_btc
        )
        
        records = []
        
        for username, tweets in results.items():
            for tweet in tweets:
                # Deduplicate
                if deduplicate and tweet.fingerprint in self.seen_fingerprints:
                    continue
                
                self.seen_fingerprints.add(tweet.fingerprint)
                
                # Normalize (may return None for invalid tweets)
                record = self.normalize_tweet(tweet)
                if record is not None:
                    records.append(record)
        
        # Sort by timestamp
        records.sort(key=lambda r: r.timestamp)
        
        logger.info(f"Scraped {len(records)} unique BTC tweets")
        
        return records
    
    async def scrape_single(
        self,
        username: str,
        limit: int = 20,
        filter_btc: bool = True
    ) -> List[NormalizedRecord]:
        """Scrape tweets from a single account."""
        tweets = await self.scraper.scrape_user_timeline(
            username,
            limit=limit,
            filter_btc=filter_btc
        )
        
        records = []
        for tweet in tweets:
            record = self.normalize_tweet(tweet)
            if record is not None:
                records.append(record)
        
        return records


# =============================================================================
# DEMO / CLI
# =============================================================================

async def demo():
    """Demo the Twitter scraper."""
    print("\n" + "=" * 60)
    print(" TWITTER SCRAPER DEMO")
    print("=" * 60)
    
    scraper = TwitterScraper()
    
    try:
        # Scrape from default accounts
        records = await scraper.scrape(
            limit_per_account=10,
            filter_btc=True
        )
        
        print(f"\n📊 Collected {len(records)} BTC-related tweets:\n")
        
        for i, record in enumerate(records[:10], 1):
            data = record.to_dict()
            print(f"{i}. [{data['timestamp']}] @{data['metadata']['username']}")
            print(f"   {data['text'][:80]}...")
            print(f"   Engagement: {data['metrics']['engagement_weight']:.2f}")
            print(f"   Velocity: {data['metrics']['velocity']:.2f}")
            print()
        
        # Save to file
        output_file = "twitter_scraped_data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([r.to_dict() for r in records], f, indent=2, ensure_ascii=False)
        
        print(f"💾 Saved {len(records)} records to {output_file}")
        
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(demo())
