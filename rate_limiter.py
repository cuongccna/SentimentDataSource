"""
Rate Limiter and Anti-Detection Module

Implements protective measures to avoid platform bans:
1. Random delays between requests
2. Platform-specific rate limits
3. Last-read tracking to avoid re-fetching
4. Exponential backoff on errors
5. Proxy support

PLATFORM RATE LIMITS (Research-based):

TELEGRAM (Telethon):
- 20-30 API calls per minute MAX
- FloodWait errors = STOP immediately
- Recommended: 1 request every 2-3 seconds
- Collection interval: 10-15 minutes minimum
- CRITICAL: Do NOT use aggressive polling

TWITTER (Syndication API):
- ~300 requests per 15 minutes
- Recommended: 5-10 seconds between requests  
- Collection interval: 30 minutes minimum

REDDIT (Public JSON):
- 60 requests per minute with valid User-Agent
- Recommended: 2 seconds between requests
- Collection interval: 5-10 minutes minimum

PROTECTION STRATEGIES:
1. Random jitter on all delays (±30%)
2. Exponential backoff on errors
3. Track last_message_id to avoid re-reading
4. Platform enable/disable flags
5. Proxy rotation support
"""

import os
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


# =============================================================================
# SAFE RATE LIMITS (Conservative values to avoid bans)
# =============================================================================

# Platform intervals (MINIMUM time between collection cycles)
SAFE_INTERVALS = {
    "telegram": 900,    # 15 minutes (was 20 seconds - WAY too fast!)
    "twitter": 1800,    # 30 minutes
    "reddit": 600,      # 10 minutes
}

# Delay between individual API calls within a collection cycle
REQUEST_DELAYS = {
    "telegram": (2.0, 5.0),   # 2-5 seconds between messages
    "twitter": (3.0, 8.0),    # 3-8 seconds between accounts
    "reddit": (1.5, 3.0),     # 1.5-3 seconds between subreddits
}

# Max items per collection cycle (to limit API calls)
MAX_ITEMS_PER_CYCLE = {
    "telegram": 10,    # Max 10 channels per cycle
    "twitter": 5,      # Max 5 accounts per cycle
    "reddit": 10,      # Max 10 subreddits per cycle
}

# Messages/posts per source per cycle
MAX_MESSAGES_PER_SOURCE = {
    "telegram": 20,    # Last 20 messages per channel
    "twitter": 10,     # Last 10 tweets per account
    "reddit": 10,      # Last 10 posts per subreddit
}


@dataclass
class PlatformConfig:
    """Configuration for each platform."""
    enabled: bool = True
    interval: int = 600
    request_delay_min: float = 2.0
    request_delay_max: float = 5.0
    max_sources_per_cycle: int = 10
    max_messages_per_source: int = 20
    use_proxy: bool = False
    proxy_url: Optional[str] = None
    last_collection_time: float = 0
    error_count: int = 0
    backoff_until: float = 0


class RateLimiter:
    """
    Rate limiter with random jitter and exponential backoff.
    """
    
    def __init__(self):
        self.platforms: Dict[str, PlatformConfig] = {}
        self.last_request_time: Dict[str, float] = defaultdict(float)
        self._load_config()
    
    def _load_config(self):
        """Load platform configurations from environment."""
        for platform in ["telegram", "twitter", "reddit"]:
            delay_range = REQUEST_DELAYS.get(platform, (2.0, 5.0))
            
            self.platforms[platform] = PlatformConfig(
                enabled=os.getenv(f"{platform.upper()}_ENABLED", "true").lower() == "true",
                interval=int(os.getenv(f"{platform.upper()}_INTERVAL", SAFE_INTERVALS[platform])),
                request_delay_min=delay_range[0],
                request_delay_max=delay_range[1],
                max_sources_per_cycle=MAX_ITEMS_PER_CYCLE.get(platform, 10),
                max_messages_per_source=MAX_MESSAGES_PER_SOURCE.get(platform, 20),
                use_proxy=os.getenv(f"{platform.upper()}_USE_PROXY", "false").lower() == "true",
                proxy_url=os.getenv(f"{platform.upper()}_PROXY_URL"),
            )
            
            logger.info(f"[{platform}] Config: enabled={self.platforms[platform].enabled}, "
                       f"interval={self.platforms[platform].interval}s")
    
    def is_enabled(self, platform: str) -> bool:
        """Check if platform is enabled."""
        return self.platforms.get(platform, PlatformConfig()).enabled
    
    def set_enabled(self, platform: str, enabled: bool):
        """Enable or disable a platform."""
        if platform in self.platforms:
            self.platforms[platform].enabled = enabled
            logger.info(f"[{platform}] {'Enabled' if enabled else 'Disabled'}")
    
    def get_interval(self, platform: str) -> int:
        """Get collection interval for platform with jitter."""
        config = self.platforms.get(platform, PlatformConfig())
        base_interval = config.interval
        
        # Add ±20% random jitter
        jitter = random.uniform(-0.2, 0.2) * base_interval
        return int(base_interval + jitter)
    
    def should_collect(self, platform: str) -> bool:
        """Check if enough time has passed since last collection."""
        config = self.platforms.get(platform, PlatformConfig())
        
        if not config.enabled:
            return False
        
        # Check backoff
        if time.time() < config.backoff_until:
            remaining = config.backoff_until - time.time()
            logger.debug(f"[{platform}] In backoff, {remaining:.0f}s remaining")
            return False
        
        # Check interval
        elapsed = time.time() - config.last_collection_time
        interval = self.get_interval(platform)
        
        return elapsed >= interval
    
    def record_collection(self, platform: str, success: bool = True):
        """Record a collection attempt."""
        config = self.platforms.get(platform, PlatformConfig())
        config.last_collection_time = time.time()
        
        if success:
            config.error_count = 0
        else:
            config.error_count += 1
            # Exponential backoff: 2^errors * 60 seconds, max 30 minutes
            backoff_seconds = min(2 ** config.error_count * 60, 1800)
            config.backoff_until = time.time() + backoff_seconds
            logger.warning(f"[{platform}] Error #{config.error_count}, backing off for {backoff_seconds}s")
    
    def get_request_delay(self, platform: str) -> float:
        """Get random delay between requests."""
        config = self.platforms.get(platform, PlatformConfig())
        delay = random.uniform(config.request_delay_min, config.request_delay_max)
        return delay
    
    def wait_between_requests(self, platform: str):
        """Sleep with random delay between API requests."""
        delay = self.get_request_delay(platform)
        time.sleep(delay)
    
    def record_rate_limit_error(self, platform: str, wait_seconds: int = 0):
        """Handle rate limit error with extended backoff."""
        config = self.platforms.get(platform, PlatformConfig())
        
        if wait_seconds > 0:
            # Platform told us to wait
            config.backoff_until = time.time() + wait_seconds + random.uniform(10, 30)
        else:
            # Generic rate limit - back off significantly
            config.error_count += 2  # Count as 2 errors
            backoff_seconds = min(2 ** config.error_count * 60, 3600)  # Max 1 hour
            config.backoff_until = time.time() + backoff_seconds
        
        logger.error(f"[{platform}] Rate limited! Backing off until {datetime.fromtimestamp(config.backoff_until)}")
    
    def get_proxy(self, platform: str) -> Optional[str]:
        """Get proxy URL for platform if configured."""
        config = self.platforms.get(platform, PlatformConfig())
        if config.use_proxy and config.proxy_url:
            return config.proxy_url
        return None


class LastReadTracker:
    """
    Track last-read message IDs to avoid re-fetching.
    Stores in database for persistence.
    """
    
    def __init__(self, database=None):
        self.database = database
        self.cache: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._load_from_db()
    
    def _load_from_db(self):
        """Load last-read positions from database."""
        if not self.database:
            return
        
        try:
            cursor = self.database.conn.cursor()
            cursor.execute("""
                SELECT platform, source_id, last_message_id, last_message_time
                FROM last_read_positions
            """)
            for row in cursor.fetchall():
                platform, source_id, msg_id, msg_time = row
                self.cache[platform][source_id] = {
                    "last_message_id": msg_id,
                    "last_message_time": msg_time
                }
            cursor.close()
            logger.info(f"Loaded {sum(len(v) for v in self.cache.values())} last-read positions")
        except Exception as e:
            logger.debug(f"Could not load last-read positions: {e}")
    
    def get_last_message_id(self, platform: str, source_id: str) -> Optional[str]:
        """Get last message ID for a source."""
        return self.cache.get(platform, {}).get(source_id, {}).get("last_message_id")
    
    def get_last_message_time(self, platform: str, source_id: str) -> Optional[datetime]:
        """Get last message time for a source."""
        return self.cache.get(platform, {}).get(source_id, {}).get("last_message_time")
    
    def update(self, platform: str, source_id: str, message_id: str, message_time: datetime = None):
        """Update last-read position."""
        if message_time is None:
            message_time = datetime.now(timezone.utc)
        
        self.cache[platform][source_id] = {
            "last_message_id": message_id,
            "last_message_time": message_time
        }
        
        # Persist to database
        if self.database:
            try:
                cursor = self.database.conn.cursor()
                cursor.execute("""
                    INSERT INTO last_read_positions (platform, source_id, last_message_id, last_message_time)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (platform, source_id) DO UPDATE SET
                        last_message_id = EXCLUDED.last_message_id,
                        last_message_time = EXCLUDED.last_message_time,
                        updated_at = NOW()
                """, (platform, source_id, message_id, message_time))
                self.database.conn.commit()
                cursor.close()
            except Exception as e:
                logger.debug(f"Could not persist last-read position: {e}")
    
    def should_process_message(self, platform: str, source_id: str, message_id: str, message_time: datetime) -> bool:
        """Check if message should be processed (not already seen)."""
        last_id = self.get_last_message_id(platform, source_id)
        last_time = self.get_last_message_time(platform, source_id)
        
        # If no last position, process everything
        if not last_id and not last_time:
            return True
        
        # Check by time if available
        if last_time and message_time:
            return message_time > last_time
        
        # Fallback: always process if we can't determine
        return True


# Singleton instances
_rate_limiter: Optional[RateLimiter] = None
_last_read_tracker: Optional[LastReadTracker] = None


def get_rate_limiter() -> RateLimiter:
    """Get singleton rate limiter."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter


def get_last_read_tracker(database=None) -> LastReadTracker:
    """Get singleton last-read tracker."""
    global _last_read_tracker
    if _last_read_tracker is None:
        _last_read_tracker = LastReadTracker(database)
    return _last_read_tracker


# =============================================================================
# PLATFORM-SPECIFIC GUIDELINES
# =============================================================================

PLATFORM_GUIDELINES = """
=============================================================================
PLATFORM RATE LIMIT GUIDELINES
=============================================================================

TELEGRAM (Telethon/Pyrogram):
------------------------------
Official limits (https://core.telegram.org/bots/faq#my-bot-is-hitting-limits):
- 30 messages/second to different users
- 20 messages/minute to same group
- FloodWaitError = MUST wait the specified time

User API (what we use) is MORE RESTRICTED:
- ~20-30 API calls per minute
- Aggressive polling WILL get account banned
- Session can be invalidated

RECOMMENDED:
- Minimum 15 minutes between collection cycles
- 3-5 seconds between channel fetches
- Use min_id to only get NEW messages
- NEVER run in tight loops

TWITTER (Syndication/Public API):
----------------------------------
Syndication API (no auth):
- ~300 requests per 15 minutes per IP
- Rate limiting returns 429

RECOMMENDED:
- 30 minutes between collection cycles
- 5-10 seconds between account fetches
- Rotate through accounts in batches
- Use since_id for incremental fetching

REDDIT (Public JSON):
----------------------
Official (https://github.com/reddit-archive/reddit/wiki/API):
- 60 requests per minute with valid User-Agent
- 429 Too Many Requests when exceeded

RECOMMENDED:
- 10 minutes between collection cycles
- 2-3 seconds between subreddit fetches
- Use before/after params for pagination
- Set proper User-Agent

=============================================================================
PROXY CONFIGURATION
=============================================================================

To use proxies, set in .env:
  TELEGRAM_USE_PROXY=true
  TELEGRAM_PROXY_URL=socks5://user:pass@host:port
  
  TWITTER_USE_PROXY=true
  TWITTER_PROXY_URL=http://user:pass@host:port
  
  REDDIT_USE_PROXY=true
  REDDIT_PROXY_URL=http://user:pass@host:port

=============================================================================
PLATFORM ENABLE/DISABLE
=============================================================================

To disable a platform, set in .env:
  TELEGRAM_ENABLED=false
  TWITTER_ENABLED=true
  REDDIT_ENABLED=true

=============================================================================
"""


def print_guidelines():
    """Print platform guidelines."""
    print(PLATFORM_GUIDELINES)


if __name__ == "__main__":
    print_guidelines()
    
    # Test rate limiter
    limiter = get_rate_limiter()
    for platform in ["telegram", "twitter", "reddit"]:
        print(f"\n{platform}:")
        print(f"  Enabled: {limiter.is_enabled(platform)}")
        print(f"  Interval: {limiter.get_interval(platform)}s")
        print(f"  Request delay: {limiter.get_request_delay(platform):.2f}s")
