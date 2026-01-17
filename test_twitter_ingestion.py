"""
Comprehensive tests for Twitter/X Ingestion Worker.

Tests the LOCKED TWITTER INGESTION CONTRACT:
- Whitelist-only ingestion
- Rate limiting (per-source and global)
- Field validation
- Metrics computation
- State persistence
- Velocity calculation

NO MOCKING - uses real instances.
NO HALLUCINATION - tests actual contract.
"""

import json
import math
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone, timedelta

from twitter_ingestion import (
    # Constants
    SOURCE_RELIABILITY_TWITTER,
    GLOBAL_RATE_LIMIT,
    DEFAULT_SOURCE_RATE_LIMIT,
    ASSET_KEYWORDS_BTC,
    
    # Enums
    TwitterSourceType,
    SourceRole,
    TweetDropReason,
    
    # Data classes
    TwitterSource,
    RawTweet,
    ProcessedTweet,
    IngestionState,
    IngestionMetrics,
    
    # Components
    TwitterSourceRegistry,
    IngestionStateManager,
    TweetRateLimiter,
    TweetValidator,
    VelocityCalculator,
    TwitterIngestionWorker,
    
    # Helpers
    compute_engagement_weight,
    compute_author_weight,
    compute_total_engagement,
    safe_log,
    
    # Factory functions
    create_source_registry,
    create_ingestion_worker,
    create_test_worker,
    get_create_twitter_sources_sql,
)


# =============================================================================
# TEST HELPERS
# =============================================================================

def create_test_source(
    id: int = 1,
    source_type: TwitterSourceType = TwitterSourceType.ACCOUNT,
    value: str = "testuser",
    asset: str = "BTC",
    role: SourceRole = SourceRole.ANALYST,
    enabled: bool = True,
    max_tweets_per_min: int = 60,
    priority: int = 0
) -> TwitterSource:
    """Create a test Twitter source."""
    return TwitterSource(
        id=id,
        source_type=source_type,
        value=value,
        asset=asset,
        role=role,
        enabled=enabled,
        max_tweets_per_min=max_tweets_per_min,
        priority=priority
    )


def create_test_tweet(
    tweet_id: str = "12345",
    text: str = "BTC is looking bullish today!",
    created_at: datetime = None,
    like_count: int = 100,
    retweet_count: int = 50,
    reply_count: int = 10,
    author_followers_count: int = 10000,
    author_username: str = "testuser",
    source_id: int = None
) -> RawTweet:
    """Create a test raw tweet."""
    if created_at is None:
        created_at = datetime.now(timezone.utc)
    
    return RawTweet(
        tweet_id=tweet_id,
        created_at=created_at,
        text=text,
        like_count=like_count,
        retweet_count=retweet_count,
        reply_count=reply_count,
        author_followers_count=author_followers_count,
        author_username=author_username,
        source_id=source_id
    )


# =============================================================================
# CONSTANTS TESTS
# =============================================================================

class TestConstants(unittest.TestCase):
    """Test module constants."""
    
    def test_source_reliability_value(self):
        """Source reliability for Twitter is 0.5."""
        self.assertEqual(SOURCE_RELIABILITY_TWITTER, 0.5)
    
    def test_global_rate_limit_value(self):
        """Global rate limit is 500 tweets per minute."""
        self.assertEqual(GLOBAL_RATE_LIMIT, 500)
    
    def test_default_source_rate_limit(self):
        """Default per-source rate limit is 60."""
        self.assertEqual(DEFAULT_SOURCE_RATE_LIMIT, 60)
    
    def test_asset_keywords_btc_exists(self):
        """Asset keywords list exists for BTC."""
        self.assertIsInstance(ASSET_KEYWORDS_BTC, list)
        self.assertTrue(len(ASSET_KEYWORDS_BTC) > 0)
    
    def test_asset_keywords_contains_btc(self):
        """Keywords contain standard BTC terms."""
        lower_keywords = [k.lower() for k in ASSET_KEYWORDS_BTC]
        self.assertIn("btc", lower_keywords)
        self.assertIn("bitcoin", lower_keywords)


# =============================================================================
# ENUM TESTS
# =============================================================================

class TestEnums(unittest.TestCase):
    """Test enum definitions."""
    
    def test_twitter_source_type_values(self):
        """TwitterSourceType has account, list, query."""
        self.assertEqual(TwitterSourceType.ACCOUNT.value, "account")
        self.assertEqual(TwitterSourceType.LIST.value, "list")
        self.assertEqual(TwitterSourceType.QUERY.value, "query")
    
    def test_source_role_values(self):
        """SourceRole has news, analyst, community."""
        self.assertEqual(SourceRole.NEWS.value, "news")
        self.assertEqual(SourceRole.ANALYST.value, "analyst")
        self.assertEqual(SourceRole.COMMUNITY.value, "community")
    
    def test_tweet_drop_reason_values(self):
        """TweetDropReason covers all drop cases."""
        reasons = [r.value for r in TweetDropReason]
        self.assertIn("not_whitelisted", reasons)
        self.assertIn("source_disabled", reasons)
        self.assertIn("source_rate_exceeded", reasons)
        self.assertIn("global_rate_exceeded", reasons)
        self.assertIn("empty_text", reasons)
        self.assertIn("no_asset_keyword", reasons)
        self.assertIn("invalid_timestamp", reasons)
        self.assertIn("low_precision_timestamp", reasons)
        self.assertIn("missing_required_field", reasons)
        self.assertIn("already_processed", reasons)


# =============================================================================
# DATA CLASS TESTS
# =============================================================================

class TestTwitterSource(unittest.TestCase):
    """Test TwitterSource data class."""
    
    def test_create_account_source(self):
        """Can create account type source."""
        source = create_test_source(
            source_type=TwitterSourceType.ACCOUNT,
            value="SatoshiNakamoto"
        )
        self.assertEqual(source.source_type, TwitterSourceType.ACCOUNT)
        self.assertEqual(source.value, "SatoshiNakamoto")
    
    def test_create_list_source(self):
        """Can create list type source."""
        source = create_test_source(
            source_type=TwitterSourceType.LIST,
            value="1234567890"
        )
        self.assertEqual(source.source_type, TwitterSourceType.LIST)
    
    def test_create_query_source(self):
        """Can create query type source."""
        source = create_test_source(
            source_type=TwitterSourceType.QUERY,
            value="BTC halving 2024"
        )
        self.assertEqual(source.source_type, TwitterSourceType.QUERY)
    
    def test_source_to_dict(self):
        """Source converts to dictionary."""
        source = create_test_source(id=42, value="testaccount")
        d = source.to_dict()
        self.assertEqual(d["id"], 42)
        self.assertEqual(d["value"], "testaccount")
        self.assertEqual(d["source_type"], "account")
        self.assertEqual(d["role"], "analyst")
    
    def test_source_enabled_default(self):
        """Source enabled field exists."""
        source = create_test_source(enabled=True)
        self.assertTrue(source.enabled)
    
    def test_source_max_tweets_per_min(self):
        """Source has max_tweets_per_min field."""
        source = create_test_source(max_tweets_per_min=30)
        self.assertEqual(source.max_tweets_per_min, 30)
    
    def test_source_priority(self):
        """Source has priority field."""
        source = create_test_source(priority=5)
        self.assertEqual(source.priority, 5)


class TestRawTweet(unittest.TestCase):
    """Test RawTweet data class."""
    
    def test_create_raw_tweet(self):
        """Can create raw tweet with all fields."""
        tweet = create_test_tweet(
            tweet_id="987654321",
            text="Bitcoin to the moon!",
            like_count=500,
            retweet_count=200,
            reply_count=50
        )
        self.assertEqual(tweet.tweet_id, "987654321")
        self.assertEqual(tweet.text, "Bitcoin to the moon!")
        self.assertEqual(tweet.like_count, 500)
        self.assertEqual(tweet.retweet_count, 200)
        self.assertEqual(tweet.reply_count, 50)
    
    def test_raw_tweet_has_required_fields(self):
        """Raw tweet has all required fields per contract."""
        tweet = create_test_tweet()
        
        # Required: tweet_id, created_at, text
        self.assertIsNotNone(tweet.tweet_id)
        self.assertIsNotNone(tweet.created_at)
        self.assertIsNotNone(tweet.text)
        
        # Required: like_count, retweet_count, reply_count
        self.assertIsNotNone(tweet.like_count)
        self.assertIsNotNone(tweet.retweet_count)
        self.assertIsNotNone(tweet.reply_count)
        
        # Required: author.followers_count
        self.assertIsNotNone(tweet.author_followers_count)
    
    def test_raw_tweet_to_dict(self):
        """Raw tweet converts to dictionary."""
        tweet = create_test_tweet()
        d = tweet.to_dict()
        
        self.assertIn("tweet_id", d)
        self.assertIn("created_at", d)
        self.assertIn("text", d)
        self.assertIn("like_count", d)
        self.assertIn("retweet_count", d)
        self.assertIn("reply_count", d)
        self.assertIn("author_followers_count", d)
    
    def test_raw_tweet_optional_author_username(self):
        """Author username is optional."""
        tweet = create_test_tweet(author_username=None)
        self.assertIsNone(tweet.author_username)
        
        tweet2 = create_test_tweet(author_username="elonmusk")
        self.assertEqual(tweet2.author_username, "elonmusk")


class TestProcessedTweet(unittest.TestCase):
    """Test ProcessedTweet data class."""
    
    def test_create_processed_tweet(self):
        """Can create processed tweet."""
        tweet = ProcessedTweet(
            tweet_id="12345",
            text="BTC bullish!",
            event_time="2025-01-17T10:00:00Z",
            asset="BTC"
        )
        self.assertEqual(tweet.tweet_id, "12345")
        self.assertEqual(tweet.asset, "BTC")
    
    def test_processed_tweet_default_values(self):
        """Processed tweet has correct defaults."""
        tweet = ProcessedTweet(
            tweet_id="12345",
            text="BTC",
            event_time="2025-01-17T10:00:00Z",
            asset="BTC"
        )
        
        self.assertEqual(tweet.source, "twitter")
        self.assertEqual(tweet.source_reliability, SOURCE_RELIABILITY_TWITTER)
        self.assertEqual(tweet.like_count, 0)
        self.assertEqual(tweet.velocity, 0.0)
    
    def test_processed_tweet_to_dict(self):
        """Processed tweet converts to dictionary."""
        tweet = ProcessedTweet(
            tweet_id="12345",
            text="BTC bullish!",
            event_time="2025-01-17T10:00:00Z",
            asset="BTC",
            engagement_weight=2.5,
            author_weight=9.21
        )
        d = tweet.to_dict()
        
        self.assertEqual(d["tweet_id"], "12345")
        self.assertEqual(d["engagement_weight"], 2.5)
        self.assertEqual(d["author_weight"], 9.21)
        self.assertIn("fingerprint", d)


class TestIngestionState(unittest.TestCase):
    """Test IngestionState data class."""
    
    def test_create_ingestion_state(self):
        """Can create ingestion state."""
        state = IngestionState(source_id=1)
        self.assertEqual(state.source_id, 1)
        self.assertIsNone(state.last_processed_tweet_id)
        self.assertEqual(state.tweets_processed, 0)
    
    def test_ingestion_state_to_dict(self):
        """State converts to dictionary."""
        now = datetime.now(timezone.utc)
        state = IngestionState(
            source_id=42,
            last_processed_tweet_id="99999",
            last_processed_event_time=now,
            tweets_processed=100
        )
        d = state.to_dict()
        
        self.assertEqual(d["source_id"], 42)
        self.assertEqual(d["last_processed_tweet_id"], "99999")
        self.assertEqual(d["tweets_processed"], 100)
    
    def test_ingestion_state_from_dict(self):
        """State can be loaded from dictionary."""
        data = {
            "source_id": 5,
            "last_processed_tweet_id": "12345",
            "last_processed_event_time": "2025-01-17T10:00:00+00:00",
            "tweets_processed": 50
        }
        state = IngestionState.from_dict(data)
        
        self.assertEqual(state.source_id, 5)
        self.assertEqual(state.last_processed_tweet_id, "12345")
        self.assertEqual(state.tweets_processed, 50)


class TestIngestionMetrics(unittest.TestCase):
    """Test IngestionMetrics data class."""
    
    def test_create_metrics(self):
        """Can create metrics with defaults."""
        metrics = IngestionMetrics()
        self.assertEqual(metrics.received, 0)
        self.assertEqual(metrics.accepted, 0)
        self.assertEqual(metrics.dropped_not_whitelisted, 0)
    
    def test_metrics_to_dict(self):
        """Metrics converts to dictionary."""
        metrics = IngestionMetrics()
        metrics.received = 100
        metrics.accepted = 80
        metrics.dropped_no_keyword = 20
        
        d = metrics.to_dict()
        self.assertEqual(d["received"], 100)
        self.assertEqual(d["accepted"], 80)
        self.assertEqual(d["dropped_no_keyword"], 20)
    
    def test_metrics_reset(self):
        """Metrics can be reset."""
        metrics = IngestionMetrics()
        metrics.received = 100
        metrics.accepted = 80
        
        metrics.reset()
        
        self.assertEqual(metrics.received, 0)
        self.assertEqual(metrics.accepted, 0)


# =============================================================================
# TWITTER SOURCE REGISTRY TESTS
# =============================================================================

class TestTwitterSourceRegistry(unittest.TestCase):
    """Test TwitterSourceRegistry."""
    
    def test_create_empty_registry(self):
        """Can create empty registry."""
        registry = TwitterSourceRegistry()
        self.assertEqual(registry.count(), 0)
    
    def test_load_from_list(self):
        """Can load sources from list."""
        sources = [
            create_test_source(id=1, value="user1"),
            create_test_source(id=2, value="user2"),
            create_test_source(id=3, value="user3")
        ]
        
        registry = TwitterSourceRegistry()
        count = registry.load_from_list(sources)
        
        self.assertEqual(count, 3)
        self.assertEqual(registry.count(), 3)
    
    def test_is_whitelisted_returns_true_for_registered(self):
        """is_whitelisted returns True for registered sources."""
        registry = TwitterSourceRegistry()
        registry.load_from_list([create_test_source(id=42)])
        
        self.assertTrue(registry.is_whitelisted(42))
    
    def test_is_whitelisted_returns_false_for_unknown(self):
        """is_whitelisted returns False for unknown sources."""
        registry = TwitterSourceRegistry()
        registry.load_from_list([create_test_source(id=1)])
        
        self.assertFalse(registry.is_whitelisted(999))
    
    def test_get_source_returns_source(self):
        """get_source returns the source object."""
        source = create_test_source(id=10, value="cryptonews")
        registry = TwitterSourceRegistry()
        registry.load_from_list([source])
        
        result = registry.get_source(10)
        self.assertIsNotNone(result)
        self.assertEqual(result.value, "cryptonews")
    
    def test_get_source_returns_none_for_unknown(self):
        """get_source returns None for unknown ID."""
        registry = TwitterSourceRegistry()
        registry.load_from_list([create_test_source(id=1)])
        
        result = registry.get_source(999)
        self.assertIsNone(result)
    
    def test_get_source_by_value(self):
        """Can get source by value (username)."""
        source = create_test_source(id=1, value="BTCMagazine")
        registry = TwitterSourceRegistry()
        registry.load_from_list([source])
        
        result = registry.get_source_by_value("BTCMagazine")
        self.assertIsNotNone(result)
        self.assertEqual(result.id, 1)
    
    def test_get_source_by_value_case_insensitive(self):
        """get_source_by_value is case insensitive."""
        source = create_test_source(id=1, value="BTCMagazine")
        registry = TwitterSourceRegistry()
        registry.load_from_list([source])
        
        result = registry.get_source_by_value("btcmagazine")
        self.assertIsNotNone(result)
    
    def test_get_all_sources(self):
        """Can get all sources."""
        sources = [
            create_test_source(id=1),
            create_test_source(id=2),
            create_test_source(id=3)
        ]
        
        registry = TwitterSourceRegistry()
        registry.load_from_list(sources)
        
        all_sources = registry.get_all_sources()
        self.assertEqual(len(all_sources), 3)
    
    def test_get_enabled_sources(self):
        """get_enabled_sources returns only enabled sources."""
        sources = [
            create_test_source(id=1, enabled=True),
            create_test_source(id=2, enabled=False),
            create_test_source(id=3, enabled=True)
        ]
        
        registry = TwitterSourceRegistry()
        registry.load_from_list(sources)
        
        enabled = registry.get_enabled_sources()
        self.assertEqual(len(enabled), 2)
    
    def test_get_enabled_sources_sorted_by_priority(self):
        """Enabled sources are sorted by priority descending."""
        sources = [
            create_test_source(id=1, priority=1),
            create_test_source(id=2, priority=5),
            create_test_source(id=3, priority=3)
        ]
        
        registry = TwitterSourceRegistry()
        registry.load_from_list(sources)
        
        enabled = registry.get_enabled_sources()
        self.assertEqual(enabled[0].priority, 5)
        self.assertEqual(enabled[1].priority, 3)
        self.assertEqual(enabled[2].priority, 1)
    
    def test_get_sources_by_type(self):
        """Can filter sources by type."""
        sources = [
            create_test_source(id=1, source_type=TwitterSourceType.ACCOUNT),
            create_test_source(id=2, source_type=TwitterSourceType.LIST),
            create_test_source(id=3, source_type=TwitterSourceType.ACCOUNT),
            create_test_source(id=4, source_type=TwitterSourceType.QUERY)
        ]
        
        registry = TwitterSourceRegistry()
        registry.load_from_list(sources)
        
        accounts = registry.get_sources_by_type(TwitterSourceType.ACCOUNT)
        self.assertEqual(len(accounts), 2)
        
        lists = registry.get_sources_by_type(TwitterSourceType.LIST)
        self.assertEqual(len(lists), 1)
        
        queries = registry.get_sources_by_type(TwitterSourceType.QUERY)
        self.assertEqual(len(queries), 1)
    
    def test_clear_removes_all_sources(self):
        """clear removes all sources."""
        sources = [create_test_source(id=i) for i in range(5)]
        
        registry = TwitterSourceRegistry()
        registry.load_from_list(sources)
        self.assertEqual(registry.count(), 5)
        
        registry.clear()
        self.assertEqual(registry.count(), 0)
    
    def test_load_replaces_existing(self):
        """Loading new sources replaces existing."""
        registry = TwitterSourceRegistry()
        registry.load_from_list([create_test_source(id=1)])
        self.assertEqual(registry.count(), 1)
        
        registry.load_from_list([create_test_source(id=2), create_test_source(id=3)])
        self.assertEqual(registry.count(), 2)
        self.assertFalse(registry.is_whitelisted(1))


# =============================================================================
# INGESTION STATE MANAGER TESTS
# =============================================================================

class TestIngestionStateManager(unittest.TestCase):
    """Test IngestionStateManager."""
    
    def test_create_state_manager(self):
        """Can create state manager."""
        manager = IngestionStateManager()
        self.assertIsNotNone(manager)
    
    def test_get_state_creates_new(self):
        """get_state creates new state if not exists."""
        manager = IngestionStateManager()
        state = manager.get_state(42)
        
        self.assertEqual(state.source_id, 42)
        self.assertIsNone(state.last_processed_tweet_id)
    
    def test_update_state(self):
        """Can update state after processing."""
        manager = IngestionStateManager()
        now = datetime.now(timezone.utc)
        
        manager.update_state(1, "tweet_12345", now)
        
        state = manager.get_state(1)
        self.assertEqual(state.last_processed_tweet_id, "tweet_12345")
        self.assertEqual(state.last_processed_event_time, now)
        self.assertEqual(state.tweets_processed, 1)
    
    def test_update_state_increments_count(self):
        """Update state increments tweet count."""
        manager = IngestionStateManager()
        now = datetime.now(timezone.utc)
        
        manager.update_state(1, "t1", now)
        manager.update_state(1, "t2", now)
        manager.update_state(1, "t3", now)
        
        state = manager.get_state(1)
        self.assertEqual(state.tweets_processed, 3)
    
    def test_get_last_event_time(self):
        """Can get last event time for a source."""
        manager = IngestionStateManager()
        now = datetime.now(timezone.utc)
        
        manager.update_state(1, "t1", now)
        
        last_time = manager.get_last_event_time(1)
        self.assertEqual(last_time, now)
    
    def test_get_last_event_time_returns_none(self):
        """Returns None if no state exists."""
        manager = IngestionStateManager()
        last_time = manager.get_last_event_time(999)
        self.assertIsNone(last_time)
    
    def test_is_already_processed(self):
        """Can check if tweet was already processed."""
        manager = IngestionStateManager()
        now = datetime.now(timezone.utc)
        
        manager.update_state(1, "tweet_12345", now)
        
        self.assertTrue(manager.is_already_processed(1, "tweet_12345"))
        self.assertFalse(manager.is_already_processed(1, "tweet_99999"))
    
    def test_state_persistence_to_file(self):
        """State persists to file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            # Create manager and update state
            manager = IngestionStateManager(state_file=state_file)
            now = datetime.now(timezone.utc)
            manager.update_state(1, "t1", now)
            
            # Create new manager with same file
            manager2 = IngestionStateManager(state_file=state_file)
            state = manager2.get_state(1)
            
            self.assertEqual(state.last_processed_tweet_id, "t1")
        finally:
            if os.path.exists(state_file):
                os.remove(state_file)
    
    def test_clear_removes_all_states(self):
        """clear removes all states."""
        manager = IngestionStateManager()
        now = datetime.now(timezone.utc)
        
        manager.update_state(1, "t1", now)
        manager.update_state(2, "t2", now)
        
        manager.clear()
        
        # New states are created
        state1 = manager.get_state(1)
        self.assertIsNone(state1.last_processed_tweet_id)


# =============================================================================
# TWEET RATE LIMITER TESTS
# =============================================================================

class TestTweetRateLimiter(unittest.TestCase):
    """Test TweetRateLimiter."""
    
    def test_create_rate_limiter(self):
        """Can create rate limiter."""
        limiter = TweetRateLimiter()
        self.assertEqual(limiter.global_limit, GLOBAL_RATE_LIMIT)
    
    def test_create_with_custom_limit(self):
        """Can create with custom global limit."""
        limiter = TweetRateLimiter(global_limit=100)
        self.assertEqual(limiter.global_limit, 100)
    
    def test_source_rate_allows_under_limit(self):
        """Source rate allows when under limit."""
        limiter = TweetRateLimiter()
        now = time.time()
        
        # Record some tweets
        for _ in range(29):
            limiter.record_tweet(1, now)
        
        # Should allow (under limit of 30)
        self.assertTrue(limiter.check_source_rate(1, 30, now))
    
    def test_source_rate_blocks_at_limit(self):
        """Source rate blocks at limit."""
        limiter = TweetRateLimiter()
        now = time.time()
        
        # Record tweets up to limit
        for _ in range(30):
            limiter.record_tweet(1, now)
        
        # Should block (at limit of 30)
        self.assertFalse(limiter.check_source_rate(1, 30, now))
    
    def test_global_rate_allows_under_limit(self):
        """Global rate allows when under limit."""
        limiter = TweetRateLimiter(global_limit=100)
        now = time.time()
        
        for _ in range(99):
            limiter.record_tweet(1, now)
        
        self.assertTrue(limiter.check_global_rate(now))
    
    def test_global_rate_blocks_at_limit(self):
        """Global rate blocks at limit."""
        limiter = TweetRateLimiter(global_limit=100)
        now = time.time()
        
        for _ in range(100):
            limiter.record_tweet(1, now)
        
        self.assertFalse(limiter.check_global_rate(now))
    
    def test_rate_limit_window_expiry(self):
        """Rate limits expire after window."""
        limiter = TweetRateLimiter(window_seconds=1)
        base_time = time.time()
        
        # Record at limit
        for _ in range(30):
            limiter.record_tweet(1, base_time)
        
        self.assertFalse(limiter.check_source_rate(1, 30, base_time))
        
        # After window expires
        later = base_time + 2  # 2 seconds later (window is 1 second)
        self.assertTrue(limiter.check_source_rate(1, 30, later))
    
    def test_get_source_count(self):
        """Can get current source count."""
        limiter = TweetRateLimiter()
        now = time.time()
        
        limiter.record_tweet(1, now)
        limiter.record_tweet(1, now)
        limiter.record_tweet(1, now)
        
        self.assertEqual(limiter.get_source_count(1), 3)
    
    def test_get_global_count(self):
        """Can get current global count."""
        limiter = TweetRateLimiter()
        now = time.time()
        
        limiter.record_tweet(1, now)
        limiter.record_tweet(2, now)
        limiter.record_tweet(3, now)
        
        self.assertEqual(limiter.get_global_count(), 3)
    
    def test_multiple_sources_independent(self):
        """Different sources have independent limits."""
        limiter = TweetRateLimiter()
        now = time.time()
        
        # Fill source 1
        for _ in range(30):
            limiter.record_tweet(1, now)
        
        # Source 2 should still be allowed
        self.assertTrue(limiter.check_source_rate(2, 30, now))
        
        # Source 1 should be blocked
        self.assertFalse(limiter.check_source_rate(1, 30, now))
    
    def test_clear_removes_all_records(self):
        """clear removes all records."""
        limiter = TweetRateLimiter()
        now = time.time()
        
        for _ in range(50):
            limiter.record_tweet(1, now)
        
        limiter.clear()
        
        self.assertEqual(limiter.get_source_count(1), 0)
        self.assertEqual(limiter.get_global_count(), 0)


# =============================================================================
# VELOCITY CALCULATOR TESTS
# =============================================================================

class TestVelocityCalculator(unittest.TestCase):
    """Test VelocityCalculator."""
    
    def test_create_calculator(self):
        """Can create velocity calculator."""
        calc = VelocityCalculator()
        self.assertIsNotNone(calc)
    
    def test_record_tweet(self):
        """Can record tweets."""
        calc = VelocityCalculator()
        now = time.time()
        
        calc.record_tweet(1, now)
        calc.record_tweet(1, now)
        calc.record_tweet(1, now)
        
        # No assertion, just verify no exception
    
    def test_velocity_zero_for_no_tweets(self):
        """Velocity is 0 for no tweets."""
        calc = VelocityCalculator()
        velocity = calc.get_velocity(1)
        self.assertEqual(velocity, 0.0)
    
    def test_velocity_increases_with_burst(self):
        """Velocity increases with burst of tweets."""
        calc = VelocityCalculator(short_window=10, long_window=60)
        now = time.time()
        
        # Record some older tweets
        for i in range(10):
            calc.record_tweet(1, now - 30)  # 30 seconds ago
        
        # Record burst of recent tweets
        for i in range(10):
            calc.record_tweet(1, now)  # Now
        
        velocity = calc.get_velocity(1, now)
        
        # Velocity should be > 1 (short rate higher than long rate)
        self.assertGreater(velocity, 1.0)
    
    def test_clear_removes_all(self):
        """clear removes all records."""
        calc = VelocityCalculator()
        now = time.time()
        
        for _ in range(10):
            calc.record_tweet(1, now)
        
        calc.clear()
        
        self.assertEqual(calc.get_velocity(1, now), 0.0)


# =============================================================================
# METRICS CALCULATION TESTS
# =============================================================================

class TestMetricsCalculation(unittest.TestCase):
    """Test metric calculation functions."""
    
    def test_safe_log_zero(self):
        """safe_log handles zero."""
        self.assertEqual(safe_log(0), 0.0)
    
    def test_safe_log_negative(self):
        """safe_log handles negative values."""
        self.assertEqual(safe_log(-5), 0.0)
    
    def test_safe_log_positive(self):
        """safe_log returns log(x+1) for positive values."""
        result = safe_log(10)
        expected = math.log(11)
        self.assertAlmostEqual(result, expected, places=5)
    
    def test_compute_total_engagement(self):
        """Total engagement: likes + 2*retweets + replies."""
        total = compute_total_engagement(100, 50, 10)
        # 100 + 2*50 + 10 = 210
        self.assertEqual(total, 210)
    
    def test_compute_total_engagement_zeros(self):
        """Total engagement with zeros."""
        total = compute_total_engagement(0, 0, 0)
        self.assertEqual(total, 0)
    
    def test_compute_engagement_weight(self):
        """Engagement weight is log of total engagement."""
        weight = compute_engagement_weight(100, 50, 10)
        expected = round(math.log(211), 4)  # log(1 + 210)
        self.assertEqual(weight, expected)
    
    def test_compute_engagement_weight_zeros(self):
        """Engagement weight for zero engagement."""
        weight = compute_engagement_weight(0, 0, 0)
        self.assertEqual(weight, 0.0)
    
    def test_compute_author_weight(self):
        """Author weight is log of followers."""
        weight = compute_author_weight(10000)
        expected = round(math.log(10001), 4)
        self.assertEqual(weight, expected)
    
    def test_compute_author_weight_zero(self):
        """Author weight for zero followers."""
        weight = compute_author_weight(0)
        self.assertEqual(weight, 0.0)


# =============================================================================
# TWEET VALIDATOR TESTS
# =============================================================================

class TestTweetValidator(unittest.TestCase):
    """Test TweetValidator."""
    
    def test_create_validator(self):
        """Can create validator."""
        validator = TweetValidator()
        self.assertIsNotNone(validator)
    
    def test_validate_valid_tweet(self):
        """Valid tweet passes validation."""
        validator = TweetValidator()
        tweet = create_test_tweet(text="BTC looking strong!")
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertTrue(is_valid)
        self.assertIsNone(reason)
    
    def test_validate_missing_tweet_id(self):
        """Tweet without ID fails."""
        validator = TweetValidator()
        tweet = create_test_tweet()
        tweet.tweet_id = None
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.MISSING_REQUIRED_FIELD)
    
    def test_validate_missing_created_at(self):
        """Tweet without created_at fails."""
        validator = TweetValidator()
        tweet = create_test_tweet()
        tweet.created_at = None
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.INVALID_TIMESTAMP)
    
    def test_validate_empty_text(self):
        """Tweet with empty text fails."""
        validator = TweetValidator()
        tweet = create_test_tweet(text="")
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.EMPTY_TEXT)
    
    def test_validate_whitespace_only_text(self):
        """Tweet with whitespace-only text fails."""
        validator = TweetValidator()
        tweet = create_test_tweet(text="   \n\t  ")
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.EMPTY_TEXT)
    
    def test_validate_no_asset_keyword(self):
        """Tweet without asset keyword fails."""
        validator = TweetValidator()
        tweet = create_test_tweet(text="The market is moving today!")
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.NO_ASSET_KEYWORD)
    
    def test_validate_with_btc_keyword(self):
        """Tweet with BTC keyword passes."""
        validator = TweetValidator()
        tweet = create_test_tweet(text="BTC is the future")
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertTrue(is_valid)
    
    def test_validate_with_bitcoin_keyword(self):
        """Tweet with Bitcoin keyword passes."""
        validator = TweetValidator()
        tweet = create_test_tweet(text="Bitcoin breaking records!")
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertTrue(is_valid)
    
    def test_validate_keyword_case_insensitive(self):
        """Asset keyword matching is case insensitive."""
        validator = TweetValidator()
        
        tweets = [
            create_test_tweet(text="BTC news"),
            create_test_tweet(text="btc news"),
            create_test_tweet(text="Bitcoin news"),
            create_test_tweet(text="BITCOIN news")
        ]
        
        for tweet in tweets:
            is_valid, reason = validator.validate_tweet(tweet)
            self.assertTrue(is_valid, f"Failed for: {tweet.text}")
    
    def test_validate_low_precision_timestamp(self):
        """Tweet with minute-only timestamp fails."""
        validator = TweetValidator()
        
        # Timestamp with no seconds (minute precision only)
        low_precision = datetime(2025, 1, 17, 10, 0, 0, tzinfo=timezone.utc)
        tweet = create_test_tweet(text="BTC news", created_at=low_precision)
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.LOW_PRECISION_TIMESTAMP)
    
    def test_validate_second_precision_timestamp(self):
        """Tweet with second precision passes."""
        validator = TweetValidator()
        
        # Timestamp with seconds
        good_precision = datetime(2025, 1, 17, 10, 30, 45, tzinfo=timezone.utc)
        tweet = create_test_tweet(text="BTC news", created_at=good_precision)
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertTrue(is_valid)
    
    def test_validate_missing_like_count(self):
        """Tweet without like_count fails."""
        validator = TweetValidator()
        tweet = create_test_tweet()
        tweet.like_count = None
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, TweetDropReason.MISSING_REQUIRED_FIELD)
    
    def test_validate_missing_retweet_count(self):
        """Tweet without retweet_count fails."""
        validator = TweetValidator()
        tweet = create_test_tweet()
        tweet.retweet_count = None
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
    
    def test_validate_missing_reply_count(self):
        """Tweet without reply_count fails."""
        validator = TweetValidator()
        tweet = create_test_tweet()
        tweet.reply_count = None
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)
    
    def test_validate_missing_followers_count(self):
        """Tweet without author_followers_count fails."""
        validator = TweetValidator()
        tweet = create_test_tweet()
        tweet.author_followers_count = None
        
        is_valid, reason = validator.validate_tweet(tweet)
        
        self.assertFalse(is_valid)


# =============================================================================
# TWITTER INGESTION WORKER TESTS
# =============================================================================

class TestTwitterIngestionWorkerBasic(unittest.TestCase):
    """Test TwitterIngestionWorker basic functionality."""
    
    def test_create_worker(self):
        """Can create worker."""
        registry = TwitterSourceRegistry()
        worker = TwitterIngestionWorker(registry=registry)
        self.assertIsNotNone(worker)
    
    def test_worker_starts_stopped(self):
        """Worker starts in stopped state."""
        worker = create_test_worker()
        self.assertFalse(worker.is_running)
    
    def test_worker_start(self):
        """Can start worker."""
        worker = create_test_worker()
        worker.start()
        self.assertTrue(worker.is_running)
    
    def test_worker_stop(self):
        """Can stop worker."""
        worker = create_test_worker()
        worker.start()
        worker.stop()
        self.assertFalse(worker.is_running)
    
    def test_get_enabled_sources(self):
        """Can get enabled sources from worker."""
        sources = [
            create_test_source(id=1, enabled=True),
            create_test_source(id=2, enabled=False),
            create_test_source(id=3, enabled=True)
        ]
        worker = create_test_worker(sources=sources)
        
        enabled = worker.get_enabled_sources()
        self.assertEqual(len(enabled), 2)
    
    def test_get_source_state(self):
        """Can get source state from worker."""
        worker = create_test_worker()
        state = worker.get_source_state(1)
        
        self.assertEqual(state.source_id, 1)


class TestTwitterIngestionWorkerWhitelist(unittest.TestCase):
    """Test whitelist enforcement in TwitterIngestionWorker."""
    
    def test_non_whitelisted_source_rejected(self):
        """Tweets from non-whitelisted source are rejected."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=999)  # Not whitelisted
        result = worker.handle_tweet(tweet)
        
        self.assertIsNone(result)
    
    def test_whitelisted_source_accepted(self):
        """Tweets from whitelisted source are accepted."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC news!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
    
    def test_disabled_source_rejected(self):
        """Tweets from disabled source are rejected."""
        sources = [create_test_source(id=1, enabled=False)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC news!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNone(result)
    
    def test_metrics_track_non_whitelisted(self):
        """Metrics track non-whitelisted drops."""
        worker = create_test_worker(sources=[create_test_source(id=1)])
        
        tweet = create_test_tweet(source_id=999)
        worker.handle_tweet(tweet)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["dropped_not_whitelisted"], 1)
    
    def test_no_source_id_rejected(self):
        """Tweet without source_id is rejected."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=None)
        result = worker.handle_tweet(tweet)
        
        self.assertIsNone(result)


class TestTwitterIngestionWorkerRateLimits(unittest.TestCase):
    """Test rate limiting in TwitterIngestionWorker."""
    
    def test_source_rate_limit_enforced(self):
        """Per-source rate limit is enforced."""
        sources = [create_test_source(id=1, max_tweets_per_min=5)]
        worker = create_test_worker(sources=sources)
        
        now = time.time()
        accepted = 0
        
        for i in range(10):
            tweet = create_test_tweet(
                tweet_id=str(i),
                source_id=1,
                text="BTC news!"
            )
            result = worker.handle_tweet(tweet, now=now)
            if result:
                accepted += 1
        
        self.assertEqual(accepted, 5)
    
    def test_global_rate_limit_enforced(self):
        """Global rate limit is enforced."""
        sources = [
            create_test_source(id=1, max_tweets_per_min=100),
            create_test_source(id=2, max_tweets_per_min=100)
        ]
        worker = create_test_worker(sources=sources, global_rate_limit=10)
        
        now = time.time()
        accepted = 0
        
        for i in range(20):
            source_id = 1 if i % 2 == 0 else 2
            tweet = create_test_tweet(
                tweet_id=str(i),
                source_id=source_id,
                text="BTC news!"
            )
            result = worker.handle_tweet(tweet, now=now)
            if result:
                accepted += 1
        
        self.assertEqual(accepted, 10)
    
    def test_metrics_track_rate_limit_drops(self):
        """Metrics track rate limit drops."""
        sources = [create_test_source(id=1, max_tweets_per_min=2)]
        worker = create_test_worker(sources=sources)
        
        now = time.time()
        
        for i in range(5):
            tweet = create_test_tweet(
                tweet_id=str(i),
                source_id=1,
                text="BTC!"
            )
            worker.handle_tweet(tweet, now=now)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["dropped_source_rate"], 3)


class TestTwitterIngestionWorkerValidation(unittest.TestCase):
    """Test tweet validation in TwitterIngestionWorker."""
    
    def test_empty_text_rejected(self):
        """Empty text tweets are rejected."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNone(result)
    
    def test_no_asset_keyword_rejected(self):
        """Tweets without asset keyword are rejected."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="No crypto here!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNone(result)
    
    def test_missing_required_field_rejected(self):
        """Tweets with missing required fields are rejected."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC news")
        tweet.like_count = None
        result = worker.handle_tweet(tweet)
        
        self.assertIsNone(result)


class TestTwitterIngestionWorkerMetrics(unittest.TestCase):
    """Test metric computation in TwitterIngestionWorker."""
    
    def test_processed_tweet_has_engagement_metrics(self):
        """Processed tweet has engagement metrics."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(
            source_id=1,
            text="BTC news!",
            like_count=100,
            retweet_count=50,
            reply_count=10
        )
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
        # 100 + 2*50 + 10 = 210
        self.assertEqual(result.total_engagement, 210)
        self.assertGreater(result.engagement_weight, 0)
    
    def test_processed_tweet_has_author_weight(self):
        """Processed tweet has author weight."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(
            source_id=1,
            text="BTC news!",
            author_followers_count=10000
        )
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
        self.assertGreater(result.author_weight, 0)
    
    def test_processed_tweet_has_fingerprint(self):
        """Processed tweet has fingerprint."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.fingerprint)
        self.assertEqual(len(result.fingerprint), 16)
    
    def test_processed_tweet_has_event_time(self):
        """Processed tweet has formatted event_time."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
        self.assertTrue(result.event_time.endswith("Z"))
        self.assertIn("T", result.event_time)
    
    def test_processed_tweet_has_source_reliability(self):
        """Processed tweet has correct source reliability."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "twitter")
        self.assertEqual(result.source_reliability, 0.5)
    
    def test_processed_tweet_has_role(self):
        """Processed tweet has role from source."""
        sources = [create_test_source(id=1, role=SourceRole.NEWS)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        result = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.role, "news")


class TestTwitterIngestionWorkerState(unittest.TestCase):
    """Test state management in TwitterIngestionWorker."""
    
    def test_state_updated_after_processing(self):
        """State is updated after processing tweet."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(
            tweet_id="12345",
            source_id=1,
            text="BTC!"
        )
        worker.handle_tweet(tweet)
        
        state = worker.get_source_state(1)
        self.assertEqual(state.last_processed_tweet_id, "12345")
        self.assertEqual(state.tweets_processed, 1)
    
    def test_already_processed_rejected(self):
        """Already processed tweets are rejected."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(
            tweet_id="12345",
            source_id=1,
            text="BTC!"
        )
        
        result1 = worker.handle_tweet(tweet)
        result2 = worker.handle_tweet(tweet)
        
        self.assertIsNotNone(result1)
        self.assertIsNone(result2)
    
    def test_metrics_track_already_processed(self):
        """Metrics track already processed drops."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(
            tweet_id="12345",
            source_id=1,
            text="BTC!"
        )
        
        worker.handle_tweet(tweet)
        worker.handle_tweet(tweet)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["dropped_already_processed"], 1)


class TestTwitterIngestionWorkerBatch(unittest.TestCase):
    """Test batch processing in TwitterIngestionWorker."""
    
    def test_process_batch(self):
        """Can process batch of tweets."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        tweets = [
            create_test_tweet(tweet_id=str(i), text="BTC!", created_at=now + timedelta(seconds=i))
            for i in range(5)
        ]
        
        results = worker.process_batch(tweets, source_id=1)
        
        self.assertEqual(len(results), 5)
    
    def test_batch_filters_by_last_time(self):
        """Batch filters tweets older than last processed time."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        now = datetime.now(timezone.utc)
        
        # Process one tweet to set last time
        first_tweet = create_test_tweet(
            tweet_id="first",
            text="BTC!",
            created_at=now
        )
        worker.handle_tweet(first_tweet, source_id=1)
        
        # Batch with mixed old/new tweets
        tweets = [
            create_test_tweet(tweet_id="old", text="BTC!", created_at=now - timedelta(seconds=10)),
            create_test_tweet(tweet_id="new1", text="BTC!", created_at=now + timedelta(seconds=1)),
            create_test_tweet(tweet_id="new2", text="BTC!", created_at=now + timedelta(seconds=2))
        ]
        
        results = worker.process_batch(tweets, source_id=1)
        
        # Only new tweets should be processed
        self.assertEqual(len(results), 2)


class TestTwitterIngestionWorkerCallback(unittest.TestCase):
    """Test callback functionality in TwitterIngestionWorker."""
    
    def test_callback_invoked_on_accept(self):
        """Callback is invoked when tweet is accepted."""
        sources = [create_test_source(id=1)]
        received = []
        
        def on_tweet(tweet):
            received.append(tweet)
        
        worker = TwitterIngestionWorker(
            registry=TwitterSourceRegistry(),
            on_tweet=on_tweet
        )
        worker.registry.load_from_list(sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        worker.handle_tweet(tweet)
        
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].tweet_id, tweet.tweet_id)
    
    def test_callback_not_invoked_on_reject(self):
        """Callback is not invoked when tweet is rejected."""
        sources = [create_test_source(id=1)]
        received = []
        
        def on_tweet(tweet):
            received.append(tweet)
        
        worker = TwitterIngestionWorker(
            registry=TwitterSourceRegistry(),
            on_tweet=on_tweet
        )
        worker.registry.load_from_list(sources)
        
        tweet = create_test_tweet(source_id=999, text="BTC!")  # Not whitelisted
        worker.handle_tweet(tweet)
        
        self.assertEqual(len(received), 0)
    
    def test_callback_error_does_not_crash(self):
        """Callback error does not crash worker."""
        sources = [create_test_source(id=1)]
        
        def on_tweet(tweet):
            raise Exception("Callback error!")
        
        worker = TwitterIngestionWorker(
            registry=TwitterSourceRegistry(),
            on_tweet=on_tweet
        )
        worker.registry.load_from_list(sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        result = worker.handle_tweet(tweet)
        
        # Should still return result
        self.assertIsNotNone(result)


class TestTwitterIngestionWorkerMetricsReset(unittest.TestCase):
    """Test metrics reset in TwitterIngestionWorker."""
    
    def test_get_metrics(self):
        """Can get metrics."""
        worker = create_test_worker()
        metrics = worker.get_metrics()
        
        self.assertIn("received", metrics)
        self.assertIn("accepted", metrics)
    
    def test_reset_metrics(self):
        """Can reset metrics."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        tweet = create_test_tweet(source_id=1, text="BTC!")
        worker.handle_tweet(tweet)
        
        metrics_before = worker.get_metrics()
        self.assertEqual(metrics_before["accepted"], 1)
        
        worker.reset_metrics()
        
        metrics_after = worker.get_metrics()
        self.assertEqual(metrics_after["accepted"], 0)


# =============================================================================
# FACTORY FUNCTION TESTS
# =============================================================================

class TestFactoryFunctions(unittest.TestCase):
    """Test factory functions."""
    
    def test_create_source_registry(self):
        """Can create source registry."""
        registry = create_source_registry()
        self.assertIsInstance(registry, TwitterSourceRegistry)
    
    def test_create_ingestion_worker(self):
        """Can create ingestion worker."""
        worker = create_ingestion_worker()
        self.assertIsInstance(worker, TwitterIngestionWorker)
    
    def test_create_ingestion_worker_with_registry(self):
        """Can create worker with custom registry."""
        registry = create_source_registry()
        registry.load_from_list([create_test_source(id=1)])
        
        worker = create_ingestion_worker(registry=registry)
        
        self.assertEqual(worker.registry.count(), 1)
    
    def test_create_ingestion_worker_with_state_file(self):
        """Can create worker with state file."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            worker = create_ingestion_worker(state_file=state_file)
            self.assertIsNotNone(worker)
        finally:
            if os.path.exists(state_file):
                os.remove(state_file)
    
    def test_create_ingestion_worker_with_callback(self):
        """Can create worker with callback."""
        received = []
        worker = create_ingestion_worker(on_tweet=lambda t: received.append(t))
        
        worker.registry.load_from_list([create_test_source(id=1)])
        tweet = create_test_tweet(source_id=1, text="BTC!")
        worker.handle_tweet(tweet)
        
        self.assertEqual(len(received), 1)
    
    def test_create_test_worker(self):
        """Can create test worker."""
        worker = create_test_worker()
        self.assertIsInstance(worker, TwitterIngestionWorker)
    
    def test_create_test_worker_with_sources(self):
        """Can create test worker with sources."""
        sources = [
            create_test_source(id=1),
            create_test_source(id=2)
        ]
        worker = create_test_worker(sources=sources)
        
        self.assertEqual(worker.registry.count(), 2)
    
    def test_get_create_twitter_sources_sql(self):
        """Can get SQL for creating tables."""
        sql = get_create_twitter_sources_sql()
        
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("twitter_sources", sql)
        self.assertIn("source_type", sql)
        self.assertIn("twitter_ingestion_state", sql)


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestTwitterIngestionIntegration(unittest.TestCase):
    """Integration tests for Twitter ingestion."""
    
    def test_full_ingestion_flow(self):
        """Test complete ingestion flow."""
        # Setup sources
        sources = [
            create_test_source(
                id=1,
                source_type=TwitterSourceType.ACCOUNT,
                value="CryptoNews",
                role=SourceRole.NEWS,
                max_tweets_per_min=10
            ),
            create_test_source(
                id=2,
                source_type=TwitterSourceType.ACCOUNT,
                value="BTCAnalyst",
                role=SourceRole.ANALYST,
                max_tweets_per_min=10
            )
        ]
        
        received = []
        worker = TwitterIngestionWorker(
            registry=TwitterSourceRegistry(),
            on_tweet=lambda t: received.append(t)
        )
        worker.registry.load_from_list(sources)
        worker.start()
        
        now = datetime.now(timezone.utc)
        
        # Ingest tweets from different sources
        tweets = [
            create_test_tweet(
                tweet_id="1",
                source_id=1,
                text="Breaking: BTC reaches new high!",
                like_count=1000,
                retweet_count=500,
                author_followers_count=100000,
                created_at=now
            ),
            create_test_tweet(
                tweet_id="2",
                source_id=2,
                text="BTC technical analysis shows bullish pattern",
                like_count=200,
                retweet_count=50,
                author_followers_count=50000,
                created_at=now + timedelta(seconds=1)
            ),
            create_test_tweet(
                tweet_id="3",
                source_id=999,  # Not whitelisted
                text="Random BTC tweet",
                created_at=now + timedelta(seconds=2)
            )
        ]
        
        for tweet in tweets:
            worker.handle_tweet(tweet)
        
        worker.stop()
        
        # Verify results
        self.assertEqual(len(received), 2)
        
        # Check metrics
        metrics = worker.get_metrics()
        self.assertEqual(metrics["received"], 3)
        self.assertEqual(metrics["accepted"], 2)
        self.assertEqual(metrics["dropped_not_whitelisted"], 1)
        
        # Check first tweet
        first = received[0]
        self.assertEqual(first.tweet_id, "1")
        self.assertEqual(first.role, "news")
        # 1000 + 2*500 + 10 (default reply_count) = 2010
        self.assertEqual(first.total_engagement, 2010)
        
        # Check second tweet
        second = received[1]
        self.assertEqual(second.tweet_id, "2")
        self.assertEqual(second.role, "analyst")
    
    def test_rate_limiting_across_sources(self):
        """Test rate limiting works across multiple sources."""
        sources = [
            create_test_source(id=1, max_tweets_per_min=3),
            create_test_source(id=2, max_tweets_per_min=3)
        ]
        
        worker = create_test_worker(sources=sources, global_rate_limit=5)
        now = time.time()
        
        accepted = 0
        for i in range(10):
            source_id = 1 if i % 2 == 0 else 2
            tweet = create_test_tweet(
                tweet_id=str(i),
                source_id=source_id,
                text="BTC news!"
            )
            result = worker.handle_tweet(tweet, now=now)
            if result:
                accepted += 1
        
        # Global limit is 5, so max 5 accepted
        self.assertEqual(accepted, 5)
    
    def test_state_persistence_integration(self):
        """Test state persists correctly."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            sources = [create_test_source(id=1)]
            
            # First worker session
            worker1 = create_ingestion_worker(state_file=state_file)
            worker1.registry.load_from_list(sources)
            
            now = datetime.now(timezone.utc)
            tweet = create_test_tweet(
                tweet_id="persist_test",
                source_id=1,
                text="BTC!",
                created_at=now
            )
            worker1.handle_tweet(tweet)
            
            # Second worker session (restart)
            worker2 = create_ingestion_worker(state_file=state_file)
            worker2.registry.load_from_list(sources)
            
            state = worker2.get_source_state(1)
            self.assertEqual(state.last_processed_tweet_id, "persist_test")
            
            # Same tweet should be rejected
            result = worker2.handle_tweet(tweet)
            self.assertIsNone(result)
            
        finally:
            if os.path.exists(state_file):
                os.remove(state_file)
    
    def test_different_source_types(self):
        """Test different source types work correctly."""
        sources = [
            create_test_source(
                id=1,
                source_type=TwitterSourceType.ACCOUNT,
                value="BTCNews"
            ),
            create_test_source(
                id=2,
                source_type=TwitterSourceType.LIST,
                value="1234567890"
            ),
            create_test_source(
                id=3,
                source_type=TwitterSourceType.QUERY,
                value="BTC halving"
            )
        ]
        
        registry = create_source_registry()
        registry.load_from_list(sources)
        
        accounts = registry.get_sources_by_type(TwitterSourceType.ACCOUNT)
        lists = registry.get_sources_by_type(TwitterSourceType.LIST)
        queries = registry.get_sources_by_type(TwitterSourceType.QUERY)
        
        self.assertEqual(len(accounts), 1)
        self.assertEqual(len(lists), 1)
        self.assertEqual(len(queries), 1)


class TestSafetyGuarantees(unittest.TestCase):
    """Test safety guarantees per contract."""
    
    def test_no_ingestion_outside_whitelist(self):
        """System MUST NOT ingest tweets outside whitelist."""
        sources = [create_test_source(id=1)]
        worker = create_test_worker(sources=sources)
        
        # Try many non-whitelisted tweets
        for i in range(100):
            tweet = create_test_tweet(
                tweet_id=str(i),
                source_id=i + 1000,  # All non-whitelisted
                text="BTC news!"
            )
            result = worker.handle_tweet(tweet)
            self.assertIsNone(result)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["accepted"], 0)
        self.assertEqual(metrics["dropped_not_whitelisted"], 100)
    
    def test_bounded_ingestion(self):
        """Twitter ingestion MUST be bounded and deterministic."""
        sources = [create_test_source(id=1, max_tweets_per_min=100)]
        worker = create_test_worker(sources=sources, global_rate_limit=50)
        
        now = time.time()
        accepted = 0
        
        # Try to ingest 1000 tweets
        for i in range(1000):
            tweet = create_test_tweet(
                tweet_id=str(i),
                source_id=1,
                text="BTC news!"
            )
            result = worker.handle_tweet(tweet, now=now)
            if result:
                accepted += 1
        
        # Should be bounded by global limit
        self.assertEqual(accepted, 50)
    
    def test_deterministic_filtering(self):
        """Same tweets should produce same results."""
        sources = [create_test_source(id=1)]
        
        tweet = create_test_tweet(
            tweet_id="deterministic",
            source_id=1,
            text="BTC news!",
            like_count=100,
            retweet_count=50,
            reply_count=10
        )
        
        # Run twice with fresh workers
        worker1 = create_test_worker(sources=sources)
        result1 = worker1.handle_tweet(tweet)
        
        worker2 = create_test_worker(sources=sources)
        result2 = worker2.handle_tweet(tweet)
        
        # Results should match
        self.assertEqual(result1.total_engagement, result2.total_engagement)
        self.assertEqual(result1.engagement_weight, result2.engagement_weight)
        self.assertEqual(result1.fingerprint, result2.fingerprint)


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
