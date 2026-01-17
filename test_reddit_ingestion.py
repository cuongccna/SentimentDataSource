"""
Comprehensive tests for Reddit Ingestion Worker.

Tests the LOCKED REDDIT INGESTION CONTRACT:
- Whitelist-only ingestion
- Rate limiting (per-subreddit and global)
- Field validation (score > 0, asset keywords)
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

from reddit_ingestion import (
    # Constants
    SOURCE_RELIABILITY_REDDIT,
    GLOBAL_RATE_LIMIT,
    DEFAULT_SUBREDDIT_LIMIT,
    MIN_SCORE,
    ASSET_KEYWORDS_BTC,
    
    # Enums
    RedditItemType,
    SubredditRole,
    RedditDropReason,
    
    # Data classes
    RedditSource,
    RawRedditItem,
    ProcessedRedditItem,
    IngestionState,
    IngestionMetrics,
    
    # Components
    RedditSourceRegistry,
    IngestionStateManager,
    RedditRateLimiter,
    RedditItemValidator,
    VelocityCalculator,
    RedditIngestionWorker,
    
    # Helpers
    compute_engagement_weight,
    compute_author_weight,
    safe_log,
    
    # Factory functions
    create_source_registry,
    create_ingestion_worker,
    create_test_worker,
    get_create_reddit_sources_sql,
)


# =============================================================================
# TEST HELPERS
# =============================================================================

def create_test_source(
    id: int = 1,
    subreddit: str = "Bitcoin",
    asset: str = "BTC",
    role: SubredditRole = SubredditRole.DISCUSSION,
    enabled: bool = True,
    max_posts_per_run: int = 100,
    priority: int = 0
) -> RedditSource:
    """Create a test Reddit source."""
    return RedditSource(
        id=id,
        subreddit=subreddit,
        asset=asset,
        role=role,
        enabled=enabled,
        max_posts_per_run=max_posts_per_run,
        priority=priority
    )


def create_test_item(
    item_id: str = "post_12345",
    item_type: RedditItemType = RedditItemType.POST,
    subreddit: str = "Bitcoin",
    created_utc: float = None,
    text: str = "BTC is looking bullish today!",
    score: int = 100,
    num_comments: int = 50,
    author_karma: int = 10000,
    author_name: str = "satoshi_fan",
    title: str = "Bitcoin Discussion",
    source_id: int = None
) -> RawRedditItem:
    """Create a test raw Reddit item."""
    if created_utc is None:
        created_utc = time.time()
    
    return RawRedditItem(
        item_id=item_id,
        item_type=item_type,
        subreddit=subreddit,
        created_utc=created_utc,
        text=text,
        score=score,
        num_comments=num_comments,
        author_karma=author_karma,
        author_name=author_name,
        title=title,
        source_id=source_id
    )


# =============================================================================
# CONSTANTS TESTS
# =============================================================================

class TestConstants(unittest.TestCase):
    """Test module constants."""
    
    def test_source_reliability_value(self):
        """Source reliability for Reddit is 0.7."""
        self.assertEqual(SOURCE_RELIABILITY_REDDIT, 0.7)
    
    def test_global_rate_limit_value(self):
        """Global rate limit is 500 items per run."""
        self.assertEqual(GLOBAL_RATE_LIMIT, 500)
    
    def test_default_subreddit_limit(self):
        """Default per-subreddit limit is 100."""
        self.assertEqual(DEFAULT_SUBREDDIT_LIMIT, 100)
    
    def test_min_score_value(self):
        """Minimum score is 1 (score > 0)."""
        self.assertEqual(MIN_SCORE, 1)
    
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
    
    def test_reddit_item_type_values(self):
        """RedditItemType has post and comment."""
        self.assertEqual(RedditItemType.POST.value, "post")
        self.assertEqual(RedditItemType.COMMENT.value, "comment")
    
    def test_subreddit_role_values(self):
        """SubredditRole has discussion, market, dev."""
        self.assertEqual(SubredditRole.DISCUSSION.value, "discussion")
        self.assertEqual(SubredditRole.MARKET.value, "market")
        self.assertEqual(SubredditRole.DEV.value, "dev")
    
    def test_reddit_drop_reason_values(self):
        """RedditDropReason covers all drop cases."""
        reasons = [r.value for r in RedditDropReason]
        self.assertIn("not_whitelisted", reasons)
        self.assertIn("subreddit_disabled", reasons)
        self.assertIn("subreddit_rate_exceeded", reasons)
        self.assertIn("global_rate_exceeded", reasons)
        self.assertIn("empty_text", reasons)
        self.assertIn("no_asset_keyword", reasons)
        self.assertIn("invalid_timestamp", reasons)
        self.assertIn("missing_required_field", reasons)
        self.assertIn("low_score", reasons)
        self.assertIn("already_processed", reasons)


# =============================================================================
# DATA CLASS TESTS
# =============================================================================

class TestRedditSource(unittest.TestCase):
    """Test RedditSource data class."""
    
    def test_create_source(self):
        """Can create Reddit source."""
        source = create_test_source(subreddit="CryptoCurrency")
        self.assertEqual(source.subreddit, "CryptoCurrency")
    
    def test_source_roles(self):
        """Can create sources with different roles."""
        discussion = create_test_source(role=SubredditRole.DISCUSSION)
        market = create_test_source(role=SubredditRole.MARKET)
        dev = create_test_source(role=SubredditRole.DEV)
        
        self.assertEqual(discussion.role, SubredditRole.DISCUSSION)
        self.assertEqual(market.role, SubredditRole.MARKET)
        self.assertEqual(dev.role, SubredditRole.DEV)
    
    def test_source_to_dict(self):
        """Source converts to dictionary."""
        source = create_test_source(id=42, subreddit="Bitcoin")
        d = source.to_dict()
        
        self.assertEqual(d["id"], 42)
        self.assertEqual(d["subreddit"], "Bitcoin")
        self.assertEqual(d["role"], "discussion")
    
    def test_source_enabled_default(self):
        """Source enabled field exists."""
        source = create_test_source(enabled=True)
        self.assertTrue(source.enabled)
    
    def test_source_max_posts_per_run(self):
        """Source has max_posts_per_run field."""
        source = create_test_source(max_posts_per_run=50)
        self.assertEqual(source.max_posts_per_run, 50)


class TestRawRedditItem(unittest.TestCase):
    """Test RawRedditItem data class."""
    
    def test_create_post(self):
        """Can create post item."""
        item = create_test_item(item_type=RedditItemType.POST)
        self.assertEqual(item.item_type, RedditItemType.POST)
    
    def test_create_comment(self):
        """Can create comment item."""
        item = create_test_item(item_type=RedditItemType.COMMENT)
        self.assertEqual(item.item_type, RedditItemType.COMMENT)
    
    def test_raw_item_has_required_fields(self):
        """Raw item has all required fields per contract."""
        item = create_test_item()
        
        # Required: item_id, created_utc, text, score
        self.assertIsNotNone(item.item_id)
        self.assertIsNotNone(item.created_utc)
        self.assertIsNotNone(item.text)
        self.assertIsNotNone(item.score)
        
        # Required: num_comments, author_karma
        self.assertIsNotNone(item.num_comments)
        self.assertIsNotNone(item.author_karma)
    
    def test_created_at_property(self):
        """created_at property converts Unix timestamp to datetime."""
        now = time.time()
        item = create_test_item(created_utc=now)
        
        self.assertIsInstance(item.created_at, datetime)
        self.assertEqual(item.created_at.tzinfo, timezone.utc)
    
    def test_raw_item_to_dict(self):
        """Raw item converts to dictionary."""
        item = create_test_item()
        d = item.to_dict()
        
        self.assertIn("item_id", d)
        self.assertIn("item_type", d)
        self.assertIn("subreddit", d)
        self.assertIn("created_utc", d)
        self.assertIn("text", d)
        self.assertIn("score", d)
        self.assertIn("num_comments", d)
        self.assertIn("author_karma", d)


class TestProcessedRedditItem(unittest.TestCase):
    """Test ProcessedRedditItem data class."""
    
    def test_create_processed_item(self):
        """Can create processed item."""
        item = ProcessedRedditItem(
            item_id="post_12345",
            item_type="post",
            subreddit="Bitcoin",
            text="BTC bullish!",
            event_time="2025-01-17T10:00:00Z",
            asset="BTC"
        )
        self.assertEqual(item.item_id, "post_12345")
        self.assertEqual(item.asset, "BTC")
    
    def test_processed_item_default_values(self):
        """Processed item has correct defaults."""
        item = ProcessedRedditItem(
            item_id="post_12345",
            item_type="post",
            subreddit="Bitcoin",
            text="BTC",
            event_time="2025-01-17T10:00:00Z",
            asset="BTC"
        )
        
        self.assertEqual(item.source, "reddit")
        self.assertEqual(item.source_reliability, SOURCE_RELIABILITY_REDDIT)
        self.assertEqual(item.score, 0)
        self.assertEqual(item.velocity, 0.0)
    
    def test_processed_item_to_dict(self):
        """Processed item converts to dictionary."""
        item = ProcessedRedditItem(
            item_id="post_12345",
            item_type="post",
            subreddit="Bitcoin",
            text="BTC bullish!",
            event_time="2025-01-17T10:00:00Z",
            asset="BTC",
            engagement_weight=2.5,
            author_weight=9.21
        )
        d = item.to_dict()
        
        self.assertEqual(d["item_id"], "post_12345")
        self.assertEqual(d["engagement_weight"], 2.5)
        self.assertEqual(d["author_weight"], 9.21)
        self.assertIn("fingerprint", d)


class TestIngestionState(unittest.TestCase):
    """Test IngestionState data class."""
    
    def test_create_ingestion_state(self):
        """Can create ingestion state."""
        state = IngestionState(source_id=1, subreddit="Bitcoin")
        self.assertEqual(state.source_id, 1)
        self.assertEqual(state.subreddit, "Bitcoin")
        self.assertIsNone(state.last_processed_item_id)
        self.assertEqual(state.items_processed, 0)
    
    def test_ingestion_state_to_dict(self):
        """State converts to dictionary."""
        now = time.time()
        state = IngestionState(
            source_id=42,
            subreddit="CryptoCurrency",
            last_processed_item_id="post_99999",
            last_processed_timestamp=now,
            items_processed=100
        )
        d = state.to_dict()
        
        self.assertEqual(d["source_id"], 42)
        self.assertEqual(d["subreddit"], "CryptoCurrency")
        self.assertEqual(d["last_processed_item_id"], "post_99999")
        self.assertEqual(d["items_processed"], 100)
    
    def test_ingestion_state_from_dict(self):
        """State can be loaded from dictionary."""
        data = {
            "source_id": 5,
            "subreddit": "Bitcoin",
            "last_processed_item_id": "post_12345",
            "last_processed_timestamp": 1705485600.0,
            "items_processed": 50
        }
        state = IngestionState.from_dict(data)
        
        self.assertEqual(state.source_id, 5)
        self.assertEqual(state.subreddit, "Bitcoin")
        self.assertEqual(state.last_processed_item_id, "post_12345")
        self.assertEqual(state.items_processed, 50)


class TestIngestionMetrics(unittest.TestCase):
    """Test IngestionMetrics data class."""
    
    def test_create_metrics(self):
        """Can create metrics with defaults."""
        metrics = IngestionMetrics()
        self.assertEqual(metrics.received, 0)
        self.assertEqual(metrics.accepted, 0)
        self.assertEqual(metrics.dropped_low_score, 0)
    
    def test_metrics_to_dict(self):
        """Metrics converts to dictionary."""
        metrics = IngestionMetrics()
        metrics.received = 100
        metrics.accepted = 80
        metrics.dropped_low_score = 10
        
        d = metrics.to_dict()
        self.assertEqual(d["received"], 100)
        self.assertEqual(d["accepted"], 80)
        self.assertEqual(d["dropped_low_score"], 10)
    
    def test_metrics_reset(self):
        """Metrics can be reset."""
        metrics = IngestionMetrics()
        metrics.received = 100
        metrics.accepted = 80
        
        metrics.reset()
        
        self.assertEqual(metrics.received, 0)
        self.assertEqual(metrics.accepted, 0)


# =============================================================================
# REDDIT SOURCE REGISTRY TESTS
# =============================================================================

class TestRedditSourceRegistry(unittest.TestCase):
    """Test RedditSourceRegistry."""
    
    def test_create_empty_registry(self):
        """Can create empty registry."""
        registry = RedditSourceRegistry()
        self.assertEqual(registry.count(), 0)
    
    def test_load_from_list(self):
        """Can load sources from list."""
        sources = [
            create_test_source(id=1, subreddit="Bitcoin"),
            create_test_source(id=2, subreddit="CryptoCurrency"),
            create_test_source(id=3, subreddit="btc")
        ]
        
        registry = RedditSourceRegistry()
        count = registry.load_from_list(sources)
        
        self.assertEqual(count, 3)
        self.assertEqual(registry.count(), 3)
    
    def test_is_whitelisted_by_subreddit(self):
        """is_whitelisted returns True for registered subreddits."""
        registry = RedditSourceRegistry()
        registry.load_from_list([create_test_source(subreddit="Bitcoin")])
        
        self.assertTrue(registry.is_whitelisted("Bitcoin"))
    
    def test_is_whitelisted_case_insensitive(self):
        """is_whitelisted is case insensitive."""
        registry = RedditSourceRegistry()
        registry.load_from_list([create_test_source(subreddit="Bitcoin")])
        
        self.assertTrue(registry.is_whitelisted("bitcoin"))
        self.assertTrue(registry.is_whitelisted("BITCOIN"))
    
    def test_is_whitelisted_returns_false_for_unknown(self):
        """is_whitelisted returns False for unknown subreddits."""
        registry = RedditSourceRegistry()
        registry.load_from_list([create_test_source(subreddit="Bitcoin")])
        
        self.assertFalse(registry.is_whitelisted("Ethereum"))
    
    def test_is_whitelisted_by_id(self):
        """is_whitelisted_by_id checks by source ID."""
        registry = RedditSourceRegistry()
        registry.load_from_list([create_test_source(id=42)])
        
        self.assertTrue(registry.is_whitelisted_by_id(42))
        self.assertFalse(registry.is_whitelisted_by_id(999))
    
    def test_get_source(self):
        """Can get source by ID."""
        source = create_test_source(id=10, subreddit="CryptoCurrency")
        registry = RedditSourceRegistry()
        registry.load_from_list([source])
        
        result = registry.get_source(10)
        self.assertIsNotNone(result)
        self.assertEqual(result.subreddit, "CryptoCurrency")
    
    def test_get_source_by_subreddit(self):
        """Can get source by subreddit name."""
        source = create_test_source(id=1, subreddit="Bitcoin")
        registry = RedditSourceRegistry()
        registry.load_from_list([source])
        
        result = registry.get_source_by_subreddit("Bitcoin")
        self.assertIsNotNone(result)
        self.assertEqual(result.id, 1)
    
    def test_get_source_by_subreddit_case_insensitive(self):
        """get_source_by_subreddit is case insensitive."""
        source = create_test_source(id=1, subreddit="Bitcoin")
        registry = RedditSourceRegistry()
        registry.load_from_list([source])
        
        result = registry.get_source_by_subreddit("bitcoin")
        self.assertIsNotNone(result)
    
    def test_get_enabled_sources(self):
        """get_enabled_sources returns only enabled sources."""
        sources = [
            create_test_source(id=1, enabled=True),
            create_test_source(id=2, enabled=False),
            create_test_source(id=3, enabled=True)
        ]
        
        registry = RedditSourceRegistry()
        registry.load_from_list(sources)
        
        enabled = registry.get_enabled_sources()
        self.assertEqual(len(enabled), 2)
    
    def test_get_enabled_sources_sorted_by_priority(self):
        """Enabled sources are sorted by priority descending."""
        sources = [
            create_test_source(id=1, subreddit="Low", priority=1),
            create_test_source(id=2, subreddit="High", priority=5),
            create_test_source(id=3, subreddit="Medium", priority=3)
        ]
        
        registry = RedditSourceRegistry()
        registry.load_from_list(sources)
        
        enabled = registry.get_enabled_sources()
        self.assertEqual(enabled[0].priority, 5)
        self.assertEqual(enabled[1].priority, 3)
        self.assertEqual(enabled[2].priority, 1)
    
    def test_get_sources_by_role(self):
        """Can filter sources by role."""
        sources = [
            create_test_source(id=1, role=SubredditRole.DISCUSSION),
            create_test_source(id=2, role=SubredditRole.MARKET),
            create_test_source(id=3, role=SubredditRole.DISCUSSION),
            create_test_source(id=4, role=SubredditRole.DEV)
        ]
        
        registry = RedditSourceRegistry()
        registry.load_from_list(sources)
        
        discussions = registry.get_sources_by_role(SubredditRole.DISCUSSION)
        self.assertEqual(len(discussions), 2)
        
        markets = registry.get_sources_by_role(SubredditRole.MARKET)
        self.assertEqual(len(markets), 1)
        
        devs = registry.get_sources_by_role(SubredditRole.DEV)
        self.assertEqual(len(devs), 1)
    
    def test_clear_removes_all_sources(self):
        """clear removes all sources."""
        sources = [create_test_source(id=i) for i in range(5)]
        
        registry = RedditSourceRegistry()
        registry.load_from_list(sources)
        self.assertEqual(registry.count(), 5)
        
        registry.clear()
        self.assertEqual(registry.count(), 0)


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
        state = manager.get_state(42, "Bitcoin")
        
        self.assertEqual(state.source_id, 42)
        self.assertEqual(state.subreddit, "Bitcoin")
        self.assertIsNone(state.last_processed_item_id)
    
    def test_update_state(self):
        """Can update state after processing."""
        manager = IngestionStateManager()
        now = time.time()
        
        manager.update_state(1, "Bitcoin", "post_12345", now)
        
        state = manager.get_state(1)
        self.assertEqual(state.last_processed_item_id, "post_12345")
        self.assertEqual(state.last_processed_timestamp, now)
        self.assertEqual(state.items_processed, 1)
    
    def test_update_state_increments_count(self):
        """Update state increments item count."""
        manager = IngestionStateManager()
        now = time.time()
        
        manager.update_state(1, "Bitcoin", "p1", now)
        manager.update_state(1, "Bitcoin", "p2", now + 1)
        manager.update_state(1, "Bitcoin", "p3", now + 2)
        
        state = manager.get_state(1)
        self.assertEqual(state.items_processed, 3)
    
    def test_get_last_timestamp(self):
        """Can get last timestamp for a source."""
        manager = IngestionStateManager()
        now = time.time()
        
        manager.update_state(1, "Bitcoin", "p1", now)
        
        last_ts = manager.get_last_timestamp(1)
        self.assertEqual(last_ts, now)
    
    def test_get_last_timestamp_returns_none(self):
        """Returns None if no state exists."""
        manager = IngestionStateManager()
        last_ts = manager.get_last_timestamp(999)
        self.assertIsNone(last_ts)
    
    def test_is_already_processed(self):
        """Can check if item was already processed."""
        manager = IngestionStateManager()
        now = time.time()
        
        manager.update_state(1, "Bitcoin", "post_12345", now)
        
        self.assertTrue(manager.is_already_processed(1, "post_12345"))
        self.assertFalse(manager.is_already_processed(1, "post_99999"))
    
    def test_state_persistence_to_file(self):
        """State persists to file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            # Create manager and update state
            manager = IngestionStateManager(state_file=state_file)
            now = time.time()
            manager.update_state(1, "Bitcoin", "p1", now)
            
            # Create new manager with same file
            manager2 = IngestionStateManager(state_file=state_file)
            state = manager2.get_state(1)
            
            self.assertEqual(state.last_processed_item_id, "p1")
        finally:
            if os.path.exists(state_file):
                os.remove(state_file)
    
    def test_clear_removes_all_states(self):
        """clear removes all states."""
        manager = IngestionStateManager()
        now = time.time()
        
        manager.update_state(1, "Bitcoin", "p1", now)
        manager.update_state(2, "CryptoCurrency", "p2", now)
        
        manager.clear()
        
        # New states are created
        state1 = manager.get_state(1)
        self.assertIsNone(state1.last_processed_item_id)


# =============================================================================
# REDDIT RATE LIMITER TESTS
# =============================================================================

class TestRedditRateLimiter(unittest.TestCase):
    """Test RedditRateLimiter."""
    
    def test_create_rate_limiter(self):
        """Can create rate limiter."""
        limiter = RedditRateLimiter()
        self.assertEqual(limiter.global_limit, GLOBAL_RATE_LIMIT)
    
    def test_create_with_custom_limit(self):
        """Can create with custom global limit."""
        limiter = RedditRateLimiter(global_limit=100)
        self.assertEqual(limiter.global_limit, 100)
    
    def test_subreddit_limit_allows_under_limit(self):
        """Subreddit limit allows when under limit."""
        limiter = RedditRateLimiter()
        
        # Record some items
        for _ in range(29):
            limiter.record_item(1)
        
        # Should allow (under limit of 30)
        self.assertTrue(limiter.check_subreddit_limit(1, 30))
    
    def test_subreddit_limit_blocks_at_limit(self):
        """Subreddit limit blocks at limit."""
        limiter = RedditRateLimiter()
        
        # Record items up to limit
        for _ in range(30):
            limiter.record_item(1)
        
        # Should block (at limit of 30)
        self.assertFalse(limiter.check_subreddit_limit(1, 30))
    
    def test_global_limit_allows_under_limit(self):
        """Global limit allows when under limit."""
        limiter = RedditRateLimiter(global_limit=100)
        
        for _ in range(99):
            limiter.record_item(1)
        
        self.assertTrue(limiter.check_global_limit())
    
    def test_global_limit_blocks_at_limit(self):
        """Global limit blocks at limit."""
        limiter = RedditRateLimiter(global_limit=100)
        
        for _ in range(100):
            limiter.record_item(1)
        
        self.assertFalse(limiter.check_global_limit())
    
    def test_get_subreddit_count(self):
        """Can get current subreddit count."""
        limiter = RedditRateLimiter()
        
        limiter.record_item(1)
        limiter.record_item(1)
        limiter.record_item(1)
        
        self.assertEqual(limiter.get_subreddit_count(1), 3)
    
    def test_get_global_count(self):
        """Can get current global count."""
        limiter = RedditRateLimiter()
        
        limiter.record_item(1)
        limiter.record_item(2)
        limiter.record_item(3)
        
        self.assertEqual(limiter.get_global_count(), 3)
    
    def test_multiple_subreddits_independent(self):
        """Different subreddits have independent limits."""
        limiter = RedditRateLimiter()
        
        # Fill subreddit 1
        for _ in range(30):
            limiter.record_item(1)
        
        # Subreddit 2 should still be allowed
        self.assertTrue(limiter.check_subreddit_limit(2, 30))
        
        # Subreddit 1 should be blocked
        self.assertFalse(limiter.check_subreddit_limit(1, 30))
    
    def test_reset_clears_counters(self):
        """reset clears all counters."""
        limiter = RedditRateLimiter()
        
        for _ in range(50):
            limiter.record_item(1)
        
        limiter.reset()
        
        self.assertEqual(limiter.get_subreddit_count(1), 0)
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
    
    def test_record_item(self):
        """Can record items."""
        calc = VelocityCalculator()
        now = time.time()
        
        calc.record_item(1, now)
        calc.record_item(1, now)
        calc.record_item(1, now)
        
        # No assertion, just verify no exception
    
    def test_velocity_zero_for_no_items(self):
        """Velocity is 0 for no items."""
        calc = VelocityCalculator()
        velocity = calc.get_velocity(1)
        self.assertEqual(velocity, 0.0)
    
    def test_velocity_increases_with_burst(self):
        """Velocity increases with burst of items."""
        calc = VelocityCalculator(short_window=60, long_window=300)
        now = time.time()
        
        # Record some older items
        for i in range(10):
            calc.record_item(1, now - 200)  # 200 seconds ago
        
        # Record burst of recent items
        for i in range(10):
            calc.record_item(1, now)  # Now
        
        velocity = calc.get_velocity(1, now)
        
        # Velocity should be > 1 (short rate higher than long rate)
        self.assertGreater(velocity, 1.0)
    
    def test_clear_removes_all(self):
        """clear removes all records."""
        calc = VelocityCalculator()
        now = time.time()
        
        for _ in range(10):
            calc.record_item(1, now)
        
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
    
    def test_compute_engagement_weight(self):
        """Engagement weight is log of score + comments."""
        weight = compute_engagement_weight(100, 50)
        expected = round(math.log(151), 4)  # log(1 + 100 + 50)
        self.assertEqual(weight, expected)
    
    def test_compute_engagement_weight_zeros(self):
        """Engagement weight for zero engagement."""
        weight = compute_engagement_weight(0, 0)
        self.assertEqual(weight, 0.0)
    
    def test_compute_author_weight(self):
        """Author weight is log of karma."""
        weight = compute_author_weight(10000)
        expected = round(math.log(10001), 4)
        self.assertEqual(weight, expected)
    
    def test_compute_author_weight_zero(self):
        """Author weight for zero karma."""
        weight = compute_author_weight(0)
        self.assertEqual(weight, 0.0)


# =============================================================================
# REDDIT ITEM VALIDATOR TESTS
# =============================================================================

class TestRedditItemValidator(unittest.TestCase):
    """Test RedditItemValidator."""
    
    def test_create_validator(self):
        """Can create validator."""
        validator = RedditItemValidator()
        self.assertIsNotNone(validator)
    
    def test_validate_valid_item(self):
        """Valid item passes validation."""
        validator = RedditItemValidator()
        item = create_test_item(text="BTC looking strong!", score=10)
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertTrue(is_valid)
        self.assertIsNone(reason)
    
    def test_validate_missing_item_id(self):
        """Item without ID fails."""
        validator = RedditItemValidator()
        item = create_test_item()
        item.item_id = None
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, RedditDropReason.MISSING_REQUIRED_FIELD)
    
    def test_validate_invalid_timestamp(self):
        """Item with invalid timestamp fails."""
        validator = RedditItemValidator()
        item = create_test_item()
        item.created_utc = 0
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, RedditDropReason.INVALID_TIMESTAMP)
    
    def test_validate_empty_text(self):
        """Item with empty text fails."""
        validator = RedditItemValidator()
        item = create_test_item(text="")
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, RedditDropReason.EMPTY_TEXT)
    
    def test_validate_whitespace_only_text(self):
        """Item with whitespace-only text fails."""
        validator = RedditItemValidator()
        item = create_test_item(text="   \n\t  ")
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, RedditDropReason.EMPTY_TEXT)
    
    def test_validate_low_score(self):
        """Item with score <= 0 fails."""
        validator = RedditItemValidator()
        
        # Score = 0
        item = create_test_item(text="BTC news", score=0)
        is_valid, reason = validator.validate_item(item)
        self.assertFalse(is_valid)
        self.assertEqual(reason, RedditDropReason.LOW_SCORE)
        
        # Negative score
        item2 = create_test_item(text="BTC news", score=-5)
        is_valid2, reason2 = validator.validate_item(item2)
        self.assertFalse(is_valid2)
        self.assertEqual(reason2, RedditDropReason.LOW_SCORE)
    
    def test_validate_score_positive(self):
        """Item with positive score passes."""
        validator = RedditItemValidator()
        item = create_test_item(text="BTC news", score=1)
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertTrue(is_valid)
    
    def test_validate_no_asset_keyword(self):
        """Item without asset keyword fails."""
        validator = RedditItemValidator()
        item = create_test_item(
            text="The market is moving today!",
            title="Market Update"
        )
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertFalse(is_valid)
        self.assertEqual(reason, RedditDropReason.NO_ASSET_KEYWORD)
    
    def test_validate_keyword_in_title(self):
        """Asset keyword in title passes."""
        validator = RedditItemValidator()
        item = create_test_item(
            text="Price is going up!",
            title="BTC Analysis"
        )
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertTrue(is_valid)
    
    def test_validate_keyword_in_text(self):
        """Asset keyword in text passes."""
        validator = RedditItemValidator()
        item = create_test_item(
            text="Bitcoin is the future",
            title="Discussion"
        )
        
        is_valid, reason = validator.validate_item(item)
        
        self.assertTrue(is_valid)
    
    def test_validate_keyword_case_insensitive(self):
        """Asset keyword matching is case insensitive."""
        validator = RedditItemValidator()
        
        items = [
            create_test_item(text="BTC news"),
            create_test_item(text="btc news"),
            create_test_item(text="Bitcoin news"),
            create_test_item(text="BITCOIN news")
        ]
        
        for item in items:
            is_valid, reason = validator.validate_item(item)
            self.assertTrue(is_valid, f"Failed for: {item.text}")


# =============================================================================
# REDDIT INGESTION WORKER TESTS
# =============================================================================

class TestRedditIngestionWorkerBasic(unittest.TestCase):
    """Test RedditIngestionWorker basic functionality."""
    
    def test_create_worker(self):
        """Can create worker."""
        registry = RedditSourceRegistry()
        worker = RedditIngestionWorker(registry=registry)
        self.assertIsNotNone(worker)
    
    def test_worker_starts_not_running(self):
        """Worker starts not running."""
        worker = create_test_worker()
        self.assertFalse(worker.is_running)
    
    def test_worker_start_run(self):
        """Can start a run."""
        worker = create_test_worker()
        worker.start_run()
        self.assertTrue(worker.is_running)
    
    def test_worker_end_run(self):
        """Can end a run."""
        worker = create_test_worker()
        worker.start_run()
        worker.end_run()
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
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        state = worker.get_source_state(1)
        
        self.assertEqual(state.source_id, 1)


class TestRedditIngestionWorkerWhitelist(unittest.TestCase):
    """Test whitelist enforcement in RedditIngestionWorker."""
    
    def test_non_whitelisted_subreddit_rejected(self):
        """Items from non-whitelisted subreddit are rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Ethereum", source_id=999)
        result = worker.handle_item(item)
        
        self.assertIsNone(result)
    
    def test_whitelisted_subreddit_accepted(self):
        """Items from whitelisted subreddit are accepted."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
    
    def test_resolve_source_by_subreddit_name(self):
        """Can resolve source by subreddit name."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        # No source_id, but subreddit matches
        item = create_test_item(subreddit="Bitcoin", source_id=None, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
    
    def test_disabled_source_rejected(self):
        """Items from disabled source are rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin", enabled=False)]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNone(result)
    
    def test_metrics_track_non_whitelisted(self):
        """Metrics track non-whitelisted drops."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Ethereum", source_id=999)
        worker.handle_item(item)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["dropped_not_whitelisted"], 1)


class TestRedditIngestionWorkerRateLimits(unittest.TestCase):
    """Test rate limiting in RedditIngestionWorker."""
    
    def test_subreddit_rate_limit_enforced(self):
        """Per-subreddit rate limit is enforced."""
        sources = [create_test_source(id=1, subreddit="Bitcoin", max_posts_per_run=5)]
        worker = create_test_worker(sources=sources)
        
        accepted = 0
        
        for i in range(10):
            item = create_test_item(
                item_id=f"post_{i}",
                subreddit="Bitcoin",
                source_id=1,
                text="BTC news!"
            )
            result = worker.handle_item(item)
            if result:
                accepted += 1
        
        self.assertEqual(accepted, 5)
    
    def test_global_rate_limit_enforced(self):
        """Global rate limit is enforced."""
        sources = [
            create_test_source(id=1, subreddit="Bitcoin", max_posts_per_run=100),
            create_test_source(id=2, subreddit="CryptoCurrency", max_posts_per_run=100)
        ]
        worker = create_test_worker(sources=sources, global_rate_limit=10)
        
        accepted = 0
        
        for i in range(20):
            source_id = 1 if i % 2 == 0 else 2
            subreddit = "Bitcoin" if source_id == 1 else "CryptoCurrency"
            item = create_test_item(
                item_id=f"post_{i}",
                subreddit=subreddit,
                source_id=source_id,
                text="BTC news!"
            )
            result = worker.handle_item(item)
            if result:
                accepted += 1
        
        self.assertEqual(accepted, 10)
    
    def test_rate_limit_resets_on_new_run(self):
        """Rate limits reset at start of new run."""
        sources = [create_test_source(id=1, subreddit="Bitcoin", max_posts_per_run=5)]
        worker = create_test_worker(sources=sources)
        
        # First run
        for i in range(10):
            item = create_test_item(
                item_id=f"post_{i}",
                subreddit="Bitcoin",
                source_id=1,
                text="BTC!"
            )
            worker.handle_item(item)
        
        self.assertEqual(worker.rate_limiter.get_subreddit_count(1), 5)
        
        # Start new run
        worker.start_run()
        
        self.assertEqual(worker.rate_limiter.get_subreddit_count(1), 0)


class TestRedditIngestionWorkerValidation(unittest.TestCase):
    """Test item validation in RedditIngestionWorker."""
    
    def test_empty_text_rejected(self):
        """Empty text items are rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="")
        result = worker.handle_item(item)
        
        self.assertIsNone(result)
    
    def test_low_score_rejected(self):
        """Low score items are rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!", score=0)
        result = worker.handle_item(item)
        
        self.assertIsNone(result)
    
    def test_no_asset_keyword_rejected(self):
        """Items without asset keyword are rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(
            subreddit="Bitcoin",
            source_id=1,
            text="No crypto here!",
            title="Random"
        )
        result = worker.handle_item(item)
        
        self.assertIsNone(result)


class TestRedditIngestionWorkerMetrics(unittest.TestCase):
    """Test metric computation in RedditIngestionWorker."""
    
    def test_processed_item_has_engagement_weight(self):
        """Processed item has engagement weight."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(
            subreddit="Bitcoin",
            source_id=1,
            text="BTC news!",
            score=100,
            num_comments=50
        )
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
        # log(1 + 100 + 50) = log(151)
        expected = round(math.log(151), 4)
        self.assertEqual(result.engagement_weight, expected)
    
    def test_processed_item_has_author_weight(self):
        """Processed item has author weight."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(
            subreddit="Bitcoin",
            source_id=1,
            text="BTC news!",
            author_karma=10000
        )
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
        expected = round(math.log(10001), 4)
        self.assertEqual(result.author_weight, expected)
    
    def test_processed_item_has_fingerprint(self):
        """Processed item has fingerprint."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
        self.assertIsNotNone(result.fingerprint)
        self.assertEqual(len(result.fingerprint), 16)
    
    def test_processed_item_has_event_time(self):
        """Processed item has formatted event_time."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
        self.assertTrue(result.event_time.endswith("Z"))
        self.assertIn("T", result.event_time)
    
    def test_processed_item_has_source_reliability(self):
        """Processed item has correct source reliability."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.source, "reddit")
        self.assertEqual(result.source_reliability, 0.7)
    
    def test_processed_item_has_role(self):
        """Processed item has role from source."""
        sources = [create_test_source(id=1, subreddit="Bitcoin", role=SubredditRole.MARKET)]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        self.assertIsNotNone(result)
        self.assertEqual(result.role, "market")


class TestRedditIngestionWorkerState(unittest.TestCase):
    """Test state management in RedditIngestionWorker."""
    
    def test_state_updated_after_processing(self):
        """State is updated after processing item."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(
            item_id="post_12345",
            subreddit="Bitcoin",
            source_id=1,
            text="BTC!"
        )
        worker.handle_item(item)
        
        state = worker.get_source_state(1)
        self.assertEqual(state.last_processed_item_id, "post_12345")
        self.assertEqual(state.items_processed, 1)
    
    def test_already_processed_rejected(self):
        """Already processed items are rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(
            item_id="post_12345",
            subreddit="Bitcoin",
            source_id=1,
            text="BTC!"
        )
        
        result1 = worker.handle_item(item)
        result2 = worker.handle_item(item)
        
        self.assertIsNotNone(result1)
        self.assertIsNone(result2)
    
    def test_metrics_track_already_processed(self):
        """Metrics track already processed drops."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(
            item_id="post_12345",
            subreddit="Bitcoin",
            source_id=1,
            text="BTC!"
        )
        
        worker.handle_item(item)
        worker.handle_item(item)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["dropped_already_processed"], 1)


class TestRedditIngestionWorkerBatch(unittest.TestCase):
    """Test batch processing in RedditIngestionWorker."""
    
    def test_process_batch(self):
        """Can process batch of items."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        now = time.time()
        items = [
            create_test_item(item_id=f"post_{i}", text="BTC!", created_utc=now + i)
            for i in range(5)
        ]
        
        results = worker.process_batch(items, source_id=1)
        
        self.assertEqual(len(results), 5)
    
    def test_batch_filters_by_last_timestamp(self):
        """Batch filters items older than last processed timestamp."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        now = time.time()
        
        # Process one item to set last timestamp
        first_item = create_test_item(
            item_id="first",
            text="BTC!",
            created_utc=now
        )
        worker.handle_item(first_item, source_id=1)
        
        # Batch with mixed old/new items
        items = [
            create_test_item(item_id="old", text="BTC!", created_utc=now - 10),
            create_test_item(item_id="new1", text="BTC!", created_utc=now + 1),
            create_test_item(item_id="new2", text="BTC!", created_utc=now + 2)
        ]
        
        results = worker.process_batch(items, source_id=1)
        
        # Only new items should be processed
        self.assertEqual(len(results), 2)


class TestRedditIngestionWorkerCallback(unittest.TestCase):
    """Test callback functionality in RedditIngestionWorker."""
    
    def test_callback_invoked_on_accept(self):
        """Callback is invoked when item is accepted."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        received = []
        
        def on_item(item):
            received.append(item)
        
        worker = RedditIngestionWorker(
            registry=RedditSourceRegistry(),
            on_item=on_item
        )
        worker.registry.load_from_list(sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        worker.handle_item(item)
        
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].item_id, item.item_id)
    
    def test_callback_not_invoked_on_reject(self):
        """Callback is not invoked when item is rejected."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        received = []
        
        def on_item(item):
            received.append(item)
        
        worker = RedditIngestionWorker(
            registry=RedditSourceRegistry(),
            on_item=on_item
        )
        worker.registry.load_from_list(sources)
        
        item = create_test_item(subreddit="Ethereum", source_id=999, text="BTC!")
        worker.handle_item(item)
        
        self.assertEqual(len(received), 0)
    
    def test_callback_error_does_not_crash(self):
        """Callback error does not crash worker."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        
        def on_item(item):
            raise Exception("Callback error!")
        
        worker = RedditIngestionWorker(
            registry=RedditSourceRegistry(),
            on_item=on_item
        )
        worker.registry.load_from_list(sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        result = worker.handle_item(item)
        
        # Should still return result
        self.assertIsNotNone(result)


class TestRedditIngestionWorkerMetricsReset(unittest.TestCase):
    """Test metrics reset in RedditIngestionWorker."""
    
    def test_get_metrics(self):
        """Can get metrics."""
        worker = create_test_worker()
        metrics = worker.get_metrics()
        
        self.assertIn("received", metrics)
        self.assertIn("accepted", metrics)
        self.assertIn("dropped_low_score", metrics)
    
    def test_reset_metrics(self):
        """Can reset metrics."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        worker.handle_item(item)
        
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
        self.assertIsInstance(registry, RedditSourceRegistry)
    
    def test_create_ingestion_worker(self):
        """Can create ingestion worker."""
        worker = create_ingestion_worker()
        self.assertIsInstance(worker, RedditIngestionWorker)
    
    def test_create_ingestion_worker_with_registry(self):
        """Can create worker with custom registry."""
        registry = create_source_registry()
        registry.load_from_list([create_test_source(id=1, subreddit="Bitcoin")])
        
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
        worker = create_ingestion_worker(on_item=lambda i: received.append(i))
        
        worker.registry.load_from_list([create_test_source(id=1, subreddit="Bitcoin")])
        item = create_test_item(subreddit="Bitcoin", source_id=1, text="BTC!")
        worker.handle_item(item)
        
        self.assertEqual(len(received), 1)
    
    def test_create_test_worker(self):
        """Can create test worker."""
        worker = create_test_worker()
        self.assertIsInstance(worker, RedditIngestionWorker)
    
    def test_create_test_worker_with_sources(self):
        """Can create test worker with sources."""
        sources = [
            create_test_source(id=1, subreddit="Bitcoin"),
            create_test_source(id=2, subreddit="CryptoCurrency")
        ]
        worker = create_test_worker(sources=sources)
        
        self.assertEqual(worker.registry.count(), 2)
    
    def test_get_create_reddit_sources_sql(self):
        """Can get SQL for creating tables."""
        sql = get_create_reddit_sources_sql()
        
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("reddit_sources", sql)
        self.assertIn("subreddit", sql)
        self.assertIn("reddit_ingestion_state", sql)


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestRedditIngestionIntegration(unittest.TestCase):
    """Integration tests for Reddit ingestion."""
    
    def test_full_ingestion_flow(self):
        """Test complete ingestion flow."""
        # Setup sources
        sources = [
            create_test_source(
                id=1,
                subreddit="Bitcoin",
                role=SubredditRole.DISCUSSION,
                max_posts_per_run=10
            ),
            create_test_source(
                id=2,
                subreddit="CryptoCurrency",
                role=SubredditRole.MARKET,
                max_posts_per_run=10
            )
        ]
        
        received = []
        worker = RedditIngestionWorker(
            registry=RedditSourceRegistry(),
            on_item=lambda i: received.append(i)
        )
        worker.registry.load_from_list(sources)
        worker.start_run()
        
        now = time.time()
        
        # Ingest items from different subreddits
        items = [
            create_test_item(
                item_id="post_1",
                subreddit="Bitcoin",
                source_id=1,
                text="BTC breaking $100k!",
                score=500,
                num_comments=100,
                author_karma=50000,
                created_utc=now
            ),
            create_test_item(
                item_id="post_2",
                subreddit="CryptoCurrency",
                source_id=2,
                text="Bitcoin market analysis",
                score=200,
                num_comments=50,
                author_karma=25000,
                created_utc=now + 1
            ),
            create_test_item(
                item_id="post_3",
                subreddit="Ethereum",  # Not whitelisted
                source_id=999,
                text="BTC news",
                created_utc=now + 2
            )
        ]
        
        for item in items:
            worker.handle_item(item)
        
        worker.end_run()
        
        # Verify results
        self.assertEqual(len(received), 2)
        
        # Check metrics
        metrics = worker.get_metrics()
        self.assertEqual(metrics["received"], 3)
        self.assertEqual(metrics["accepted"], 2)
        self.assertEqual(metrics["dropped_not_whitelisted"], 1)
        
        # Check first item
        first = received[0]
        self.assertEqual(first.item_id, "post_1")
        self.assertEqual(first.role, "discussion")
        self.assertEqual(first.subreddit, "Bitcoin")
        
        # Check second item
        second = received[1]
        self.assertEqual(second.item_id, "post_2")
        self.assertEqual(second.role, "market")
    
    def test_rate_limiting_across_subreddits(self):
        """Test rate limiting works across multiple subreddits."""
        sources = [
            create_test_source(id=1, subreddit="Bitcoin", max_posts_per_run=3),
            create_test_source(id=2, subreddit="CryptoCurrency", max_posts_per_run=3)
        ]
        
        worker = create_test_worker(sources=sources, global_rate_limit=5)
        
        now = time.time()
        accepted = 0
        
        for i in range(10):
            source_id = 1 if i % 2 == 0 else 2
            subreddit = "Bitcoin" if source_id == 1 else "CryptoCurrency"
            item = create_test_item(
                item_id=f"post_{i}",
                subreddit=subreddit,
                source_id=source_id,
                text="BTC news!",
                created_utc=now + i
            )
            result = worker.handle_item(item)
            if result:
                accepted += 1
        
        # Global limit is 5, so max 5 accepted
        self.assertEqual(accepted, 5)
    
    def test_state_persistence_integration(self):
        """Test state persists correctly."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            sources = [create_test_source(id=1, subreddit="Bitcoin")]
            
            # First worker session
            worker1 = create_ingestion_worker(state_file=state_file)
            worker1.registry.load_from_list(sources)
            
            now = time.time()
            item = create_test_item(
                item_id="persist_test",
                subreddit="Bitcoin",
                source_id=1,
                text="BTC!",
                created_utc=now
            )
            worker1.handle_item(item)
            
            # Second worker session (restart)
            worker2 = create_ingestion_worker(state_file=state_file)
            worker2.registry.load_from_list(sources)
            
            state = worker2.get_source_state(1)
            self.assertEqual(state.last_processed_item_id, "persist_test")
            
            # Same item should be rejected
            result = worker2.handle_item(item)
            self.assertIsNone(result)
            
        finally:
            if os.path.exists(state_file):
                os.remove(state_file)
    
    def test_different_roles(self):
        """Test different subreddit roles work correctly."""
        sources = [
            create_test_source(
                id=1,
                subreddit="Bitcoin",
                role=SubredditRole.DISCUSSION
            ),
            create_test_source(
                id=2,
                subreddit="BitcoinMarkets",
                role=SubredditRole.MARKET
            ),
            create_test_source(
                id=3,
                subreddit="BitcoinDev",
                role=SubredditRole.DEV
            )
        ]
        
        registry = create_source_registry()
        registry.load_from_list(sources)
        
        discussions = registry.get_sources_by_role(SubredditRole.DISCUSSION)
        markets = registry.get_sources_by_role(SubredditRole.MARKET)
        devs = registry.get_sources_by_role(SubredditRole.DEV)
        
        self.assertEqual(len(discussions), 1)
        self.assertEqual(len(markets), 1)
        self.assertEqual(len(devs), 1)


class TestSafetyGuarantees(unittest.TestCase):
    """Test safety guarantees per contract."""
    
    def test_no_ingestion_outside_whitelist(self):
        """System MUST NOT ingest data outside whitelist."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        now = time.time()
        
        # Try many non-whitelisted items
        for i in range(100):
            item = create_test_item(
                item_id=f"post_{i}",
                subreddit=f"RandomSubreddit{i}",
                source_id=i + 1000,
                text="BTC news!",
                created_utc=now + i
            )
            result = worker.handle_item(item)
            self.assertIsNone(result)
        
        metrics = worker.get_metrics()
        self.assertEqual(metrics["accepted"], 0)
        self.assertEqual(metrics["dropped_not_whitelisted"], 100)
    
    def test_bounded_ingestion(self):
        """Reddit ingestion MUST be slow, bounded, and stable."""
        sources = [create_test_source(id=1, subreddit="Bitcoin", max_posts_per_run=100)]
        worker = create_test_worker(sources=sources, global_rate_limit=50)
        
        now = time.time()
        accepted = 0
        
        # Try to ingest 1000 items
        for i in range(1000):
            item = create_test_item(
                item_id=f"post_{i}",
                subreddit="Bitcoin",
                source_id=1,
                text="BTC news!",
                created_utc=now + i
            )
            result = worker.handle_item(item)
            if result:
                accepted += 1
        
        # Should be bounded by global limit
        self.assertEqual(accepted, 50)
    
    def test_deterministic_filtering(self):
        """Same items should produce same results."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        
        now = time.time()
        item = create_test_item(
            item_id="deterministic",
            subreddit="Bitcoin",
            source_id=1,
            text="BTC news!",
            score=100,
            num_comments=50,
            created_utc=now
        )
        
        # Run twice with fresh workers
        worker1 = create_test_worker(sources=sources)
        result1 = worker1.handle_item(item)
        
        worker2 = create_test_worker(sources=sources)
        result2 = worker2.handle_item(item)
        
        # Results should match
        self.assertEqual(result1.engagement_weight, result2.engagement_weight)
        self.assertEqual(result1.author_weight, result2.author_weight)
        self.assertEqual(result1.fingerprint, result2.fingerprint)
    
    def test_score_filter_prevents_low_quality(self):
        """Score > 0 filter prevents low-quality content."""
        sources = [create_test_source(id=1, subreddit="Bitcoin")]
        worker = create_test_worker(sources=sources)
        
        now = time.time()
        
        # Items with various scores
        test_cases = [
            (-10, False),  # Downvoted
            (-1, False),   # Slightly downvoted
            (0, False),    # No votes
            (1, True),     # Minimum positive
            (10, True),    # Good score
            (100, True),   # High score
        ]
        
        for score, should_accept in test_cases:
            worker.reset_metrics()
            worker.rate_limiter.reset()
            
            item = create_test_item(
                item_id=f"score_{score}",
                subreddit="Bitcoin",
                source_id=1,
                text="BTC news!",
                score=score,
                created_utc=now
            )
            result = worker.handle_item(item)
            
            if should_accept:
                self.assertIsNotNone(result, f"Score {score} should be accepted")
            else:
                self.assertIsNone(result, f"Score {score} should be rejected")


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
