"""
Demo: Reddit Ingestion Worker for Crypto BotTrading Social Data Pipeline.

This demo showcases the LOCKED REDDIT INGESTION CONTRACT:
- Whitelist-only ingestion (CRITICAL)
- Per-subreddit and global rate limiting
- Field validation (score > 0, asset keywords)
- Engagement and author metrics
- State persistence
- Velocity tracking

Reddit provides CROWD CONFIRMATION, not early signals.
Source reliability: 0.7 (higher than Twitter's 0.5)
"""

import json
import math
import time
import tempfile
from datetime import datetime, timezone

from reddit_ingestion import (
    # Constants
    SOURCE_RELIABILITY_REDDIT,
    GLOBAL_RATE_LIMIT,
    DEFAULT_SUBREDDIT_LIMIT,
    
    # Enums
    RedditItemType,
    SubredditRole,
    RedditDropReason,
    
    # Data classes
    RedditSource,
    RawRedditItem,
    ProcessedRedditItem,
    
    # Components
    RedditSourceRegistry,
    RedditRateLimiter,
    RedditItemValidator,
    VelocityCalculator,
    RedditIngestionWorker,
    
    # Factory functions
    create_source_registry,
    create_ingestion_worker,
    create_test_worker,
    get_create_reddit_sources_sql,
    
    # Helpers
    compute_engagement_weight,
    compute_author_weight,
)


def section(title: str) -> None:
    """Print a section header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def demo_constants() -> None:
    """Demo: Module constants."""
    section("MODULE CONSTANTS")
    
    print(f"Source reliability (Reddit): {SOURCE_RELIABILITY_REDDIT}")
    print(f"Global rate limit per run:   {GLOBAL_RATE_LIMIT}")
    print(f"Default per-subreddit limit: {DEFAULT_SUBREDDIT_LIMIT}")
    
    print("\nReddit source types:")
    for item_type in RedditItemType:
        print(f"  - {item_type.value}")
    
    print("\nSubreddit roles:")
    for role in SubredditRole:
        print(f"  - {role.value}")
    
    print("\n[[OK]] Constants loaded correctly")


def demo_source_registry() -> None:
    """Demo: Source registry and whitelist management."""
    section("SOURCE REGISTRY - WHITELIST MANAGEMENT")
    
    # Create registry
    registry = create_source_registry()
    print(f"Created empty registry (count: {registry.count()})")
    
    # Define sources
    sources = [
        RedditSource(
            id=1,
            subreddit="Bitcoin",
            asset="BTC",
            role=SubredditRole.DISCUSSION,
            enabled=True,
            max_posts_per_run=100,
            priority=5
        ),
        RedditSource(
            id=2,
            subreddit="CryptoCurrency",
            asset="BTC",
            role=SubredditRole.DISCUSSION,
            enabled=True,
            max_posts_per_run=50,
            priority=3
        ),
        RedditSource(
            id=3,
            subreddit="BitcoinMarkets",
            asset="BTC",
            role=SubredditRole.MARKET,
            enabled=True,
            max_posts_per_run=75,
            priority=4
        ),
        RedditSource(
            id=4,
            subreddit="BitcoinDev",
            asset="BTC",
            role=SubredditRole.DEV,
            enabled=False,  # Disabled
            max_posts_per_run=25,
            priority=2
        ),
    ]
    
    # Load sources
    count = registry.load_from_list(sources)
    print(f"Loaded {count} sources")
    
    # Test whitelist lookup
    print("\nWhitelist checks:")
    test_subreddits = ["Bitcoin", "bitcoin", "BITCOIN", "Ethereum", "wallstreetbets"]
    for sub in test_subreddits:
        is_whitelisted = registry.is_whitelisted(sub)
        status = "[OK] WHITELISTED" if is_whitelisted else "[X] NOT WHITELISTED"
        print(f"  r/{sub}: {status}")
    
    # Get enabled sources
    print("\nEnabled sources (sorted by priority):")
    enabled = registry.get_enabled_sources()
    for source in enabled:
        print(f"  {source.priority}: r/{source.subreddit} ({source.role.value})")
    
    # Filter by role
    print("\nSources by role:")
    for role in SubredditRole:
        sources_for_role = registry.get_sources_by_role(role)
        print(f"  {role.value}: {len(sources_for_role)} source(s)")
    
    print("\n[[OK]] Registry demo complete")


def demo_rate_limiting() -> None:
    """Demo: Rate limiting (per-subreddit and global)."""
    section("RATE LIMITING")
    
    print("Reddit uses PER-RUN rate limits (not per-minute like Twitter/Telegram)")
    
    limiter = RedditRateLimiter(global_limit=10)
    print(f"\nCreated limiter with global limit: {limiter.global_limit}")
    
    # Simulate recording items
    print("\nRecording items for subreddit 1 (limit=5):")
    for i in range(7):
        under_sub_limit = limiter.check_subreddit_limit(1, max_per_run=5)
        under_global_limit = limiter.check_global_limit()
        
        if under_sub_limit and under_global_limit:
            limiter.record_item(1)
            print(f"  Item {i+1}: [OK] Accepted (sub: {limiter.get_subreddit_count(1)}/5, global: {limiter.get_global_count()}/10)")
        else:
            reason = "subreddit limit" if not under_sub_limit else "global limit"
            print(f"  Item {i+1}: [X] Rejected ({reason})")
    
    # Record for another subreddit
    print("\nRecording items for subreddit 2 (limit=10):")
    for i in range(7):
        under_sub_limit = limiter.check_subreddit_limit(2, max_per_run=10)
        under_global_limit = limiter.check_global_limit()
        
        if under_sub_limit and under_global_limit:
            limiter.record_item(2)
            print(f"  Item {i+1}: [OK] Accepted (sub: {limiter.get_subreddit_count(2)}/10, global: {limiter.get_global_count()}/10)")
        else:
            reason = "subreddit limit" if not under_sub_limit else "global limit"
            print(f"  Item {i+1}: [X] Rejected ({reason})")
    
    # Reset
    print("\nResetting rate limiter (start of new run)...")
    limiter.reset()
    print(f"  Subreddit 1: {limiter.get_subreddit_count(1)}")
    print(f"  Subreddit 2: {limiter.get_subreddit_count(2)}")
    print(f"  Global: {limiter.get_global_count()}")
    
    print("\n[[OK]] Rate limiting demo complete")


def demo_item_validation() -> None:
    """Demo: Item validation rules."""
    section("ITEM VALIDATION")
    
    validator = RedditItemValidator()
    print("Validation rules:")
    print("  1. Must have item_id, created_utc, text, score")
    print("  2. Text must not be empty")
    print("  3. Score must be > 0 (upvotes)")
    print("  4. Must contain asset keyword (BTC, Bitcoin, etc.)")
    
    now = time.time()
    
    # Test cases
    test_items = [
        ("Valid item", RawRedditItem(
            item_id="valid_1",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now,
            text="BTC is looking bullish today!",
            score=100,
            num_comments=50,
            author_karma=10000,
            author_name="hodler"
        )),
        ("Empty text", RawRedditItem(
            item_id="empty_text",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now,
            text="",
            score=100,
            num_comments=50,
            author_karma=10000,
            author_name="hodler"
        )),
        ("Low score (0)", RawRedditItem(
            item_id="low_score",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now,
            text="BTC news!",
            score=0,
            num_comments=50,
            author_karma=10000,
            author_name="hodler"
        )),
        ("Negative score", RawRedditItem(
            item_id="neg_score",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now,
            text="BTC news!",
            score=-5,
            num_comments=50,
            author_karma=10000,
            author_name="hodler"
        )),
        ("No asset keyword", RawRedditItem(
            item_id="no_keyword",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now,
            text="The market is moving today!",
            score=100,
            num_comments=50,
            author_karma=10000,
            author_name="hodler",
            title="Market Update"
        )),
        ("Keyword in title only", RawRedditItem(
            item_id="title_keyword",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now,
            text="Great analysis of the market!",
            score=100,
            num_comments=50,
            author_karma=10000,
            author_name="hodler",
            title="BTC Technical Analysis"
        )),
    ]
    
    print("\nValidation results:")
    for name, item in test_items:
        is_valid, reason = validator.validate_item(item)
        if is_valid:
            print(f"  [OK] {name}: VALID")
        else:
            print(f"  [X] {name}: INVALID ({reason.value})")
    
    print("\n[[OK]] Validation demo complete")


def demo_metrics_computation() -> None:
    """Demo: Engagement and author weight computation."""
    section("METRICS COMPUTATION")
    
    print("Engagement weight = log(1 + score + num_comments)")
    print("Author weight = log(1 + author_karma)")
    
    test_cases = [
        (0, 0),
        (10, 5),
        (100, 50),
        (500, 200),
        (1000, 500),
        (5000, 2000),
    ]
    
    print("\nEngagement weights:")
    for score, comments in test_cases:
        weight = compute_engagement_weight(score, comments)
        total = score + comments
        print(f"  score={score:5d}, comments={comments:4d} -> weight={weight:.4f}")
    
    karma_cases = [0, 100, 1000, 10000, 50000, 100000]
    
    print("\nAuthor weights:")
    for karma in karma_cases:
        weight = compute_author_weight(karma)
        print(f"  karma={karma:6d} -> weight={weight:.4f}")
    
    print("\n[[OK]] Metrics demo complete")


def demo_velocity_tracking() -> None:
    """Demo: Velocity calculation for burst detection."""
    section("VELOCITY TRACKING")
    
    calc = VelocityCalculator(short_window=60, long_window=300)
    print("Short window: 60 seconds")
    print("Long window: 300 seconds (5 minutes)")
    print("Velocity = short_rate / long_rate (ratio > 1 = burst)")
    
    now = time.time()
    
    # Record historical items (spread over long window)
    print("\nRecording 10 items over past 5 minutes...")
    for i in range(10):
        calc.record_item(1, now - 250 + (i * 25))  # Every 25 seconds
    
    velocity_before = calc.get_velocity(1, now)
    print(f"Velocity before burst: {velocity_before:.2f}")
    
    # Record burst of items
    print("\nRecording burst of 10 items in last minute...")
    for i in range(10):
        calc.record_item(1, now - 50 + (i * 5))  # Every 5 seconds
    
    velocity_after = calc.get_velocity(1, now)
    print(f"Velocity after burst: {velocity_after:.2f}")
    
    if velocity_after > 1.0:
        print("=> Burst detected! Short-term rate exceeds long-term rate")
    
    print("\n[[OK]] Velocity demo complete")


def demo_full_ingestion_flow() -> None:
    """Demo: Complete ingestion workflow."""
    section("FULL INGESTION FLOW")
    
    print("Setting up worker with 3 subreddit sources...")
    
    # Create sources
    sources = [
        RedditSource(
            id=1,
            subreddit="Bitcoin",
            asset="BTC",
            role=SubredditRole.DISCUSSION,
            enabled=True,
            max_posts_per_run=5,
            priority=5
        ),
        RedditSource(
            id=2,
            subreddit="CryptoCurrency",
            asset="BTC",
            role=SubredditRole.DISCUSSION,
            enabled=True,
            max_posts_per_run=5,
            priority=3
        ),
        RedditSource(
            id=3,
            subreddit="BitcoinMarkets",
            asset="BTC",
            role=SubredditRole.MARKET,
            enabled=True,
            max_posts_per_run=5,
            priority=4
        ),
    ]
    
    # Track received items
    received = []
    
    def on_item(item: ProcessedRedditItem) -> None:
        received.append(item)
    
    # Create worker
    worker = create_ingestion_worker(
        global_rate_limit=10,
        on_item=on_item
    )
    worker.registry.load_from_list(sources)
    
    print(f"Loaded {worker.registry.count()} sources")
    print(f"Global rate limit: 10")
    
    # Start run
    worker.start_run()
    print("\n--- Run started ---")
    
    now = time.time()
    
    # Simulate incoming items
    items = [
        # Valid items from whitelisted subreddits
        RawRedditItem(
            item_id="post_001",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now + 1,
            text="BTC breaking $100k resistance! Major bullish signal.",
            score=500,
            num_comments=200,
            author_karma=50000,
            author_name="crypto_analyst",
            title="BTC Technical Analysis"
        ),
        RawRedditItem(
            item_id="post_002",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now + 2,
            text="Bitcoin ETF inflows at record high today!",
            score=300,
            num_comments=100,
            author_karma=25000,
            author_name="etf_watcher",
            title="ETF Update"
        ),
        RawRedditItem(
            item_id="comment_001",
            item_type=RedditItemType.COMMENT,
            subreddit="CryptoCurrency",
            created_utc=now + 3,
            text="BTC dominance increasing again. Altseason delayed.",
            score=50,
            num_comments=0,
            author_karma=10000,
            author_name="market_observer"
        ),
        
        # Item from non-whitelisted subreddit (will be rejected)
        RawRedditItem(
            item_id="post_003",
            item_type=RedditItemType.POST,
            subreddit="wallstreetbets",
            created_utc=now + 4,
            text="YOLO into BTC!",
            score=1000,
            num_comments=500,
            author_karma=5000,
            author_name="ape_trader"
        ),
        
        # Item with low score (will be rejected)
        RawRedditItem(
            item_id="post_004",
            item_type=RedditItemType.POST,
            subreddit="Bitcoin",
            created_utc=now + 5,
            text="BTC is dead (again)",
            score=0,
            num_comments=10,
            author_karma=100,
            author_name="fud_spreader"
        ),
        
        # Item without asset keyword (will be rejected)
        RawRedditItem(
            item_id="post_005",
            item_type=RedditItemType.POST,
            subreddit="BitcoinMarkets",
            created_utc=now + 6,
            text="The market is interesting today!",
            score=50,
            num_comments=20,
            author_karma=5000,
            author_name="vague_poster",
            title="Market Update"
        ),
        
        # More valid items
        RawRedditItem(
            item_id="post_006",
            item_type=RedditItemType.POST,
            subreddit="BitcoinMarkets",
            created_utc=now + 7,
            text="Bitcoin trading at strong support. BTC looks ready for next leg up.",
            score=150,
            num_comments=75,
            author_karma=30000,
            author_name="chart_master",
            title="BTC Support Analysis"
        ),
    ]
    
    print(f"\nProcessing {len(items)} items...")
    
    for item in items:
        result = worker.handle_item(item)
        status = "[OK] ACCEPTED" if result else "[X] REJECTED"
        source_info = f"r/{item.subreddit}"
        print(f"  {item.item_id}: {status} ({source_info})")
    
    # End run
    worker.end_run()
    print("\n--- Run ended ---")
    
    # Show metrics
    metrics = worker.get_metrics()
    print("\nIngestion metrics:")
    print(f"  Received:            {metrics['received']}")
    print(f"  Accepted:            {metrics['accepted']}")
    print(f"  Dropped (not whitelisted): {metrics['dropped_not_whitelisted']}")
    print(f"  Dropped (low score):       {metrics['dropped_low_score']}")
    print(f"  Dropped (no keyword):      {metrics['dropped_no_keyword']}")
    
    # Show processed items
    print("\nProcessed items:")
    for item in received:
        print(f"\n  Item: {item.item_id}")
        print(f"    Type: {item.item_type}")
        print(f"    Subreddit: r/{item.subreddit}")
        print(f"    Role: {item.role}")
        print(f"    Score: {item.score}")
        print(f"    Engagement weight: {item.engagement_weight:.4f}")
        print(f"    Author weight: {item.author_weight:.4f}")
        print(f"    Fingerprint: {item.fingerprint}")
    
    print("\n[[OK]] Full ingestion demo complete")


def demo_whitelist_enforcement() -> None:
    """Demo: Critical whitelist enforcement."""
    section("WHITELIST ENFORCEMENT (CRITICAL)")
    
    print("Per contract: System MUST NOT ingest data outside whitelist")
    print("Non-whitelisted subreddits are IGNORED COMPLETELY\n")
    
    # Create worker with single whitelisted source
    sources = [
        RedditSource(
            id=1,
            subreddit="Bitcoin",
            asset="BTC",
            role=SubredditRole.DISCUSSION,
            enabled=True,
            max_posts_per_run=100,
            priority=5
        )
    ]
    
    worker = create_test_worker(sources=sources)
    
    now = time.time()
    
    # Try to ingest from many non-whitelisted subreddits
    non_whitelisted = [
        "Ethereum", "Solana", "Cardano", "Dogecoin",
        "wallstreetbets", "stocks", "investing", "personalfinance",
        "politics", "news", "worldnews", "funny"
    ]
    
    print(f"Attempting to ingest from {len(non_whitelisted)} non-whitelisted subreddits:")
    
    for sub in non_whitelisted:
        item = RawRedditItem(
            item_id=f"post_{sub}",
            item_type=RedditItemType.POST,
            subreddit=sub,
            created_utc=now,
            text="BTC news!",
            score=1000,
            num_comments=500,
            author_karma=50000,
            author_name="attacker"
        )
        result = worker.handle_item(item)
        status = "BLOCKED" if result is None else "ACCEPTED"
        print(f"  r/{sub}: {status}")
    
    # Also try whitelisted
    print(f"\nIngesting from whitelisted r/Bitcoin:")
    item = RawRedditItem(
        item_id="post_bitcoin",
        item_type=RedditItemType.POST,
        subreddit="Bitcoin",
        created_utc=now,
        text="BTC looking strong!",
        score=100,
        num_comments=50,
        author_karma=10000,
        author_name="holder"
    )
    result = worker.handle_item(item)
    status = "ALLOWED" if result is not None else "BLOCKED"
    print(f"  r/Bitcoin: {status}")
    
    # Show metrics
    metrics = worker.get_metrics()
    print(f"\nFinal metrics:")
    print(f"  Total received: {metrics['received']}")
    print(f"  Accepted: {metrics['accepted']}")
    print(f"  Blocked (not whitelisted): {metrics['dropped_not_whitelisted']}")
    
    print("\n[[OK]] All non-whitelisted subreddits blocked successfully")


def demo_database_schema() -> None:
    """Demo: Database schema generation."""
    section("DATABASE SCHEMA")
    
    sql = get_create_reddit_sources_sql()
    
    print("SQL for reddit_sources and reddit_ingestion_state tables:")
    print("-" * 50)
    print(sql[:2000])  # Show first 2000 chars
    if len(sql) > 2000:
        print("... (truncated)")
    
    print("\n[[OK]] Schema demo complete")


def main() -> None:
    """Run all demos."""
    print("\n" + "=" * 60)
    print(" REDDIT INGESTION WORKER DEMO")
    print(" Crypto BotTrading Social Data Pipeline")
    print("=" * 60)
    
    print("\nKey contract points:")
    print("  - ONLY ingest from explicitly whitelisted subreddits")
    print("  - Non-whitelisted subreddits -> IGNORE COMPLETELY")
    print("  - Uses public JSON endpoints only (NO paid Reddit APIs)")
    print("  - Reddit provides CROWD CONFIRMATION, not early signals")
    print("  - Source reliability: 0.7 (higher than Twitter's 0.5)")
    
    try:
        demo_constants()
        demo_source_registry()
        demo_rate_limiting()
        demo_item_validation()
        demo_metrics_computation()
        demo_velocity_tracking()
        demo_whitelist_enforcement()
        demo_full_ingestion_flow()
        demo_database_schema()
        
        print("\n" + "=" * 60)
        print(" ALL DEMOS COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] Demo failed: {e}")
        raise


if __name__ == "__main__":
    main()
