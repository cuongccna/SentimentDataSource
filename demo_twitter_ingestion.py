"""
Demo of Twitter/X Ingestion Worker for Crypto BotTrading.

LOCKED TWITTER INGESTION CONTRACT.

Demonstrates:
- Whitelist-based ingestion (ONLY whitelisted sources)
- Source types: account, list, query
- Rate limiting (per-source and global)
- Tweet validation (asset keywords, timestamp precision)
- Metrics computation (engagement weight, author weight)
- State persistence
- Safety guarantees

ABSOLUTE NON-NEGOTIABLE RULES:
- DO NOT read the global Twitter firehose
- DO NOT search all tweets containing BTC
- DO NOT scrape trending topics
- DO NOT infer which accounts or queries to read
- DO NOT use paid Twitter APIs
- DO NOT generate trading signals
- ONLY ingest from explicitly whitelisted sources
- If a source is not whitelisted → IGNORE COMPLETELY
"""

from datetime import datetime, timezone, timedelta
import time

from twitter_ingestion import (
    TwitterSourceType,
    SourceRole,
    TwitterSource,
    RawTweet,
    TwitterSourceRegistry,
    TweetRateLimiter,
    VelocityCalculator,
    TwitterIngestionWorker,
    compute_total_engagement,
    compute_engagement_weight,
    compute_author_weight,
    create_ingestion_worker,
    create_test_worker,
    get_create_twitter_sources_sql,
)


def print_section(title: str) -> None:
    """Print section header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def demo_whitelist_enforcement():
    """Demonstrate whitelist enforcement - core safety feature."""
    print_section("1. WHITELIST ENFORCEMENT (SAFETY CRITICAL)")
    
    # Create whitelist with 3 sources
    sources = [
        TwitterSource(
            id=1,
            source_type=TwitterSourceType.ACCOUNT,
            value="CryptoNewsOfficial",
            asset="BTC",
            role=SourceRole.NEWS,
            enabled=True,
            max_tweets_per_min=30,
            priority=10
        ),
        TwitterSource(
            id=2,
            source_type=TwitterSourceType.ACCOUNT,
            value="BTCAnalyst",
            asset="BTC",
            role=SourceRole.ANALYST,
            enabled=True,
            max_tweets_per_min=20,
            priority=5
        ),
        TwitterSource(
            id=3,
            source_type=TwitterSourceType.QUERY,
            value="BTC halving 2024",
            asset="BTC",
            role=SourceRole.COMMUNITY,
            enabled=True,
            max_tweets_per_min=50,
            priority=3
        )
    ]
    
    worker = create_test_worker(sources=sources)
    now = datetime.now(timezone.utc)
    
    print("\n✅ Registered whitelist sources:")
    for s in sources:
        print(f"   - ID {s.id}: {s.source_type.value} '{s.value}' ({s.role.value})")
    
    # Test whitelisted tweet
    print("\n📩 Testing whitelisted tweet (source_id=1)...")
    tweet1 = RawTweet(
        tweet_id="tweet_001",
        created_at=now,
        text="BTC breaking $100k resistance level!",
        like_count=500,
        retweet_count=200,
        reply_count=50,
        author_followers_count=100000,
        author_username="CryptoNewsOfficial",
        source_id=1
    )
    
    result1 = worker.handle_tweet(tweet1)
    if result1:
        print(f"   ✅ ACCEPTED: {result1.text[:40]}...")
        print(f"      - Engagement weight: {result1.engagement_weight}")
        print(f"      - Author weight: {result1.author_weight}")
        print(f"      - Role: {result1.role}")
    
    # Test NON-whitelisted tweet (MUST BE REJECTED)
    print("\n📩 Testing NON-whitelisted tweet (source_id=999)...")
    tweet2 = RawTweet(
        tweet_id="tweet_002",
        created_at=now,
        text="BTC spam from unknown source!",
        like_count=1000000,  # High engagement doesn't matter
        retweet_count=500000,
        reply_count=100000,
        author_followers_count=10000000,  # High followers doesn't matter
        author_username="UnknownSpammer",
        source_id=999  # NOT IN WHITELIST
    )
    
    result2 = worker.handle_tweet(tweet2)
    if result2 is None:
        print("   🚫 REJECTED: Source not whitelisted (CORRECT BEHAVIOR)")
    
    # Test disabled source
    print("\n📩 Testing disabled source (source_id=2, now disabled)...")
    sources[1].enabled = False  # Disable analyst
    worker.registry.load_from_list(sources)
    
    tweet3 = RawTweet(
        tweet_id="tweet_003",
        created_at=now,
        text="BTC technical analysis from disabled source",
        like_count=100,
        retweet_count=20,
        reply_count=5,
        author_followers_count=50000,
        source_id=2  # Disabled
    )
    
    result3 = worker.handle_tweet(tweet3)
    if result3 is None:
        print("   🚫 REJECTED: Source disabled (CORRECT BEHAVIOR)")
    
    metrics = worker.get_metrics()
    print("\n📊 Metrics:")
    print(f"   - Received: {metrics['received']}")
    print(f"   - Accepted: {metrics['accepted']}")
    print(f"   - Dropped (not whitelisted): {metrics['dropped_not_whitelisted']}")
    print(f"   - Dropped (disabled): {metrics['dropped_disabled']}")


def demo_rate_limiting():
    """Demonstrate rate limiting (per-source and global)."""
    print_section("2. RATE LIMITING")
    
    # Source with low rate limit
    sources = [
        TwitterSource(
            id=1,
            source_type=TwitterSourceType.ACCOUNT,
            value="HighVolumeSource",
            asset="BTC",
            role=SourceRole.COMMUNITY,
            enabled=True,
            max_tweets_per_min=5,  # Only 5 tweets per minute!
            priority=0
        )
    ]
    
    # Global limit of 10
    worker = create_test_worker(sources=sources, global_rate_limit=10)
    now = time.time()
    dt_now = datetime.now(timezone.utc)
    
    print(f"\n⚙️  Per-source limit: 5 tweets/min")
    print(f"⚙️  Global limit: 10 tweets/min")
    
    print("\n📩 Sending 20 tweets from source_id=1...")
    
    accepted = 0
    for i in range(20):
        tweet = RawTweet(
            tweet_id=f"rate_test_{i}",
            created_at=dt_now + timedelta(seconds=i),
            text=f"BTC update #{i} - testing rate limits",
            like_count=10,
            retweet_count=5,
            reply_count=1,
            author_followers_count=1000,
            source_id=1
        )
        result = worker.handle_tweet(tweet, now=now)
        if result:
            accepted += 1
    
    print(f"\n📊 Results:")
    print(f"   - Total sent: 20")
    print(f"   - Accepted: {accepted}")
    print(f"   - Dropped (rate limit): {20 - accepted}")
    
    metrics = worker.get_metrics()
    print(f"\n   Detailed drops:")
    print(f"   - Source rate exceeded: {metrics['dropped_source_rate']}")


def demo_tweet_validation():
    """Demonstrate tweet validation rules."""
    print_section("3. TWEET VALIDATION")
    
    sources = [
        TwitterSource(
            id=1,
            source_type=TwitterSourceType.ACCOUNT,
            value="Validator",
            asset="BTC",
            role=SourceRole.NEWS,
            enabled=True,
            max_tweets_per_min=100,
            priority=0
        )
    ]
    
    worker = create_test_worker(sources=sources)
    now = datetime.now(timezone.utc)
    
    test_cases = [
        # (description, tweet, expected_accepted)
        (
            "Valid tweet with BTC keyword",
            RawTweet(
                tweet_id="valid_1",
                created_at=now,
                text="BTC is looking bullish today!",
                like_count=100,
                retweet_count=50,
                reply_count=10,
                author_followers_count=10000,
                source_id=1
            ),
            True
        ),
        (
            "Valid tweet with Bitcoin keyword",
            RawTweet(
                tweet_id="valid_2",
                created_at=now + timedelta(seconds=1),
                text="Bitcoin breaking new highs!",
                like_count=200,
                retweet_count=100,
                reply_count=20,
                author_followers_count=20000,
                source_id=1
            ),
            True
        ),
        (
            "Invalid: No asset keyword",
            RawTweet(
                tweet_id="invalid_1",
                created_at=now + timedelta(seconds=2),
                text="The market is looking good today",  # No BTC/Bitcoin
                like_count=100,
                retweet_count=50,
                reply_count=10,
                author_followers_count=10000,
                source_id=1
            ),
            False
        ),
        (
            "Invalid: Empty text",
            RawTweet(
                tweet_id="invalid_2",
                created_at=now + timedelta(seconds=3),
                text="",
                like_count=100,
                retweet_count=50,
                reply_count=10,
                author_followers_count=10000,
                source_id=1
            ),
            False
        ),
        (
            "Invalid: Minute-only timestamp (low precision)",
            RawTweet(
                tweet_id="invalid_3",
                created_at=datetime(2025, 1, 17, 10, 0, 0, tzinfo=timezone.utc),  # No seconds
                text="BTC update with bad timestamp",
                like_count=100,
                retweet_count=50,
                reply_count=10,
                author_followers_count=10000,
                source_id=1
            ),
            False
        ),
    ]
    
    print("\n📝 Testing validation rules:")
    
    for description, tweet, expected in test_cases:
        result = worker.handle_tweet(tweet)
        status = "✅ ACCEPTED" if result else "🚫 REJECTED"
        expected_str = "expected" if (result is not None) == expected else "UNEXPECTED!"
        print(f"\n   {description}")
        print(f"   → {status} ({expected_str})")
    
    metrics = worker.get_metrics()
    print(f"\n📊 Validation metrics:")
    print(f"   - Dropped (no keyword): {metrics['dropped_no_keyword']}")
    print(f"   - Dropped (empty): {metrics['dropped_empty']}")
    print(f"   - Dropped (low precision): {metrics['dropped_low_precision']}")


def demo_metrics_computation():
    """Demonstrate engagement and author weight computation."""
    print_section("4. METRICS COMPUTATION")
    
    print("\n📐 Engagement Weight Formula: log(1 + likes + 2*retweets + replies)")
    print("📐 Author Weight Formula: log(1 + followers)")
    
    test_cases = [
        ("Viral tweet", 10000, 5000, 1000, 1000000),
        ("Popular tweet", 500, 200, 50, 50000),
        ("Normal tweet", 50, 10, 5, 5000),
        ("Low engagement", 5, 1, 0, 500),
    ]
    
    print("\n📊 Sample calculations:")
    
    for name, likes, retweets, replies, followers in test_cases:
        total = compute_total_engagement(likes, retweets, replies)
        engagement = compute_engagement_weight(likes, retweets, replies)
        author = compute_author_weight(followers)
        
        print(f"\n   {name}:")
        print(f"      Likes: {likes:,}, Retweets: {retweets:,}, Replies: {replies:,}")
        print(f"      Total engagement: {total:,}")
        print(f"      Engagement weight: {engagement:.4f}")
        print(f"      Followers: {followers:,}")
        print(f"      Author weight: {author:.4f}")


def demo_velocity_calculation():
    """Demonstrate velocity calculation for burst detection."""
    print_section("5. VELOCITY CALCULATION")
    
    calculator = VelocityCalculator(short_window=10, long_window=60)
    now = time.time()
    
    print("\n📈 Velocity = (short window rate) / (long window rate)")
    print("   Short window: 10 seconds, Long window: 60 seconds")
    
    # Simulate baseline activity
    print("\n📩 Simulating baseline activity (10 tweets over 60 seconds)...")
    for i in range(10):
        calculator.record_tweet(1, now - 60 + i * 6)  # Spread over 60 seconds
    
    baseline_velocity = calculator.get_velocity(1, now)
    print(f"   Baseline velocity: {baseline_velocity:.2f}")
    
    # Simulate burst
    print("\n📩 Simulating burst (10 tweets in 10 seconds)...")
    for i in range(10):
        calculator.record_tweet(1, now - 10 + i)  # All in last 10 seconds
    
    burst_velocity = calculator.get_velocity(1, now)
    print(f"   Burst velocity: {burst_velocity:.2f}")
    
    if burst_velocity > 1.5:
        print("   ⚠️  HIGH VELOCITY DETECTED - Potential FOMO/Panic signal!")


def demo_state_persistence():
    """Demonstrate state persistence for cursor management."""
    print_section("6. STATE PERSISTENCE")
    
    print("\n💾 State MUST survive restart")
    print("💾 NO silent reset of cursors")
    
    sources = [
        TwitterSource(
            id=1,
            source_type=TwitterSourceType.ACCOUNT,
            value="PersistentSource",
            asset="BTC",
            role=SourceRole.NEWS,
            enabled=True,
            max_tweets_per_min=100,
            priority=0
        )
    ]
    
    worker = create_test_worker(sources=sources)
    now = datetime.now(timezone.utc)
    
    # Process some tweets
    print("\n📩 Processing 5 tweets...")
    for i in range(5):
        tweet = RawTweet(
            tweet_id=f"state_test_{i}",
            created_at=now + timedelta(seconds=i),
            text=f"BTC update #{i}",
            like_count=10,
            retweet_count=5,
            reply_count=1,
            author_followers_count=1000,
            source_id=1
        )
        worker.handle_tweet(tweet)
    
    state = worker.get_source_state(1)
    print(f"\n💾 Current state for source_id=1:")
    print(f"   - Last tweet ID: {state.last_processed_tweet_id}")
    print(f"   - Last event time: {state.last_processed_event_time}")
    print(f"   - Tweets processed: {state.tweets_processed}")
    
    # Try to reprocess same tweet
    print("\n📩 Attempting to reprocess same tweet...")
    duplicate = RawTweet(
        tweet_id="state_test_4",  # Same as last processed
        created_at=now + timedelta(seconds=4),
        text="BTC update #4",
        like_count=10,
        retweet_count=5,
        reply_count=1,
        author_followers_count=1000,
        source_id=1
    )
    result = worker.handle_tweet(duplicate)
    
    if result is None:
        print("   🚫 REJECTED: Already processed (CORRECT BEHAVIOR)")


def demo_database_schema():
    """Show database schema for Twitter sources."""
    print_section("7. DATABASE SCHEMA")
    
    print("\n📋 SQL for creating Twitter sources table:")
    print("-" * 50)
    
    sql = get_create_twitter_sources_sql()
    print(sql)
    
    print("-" * 50)
    print("\n💡 Tables created:")
    print("   - twitter_sources: Whitelist registry")
    print("   - twitter_ingestion_state: Cursor persistence")


def demo_safety_summary():
    """Summarize safety guarantees."""
    print_section("8. SAFETY GUARANTEES SUMMARY")
    
    print("""
    ✅ GUARANTEED BEHAVIORS:
    
    1. WHITELIST ONLY
       - System MUST NOT ingest tweets outside whitelist
       - Non-whitelisted sources → IGNORE COMPLETELY
       
    2. BOUNDED INGESTION
       - Per-source rate limits enforced
       - Global rate limit: 500 tweets/min MAX
       - Adding many Twitter follows MUST NOT increase load
       
    3. DETERMINISTIC
       - Same input → Same output
       - No random or inferred sources
       
    4. STATE PERSISTENCE
       - Cursors survive restart
       - No silent reset
       
    5. NO TRADING SIGNALS
       - Twitter provides context only
       - Does NOT decide sentiment or trades
    
    🚫 PROHIBITED BEHAVIORS:
    
    - DO NOT read global Twitter firehose
    - DO NOT search all tweets containing BTC
    - DO NOT scrape trending topics
    - DO NOT infer which accounts to read
    - DO NOT use paid Twitter APIs
    - DO NOT generate trading signals
    """)


def main():
    """Run all demos."""
    print("\n" + "=" * 60)
    print("  TWITTER/X INGESTION WORKER DEMO")
    print("  LOCKED TWITTER INGESTION CONTRACT")
    print("=" * 60)
    
    demo_whitelist_enforcement()
    demo_rate_limiting()
    demo_tweet_validation()
    demo_metrics_computation()
    demo_velocity_calculation()
    demo_state_persistence()
    demo_database_schema()
    demo_safety_summary()
    
    print("\n" + "=" * 60)
    print("  DEMO COMPLETE")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
