"""
Demo script for Telegram Ingestion Worker.

This demonstrates:
1. Whitelist-based message filtering
2. Rate limiting (per-group and global)
3. Manipulation detection
4. Velocity calculation
5. Contract compliance

LOCKED TELEGRAM INGESTION CONTRACT.
"""

from datetime import datetime, timezone

from telegram_ingestion import (
    SourceType,
    SourceRole,
    TelegramSource,
    TelegramMessage,
    TelegramSourceRegistry,
    MessageRateLimiter,
    ManipulationDetector,
    VelocityCalculator,
    TelegramIngestionWorker,
    create_test_worker,
    get_create_telegram_sources_sql
)


def demo_whitelist_registry():
    """Demonstrate whitelist registry."""
    print("=" * 60)
    print("DEMO: Whitelist Registry")
    print("=" * 60)
    
    registry = TelegramSourceRegistry()
    
    # Define allowed sources
    sources = [
        TelegramSource(
            tg_id=1234567890,
            source_type=SourceType.CHANNEL,
            role=SourceRole.NEWS,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=30
        ),
        TelegramSource(
            tg_id=9876543210,
            source_type=SourceType.GROUP,
            role=SourceRole.PANIC,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=20
        ),
        TelegramSource(
            tg_id=5555555555,
            source_type=SourceType.GROUP,
            role=SourceRole.COMMUNITY,
            asset="BTC",
            enabled=False,  # Disabled
            max_msgs_per_min=10
        )
    ]
    
    count = registry.load_from_list(sources)
    print(f"\nLoaded {count} sources into registry")
    
    # Check whitelist status
    test_ids = [1234567890, 9876543210, 5555555555, 111111111]
    for tg_id in test_ids:
        is_whitelisted = registry.is_whitelisted(tg_id)
        print(f"  tg_id {tg_id}: whitelisted = {is_whitelisted}")
    
    # Get enabled sources
    enabled = registry.get_enabled_sources()
    print(f"\nEnabled sources: {len(enabled)}")
    for s in enabled:
        print(f"  - {s.tg_id}: {s.source_type.value}, role={s.role.value}")
    print()


def demo_message_filtering():
    """Demonstrate message filtering rules."""
    print("=" * 60)
    print("DEMO: Message Filtering Rules")
    print("=" * 60)
    
    sources = [
        TelegramSource(
            tg_id=123,
            source_type=SourceType.CHANNEL,
            role=SourceRole.NEWS,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=30
        )
    ]
    worker = create_test_worker(sources=sources)
    
    now = datetime.now(timezone.utc)
    
    test_cases = [
        ("Valid BTC message", TelegramMessage(123, "Bitcoin is up 10%!", now)),
        ("Not whitelisted", TelegramMessage(999, "BTC news", now)),
        ("Empty text", TelegramMessage(123, "", now)),
        ("No asset keyword", TelegramMessage(123, "Hello world", now)),
        ("Whitespace only", TelegramMessage(123, "   \n   ", now)),
    ]
    
    print("\nFiltering test messages:")
    for name, msg in test_cases:
        result = worker.handle_message(msg)
        status = "ACCEPTED" if result else "DROPPED"
        print(f"  {name}: {status}")
    
    metrics = worker.get_metrics()
    print(f"\nMetrics: {metrics}")
    print()


def demo_rate_limiting():
    """Demonstrate rate limiting."""
    print("=" * 60)
    print("DEMO: Rate Limiting")
    print("=" * 60)
    
    sources = [
        TelegramSource(
            tg_id=123,
            source_type=SourceType.CHANNEL,
            role=SourceRole.NEWS,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=5  # Low limit for demo
        )
    ]
    worker = create_test_worker(sources=sources, global_rate_limit=10)
    
    now = datetime.now(timezone.utc)
    
    print("\nSending 8 messages to group with limit=5:")
    accepted = 0
    dropped = 0
    
    for i in range(8):
        msg = TelegramMessage(
            chat_id=123,
            text=f"Bitcoin update #{i+1}",
            timestamp=now
        )
        result = worker.handle_message(msg)
        if result:
            accepted += 1
            print(f"  Message {i+1}: ACCEPTED")
        else:
            dropped += 1
            print(f"  Message {i+1}: DROPPED (rate limit)")
    
    print(f"\nResult: {accepted} accepted, {dropped} dropped")
    print()


def demo_manipulation_detection():
    """Demonstrate manipulation detection."""
    print("=" * 60)
    print("DEMO: Manipulation Detection")
    print("=" * 60)
    
    detector = ManipulationDetector(threshold=3)
    
    # Same message in multiple chats = manipulation
    text = "BTC to 100K! Buy now!"
    
    print(f"\nSending same message to multiple chats:")
    print(f"  Text: '{text}'")
    
    for i, chat_id in enumerate([111, 222, 333, 444]):
        is_manip, fingerprint = detector.check_manipulation(text, chat_id)
        status = "MANIPULATION DETECTED" if is_manip else "OK"
        print(f"  Chat {chat_id}: {status}")
    
    print("\nNote: Manipulation flagged when same message appears")
    print("      in 3+ different chats within detection window.")
    print()


def demo_velocity_calculation():
    """Demonstrate velocity calculation."""
    print("=" * 60)
    print("DEMO: Velocity Calculation")
    print("=" * 60)
    
    sources = [
        TelegramSource(
            tg_id=123,
            source_type=SourceType.CHANNEL,
            role=SourceRole.NEWS,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=100
        )
    ]
    worker = create_test_worker(sources=sources)
    
    now = datetime.now(timezone.utc)
    
    print("\nSending messages and tracking velocity:")
    
    for i in range(5):
        msg = TelegramMessage(
            chat_id=123,
            text=f"Bitcoin news update #{i+1}",
            timestamp=now
        )
        result = worker.handle_message(msg)
        if result:
            print(f"  Message {i+1}: velocity = {result.velocity}")
    print()


def demo_full_pipeline():
    """Demonstrate full ingestion pipeline."""
    print("=" * 60)
    print("DEMO: Full Ingestion Pipeline")
    print("=" * 60)
    
    # Define sources
    sources = [
        TelegramSource(
            tg_id=1001,
            source_type=SourceType.CHANNEL,
            role=SourceRole.NEWS,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=30
        ),
        TelegramSource(
            tg_id=1002,
            source_type=SourceType.GROUP,
            role=SourceRole.PANIC,
            asset="BTC",
            enabled=True,
            max_msgs_per_min=20
        )
    ]
    
    # Track processed messages
    processed_messages = []
    
    def on_message(msg):
        processed_messages.append(msg)
    
    worker = create_test_worker(sources=sources)
    worker.on_message = on_message
    worker.start()
    
    print(f"\nWorker started: {worker.is_running}")
    print(f"Whitelisted IDs: {worker.get_whitelisted_ids()}")
    
    now = datetime.now(timezone.utc)
    
    # Simulate incoming messages
    messages = [
        TelegramMessage(1001, "Bitcoin breaking out! $BTC", now),
        TelegramMessage(1002, "PANIC: BTC dropping fast!", now),
        TelegramMessage(9999, "This should be ignored", now),  # Not whitelisted
        TelegramMessage(1001, "More BTC news here", now),
    ]
    
    print("\nProcessing messages:")
    for msg in messages:
        result = worker.handle_message(msg)
        if result:
            print(f"  Chat {msg.chat_id}: ACCEPTED")
            print(f"    - Asset: {result.asset}")
            print(f"    - Role: {result.role}")
            print(f"    - Source reliability: {result.source_reliability}")
        else:
            print(f"  Chat {msg.chat_id}: DROPPED")
    
    worker.stop()
    print(f"\nWorker stopped: {not worker.is_running}")
    
    metrics = worker.get_metrics()
    print(f"\nFinal metrics:")
    print(f"  Received: {metrics['received']}")
    print(f"  Accepted: {metrics['accepted']}")
    print(f"  Dropped (not whitelisted): {metrics['dropped_not_whitelisted']}")
    print()


def demo_sql_schema():
    """Show SQL schema for telegram_sources table."""
    print("=" * 60)
    print("DEMO: PostgreSQL Schema")
    print("=" * 60)
    
    sql = get_create_telegram_sources_sql()
    print("\nSQL to create telegram_sources table:")
    print(sql)
    print()


def main():
    print("\n" + "#" * 60)
    print("# TELEGRAM INGESTION WORKER - DEMO")
    print("# Crypto Social Data Pipeline")
    print("#" * 60)
    print("\nLOCKED TELEGRAM INGESTION CONTRACT")
    print("- Only whitelisted chats are processed")
    print("- Non-whitelisted chats are COMPLETELY IGNORED")
    print("- Rate limits prevent overwhelming the system")
    print("- Messages provide RISK CONTEXT ONLY")
    
    demo_whitelist_registry()
    demo_message_filtering()
    demo_rate_limiting()
    demo_manipulation_detection()
    demo_velocity_calculation()
    demo_full_pipeline()
    demo_sql_schema()
    
    print("=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)
    print("\nTo use in production:")
    print("  1. Create telegram_sources table in PostgreSQL")
    print("  2. Insert whitelisted tg_id values")
    print("  3. Load registry from database")
    print("  4. Connect Telegram client (Telethon)")
    print("  5. Route messages through worker.handle_message()")
    print("\n")


if __name__ == "__main__":
    main()
