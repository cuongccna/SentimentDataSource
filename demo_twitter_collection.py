"""
Demo: Twitter Data Collection and Pipeline Integration.

This demo shows:
1. Real-time Twitter scraping from whitelisted accounts
2. Data normalization and filtering
3. Integration with Event Store
4. Database storage

Run this to test the full Twitter data collection pipeline.
"""

import os
import asyncio
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Import our modules
from twitter_scraper import TwitterScraper, ScrapedTweet, NormalizedRecord


def print_section(title: str) -> None:
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


async def demo_scraper():
    """Demo: Scrape Twitter data."""
    print_section("1. TWITTER SCRAPING")
    
    # Accounts to monitor
    accounts = [
        "whale_alert",      # Whale movements
        "Cointelegraph",    # Crypto news
        "BitcoinMagazine",  # Bitcoin news
        "WuBlockchain",     # Mining & news
        "DocumentingBTC",   # Bitcoin education
    ]
    
    print(f"\n📡 Monitoring {len(accounts)} Twitter accounts:")
    for acc in accounts:
        print(f"   - @{acc}")
    
    scraper = TwitterScraper(accounts=accounts)
    
    try:
        # Scrape BTC-related tweets
        records = await scraper.scrape(
            limit_per_account=15,
            filter_btc=True,
            deduplicate=True
        )
        
        print(f"\n✅ Collected {len(records)} BTC-related tweets")
        
        return records
        
    finally:
        await scraper.close()


def demo_normalization(records: list):
    """Demo: Show normalized data format."""
    print_section("2. DATA NORMALIZATION")
    
    print(f"\n📊 Normalized {len(records)} records")
    print("\nSample records:")
    
    for i, record in enumerate(records[:5], 1):
        data = record.to_dict()
        print(f"\n  Record {i}:")
        print(f"    Source: {data['source']}")
        print(f"    Asset: {data['asset']}")
        print(f"    Time: {data['timestamp']}")
        print(f"    Text: {data['text'][:60]}...")
        print(f"    Engagement: {data['metrics']['engagement_weight']:.2f}")
        print(f"    Velocity: {data['metrics']['velocity']:.2f}")
        print(f"    Reliability: {data['source_reliability']}")
        print(f"    Fingerprint: {data['fingerprint']}")


def demo_filtering(records: list):
    """Demo: Show filtering capabilities."""
    print_section("3. DATA FILTERING")
    
    # Group by source account
    by_account = {}
    for record in records:
        username = record.metadata.get("username", "unknown")
        if username not in by_account:
            by_account[username] = []
        by_account[username].append(record)
    
    print("\n📊 Tweets by account:")
    for username, tweets in sorted(by_account.items(), key=lambda x: -len(x[1])):
        print(f"   @{username}: {len(tweets)} tweets")
    
    # Find high-engagement tweets
    high_engagement = [r for r in records if r.engagement_weight > 1.0]
    print(f"\n🔥 High engagement tweets: {len(high_engagement)}")
    
    # Check for whale alerts
    whale_alerts = [r for r in records if "whale" in r.text.lower() or "💤" in r.text]
    print(f"🐋 Whale-related tweets: {len(whale_alerts)}")


async def demo_event_store(records: list):
    """Demo: Store to Event Store."""
    print_section("4. EVENT STORE INTEGRATION")
    
    try:
        from event_store import PostgresEventStore
        
        # Connect to database
        store = PostgresEventStore()
        
        if not store.connection:
            print("⚠️ Database not connected. Skipping Event Store demo.")
            return
        
        print("\n📦 Storing tweets to Event Store...")
        
        stored_count = 0
        
        for record in records:
            data = record.to_dict()
            
            # Create event
            success = store.append_event(
                event_type="social.twitter.tweet",
                payload=data,
                timestamp=record.timestamp
            )
            
            if success:
                stored_count += 1
        
        print(f"✅ Stored {stored_count}/{len(records)} events")
        
        # Query recent events
        recent = store.get_events_by_type("social.twitter.tweet", limit=5)
        print(f"\n📖 Recent Twitter events in store: {len(recent)}")
        
    except ImportError:
        print("⚠️ event_store module not available")
    except Exception as e:
        print(f"⚠️ Event Store error: {e}")


async def demo_database_storage(records: list):
    """Demo: Store to PostgreSQL twitter_messages table."""
    print_section("5. DATABASE STORAGE")
    
    try:
        import psycopg2
        
        # Connect to database
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
            user=os.getenv("POSTGRES_USER", "sentiment_user"),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )
        
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS twitter_messages (
                id SERIAL PRIMARY KEY,
                tweet_id VARCHAR(64) UNIQUE,
                username VARCHAR(64),
                text TEXT,
                event_time TIMESTAMP WITH TIME ZONE,
                asset VARCHAR(16),
                like_count INTEGER DEFAULT 0,
                retweet_count INTEGER DEFAULT 0,
                reply_count INTEGER DEFAULT 0,
                engagement_weight FLOAT DEFAULT 0,
                author_weight FLOAT DEFAULT 0,
                velocity FLOAT DEFAULT 1,
                source_reliability FLOAT DEFAULT 0.5,
                fingerprint VARCHAR(32),
                is_retweet BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_twitter_messages_time 
            ON twitter_messages(event_time);
            
            CREATE INDEX IF NOT EXISTS idx_twitter_messages_asset 
            ON twitter_messages(asset);
        """)
        conn.commit()
        
        print("📦 Storing tweets to PostgreSQL...")
        
        stored_count = 0
        
        for record in records:
            data = record.to_dict()
            meta = data.get("metadata", {})
            
            try:
                cursor.execute("""
                    INSERT INTO twitter_messages (
                        tweet_id, username, text, event_time, asset,
                        like_count, retweet_count, reply_count,
                        engagement_weight, author_weight, velocity,
                        source_reliability, fingerprint, is_retweet
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tweet_id) DO NOTHING
                """, (
                    meta.get("tweet_id"),
                    meta.get("username"),
                    data.get("text"),
                    data.get("timestamp"),
                    data.get("asset"),
                    meta.get("like_count", 0),
                    meta.get("retweet_count", 0),
                    meta.get("reply_count", 0),
                    data["metrics"]["engagement_weight"],
                    data["metrics"]["author_weight"],
                    data["metrics"]["velocity"],
                    data.get("source_reliability", 0.5),
                    data.get("fingerprint"),
                    meta.get("is_retweet", False)
                ))
                stored_count += 1
            except Exception as e:
                print(f"   Error storing tweet: {e}")
        
        conn.commit()
        
        print(f"✅ Stored {stored_count}/{len(records)} tweets to database")
        
        # Query summary
        cursor.execute("SELECT COUNT(*) FROM twitter_messages")
        total = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT username, COUNT(*) as cnt 
            FROM twitter_messages 
            GROUP BY username 
            ORDER BY cnt DESC 
            LIMIT 5
        """)
        top_accounts = cursor.fetchall()
        
        print(f"\n📊 Total tweets in database: {total}")
        print("\n   Top accounts:")
        for username, count in top_accounts:
            print(f"     @{username}: {count} tweets")
        
        cursor.close()
        conn.close()
        
    except ImportError:
        print("⚠️ psycopg2 not available")
    except Exception as e:
        print(f"⚠️ Database error: {e}")


async def main():
    """Run the full demo."""
    print("\n" + "=" * 60)
    print(" TWITTER DATA COLLECTION DEMO")
    print(" Real-time BTC Tweet Collection Pipeline")
    print("=" * 60)
    
    start_time = datetime.now()
    
    # Step 1: Scrape
    records = await demo_scraper()
    
    if not records:
        print("\n❌ No data collected. Exiting.")
        return
    
    # Step 2: Show normalization
    demo_normalization(records)
    
    # Step 3: Show filtering
    demo_filtering(records)
    
    # Step 4: Event Store (optional)
    await demo_event_store(records)
    
    # Step 5: Database storage
    await demo_database_storage(records)
    
    # Summary
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print_section("SUMMARY")
    print(f"\n  ⏱️ Total time: {elapsed:.1f} seconds")
    print(f"  📊 Tweets collected: {len(records)}")
    print(f"  📡 Sources: Twitter Syndication API")
    print(f"  💾 Storage: PostgreSQL")
    
    # Save to file
    output_file = "twitter_demo_output.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump([r.to_dict() for r in records], f, indent=2, ensure_ascii=False)
    print(f"\n  💾 Saved to: {output_file}")
    
    print("\n" + "=" * 60)
    print(" ✅ Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
