"""
Quick local test for production worker components.
"""
import asyncio
from datetime import datetime, timezone

def test_database():
    """Test database connection and auto message_id."""
    print("\n" + "="*60)
    print("TEST 1: DATABASE CONNECTION")
    print("="*60)
    
    from production_worker import PostgreSQLDatabase
    
    try:
        db = PostgreSQLDatabase()
        print("✅ Database connected")
        
        # Test insert with empty message_id (should auto-generate)
        test_id = db.insert_raw_event(
            message_id='',
            source='test_local',
            source_id='test',
            text='Test BTC message for local validation ' + str(datetime.now()),
            timestamp='2026-01-18T10:00:00Z',
            metadata={'test': True}
        )
        print(f"✅ Insert with auto ID returned: {test_id}")
        
        # Verify the record
        cursor = db.conn.cursor()
        cursor.execute(
            "SELECT message_id FROM ingested_messages WHERE id = %s",
            (test_id,)
        )
        row = cursor.fetchone()
        if row:
            print(f"✅ Auto-generated message_id: {row[0]}")
        
        # Test pipeline-style insert (with event_time instead of timestamp)
        test_id2 = db.insert_raw_event(
            source='test_pipeline',
            event_time=datetime.now(timezone.utc),
            text='Test pipeline insert for BTC ' + str(datetime.now()),
            fingerprint='test_fp_' + str(datetime.now().timestamp()),
            source_reliability=0.8,
            engagement_weight=0.5,
            author_weight=0.3,
            velocity=1.0
        )
        print(f"✅ Pipeline-style insert returned: {test_id2}")
        
        # Cleanup
        cursor.execute("DELETE FROM ingested_messages WHERE source IN ('test_local', 'test_pipeline')")
        db.conn.commit()
        print("✅ Cleanup done")
        
        db.close()
        return True
    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_twitter_collector():
    """Test Twitter collector."""
    print("\n" + "="*60)
    print("TEST 2: TWITTER COLLECTOR")
    print("="*60)
    
    from production_worker import TwitterSourceCollector
    
    try:
        collector = TwitterSourceCollector()
        events = collector.collect(since=None)
        print(f"✅ Collected {len(events)} Twitter events")
        
        if events:
            e = events[0]
            msg_id = e.get('message_id', 'MISSING')
            print(f"   First event message_id: {msg_id}")
            
            if not msg_id or msg_id == 'tw_':
                print("   ⚠️  WARNING: message_id is empty or invalid!")
            else:
                print("   ✅ message_id looks valid")
        
        return True
    except Exception as e:
        print(f"❌ Twitter collector error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_reddit_crawler():
    """Test Reddit crawler."""
    print("\n" + "="*60)
    print("TEST 3: REDDIT CRAWLER")
    print("="*60)
    
    from reddit_crawler import RedditCrawler
    
    try:
        crawler = RedditCrawler(subreddits=['bitcoin'])
        records = crawler.crawl_subreddit('bitcoin', post_limit=5)
        print(f"✅ Crawled {len(records)} Reddit records")
        
        if records:
            r = records[0]
            post_id = r.get('id', 'MISSING')
            print(f"   First record id: {post_id}")
            
            if not post_id:
                print("   ⚠️  WARNING: id is empty!")
            else:
                print("   ✅ id looks valid")
        else:
            print("   ⚠️  No records (may be due to BTC keyword filter)")
        
        return True
    except Exception as e:
        print(f"❌ Reddit crawler error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_telegram_reporter():
    """Test Telegram reporter config."""
    print("\n" + "="*60)
    print("TEST 4: TELEGRAM REPORTER CONFIG")
    print("="*60)
    
    from production_worker import TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID, REPORT_INTERVAL_MINUTES
    
    if TELEGRAM_BOT_TOKEN:
        print(f"✅ Bot Token: {TELEGRAM_BOT_TOKEN[:20]}...")
    else:
        print("❌ Bot Token: NOT SET")
    
    if TELEGRAM_CHANNEL_ID:
        print(f"✅ Channel ID: {TELEGRAM_CHANNEL_ID}")
    else:
        print("❌ Channel ID: NOT SET")
    
    print(f"ℹ️  Report Interval: {REPORT_INTERVAL_MINUTES} minutes")
    
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHANNEL_ID)


def test_telegram_collector():
    """Test Telegram collector (requires auth session)."""
    print("\n" + "="*60)
    print("TEST 5: TELEGRAM COLLECTOR")
    print("="*60)
    
    import os
    from production_worker import TelegramSourceCollector, HAS_TELETHON
    
    if not HAS_TELETHON:
        print("❌ Telethon not installed")
        return False
    
    # Check credentials
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    
    if not api_id or not api_hash:
        print("❌ TELEGRAM_API_ID or TELEGRAM_API_HASH not set")
        return False
    
    print(f"✅ API ID: {api_id}")
    print(f"✅ API Hash: {api_hash[:10]}...")
    
    # Check session file
    session_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "telegram_production_session.session"
    )
    
    if os.path.exists(session_file):
        print(f"✅ Session file exists: {session_file}")
    else:
        print(f"⚠️  Session file not found: {session_file}")
        print("   Run: python auth_telegram.py")
        return False
    
    try:
        collector = TelegramSourceCollector()
        events = collector.collect(since=None)
        print(f"✅ Collected {len(events)} Telegram events")
        
        if events:
            e = events[0]
            msg_id = e.get('message_id', 'MISSING')
            source_id = e.get('source_id', 'MISSING')
            print(f"   First event:")
            print(f"     message_id: {msg_id}")
            print(f"     source_id: {source_id}")
            
            if not msg_id or not msg_id.startswith('tg_'):
                print("   ⚠️  WARNING: message_id format invalid!")
            else:
                print("   ✅ message_id format valid")
        
        return True
    except Exception as e:
        print(f"❌ Telegram collector error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("="*60)
    print(" LOCAL PRODUCTION WORKER TEST")
    print("="*60)
    
    results = {
        "Database": test_database(),
        "Twitter": test_twitter_collector(),
        "Reddit": test_reddit_crawler(),
        "Telegram Config": test_telegram_reporter(),
        "Telegram Collector": test_telegram_collector(),
    }
    
    print("\n" + "="*60)
    print(" SUMMARY")
    print("="*60)
    
    all_passed = True
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 All tests passed! Ready to deploy.")
    else:
        print("⚠️  Some tests failed. Check logs above.")
    
    return all_passed


if __name__ == "__main__":
    main()
