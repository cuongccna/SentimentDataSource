"""
Test pipeline insert flow - simulating what happens in background_worker.
"""
import os
from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timezone
import hashlib

def test_full_pipeline():
    """Test the complete pipeline including sentiment and risk stages."""
    print("="*60)
    print("TEST: Full Pipeline Flow (Raw → Sentiment → Risk)")
    print("="*60)
    
    from production_worker import PostgreSQLDatabase
    from background_worker import PipelineExecutor, WorkerMetrics
    
    db = PostgreSQLDatabase()
    metrics = WorkerMetrics()
    pipeline = PipelineExecutor(db, metrics)
    
    # Simulate event from Telegram collector
    event = {
        'text': 'BTC is pumping hard! Bullish momentum continues as whales accumulate ' + str(datetime.now()),
        'created_at': datetime.now(timezone.utc).isoformat(),
        'username': 'whale_alert',
        'likes': 500,
        'source_id': 'test_channel',
        'message_id': f'test_{datetime.now().timestamp()}'
    }
    source = 'telegram'
    
    print(f"\n1. Event: {event['text'][:50]}...")
    print(f"   Source: {source}")
    
    # Run through full pipeline
    print("\n2. Running pipeline.process_event()...")
    success = pipeline.process_event(event, source)
    
    if success:
        print(f"   ✅ Pipeline SUCCESS!")
    else:
        print(f"   ❌ Pipeline FAILED!")
        # Check metrics for failure reason
        print(f"\n   Metrics: {metrics}")
        
    # Check what was inserted
    print("\n3. Database state:")
    cursor = db.conn.cursor()
    
    # Count ingested_messages
    cursor.execute("SELECT COUNT(*) FROM ingested_messages WHERE source = 'telegram'")
    count_raw = cursor.fetchone()[0]
    print(f"   ingested_messages (telegram): {count_raw}")
    
    # Count sentiment_results
    cursor.execute("SELECT COUNT(*) FROM sentiment_results")
    count_sentiment = cursor.fetchone()[0]
    print(f"   sentiment_results: {count_sentiment}")
    
    # Count risk_indicators
    cursor.execute("SELECT COUNT(*) FROM risk_indicators")
    count_risk = cursor.fetchone()[0]
    print(f"   risk_indicators: {count_risk}")
    
    db.close()
    return success


def test_pipeline_flow():
    """Simulate exact pipeline flow from background_worker."""
    print("="*60)
    print("TEST: Pipeline Insert Flow")
    print("="*60)
    
    from production_worker import PostgreSQLDatabase
    
    # Simulate event from collector
    event = {
        'text': 'BTC hits $100K test pipeline ' + str(datetime.now()),
        'created_at': datetime.now(timezone.utc).isoformat(),
        'username': 'test_user',
        'likes': 100,
        'retweets': 50
    }
    source = 'twitter'
    
    print(f"\n1. Event: {event}")
    print(f"   Source: {source}")
    
    # Simulate pipeline logic
    text = event.get('text', '')
    fingerprint = hashlib.md5(text.encode()).hexdigest()
    event_time = datetime.now(timezone.utc)
    ingest_time = datetime.now(timezone.utc)
    
    # Compute metrics (simplified)
    source_reliability = 0.8
    engagement_weight = (event.get('likes', 0) + event.get('retweets', 0)) / 1000.0
    author_weight = 0.5
    velocity = 0.0
    manipulation_flag = False
    
    print(f"\n2. Computed values:")
    print(f"   fingerprint: {fingerprint}")
    print(f"   event_time: {event_time}")
    print(f"   source_reliability: {source_reliability}")
    print(f"   engagement_weight: {engagement_weight}")
    
    # Call insert_raw_event with same params as pipeline
    db = PostgreSQLDatabase()
    
    print("\n3. Calling insert_raw_event (pipeline style)...")
    try:
        raw_id = db.insert_raw_event(
            source=source,
            asset='BTC',
            event_time=event_time,
            ingest_time=ingest_time,  # This param is ignored
            text=text,
            source_reliability=source_reliability,
            engagement_weight=engagement_weight,
            author_weight=author_weight,
            velocity=velocity,
            manipulation_flag=manipulation_flag,  # This param is ignored
            fingerprint=fingerprint,
            dropped=False  # This param is ignored
        )
        
        if raw_id:
            print(f"   ✅ Insert SUCCESS! raw_id={raw_id}")
            
            # Verify in database
            cursor = db.conn.cursor()
            cursor.execute("""
                SELECT message_id, source, text, event_time, fingerprint 
                FROM ingested_messages 
                WHERE id = %s
            """, (raw_id,))
            row = cursor.fetchone()
            if row:
                print(f"\n4. Verification:")
                print(f"   message_id: {row[0]}")
                print(f"   source: {row[1]}")
                print(f"   text: {row[2][:50]}...")
                print(f"   event_time: {row[3]}")
                print(f"   fingerprint: {row[4]}")
            
            # Cleanup
            cursor.execute("DELETE FROM ingested_messages WHERE id = %s", (raw_id,))
            db.conn.commit()
            print("\n5. Cleanup done")
            return True
        else:
            print(f"   ❌ Insert returned None (possibly duplicate)")
            return False
            
    except Exception as e:
        print(f"   ❌ Insert FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()


if __name__ == "__main__":
    # Run full pipeline test first
    success1 = test_full_pipeline()
    
    print("\n")
    
    # Run simple insert test
    success2 = test_pipeline_flow()
    
    print("\n" + "="*60)
    if success1 and success2:
        print("🎉 All pipeline tests passed!")
    else:
        print(f"Results: Full pipeline={'✅' if success1 else '❌'}, Insert={'✅' if success2 else '❌'}")
    print("="*60)
