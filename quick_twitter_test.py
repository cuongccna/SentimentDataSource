"""Quick test of Twitter collector with batch rotation."""
import sys
sys.path.insert(0, '.')

from production_worker import TwitterSourceCollector

print("Testing Twitter collection with batch rotation...")

# Create collector (will load from DB and use batching)
collector = TwitterSourceCollector()

print(f"\nTotal accounts: {len(collector.all_accounts)}")
print(f"Batch size: {collector.ACCOUNTS_PER_BATCH}")
print(f"Current batch: {collector.accounts}")

# Collect tweets (this will rotate to next batch)
tweets = collector.collect(since=None)

print(f"\nCollected {len(tweets)} tweets")

if tweets:
    print("\nSample tweets:")
    for tweet in tweets[:5]:
        username = tweet.get("source_id", "unknown")
        text = tweet.get("text", "")[:60]
        engagement = tweet.get("metadata", {}).get("total_engagement", 0)
        print(f"  @{username}: {text}...")
        print(f"    Engagement: {engagement}")
