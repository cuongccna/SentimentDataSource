#!/usr/bin/env python3
"""Add more Telegram channels and Reddit subreddits to database."""

import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='sentiment_db',
    user='sentiment_user',
    password='Cuongnv123456'
)
cur = conn.cursor()

# ============================================================================
# ADD 10 MORE TELEGRAM CHANNELS (Crypto focused, reputable)
# ============================================================================
telegram_channels = [
    # Channel ID, Channel Name, Asset, Channel Type, Priority
    ('-1001157066728', 'Coin Bureau', 'BTC', 'education', 8),
    ('-1001382978349', 'The Block', 'BTC', 'news', 8),
    ('-1001221690015', 'Messari', 'BTC', 'analytics', 7),
    ('-1001487169582', 'Santiment', 'BTC', 'analytics', 7),
    ('-1001524675531', 'Glassnode Alerts', 'BTC', 'analytics', 8),
    ('-1001296358563', 'CoinDesk', 'BTC', 'news', 7),
    ('-1001413670564', 'Cointelegraph', 'BTC', 'news', 7),
    ('-1001247280084', 'Bitcoin Archive', 'BTC', 'news', 6),
    ('-1001382141207', 'Crypto Panic', 'BTC', 'aggregator', 6),
    ('-1001225143879', 'Decrypt', 'BTC', 'news', 6),
]

print("Adding 10 new Telegram channels...")
for channel in telegram_channels:
    try:
        cur.execute("""
            INSERT INTO telegram_sources (channel_id, channel_name, asset, channel_type, enabled, priority)
            VALUES (%s, %s, %s, %s, TRUE, %s)
            ON CONFLICT (channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name,
                priority = EXCLUDED.priority
        """, channel)
        print(f"  ✅ Added: {channel[1]}")
    except Exception as e:
        print(f"  ❌ Error adding {channel[1]}: {e}")

# ============================================================================
# ADD 10 MORE REDDIT SUBREDDITS (Crypto focused)
# ============================================================================
reddit_subs = [
    # Subreddit, Asset, Role, Max Posts, Priority
    ('CryptoTechnology', 'BTC', 'technical', 50, 6),
    ('ethfinance', 'ETH', 'discussion', 50, 5),
    ('defi', 'BTC', 'discussion', 30, 4),
    ('BitcoinBeginners', 'BTC', 'education', 30, 3),
    ('CryptoMoonShots', 'BTC', 'speculation', 20, 2),
    ('SatoshiStreetBets', 'BTC', 'trading', 30, 3),
    ('BitcoinMining', 'BTC', 'mining', 20, 4),
    ('CryptoCurrencyTrading', 'BTC', 'trading', 40, 5),
    ('altcoin', 'BTC', 'discussion', 20, 2),
    ('Bitcoincash', 'BCH', 'discussion', 20, 2),
]

print("\nAdding 10 new Reddit subreddits...")
for sub in reddit_subs:
    try:
        cur.execute("""
            INSERT INTO reddit_sources (subreddit, asset, role, enabled, max_posts_per_run, priority)
            VALUES (%s, %s, %s, TRUE, %s, %s)
            ON CONFLICT (subreddit) DO UPDATE SET
                role = EXCLUDED.role,
                priority = EXCLUDED.priority
        """, sub)
        print(f"  ✅ Added: r/{sub[0]}")
    except Exception as e:
        print(f"  ❌ Error adding r/{sub[0]}: {e}")

conn.commit()

# ============================================================================
# VERIFY
# ============================================================================
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

cur.execute("SELECT COUNT(*) FROM telegram_sources")
tg_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM reddit_sources")
reddit_count = cur.fetchone()[0]

print(f"\n📱 Telegram channels: {tg_count}")
print(f"🔴 Reddit subreddits: {reddit_count}")

# Show all Telegram
cur.execute("""
    SELECT channel_id, channel_name, channel_type, priority 
    FROM telegram_sources 
    ORDER BY priority DESC
""")
print("\n=== ALL TELEGRAM SOURCES ===")
for r in cur.fetchall():
    print(f"  [{r[3]}] {r[1]} ({r[2]})")

# Show all Reddit
cur.execute("""
    SELECT subreddit, role, priority 
    FROM reddit_sources 
    ORDER BY priority DESC
""")
print("\n=== ALL REDDIT SOURCES ===")
for r in cur.fetchall():
    print(f"  [{r[2]}] r/{r[0]} ({r[1]})")

conn.close()
print("\n✅ Done!")
