#!/usr/bin/env python3
"""Check tweets in database."""

import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='sentiment_db',
    user='sentiment_user',
    password='Cuongnv123456'
)
cur = conn.cursor()

# Check columns
cur.execute("""
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'twitter_messages'
""")
print("Columns:", [r[0] for r in cur.fetchall()])

# Count tweets
cur.execute("SELECT COUNT(*) FROM twitter_messages")
print(f"\nTotal tweets in DB: {cur.fetchone()[0]}")

# Sample tweets
cur.execute("""
    SELECT username, text, created_at 
    FROM twitter_messages 
    ORDER BY created_at DESC LIMIT 5
""")
print("\nRecent tweets:")
for row in cur.fetchall():
    print(f"  @{row[0]}: {row[1][:80]}...")
    print(f"    Created: {row[2]}")

# Tweets per user
cur.execute("""
    SELECT username, COUNT(*) as cnt 
    FROM twitter_messages 
    GROUP BY username 
    ORDER BY cnt DESC LIMIT 10
""")
print("\nTweets per account:")
for row in cur.fetchall():
    print(f"  @{row[0]}: {row[1]}")

conn.close()
