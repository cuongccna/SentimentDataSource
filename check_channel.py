#!/usr/bin/env python3
"""Check if a Telegram channel has been collected."""

import psycopg2
from db_config import get_database_connection

CHANNEL_ID = "5874672835"
CHANNEL_NAME = "Săn Kèo Tinh Hoa"

conn = get_database_connection()
cur = conn.cursor()

# Check telegram_sources
print("=" * 60)
print(f"Checking channel: {CHANNEL_NAME} (ID: {CHANNEL_ID})")
print("=" * 60)

print("\n📋 Telegram Sources Table:")
cur.execute("""
    SELECT channel_id, channel_name, enabled, priority
    FROM telegram_sources 
    WHERE channel_id = %s OR channel_name ILIKE %s
""", (CHANNEL_ID, f"%{CHANNEL_NAME[:10]}%"))
rows = cur.fetchall()
if rows:
    for r in rows:
        status = "✅ Enabled" if r[2] else "❌ Disabled"
        print(f"  ID: {r[0]}, Name: {r[1]}, {status}, Priority: {r[3]}")
else:
    print("  ❌ Not found in telegram_sources")

# Check ingested_messages from this channel (source_id is TEXT)
print("\n📊 Ingested Messages:")
cur.execute("""
    SELECT COUNT(*), MIN(created_at), MAX(created_at)
    FROM ingested_messages 
    WHERE source = 'telegram' AND source_id::text = %s
""", (CHANNEL_ID,))
row = cur.fetchone()
print(f"  Total: {row[0]} messages")
if row[1]:
    print(f"  First: {row[1]}")
    print(f"  Last:  {row[2]}")

# Show recent messages
if row[0] and row[0] > 0:
    print("\n📝 Recent messages:")
    cur.execute("""
        SELECT LEFT(text, 100), asset, created_at 
        FROM ingested_messages 
        WHERE source = 'telegram' AND source_id::text = %s
        ORDER BY created_at DESC
        LIMIT 5
    """, (CHANNEL_ID,))
    for r in cur.fetchall():
        print(f"  [{r[2]}] [{r[1]}] {r[0]}...")

# Check column type of source_id
print("\n📊 Column info:")
cur.execute("""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = 'ingested_messages' AND column_name = 'source_id'
""")
for r in cur.fetchall():
    print(f"  source_id type: {r[1]}")

cur.close()
conn.close()
