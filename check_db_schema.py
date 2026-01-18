#!/usr/bin/env python3
"""Compare local database schema with deploy.sh migration."""

import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='sentiment_db',
    user='sentiment_user',
    password='Cuongnv123456'
)
cur = conn.cursor()

print("=" * 70)
print("LOCAL DATABASE SCHEMA ANALYSIS")
print("=" * 70)

# Get all tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name
""")
tables = [r[0] for r in cur.fetchall()]

print(f"\n📋 TABLES ({len(tables)} total):")
for t in tables:
    print(f"  - {t}")

# Get columns for each important table
important_tables = [
    'telegram_sources',
    'twitter_sources', 
    'twitter_messages',
    'reddit_sources',
    'ingested_messages',
    'sentiment_results',
    'aggregated_signals',
    'alert_history',
    'risk_indicators',
    'data_quality_metrics'
]

print("\n" + "=" * 70)
print("TABLE SCHEMAS")
print("=" * 70)

for table in important_tables:
    if table in tables:
        cur.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
        """)
        columns = cur.fetchall()
        print(f"\n✅ {table}:")
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            default = f" DEFAULT {col[3][:30]}..." if col[3] and len(str(col[3])) > 30 else (f" DEFAULT {col[3]}" if col[3] else "")
            print(f"   - {col[0]}: {col[1]} {nullable}{default}")
    else:
        print(f"\n❌ {table}: NOT FOUND")

# Check row counts
print("\n" + "=" * 70)
print("ROW COUNTS")
print("=" * 70)

for table in tables:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count} rows")
    except:
        pass

conn.close()
