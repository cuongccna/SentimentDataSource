"""
Pipeline Runner Script

This script performs the complete pipeline:
1. Check database connection and create tables
2. Insert sample data for telegram, twitter, reddit sources
3. Process sample messages and save to database
4. Run sentiment calculation and save results
5. Send alerts to Telegram channel
"""

import os
import sys
import time
import json
import hashlib
import asyncio
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

# Load environment variables
from dotenv import load_dotenv
load_dotenv(override=True)

# Database configuration from .env
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DATABASE", "sentiment_db"),
    "user": os.getenv("POSTGRES_USER", "sentiment_user"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

# Telegram configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")


def log(message: str, level: str = "INFO") -> None:
    """Log with timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{level}] {message}")


# =============================================================================
# STEP 1: DATABASE CONNECTION AND TABLE CREATION
# =============================================================================

def check_database_connection():
    """Check database connection and create tables."""
    log("=" * 60)
    log("STEP 1: DATABASE CONNECTION AND TABLE CREATION")
    log("=" * 60)
    
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        log("psycopg2 not installed. Installing...", "WARN")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
        import psycopg2
        from psycopg2.extras import RealDictCursor
    
    log(f"Connecting to PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        log("Database connection successful!", "SUCCESS")
        
        cursor = conn.cursor()
        
        # Create tables
        log("Creating tables...")
        
        # Telegram Sources Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS telegram_sources (
                id SERIAL PRIMARY KEY,
                channel_id VARCHAR(100) UNIQUE NOT NULL,
                channel_name VARCHAR(255) NOT NULL,
                asset VARCHAR(20) NOT NULL DEFAULT 'BTC',
                channel_type VARCHAR(50) NOT NULL DEFAULT 'signal',
                enabled BOOLEAN NOT NULL DEFAULT true,
                priority SMALLINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        log("  - telegram_sources table created")
        
        # Twitter Sources Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS twitter_sources (
                id SERIAL PRIMARY KEY,
                account_id VARCHAR(100) UNIQUE NOT NULL,
                handle VARCHAR(100) NOT NULL,
                asset VARCHAR(20) NOT NULL DEFAULT 'BTC',
                account_type VARCHAR(50) NOT NULL DEFAULT 'analyst',
                enabled BOOLEAN NOT NULL DEFAULT true,
                followers_weight REAL NOT NULL DEFAULT 1.0,
                priority SMALLINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        log("  - twitter_sources table created")
        
        # Reddit Sources Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS reddit_sources (
                id SERIAL PRIMARY KEY,
                subreddit VARCHAR(100) UNIQUE NOT NULL,
                asset VARCHAR(20) NOT NULL DEFAULT 'BTC',
                role VARCHAR(50) NOT NULL DEFAULT 'discussion',
                enabled BOOLEAN NOT NULL DEFAULT true,
                max_posts_per_run INTEGER NOT NULL DEFAULT 100,
                priority SMALLINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        log("  - reddit_sources table created")
        
        # Ingested Messages Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingested_messages (
                id SERIAL PRIMARY KEY,
                message_id VARCHAR(100) NOT NULL,
                source VARCHAR(20) NOT NULL,
                source_id INTEGER,
                source_name VARCHAR(255),
                asset VARCHAR(20) NOT NULL DEFAULT 'BTC',
                text TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                fingerprint VARCHAR(32) UNIQUE NOT NULL,
                engagement_weight REAL DEFAULT 0,
                author_weight REAL DEFAULT 0,
                velocity REAL DEFAULT 0,
                source_reliability REAL NOT NULL,
                raw_data JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_ingested_messages_source 
            ON ingested_messages(source);
            
            CREATE INDEX IF NOT EXISTS idx_ingested_messages_asset 
            ON ingested_messages(asset);
            
            CREATE INDEX IF NOT EXISTS idx_ingested_messages_event_time 
            ON ingested_messages(event_time);
        """)
        log("  - ingested_messages table created")
        
        # Sentiment Results Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_results (
                id SERIAL PRIMARY KEY,
                message_id INTEGER REFERENCES ingested_messages(id),
                asset VARCHAR(20) NOT NULL,
                sentiment_score REAL NOT NULL,
                sentiment_label VARCHAR(20) NOT NULL,
                confidence REAL NOT NULL,
                weighted_score REAL NOT NULL,
                signal_strength REAL NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_sentiment_results_asset 
            ON sentiment_results(asset);
            
            CREATE INDEX IF NOT EXISTS idx_sentiment_results_created_at 
            ON sentiment_results(created_at);
        """)
        log("  - sentiment_results table created")
        
        # Aggregated Signals Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS aggregated_signals (
                id SERIAL PRIMARY KEY,
                asset VARCHAR(20) NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                window_end TIMESTAMPTZ NOT NULL,
                total_messages INTEGER NOT NULL,
                bullish_count INTEGER NOT NULL DEFAULT 0,
                bearish_count INTEGER NOT NULL DEFAULT 0,
                neutral_count INTEGER NOT NULL DEFAULT 0,
                avg_sentiment REAL NOT NULL,
                weighted_sentiment REAL NOT NULL,
                signal_type VARCHAR(20) NOT NULL,
                signal_strength REAL NOT NULL,
                velocity REAL NOT NULL DEFAULT 0,
                sources_breakdown JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_aggregated_signals_asset 
            ON aggregated_signals(asset);
            
            CREATE INDEX IF NOT EXISTS idx_aggregated_signals_window 
            ON aggregated_signals(window_start, window_end);
        """)
        log("  - aggregated_signals table created")
        
        # Alert History Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                id SERIAL PRIMARY KEY,
                asset VARCHAR(20) NOT NULL,
                alert_type VARCHAR(50) NOT NULL,
                signal_id INTEGER REFERENCES aggregated_signals(id),
                message TEXT NOT NULL,
                telegram_message_id VARCHAR(100),
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                success BOOLEAN NOT NULL DEFAULT true
            );
        """)
        log("  - alert_history table created")
        
        conn.commit()
        log("All tables created successfully!", "SUCCESS")
        
        return conn
        
    except Exception as e:
        log(f"Database connection failed: {e}", "ERROR")
        raise


# =============================================================================
# STEP 2: INSERT SAMPLE SOURCES
# =============================================================================

def insert_sample_sources(conn):
    """Insert sample sources for telegram, twitter, and reddit."""
    log("")
    log("=" * 60)
    log("STEP 2: INSERT SAMPLE SOURCES")
    log("=" * 60)
    
    cursor = conn.cursor()
    
    # Telegram Sources
    telegram_sources = [
        ("whale_alert_btc", "Whale Alert BTC", "BTC", "whale", True, 5),
        ("btc_signals_vip", "BTC Signals VIP", "BTC", "signal", True, 4),
        ("crypto_news_fast", "Crypto News Fast", "BTC", "news", True, 3),
        ("trading_view_btc", "TradingView BTC Ideas", "BTC", "analysis", True, 3),
        ("btc_whale_watch", "BTC Whale Watch", "BTC", "whale", True, 4),
    ]
    
    log("Inserting Telegram sources...")
    for channel_id, name, asset, ctype, enabled, priority in telegram_sources:
        try:
            cursor.execute("""
                INSERT INTO telegram_sources (channel_id, channel_name, asset, channel_type, enabled, priority)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (channel_id) DO UPDATE SET 
                    channel_name = EXCLUDED.channel_name,
                    updated_at = NOW()
            """, (channel_id, name, asset, ctype, enabled, priority))
            log(f"  + {name} ({channel_id})")
        except Exception as e:
            log(f"  - Failed: {name}: {e}", "WARN")
    
    # Twitter Sources
    twitter_sources = [
        ("elonmusk", "elonmusk", "BTC", "influencer", True, 2.0, 5),
        ("saborweb3", "saborweb3", "BTC", "analyst", True, 1.5, 4),
        ("whale_alert", "whale_alert", "BTC", "whale", True, 1.8, 5),
        ("cabordog", "CaBorDog", "BTC", "analyst", True, 1.2, 3),
        ("btc_archive", "BTC_Archive", "BTC", "news", True, 1.0, 3),
        ("documentingbtc", "DocumentingBTC", "BTC", "news", True, 1.0, 3),
    ]
    
    log("Inserting Twitter sources...")
    for account_id, handle, asset, atype, enabled, followers_weight, priority in twitter_sources:
        try:
            cursor.execute("""
                INSERT INTO twitter_sources (account_id, handle, asset, account_type, enabled, followers_weight, priority)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO UPDATE SET 
                    handle = EXCLUDED.handle,
                    updated_at = NOW()
            """, (account_id, handle, asset, atype, enabled, followers_weight, priority))
            log(f"  + @{handle}")
        except Exception as e:
            log(f"  - Failed: @{handle}: {e}", "WARN")
    
    # Reddit Sources
    reddit_sources = [
        ("Bitcoin", "BTC", "discussion", True, 100, 5),
        ("CryptoCurrency", "BTC", "discussion", True, 100, 4),
        ("BitcoinMarkets", "BTC", "market", True, 75, 5),
        ("btc", "BTC", "discussion", True, 50, 3),
        ("CryptoMarkets", "BTC", "market", True, 50, 3),
    ]
    
    log("Inserting Reddit sources...")
    for subreddit, asset, role, enabled, max_posts, priority in reddit_sources:
        try:
            cursor.execute("""
                INSERT INTO reddit_sources (subreddit, asset, role, enabled, max_posts_per_run, priority)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (subreddit) DO UPDATE SET 
                    role = EXCLUDED.role,
                    updated_at = NOW()
            """, (subreddit, asset, role, enabled, max_posts, priority))
            log(f"  + r/{subreddit}")
        except Exception as e:
            log(f"  - Failed: r/{subreddit}: {e}", "WARN")
    
    conn.commit()
    log("Sample sources inserted successfully!", "SUCCESS")


# =============================================================================
# STEP 3: PROCESS SAMPLE MESSAGES
# =============================================================================

def process_sample_messages(conn):
    """Generate and process sample messages from all sources."""
    log("")
    log("=" * 60)
    log("STEP 3: PROCESS SAMPLE MESSAGES")
    log("=" * 60)
    
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)
    
    # Sample messages with different sentiments
    sample_messages = [
        # Telegram messages (source_reliability = 0.3)
        {
            "source": "telegram",
            "source_name": "Whale Alert BTC",
            "message_id": f"tg_{int(time.time()*1000)}_1",
            "text": "WHALE ALERT: 5,000 BTC ($500M) transferred from unknown wallet to Coinbase. Potential sell pressure incoming!",
            "event_time": now - timedelta(minutes=5),
            "engagement_weight": 2.5,
            "author_weight": 8.0,
            "source_reliability": 0.3,
        },
        {
            "source": "telegram",
            "source_name": "BTC Signals VIP",
            "message_id": f"tg_{int(time.time()*1000)}_2",
            "text": "BTC breaking out of resistance at $105k! Strong bullish momentum. Target: $110k within 48 hours. LONG signal confirmed.",
            "event_time": now - timedelta(minutes=3),
            "engagement_weight": 3.0,
            "author_weight": 7.5,
            "source_reliability": 0.3,
        },
        {
            "source": "telegram",
            "source_name": "Crypto News Fast",
            "message_id": f"tg_{int(time.time()*1000)}_3",
            "text": "BREAKING: SEC approves new Bitcoin ETF. Institutional adoption accelerating. Very bullish for BTC!",
            "event_time": now - timedelta(minutes=2),
            "engagement_weight": 4.0,
            "author_weight": 6.0,
            "source_reliability": 0.3,
        },
        
        # Twitter messages (source_reliability = 0.5)
        {
            "source": "twitter",
            "source_name": "@whale_alert",
            "message_id": f"tw_{int(time.time()*1000)}_1",
            "text": "1,000 BTC (105,234,567 USD) transferred from Binance to unknown wallet. Accumulation continues!",
            "event_time": now - timedelta(minutes=10),
            "engagement_weight": 5.2,  # log(1000 likes + 500 retweets)
            "author_weight": 12.0,     # log(1M followers)
            "source_reliability": 0.5,
        },
        {
            "source": "twitter",
            "source_name": "@saborweb3",
            "message_id": f"tw_{int(time.time()*1000)}_2",
            "text": "BTC technical analysis: Golden cross forming on the daily chart. This is extremely bullish. Last time this happened, we saw 40% gains in 30 days.",
            "event_time": now - timedelta(minutes=8),
            "engagement_weight": 4.0,
            "author_weight": 10.5,
            "source_reliability": 0.5,
        },
        {
            "source": "twitter",
            "source_name": "@CaBorDog",
            "message_id": f"tw_{int(time.time()*1000)}_3",
            "text": "Careful with BTC here. RSI showing overbought conditions. Short-term pullback possible to $98k support.",
            "event_time": now - timedelta(minutes=6),
            "engagement_weight": 3.5,
            "author_weight": 9.0,
            "source_reliability": 0.5,
        },
        
        # Reddit messages (source_reliability = 0.7)
        {
            "source": "reddit",
            "source_name": "r/Bitcoin",
            "message_id": f"rd_{int(time.time()*1000)}_1",
            "text": "Just bought my first Bitcoin! The future is here. HODL forever! This is life-changing technology.",
            "event_time": now - timedelta(minutes=15),
            "engagement_weight": 4.5,  # log(500 upvotes + 100 comments)
            "author_weight": 7.0,
            "source_reliability": 0.7,
        },
        {
            "source": "reddit",
            "source_name": "r/BitcoinMarkets",
            "message_id": f"rd_{int(time.time()*1000)}_2",
            "text": "Funding rates extremely positive. Leverage longs at all-time high. Be careful, this often precedes a correction.",
            "event_time": now - timedelta(minutes=12),
            "engagement_weight": 5.0,
            "author_weight": 8.5,
            "source_reliability": 0.7,
        },
        {
            "source": "reddit",
            "source_name": "r/CryptoCurrency",
            "message_id": f"rd_{int(time.time()*1000)}_3",
            "text": "Bitcoin dominance rising again. Altcoins bleeding. This is bullish for BTC in the medium term.",
            "event_time": now - timedelta(minutes=7),
            "engagement_weight": 4.2,
            "author_weight": 7.8,
            "source_reliability": 0.7,
        },
        {
            "source": "reddit",
            "source_name": "r/Bitcoin",
            "message_id": f"rd_{int(time.time()*1000)}_4",
            "text": "Sold all my BTC at 105k. Market is overheated. Will buy back at 90k. Mark my words, crash incoming!",
            "event_time": now - timedelta(minutes=4),
            "engagement_weight": 3.8,
            "author_weight": 6.5,
            "source_reliability": 0.7,
        },
    ]
    
    log(f"Processing {len(sample_messages)} sample messages...")
    
    inserted_count = 0
    for msg in sample_messages:
        # Generate fingerprint
        fingerprint = hashlib.md5(
            f"{msg['source']}:{msg['message_id']}:{msg['text'][:50]}".encode()
        ).hexdigest()[:16]
        
        # Prepare raw_data without datetime for JSON
        raw_data = {
            "source": msg["source"],
            "source_name": msg["source_name"],
            "message_id": msg["message_id"],
            "text": msg["text"],
            "event_time": msg["event_time"].isoformat(),
            "engagement_weight": msg["engagement_weight"],
            "author_weight": msg["author_weight"],
            "source_reliability": msg["source_reliability"],
        }
        
        try:
            cursor.execute("""
                INSERT INTO ingested_messages 
                (message_id, source, source_name, asset, text, event_time, 
                 fingerprint, engagement_weight, author_weight, source_reliability, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fingerprint) DO NOTHING
                RETURNING id
            """, (
                msg["message_id"],
                msg["source"],
                msg["source_name"],
                "BTC",
                msg["text"],
                msg["event_time"],
                fingerprint,
                msg["engagement_weight"],
                msg["author_weight"],
                msg["source_reliability"],
                json.dumps(raw_data)
            ))
            
            result = cursor.fetchone()
            if result:
                inserted_count += 1
                log(f"  + [{msg['source'].upper()}] {msg['source_name']}: {msg['text'][:50]}...")
            else:
                log(f"  - [SKIP] Already exists: {msg['message_id']}")
                
        except Exception as e:
            log(f"  - Failed: {msg['message_id']}: {e}", "ERROR")
    
    conn.commit()
    log(f"Inserted {inserted_count} new messages", "SUCCESS")


# =============================================================================
# STEP 4: RUN SENTIMENT ANALYSIS AND AGGREGATION
# =============================================================================

def simple_sentiment_analysis(text: str) -> tuple:
    """
    Simple keyword-based sentiment analysis.
    Returns (score, label, confidence)
    """
    text_lower = text.lower()
    
    # Bullish keywords
    bullish_words = [
        "bullish", "buy", "long", "moon", "pump", "breakout", "accumulation",
        "bullrun", "gains", "profit", "hodl", "adoption", "approved", "etf",
        "institutional", "golden cross", "support", "breaking out", "target",
        "life-changing", "future", "rising", "accelerating"
    ]
    
    # Bearish keywords  
    bearish_words = [
        "bearish", "sell", "short", "crash", "dump", "breakdown", "distribution",
        "loss", "fear", "panic", "correction", "pullback", "overbought",
        "careful", "warning", "bleeding", "sold", "overheated", "pressure"
    ]
    
    bullish_count = sum(1 for word in bullish_words if word in text_lower)
    bearish_count = sum(1 for word in bearish_words if word in text_lower)
    
    total = bullish_count + bearish_count
    if total == 0:
        return 0.0, "neutral", 0.5
    
    # Calculate sentiment score (-1 to 1)
    score = (bullish_count - bearish_count) / max(total, 1)
    score = max(-1.0, min(1.0, score))
    
    # Determine label
    if score > 0.2:
        label = "bullish"
    elif score < -0.2:
        label = "bearish"
    else:
        label = "neutral"
    
    # Confidence based on keyword density
    confidence = min(0.95, 0.5 + (total * 0.1))
    
    return score, label, confidence


def run_sentiment_analysis(conn):
    """Run sentiment analysis on ingested messages."""
    log("")
    log("=" * 60)
    log("STEP 4: SENTIMENT ANALYSIS AND AGGREGATION")
    log("=" * 60)
    
    cursor = conn.cursor()
    
    # Get unprocessed messages
    cursor.execute("""
        SELECT id, text, source, source_reliability, engagement_weight, author_weight
        FROM ingested_messages
        WHERE id NOT IN (SELECT message_id FROM sentiment_results WHERE message_id IS NOT NULL)
        ORDER BY event_time DESC
    """)
    
    messages = cursor.fetchall()
    log(f"Analyzing {len(messages)} messages...")
    
    results = []
    for msg_id, text, source, reliability, engagement, author in messages:
        score, label, confidence = simple_sentiment_analysis(text)
        
        # Calculate weighted score
        # weighted_score = score * reliability * (engagement + author) / 20
        weighted_score = score * reliability * (1 + (engagement + author) / 20)
        
        # Signal strength
        signal_strength = abs(weighted_score) * confidence
        
        results.append({
            "message_id": msg_id,
            "score": score,
            "label": label,
            "confidence": confidence,
            "weighted_score": weighted_score,
            "signal_strength": signal_strength
        })
        
        # Insert sentiment result
        cursor.execute("""
            INSERT INTO sentiment_results 
            (message_id, asset, sentiment_score, sentiment_label, confidence, weighted_score, signal_strength)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (msg_id, "BTC", score, label, confidence, weighted_score, signal_strength))
        
        icon = "+" if score > 0 else "-" if score < 0 else "o"
        log(f"  [{icon}] {label.upper():8} (score={score:.2f}, weight={weighted_score:.3f}): {text[:40]}...")
    
    conn.commit()
    
    # Calculate aggregated signal
    log("")
    log("Aggregating signals...")
    
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=30)
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN sentiment_label = 'bullish' THEN 1 ELSE 0 END) as bullish,
            SUM(CASE WHEN sentiment_label = 'bearish' THEN 1 ELSE 0 END) as bearish,
            SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral,
            AVG(sentiment_score) as avg_score,
            AVG(weighted_score) as avg_weighted,
            AVG(signal_strength) as avg_strength
        FROM sentiment_results sr
        JOIN ingested_messages im ON sr.message_id = im.id
        WHERE im.event_time >= %s
    """, (window_start,))
    
    agg = cursor.fetchone()
    total, bullish, bearish, neutral, avg_score, avg_weighted, avg_strength = agg
    
    if total > 0:
        # Determine signal type
        if avg_weighted > 0.1:
            signal_type = "BULLISH"
        elif avg_weighted < -0.1:
            signal_type = "BEARISH"
        else:
            signal_type = "NEUTRAL"
        
        # Calculate velocity (messages per minute)
        velocity = total / 30.0
        
        # Get sources breakdown
        cursor.execute("""
            SELECT im.source, COUNT(*) as count, AVG(sr.weighted_score) as avg_score
            FROM sentiment_results sr
            JOIN ingested_messages im ON sr.message_id = im.id
            WHERE im.event_time >= %s
            GROUP BY im.source
        """, (window_start,))
        
        sources_breakdown = {}
        for source, count, src_avg in cursor.fetchall():
            sources_breakdown[source] = {"count": count, "avg_score": float(src_avg or 0)}
        
        # Insert aggregated signal
        cursor.execute("""
            INSERT INTO aggregated_signals 
            (asset, window_start, window_end, total_messages, bullish_count, bearish_count,
             neutral_count, avg_sentiment, weighted_sentiment, signal_type, signal_strength,
             velocity, sources_breakdown)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            "BTC", window_start, now, total, bullish, bearish, neutral,
            avg_score, avg_weighted, signal_type, avg_strength, velocity,
            json.dumps(sources_breakdown)
        ))
        
        signal_id = cursor.fetchone()[0]
        conn.commit()
        
        log("")
        log("=" * 40)
        log(f"AGGREGATED SIGNAL: {signal_type}")
        log("=" * 40)
        log(f"  Window: Last 30 minutes")
        log(f"  Total Messages: {total}")
        log(f"  Bullish: {bullish} | Bearish: {bearish} | Neutral: {neutral}")
        log(f"  Average Sentiment: {avg_score:.3f}")
        log(f"  Weighted Sentiment: {avg_weighted:.3f}")
        log(f"  Signal Strength: {avg_strength:.3f}")
        log(f"  Velocity: {velocity:.2f} msg/min")
        log("")
        log("  Sources Breakdown:")
        for source, data in sources_breakdown.items():
            log(f"    - {source}: {data['count']} msgs, avg={data['avg_score']:.3f}")
        
        return {
            "signal_id": signal_id,
            "signal_type": signal_type,
            "weighted_sentiment": avg_weighted,
            "signal_strength": avg_strength,
            "total_messages": total,
            "bullish": bullish,
            "bearish": bearish,
            "neutral": neutral,
            "velocity": velocity,
            "sources": sources_breakdown
        }
    
    return None


# =============================================================================
# STEP 5: SEND TELEGRAM ALERT
# =============================================================================

async def send_telegram_alert(conn, signal_data):
    """Send alert to Telegram channel."""
    log("")
    log("=" * 60)
    log("STEP 5: SEND TELEGRAM ALERT")
    log("=" * 60)
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        log("Telegram credentials not configured. Skipping...", "WARN")
        return
    
    if not signal_data:
        log("No signal data to send.", "WARN")
        return
    
    try:
        import aiohttp
    except ImportError:
        log("aiohttp not installed. Installing...", "WARN")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "aiohttp"])
        import aiohttp
    
    # Format message
    signal_type = signal_data["signal_type"]
    
    if signal_type == "BULLISH":
        emoji = "🟢"
        icon = "📈"
    elif signal_type == "BEARISH":
        emoji = "🔴"
        icon = "📉"
    else:
        emoji = "⚪"
        icon = "➡️"
    
    message = f"""
{emoji} *BTC SENTIMENT SIGNAL* {emoji}

{icon} *Signal: {signal_type}*

📊 *Analysis Summary (Last 30 min)*
• Total Messages: {signal_data['total_messages']}
• 🟢 Bullish: {signal_data['bullish']}
• 🔴 Bearish: {signal_data['bearish']}
• ⚪ Neutral: {signal_data['neutral']}

📈 *Metrics*
• Weighted Sentiment: {signal_data['weighted_sentiment']:.4f}
• Signal Strength: {signal_data['signal_strength']:.4f}
• Velocity: {signal_data['velocity']:.2f} msg/min

📡 *Sources*
"""
    
    for source, data in signal_data["sources"].items():
        source_emoji = "📱" if source == "telegram" else "🐦" if source == "twitter" else "👽"
        message += f"• {source_emoji} {source.title()}: {data['count']} msgs (avg: {data['avg_score']:.3f})\n"
    
    message += f"""
⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
    
_Powered by Social Sentiment Pipeline_
"""
    
    log(f"Sending alert to Telegram channel {TELEGRAM_CHANNEL_ID}...")
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                result = await response.json()
                
                if result.get("ok"):
                    msg_id = result["result"]["message_id"]
                    log(f"Alert sent successfully! Message ID: {msg_id}", "SUCCESS")
                    
                    # Save to alert history
                    cursor = conn.cursor()
                    cursor.execute("""
                        INSERT INTO alert_history 
                        (asset, alert_type, signal_id, message, telegram_message_id, success)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        "BTC",
                        signal_type,
                        signal_data["signal_id"],
                        message,
                        str(msg_id),
                        True
                    ))
                    conn.commit()
                else:
                    error = result.get("description", "Unknown error")
                    log(f"Failed to send alert: {error}", "ERROR")
                    
    except Exception as e:
        log(f"Error sending Telegram alert: {e}", "ERROR")


# =============================================================================
# MAIN
# =============================================================================

async def main():
    """Run the complete pipeline."""
    log("")
    log("*" * 60)
    log("  SOCIAL SENTIMENT PIPELINE - FULL RUN")
    log("*" * 60)
    log("")
    
    start_time = time.time()
    
    try:
        # Step 1: Database connection and tables
        conn = check_database_connection()
        
        # Step 2: Insert sample sources
        insert_sample_sources(conn)
        
        # Step 3: Process sample messages
        process_sample_messages(conn)
        
        # Step 4: Run sentiment analysis
        signal_data = run_sentiment_analysis(conn)
        
        # Step 5: Send Telegram alert
        await send_telegram_alert(conn, signal_data)
        
        # Close connection
        conn.close()
        
        elapsed = time.time() - start_time
        log("")
        log("*" * 60)
        log(f"  PIPELINE COMPLETED SUCCESSFULLY")
        log(f"  Total time: {elapsed:.2f} seconds")
        log("*" * 60)
        
    except Exception as e:
        log(f"Pipeline failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
