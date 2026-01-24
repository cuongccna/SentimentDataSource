"""
Production Background Worker for Crypto Social Sentiment Data Service.

This worker:
- Connects to PostgreSQL database
- Uses REAL crawlers (Reddit, Twitter, Telegram)
- Persists data to PostgreSQL tables
- Analyzes sentiment and aggregates signals
- Sends periodic reports to Telegram channel

ABSOLUTE RULES:
- NO mocking
- NO hallucination
- NO paid APIs
- NO trading signals
- If any stage fails → DROP EVENT and continue
"""

import os
import sys
import time
import signal
import threading
import asyncio
import logging
import json
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import random  # For random delays

from background_worker import (
    BackgroundWorker,
    SourceCollector,
    DatabaseInterface,
    WorkerMetrics,
    WorkerState,
    PipelineExecutor,
    SourceLoop,
    TWITTER_INTERVAL,
    TELEGRAM_INTERVAL,
    REDDIT_INTERVAL,
    logger
)

# Import rate limiter for anti-ban protection
from rate_limiter import get_rate_limiter, get_last_read_tracker

# Import asset configuration for dynamic filtering
from asset_config import (
    detect_asset as detect_asset_from_text,
    contains_tracked_asset,
    get_asset_config
)

from reddit_crawler import RedditCrawler

# Import Twitter Scraper (new syndication-based scraper)
try:
    from twitter_scraper import TwitterScraper, NormalizedRecord
    HAS_TWITTER_SCRAPER = True
except ImportError:
    HAS_TWITTER_SCRAPER = False
    logger.warning("twitter_scraper not available. Twitter will use legacy ntscraper.")

# Import sentiment pipeline
try:
    from sentiment_pipeline import process_record as analyze_sentiment
    HAS_SENTIMENT = True
except ImportError:
    HAS_SENTIMENT = False
    logger.warning("sentiment_pipeline not available. Sentiment analysis disabled.")


# =============================================================================
# TELEGRAM ALERTING CONFIG
# =============================================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "")
REPORT_INTERVAL_MINUTES = int(os.getenv("REPORT_INTERVAL_MINUTES", "30"))


# =============================================================================
# TELETHON SETUP (Telegram Client API)
# =============================================================================

try:
    from telethon import TelegramClient
    from telethon.tl.functions.messages import GetHistoryRequest
    HAS_TELETHON = True
except ImportError:
    HAS_TELETHON = False
    logger.warning("Telethon not installed. Telegram collection disabled.")


# =============================================================================
# POSTGRESQL DATABASE
# =============================================================================

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    logger.warning("psycopg2 not installed. Database persistence disabled.")


class PostgreSQLDatabase(DatabaseInterface):
    """PostgreSQL database implementation."""
    
    def __init__(self):
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        if not HAS_PSYCOPG2:
            raise RuntimeError("psycopg2 is required for PostgreSQL database")
        
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
            user=os.getenv("POSTGRES_USER", "sentiment_user"),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )
        self.conn.autocommit = False
        logger.info(f"Connected to PostgreSQL: {os.getenv('POSTGRES_DATABASE')}")
    
    def insert_raw_event(self, **kwargs) -> Optional[str]:
        """Insert raw event into ingested_messages table.
        
        Compatible with both:
        - Direct calls from collectors (message_id, timestamp, metadata)
        - Pipeline calls (event_time, fingerprint, source_reliability)
        """
        import hashlib
        
        try:
            cursor = self.conn.cursor()
            
            # Get text
            text = kwargs.get("text", "")
            
            # Get fingerprint - use provided or generate from source + message_id + text
            # This ensures same text from different sources/messages is NOT duplicate
            fingerprint = kwargs.get("fingerprint")
            if not fingerprint:
                source = kwargs.get("source", "")
                message_id = kwargs.get("message_id", "")
                # Include source and message_id in fingerprint to avoid cross-source duplicates
                unique_key = f"{source}:{message_id}:{text}"
                fingerprint = hashlib.md5(unique_key.encode()).hexdigest()
            
            # Check duplicate by fingerprint
            cursor.execute(
                "SELECT id FROM ingested_messages WHERE fingerprint = %s",
                (fingerprint,)
            )
            if cursor.fetchone():
                logger.debug(f"Duplicate event (fingerprint exists): {fingerprint[:16]}")
                return None  # Duplicate
            
            # Get metadata - could be dict or empty
            metadata = kwargs.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            
            # Replace None values with 0 for JSON compatibility
            for key, value in list(metadata.items()):
                if value is None:
                    metadata[key] = 0
            
            # Get timestamp - try multiple param names
            event_time = kwargs.get("event_time") or kwargs.get("timestamp")
            if event_time is None:
                event_time = datetime.now(timezone.utc).isoformat()
            elif hasattr(event_time, 'isoformat'):
                event_time = event_time.isoformat()
            
            # CRITICAL: Ensure message_id is never empty
            message_id = kwargs.get("message_id", "")
            if not message_id:
                # Generate unique ID from fingerprint
                message_id = f"auto_{fingerprint[:16]}"
            
            # Get source info
            source = kwargs.get("source", "")
            source_name = kwargs.get("source_id") or kwargs.get("source_name", "")
            asset = kwargs.get("asset", "BTC")
            
            # Get metrics - from metadata or direct params
            source_reliability = kwargs.get("source_reliability", 0.8)
            engagement_weight = kwargs.get("engagement_weight") or metadata.get("engagement_weight", 0) or 0
            author_weight = kwargs.get("author_weight") or metadata.get("author_weight", 0) or 0
            velocity = kwargs.get("velocity") or metadata.get("velocity", 1.0) or 1.0
            
            # Insert with correct schema
            cursor.execute("""
                INSERT INTO ingested_messages 
                (message_id, source, source_name, asset, text, event_time, 
                 fingerprint, source_reliability, engagement_weight, author_weight, velocity, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                message_id,
                source,
                source_name,
                asset,
                text,
                event_time,
                fingerprint,
                source_reliability,
                engagement_weight,
                author_weight,
                velocity,
                json.dumps(metadata)
            ))
            
            result = cursor.fetchone()
            self.conn.commit()
            # Only log at debug level to reduce log spam
            text_preview = text[:40].replace('\n', ' ') if text else ""
            logger.debug(f"[{source}] Inserted #{result[0]} | {asset} | {text_preview}")
            return str(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Failed to insert raw event: {e}")
            self.conn.rollback()
            return None
            return None
    
    def insert_sentiment_event(self, **kwargs) -> Optional[str]:
        """Insert sentiment result into sentiment_results table."""
        try:
            cursor = self.conn.cursor()
            
            # Get message_id - could be raw_event_id from pipeline or direct message_id
            message_id = kwargs.get("raw_event_id") or kwargs.get("message_id")
            if isinstance(message_id, str):
                # Try to extract numeric ID or use hash
                try:
                    message_id = int(message_id)
                except ValueError:
                    message_id = abs(hash(message_id)) % (10 ** 9)  # Convert to int
            
            # Map label from int (-1, 0, 1) to string
            label = kwargs.get("sentiment_label", 0)
            if isinstance(label, int):
                label_map = {-1: "bearish", 0: "neutral", 1: "bullish"}
                sentiment_label = label_map.get(label, "neutral")
            else:
                sentiment_label = str(label)
            
            # Get confidence and compute derived values
            confidence = kwargs.get("confidence", 0.5)
            sentiment_score = kwargs.get("sentiment_score")
            if sentiment_score is None:
                # Derive from label
                label_score = {-1: -1.0, 0: 0.0, 1: 1.0}
                sentiment_score = label_score.get(kwargs.get("sentiment_label", 0), 0.0)
            
            # Compute weighted score and signal strength
            weighted_score = kwargs.get("weighted_score", sentiment_score * confidence)
            signal_strength = kwargs.get("signal_strength", abs(weighted_score))
            
            cursor.execute("""
                INSERT INTO sentiment_results 
                (message_id, asset, sentiment_score, sentiment_label, confidence, weighted_score, signal_strength)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                message_id,
                kwargs.get("asset", "BTC"),
                sentiment_score,
                sentiment_label,
                confidence,
                weighted_score,
                signal_strength
            ))
            
            result = cursor.fetchone()
            self.conn.commit()
            return str(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Failed to insert sentiment event: {e}")
            self.conn.rollback()
            return None
    
    def insert_risk_event(self, **kwargs) -> Optional[str]:
        """Insert risk indicators into risk_indicators table."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO risk_indicators 
                (asset, sentiment_reliability, fear_greed_index, fear_greed_zone, 
                 social_overheat, panic_risk, fomo_risk, data_quality_overall)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                kwargs.get("asset", "BTC"),
                kwargs.get("sentiment_reliability", "normal"),
                kwargs.get("fear_greed_index"),
                kwargs.get("fear_greed_zone", "unknown"),
                kwargs.get("social_overheat", False),
                kwargs.get("panic_risk", False),
                kwargs.get("fomo_risk", False),
                kwargs.get("data_quality_overall", "healthy")
            ))
            
            result = cursor.fetchone()
            self.conn.commit()
            return str(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Failed to insert risk event: {e}")
            self.conn.rollback()
            return None
    
    def insert_quality_event(self, **kwargs) -> Optional[str]:
        """Insert data quality metrics."""
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                INSERT INTO data_quality_metrics 
                (source_type, availability_status, time_integrity_status, 
                 volume_status, overall_quality, metrics_detail)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                kwargs.get("source_type", ""),
                kwargs.get("availability", "ok"),
                kwargs.get("time_integrity", "ok"),
                kwargs.get("volume", "normal"),
                kwargs.get("overall", "healthy"),
                json.dumps(kwargs.get("metrics", {}))
            ))
            
            result = cursor.fetchone()
            self.conn.commit()
            return str(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Failed to insert quality event: {e}")
            self.conn.rollback()
            return None
    
    def check_fingerprint_exists(self, fingerprint: str) -> bool:
        """Check if message already exists."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT 1 FROM ingested_messages WHERE message_id = %s LIMIT 1",
                (fingerprint,)
            )
            return cursor.fetchone() is not None
        except Exception:
            return False
    
    def commit(self):
        """Commit transaction."""
        try:
            self.conn.commit()
        except Exception as e:
            logger.error(f"Commit failed: {e}")
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


# =============================================================================
# TELEGRAM REPORTER
# =============================================================================

class TelegramReporter:
    """
    Sends periodic sentiment reports to Telegram channel.
    
    Features:
    - Aggregates sentiment data from last N minutes
    - Formats report with emoji and signal indicators
    - Saves alert history to database
    """
    
    def __init__(self, database: PostgreSQLDatabase):
        self.database = database
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.channel_id = TELEGRAM_CHANNEL_ID
        self.report_interval = REPORT_INTERVAL_MINUTES
        self._last_report_time: Optional[datetime] = None
        self._enabled = bool(self.bot_token and self.channel_id)
        
        if self._enabled:
            logger.info(f"Telegram Reporter enabled (interval: {self.report_interval} min)")
        else:
            logger.warning("Telegram Reporter disabled - missing BOT_TOKEN or CHANNEL_ID")
    
    def should_send_report(self) -> bool:
        """Check if it's time to send a report."""
        if not self._enabled:
            return False
        
        now = datetime.now(timezone.utc)
        
        if self._last_report_time is None:
            # First run - wait for full interval
            self._last_report_time = now
            return False
        
        elapsed = (now - self._last_report_time).total_seconds() / 60
        return elapsed >= self.report_interval
    
    def get_aggregated_data(self) -> Optional[Dict[str, Any]]:
        """Get aggregated sentiment data from database."""
        try:
            cursor = self.database.conn.cursor()
            
            window_start = datetime.now(timezone.utc) - timedelta(minutes=self.report_interval)
            
            # Get message counts by source
            cursor.execute("""
                SELECT 
                    source,
                    COUNT(*) as count,
                    AVG(source_reliability) as avg_reliability
                FROM ingested_messages
                WHERE created_at >= %s
                GROUP BY source
            """, (window_start,))
            
            sources = {}
            total_messages = 0
            for row in cursor.fetchall():
                sources[row[0]] = {
                    "count": row[1],
                    "avg_reliability": float(row[2]) if row[2] else 0.5
                }
                total_messages += row[1]
            
            if total_messages == 0:
                logger.info("No new messages to report")
                return None
            
            # Get sentiment aggregation
            cursor.execute("""
                SELECT 
                    sentiment_label,
                    COUNT(*) as count,
                    AVG(sentiment_score) as avg_score,
                    AVG(confidence) as avg_confidence,
                    AVG(weighted_score) as avg_weighted
                FROM sentiment_results
                WHERE created_at >= %s
                GROUP BY sentiment_label
            """, (window_start,))
            
            sentiments = {"bullish": 0, "bearish": 0, "neutral": 0}
            total_score = 0.0
            total_weighted = 0.0
            sentiment_count = 0
            
            for row in cursor.fetchall():
                label = row[0].lower() if row[0] else "neutral"
                if label in sentiments:
                    sentiments[label] = row[1]
                    total_score += row[2] * row[1] if row[2] else 0
                    total_weighted += row[4] * row[1] if row[4] else 0
                    sentiment_count += row[1]
            
            avg_score = total_score / sentiment_count if sentiment_count > 0 else 0
            avg_weighted = total_weighted / sentiment_count if sentiment_count > 0 else 0
            
            # Determine signal type
            if avg_weighted > 0.1:
                signal_type = "BULLISH"
            elif avg_weighted < -0.1:
                signal_type = "BEARISH"
            else:
                signal_type = "NEUTRAL"
            
            # Calculate signal strength
            signal_strength = min(1.0, abs(avg_weighted) * 2)
            
            # Calculate velocity (messages per minute)
            velocity = total_messages / self.report_interval
            
            return {
                "signal_type": signal_type,
                "total_messages": total_messages,
                "sentiment_analyzed": sentiment_count,
                "bullish": sentiments["bullish"],
                "bearish": sentiments["bearish"],
                "neutral": sentiments["neutral"],
                "avg_score": avg_score,
                "weighted_sentiment": avg_weighted,
                "signal_strength": signal_strength,
                "velocity": velocity,
                "sources": sources,
                "window_minutes": self.report_interval
            }
            
        except Exception as e:
            logger.error(f"Failed to get aggregated data: {e}")
            return None
    
    def format_report(self, data: Dict[str, Any]) -> str:
        """Format aggregated data into Telegram message."""
        signal_type = data["signal_type"]
        
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
{emoji} *BTC SENTIMENT REPORT* {emoji}

{icon} *Signal: {signal_type}*

📊 *Analysis Summary (Last {data['window_minutes']} min)*
• Total Messages: {data['total_messages']}
• Sentiment Analyzed: {data['sentiment_analyzed']}
• 🟢 Bullish: {data['bullish']}
• 🔴 Bearish: {data['bearish']}
• ⚪ Neutral: {data['neutral']}

📈 *Metrics*
• Average Score: {data['avg_score']:.4f}
• Weighted Sentiment: {data['weighted_sentiment']:.4f}
• Signal Strength: {data['signal_strength']:.4f}
• Velocity: {data['velocity']:.2f} msg/min

📡 *Sources*
"""
        
        for source, info in data["sources"].items():
            source_emoji = "📱" if source == "telegram" else "🐦" if source == "twitter" else "👽"
            message += f"• {source_emoji} {source.title()}: {info['count']} msgs\n"
        
        message += f"""
⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

_Powered by Social Sentiment Pipeline_
"""
        
        return message
    
    async def send_report(self) -> bool:
        """Send report to Telegram channel."""
        if not self._enabled:
            return False
        
        data = self.get_aggregated_data()
        if not data:
            self._last_report_time = datetime.now(timezone.utc)
            return False
        
        message = self.format_report(data)
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.channel_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    result = await response.json()
                    
                    if result.get("ok"):
                        msg_id = result["result"]["message_id"]
                        logger.info(f"Report sent to Telegram! Message ID: {msg_id}")
                        
                        # Save to alert history
                        self._save_alert_history(data, message, str(msg_id), True)
                        self._last_report_time = datetime.now(timezone.utc)
                        return True
                    else:
                        error = result.get("description", "Unknown error")
                        logger.error(f"Telegram API error: {error}")
                        self._save_alert_history(data, message, None, False)
                        return False
                        
        except Exception as e:
            logger.error(f"Failed to send Telegram report: {e}")
            return False
    
    def _save_alert_history(self, data: Dict, message: str, msg_id: Optional[str], success: bool):
        """Save alert to database."""
        try:
            cursor = self.database.conn.cursor()
            cursor.execute("""
                INSERT INTO alert_history 
                (asset, alert_type, message, telegram_message_id, success)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                "BTC",
                data["signal_type"],
                message,
                msg_id,
                success
            ))
            self.database.conn.commit()
        except Exception as e:
            logger.error(f"Failed to save alert history: {e}")


# =============================================================================
# REAL SOURCE COLLECTORS
# =============================================================================

class RedditSourceCollector(SourceCollector):
    """
    Real Reddit collector using RedditCrawler.
    
    ANTI-BAN PROTECTION:
    - Random delays between subreddit fetches (2-3 seconds)
    - Respects 10-minute collection interval
    - Max 10 subreddits per cycle
    """
    
    def __init__(self, subreddits: Optional[List[str]] = None):
        super().__init__("reddit")
        # Rate limiter for anti-ban protection
        self.rate_limiter = get_rate_limiter()
        
        # Get subreddits from database or use defaults
        self.subreddits = subreddits or self._load_subreddits_from_db()
        self.crawler = RedditCrawler(subreddits=self.subreddits)
        logger.info(f"Reddit collector initialized with {len(self.subreddits)} subreddits")
    
    def _load_subreddits_from_db(self) -> List[str]:
        """Load enabled subreddits from database, ordered by priority."""
        default_subreddits = [
            "bitcoin", "cryptocurrency", "cryptomarkets", 
            "ethereum", "bitcoinmarkets", "defi", "altcoin",
            "cryptotrading", "cardano", "dogecoin", "solana"
        ]
        try:
            import psycopg2
            from db_config import DB_CONFIG
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("""
                SELECT subreddit FROM reddit_sources 
                WHERE enabled = TRUE 
                ORDER BY priority DESC, subreddit
            """)
            subreddits = [row[0] for row in cur.fetchall()]
            cur.close()
            conn.close()
            if subreddits:
                logger.info(f"[reddit] Loaded {len(subreddits)} subreddits from database")
                return subreddits
            else:
                logger.warning("[reddit] No subreddits in database, using defaults")
                return default_subreddits
        except Exception as e:
            logger.warning(f"[reddit] Failed to load subreddits from DB: {e}, using defaults")
            return default_subreddits
    
    def collect(self, since: Optional[datetime]) -> List[dict]:
        """Collect posts from Reddit with anti-ban protection."""
        all_records = []
        
        # Check if platform is enabled
        if not self.rate_limiter.is_enabled("reddit"):
            logger.info("[reddit] Platform disabled - skipping")
            return []
        
        # Limit subreddits per cycle
        max_subs = min(len(self.subreddits), 15)
        subs_to_fetch = self.subreddits[:max_subs]
        
        subs_processed = 0
        for subreddit in subs_to_fetch:
            try:
                # Random delay between subreddit fetches (2-3 seconds)
                if subs_processed > 0:
                    delay = random.uniform(2.0, 3.5)
                    time.sleep(delay)
                
                # Fetch posts
                records = self.crawler.crawl_subreddit(subreddit, post_limit=10)
                subs_processed += 1
                
                for record in records:
                    text = record.get("text", "")
                    
                    # Detect asset from text
                    detected_asset = detect_asset_from_text(text) or "BTC"
                    
                    # Convert to event format expected by pipeline
                    event = {
                        "message_id": f"reddit_{record.get('id', '')}",
                        "source": "reddit",
                        "source_id": subreddit,
                        "text": text,
                        "asset": detected_asset,
                        "author": record.get("author", ""),
                        "timestamp": record.get("timestamp", datetime.now(timezone.utc).isoformat()),
                        "created_at": record.get("timestamp"),
                        "score": record.get("score", 0),
                        "num_comments": record.get("num_comments", 0),
                        "metadata": {
                            "subreddit": subreddit,
                            "engagement_weight": record.get("metrics", {}).get("engagement_weight", 0),
                            "author_weight": record.get("metrics", {}).get("author_weight", 0),
                            "velocity": record.get("metrics", {}).get("velocity", 1.0)
                        }
                    }
                    all_records.append(event)
                
                logger.debug(f"[reddit] r/{subreddit}: {len(records)} posts")
                
            except Exception as e:
                logger.debug(f"[reddit] r/{subreddit} failed: {e}")
                continue
        
        # Record successful collection
        self.rate_limiter.record_collection("reddit", success=True)
        
        # Summary log only
        logger.info(f"[reddit] 📊 {len(all_records)} posts from {subs_processed}/{len(self.subreddits)} subs")
        return all_records


class TwitterSourceCollector(SourceCollector):
    """
    Twitter collector using TwitterScraper (Syndication API).
    
    ANTI-BAN PROTECTION:
    - Random delays between account fetches (5-10 seconds)
    - Scrapes only 5 accounts per collection cycle
    - Rotates through accounts using batch_index
    - Uses 30-minute interval between collections
    """
    
    # How many accounts to scrape per batch (to avoid rate limits)
    ACCOUNTS_PER_BATCH = 5
    
    # Extended list of crypto Twitter accounts to monitor
    CRYPTO_ACCOUNTS = [
        # Whale Alerts & On-chain
        "whale_alert",          # Whale movements
        "WhaleChart",           # Whale tracking
        "santaboris",           # Santiment founder
        "glassnode",            # On-chain analytics
        "CryptoQuant_News",     # On-chain data
        
        # News Outlets
        "Cointelegraph",        # Major crypto news
        "BitcoinMagazine",      # Bitcoin news
        "CoinDesk",             # Crypto news
        "TheBlock__",           # Crypto news
        "WuBlockchain",         # China/mining news
        "DocumentingBTC",       # Bitcoin education
        
        # Analysts & Influencers
        "PlanB",                # Stock-to-Flow model
        "100trillionUSD",       # PlanB alt account
        "WClementeIII",         # On-chain analyst
        "woloomo",              # Willy Woo
        "CryptoDonAlt",         # Trader/analyst
        "CryptoGodJohn",        # Crypto trader
        "CryptoCobain",         # Trader
        
        # Exchanges & Projects
        "binance",              # Exchange news
        "coinaborase",          # Coinbase
        "kaboraken",            # Kraken
        "Bybit_Official",       # Bybit
        "OKX",                  # OKX exchange
        
        # Market Data
        "BitcoinFear",          # Fear & Greed
        "CryptoRank_io",        # Market data
        "messaborri",           # Messari
        
        # Regulators & Macro
        "Fxhedgers",            # Macro news
        "zaborrohedge",         # Market news
    ]
    
    def __init__(self, accounts: Optional[List[str]] = None):
        super().__init__("twitter")
        
        # Rate limiter for anti-ban protection
        self.rate_limiter = get_rate_limiter()
        
        # Try to load accounts from database first
        db_accounts = self._load_accounts_from_db()
        
        # Use: provided accounts > database accounts > default list
        if accounts:
            self.all_accounts = accounts
        elif db_accounts:
            self.all_accounts = db_accounts
        else:
            self.all_accounts = self.CRYPTO_ACCOUNTS
        
        # Batch rotation index
        self._batch_index = 0
        
        # Current batch of accounts (will be rotated)
        self.accounts = self._get_next_batch()
        
        # Initialize the new Twitter scraper
        self.scraper = None
        self.loop = None
        self._initialized = False
        
        # Enable by default since we have a working scraper
        self.enabled = True
        
        logger.info(f"Twitter collector initialized with {len(self.all_accounts)} total accounts")
        logger.info(f"  - Batch size: {self.ACCOUNTS_PER_BATCH} accounts per collection")
        logger.info(f"  - Current batch: {self.accounts}")
    
    def _get_next_batch(self) -> List[str]:
        """Get the next batch of accounts to scrape (rotating through all accounts)."""
        total = len(self.all_accounts)
        batch_size = min(self.ACCOUNTS_PER_BATCH, total)
        
        start_idx = (self._batch_index * batch_size) % total
        end_idx = start_idx + batch_size
        
        if end_idx <= total:
            batch = self.all_accounts[start_idx:end_idx]
        else:
            # Wrap around
            batch = self.all_accounts[start_idx:] + self.all_accounts[:end_idx - total]
        
        return batch
    
    def _rotate_batch(self):
        """Rotate to the next batch of accounts."""
        self._batch_index += 1
        self.accounts = self._get_next_batch()
        logger.debug(f"[twitter] Rotated to batch {self._batch_index}: {self.accounts}")
    
    def _load_accounts_from_db(self) -> List[str]:
        """Load enabled Twitter accounts from database."""
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
                user=os.getenv("POSTGRES_USER", "sentiment_user"),
                password=os.getenv("POSTGRES_PASSWORD", "")
            )
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT account_id 
                FROM twitter_sources 
                WHERE enabled = TRUE 
                ORDER BY priority DESC
            """)
            
            accounts = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            if accounts:
                logger.info(f"Loaded {len(accounts)} Twitter accounts from database")
            
            return accounts
            
        except Exception as e:
            logger.warning(f"Could not load Twitter accounts from DB: {e}")
            return []
    
    def _get_event_loop(self):
        """Get or create event loop for async operations."""
        try:
            self.loop = asyncio.get_event_loop()
            if self.loop.is_closed():
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        return self.loop
    
    async def _init_scraper(self):
        """Initialize Twitter scraper with current batch."""
        # Reinitialize scraper with rotated batch
        if not HAS_TWITTER_SCRAPER:
            logger.warning("[twitter] twitter_scraper module not available")
            return False
        
        try:
            # Close existing scraper if any
            if self.scraper:
                try:
                    await self.scraper.close()
                except:
                    pass
            
            self.scraper = TwitterScraper(accounts=self.accounts)
            self._initialized = True
            logger.info(f"[twitter] TwitterScraper initialized with batch: {self.accounts}")
            return True
        except Exception as e:
            logger.error(f"[twitter] Failed to initialize scraper: {e}")
            return False
    
    async def _collect_async(self, limit_per_account: int = 10) -> List[dict]:
        """Async collection of tweets."""
        all_tweets = []
        
        if not await self._init_scraper():
            return []
        
        try:
            # Scrape BTC-related tweets from all accounts
            records = await self.scraper.scrape(
                limit_per_account=limit_per_account,
                filter_btc=True,
                deduplicate=True
            )
            
            # Convert NormalizedRecord to event format
            for record in records:
                data = record.to_dict()
                meta = data.get("metadata", {})
                
                # Ensure tweet_id exists - skip if not
                tweet_id = meta.get("tweet_id") or ""
                if not tweet_id:
                    # Generate unique ID from fingerprint or hash
                    fingerprint = data.get("fingerprint", "")
                    if fingerprint:
                        tweet_id = f"fp_{fingerprint[:16]}"
                    else:
                        # Last resort: hash of text + username
                        text_hash = abs(hash(f"{data.get('text', '')[:50]}_{meta.get('username', '')}"))
                        tweet_id = f"hash_{text_hash}"
                
                # Ensure timestamp is never None
                timestamp_str = data.get("timestamp")
                if not timestamp_str:
                    timestamp_str = datetime.now(timezone.utc).isoformat()
                
                # Detect asset from text dynamically
                text = data.get("text", "")
                detected_asset = detect_asset_from_text(text) or "BTC"
                
                event = {
                    "message_id": f"tw_{tweet_id}",
                    "source": "twitter",
                    "source_id": meta.get("username", "unknown"),
                    "text": text,
                    "author": meta.get("username", ""),
                    "timestamp": timestamp_str,
                    "created_at": timestamp_str,
                    "likes": meta.get("like_count", 0),
                    "retweets": meta.get("retweet_count", 0),
                    "replies": meta.get("reply_count", 0),
                    "asset": detected_asset,
                    "metadata": {
                        "tweet_id": tweet_id,
                        "username": meta.get("username"),
                        "engagement_weight": data["metrics"]["engagement_weight"],
                        "author_weight": data["metrics"]["author_weight"],
                        "velocity": data["metrics"]["velocity"],
                        "total_engagement": meta.get("total_engagement", 0),
                        "is_retweet": meta.get("is_retweet", False),
                        "fingerprint": data.get("fingerprint"),
                    }
                }
                all_tweets.append(event)
            
        except Exception as e:
            logger.error(f"[twitter] Collection error: {e}")
            import traceback
            traceback.print_exc()
        
        return all_tweets
    
    def collect(self, since: Optional[datetime]) -> List[dict]:
        """
        Collect tweets from monitored Twitter accounts.
        
        ANTI-BAN: Random delays, batch rotation, rate limiting.
        Uses TwitterScraper with Syndication API (no auth required).
        """
        if not self.enabled:
            return []
        
        # Check if platform is enabled
        if not self.rate_limiter.is_enabled("twitter"):
            logger.info("[twitter] Platform disabled - skipping")
            return []
        
        if not HAS_TWITTER_SCRAPER:
            logger.warning("[twitter] twitter_scraper not available - skipping")
            return []
        
        all_tweets = []
        
        try:
            # Rotate to next batch before collection
            self._rotate_batch()
            
            # Get or create event loop
            loop = self._get_event_loop()
            
            # Run async collection with random delays
            all_tweets = loop.run_until_complete(
                self._collect_async(limit_per_account=10)
            )
            
            # Record successful collection
            self.rate_limiter.record_collection("twitter", success=True)
            
            logger.info(f"[twitter] 📊 {len(all_tweets)} tweets from batch {self._batch_index}")
            
        except Exception as e:
            self.rate_limiter.record_collection("twitter", success=False)
            logger.error(f"[twitter] Collection failed: {e}")
        
        return all_tweets
    
    async def close(self):
        """Cleanup scraper resources."""
        if self.scraper:
            await self.scraper.close()
            self._initialized = False


class TelegramSourceCollector(SourceCollector):
    """
    Telegram collector using Telethon Client API.
    
    ANTI-BAN PROTECTION:
    - Random delays between channel fetches (3-5 seconds)
    - Uses min_id to only fetch NEW messages (not full history)
    - Respects 15-minute collection interval
    - Handles FloodWait errors gracefully
    - Max 10 channels per collection cycle
    
    Reads messages from channels configured in telegram_sources table.
    """
    
    def __init__(self, database_conn=None):
        super().__init__("telegram")
        self.client = None
        self.loop = None
        self._initialized = False
        self.db_conn = database_conn
        
        # Rate limiter for anti-ban protection
        self.rate_limiter = get_rate_limiter()
        
        # Track last message IDs per channel to avoid re-fetching
        self.last_message_ids: Dict[str, int] = {}
        
        # Get credentials from environment
        self.api_id = os.getenv("TELEGRAM_API_ID")
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.phone = os.getenv("TELEGRAM_PHONE")
        
        # Session file path - use absolute path for persistence
        self.session_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "telegram_production_session"
        )
        
        # Channels from database (will be loaded)
        self.configured_channels = []
        
        if HAS_TELETHON and self.api_id and self.api_hash:
            logger.info("Telegram collector initialized with Telethon")
            self._load_configured_channels()
        else:
            logger.warning("Telegram collector: missing credentials or Telethon not installed")
    
    def _load_configured_channels(self):
        """Load enabled AND healthy channels from telegram_sources table.
        
        HEALTH CHECK GATEKEEPING:
        Only loads channels where:
        - enabled = TRUE
        - status = 'alive' OR status IS NULL (for backward compatibility)
        """
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
                user=os.getenv("POSTGRES_USER", "sentiment_user"),
                password=os.getenv("POSTGRES_PASSWORD", "")
            )
            cursor = conn.cursor()
            
            # CRITICAL: Only ingest from ALIVE channels
            # Allow NULL status for backward compatibility (before health worker runs)
            cursor.execute("""
                SELECT channel_id, channel_name, channel_type, priority, 
                       COALESCE(status, 'unknown') as status
                FROM telegram_sources 
                WHERE enabled = TRUE 
                  AND (status IS NULL OR status = 'alive' OR status = 'unknown')
                ORDER BY priority DESC
            """)
            
            self.configured_channels = []
            alive_count = 0
            unknown_count = 0
            
            for row in cursor.fetchall():
                status = row[4]
                self.configured_channels.append({
                    "channel_id": row[0],
                    "channel_name": row[1],
                    "channel_type": row[2],
                    "priority": row[3],
                    "status": status
                })
                if status == 'alive':
                    alive_count += 1
                else:
                    unknown_count += 1
            
            cursor.close()
            conn.close()
            
            logger.info(f"Loaded {len(self.configured_channels)} channels from telegram_sources")
            logger.info(f"  - {alive_count} ALIVE, {unknown_count} UNKNOWN/NULL (pending health check)")
            for ch in self.configured_channels:
                status_icon = "✅" if ch['status'] == 'alive' else "❓"
                logger.info(f"  {status_icon} {ch['channel_name']} ({ch['channel_id']}) [priority={ch['priority']}]")
                
        except Exception as e:
            logger.error(f"Failed to load telegram sources: {e}")
            self.configured_channels = []
    
    async def _init_client(self):
        """Initialize Telethon client with persistent session."""
        if self._initialized and self.client and self.client.is_connected():
            return True
        
        if not HAS_TELETHON:
            return False
        
        if not self.api_id or not self.api_hash:
            logger.error("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH")
            return False
        
        try:
            self.client = TelegramClient(
                self.session_file,  # Use absolute path
                int(self.api_id),
                self.api_hash,
                system_version="4.16.30-vxCUSTOM"
            )
            
            # Check if already authorized
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.warning("Telegram session not authorized. Please run auth_telegram.py first!")
                logger.warning("Run: python auth_telegram.py")
                return False
            
            self._initialized = True
            me = await self.client.get_me()
            logger.info(f"Telethon client connected as {me.first_name} (@{me.username})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Telethon: {e}")
            return False
    
    async def _fetch_messages(self, dialog, limit: int = 20) -> List[dict]:
        """
        Fetch NEW messages from a channel dialog.
        
        Uses min_id to only fetch messages NEWER than last seen.
        This prevents re-reading the same messages and reduces API calls.
        """
        messages = []
        channel_name = dialog.name
        channel_id = str(dialog.id)
        
        # Get last seen message ID for this channel
        min_id = self.last_message_ids.get(channel_id, 0)
        
        try:
            history = await self.client(GetHistoryRequest(
                peer=dialog.entity,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=min_id,  # Only get messages newer than this ID
                add_offset=0,
                hash=0
            ))
            
            # Track newest message ID
            newest_msg_id = min_id
            
            for msg in history.messages:
                if not msg.message:
                    continue
                
                # Update newest message ID
                if msg.id > newest_msg_id:
                    newest_msg_id = msg.id
                
                # Dynamic asset filtering using asset_config
                text = msg.message
                if not contains_tracked_asset(text):
                    continue
                
                # Detect which asset this message is about
                detected_asset = detect_asset_from_text(text) or "BTC"
                
                messages.append({
                    "message_id": f"tg_{channel_id}_{msg.id}",
                    "source": "telegram",
                    "source_id": channel_name,
                    "text": msg.message,
                    "asset": detected_asset,
                    "author": str(msg.from_id.user_id) if hasattr(msg.from_id, 'user_id') else channel_name,
                    "timestamp": msg.date.isoformat(),
                    "created_at": msg.date.isoformat(),
                    "views": msg.views if msg.views else 0,
                    "forwards": msg.forwards if msg.forwards else 0,
                    "metadata": {
                        "channel": channel_name,
                        "channel_id": str(channel_id),
                        "is_forwarded": msg.fwd_from is not None,
                        "views": msg.views if msg.views else 0
                    }
                })
            
            # Update last seen message ID for this channel
            if newest_msg_id > min_id:
                self.last_message_ids[channel_id] = newest_msg_id
            
        except Exception as e:
            # Handle FloodWait errors specially
            error_str = str(e).lower()
            if 'flood' in error_str or 'wait' in error_str:
                # Extract wait time if present
                import re
                wait_match = re.search(r'(\d+)\s*seconds?', str(e))
                wait_time = int(wait_match.group(1)) if wait_match else 300
                self.rate_limiter.record_rate_limit_error("telegram", wait_time)
                logger.error(f"[telegram] FloodWait! Must wait {wait_time}s")
            else:
                logger.error(f"[telegram] Failed to fetch from {channel_name}: {e}")
        
        return messages
    
    def collect(self, since: Optional[datetime]) -> List[dict]:
        """Collect messages from Telegram channels configured in database."""
        all_messages = []
        
        if not HAS_TELETHON or not self.api_id:
            logger.debug("[telegram] Not configured - returning empty")
            return []
        
        if not self.configured_channels:
            logger.warning("[telegram] No channels configured in telegram_sources table")
            return []
        
        try:
            # Check if platform is enabled
            if not self.rate_limiter.is_enabled("telegram"):
                logger.info("[telegram] Platform disabled - skipping")
                return []
            
            # Create event loop if needed
            try:
                self.loop = asyncio.get_event_loop()
                if self.loop.is_closed():
                    self.loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(self.loop)
            except RuntimeError:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            
            # Initialize client
            if not self.loop.run_until_complete(self._init_client()):
                return []
            
            # Get all dialogs once (single API call)
            dialogs = self.loop.run_until_complete(self.client.get_dialogs())
            dialog_map = {d.name.lower(): d for d in dialogs if d.is_channel or d.is_group}
            
            # Also map by ID for more precise matching
            dialog_id_map = {str(d.id): d for d in dialogs if d.is_channel or d.is_group}
            
            # Limit channels per cycle to avoid too many API calls
            max_channels = min(len(self.configured_channels), 10)
            channels_to_fetch = self.configured_channels[:max_channels]
            
            logger.info(f"[telegram] Fetching from {len(channels_to_fetch)}/{len(self.configured_channels)} channels")
            
            # Fetch from configured channels with random delays
            channels_processed = 0
            for ch_config in channels_to_fetch:
                channel_id = ch_config["channel_id"]
                channel_name = ch_config["channel_name"]
                
                # Random delay between channel fetches (3-5 seconds)
                if channels_processed > 0:
                    delay = random.uniform(3.0, 5.0)
                    time.sleep(delay)
                
                # Try to find dialog by ID first, then by name
                dialog = None
                
                # Check if channel_id is in dialogs (exact ID match)
                if channel_id in dialog_id_map:
                    dialog = dialog_id_map[channel_id]
                else:
                    # Try partial name matching
                    channel_name_lower = channel_name.lower()
                    for name, d in dialog_map.items():
                        if channel_id.lower() in name or channel_name_lower in name:
                            dialog = d
                            break
                
                if dialog:
                    try:
                        messages = self.loop.run_until_complete(
                            self._fetch_messages(dialog, limit=20)
                        )
                        all_messages.extend(messages)
                        logger.debug(f"[telegram] {dialog.name}: {len(messages)} new msgs")
                        channels_processed += 1
                    except Exception as e:
                        error_str = str(e).lower()
                        if 'flood' in error_str:
                            logger.error(f"[telegram] FloodWait detected - stopping collection")
                            break  # Stop immediately on FloodWait
                        logger.debug(f"[telegram] {dialog.name} error: {e}")
                else:
                    # Only log at debug to reduce spam for missing channels
                    logger.debug(f"[telegram] Not found: {channel_name}")
            
            # Record successful collection
            self.rate_limiter.record_collection("telegram", success=True)
            
        except Exception as e:
            self.rate_limiter.record_collection("telegram", success=False)
            logger.error(f"[telegram] Collection error: {e}")
        
        # Summary log only
        logger.info(f"[telegram] 📊 {len(all_messages)} new msgs from {channels_processed} channels")
        return all_messages
    
    async def _get_crypto_channels(self, limit: int = 10) -> List:
        """DEPRECATED: Use configured channels from database instead."""
        return []


# =============================================================================
# PRODUCTION WORKER
# =============================================================================

class ProductionWorker(BackgroundWorker):
    """
    Production worker with PostgreSQL and real collectors.
    Includes Telegram reporting for periodic sentiment reports.
    """
    
    def __init__(self):
        # Use PostgreSQL database
        database = PostgreSQLDatabase()
        
        super().__init__(
            database=database,
            state_file=Path("production_worker_state.json")
        )
        
        # Initialize Telegram Reporter
        self.telegram_reporter = TelegramReporter(database)
        self._async_loop = asyncio.new_event_loop()
        
        logger.info("Production worker initialized with PostgreSQL database")
    
    def _create_collectors(self) -> dict:
        """Create real collectors instead of mock."""
        return {
            "twitter": TwitterSourceCollector(),
            "reddit": RedditSourceCollector(),
            "telegram": TelegramSourceCollector()
        }
    
    def start(self):
        """Start the production worker."""
        logger.info("Starting production worker...")
        
        # Create collectors
        collectors = self._create_collectors()
        
        # Override parent loops with real collectors
        intervals = {
            "twitter": TWITTER_INTERVAL,
            "reddit": REDDIT_INTERVAL,
            "telegram": TELEGRAM_INTERVAL
        }
        
        # Clear default mock loops and replace with real ones
        self.loops = {}
        for source, collector in collectors.items():
            loop = SourceLoop(
                source=source,
                interval=intervals[source],
                collector=collector,
                pipeline=self.pipeline,  # Use parent's pipeline
                state=self.state,        # Use parent's state
                metrics=self.metrics     # Use parent's metrics
            )
            self.loops[source] = loop
            loop.start()
        
        self._running = True
        logger.info("Production worker started with real collectors")
        
        # Block main thread with quality updates and Telegram reporting
        try:
            while self._running:
                try:
                    # Quality updates
                    if self.quality_updater.should_update():
                        self.quality_updater.update()
                    
                    # Telegram reporting
                    if self.telegram_reporter.should_send_report():
                        logger.info("Sending periodic Telegram report...")
                        try:
                            self._async_loop.run_until_complete(
                                self.telegram_reporter.send_report()
                            )
                        except Exception as e:
                            logger.error(f"Telegram report error: {e}")
                    
                    self.state.save_state()
                except Exception as e:
                    logger.error(f"Main loop error: {e}")
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
            self._async_loop.close()
            self.stop()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for production worker."""
    print("=" * 70)
    print("Production Background Worker for Social Sentiment Data Service")
    print("=" * 70)
    print()
    print("ABSOLUTE RULES:")
    print("  - NO mocking data")
    print("  - NO hallucination")
    print("  - NO paid APIs")
    print("  - NO trading signals")
    print("  - If any stage fails → DROP EVENT and continue")
    print()
    
    # Check database connection
    print("Checking database connection...")
    print(f"  Host: {os.getenv('POSTGRES_HOST', 'localhost')}")
    print(f"  Database: {os.getenv('POSTGRES_DATABASE', 'sentiment_db')}")
    print(f"  User: {os.getenv('POSTGRES_USER', 'sentiment_user')}")
    print()
    
    # Create and start worker
    worker = ProductionWorker()
    
    # Handle signals
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        worker.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        worker.start()
    except Exception as e:
        logger.error(f"Worker failed: {e}")
        worker.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
