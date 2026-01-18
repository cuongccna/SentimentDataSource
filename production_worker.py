"""
Production Background Worker for Crypto Social Sentiment Data Service.

This worker:
- Connects to PostgreSQL database
- Uses REAL crawlers (Reddit, Twitter, Telegram)
- Persists data to PostgreSQL tables

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
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

from reddit_crawler import RedditCrawler

# Import Twitter Scraper (new syndication-based scraper)
try:
    from twitter_scraper import TwitterScraper, NormalizedRecord
    HAS_TWITTER_SCRAPER = True
except ImportError:
    HAS_TWITTER_SCRAPER = False
    logger.warning("twitter_scraper not available. Twitter will use legacy ntscraper.")


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
        """Insert raw event into ingested_messages table."""
        import hashlib
        
        try:
            cursor = self.conn.cursor()
            
            # Create fingerprint from text
            text = kwargs.get("text", "")
            fingerprint = hashlib.md5(text.encode()).hexdigest()
            
            # Check duplicate by fingerprint
            cursor.execute(
                "SELECT id FROM ingested_messages WHERE fingerprint = %s",
                (fingerprint,)
            )
            if cursor.fetchone():
                return None  # Duplicate
            
            # Get metadata and ensure proper JSON format
            metadata = kwargs.get("metadata", {})
            # Replace None values with 0 for JSON compatibility
            if isinstance(metadata, dict):
                for key, value in metadata.items():
                    if value is None:
                        metadata[key] = 0
            
            # Ensure timestamp is not None
            event_time = kwargs.get("timestamp")
            if event_time is None:
                event_time = datetime.now(timezone.utc).isoformat()
            
            # Insert with correct schema
            cursor.execute("""
                INSERT INTO ingested_messages 
                (message_id, source, source_name, asset, text, event_time, 
                 fingerprint, source_reliability, engagement_weight, author_weight, velocity, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                kwargs.get("message_id", ""),
                kwargs.get("source", ""),
                kwargs.get("source_id", ""),  # source_name = source_id (channel/subreddit name)
                kwargs.get("asset", "BTC"),
                text,
                event_time,
                fingerprint,
                0.8,  # default reliability
                metadata.get("engagement_weight", 0) or 0,
                metadata.get("author_weight", 0) or 0,
                metadata.get("velocity", 1.0) or 1.0,
                json.dumps(metadata)
            ))
            
            result = cursor.fetchone()
            self.conn.commit()
            return str(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Failed to insert raw event: {e}")
            self.conn.rollback()
            return None
    
    def insert_sentiment_event(self, **kwargs) -> Optional[str]:
        """Insert sentiment result into sentiment_results table."""
        try:
            cursor = self.conn.cursor()
            
            # Get message_id as integer (reference to ingested_messages.id)
            message_id = kwargs.get("message_id")
            if isinstance(message_id, str):
                # Try to extract numeric ID or use hash
                try:
                    message_id = int(message_id)
                except ValueError:
                    message_id = abs(hash(message_id)) % (10 ** 9)  # Convert to int
            
            cursor.execute("""
                INSERT INTO sentiment_results 
                (message_id, asset, sentiment_score, sentiment_label, confidence, weighted_score, signal_strength)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                message_id,
                kwargs.get("asset", "BTC"),
                kwargs.get("sentiment_score", 0.0),
                kwargs.get("sentiment_label", "neutral"),
                kwargs.get("confidence", 0.5),
                kwargs.get("weighted_score", 0.0),
                kwargs.get("signal_strength", 0.0)
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
# REAL SOURCE COLLECTORS
# =============================================================================

class RedditSourceCollector(SourceCollector):
    """Real Reddit collector using RedditCrawler."""
    
    def __init__(self, subreddits: Optional[List[str]] = None):
        super().__init__("reddit")
        # Get subreddits from database or use defaults
        self.subreddits = subreddits or [
            "bitcoin", "cryptocurrency", "bitcoinmarkets", 
            "cryptomarkets", "ethtrader"
        ]
        self.crawler = RedditCrawler(subreddits=self.subreddits)
        logger.info(f"Reddit collector initialized with subreddits: {self.subreddits}")
    
    def collect(self, since: Optional[datetime]) -> List[dict]:
        """Collect posts from Reddit."""
        all_records = []
        
        for subreddit in self.subreddits:
            try:
                logger.info(f"[reddit] Fetching r/{subreddit}...")
                
                # Use crawler to fetch posts - use post_limit instead of limit
                records = self.crawler.crawl_subreddit(subreddit, post_limit=10)
                
                for record in records:
                    # Convert to event format expected by pipeline
                    event = {
                        "message_id": f"reddit_{record.get('id', '')}",
                        "source": "reddit",
                        "source_id": subreddit,
                        "text": record.get("text", ""),
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
                
                logger.info(f"[reddit] Got {len(records)} posts from r/{subreddit}")
                
            except Exception as e:
                logger.error(f"[reddit] Failed to fetch r/{subreddit}: {e}")
                continue
        
        logger.info(f"[reddit] Total collected: {len(all_records)} records")
        return all_records


class TwitterSourceCollector(SourceCollector):
    """
    Twitter collector using TwitterScraper (Syndication API).
    
    Monitors whitelisted Twitter accounts for BTC-related content.
    NO API key required - uses public syndication endpoints.
    
    Rate Limit Handling:
    - Scrapes only ACCOUNTS_PER_BATCH accounts per collection cycle
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
        logger.info(f"[twitter] Rotated to batch {self._batch_index}: {self.accounts}")
    
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
                
                # Ensure timestamp is never None
                timestamp_str = data.get("timestamp")
                if not timestamp_str:
                    timestamp_str = datetime.now(timezone.utc).isoformat()
                
                event = {
                    "message_id": f"tw_{meta.get('tweet_id', '')}",
                    "source": "twitter",
                    "source_id": meta.get("username", "unknown"),
                    "text": data.get("text", ""),
                    "author": meta.get("username", ""),
                    "timestamp": timestamp_str,
                    "created_at": timestamp_str,
                    "likes": meta.get("like_count", 0),
                    "retweets": meta.get("retweet_count", 0),
                    "replies": meta.get("reply_count", 0),
                    "asset": data.get("asset", "BTC"),
                    "metadata": {
                        "tweet_id": meta.get("tweet_id"),
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
        
        Uses TwitterScraper with Syndication API (no auth required).
        Rotates through batches of accounts to avoid rate limits.
        """
        if not self.enabled:
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
            
            # Run async collection
            all_tweets = loop.run_until_complete(
                self._collect_async(limit_per_account=10)
            )
            
            logger.info(f"[twitter] Collected {len(all_tweets)} BTC tweets from batch {self._batch_index}")
            
        except Exception as e:
            logger.error(f"[twitter] Collection failed: {e}")
            import traceback
            traceback.print_exc()
        
        return all_tweets
    
    async def close(self):
        """Cleanup scraper resources."""
        if self.scraper:
            await self.scraper.close()
            self._initialized = False


class TelegramSourceCollector(SourceCollector):
    """
    Telegram collector using Telethon Client API.
    
    Reads messages from channels configured in telegram_sources table.
    """
    
    def __init__(self, database_conn=None):
        super().__init__("telegram")
        self.client = None
        self.loop = None
        self._initialized = False
        self.db_conn = database_conn
        
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
        """Load enabled channels from telegram_sources table."""
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
                SELECT channel_id, channel_name, channel_type, priority 
                FROM telegram_sources 
                WHERE enabled = TRUE 
                ORDER BY priority DESC
            """)
            
            self.configured_channels = []
            for row in cursor.fetchall():
                self.configured_channels.append({
                    "channel_id": row[0],
                    "channel_name": row[1],
                    "channel_type": row[2],
                    "priority": row[3]
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Loaded {len(self.configured_channels)} channels from telegram_sources")
            for ch in self.configured_channels:
                logger.info(f"  - {ch['channel_name']} ({ch['channel_id']}) [priority={ch['priority']}]")
                
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
        """Fetch recent messages from a channel dialog."""
        messages = []
        channel_name = dialog.name
        channel_id = dialog.id
        
        try:
            history = await self.client(GetHistoryRequest(
                peer=dialog.entity,
                limit=limit,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            
            for msg in history.messages:
                if not msg.message:
                    continue
                
                # Filter for BTC-related messages (optional - can collect all)
                text = msg.message.lower()
                if not any(kw in text for kw in ["btc", "bitcoin", "$btc", "crypto", "eth", "sol"]):
                    continue
                
                messages.append({
                    "message_id": f"tg_{channel_id}_{msg.id}",
                    "source": "telegram",
                    "source_id": channel_name,
                    "text": msg.message,
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
            
        except Exception as e:
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
            
            # Get all dialogs once
            dialogs = self.loop.run_until_complete(self.client.get_dialogs())
            dialog_map = {d.name.lower(): d for d in dialogs if d.is_channel or d.is_group}
            
            # Also map by ID for more precise matching
            dialog_id_map = {str(d.id): d for d in dialogs if d.is_channel or d.is_group}
            
            logger.info(f"[telegram] Searching for {len(self.configured_channels)} configured channels")
            
            # Fetch from configured channels only
            for ch_config in self.configured_channels:
                channel_id = ch_config["channel_id"]
                channel_name = ch_config["channel_name"]
                
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
                        logger.info(f"[telegram] Fetching from {dialog.name} (configured as {channel_name})...")
                        messages = self.loop.run_until_complete(
                            self._fetch_messages(dialog, limit=20)
                        )
                        all_messages.extend(messages)
                        logger.info(f"[telegram] Got {len(messages)} messages from {dialog.name}")
                    except Exception as e:
                        logger.error(f"[telegram] Error fetching {dialog.name}: {e}")
                else:
                    logger.warning(f"[telegram] Channel not found: {channel_name} ({channel_id})")
                    logger.warning(f"[telegram] Make sure you've joined this channel/group")
            
        except Exception as e:
            logger.error(f"[telegram] Collection error: {e}")
        
        logger.info(f"[telegram] Total collected: {len(all_messages)} messages")
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
    """
    
    def __init__(self):
        # Use PostgreSQL database
        database = PostgreSQLDatabase()
        
        super().__init__(
            database=database,
            state_file=Path("production_worker_state.json")
        )
        
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
        
        # Block main thread with quality updates
        try:
            while self._running:
                try:
                    if self.quality_updater.should_update():
                        self.quality_updater.update()
                    self.state.save_state()
                except Exception as e:
                    logger.error(f"Quality update error: {e}")
                time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
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
