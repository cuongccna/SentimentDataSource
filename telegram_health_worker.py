"""
Telegram Channel Health Worker

PURPOSE:
Determine whether a Telegram channel/group is ALIVE, INACTIVE,
DEAD, PRIVATE, or ERROR before allowing ingestion.
This worker acts as a GATEKEEPER for Telegram ingestion.

ABSOLUTE RULES:
- Health check MUST run BEFORE any data ingestion
- NO data ingestion from dead/private channels
- NO bypass of health check
- Channel lifecycle is fully observable and backtest-safe

EXECUTION MODEL:
- Runs periodically (every 10-30 minutes)
- Runs INDEPENDENTLY from ingestion worker
- Must complete before next ingestion cycle
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from enum import Enum

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Telethon imports
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UserBannedInChannelError,
    FloodWaitError,
    AuthKeyUnregisteredError,
    SessionPasswordNeededError,
)
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import Channel, Chat, User

load_dotenv()

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ'
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("telegram_health_worker")


# =============================================================================
# ENUMS AND CONSTANTS
# =============================================================================

class ChannelStatus(str, Enum):
    """Channel health status values."""
    UNKNOWN = "unknown"
    ALIVE = "alive"
    INACTIVE = "inactive"
    DEAD = "dead"
    PRIVATE = "private"
    ERROR = "error"


# Activity thresholds by channel type (in hours)
ACTIVITY_THRESHOLDS = {
    "news": 24,
    "analytics": 24,
    "aggregator": 24,
    "whale": 12,
    "liquidation": 12,
    "price": 12,
    "community": 48,
    "education": 24,
    # Default for unknown types
    "default": 24
}

# Error escalation threshold
MAX_CONSECUTIVE_ERRORS = 3

# Health check interval (seconds)
HEALTH_CHECK_INTERVAL = 600  # 10 minutes


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TelegramSource:
    """Represents a Telegram source from database."""
    id: int
    channel_id: int
    channel_name: str
    asset: str
    channel_type: str
    enabled: bool
    priority: int
    status: str
    error_count: int
    last_message_at: Optional[datetime]
    last_checked_at: Optional[datetime]
    username: Optional[str] = None  # Telegram username (e.g., @whale_alert_io)
    
    def get_activity_threshold(self) -> timedelta:
        """Get activity threshold based on channel type."""
        hours = ACTIVITY_THRESHOLDS.get(
            self.channel_type, 
            ACTIVITY_THRESHOLDS["default"]
        )
        return timedelta(hours=hours)


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

class HealthDatabase:
    """Database operations for health worker."""
    
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
            user=os.getenv("POSTGRES_USER", "sentiment_user"),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )
        self.conn.autocommit = False
        logger.info("Health Worker connected to database")
    
    def ensure_schema(self) -> None:
        """Ensure required columns exist in telegram_sources."""
        cursor = self.conn.cursor()
        
        # Add missing columns if they don't exist
        columns_to_add = [
            ("last_message_at", "TIMESTAMP WITH TIME ZONE"),
            ("last_checked_at", "TIMESTAMP WITH TIME ZONE"),
            ("status", "VARCHAR(20) DEFAULT 'unknown'"),
            ("error_count", "INTEGER DEFAULT 0"),
        ]
        
        for column_name, column_type in columns_to_add:
            try:
                cursor.execute(f"""
                    ALTER TABLE telegram_sources 
                    ADD COLUMN IF NOT EXISTS {column_name} {column_type}
                """)
            except Exception as e:
                logger.debug(f"Column {column_name} might already exist: {e}")
        
        self.conn.commit()
        logger.info("Database schema verified")
    
    def get_enabled_sources(self) -> List[TelegramSource]:
        """Get all enabled Telegram sources."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                id, channel_id, channel_name, asset, 
                COALESCE(channel_type, 'news') as channel_type,
                enabled, priority,
                COALESCE(status, 'unknown') as status,
                COALESCE(error_count, 0) as error_count,
                last_message_at, last_checked_at,
                username
            FROM telegram_sources
            WHERE enabled = TRUE
            ORDER BY priority DESC
        """)
        
        rows = cursor.fetchall()
        sources = []
        
        for row in rows:
            sources.append(TelegramSource(
                id=row["id"],
                channel_id=int(row["channel_id"]),
                channel_name=row["channel_name"],
                asset=row["asset"],
                channel_type=row["channel_type"],
                enabled=row["enabled"],
                priority=row["priority"],
                status=row["status"],
                error_count=row["error_count"],
                last_message_at=row["last_message_at"],
                last_checked_at=row["last_checked_at"],
                username=row.get("username")
            ))
        
        return sources
    
    def update_source_health(
        self,
        source_id: int,
        status: ChannelStatus,
        last_message_at: Optional[datetime] = None,
        error_count: int = 0,
        disable: bool = False
    ) -> None:
        """Update health status of a Telegram source."""
        cursor = self.conn.cursor()
        
        now = datetime.now(timezone.utc)
        
        if disable:
            cursor.execute("""
                UPDATE telegram_sources
                SET status = %s,
                    last_checked_at = %s,
                    error_count = %s,
                    enabled = FALSE
                WHERE id = %s
            """, (status.value, now, error_count, source_id))
        elif last_message_at:
            cursor.execute("""
                UPDATE telegram_sources
                SET status = %s,
                    last_checked_at = %s,
                    last_message_at = %s,
                    error_count = %s
                WHERE id = %s
            """, (status.value, now, last_message_at, error_count, source_id))
        else:
            cursor.execute("""
                UPDATE telegram_sources
                SET status = %s,
                    last_checked_at = %s,
                    error_count = %s
                WHERE id = %s
            """, (status.value, now, error_count, source_id))
        
        self.conn.commit()
    
    def get_health_summary(self) -> Dict[str, int]:
        """Get summary of channel health statuses."""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                COALESCE(status, 'unknown') as status,
                COUNT(*) as count
            FROM telegram_sources
            GROUP BY status
        """)
        
        return {row["status"]: row["count"] for row in cursor.fetchall()}
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


# =============================================================================
# TELEGRAM HEALTH CHECKER
# =============================================================================

class TelegramHealthChecker:
    """
    Checks health of Telegram channels.
    
    Determines whether a channel is:
    - ALIVE: Can resolve + has recent activity
    - INACTIVE: Can resolve but no recent activity
    - DEAD: Cannot resolve (invalid/banned)
    - PRIVATE: Channel is private
    - ERROR: Temporary error occurred
    """
    
    def __init__(self):
        self.api_id = os.getenv("TELEGRAM_API_ID")
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.session_file = os.getenv(
            "TELEGRAM_SESSION_FILE",
            "telegram_production_session"
        )
        self.client: Optional[TelegramClient] = None
    
    async def connect(self) -> bool:
        """Connect to Telegram."""
        if not self.api_id or not self.api_hash:
            logger.error("TELEGRAM_API_ID and TELEGRAM_API_HASH required")
            return False
        
        try:
            self.client = TelegramClient(
                self.session_file,
                int(self.api_id),
                self.api_hash
            )
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.error("Telegram session not authorized")
                return False
            
            me = await self.client.get_me()
            logger.info(f"Connected to Telegram as {me.first_name} (@{me.username})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Telegram: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from Telegram."""
        if self.client:
            await self.client.disconnect()
            logger.info("Disconnected from Telegram")
    
    async def check_channel_health(
        self, 
        source: TelegramSource
    ) -> tuple[ChannelStatus, Optional[datetime], int]:
        """
        Check health of a single channel.
        
        Returns:
            (status, last_message_at, error_count)
        """
        if not self.client:
            return ChannelStatus.ERROR, None, source.error_count + 1
        
        try:
            # Step 1: Resolve channel entity
            # Try username first (more reliable), then channel_id
            entity = None
            
            if source.username:
                try:
                    entity = await self.client.get_entity(f"@{source.username.lstrip('@')}")
                except Exception as e:
                    logger.debug(f"[{source.channel_name}] Username @{source.username} failed: {e}")
            
            if entity is None:
                try:
                    entity = await self.client.get_entity(source.channel_id)
                except Exception as e:
                    logger.debug(f"[{source.channel_name}] Channel ID {source.channel_id} failed: {e}")
                    raise  # Re-raise to be caught by outer exception handlers
            
            if entity is None:
                logger.warning(f"[{source.channel_name}] Entity is None")
                return ChannelStatus.DEAD, None, 0
            
            # Step 2: Get last message time
            try:
                messages = await self.client(GetHistoryRequest(
                    peer=entity,
                    limit=1,
                    offset_date=None,
                    offset_id=0,
                    max_id=0,
                    min_id=0,
                    add_offset=0,
                    hash=0
                ))
                
                if messages.messages:
                    last_msg = messages.messages[0]
                    last_message_at = last_msg.date
                    
                    if last_message_at.tzinfo is None:
                        last_message_at = last_message_at.replace(tzinfo=timezone.utc)
                else:
                    last_message_at = None
                    
            except Exception as e:
                logger.warning(f"[{source.channel_name}] Cannot get messages: {e}")
                last_message_at = source.last_message_at
            
            # Step 3: Determine status based on activity
            if last_message_at is None:
                status = ChannelStatus.UNKNOWN
                logger.info(f"[{source.channel_name}] Status: UNKNOWN (no messages)")
            else:
                threshold = source.get_activity_threshold()
                age = datetime.now(timezone.utc) - last_message_at
                
                if age <= threshold:
                    status = ChannelStatus.ALIVE
                    logger.info(
                        f"[{source.channel_name}] Status: ALIVE "
                        f"(last msg {age.total_seconds()/3600:.1f}h ago)"
                    )
                else:
                    status = ChannelStatus.INACTIVE
                    logger.warning(
                        f"[{source.channel_name}] Status: INACTIVE "
                        f"(last msg {age.total_seconds()/3600:.1f}h ago, "
                        f"threshold: {threshold.total_seconds()/3600:.0f}h)"
                    )
            
            return status, last_message_at, 0
            
        except ChannelInvalidError:
            logger.error(f"[{source.channel_name}] CHANNEL_INVALID - marking as DEAD")
            return ChannelStatus.DEAD, None, 0
            
        except ChannelPrivateError:
            logger.error(f"[{source.channel_name}] CHANNEL_PRIVATE - marking as PRIVATE")
            return ChannelStatus.PRIVATE, None, 0
            
        except UserBannedInChannelError:
            logger.error(f"[{source.channel_name}] USER_BANNED - marking as DEAD")
            return ChannelStatus.DEAD, None, 0
            
        except FloodWaitError as e:
            logger.warning(
                f"[{source.channel_name}] FloodWait {e.seconds}s - marking as ERROR"
            )
            return ChannelStatus.ERROR, source.last_message_at, source.error_count + 1
            
        except Exception as e:
            logger.error(f"[{source.channel_name}] Unexpected error: {e}")
            return ChannelStatus.ERROR, source.last_message_at, source.error_count + 1


# =============================================================================
# HEALTH WORKER
# =============================================================================

class TelegramHealthWorker:
    """
    Main health worker that runs periodically.
    
    Acts as a GATEKEEPER for Telegram ingestion:
    - Checks all enabled channels
    - Updates their health status
    - Disables dead/private channels
    - Escalates persistent errors
    """
    
    def __init__(self):
        self.db = HealthDatabase()
        self.checker = TelegramHealthChecker()
        self._running = False
    
    async def run_health_check(self) -> Dict[str, Any]:
        """
        Run health check on all enabled channels.
        
        Returns summary of results.
        """
        logger.info("="*60)
        logger.info("TELEGRAM HEALTH CHECK STARTED")
        logger.info("="*60)
        
        # Ensure schema
        self.db.ensure_schema()
        
        # Connect to Telegram
        if not await self.checker.connect():
            logger.error("Cannot connect to Telegram - aborting health check")
            return {"error": "Cannot connect to Telegram"}
        
        try:
            # Get enabled sources
            sources = self.db.get_enabled_sources()
            logger.info(f"Checking {len(sources)} enabled channels...")
            
            results = {
                "total": len(sources),
                "alive": 0,
                "inactive": 0,
                "dead": 0,
                "private": 0,
                "error": 0,
                "unknown": 0,
                "disabled": 0
            }
            
            for source in sources:
                logger.info(f"Checking: {source.channel_name} ({source.channel_id})")
                
                # Check health
                status, last_message_at, error_count = await self.checker.check_channel_health(source)
                
                # Determine if should disable
                should_disable = False
                
                if status == ChannelStatus.DEAD:
                    should_disable = True
                    results["disabled"] += 1
                    logger.warning(f"  → DISABLING: Channel is DEAD")
                    
                elif status == ChannelStatus.PRIVATE:
                    should_disable = True
                    results["disabled"] += 1
                    logger.warning(f"  → DISABLING: Channel is PRIVATE")
                    
                elif status == ChannelStatus.ERROR:
                    if error_count >= MAX_CONSECUTIVE_ERRORS:
                        should_disable = True
                        results["disabled"] += 1
                        logger.warning(
                            f"  → DISABLING: {error_count} consecutive errors"
                        )
                
                # Update database
                self.db.update_source_health(
                    source_id=source.id,
                    status=status,
                    last_message_at=last_message_at,
                    error_count=error_count,
                    disable=should_disable
                )
                
                # Track results
                results[status.value] += 1
                
                # Small delay between checks
                await asyncio.sleep(0.5)
            
            # Log summary
            logger.info("="*60)
            logger.info("HEALTH CHECK COMPLETE")
            logger.info("="*60)
            logger.info(f"  Total checked: {results['total']}")
            logger.info(f"  ✅ Alive:      {results['alive']}")
            logger.info(f"  ⚠️  Inactive:   {results['inactive']}")
            logger.info(f"  ❌ Dead:       {results['dead']}")
            logger.info(f"  🔒 Private:    {results['private']}")
            logger.info(f"  ⚡ Error:      {results['error']}")
            logger.info(f"  ❓ Unknown:    {results['unknown']}")
            logger.info(f"  🚫 Disabled:   {results['disabled']}")
            logger.info("="*60)
            
            return results
            
        finally:
            await self.checker.disconnect()
    
    async def run_loop(self, interval: int = None):
        """Run health check in a loop."""
        self._running = True
        check_interval = interval or HEALTH_CHECK_INTERVAL
        logger.info(f"Health Worker starting (interval: {check_interval}s)")
        
        while self._running:
            try:
                await self.run_health_check()
            except Exception as e:
                logger.error(f"Health check failed: {e}")
            
            # Wait for next cycle
            logger.info(f"Next health check in {check_interval}s...")
            await asyncio.sleep(check_interval)
    
    def stop(self):
        """Stop the health worker."""
        self._running = False
        logger.info("Health Worker stopping...")
    
    def close(self):
        """Clean up resources."""
        self.db.close()


# =============================================================================
# CLI INTERFACE
# =============================================================================

async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Telegram Channel Health Worker")
    parser.add_argument(
        "--once", 
        action="store_true",
        help="Run health check once and exit"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=HEALTH_CHECK_INTERVAL,
        help=f"Check interval in seconds (default: {HEALTH_CHECK_INTERVAL})"
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show current health summary and exit"
    )
    
    args = parser.parse_args()
    
    worker = TelegramHealthWorker()
    
    try:
        if args.summary:
            # Just show summary
            summary = worker.db.get_health_summary()
            print("\n" + "="*50)
            print("TELEGRAM CHANNEL HEALTH SUMMARY")
            print("="*50)
            total = sum(summary.values())
            for status, count in sorted(summary.items()):
                pct = (count / total * 100) if total > 0 else 0
                print(f"  {status:12}: {count:3} ({pct:5.1f}%)")
            print("="*50)
            print(f"  Total: {total}")
            print("="*50 + "\n")
            
        elif args.once:
            # Run once
            await worker.run_health_check()
            
        else:
            # Run in loop with custom interval
            await worker.run_loop(interval=args.interval)
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        worker.close()


if __name__ == "__main__":
    asyncio.run(main())
