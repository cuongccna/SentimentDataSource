"""
Database configuration and connection utilities.

Loads configuration from .env file and provides PostgreSQL connections.
"""

import os
from dataclasses import dataclass
from typing import Optional, Any


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "sentimentdata"
    user: str = "postgres"
    password: str = ""
    
    @property
    def connection_string(self) -> str:
        """Get connection string for psycopg2."""
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.user} "
            f"password={self.password}"
        )
    
    @property
    def connection_url(self) -> str:
        """Get connection URL format."""
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class TelegramConfig:
    """Telegram API configuration."""
    api_id: Optional[str] = None
    api_hash: Optional[str] = None
    phone: Optional[str] = None
    bot_token: Optional[str] = None
    channel_id: Optional[str] = None
    global_rate_limit: int = 100
    
    @property
    def is_client_configured(self) -> bool:
        """Check if Telegram Client API is configured."""
        return bool(self.api_id and self.api_hash)
    
    @property
    def is_bot_configured(self) -> bool:
        """Check if Telegram Bot API is configured."""
        return bool(self.bot_token and self.channel_id)


# =============================================================================
# ENVIRONMENT LOADING
# =============================================================================

def load_env_file(filepath: str = ".env") -> dict:
    """
    Load environment variables from .env file.
    
    Returns dict of key-value pairs.
    Does NOT override existing environment variables.
    """
    env_vars = {}
    
    if not os.path.exists(filepath):
        return env_vars
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                
                # Parse key=value
                if "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    env_vars[key] = value
                    
                    # Set in environment if not already set
                    if key not in os.environ:
                        os.environ[key] = value
    except Exception:
        pass
    
    return env_vars


def load_database_config(env_file: str = ".env") -> DatabaseConfig:
    """
    Load database configuration from environment.
    
    First loads .env file, then reads from os.environ.
    """
    load_env_file(env_file)
    
    return DatabaseConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DATABASE", "sentimentdata"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "")
    )


def load_telegram_config(env_file: str = ".env") -> TelegramConfig:
    """
    Load Telegram configuration from environment.
    
    First loads .env file, then reads from os.environ.
    """
    load_env_file(env_file)
    
    return TelegramConfig(
        api_id=os.environ.get("TELEGRAM_API_ID"),
        api_hash=os.environ.get("TELEGRAM_API_HASH"),
        phone=os.environ.get("TELEGRAM_PHONE"),
        bot_token=os.environ.get("TELEGRAM_BOT_TOKEN"),
        channel_id=os.environ.get("TELEGRAM_CHANNEL_ID"),
        global_rate_limit=int(os.environ.get("TELEGRAM_GLOBAL_RATE_LIMIT", "100"))
    )


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_database_connection(config: Optional[DatabaseConfig] = None) -> Any:
    """
    Get a PostgreSQL database connection.
    
    Requires psycopg2 to be installed.
    Returns None if connection fails.
    """
    if config is None:
        config = load_database_config()
    
    try:
        import psycopg2
        
        connection = psycopg2.connect(config.connection_string)
        return connection
    
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        return None
    
    except Exception as e:
        print(f"ERROR: Database connection failed: {e}")
        return None


def test_database_connection(config: Optional[DatabaseConfig] = None) -> bool:
    """
    Test if database connection works.
    
    Returns True if connection successful, False otherwise.
    """
    conn = get_database_connection(config)
    
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except Exception:
        return False


# =============================================================================
# SCHEMA INITIALIZATION
# =============================================================================

def initialize_telegram_sources_table(
    connection: Any,
    drop_existing: bool = False
) -> bool:
    """
    Create telegram_sources table in database.
    
    Args:
        connection: PostgreSQL connection
        drop_existing: If True, drops existing table first
    
    Returns:
        True if successful, False otherwise
    """
    if connection is None:
        print("ERROR: No database connection")
        return False
    
    try:
        cursor = connection.cursor()
        
        if drop_existing:
            cursor.execute("DROP TABLE IF EXISTS telegram_sources CASCADE")
        
        sql = """
            CREATE TABLE IF NOT EXISTS telegram_sources (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT NOT NULL UNIQUE,
                type VARCHAR(20) NOT NULL CHECK (type IN ('channel', 'group')),
                role VARCHAR(20) NOT NULL CHECK (role IN ('news', 'panic', 'community')),
                asset VARCHAR(10) NOT NULL DEFAULT 'BTC',
                enabled BOOLEAN NOT NULL DEFAULT true,
                max_msgs_per_min INTEGER NOT NULL DEFAULT 30,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_telegram_sources_tg_id 
            ON telegram_sources(tg_id);
            
            CREATE INDEX IF NOT EXISTS idx_telegram_sources_enabled 
            ON telegram_sources(enabled);
        """
        
        cursor.execute(sql)
        connection.commit()
        print("SUCCESS: telegram_sources table created")
        return True
    
    except Exception as e:
        print(f"ERROR: Failed to create table: {e}")
        connection.rollback()
        return False


def insert_telegram_source(
    connection: Any,
    tg_id: int,
    source_type: str,
    role: str,
    asset: str = "BTC",
    enabled: bool = True,
    max_msgs_per_min: int = 30
) -> bool:
    """
    Insert a telegram source into the database.
    
    Args:
        connection: PostgreSQL connection
        tg_id: Telegram chat ID
        source_type: 'channel' or 'group'
        role: 'news', 'panic', or 'community'
        asset: Asset symbol (default 'BTC')
        enabled: Whether source is enabled
        max_msgs_per_min: Rate limit for this source
    
    Returns:
        True if successful, False otherwise
    """
    if connection is None:
        return False
    
    try:
        cursor = connection.cursor()
        
        sql = """
            INSERT INTO telegram_sources 
            (tg_id, type, role, asset, enabled, max_msgs_per_min)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tg_id) DO UPDATE SET
                type = EXCLUDED.type,
                role = EXCLUDED.role,
                asset = EXCLUDED.asset,
                enabled = EXCLUDED.enabled,
                max_msgs_per_min = EXCLUDED.max_msgs_per_min,
                updated_at = NOW()
        """
        
        cursor.execute(sql, (tg_id, source_type, role, asset, enabled, max_msgs_per_min))
        connection.commit()
        return True
    
    except Exception as e:
        print(f"ERROR: Failed to insert source: {e}")
        connection.rollback()
        return False


# =============================================================================
# CLI HELPER
# =============================================================================

def print_config_status():
    """Print current configuration status."""
    print("\n" + "=" * 50)
    print("CONFIGURATION STATUS")
    print("=" * 50)
    
    db_config = load_database_config()
    tg_config = load_telegram_config()
    
    print("\nPostgreSQL:")
    print(f"  Host: {db_config.host}")
    print(f"  Port: {db_config.port}")
    print(f"  Database: {db_config.database}")
    print(f"  User: {db_config.user}")
    print(f"  Password: {'***' if db_config.password else '(not set)'}")
    
    # Test connection
    if db_config.password:
        connected = test_database_connection(db_config)
        print(f"  Connection: {'OK' if connected else 'FAILED'}")
    else:
        print("  Connection: (password not configured)")
    
    print("\nTelegram Client API:")
    print(f"  API ID: {'***' if tg_config.api_id else '(not set)'}")
    print(f"  API Hash: {'***' if tg_config.api_hash else '(not set)'}")
    print(f"  Configured: {tg_config.is_client_configured}")
    
    print("\nTelegram Bot API (Alerting):")
    print(f"  Bot Token: {'***' if tg_config.bot_token else '(not set)'}")
    print(f"  Channel ID: {tg_config.channel_id or '(not set)'}")
    print(f"  Configured: {tg_config.is_bot_configured}")
    
    print("\n" + "=" * 50)


if __name__ == "__main__":
    print_config_status()
