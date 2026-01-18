"""
Asset Configuration Manager

Manages the list of supported crypto assets dynamically from database.
Assets can be added/removed/enabled/disabled without restarting the worker.

Usage:
    from asset_config import AssetConfig
    
    config = AssetConfig()
    
    # Get all active assets
    assets = config.get_active_assets()
    
    # Detect asset from text
    asset = config.detect_asset("Bitcoin is pumping!")  # Returns "BTC"
    
    # Check if text mentions any tracked asset
    if config.contains_tracked_asset("ETH to the moon!"):
        ...
    
    # Reload config from database
    config.reload()
"""

import os
import re
import time
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class Asset:
    """Represents a tracked crypto asset."""
    symbol: str
    name: str
    keywords: List[str]
    is_active: bool
    priority: int
    
    def matches_text(self, text: str) -> bool:
        """Check if text contains any of this asset's keywords."""
        text_lower = text.lower()
        return any(kw.lower() in text_lower for kw in self.keywords)


class AssetConfig:
    """
    Dynamic asset configuration manager.
    
    Loads asset configuration from database and provides methods for:
    - Getting list of active assets
    - Detecting which asset a text is about
    - Checking if text mentions any tracked asset
    - Auto-reloading configuration periodically
    """
    
    # Cache refresh interval (seconds)
    CACHE_TTL = 300  # 5 minutes
    
    def __init__(self, auto_reload: bool = True):
        """
        Initialize asset configuration.
        
        Args:
            auto_reload: Whether to auto-reload from DB when cache expires
        """
        self._assets: Dict[str, Asset] = {}
        self._keywords_to_asset: Dict[str, str] = {}
        self._last_reload: float = 0
        self._auto_reload = auto_reload
        self._compiled_patterns: Dict[str, re.Pattern] = {}
        
        # Load initial configuration
        self.reload()
    
    def _get_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
            user=os.getenv("POSTGRES_USER", "sentiment_user"),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )
    
    def reload(self) -> None:
        """Reload asset configuration from database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT symbol, name, keywords, is_active, priority
                FROM asset_config
                ORDER BY priority DESC
            """)
            
            self._assets.clear()
            self._keywords_to_asset.clear()
            self._compiled_patterns.clear()
            
            for row in cursor.fetchall():
                symbol, name, keywords, is_active, priority = row
                
                asset = Asset(
                    symbol=symbol,
                    name=name,
                    keywords=keywords if keywords else [],
                    is_active=is_active,
                    priority=priority
                )
                
                self._assets[symbol] = asset
                
                # Build keyword lookup (for active assets only)
                if is_active:
                    for kw in keywords:
                        kw_lower = kw.lower()
                        # Higher priority asset wins if keyword conflict
                        if kw_lower not in self._keywords_to_asset:
                            self._keywords_to_asset[kw_lower] = symbol
                    
                    # Compile regex pattern for this asset
                    # Match: $BTC, #BTC, BTC (word boundary)
                    escaped_keywords = [re.escape(kw) for kw in keywords]
                    pattern = r'(?:^|[\s$#])(' + '|'.join(escaped_keywords) + r')(?:$|[\s.,!?])'
                    self._compiled_patterns[symbol] = re.compile(pattern, re.IGNORECASE)
            
            cursor.close()
            conn.close()
            
            self._last_reload = time.time()
            logger.info(f"Loaded {len(self._assets)} assets, {len(self.get_active_assets())} active")
            
        except Exception as e:
            logger.error(f"Failed to reload asset config: {e}")
            # Keep existing config if reload fails
    
    def _check_reload(self) -> None:
        """Check if cache needs refresh."""
        if self._auto_reload and (time.time() - self._last_reload) > self.CACHE_TTL:
            self.reload()
    
    def get_all_assets(self) -> List[Asset]:
        """Get all configured assets (active and inactive)."""
        self._check_reload()
        return list(self._assets.values())
    
    def get_active_assets(self) -> List[Asset]:
        """Get only active assets, sorted by priority."""
        self._check_reload()
        return [a for a in self._assets.values() if a.is_active]
    
    def get_active_symbols(self) -> List[str]:
        """Get list of active asset symbols."""
        return [a.symbol for a in self.get_active_assets()]
    
    def get_asset(self, symbol: str) -> Optional[Asset]:
        """Get asset by symbol."""
        self._check_reload()
        return self._assets.get(symbol.upper())
    
    def detect_asset(self, text: str) -> Optional[str]:
        """
        Detect which asset the text is about.
        
        Returns the highest priority asset that matches.
        Returns None if no asset matches.
        """
        self._check_reload()
        
        if not text:
            return None
        
        text_lower = text.lower()
        
        # Check patterns in priority order
        for asset in self.get_active_assets():
            if asset.matches_text(text):
                return asset.symbol
        
        return None
    
    def detect_all_assets(self, text: str) -> List[str]:
        """
        Detect all assets mentioned in text.
        
        Returns list of asset symbols, sorted by priority.
        """
        self._check_reload()
        
        if not text:
            return []
        
        found = []
        for asset in self.get_active_assets():
            if asset.matches_text(text):
                found.append(asset.symbol)
        
        return found
    
    def contains_tracked_asset(self, text: str) -> bool:
        """Check if text mentions any tracked (active) asset."""
        return self.detect_asset(text) is not None
    
    def get_keywords_for_asset(self, symbol: str) -> List[str]:
        """Get keywords for a specific asset."""
        asset = self.get_asset(symbol)
        return asset.keywords if asset else []
    
    def get_all_keywords(self) -> Set[str]:
        """Get all keywords from all active assets."""
        self._check_reload()
        return set(self._keywords_to_asset.keys())


# Singleton instance for convenience
_default_config: Optional[AssetConfig] = None


def get_asset_config() -> AssetConfig:
    """Get the default AssetConfig instance (singleton)."""
    global _default_config
    if _default_config is None:
        _default_config = AssetConfig()
    return _default_config


def detect_asset(text: str) -> Optional[str]:
    """Convenience function to detect asset from text."""
    return get_asset_config().detect_asset(text)


def get_active_assets() -> List[str]:
    """Convenience function to get active asset symbols."""
    return get_asset_config().get_active_symbols()


def contains_tracked_asset(text: str) -> bool:
    """Convenience function to check if text contains tracked asset."""
    return get_asset_config().contains_tracked_asset(text)


# =============================================================================
# DATABASE MANAGEMENT FUNCTIONS
# =============================================================================

def add_asset(symbol: str, name: str, keywords: List[str], 
              is_active: bool = True, priority: int = 5) -> bool:
    """
    Add a new asset to the configuration.
    
    Args:
        symbol: Asset symbol (e.g., "BTC")
        name: Full name (e.g., "Bitcoin")
        keywords: List of keywords to match
        is_active: Whether to actively track this asset
        priority: Higher priority wins in keyword conflicts
    
    Returns:
        True if successful
    """
    try:
        config = get_asset_config()
        conn = config._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO asset_config (symbol, name, keywords, is_active, priority)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                keywords = EXCLUDED.keywords,
                is_active = EXCLUDED.is_active,
                priority = EXCLUDED.priority,
                updated_at = NOW()
        """, (symbol.upper(), name, keywords, is_active, priority))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # Reload config
        config.reload()
        
        logger.info(f"Added/updated asset: {symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to add asset {symbol}: {e}")
        return False


def remove_asset(symbol: str) -> bool:
    """Remove an asset from configuration."""
    try:
        config = get_asset_config()
        conn = config._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM asset_config WHERE symbol = %s", (symbol.upper(),))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        config.reload()
        
        logger.info(f"Removed asset: {symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to remove asset {symbol}: {e}")
        return False


def set_asset_active(symbol: str, is_active: bool) -> bool:
    """Enable or disable an asset."""
    try:
        config = get_asset_config()
        conn = config._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE asset_config 
            SET is_active = %s, updated_at = NOW()
            WHERE symbol = %s
        """, (is_active, symbol.upper()))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        config.reload()
        
        status = "enabled" if is_active else "disabled"
        logger.info(f"Asset {symbol} {status}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update asset {symbol}: {e}")
        return False


def update_keywords(symbol: str, keywords: List[str]) -> bool:
    """Update keywords for an asset."""
    try:
        config = get_asset_config()
        conn = config._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE asset_config 
            SET keywords = %s, updated_at = NOW()
            WHERE symbol = %s
        """, (keywords, symbol.upper()))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        config.reload()
        
        logger.info(f"Updated keywords for {symbol}: {keywords}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update keywords for {symbol}: {e}")
        return False


def list_assets() -> None:
    """Print all configured assets."""
    config = get_asset_config()
    config.reload()
    
    print("\n" + "="*80)
    print("ASSET CONFIGURATION")
    print("="*80)
    
    for asset in config.get_all_assets():
        status = "✅ ACTIVE" if asset.is_active else "❌ INACTIVE"
        print(f"\n{asset.symbol} - {asset.name}")
        print(f"  Status: {status}")
        print(f"  Priority: {asset.priority}")
        print(f"  Keywords: {', '.join(asset.keywords)}")
    
    print("\n" + "="*80)
    print(f"Total: {len(config.get_all_assets())} assets, "
          f"{len(config.get_active_assets())} active")
    print("="*80 + "\n")


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        list_assets()
        print("\nUsage:")
        print("  python asset_config.py list              - List all assets")
        print("  python asset_config.py add SYMBOL NAME   - Add new asset")
        print("  python asset_config.py enable SYMBOL     - Enable asset")
        print("  python asset_config.py disable SYMBOL    - Disable asset")
        print("  python asset_config.py remove SYMBOL     - Remove asset")
        print("  python asset_config.py test 'text'       - Test asset detection")
        sys.exit(0)
    
    command = sys.argv[1].lower()
    
    if command == "list":
        list_assets()
    
    elif command == "add" and len(sys.argv) >= 4:
        symbol = sys.argv[2].upper()
        name = sys.argv[3]
        keywords = sys.argv[4].split(",") if len(sys.argv) > 4 else [symbol.lower()]
        add_asset(symbol, name, keywords)
        print(f"Added asset: {symbol}")
    
    elif command == "enable" and len(sys.argv) >= 3:
        symbol = sys.argv[2].upper()
        set_asset_active(symbol, True)
        print(f"Enabled: {symbol}")
    
    elif command == "disable" and len(sys.argv) >= 3:
        symbol = sys.argv[2].upper()
        set_asset_active(symbol, False)
        print(f"Disabled: {symbol}")
    
    elif command == "remove" and len(sys.argv) >= 3:
        symbol = sys.argv[2].upper()
        remove_asset(symbol)
        print(f"Removed: {symbol}")
    
    elif command == "test" and len(sys.argv) >= 3:
        text = " ".join(sys.argv[2:])
        config = get_asset_config()
        
        print(f"\nText: {text}")
        print(f"Primary asset: {config.detect_asset(text)}")
        print(f"All assets: {config.detect_all_assets(text)}")
        print(f"Contains tracked: {config.contains_tracked_asset(text)}")
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
