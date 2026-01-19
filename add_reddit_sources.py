#!/usr/bin/env python3
"""Add comprehensive Reddit sources for crypto sentiment analysis."""

import psycopg2
from db_config import load_database_config, get_database_connection

# Complete list of Reddit subreddits for crypto sentiment
# Categories:
# 1. News & General Discussion
# 2. Coin-specific
# 3. Trading & Market Trends
# 4. Ecosystem & Technical
# 5. Learning & Support

REDDIT_SOURCES = [
    # ============================================
    # 1. NEWS & GENERAL DISCUSSION (Priority 5-6)
    # ============================================
    ("CryptoCurrency", "MULTI", "news", 100, 6),        # Largest crypto community
    ("CryptoMarkets", "MULTI", "market", 80, 6),        # Market focused
    ("Altcoin", "MULTI", "discussion", 50, 5),          # Altcoin discussions
    ("CryptoMoonShots", "MULTI", "speculation", 40, 5), # Low cap gems
    
    # ============================================
    # 2. COIN-SPECIFIC (Priority 5)
    # ============================================
    ("Bitcoin", "BTC", "discussion", 100, 5),           # Bitcoin main
    ("Ethereum", "ETH", "discussion", 80, 5),           # Ethereum main
    ("Cardano", "ADA", "discussion", 50, 5),            # Cardano/ADA
    ("Dogecoin", "DOGE", "discussion", 50, 5),          # Dogecoin
    ("Solana", "SOL", "discussion", 50, 5),             # Solana
    ("XRP", "XRP", "discussion", 50, 5),                # Ripple XRP
    ("binance", "BNB", "discussion", 40, 5),            # Binance/BNB
    ("Tronix", "TRX", "discussion", 30, 5),             # TRON
    ("Pepecoin", "PEPE", "discussion", 30, 5),          # PEPE meme
    
    # ============================================
    # 3. TRADING & MARKET TRENDS (Priority 4-5)
    # ============================================
    ("CryptoTrading", "MULTI", "trading", 50, 5),       # Trading strategies
    ("DayTradingCrypto", "MULTI", "trading", 40, 4),    # Day trading
    ("LongTermCrypto", "MULTI", "investment", 30, 4),   # Long term holds
    ("CryptoSignals", "MULTI", "signals", 30, 4),       # Trade signals
    ("BitcoinMarkets", "BTC", "market", 80, 5),         # BTC market analysis
    ("ethtrader", "ETH", "trading", 50, 4),             # ETH trading
    ("ethfinance", "ETH", "finance", 50, 4),            # ETH finance
    ("CryptoCurrencyTrading", "MULTI", "trading", 40, 4),
    ("SatoshiStreetBets", "MULTI", "speculation", 40, 4),
    
    # ============================================
    # 4. ECOSYSTEM & TECHNICAL (Priority 3-4)
    # ============================================
    ("DeFi", "MULTI", "defi", 50, 4),                   # DeFi discussions
    ("NFT", "MULTI", "nft", 40, 3),                     # NFT trends
    ("CryptoTechnology", "MULTI", "technical", 40, 4),  # Blockchain tech
    ("Blockchain", "MULTI", "technical", 30, 3),        # General blockchain
    ("defi", "MULTI", "defi", 40, 4),                   # DeFi (lowercase)
    
    # ============================================
    # 5. LEARNING & SUPPORT (Priority 2-3)
    # ============================================
    ("BitcoinBeginners", "BTC", "education", 30, 3),    # BTC beginners
    ("CryptoBeginners", "MULTI", "education", 30, 3),   # Crypto beginners
    ("CryptoApps", "MULTI", "apps", 20, 2),             # Crypto apps/wallets
    
    # ============================================
    # ADDITIONAL USEFUL SUBREDDITS
    # ============================================
    ("btc", "BTC", "discussion", 50, 3),
    ("BitcoinMining", "BTC", "mining", 20, 3),
    ("Bitcoincash", "BCH", "discussion", 20, 2),
    ("litecoin", "LTC", "discussion", 30, 3),
    ("Monero", "XMR", "discussion", 30, 3),
    ("Chainlink", "LINK", "discussion", 30, 3),
    ("Polkadot", "DOT", "discussion", 30, 3),
    ("Avalanche", "AVAX", "discussion", 30, 3),
    ("cosmosnetwork", "ATOM", "discussion", 30, 3),
    ("maticnetwork", "MATIC", "discussion", 30, 3),
]

def main():
    print("=" * 60)
    print("Adding Reddit Subreddits for Crypto Sentiment Analysis")
    print("=" * 60)
    
    conn = get_database_connection()
    cur = conn.cursor()
    
    # Get existing subreddits
    cur.execute("SELECT subreddit FROM reddit_sources")
    existing = {row[0].lower() for row in cur.fetchall()}
    print(f"\nExisting subreddits: {len(existing)}")
    
    added = 0
    updated = 0
    
    for subreddit, asset, role, max_posts, priority in REDDIT_SOURCES:
        try:
            cur.execute("""
                INSERT INTO reddit_sources (subreddit, asset, role, enabled, max_posts_per_run, priority)
                VALUES (%s, %s, %s, TRUE, %s, %s)
                ON CONFLICT (subreddit) DO UPDATE SET
                    asset = EXCLUDED.asset,
                    role = EXCLUDED.role,
                    max_posts_per_run = EXCLUDED.max_posts_per_run,
                    priority = EXCLUDED.priority
            """, (subreddit, asset, role, max_posts, priority))
            
            if subreddit.lower() in existing:
                updated += 1
                print(f"  📝 Updated: r/{subreddit} ({role}, {asset})")
            else:
                added += 1
                print(f"  ✅ Added: r/{subreddit} ({role}, {asset})")
                
        except Exception as e:
            print(f"  ❌ Error adding r/{subreddit}: {e}")
    
    conn.commit()
    
    # Final count
    cur.execute("SELECT COUNT(*) FROM reddit_sources WHERE enabled = TRUE")
    total = cur.fetchone()[0]
    
    print(f"\n{'=' * 60}")
    print(f"Summary:")
    print(f"  - Added: {added} new subreddits")
    print(f"  - Updated: {updated} existing subreddits")
    print(f"  - Total enabled: {total} subreddits")
    print(f"{'=' * 60}")
    
    # Show all subreddits by category
    print("\n📊 Subreddits by Role:")
    cur.execute("""
        SELECT role, COUNT(*), STRING_AGG(subreddit, ', ' ORDER BY priority DESC)
        FROM reddit_sources 
        WHERE enabled = TRUE
        GROUP BY role
        ORDER BY COUNT(*) DESC
    """)
    for role, count, subs in cur.fetchall():
        print(f"  {role}: {count}")
        # Wrap long lines
        if len(subs) > 70:
            subs = subs[:70] + "..."
        print(f"    {subs}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
