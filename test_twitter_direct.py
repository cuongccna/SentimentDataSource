"""
Twitter/X Real-time Data Collection Test - Direct Connection.

Test Twitter scraping WITHOUT proxy (direct connection).
Uses multiple fallback methods:
1. Nitter instances (no login required)
2. Twitter syndication API
3. Twitter GraphQL API (guest mode)

If all methods fail, you may need to use a proxy due to IP blocks.
"""

import os
import sys
import asyncio
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Load environment
load_dotenv()


def _log(level: str, msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{ts} [{level}] {msg}")


def info(msg: str):
    _log("INFO", msg)


def error(msg: str):
    _log("ERROR", msg)


def warning(msg: str):
    _log("WARNING", msg)


def success(msg: str):
    _log("✅ SUCCESS", msg)


# =============================================================================
# WORKING NITTER INSTANCES (Regularly updated)
# =============================================================================

NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.cz",
    "https://nitter.net",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
    "https://nitter.unixfox.eu",
]

# Crypto Twitter accounts to monitor
CRYPTO_ACCOUNTS = [
    "whale_alert",      # Whale movements
    "Cointelegraph",    # Crypto news
    "BitcoinMagazine",  # Bitcoin news
    "WuBlockchain",     # Mining & news
    "DocumentingBTC",   # Bitcoin education
    "glaboratory",      # On-chain data
]

# BTC Keywords for filtering
BTC_KEYWORDS = ["btc", "bitcoin", "₿", "satoshi", "sats", "$btc", "#btc"]


def contains_btc_keyword(text: str) -> bool:
    """Check if text contains BTC-related keywords."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in BTC_KEYWORDS)


# =============================================================================
# TEST 1: NITTER SCRAPER
# =============================================================================

async def scrape_nitter(username: str, instance: str = None) -> List[Dict]:
    """Scrape tweets from a user via Nitter."""
    import httpx
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    
    instances = [instance] if instance else NITTER_INSTANCES
    
    for inst in instances:
        try:
            async with httpx.AsyncClient(headers=headers, timeout=15.0, follow_redirects=True) as client:
                url = f"{inst}/{username}"
                response = await client.get(url)
                
                if response.status_code != 200:
                    continue
                
                html = response.text
                
                # Parse tweets from HTML
                tweets = []
                
                # Find tweet containers
                # Nitter uses class="timeline-item" for tweets
                tweet_pattern = r'<div class="timeline-item[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>'
                
                # Simpler approach: extract tweet text and metadata
                text_pattern = r'<div class="tweet-content[^"]*"[^>]*>([^<]+(?:<[^>]+>[^<]*</[^>]+>)*[^<]*)</div>'
                
                text_matches = re.findall(text_pattern, html, re.DOTALL)
                
                # Extract timestamps
                time_pattern = r'<span class="tweet-date"[^>]*><a[^>]*title="([^"]+)"'
                time_matches = re.findall(time_pattern, html)
                
                # Extract stats
                stat_pattern = r'<span class="tweet-stat"[^>]*><span class="icon-[^"]*"></span>\s*(\d+)'
                stat_matches = re.findall(stat_pattern, html)
                
                for i, text in enumerate(text_matches[:10]):
                    # Clean HTML tags
                    clean_text = re.sub(r'<[^>]+>', '', text).strip()
                    
                    if clean_text:
                        tweet = {
                            "text": clean_text,
                            "username": username,
                            "source": "nitter",
                            "instance": inst,
                            "timestamp": time_matches[i] if i < len(time_matches) else None,
                        }
                        
                        # Check for BTC content
                        if contains_btc_keyword(clean_text):
                            tweet["has_btc"] = True
                        
                        tweets.append(tweet)
                
                if tweets:
                    return tweets
                    
        except Exception as e:
            continue
    
    return []


async def test_nitter_scraper():
    """Test Nitter-based Twitter scraping."""
    print("\n" + "=" * 60)
    print(" TEST 1: NITTER SCRAPER")
    print("=" * 60)
    
    try:
        import httpx
    except ImportError:
        error("httpx not installed. Run: pip install httpx")
        return None
    
    # Find working instance
    working_instance = None
    
    info("Finding working Nitter instance...")
    
    for instance in NITTER_INSTANCES:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(instance, follow_redirects=True)
                if response.status_code == 200 and "nitter" in response.text.lower():
                    success(f"Found working instance: {instance}")
                    working_instance = instance
                    break
        except:
            continue
    
    if not working_instance:
        warning("No working Nitter instance found")
        return None
    
    # Scrape crypto accounts
    all_tweets = []
    
    for username in CRYPTO_ACCOUNTS[:3]:  # Test with first 3
        info(f"Scraping @{username}...")
        
        tweets = await scrape_nitter(username, working_instance)
        
        if tweets:
            btc_tweets = [t for t in tweets if t.get("has_btc")]
            success(f"Found {len(tweets)} tweets, {len(btc_tweets)} with BTC keywords")
            
            for tweet in btc_tweets[:2]:
                print(f"    [{tweet['username']}] {tweet['text'][:80]}...")
            
            all_tweets.extend(tweets)
        else:
            warning(f"No tweets from @{username}")
        
        await asyncio.sleep(1)  # Rate limit
    
    return all_tweets if all_tweets else None


# =============================================================================
# TEST 2: TWITTER SYNDICATION API
# =============================================================================

async def test_syndication_api():
    """Test Twitter Syndication API (embedded tweets)."""
    print("\n" + "=" * 60)
    print(" TEST 2: TWITTER SYNDICATION API")
    print("=" * 60)
    
    try:
        import httpx
    except ImportError:
        error("httpx not installed")
        return None
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    }
    
    all_data = []
    
    for username in CRYPTO_ACCOUNTS[:2]:
        info(f"Fetching embedded timeline for @{username}...")
        
        try:
            async with httpx.AsyncClient(headers=headers, timeout=20.0) as client:
                # Twitter syndication API
                url = f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}"
                
                response = await client.get(url, params={"showReplies": "false"})
                
                if response.status_code == 200:
                    html = response.text
                    
                    # Extract tweet texts
                    text_pattern = r'"text":"([^"]{10,})"'
                    matches = re.findall(text_pattern, html)
                    
                    if matches:
                        success(f"Found {len(matches)} tweets from @{username}")
                        
                        btc_count = 0
                        for text in matches[:10]:
                            # Decode unicode escapes
                            try:
                                decoded = text.encode('latin1').decode('unicode_escape')
                            except:
                                decoded = text
                            
                            if contains_btc_keyword(decoded):
                                btc_count += 1
                                print(f"    [BTC] {decoded[:80]}...")
                                all_data.append({
                                    "text": decoded,
                                    "username": username,
                                    "has_btc": True,
                                    "source": "syndication"
                                })
                            else:
                                all_data.append({
                                    "text": decoded,
                                    "username": username,
                                    "has_btc": False,
                                    "source": "syndication"
                                })
                        
                        info(f"  BTC-related: {btc_count}")
                    else:
                        warning(f"No tweet content found for @{username}")
                else:
                    warning(f"@{username}: HTTP {response.status_code}")
                    
        except Exception as e:
            warning(f"@{username}: {e}")
        
        await asyncio.sleep(1)
    
    return all_data if all_data else None


# =============================================================================
# TEST 3: TWITTER GUEST API
# =============================================================================

async def test_twitter_guest_api():
    """Test Twitter GraphQL API with guest token."""
    print("\n" + "=" * 60)
    print(" TEST 3: TWITTER GUEST API")
    print("=" * 60)
    
    try:
        import httpx
    except ImportError:
        error("httpx not installed")
        return None
    
    # Public bearer token (used by Twitter web app)
    BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
    }
    
    try:
        async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
            # Step 1: Get guest token
            info("Obtaining guest token...")
            
            response = await client.post("https://api.twitter.com/1.1/guest/activate.json")
            
            if response.status_code == 200:
                data = response.json()
                guest_token = data.get("guest_token")
                
                if guest_token:
                    success(f"Guest token: {guest_token[:20]}...")
                    
                    # Add guest token to headers
                    headers["x-guest-token"] = guest_token
                    
                    # Step 2: Search for BTC tweets
                    info("Searching for BTC tweets...")
                    
                    # Twitter GraphQL search endpoint
                    search_url = "https://api.twitter.com/2/search/adaptive.json"
                    params = {
                        "q": "BTC bitcoin",
                        "count": 20,
                        "tweet_mode": "extended",
                    }
                    
                    async with httpx.AsyncClient(headers=headers, timeout=30.0) as search_client:
                        search_response = await search_client.get(search_url, params=params)
                        
                        if search_response.status_code == 200:
                            success("Search successful!")
                            search_data = search_response.json()
                            
                            # Parse tweets
                            tweets = search_data.get("globalObjects", {}).get("tweets", {})
                            
                            if tweets:
                                success(f"Found {len(tweets)} tweets")
                                
                                for tweet_id, tweet in list(tweets.items())[:5]:
                                    text = tweet.get("full_text", tweet.get("text", ""))
                                    print(f"    [{tweet_id}] {text[:80]}...")
                                
                                return list(tweets.values())
                            else:
                                warning("No tweets in response")
                        elif search_response.status_code == 429:
                            warning("Rate limited by Twitter")
                        else:
                            warning(f"Search failed: HTTP {search_response.status_code}")
                else:
                    warning("No guest token in response")
            else:
                warning(f"Guest activation failed: HTTP {response.status_code}")
                if response.status_code == 403:
                    info("Twitter may be blocking your IP. Try using a proxy.")
                    
    except Exception as e:
        error(f"Guest API test failed: {e}")
    
    return None


# =============================================================================
# TEST 4: TWIKIT (IF AVAILABLE)
# =============================================================================

async def test_twikit_guest():
    """Test twikit library with guest mode."""
    print("\n" + "=" * 60)
    print(" TEST 4: TWIKIT LIBRARY")
    print("=" * 60)
    
    try:
        from twikit import Client
    except ImportError:
        warning("twikit not installed. Skipping.")
        return None
    
    cookies_file = "twitter_cookies.json"
    
    if os.path.exists(cookies_file):
        info(f"Found cookies file: {cookies_file}")
        
        try:
            client = Client("en-US")
            client.load_cookies(cookies_file)
            
            info("Searching for BTC tweets...")
            tweets = await client.search_tweet("BTC bitcoin", "Latest", count=10)
            
            if tweets:
                success(f"Found {len(tweets)} tweets!")
                
                for tweet in tweets[:5]:
                    text = tweet.text[:80] if len(tweet.text) > 80 else tweet.text
                    print(f"    @{tweet.user.screen_name}: {text}...")
                
                return tweets
            else:
                warning("No tweets returned")
                
        except Exception as e:
            error(f"twikit error: {e}")
            return None
    else:
        warning("No cookies file. Run twitter_login.py to authenticate.")
        return None


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================

async def main():
    """Run all Twitter collection tests."""
    print("\n" + "=" * 60)
    print(" TWITTER/X DATA COLLECTION TEST")
    print(" Direct Connection (No Proxy)")
    print("=" * 60)
    
    results = {}
    
    # Test 1: Nitter
    results["nitter"] = await test_nitter_scraper()
    
    # Test 2: Syndication API
    results["syndication"] = await test_syndication_api()
    
    # Test 3: Guest API
    results["guest"] = await test_twitter_guest_api()
    
    # Test 4: Twikit
    results["twikit"] = await test_twikit_guest()
    
    # Summary
    print("\n" + "=" * 60)
    print(" SUMMARY")
    print("=" * 60)
    
    print(f"\n  1. Nitter Scraper: {'✅ PASS (' + str(len(results['nitter'])) + ' tweets)' if results['nitter'] else '❌ FAIL'}")
    print(f"  2. Syndication API: {'✅ PASS (' + str(len(results['syndication'])) + ' tweets)' if results['syndication'] else '❌ FAIL'}")
    print(f"  3. Guest API: {'✅ PASS' if results['guest'] else '⚠️ BLOCKED'}")
    print(f"  4. Twikit: {'✅ PASS' if results['twikit'] else '⚠️ NO COOKIES'}")
    
    # Count BTC-related tweets
    btc_tweets = []
    for source, data in results.items():
        if data and isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("has_btc"):
                    btc_tweets.append(item)
    
    print(f"\n  Total BTC-related tweets collected: {len(btc_tweets)}")
    
    if btc_tweets:
        print("\n  Sample BTC tweets:")
        for tweet in btc_tweets[:5]:
            text = tweet.get("text", "")[:70]
            user = tweet.get("username", "unknown")
            print(f"    @{user}: {text}...")
    
    print("\n" + "=" * 60)
    
    if results["nitter"] or results["syndication"]:
        print("\n✅ Twitter data collection is WORKING!")
        print("   Data can be collected via Nitter or Syndication API.")
    elif results["twikit"]:
        print("\n✅ Twikit is working with saved cookies!")
    else:
        print("\n⚠️ Direct access is limited/blocked.")
        print("   Options:")
        print("   1. Use a working proxy (check proxy connection)")
        print("   2. Run twitter_login.py to authenticate with twikit")
        print("   3. Wait for Nitter instances to come back online")


if __name__ == "__main__":
    asyncio.run(main())
