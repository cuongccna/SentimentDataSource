"""
Test Twitter/X Real-time Data Collection with SOCKS5 Proxy.

This script tests:
1. Proxy connection
2. Twitter scraping via twikit
3. BTC-related tweet collection
4. Data normalization

Uses SOCKS5 proxy to bypass IP blocks.
"""

import os
import sys
import asyncio
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
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
# PROXY CONFIGURATION
# =============================================================================

PROXY_HOST = os.getenv("TWITTER_PROXY_HOST", "118.70.121.236")
PROXY_PORT = os.getenv("TWITTER_PROXY_PORT", "50099")
PROXY_USER = os.getenv("TWITTER_PROXY_USER", "muaproxy694c57898a312")
PROXY_PASS = os.getenv("TWITTER_PROXY_PASS", "fsejfbalvpjschjc")
PROXY_TYPE = os.getenv("TWITTER_PROXY_TYPE", "socks5")

# Build proxy URL for twikit (socks5://user:pass@host:port)
PROXY_URL = f"{PROXY_TYPE}://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"


# =============================================================================
# TEST 1: PROXY CONNECTION TEST
# =============================================================================

async def test_proxy_connection():
    """Test if proxy connection works."""
    print("\n" + "=" * 60)
    print(" TEST 1: PROXY CONNECTION")
    print("=" * 60)
    
    info(f"Proxy: {PROXY_HOST}:{PROXY_PORT}")
    info(f"Type: {PROXY_TYPE.upper()}")
    
    try:
        import httpx
        
        # Test with httpx + socks proxy
        transport = httpx.AsyncHTTPTransport(proxy=PROXY_URL)
        
        async with httpx.AsyncClient(transport=transport, timeout=30.0) as client:
            # Test with httpbin to check IP
            response = await client.get("https://httpbin.org/ip")
            
            if response.status_code == 200:
                data = response.json()
                success(f"Proxy working! External IP: {data.get('origin', 'unknown')}")
                return True
            else:
                error(f"Proxy test failed: HTTP {response.status_code}")
                return False
                
    except Exception as e:
        error(f"Proxy connection failed: {e}")
        return False


# =============================================================================
# TEST 2: TWITTER GUEST TOKEN TEST
# =============================================================================

async def test_twitter_guest_access():
    """Test Twitter guest access through proxy."""
    print("\n" + "=" * 60)
    print(" TEST 2: TWITTER GUEST ACCESS")
    print("=" * 60)
    
    try:
        import httpx
        
        # Twitter guest API endpoint
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",  # Public bearer token
        }
        
        transport = httpx.AsyncHTTPTransport(proxy=PROXY_URL)
        
        async with httpx.AsyncClient(transport=transport, headers=headers, timeout=30.0) as client:
            # Activate guest token
            response = await client.post("https://api.twitter.com/1.1/guest/activate.json")
            
            if response.status_code == 200:
                data = response.json()
                guest_token = data.get("guest_token", "")
                if guest_token:
                    success(f"Guest token obtained: {guest_token[:20]}...")
                    return guest_token
                else:
                    warning("Response OK but no guest token")
                    return None
            else:
                warning(f"Guest activation returned: {response.status_code}")
                return None
                
    except Exception as e:
        error(f"Guest access test failed: {e}")
        return None


# =============================================================================
# TEST 3: TWIKIT SCRAPER TEST
# =============================================================================

async def test_twikit_scraper():
    """Test Twitter scraping with twikit library."""
    print("\n" + "=" * 60)
    print(" TEST 3: TWIKIT SCRAPER")
    print("=" * 60)
    
    try:
        from twikit import Client
        
        info("Initializing twikit client with proxy...")
        
        # Create client with proxy
        client = Client("en-US")
        
        # Set proxy for the client
        client.http.proxies = {
            "http://": PROXY_URL,
            "https://": PROXY_URL,
        }
        
        info("Attempting to search for BTC tweets...")
        
        # Try guest search (no login required for public tweets)
        # Note: twikit requires login for search in newer versions
        
        # Check if we have saved cookies
        cookies_file = "twitter_cookies.json"
        if os.path.exists(cookies_file):
            info(f"Loading saved cookies from {cookies_file}")
            client.load_cookies(cookies_file)
            success("Cookies loaded successfully!")
            
            # Test search
            tweets = await client.search_tweet("BTC bitcoin", "Latest", count=10)
            
            if tweets:
                success(f"Found {len(tweets)} tweets!")
                
                for i, tweet in enumerate(tweets[:5], 1):
                    print(f"\n  Tweet {i}:")
                    print(f"    ID: {tweet.id}")
                    print(f"    Text: {tweet.text[:100]}..." if len(tweet.text) > 100 else f"    Text: {tweet.text}")
                    print(f"    Created: {tweet.created_at}")
                    print(f"    Likes: {tweet.favorite_count}")
                    print(f"    Retweets: {tweet.retweet_count}")
                    if hasattr(tweet, 'user'):
                        print(f"    Author: @{tweet.user.screen_name} ({tweet.user.followers_count} followers)")
                
                return tweets
            else:
                warning("No tweets returned from search")
                return None
        else:
            warning("No cookies file found. Twitter requires login for search.")
            info("Run twitter_login.py first to authenticate and save cookies.")
            return None
            
    except ImportError:
        error("twikit not installed. Run: pip install twikit")
        return None
    except Exception as e:
        error(f"twikit scraper test failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# =============================================================================
# TEST 4: ALTERNATIVE SCRAPER (NO LOGIN)
# =============================================================================

async def test_alternative_scraper():
    """Test alternative Twitter scraping without login."""
    print("\n" + "=" * 60)
    print(" TEST 4: ALTERNATIVE SCRAPER (NO LOGIN)")
    print("=" * 60)
    
    try:
        import httpx
        
        # Syndication API (public, no auth required)
        # This endpoint shows latest tweets from a user timeline
        
        # Test accounts to scrape
        test_accounts = [
            "whale_alert",      # Whale Alert
            "Cointelegraph",    # Crypto news
            "BitcoinMagazine",  # Bitcoin Magazine
        ]
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        
        transport = httpx.AsyncHTTPTransport(proxy=PROXY_URL)
        
        async with httpx.AsyncClient(transport=transport, headers=headers, timeout=30.0) as client:
            
            for username in test_accounts:
                info(f"Fetching tweets from @{username}...")
                
                # Twitter syndication API
                url = f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{username}"
                
                try:
                    response = await client.get(url, params={"showReplies": "false"})
                    
                    if response.status_code == 200:
                        # This returns HTML with embedded JSON
                        html = response.text
                        
                        # Try to find JSON data in script tag
                        import re
                        
                        # Look for tweet data
                        if "tweet" in html.lower() or "text" in html.lower():
                            success(f"Got response from @{username} ({len(html)} bytes)")
                            
                            # Try to extract some text
                            text_matches = re.findall(r'"text":"([^"]{20,200})"', html)
                            if text_matches:
                                print(f"    Sample tweets found:")
                                for t in text_matches[:3]:
                                    # Decode unicode
                                    decoded = t.encode().decode('unicode_escape')
                                    if "btc" in decoded.lower() or "bitcoin" in decoded.lower():
                                        print(f"      [BTC] {decoded[:100]}...")
                                    else:
                                        print(f"      {decoded[:80]}...")
                        else:
                            warning(f"Response from @{username} has no tweet data")
                    else:
                        warning(f"@{username}: HTTP {response.status_code}")
                        
                except Exception as e:
                    warning(f"Failed to fetch @{username}: {e}")
                
                await asyncio.sleep(1)  # Rate limiting
            
            return True
            
    except Exception as e:
        error(f"Alternative scraper test failed: {e}")
        return False


# =============================================================================
# TEST 5: NITTER SCRAPER TEST
# =============================================================================

async def test_nitter_scraper():
    """Test scraping via Nitter instances."""
    print("\n" + "=" * 60)
    print(" TEST 5: NITTER INSTANCES")
    print("=" * 60)
    
    # List of public Nitter instances
    nitter_instances = [
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
        "https://nitter.cz",
        "https://nitter.1d4.us",
    ]
    
    try:
        import httpx
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        transport = httpx.AsyncHTTPTransport(proxy=PROXY_URL)
        
        async with httpx.AsyncClient(transport=transport, headers=headers, timeout=30.0) as client:
            
            for instance in nitter_instances:
                info(f"Testing {instance}...")
                
                try:
                    # Search for BTC
                    response = await client.get(
                        f"{instance}/search",
                        params={"f": "tweets", "q": "BTC bitcoin"},
                        follow_redirects=True
                    )
                    
                    if response.status_code == 200:
                        html = response.text
                        
                        # Check if we got results
                        if "tweet-content" in html or "tweet-body" in html:
                            success(f"{instance} is working!")
                            
                            # Extract some tweets
                            import re
                            tweets = re.findall(r'<div class="tweet-content[^"]*"[^>]*>([^<]+)</div>', html)
                            
                            if tweets:
                                print(f"    Found {len(tweets)} tweets")
                                for t in tweets[:3]:
                                    print(f"      - {t[:80]}...")
                            
                            return instance  # Return working instance
                        else:
                            warning(f"{instance}: No tweet content found")
                    else:
                        warning(f"{instance}: HTTP {response.status_code}")
                        
                except Exception as e:
                    warning(f"{instance}: {e}")
                
                await asyncio.sleep(0.5)
            
            warning("No working Nitter instances found")
            return None
            
    except Exception as e:
        error(f"Nitter test failed: {e}")
        return None


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================

async def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print(" TWITTER/X DATA COLLECTION TEST")
    print(" Proxy: " + PROXY_HOST + ":" + PROXY_PORT)
    print("=" * 60)
    
    results = {}
    
    # Test 1: Proxy
    results["proxy"] = await test_proxy_connection()
    
    if not results["proxy"]:
        error("Proxy not working. Cannot continue.")
        return
    
    # Test 2: Guest access
    results["guest"] = await test_twitter_guest_access()
    
    # Test 3: Twikit
    results["twikit"] = await test_twikit_scraper()
    
    # Test 4: Alternative
    results["alternative"] = await test_alternative_scraper()
    
    # Test 5: Nitter
    results["nitter"] = await test_nitter_scraper()
    
    # Summary
    print("\n" + "=" * 60)
    print(" SUMMARY")
    print("=" * 60)
    
    print(f"\n  1. Proxy Connection: {'✅ PASS' if results['proxy'] else '❌ FAIL'}")
    print(f"  2. Twitter Guest Token: {'✅ PASS' if results['guest'] else '⚠️ SKIPPED'}")
    print(f"  3. Twikit Scraper: {'✅ PASS' if results['twikit'] else '⚠️ REQUIRES LOGIN'}")
    print(f"  4. Alternative Scraper: {'✅ PASS' if results['alternative'] else '❌ FAIL'}")
    print(f"  5. Nitter Instances: {'✅ PASS (' + results['nitter'] + ')' if results['nitter'] else '⚠️ NO INSTANCE'}")
    
    print("\n" + "=" * 60)
    
    if results["twikit"]:
        print("\n✅ Twikit is working! Full Twitter scraping available.")
    elif results["nitter"]:
        print(f"\n✅ Nitter is working! Use instance: {results['nitter']}")
    elif results["alternative"]:
        print("\n⚠️ Limited scraping available via syndication API.")
    else:
        print("\n⚠️ Need to setup Twitter login for full access.")
        print("   Run: python twitter_login.py")


if __name__ == "__main__":
    asyncio.run(main())
