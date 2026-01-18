"""
Twitter/X Login Script for Twikit.

This script handles Twitter authentication and saves cookies
for reuse in data collection.

IMPORTANT: Twitter requires login for search functionality.
Run this once to authenticate, then cookies will be saved.
"""

import os
import asyncio
from datetime import datetime, timezone
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


def success(msg: str):
    _log("✅ SUCCESS", msg)


# Proxy configuration
PROXY_HOST = os.getenv("TWITTER_PROXY_HOST", "118.70.121.236")
PROXY_PORT = os.getenv("TWITTER_PROXY_PORT", "50099")
PROXY_USER = os.getenv("TWITTER_PROXY_USER", "muaproxy694c57898a312")
PROXY_PASS = os.getenv("TWITTER_PROXY_PASS", "fsejfbalvpjschjc")
PROXY_TYPE = os.getenv("TWITTER_PROXY_TYPE", "socks5")
PROXY_URL = f"{PROXY_TYPE}://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"

# Twitter credentials (from env or prompt)
TWITTER_USERNAME = os.getenv("TWITTER_USERNAME", "")
TWITTER_PASSWORD = os.getenv("TWITTER_PASSWORD", "")
TWITTER_EMAIL = os.getenv("TWITTER_EMAIL", "")

COOKIES_FILE = "twitter_cookies.json"


async def login_with_credentials(username: str, password: str, email: str = None):
    """Login to Twitter with username/password."""
    try:
        from twikit import Client
        
        info("Creating twikit client...")
        client = Client("en-US")
        
        # Set proxy
        client.http.proxies = {
            "http://": PROXY_URL,
            "https://": PROXY_URL,
        }
        
        info(f"Logging in as @{username}...")
        
        # Login
        await client.login(
            auth_info_1=username,
            auth_info_2=email,
            password=password
        )
        
        success("Login successful!")
        
        # Save cookies
        client.save_cookies(COOKIES_FILE)
        success(f"Cookies saved to {COOKIES_FILE}")
        
        # Verify by getting user info
        me = await client.user()
        print(f"\n  Logged in as: @{me.screen_name}")
        print(f"  Name: {me.name}")
        print(f"  Followers: {me.followers_count}")
        
        return client
        
    except Exception as e:
        error(f"Login failed: {e}")
        import traceback
        traceback.print_exc()
        return None


async def login_interactive():
    """Interactive login flow."""
    print("\n" + "=" * 60)
    print(" TWITTER/X LOGIN")
    print("=" * 60)
    
    print("\nTwitter requires authentication for search functionality.")
    print("Your credentials will be used once to generate cookies.")
    print("Cookies are saved locally and reused for future sessions.")
    print("\nProxy: " + PROXY_HOST + ":" + PROXY_PORT)
    
    # Get credentials
    if TWITTER_USERNAME and TWITTER_PASSWORD:
        info("Using credentials from environment variables")
        username = TWITTER_USERNAME
        password = TWITTER_PASSWORD
        email = TWITTER_EMAIL
    else:
        print("\nEnter your Twitter credentials:")
        username = input("  Username or Email: ").strip()
        password = input("  Password: ").strip()
        email = input("  Email (for 2FA, optional): ").strip() or None
    
    if not username or not password:
        error("Username and password are required")
        return None
    
    return await login_with_credentials(username, password, email)


async def test_logged_in_session():
    """Test if saved cookies work."""
    print("\n" + "=" * 60)
    print(" TESTING SAVED SESSION")
    print("=" * 60)
    
    if not os.path.exists(COOKIES_FILE):
        error(f"No cookies file found: {COOKIES_FILE}")
        return False
    
    try:
        from twikit import Client
        
        client = Client("en-US")
        
        # Set proxy
        client.http.proxies = {
            "http://": PROXY_URL,
            "https://": PROXY_URL,
        }
        
        # Load cookies
        info(f"Loading cookies from {COOKIES_FILE}...")
        client.load_cookies(COOKIES_FILE)
        
        # Test by searching
        info("Testing search...")
        tweets = await client.search_tweet("BTC", "Latest", count=5)
        
        if tweets:
            success(f"Session valid! Found {len(tweets)} tweets")
            
            print("\nSample tweets:")
            for tweet in tweets[:3]:
                print(f"  @{tweet.user.screen_name}: {tweet.text[:60]}...")
            
            return True
        else:
            warning("Search returned no results")
            return False
            
    except Exception as e:
        error(f"Session test failed: {e}")
        return False


async def main():
    """Main function."""
    print("\n" + "=" * 60)
    print(" TWITTER/X AUTHENTICATION SETUP")
    print("=" * 60)
    
    # Check if we already have valid cookies
    if os.path.exists(COOKIES_FILE):
        info(f"Found existing cookies: {COOKIES_FILE}")
        
        choice = input("\nTest existing session? [Y/n]: ").strip().lower()
        
        if choice != "n":
            valid = await test_logged_in_session()
            
            if valid:
                print("\n✅ Existing session is valid. Ready to scrape!")
                return
            else:
                print("\n⚠️ Existing session expired. Need to re-login.")
    
    # Login flow
    await login_interactive()
    
    print("\n" + "=" * 60)
    print(" DONE")
    print("=" * 60)
    print("\nYou can now run test_twitter_realtime.py to test data collection.")


if __name__ == "__main__":
    asyncio.run(main())
