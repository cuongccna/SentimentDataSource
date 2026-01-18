"""
Telegram Authentication Script - Chạy 1 lần để xác thực

Script này tạo session file cho Telethon.
Sau khi xác thực thành công, production_worker.py sẽ sử dụng session này
mà không cần nhập OTP mỗi lần.

Usage:
    python auth_telegram.py
"""
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()

# Import Telethon
try:
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError
except ImportError:
    print("ERROR: Telethon not installed. Run: pip install telethon")
    exit(1)

# Credentials from .env
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")

# Session file - same as production_worker uses
SESSION_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "telegram_production_session"
)

async def main():
    print("=" * 60)
    print("Telegram Authentication for Production Worker")
    print("=" * 60)
    print()
    print(f"API ID: {API_ID}")
    print(f"API Hash: {API_HASH[:10]}..." if API_HASH else "API Hash: NOT SET")
    print(f"Phone: {PHONE}")
    print(f"Session File: {SESSION_FILE}")
    print()
    
    if not API_ID or not API_HASH or not PHONE:
        print("ERROR: Missing credentials in .env file!")
        print("Required: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
        return
    
    # Create client
    client = TelegramClient(
        SESSION_FILE,
        int(API_ID),
        API_HASH,
        system_version="4.16.30-vxCUSTOM"
    )
    
    print("Connecting to Telegram...")
    await client.connect()
    
    # Check if already authorized
    if await client.is_user_authorized():
        me = await client.get_me()
        print()
        print("✅ Already authenticated!")
        print(f"   User: {me.first_name} {me.last_name or ''}")
        print(f"   Username: @{me.username}")
        print(f"   Phone: {me.phone}")
        print()
        print("Session file is ready. You can now run production_worker.py")
        await client.disconnect()
        return
    
    # Not authorized - need to login
    print()
    print("Not authenticated. Starting login process...")
    print("You will receive an OTP code on Telegram.")
    print()
    
    await client.send_code_request(PHONE)
    
    code = input("Enter the OTP code you received: ").strip()
    
    try:
        await client.sign_in(PHONE, code)
    except SessionPasswordNeededError:
        # 2FA enabled
        print()
        print("Two-Factor Authentication is enabled.")
        password = input("Enter your 2FA password: ").strip()
        await client.sign_in(password=password)
    
    # Verify success
    me = await client.get_me()
    print()
    print("✅ Authentication successful!")
    print(f"   User: {me.first_name} {me.last_name or ''}")
    print(f"   Username: @{me.username}")
    print()
    print(f"Session saved to: {SESSION_FILE}.session")
    print()
    print("You can now run production_worker.py without entering OTP again!")
    
    # List available channels
    print()
    print("=" * 60)
    print("Your Crypto Channels (to add to telegram_sources table):")
    print("=" * 60)
    
    dialogs = await client.get_dialogs()
    crypto_keywords = ['btc', 'bitcoin', 'crypto', 'trading', 'signal', 'whale', 
                      'defi', 'eth', 'binance', 'alert']
    
    count = 0
    for dialog in dialogs:
        if dialog.is_channel or dialog.is_group:
            name_lower = dialog.name.lower()
            if any(kw in name_lower for kw in crypto_keywords):
                print(f"  - ID: {dialog.id}")
                print(f"    Name: {dialog.name}")
                print()
                count += 1
                if count >= 20:
                    print("  ... (showing first 20)")
                    break
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
