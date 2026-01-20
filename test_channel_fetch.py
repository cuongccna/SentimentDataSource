#!/usr/bin/env python3
"""Test fetching messages from a specific Telegram channel."""

import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()

api_id = os.getenv('TELEGRAM_API_ID')
api_hash = os.getenv('TELEGRAM_API_HASH')
session = os.getenv('TELEGRAM_SESSION_NAME', 'telegram_session')

CHANNEL_ID = 5874672835

async def test_channel():
    client = TelegramClient(session, api_id, api_hash)
    await client.start()
    
    print(f"Testing channel ID: {CHANNEL_ID}")
    print("=" * 60)
    
    try:
        entity = await client.get_entity(CHANNEL_ID)
        name = entity.title if hasattr(entity, 'title') else entity.first_name
        print(f"✅ Found: {name}")
        print(f"   Type: {type(entity).__name__}")
        
        messages = await client.get_messages(entity, limit=5)
        print(f"\n📝 Got {len(messages)} messages:")
        
        for msg in messages:
            if msg.text:
                text = msg.text[:100].replace('\n', ' ')
                print(f"  [{msg.date}] {text}...")
            else:
                print(f"  [{msg.date}] (media/no text)")
                
    except Exception as e:
        print(f"❌ Error: {e}")
    
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(test_channel())
