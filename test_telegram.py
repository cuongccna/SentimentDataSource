"""
Test Telegram Collection - Xác thực và thu thập tin nhắn
"""
import os
import asyncio
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# Telegram credentials
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")
PHONE = os.getenv("TELEGRAM_PHONE", "")

print(f"API_ID: {API_ID}")
print(f"API_HASH: {API_HASH[:10]}...")
print(f"PHONE: {PHONE}")

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

# Session file will be saved as telegram_session.session
SESSION_FILE = "telegram_session"

async def main():
    print("\n=== Connecting to Telegram ===")
    
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    await client.start(phone=PHONE)
    
    me = await client.get_me()
    print(f"✅ Logged in as: {me.first_name} (@{me.username})")
    
    # Lấy danh sách channels/groups đã tham gia
    print("\n=== Danh sách channels/groups ===")
    dialogs = await client.get_dialogs()
    
    crypto_channels = []
    for dialog in dialogs:
        if dialog.is_channel or dialog.is_group:
            name = dialog.name
            # Lọc channels liên quan crypto
            if any(kw in name.lower() for kw in ['btc', 'bitcoin', 'crypto', 'trading', 'signal', 'whale']):
                crypto_channels.append(dialog)
                print(f"  📢 {name} (ID: {dialog.id})")
    
    if not crypto_channels:
        print("  ⚠️ Không tìm thấy crypto channels. Hiển thị tất cả channels:")
        for i, dialog in enumerate(dialogs[:20]):
            if dialog.is_channel or dialog.is_group:
                print(f"  {i+1}. {dialog.name} (ID: {dialog.id})")
    
    # Thu thập tin nhắn từ channels
    print("\n=== Thu thập tin nhắn ===")
    all_messages = []
    
    channels_to_check = crypto_channels if crypto_channels else [d for d in dialogs if d.is_channel or d.is_group][:5]
    
    for dialog in channels_to_check[:5]:  # Giới hạn 5 channels
        try:
            history = await client(GetHistoryRequest(
                peer=dialog.entity,
                limit=10,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0
            ))
            
            print(f"\n📢 {dialog.name}: {len(history.messages)} tin nhắn")
            
            for msg in history.messages:
                if msg.message:  # Chỉ lấy tin có text
                    text = msg.message[:100] + "..." if len(msg.message) > 100 else msg.message
                    print(f"  - [{msg.date.strftime('%Y-%m-%d %H:%M')}] {text}")
                    
                    all_messages.append({
                        "message_id": f"tg_{dialog.id}_{msg.id}",
                        "source": "telegram",
                        "source_id": str(dialog.id),
                        "source_name": dialog.name,
                        "text": msg.message,
                        "author": getattr(msg.from_id, 'user_id', None) if msg.from_id else None,
                        "timestamp": msg.date.isoformat(),
                        "views": getattr(msg, 'views', 0),
                    })
                    
        except Exception as e:
            print(f"  ❌ Lỗi: {e}")
    
    print(f"\n=== Tổng cộng: {len(all_messages)} tin nhắn ===")
    
    # Lưu vào database
    if all_messages:
        print("\n=== Lưu vào PostgreSQL ===")
        await save_to_database(all_messages)
    
    await client.disconnect()

async def save_to_database(messages):
    """Lưu tin nhắn vào database"""
    import psycopg2
    import hashlib
    
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
        user=os.getenv("POSTGRES_USER", "sentiment_user"),
        password=os.getenv("POSTGRES_PASSWORD", "")
    )
    
    cursor = conn.cursor()
    
    saved = 0
    for msg in messages:
        try:
            # Tạo fingerprint từ text
            fingerprint = hashlib.md5(msg["text"].encode()).hexdigest()
            
            cursor.execute("""
                INSERT INTO ingested_messages 
                (message_id, source, source_id, source_name, text, event_time, fingerprint, source_reliability, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fingerprint) DO NOTHING
            """, (
                msg["message_id"],
                msg["source"],
                None,  # source_id is integer, set to None
                msg["source_name"],
                msg["text"],
                msg["timestamp"],
                fingerprint,
                0.8,  # default reliability
                f'{{"channel_id": "{msg["source_id"]}", "views": {msg.get("views", 0)}}}'
            ))
            if cursor.rowcount > 0:
                saved += 1
        except Exception as e:
            print(f"  ⚠️ Lỗi lưu: {e}")
            conn.rollback()
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Đã lưu {saved} tin nhắn mới vào database")
    
    # Kiểm tra kết quả
    print("\n=== Kiểm tra database ===")
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DATABASE", "sentiment_db"),
        user=os.getenv("POSTGRES_USER", "sentiment_user"),
        password=os.getenv("POSTGRES_PASSWORD", "")
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM ingested_messages WHERE source = 'telegram'")
    count = cursor.fetchone()[0]
    print(f"📊 Tổng tin nhắn Telegram trong DB: {count}")
    
    cursor.execute("""
        SELECT message_id, source_name, LEFT(text, 60), event_time 
        FROM ingested_messages 
        WHERE source = 'telegram' 
        ORDER BY created_at DESC 
        LIMIT 5
    """)
    
    print("\n📝 5 tin nhắn mới nhất:")
    for row in cursor.fetchall():
        print(f"  - [{row[1]}] {row[2]}...")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())
