import os
import json
import asyncio
from datetime import datetime
from telethon.sync import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")

# You can add more channels to this list
CHANNELS = [
    "lobelia4cosmetics",
    "tikvahpharma",
    "CheMed123"
]

async def scrape_channel(channel_username):
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"data/raw/telegram_messages/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{channel_username}.json")

    messages_data = []

    async with TelegramClient("anon", API_ID, API_HASH) as client:
        async for message in client.iter_messages(channel_username, limit=100):  # adjust limit
            msg = {
                "id": message.id,
                "date": str(message.date),
                "text": message.text,
                "sender_id": message.sender_id,
                "has_image": isinstance(message.media, MessageMediaPhoto),
                "image_path": None
            }

            # Save image path if available
            if msg["has_image"]:
                img_file = f"data/raw/images/{date_str}/{channel_username}_{message.id}.jpg"
                os.makedirs(os.path.dirname(img_file), exist_ok=True)
                await message.download_media(img_file)
                msg["image_path"] = img_file

            messages_data.append(msg)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(messages_data, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(messages_data)} messages from {channel_username} to {output_path}")


async def main():
    for channel in CHANNELS:
        try:
            await scrape_channel(channel)
        except Exception as e:
            print(f"Error scraping {channel}: {e}")

if __name__ == "__main__":
    asyncio.run(main())