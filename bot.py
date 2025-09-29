import os
import time
from pyrogram import Client, filters
from pyrogram.types import Message

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API
from uploader import upload_to_abyss


bot = Client(
    "abyss-bot",
    api_id=APP_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)


# store last progress update times per message
last_updates = {}


async def update_download_progress(current, total, message: Message):
    now = time.time()
    last_time = last_updates.get(message.id, 0)

    # only update every 5 seconds or on completion
    if now - last_time < 5 and current != total:
        return

    percent = int(current * 100 / total)
    try:
        await message.edit_text(f"Downloading: {percent}%")
    except:
        pass

    last_updates[message.id] = now


@bot.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text("Downloading... 0%")

    # Download file with progress callback
    file_path = await message.download(
        progress=lambda c, t: bot.loop.create_task(
            update_download_progress(c, t, status)
        )
    )

    # Upload part
    try:
        await status.edit_text("Uploading... 0%")
        slug = upload_to_abyss(file_path, ABYSS_API)
        final_url = f"https://zplayer.io/?v={slug}"
        await status.edit_text(f"✅ Uploaded!\n{final_url}")
    except Exception as e:
        await status.edit_text(f"❌ Upload failed: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    bot.run()
