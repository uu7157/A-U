import os
import asyncio
import time
import shutil
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


async def progress(current, total, message: Message, action: str, start_time):
    now = time.time()
    if now - start_time < 5:  # update only every 5s
        return

    percent = int(current * 100 / total)
    try:
        await message.edit_text(f"{action}: {percent}%")
    except:
        pass

    start_time = now
    return start_time


@bot.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text("Downloading... 0%")
    start_time = time.time()

    file_path = await message.download(
        progress=lambda c, t: asyncio.create_task(
            update_download_progress(c, t, status, start_time)
        )
    )

    # Upload part
    start_time = time.time()
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


async def update_download_progress(current, total, message, last_update):
    now = time.time()
    if now - last_update < 5:
        return
    percent = int(current * 100 / total)
    try:
        await message.edit_text(f"Downloading: {percent}%")
    except:
        pass
    last_update = now


if __name__ == "__main__":
    bot.run()
