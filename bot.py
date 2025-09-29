import os
import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor
import requests

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API

bot = Client(
    "abyss-bot",
    api_id=APP_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# Executor for blocking uploads
upload_executor = ThreadPoolExecutor(max_workers=3)  # Adjust as needed

# Store per-message progress info
progress_data = {}

# ----------------------
# Utility functions
# ----------------------
def human_readable(size):
    """Convert bytes to human readable format"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024


async def edit_progress(message: Message, tag: str, current, total, last_update):
    now = time.time()
    last_time, last_bytes = last_update.get(message.id, (0, 0))

    # Update every 2s or on completion
    if now - last_time < 2 and current != total:
        return

    speed = 0
    eta = 0
    if last_time > 0:
        diff_bytes = current - last_bytes
        diff_time = now - last_time
        if diff_time > 0:
            speed = diff_bytes / diff_time
            if speed > 0:
                eta = (total - current) / speed

    percent = int(current * 100 / total)
    speed_str = f"{human_readable(speed)}/s" if speed else "0 B/s"
    eta_str = f"{int(eta)}s" if eta else "-"

    try:
        await message.edit_text(
            f"{tag}: {percent}%\n"
            f"Speed: {speed_str}\n"
            f"ETA: {eta_str}"
        )
    except:
        pass

    last_update[message.id] = (now, current)


# ----------------------
# Upload function
# ----------------------
def upload_to_abyss(file_path: str, api_key: str):
    """Blocking upload function"""
    url = f"http://up.hydrax.net/{api_key}"
    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f, "video/mp4")}
        response = requests.post(url, files=files)
        response.raise_for_status()
        data = response.json()
        return data.get("url") or data.get("slug")


# ----------------------
# Handler
# ----------------------
@bot.on_message(filters.video | filters.document)
async def handle_file(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    # Reply to this specific file
    status = await message.reply_text(f"Downloading {file.file_name}... 0%")

    # Initialize progress tracking for this message
    progress_data[status.id] = (0, 0)

    # ----------------------
    # Download with larger chunk size
    # ----------------------
    file_path = await client.download_media(
        file,
        file_name=f"./downloads/{file.file_name}",
        progress=lambda current, total: asyncio.create_task(
            edit_progress(status, "Downloading", current, total, progress_data)
        ),
        chunk_size=1024 * 1024  # 1 MB chunks → faster
    )

    # ----------------------
    # Upload in executor to avoid blocking
    # ----------------------
    progress_data[status.id] = (0, 0)
    await status.edit_text(f"Uploading {file.file_name}... 0%")

    loop = asyncio.get_event_loop()
    start_time = time.time()
    try:
        slug = await loop.run_in_executor(upload_executor, upload_to_abyss, file_path, ABYSS_API)
        elapsed = time.time() - start_time
        size = os.path.getsize(file_path)
        speed = size / elapsed if elapsed > 0 else 0

        final_url = f"https://zplayer.io/?v={slug}"
        await status.edit_text(
            f"✅ Uploaded!\n{final_url}\n"
            f"Upload Speed: {human_readable(speed)}/s\n"
            f"Time: {int(elapsed)}s"
        )
    except Exception as e:
        await status.edit_text(f"❌ Upload failed: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    # Create downloads folder if missing
    if not os.path.exists("./downloads"):
        os.makedirs("./downloads")
    bot.run()
