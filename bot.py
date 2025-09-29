import os
import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API
from uploader import upload_to_abyss  # your existing uploader.py

bot = Client(
    "abyss-bot",
    api_id=APP_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

upload_executor = ThreadPoolExecutor(max_workers=3)
progress_data = {}  # store last update per message for speed/ETA


# ----------------------
# Utilities
# ----------------------
def human_readable(size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024


async def edit_progress(message: Message, tag: str, current, total, last_update):
    now = time.time()
    last_time, last_bytes = last_update.get(message.id, (0, 0))
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
# Safe download progress callback
# ----------------------
def safe_progress_callback(message, current, total):
    try:
        loop = asyncio.get_running_loop()
        asyncio.run_coroutine_threadsafe(
            edit_progress(message, "Downloading", current, total, progress_data),
            loop
        )
    except RuntimeError:
        # ignore if no running loop in thread
        pass


# ----------------------
# Wrapper for upload (runs in executor)
# ----------------------
def upload_file(file_path, status_message):
    start_time = time.time()

    def upload_callback(current, total):
        # We can't safely do live updates from this thread due to asyncio limits
        pass

    slug = upload_to_abyss(file_path, ABYSS_API, progress_callback=upload_callback)
    elapsed = time.time() - start_time
    size = os.path.getsize(file_path)
    speed = size / elapsed if elapsed > 0 else 0
    return slug, elapsed, speed


# ----------------------
# Handler
# ----------------------
@bot.on_message(filters.video | filters.document)
async def handle_file(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    # Reply to this file
    status = await message.reply_text(f"Downloading {file.file_name}... 0%", quote=True)
    file_path = None

    try:
        progress_data[status.id] = (0, 0)
        # Download file safely without crashing on threads
        file_path = await message.download(
            file_name=f"./downloads/{file.file_name}",
            progress=lambda c, t: safe_progress_callback(status, c, t)
        )

        # Upload starts after full download
        await status.edit_text(f"Uploading {file.file_name}...")

        loop = asyncio.get_event_loop()
        # Run upload in executor
        slug, elapsed, speed = await loop.run_in_executor(
            upload_executor, upload_file, file_path, status
        )

        final_url = f"https://zplayer.io/?v={slug}"
        await status.edit_text(
            f"✅ Uploaded!\n{final_url}\n"
            f"Avg Upload Speed: {human_readable(speed)}/s\n"
            f"Time: {int(elapsed)}s"
        )

    except Exception as e:
        await status.edit_text(f"❌ Failed: {e}")
    finally:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)


# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    os.makedirs("./downloads", exist_ok=True)
    bot.run()
