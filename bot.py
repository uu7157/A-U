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


# ----------------------
# Utilities
# ----------------------
def human_readable(size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024


async def edit_progress(message: Message, tag: str, current: int, total: int):
    """Edit message with progress, speed, and ETA."""
    percent = int(current * 100 / total) if total else 0
    speed = human_readable(current / 1)  # rough instant speed (simplified)
    eta = int((total - current) / (current / 1)) if current > 0 else "-"
    try:
        await message.edit_text(
            f"{tag}: {percent}%\n"
            f"Downloaded: {human_readable(current)} / {human_readable(total)}\n"
            f"Speed: {speed}/s\n"
            f"ETA: {eta}s"
        )
    except Exception as e:
        print("Failed to update progress:", e)


def safe_asyncio_task(coro):
    """Schedule an async coroutine safely from a thread."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop in this thread
        loop = asyncio.get_event_loop()
    return asyncio.run_coroutine_threadsafe(coro, loop)


# ----------------------
# Upload function
# ----------------------
def upload_file(file_path: str):
    start_time = time.time()
    slug = upload_to_abyss(file_path, ABYSS_API)
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

    os.makedirs("./downloads", exist_ok=True)
    file_path = f"./downloads/{file.file_name}"

    # Reply to the file
    status = await message.reply_text(f"Starting download {file.file_name}...", quote=True)

    try:
        total_size = getattr(file, "file_size", 0)

        # Download with Pyrogram progress callback
        file_path = await message.download(
            file_name=file_path,
            progress=lambda current, total: safe_asyncio_task(
                edit_progress(status, "Downloading", current, total)
            )
        )

        # Ensure final download info
        await status.edit_text(f"✅ Downloaded {file.file_name} ({human_readable(os.path.getsize(file_path))})")

        # Upload
        await status.edit_text(f"Uploading {file.file_name}...")
        loop = asyncio.get_event_loop()
        slug, elapsed, speed = await loop.run_in_executor(upload_executor, upload_file, file_path)

        final_url = f"https://zplayer.io/?v={slug}"
        await status.edit_text(
            f"✅ Uploaded!\n{final_url}\n"
            f"Avg Upload Speed: {human_readable(speed)}/s\n"
            f"Time: {int(elapsed)}s"
        )

    except Exception as e:
        print("Error handling file:", e)
        await status.edit_text(f"❌ Failed: {e}")

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


# ----------------------
# Run the bot
# ----------------------
if __name__ == "__main__":
    bot.run()
