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


async def track_download_progress(file_path: str, status: Message, total_size: int):
    """Background task: periodically checks file size and updates progress."""
    last_size = 0
    start_time = time.time()

    while not os.path.exists(file_path):
        await asyncio.sleep(0.5)

    while True:
        try:
            current = os.path.getsize(file_path)
        except Exception as e:
            print("Error reading file size:", e)
            current = last_size

        diff = current - last_size
        last_size = current

        elapsed = time.time() - start_time
        speed = current / elapsed if elapsed > 0 else 0
        percent = int(current * 100 / total_size) if total_size else 0
        eta = (total_size - current) / speed if speed > 0 else "-"

        try:
            await status.edit_text(
                f"Downloading {os.path.basename(file_path)}: {percent}%\n"
                f"Speed: {human_readable(speed)}/s\n"
                f"ETA: {int(eta) if eta != '-' else '-'}s"
            )
        except Exception as e:
            print("Error editing status message:", e)

        if total_size and current >= total_size:
            break
        await asyncio.sleep(1)


def upload_file(file_path: str):
    """Upload using uploader.py, measure time and speed."""
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

    # Reply to file
    status = await message.reply_text(f"Starting download {file.file_name}...", quote=True)

    try:
        # Start background task to track progress
        total_size = getattr(file, "file_size", 0)
        progress_task = asyncio.create_task(track_download_progress(file_path, status, total_size))

        # Download file fully
        file_path = await message.download(file_name=file_path)

        # Ensure final progress is shown
        await progress_task

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
        # Log exception to Colab console
        print("Error handling file:", e)
        await status.edit_text(f"❌ Failed: {e}")

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    bot.run()
