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


async def update_download_progress(message: Message, file_path: str):
    """Periodically update download progress until file is fully downloaded."""
    total = None
    last_size = 0
    while True:
        if not os.path.exists(file_path):
            await asyncio.sleep(0.5)
            continue
        current = os.path.getsize(file_path)
        if total is None:
            total = getattr(message.video or message.document, "file_size", None) or 0

        speed = (current - last_size) / 1 if last_size else 0
        eta = (total - current) / speed if speed > 0 else "-"
        percent = int(current * 100 / total) if total else 0
        speed_str = human_readable(speed) + "/s"
        eta_str = f"{int(eta)}s" if eta != "-" else "-"

        try:
            await message.edit_text(
                f"Downloading {os.path.basename(file_path)}: {percent}%\n"
                f"Speed: {speed_str}\n"
                f"ETA: {eta_str}"
            )
        except:
            pass

        if total and current >= total:
            break
        last_size = current
        await asyncio.sleep(1)  # update every 1 second


def upload_file(file_path: str):
    """Upload the file using your existing uploader.py and measure speed."""
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

    # Ensure download directory exists
    os.makedirs("./downloads", exist_ok=True)
    file_path = f"./downloads/{file.file_name}"

    # Reply to file
    status = await message.reply_text(f"Downloading {file.file_name}... 0%", quote=True)

    try:
        # Start download in background
        download_task = asyncio.create_task(update_download_progress(status, file_path))
        file_path = await message.download(file_name=file_path)
        await download_task  # wait for final progress update

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
        await status.edit_text(f"❌ Failed: {e}")

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    bot.run()
