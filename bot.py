import os
import time
import asyncio
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API
from uploader import upload_to_abyss  # use your existing uploader.py

bot = Client(
    "abyss-bot",
    api_id=APP_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

upload_executor = ThreadPoolExecutor(max_workers=3)
progress_data = {}  # track progress per message

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
# Wrapper for upload with progress
# ----------------------
def upload_with_progress(file_path, api_key, progress_callback=None):
    """
    Calls the existing uploader.py function and updates progress manually.
    """
    # Wrap the file to report read bytes
    class ProgressFile:
        def __init__(self, path):
            self.fp = open(path, "rb")
            self.size = os.path.getsize(path)
            self.read_bytes = 0

        def read(self, n=-1):
            data = self.fp.read(n)
            if data:
                self.read_bytes += len(data)
                if progress_callback:
                    progress_callback(self.read_bytes, self.size)
            return data

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.fp.close()

    # Use ProgressFile but call uploader.py normally
    def progress_report(current, total):
        # call async coroutine from sync thread
        loop = asyncio.get_event_loop()
        asyncio.run_coroutine_threadsafe(
            edit_progress(status_message, "Uploading", current, total, progress_data),
            loop
        )

    return upload_to_abyss(file_path, api_key, progress_callback=progress_report)

# ----------------------
# Handler
# ----------------------
@bot.on_message(filters.video | filters.document)
async def handle_file(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    # Reply to the video/document
    status = await message.reply_text(f"Downloading {file.file_name}... 0%", quote=True)
    file_path = None

    try:
        # Download with Pyrogram default progress
        progress_data[status.id] = (0, 0)
        file_path = await message.download(
            file_name=f"./downloads/{file.file_name}",
            progress=lambda c, t: asyncio.run_coroutine_threadsafe(
                edit_progress(status, "Downloading", c, t, progress_data),
                asyncio.get_event_loop()
            )
        )

        # Upload with live progress
        progress_data[status.id] = (0, 0)
        await status.edit_text(f"Uploading {file.file_name}... 0%")
        loop = asyncio.get_event_loop()
        start_time = time.time()

        # Set status_message for progress callback
        global status_message
        status_message = status

        # Run upload in thread executor
        slug = await loop.run_in_executor(
            upload_executor,
            upload_with_progress,
            file_path,
            ABYSS_API
        )

        elapsed = time.time() - start_time
        size = os.path.getsize(file_path)
        speed = size / elapsed if elapsed > 0 else 0

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
