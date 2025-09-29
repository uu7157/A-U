import asyncio
import time
import os
import sys
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor
from uploader import upload_to_abyss
from custom_dl import TGCustomYield
from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Client("abyss-bot", api_id=APP_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
upload_executor = ThreadPoolExecutor(max_workers=3)

os.makedirs("./downloads", exist_ok=True)

# ----------------------
# Utilities
# ----------------------
def human_readable(size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024


async def safe_edit(message: Message, text: str):
    try:
        await message.edit_text(text)
    except Exception:
        pass


# ----------------------
# Download & upload handler
# ----------------------
async def handle_file(message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text(f"Preparing {file.file_name}...", quote=True)

    local_path = f"./downloads/{file.file_name}"
    start_time = time.time()

    try:
        downloader = TGCustomYield(bot)

        # ✅ New download method with logging inside custom_dl
        await safe_edit(status, f"⬇️ Downloading {file.file_name}...")
        await downloader.download_to_file(message, local_path)

        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            raise RuntimeError("Download failed or file is empty")

        await safe_edit(status, f"✅ Download complete! Uploading to Abyss...")

        # Upload using existing uploader in thread pool
        loop = asyncio.get_event_loop()

        def upload_task():
            return upload_to_abyss(local_path, api_key=ABYSS_API)

        slug = await loop.run_in_executor(upload_executor, upload_task)

        elapsed_total = time.time() - start_time
        size = os.path.getsize(local_path)
        avg_speed = human_readable(size / elapsed_total) if elapsed_total else "0 B"

        await safe_edit(
            status,
            f"✅ Uploaded!\nhttps://zplayer.io/?v={slug}\n"
            f"Avg Upload Speed: {avg_speed}/s\n"
            f"Total Time: {int(elapsed_total)}s"
        )

    except Exception as e:
        logger.exception("Error handling file")
        await safe_edit(status, f"❌ Failed: {e}")

    finally:
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass


@bot.on_message(filters.video | filters.document)
async def handle_message(client, message: Message):
    asyncio.create_task(handle_file(message))


if __name__ == "__main__":
    bot.run()
