import asyncio
import time
import os
import sys
import math
import logging
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor

from uploader import upload_to_abyss
from custom_dl import TGCustomYield
from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API

# Logging for Colab (errors / debug)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Client("abyss-bot", api_id=APP_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
upload_executor = ThreadPoolExecutor(max_workers=3)

os.makedirs("./downloads", exist_ok=True)

# ----------------------
# Utilities
# ----------------------
def human_readable(size: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


async def safe_edit(message: Message, text: str):
    try:
        await message.edit_text(text)
    except Exception as e:
        # ignore edit failures (message deleted, flood, etc.)
        logger.debug("safe_edit failed: %s", e)


# ----------------------
# Download & upload handler
# ----------------------
TG_CHUNK_SIZE = 512 * 1024  # 512 KB (Telegram GetFile limit)
UPDATE_INTERVAL = 5  # seconds between Telegram edits

async def handle_file(message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text(f"Preparing {file.file_name}...", quote=True)
    local_path = f"./downloads/{file.file_name}"
    start_time = time.time()

    try:
        downloader = TGCustomYield(bot)

        total_size = getattr(file, "file_size", 0) or 0
        downloaded = 0

        # choose chunk size (clamped to TG limit)
        chunk_size = TG_CHUNK_SIZE

        # compute number of parts; if total_size unknown, use a large fallback
        if total_size > 0:
            part_count = math.ceil(total_size / chunk_size)
        else:
            part_count = 10**9  # fallback - will stop when Telegram returns no more data

        logger.info("Starting download: %s size=%s chunk=%s parts=%s",
                    file.file_name, total_size, chunk_size, part_count)

        last_update = 0

        # Download file using TG DC in multiple parts
        with open(local_path, "wb") as f:
            async for chunk in downloader.yield_file(
                media_msg=message,
                offset=0,
                first_part_cut=0,
                last_part_cut=0,
                part_count=part_count,
                chunk_size=chunk_size
            ):
                if not chunk:
                    # if yield_file returns None/empty chunk, stop
                    logger.warning("Received empty chunk while downloading %s", file.file_name)
                    break

                f.write(chunk)
                downloaded += len(chunk)

                now = time.time()
                # update Telegram every UPDATE_INTERVAL seconds, or when download finished
                if now - last_update >= UPDATE_INTERVAL or (total_size and downloaded >= total_size):
                    percent = int(downloaded * 100 / total_size) if total_size else 0
                    elapsed = now - start_time if now - start_time > 0 else 0.0001
                    speed = human_readable(downloaded / elapsed)
                    eta = int((total_size - downloaded) / (downloaded / elapsed)) if (total_size and downloaded) else "-"
                    await safe_edit(
                        status,
                        f"⬇️ Downloading {file.file_name}: {percent}%\n"
                        f"Speed: {speed}/s\nETA: {eta}s"
                    )
                    last_update = now

        # verify file exists and has data
        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            raise RuntimeError("Download failed or file is empty")

        await safe_edit(status, f"✅ Download complete! Uploading to Abyss...")

        # Upload using existing uploader in thread pool (synchronous)
        loop = asyncio.get_event_loop()

        def upload_task():
            try:
                return upload_to_abyss(local_path, api_key=ABYSS_API)
            except Exception as ex:
                # re-raise to be caught by outer except
                raise

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
        # print full traceback to Colab logs for debugging
        logger.exception("Error handling file")
        await safe_edit(status, f"❌ Failed: {e}")

    finally:
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            logger.exception("Failed to remove local file")


@bot.on_message(filters.video | filters.document)
async def handle_message(client, message: Message):
    # run each file as a background task so multiple files can be processed concurrently
    asyncio.create_task(handle_file(message))


if __name__ == "__main__":
    bot.run()
