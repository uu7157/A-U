import asyncio
import time
import sys
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor

from uploader import upload_to_abyss  # your existing uploader.py
from custom_dl import TGCustomYield  # DC streaming downloader
from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API

bot = Client("abyss-bot", api_id=APP_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
upload_executor = ThreadPoolExecutor(max_workers=3)


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
    """Safe edit to avoid flooding or exceptions."""
    try:
        await message.edit_text(text)
    except Exception:
        pass


# ----------------------
# Streaming download & upload
# ----------------------
async def stream_telegram_to_abyss(message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text(f"Preparing {file.file_name}...", quote=True)

    try:
        downloader = TGCustomYield(bot)

        total_size = getattr(file, "file_size", 0)
        downloaded = 0
        start_time = time.time()

        # ----------------------
        # Async generator yielding chunks
        # ----------------------
        async def chunk_generator():
            nonlocal downloaded
            async for chunk in downloader.yield_file(
                media_msg=message,
                offset=0,
                first_part_cut=0,
                last_part_cut=0,
                part_count=1,
                chunk_size=4 * 1024 * 1024  # 4 MB chunks
            ):
                downloaded += len(chunk)
                percent = int(downloaded * 100 / total_size) if total_size else 0
                elapsed = max(time.time() - start_time, 0.001)
                speed = downloaded / elapsed
                eta = int((total_size - downloaded) / speed) if downloaded else "-"
                await safe_edit(
                    status,
                    f"Downloading {file.file_name}: {percent}%\n"
                    f"Speed: {speed / 1024 / 1024:.2f} MB/s\nETA: {eta}s"
                )
                yield chunk

        # ----------------------
        # Upload to Abyss
        # ----------------------
        slug = await upload_to_abyss(
            file_generator=chunk_generator(),  # async generator
            api_key=ABYSS_API,
            progress_callback=None  # optional: can add upload progress
        )

        elapsed = int(time.time() - start_time)
        await safe_edit(
            status,
            f"✅ Uploaded!\nhttps://zplayer.io/?v={slug}\nTime: {elapsed}s"
        )

    except Exception as e:
        print("Error streaming file:", e, file=sys.stderr)
        await safe_edit(status, f"❌ Failed: {e}")


# ----------------------
# Message handler
# ----------------------
@bot.on_message(filters.video | filters.document)
async def handle_message(client, message: Message):
    # Each file handled in its own async task
    asyncio.create_task(stream_telegram_to_abyss(message))


# ----------------------
# Run bot
# ----------------------
if __name__ == "__main__":
    bot.run()
