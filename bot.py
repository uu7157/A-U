import asyncio
import time
import sys
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor
from uploader import upload_to_abyss  # your uploader.py
from custom_dl import TGCustomYield  # raw DC downloader

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API

bot = Client("abyss-bot", api_id=APP_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
upload_executor = ThreadPoolExecutor(max_workers=3)

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
# Streaming download & upload
# ----------------------
async def stream_telegram_to_abyss(message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text(f"Preparing {file.file_name}...", quote=True)

    try:
        downloader = TGCustomYield()
        downloader.main_bot = bot

        # total size
        total_size = getattr(file, "file_size", 0)
        downloaded = 0
        uploaded = 0
        start_time = time.time()

        # Generator yielding chunks
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
                speed = human_readable(downloaded / (time.time() - start_time))
                eta = int((total_size - downloaded) / (downloaded / (time.time() - start_time))) if downloaded else "-"
                await safe_edit(
                    status,
                    f"Downloading {file.file_name}: {percent}%\n"
                    f"Speed: {speed}/s\nETA: {eta}s"
                )
                yield chunk

        # Wrapper for streaming upload to Abyss
        def upload_wrapper():
            nonlocal uploaded
            class IterableWrapper:
                def __init__(self, gen):
                    self.gen = gen
                    self.bytes_uploaded = 0

                def __iter__(self):
                    for chunk in asyncio.run(self.gen.__aiter__()):
                        self.bytes_uploaded += len(chunk)
                        uploaded = self.bytes_uploaded
                        yield chunk

            # Call existing uploader.py with streaming wrapper
            # For compatibility, we write a temporary generator-based file-like object
            class StreamFile:
                def __init__(self, gen):
                    self.gen = gen

                def read(self, n=-1):
                    try:
                        return next(self.gen)
                    except StopIteration:
                        return b""

            wrapper = IterableWrapper(chunk_generator())
            return upload_to_abyss(file_path=None, api_key=ABYSS_API, progress_callback=None)  # uploader should accept streaming wrapper

        loop = asyncio.get_event_loop()
        slug = await loop.run_in_executor(upload_executor, upload_wrapper)

        elapsed = time.time() - start_time
        await safe_edit(status, f"✅ Uploaded!\nhttps://zplayer.io/?v={slug}\nTime: {int(elapsed)}s")

    except Exception as e:
        print("Error streaming file:", e, file=sys.stderr)
        await safe_edit(status, f"❌ Failed: {e}")


@bot.on_message(filters.video | filters.document)
async def handle_message(client, message: Message):
    asyncio.create_task(stream_telegram_to_abyss(message))


if __name__ == "__main__":
    bot.run()
