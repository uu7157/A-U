import asyncio
import time
import sys
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor
from uploader import upload_to_abyss  # your existing uploader.py
from custom_dl import TGCustomYield  # Telegram DC streaming

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

        total_size = getattr(file, "file_size", 0)
        downloaded_bytes = 0
        start_time = time.time()

        # ----------------------
        # Async generator for Telegram chunks
        # ----------------------
        async def chunk_generator():
            nonlocal downloaded_bytes
            async for chunk in downloader.yield_file(
                media_msg=message,
                offset=0,
                first_part_cut=0,
                last_part_cut=0,
                part_count=1,
                chunk_size=4 * 1024 * 1024,  # 4MB chunks
            ):
                downloaded_bytes += len(chunk)
                elapsed = time.time() - start_time
                percent = int(downloaded_bytes * 100 / total_size) if total_size else 0
                speed = human_readable(downloaded_bytes / elapsed) if elapsed > 0 else "0 B"
                eta = int((total_size - downloaded_bytes) / (downloaded_bytes / elapsed)) if downloaded_bytes else "-"
                await safe_edit(
                    status,
                    f"Downloading {file.file_name}: {percent}%\nSpeed: {speed}/s\nETA: {eta}s"
                )
                yield chunk

        # ----------------------
        # Streaming wrapper for uploader.py
        # ----------------------
        class StreamFile:
            def __init__(self, async_gen):
                self.async_gen = async_gen.__aiter__()
                self.buffer = b""

            def read(self, n=-1):
                import asyncio
                loop = asyncio.get_event_loop()
                while n == -1 or len(self.buffer) < n:
                    try:
                        chunk = loop.run_until_complete(self.async_gen.__anext__())
                        self.buffer += chunk
                    except StopAsyncIteration:
                        break
                if n == -1:
                    data, self.buffer = self.buffer, b""
                else:
                    data, self.buffer = self.buffer[:n], self.buffer[n:]
                return data

        # Progress callback for upload
        def progress_callback(current, total):
            percent = int(current * 100 / total) if total else 0
            asyncio.run_coroutine_threadsafe(
                safe_edit(status, f"Uploading {file.file_name}: {percent}%"),
                asyncio.get_event_loop()
            )

        # ----------------------
        # Run upload in ThreadPoolExecutor
        # ----------------------
        loop = asyncio.get_event_loop()
        slug = await loop.run_in_executor(
            upload_executor,
            lambda: upload_to_abyss(StreamFile(chunk_generator()), ABYSS_API, progress_callback)
        )

        elapsed = time.time() - start_time
        await safe_edit(
            status,
            f"✅ Uploaded!\nhttps://zplayer.io/?v={slug}\nTime: {int(elapsed)}s"
        )

    except Exception as e:
        print("Error streaming file:", e, file=sys.stderr)
        await safe_edit(status, f"❌ Failed: {e}")


# ----------------------
# Message handler
# ----------------------
@bot.on_message(filters.video | filters.document)
async def handle_message(client, message: Message):
    asyncio.create_task(stream_telegram_to_abyss(message))


# ----------------------
# Run bot
# ----------------------
if __name__ == "__main__":
    bot.run()
