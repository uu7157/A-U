import asyncio
import time
import os
import sys
from pyrogram import Client, filters
from pyrogram.types import Message
from concurrent.futures import ThreadPoolExecutor
from uploader import upload_to_abyss
from custom_dl import TGCustomYield
from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API

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

    try:
        downloader = TGCustomYield(bot)

        total_size = getattr(file, "file_size", 0)
        downloaded = 0
        start_time = time.time()

        local_path = f"./downloads/{file.file_name}"

        # Download file using TG DC
        with open(local_path, "wb") as f:
            async for chunk in downloader.yield_file(
                media_msg=message,
                offset=0,
                first_part_cut=0,
                last_part_cut=0,
                part_count=1,
                chunk_size=4 * 1024 * 1024
            ):
                f.write(chunk)
                downloaded += len(chunk)
                percent = int(downloaded * 100 / total_size) if total_size else 0
                elapsed = time.time() - start_time
                speed = human_readable(downloaded / elapsed) if elapsed else "0 B"
                eta = int((total_size - downloaded) / (downloaded / elapsed)) if downloaded else "-"
                await safe_edit(
                    status,
                    f"Downloading {file.file_name}: {percent}%\n"
                    f"Speed: {speed}/s\nETA: {eta}s"
                )

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
        print("Error handling file:", e, file=sys.stderr)
        await safe_edit(status, f"❌ Failed: {e}")

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


@bot.on_message(filters.video | filters.document)
async def handle_message(client, message: Message):
    asyncio.create_task(handle_file(message))


if __name__ == "__main__":
    bot.run()
