import os
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation
from pyrogram.errors import FloodWait

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API
from uploader import upload_to_abyss

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

def safe_asyncio_task(coro):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop()
    return asyncio.run_coroutine_threadsafe(coro, loop)

async def edit_download_progress(message: Message, tag: str, current: int, total: int, start_time: float, last_update: dict):
    now = time.time()
    last_time, last_bytes = last_update.get(message.id, (start_time, 0))
    if now - last_time < 1 and current != total:
        return
    diff_bytes = current - last_bytes
    diff_time = now - last_time
    speed = diff_bytes / diff_time if diff_time > 0 else 0
    eta = (total - current) / speed if speed > 0 else "-"
    percent = int(current * 100 / total) if total else 0
    try:
        await message.edit_text(
            f"{tag}: {percent}%\n"
            f"Downloaded: {human_readable(current)} / {human_readable(total)}\n"
            f"Speed: {human_readable(speed)}/s\n"
            f"ETA: {int(eta) if eta != '-' else '-'}s"
        )
    except Exception as e:
        print("Failed to edit download progress:", e)
    last_update[message.id] = (now, current)

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
# Fast multi-part download
# ----------------------
async def fast_download(client, message, file_path, parts=3, progress_callback=None):
    media = message.document or message.video
    size = media.file_size
    block_size = 512 * 1024  # 512 KB per chunk

    # Get raw document/video object for MTProto
    if message.document:
        raw_doc = message.document._raw
    else:
        raw_doc = message.video._raw

    file_location = InputDocumentFileLocation(
        id=raw_doc.id,
        access_hash=raw_doc.access_hash,
        file_reference=raw_doc.file_reference,
        thumb_size=""
    )

    # Prepare per-part ranges
    part_size = size // parts
    part_ranges = [(i * part_size, size if i == parts-1 else (i+1) * part_size) for i in range(parts)]
    bytes_downloaded = [0] * parts

    async def download_part(idx, start, stop):
        offset = start
        with open(f"{file_path}.part{idx}", "wb") as f:
            while offset < stop:
                limit = min(block_size, stop - offset)
                try:
                    r = await client.invoke(GetFile(location=file_location, offset=offset, limit=limit))
                except FloodWait as e:
                    await asyncio.sleep(e.x)
                    continue
                f.write(r.bytes)
                offset += len(r.bytes)
                bytes_downloaded[idx] = offset - start
                if progress_callback:
                    total_downloaded = sum(bytes_downloaded)
                    await progress_callback(total_downloaded, size)

    # Run all parts in parallel
    await asyncio.gather(*[download_part(i, s, e) for i, (s, e) in enumerate(part_ranges)])

    # Combine parts
    with open(file_path, "wb") as final:
        for i in range(parts):
            with open(f"{file_path}.part{i}", "rb") as part_file:
                final.write(part_file.read())
            os.remove(f"{file_path}.part{i}")

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
    status = await message.reply_text(f"Starting download {file.file_name}...", quote=True)

    last_update = {}
    start_time = time.time()

    try:
        # ------------------
        # Download with live progress
        # ------------------
        async def progress_callback(current, total):
            await edit_download_progress(status, "Downloading", current, total, start_time, last_update)

        await fast_download(client, message, file_path, parts=3, progress_callback=progress_callback)
        await status.edit_text(f"✅ Downloaded {file.file_name} ({human_readable(os.path.getsize(file_path))})")

        # ------------------
        # Upload
        # ------------------
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
# Run
# ----------------------
if __name__ == "__main__":
    bot.run()
