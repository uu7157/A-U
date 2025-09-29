import os
import time
from pyrogram import Client, filters
from pyrogram.types import Message

from config import APP_ID, API_HASH, BOT_TOKEN, ABYSS_API
from uploader import upload_to_abyss


bot = Client(
    "abyss-bot",
    api_id=APP_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)


# Store last update info per message
progress_data = {}


def human_readable(size):
    """Convert bytes to human readable MB/GB"""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024


async def update_progress(current, total, message: Message, tag: str):
    now = time.time()
    last_time, last_bytes = progress_data.get(message.id, (0, 0))

    # update only every 3s or when finished
    if now - last_time < 3 and current != total:
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

    progress_data[message.id] = (now, current)


@bot.on_message(filters.video | filters.document)
async def handle_video(client, message: Message):
    file = message.video or message.document
    if not file:
        return

    status = await message.reply_text("Downloading... 0%")

    # Reset progress tracking
    progress_data[status.id] = (0, 0)

    # Download with progress
    file_path = await message.download(
        progress=lambda c, t: bot.loop.create_task(
            update_progress(c, t, status, "Downloading")
        )
    )

    # Upload part
    try:
        progress_data[status.id] = (0, 0)
        await status.edit_text("Uploading... 0%")

        # since upload_to_abyss is sync, we can’t hook chunk-speed easily,
        # but we simulate progress before/after request
        start = time.time()
        slug = upload_to_abyss(file_path, ABYSS_API)
        elapsed = time.time() - start

        final_url = f"https://zplayer.io/?v={slug}"
        size = os.path.getsize(file_path)
        speed = size / elapsed if elapsed > 0 else 0

        await status.edit_text(
            f"✅ Uploaded!\n{final_url}\n\n"
            f"Upload Speed: {human_readable(speed)}/s\n"
            f"Time: {int(elapsed)}s"
        )
    except Exception as e:
        await status.edit_text(f"❌ Upload failed: {e}")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    bot.run()
