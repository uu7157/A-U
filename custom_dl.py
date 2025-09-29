import math
import os
import logging
from typing import Union
from pyrogram.types import Message
from pyrogram import Client, utils, raw
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calc_chunk_size(length: int) -> int:
    """Calculate optimal chunk size for Telegram (max 512 KB)."""
    suggested = 2 ** max(min(math.ceil(math.log2(length / 1024)), 10), 2) * 1024
    return min(suggested, 512 * 1024)


def offset_fix(offset: int, chunksize: int) -> int:
    """Align offset to chunk size."""
    return offset - (offset % chunksize)


class TGCustomYield:
    def __init__(self, bot: Client):
        """
        bot: your pyrogram Client instance
        """
        self.main_bot = bot

    @staticmethod
    async def generate_file_properties(msg: Message) -> FileId:
        error_message = "This message doesn't contain any downloadable media"
        available_media = ("audio", "document", "photo", "sticker", "animation",
                           "video", "voice", "video_note")

        if isinstance(msg, Message):
            for kind in available_media:
                media = getattr(msg, kind, None)
                if media is not None:
                    break
            else:
                raise ValueError(error_message)
        else:
            media = msg

        if isinstance(media, str):
            file_id_str = media
        else:
            file_id_str = media.file_id

        file_id_obj = FileId.decode(file_id_str)

        # Add attributes to avoid breaks
        setattr(file_id_obj, "file_size", getattr(media, "file_size", 0))
        setattr(file_id_obj, "mime_type", getattr(media, "mime_type", ""))
        setattr(file_id_obj, "file_name", getattr(media, "file_name", ""))

        return file_id_obj

    async def generate_media_session(self, client: Client, msg: Message) -> Session:
        data = await self.generate_file_properties(msg)
        media_session = client.media_sessions.get(data.dc_id, None)

        if media_session is None:
            if data.dc_id != await client.storage.dc_id():
                media_session = Session(
                    client, data.dc_id,
                    await Auth(client, data.dc_id,
                               await client.storage.test_mode()).create(),
                    await client.storage.test_mode(),
                    is_media=True
                )
                await media_session.start()

                for _ in range(3):
                    exported_auth = await client.invoke(
                        raw.functions.auth.ExportAuthorization(dc_id=data.dc_id)
                    )
                    try:
                        await media_session.send(raw.functions.auth.ImportAuthorization(
                            id=exported_auth.id,
                            bytes=exported_auth.bytes
                        ))
                    except AuthBytesInvalid:
                        continue
                    else:
                        break
                else:
                    await media_session.stop()
                    raise AuthBytesInvalid
            else:
                media_session = Session(
                    client, data.dc_id,
                    await client.storage.auth_key(),
                    await client.storage.test_mode(),
                    is_media=True
                )
                await media_session.start()

            client.media_sessions[data.dc_id] = media_session

        return media_session

    @staticmethod
    async def get_location(file_id: FileId):
        file_type = file_id.file_type

        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(user_id=file_id.chat_id,
                                               access_hash=file_id.chat_access_hash)
            else:
                if file_id.chat_access_hash == 0:
                    peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
                else:
                    peer = raw.types.InputPeerChannel(
                        channel_id=utils.get_channel_id(file_id.chat_id),
                        access_hash=file_id.chat_access_hash
                    )
            location = raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG
            )
        elif file_type == FileType.PHOTO:
            location = raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        else:
            location = raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )

        return location

    async def yield_file(self, media_msg: Message, offset: int, part_count: int,
                         chunk_size: int, first_part_cut: int = 0,
                         last_part_cut: int = 0) -> Union[bytes, None]:
        """Yield chunks of a Telegram file."""
        client = self.main_bot
        data = await self.generate_file_properties(media_msg)
        media_session = await self.generate_media_session(client, media_msg)
        location = await self.get_location(data)

        current_part = 1
        try:
            r = await media_session.send(
                raw.functions.upload.GetFile(location=location,
                                             offset=offset,
                                             limit=chunk_size)
            )
        except Exception as e:
            logger.error(f"GetFile failed: {e}")
            return

        if not isinstance(r, raw.types.upload.File):
            logger.error("Telegram did not return a File object")
            return

        while current_part <= part_count:
            chunk = getattr(r, "bytes", None)
            if not chunk:
                logger.warning(f"No chunk received at part {current_part}")
                break

            offset += chunk_size
            if part_count == 1:
                yield chunk[first_part_cut:last_part_cut or None]
                break
            if current_part == 1:
                yield chunk[first_part_cut:]
            elif 1 < current_part < part_count:
                yield chunk
            else:  # last part
                yield chunk[:last_part_cut or None]

            try:
                r = await media_session.send(
                    raw.functions.upload.GetFile(location=location,
                                                 offset=offset,
                                                 limit=chunk_size)
                )
            except Exception as e:
                logger.error(f"GetFile failed at offset {offset}: {e}")
                break

            if not isinstance(r, raw.types.upload.File):
                logger.error(f"Invalid response at offset {offset}")
                break

            current_part += 1

    async def download_to_file(self, media_msg: Message, dest_path: str) -> str:
        """Download full file from Telegram into dest_path."""
        data = await self.generate_file_properties(media_msg)
        total_size = getattr(data, "file_size", 0)
        chunk_size = calc_chunk_size(total_size)
        part_count = (total_size + chunk_size - 1) // chunk_size

        logger.info(f"Downloading {data.file_name} ({total_size} bytes) "
                    f"in {part_count} parts of {chunk_size} bytes")

        downloaded = 0
        with open(dest_path, "wb") as f:
            async for chunk in self.yield_file(
                media_msg=media_msg,
                offset=0,
                part_count=part_count,
                chunk_size=chunk_size
            ):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                logger.info(f"Progress: {downloaded}/{total_size} "
                            f"({downloaded * 100 // total_size if total_size else 0}%)")

        if os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
            logger.info(f"Download complete: {dest_path}")
        else:
            logger.error(f"Download failed, file is empty: {dest_path}")

        return dest_path
