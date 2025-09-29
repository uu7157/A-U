import math
from typing import Union
from pyrogram.types import Message
from pyrogram import Client, utils, raw
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource

# Telegram GetFile max chunk limit = 512 KB
TG_MAX_CHUNK = 512 * 1024  # 524288 bytes


async def chunk_size(length: int) -> int:
    """Calculate optimal chunk size for downloading (but clamp later)."""
    return 2 ** max(min(math.ceil(math.log2(length / 1024)), 10), 2) * 1024


async def offset_fix(offset: int, chunksize: int) -> int:
    """Align offset to chunk size."""
    offset -= offset % chunksize
    return offset


class TGCustomYield:
    def __init__(self, bot: Client):
        """
        bot: your pyrogram Client instance
        """
        self.main_bot = bot

    @staticmethod
    async def generate_file_properties(msg: Message) -> FileId:
        error_message = "This message doesn't contain any downloadable media"
        available_media = ("audio", "document", "photo", "sticker", "animation", "video", "voice", "video_note")

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
                    await Auth(client, data.dc_id, await client.storage.test_mode()).create(),
                    await client.storage.test_mode(),
                    is_media=True
                )
                await media_session.start()

                for _ in range(3):
                    exported_auth = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=data.dc_id))
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
                peer = raw.types.InputPeerUser(user_id=file_id.chat_id, access_hash=file_id.chat_access_hash)
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

    async def yield_file(self, media_msg: Message, offset: int, first_part_cut: int,
                         last_part_cut: int, part_count: int, chunk_size: int) -> Union[bytes, None]:
        client = self.main_bot
        data = await self.generate_file_properties(media_msg)
        media_session = await self.generate_media_session(client, media_msg)

        current_part = 1
        location = await self.get_location(data)

        # Clamp chunk_size to Telegram max
        chunk_size = min(chunk_size, TG_MAX_CHUNK)

        r = await media_session.send(raw.functions.upload.GetFile(
            location=location, offset=offset, limit=chunk_size
        ))
        if not isinstance(r, raw.types.upload.File):
            return

        while current_part <= part_count:
            chunk = getattr(r, "bytes", None)
            if not chunk:
                break

            offset += len(chunk)  # safe increment

            if part_count == 1:
                yield chunk[first_part_cut:last_part_cut]
                break
            if current_part == 1:
                yield chunk[first_part_cut:]
            elif 1 < current_part <= part_count:
                yield chunk

            r = await media_session.send(raw.functions.upload.GetFile(
                location=location, offset=offset, limit=chunk_size
            ))
            if not isinstance(r, raw.types.upload.File):
                break

            current_part += 1

    async def download_as_bytesio(self, media_msg: Message):
        """Return full file as a list of bytes chunks."""
        client = self.main_bot
        data = await self.generate_file_properties(media_msg)
        media_session = await self.generate_media_session(client, media_msg)
        location = await self.get_location(data)

        limit = TG_MAX_CHUNK  # enforce max
        offset = 0

        r = await media_session.send(raw.functions.upload.GetFile(location=location, offset=offset, limit=limit))
        if not isinstance(r, raw.types.upload.File):
            return []

        m_file = []
        while True:
            chunk = getattr(r, "bytes", None)
            if not chunk:
                break
            m_file.append(chunk)
            offset += len(chunk)
            r = await media_session.send(raw.functions.upload.GetFile(location=location, offset=offset, limit=limit))
            if not isinstance(r, raw.types.upload.File):
                break
        return m_file
