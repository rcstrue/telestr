"""
Torrent downloader using libtorrent.
Downloads to /tmp, uploads to Telegram via Pyrogram, then deletes local file.
All heavy I/O (file upload + streaming) goes through Telegram CDN.
"""

import os
import re
import time
import logging
import asyncio
import libtorrent as lt
from datetime import datetime

import db

logger = logging.getLogger(__name__)
TMP_DIR = "/tmp/torrents"
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB max per file (Render disk limit)

os.makedirs(TMP_DIR, exist_ok=True)


def parse_media_info(filename: str) -> dict:
    """Extract media type, season, episode, year, quality from filename."""
    info = {"media_type": "movie", "season": None, "episode": None, "year": None}
    name = filename

    # Try S01E04 or S1E4 pattern (TV show)
    m = re.search(r'[.\s_-]?S(\d{1,2})E(\d{1,3})', name, re.IGNORECASE)
    if m:
        info["media_type"] = "series"
        info["season"] = int(m.group(1))
        info["episode"] = int(m.group(2))
        name = name[:m.start()] + name[m.end():]
    else:
        # Try just E01-E13 range pattern
        m = re.search(r'[.\s_-]?(?:E|Ep|Episode)\s*(\d{1,3})\s*[-~to&+]+\s*(\d{1,3})', name, re.IGNORECASE)
        if m:
            info["media_type"] = "series"
            info["episode"] = int(m.group(1))  # store start of range
            name = name[:m.start()] + name[m.end():]

    # Extract year
    m = re.search(r'[.\s_-]?(\d{4})[.\s_-]', name)
    if m:
        y = int(m.group(1))
        if 1900 <= y <= 2030:
            info["year"] = y
            name = name[:m.start()] + name[m.end():]

    # Clean up name
    clean = re.sub(r'[.\s_-]+', ' ', name).strip()
    clean = re.sub(r'\b(\d{3,4}p|web[\-]?dl|blu[\-]?ray|webrip|hevc|x26[45]|aac|ac3|ddp|remux|hdtv|proper|repack|extended|unrated)\b', '', clean, flags=re.IGNORECASE)
    clean = re.sub(r'\s+', ' ', clean).strip()
    clean = re.sub(r'\([^)]*\)', '', clean).strip()
    info["clean_name"] = clean
    return info


class TorrentDownloader:
    def __init__(self):
        self.active = {}  # download_id -> {'session', 'handle', 'params'}

    def _make_session(self):
        ses = lt.session()
        ses.listen_on(6881, 6891)
        return ses

    async def download_and_upload(
        self,
        magnet: str,
        download_id: str,
        tg_upload_func,
        channel_id: int,
    ):
        """
        Full pipeline: download torrent -> upload to Telegram -> cleanup.
        tg_upload_func: async callable(file_path, caption) -> message
        """
        try:
            await db.update_download(download_id, status="downloading", progress=0)
            file_path, file_size = await self._download_torrent(magnet, download_id)

            if file_size > MAX_FILE_SIZE:
                await db.update_download(
                    download_id,
                    status="failed",
                    error=f"File too large ({file_size / (1024*1024):.0f} MB). Max is {MAX_FILE_SIZE // (1024*1024)} MB on this plan.",
                )
                try:
                    os.remove(file_path)
                except OSError:
                    pass
                return None

            await db.update_download(
                download_id, status="uploading", progress=95, file_name=os.path.basename(file_path)
            )

            caption = os.path.basename(file_path)
            msg = await tg_upload_func(channel_id, file_path, caption)

            # Store in database
            file_name = os.path.basename(file_path)
            media_info = parse_media_info(file_name)
            await db.add_file(
                name=file_name,
                size=file_size,
                channel_id=channel_id,
                message_id=msg.id,
                media_type=media_info["media_type"],
                season=media_info.get("season"),
                episode=media_info.get("episode"),
                year=media_info.get("year"),
            )

            await db.update_download(download_id, status="completed", progress=100)

            # Cleanup
            try:
                os.remove(file_path)
            except OSError:
                pass

            return {"name": file_name, "message_id": msg.id, "channel_id": channel_id}

        except Exception as e:
            logger.exception(f"Download failed for {download_id}")
            await db.update_download(download_id, status="failed", error=str(e))
            return None

    async def _download_torrent(self, magnet: str, download_id: str):
        """Download torrent using libtorrent. Returns (file_path, file_size)."""
        loop = asyncio.get_event_loop()

        def _sync_download():
            ses = self._make_session()
            params = {
                'save_path': TMP_DIR,
                'storage_mode': lt.storage_mode_t(2),  # sparse
            }
            handle = lt.add_magnet_uri(ses, magnet, params)

            # Wait for metadata
            logger.info(f"[{download_id}] Getting torrent metadata...")
            for _ in range(120):  # 2 min timeout for metadata
                if handle.has_metadata():
                    break
                time.sleep(1)
            else:
                raise TimeoutError("Could not get torrent metadata in 2 minutes")

            # Find the largest video file
            torrent_info = handle.torrent_file()
            files = torrent_info.files()
            video_exts = {'.mkv', '.mp4', '.avi', '.webm', '.mov', '.wmv', '.flv'}
            largest_idx = -1
            largest_size = 0

            def _get_file_info(files_obj, idx):
                """Get (path, size) for file at idx, compatible with multiple libtorrent versions."""
                try:
                    # libtorrent 2.x pip package
                    path = files_obj.file_path(idx)
                    size = files_obj.file_size(idx)
                    return path, size
                except AttributeError:
                    pass
                try:
                    # Some system builds use .at(i)
                    fe = files_obj.at(idx)
                    return str(fe.path), int(fe.size)
                except AttributeError:
                    pass
                try:
                    # Fallback: direct indexing
                    fe = files_obj[idx]
                    return str(fe.path), int(fe.size)
                except (AttributeError, TypeError):
                    pass
                # Last resort: torrent_info.file_at()
                fe = torrent_info.file_at(idx)
                return str(fe.path), int(fe.size)

            num_files = files.num_files()
            for i in range(num_files):
                fname, fsize = _get_file_info(files, i)
                ext = os.path.splitext(fname)[1].lower()
                if ext in video_exts and fsize > largest_size:
                    largest_size = fsize
                    largest_idx = i

            if largest_idx == -1:
                # No video file found, pick the largest file
                for i in range(num_files):
                    _, fsize = _get_file_info(files, i)
                    if fsize > largest_size:
                        largest_size = fsize
                        largest_idx = i

            target_name, _ = _get_file_info(files, largest_idx)
            target_path = os.path.join(TMP_DIR, os.path.basename(target_name))

            # Prioritize only the largest file
            handle.prioritize_files([0] * files.num_files())
            handle.file_priority(largest_idx, 7)
            handle.resume()

            logger.info(f"[{download_id}] Downloading: {target_name} ({largest_size / (1024*1024):.1f} MB)")

            # Download loop
            last_progress = 0
            while not handle.is_seed():
                status = handle.status()
                progress = status.progress * 100
                dl_speed = status.download_rate / 1024

                if abs(progress - last_progress) > 1:
                    last_progress = progress
                    logger.info(
                        f"[{download_id}] {progress:.1f}% - "
                        f"{dl_speed:.0f} KB/s - "
                        f"{status.num_peers} peers"
                    )
                    # Update DB (fire and forget, sync in thread)
                    try:
                        asyncio.get_event_loop().create_task(
                            db.update_download(download_id, progress=progress, file_name=os.path.basename(target_name))
                        )
                    except RuntimeError:
                        pass

                time.sleep(3)

            ses.pause()
            final_path = os.path.join(TMP_DIR, target_name)
            if not os.path.exists(final_path):
                # libtorrent might have saved with full path structure
                for root, dirs, filenames in os.walk(TMP_DIR):
                    for fn in filenames:
                        if fn == os.path.basename(target_name):
                            final_path = os.path.join(root, fn)
                            break

            return final_path, largest_size

        # Run libtorrent in a thread (it's synchronous)
        return await loop.run_in_executor(None, _sync_download)

    def get_status(self):
        return list(self.active.keys())


downloader = TorrentDownloader()