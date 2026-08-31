"""
SQLite database for storing file metadata.
Runs entirely in-memory / on ephemeral disk.
On restart, re-scans the Telegram channel to rebuild the catalog.
"""

import aiosqlite
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
DB_PATH = os.environ.get("DB_PATH", "/tmp/tgstremio.db")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                size INTEGER DEFAULT 0,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                imdb_id TEXT,
                tmdb_id INTEGER,
                media_type TEXT DEFAULT 'movie',
                season INTEGER,
                episode INTEGER,
                poster_url TEXT,
                description TEXT,
                year INTEGER,
                added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS downloads (
                id TEXT PRIMARY KEY,
                magnet TEXT NOT NULL,
                status TEXT DEFAULT 'queued',
                progress REAL DEFAULT 0,
                file_name TEXT,
                error TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS webdav_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS webdav_files (
                sid TEXT PRIMARY KEY,
                account_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                name TEXT NOT NULL,
                webdav_base TEXT NOT NULL,
                content_type TEXT DEFAULT '',
                size INTEGER DEFAULT 0,
                added_at TEXT DEFAULT (datetime('now'))
            );
        """)
        await db.commit()
    logger.info(f"Database initialized at {DB_PATH}")


async def add_file(
    name: str,
    size: int,
    channel_id: int,
    message_id: int,
    media_type: str = "movie",
    imdb_id: str = None,
    tmdb_id: int = None,
    season: int = None,
    episode: int = None,
    poster_url: str = None,
    description: str = None,
    year: int = None,
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """INSERT INTO files
            (name, size, channel_id, message_id, media_type, imdb_id,
             tmdb_id, season, episode, poster_url, description, year)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, size, channel_id, message_id, media_type, imdb_id,
             tmdb_id, season, episode, poster_url, description, year),
        )
        await db.commit()
        return cursor.lastrowid


async def get_all_files():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM files ORDER BY added_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_files_by_type(media_type: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM files WHERE media_type = ? ORDER BY added_at DESC",
            (media_type,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_by_imdb_id(imdb_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM files WHERE imdb_id = ?", (imdb_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_by_id(file_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM files WHERE id = ?", (file_id,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def delete_file(file_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM files WHERE id = ?", (file_id,))
        await db.commit()


async def get_file_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM files")
        row = await cursor.fetchone()
        return row[0]


# --- Download tracking ---

async def create_download(download_id: str, magnet: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO downloads (id, magnet) VALUES (?, ?)",
            (download_id, magnet),
        )
        await db.commit()


async def update_download(
    download_id: str,
    status: str = None,
    progress: float = None,
    file_name: str = None,
    error: str = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        parts = []
        vals = []
        if status is not None:
            parts.append("status = ?")
            vals.append(status)
        if progress is not None:
            parts.append("progress = ?")
            vals.append(progress)
        if file_name is not None:
            parts.append("file_name = ?")
            vals.append(file_name)
        if error is not None:
            parts.append("error = ?")
            vals.append(error)
        vals.append(download_id)
        await db.execute(
            f"UPDATE downloads SET {', '.join(parts)} WHERE id = ?", vals
        )
        await db.commit()


async def get_downloads():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM downloads ORDER BY created_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_active_downloads():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM downloads WHERE status IN ('downloading', 'uploading')"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


# --- WebDAV account management ---

async def add_webdav_account(name: str, url: str, username: str, password: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO webdav_accounts (name, url, username, password) VALUES (?, ?, ?, ?)",
            (name, url, username, password),
        )
        await db.commit()
        return cursor.lastrowid


async def get_webdav_accounts():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT id, name, url, username, added_at FROM webdav_accounts ORDER BY added_at DESC")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_webdav_account(account_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM webdav_accounts WHERE id = ?", (account_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def delete_webdav_account(account_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM webdav_accounts WHERE id = ?", (account_id,))
        await db.execute("DELETE FROM webdav_files WHERE account_id = ?", (account_id,))
        await db.commit()


# --- WebDAV file mapping (persistent sid -> file info) ---

async def upsert_webdav_file(sid: str, account_id: int, path: str, name: str,
                              webdav_base: str, content_type: str = "", size: int = 0):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO webdav_files (sid, account_id, path, name, webdav_base, content_type, size)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (sid, account_id, path, name, webdav_base, content_type, size))
        await db.commit()


async def get_webdav_file(sid: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM webdav_files WHERE sid = ?", (sid,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_all_webdav_files():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM webdav_files ORDER BY name")
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def get_webdav_file_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM webdav_files")
        row = await cursor.fetchone()
        return row[0]


async def clear_webdav_files(account_id: int = None):
    async with aiosqlite.connect(DB_PATH) as db:
        if account_id:
            await db.execute("DELETE FROM webdav_files WHERE account_id = ?", (account_id,))
        else:
            await db.execute("DELETE FROM webdav_files")
        await db.commit()


async def webdav_account_exists(username: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM webdav_accounts WHERE username = ?", (username,))
        row = await cursor.fetchone()
        return row is not None
