"""FastAPI app: Telegram bot webhook, Stremio addon, streaming proxy, dashboard API.
Render only serves metadata and lightweight API calls.
All heavy file data (storage + streaming) lives on Telegram CDN.
"""

import os
import uuid
import logging
import asyncio
import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pyrogram import Client

import db
from downloader import downloader, parse_media_info
from webdav_client import list_video_files, stream_webdav_file, test_connection as webdav_test

import time as _time
import uuid as _uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------- Config ----------
APP_ID = int(os.environ["API_ID"])
APP_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ["SESSION_STRING"]
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
BASE_URL = os.environ.get("BASE_URL", "")
PORT = int(os.environ.get("PORT", "8000"))
# ---------- Default Torrin accounts ----------
DEFAULT_TORRIN_ACCOUNTS = [
    {"name": "Torrin 1", "url": "https://webdav.torrin.app/", "username": "tr_UUnleDf05nJsa_G8", "password": "tr_UUnleDf05nJsa_G8"},
    {"name": "Torrin 2", "url": "https://webdav.torrin.app/", "username": "tr_rWarwVR64dFD9z11", "password": "tr_rWarwVR64dFD9z11"},
]

ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin")

# ---------- Pyrogram clients ----------
# Userbot for uploading files + generating stream links
user_client: Client = None
# Bot for receiving magnet links
from pyrogram import Client as BotClient
bot_client: BotClient = None


async def startup_tg_clients():
    global user_client, bot_client

    user_client = Client(
        name="userbot",
        api_id=APP_ID,
        api_hash=APP_HASH,
        session_string=SESSION_STRING,
    )
    await user_client.start()
    logger.info("Userbot connected")

    bot_client = BotClient(
        name="bot",
        api_id=APP_ID,
        api_hash=APP_HASH,
        bot_token=BOT_TOKEN,
    )
    await bot_client.start()
    logger.info("Bot connected")

    # Set webhook via Bot API (Pyrogram doesn't have set_webhook)
    if BASE_URL:
        webhook_url = f"{BASE_URL}/webhook"
        async with httpx.AsyncClient() as hc:
            await hc.get(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook", params={"url": webhook_url})
        logger.info(f"Webhook set to {webhook_url}")

    # Scan existing channel messages on first boot to rebuild catalog
    await rescan_channel()


async def rescan_channel():
    """Re-scan Telegram channel to rebuild file catalog from stored messages."""
    try:
        count = await db.get_file_count()
        if count > 0:
            logger.info(f"DB already has {count} files, skipping rescan")
            return

        logger.info("Scanning channel for existing files...")
        async for msg in user_client.get_chat_history(CHANNEL_ID):
            if not msg.document and not msg.video:
                continue

            file_name = (msg.document or msg.video).file_name or f"file_{msg.id}"
            file_size = (msg.document or msg.video).file_size or 0
            media_info = parse_media_info(file_name)

            await db.add_file(
                name=file_name,
                size=file_size,
                channel_id=CHANNEL_ID,
                message_id=msg.id,
                media_type=media_info["media_type"],
                season=media_info.get("season"),
                episode=media_info.get("episode"),
                year=media_info.get("year"),
            )

        total = await db.get_file_count()
        logger.info(f"Channel scan complete: {total} files found")
    except Exception as e:
        logger.error(f"Channel rescan failed: {e}")


async def shutdown_tg_clients():
    global user_client, bot_client
    if user_client:
        await user_client.stop()
    if bot_client:
        await bot_client.stop()


# ---------- Lifespan ----------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()

    # Auto-add default Torrin accounts if they don't exist
    for acc in DEFAULT_TORRIN_ACCOUNTS:
        if not await db.webdav_account_exists(acc["username"]):
            aid = await db.add_webdav_account(acc["name"], acc["url"], acc["username"], acc["password"])
            logger.info(f"Auto-added default Torrin account: {acc['name']} (id={aid})")

    # Restore WebDAV file map from DB into memory
    await _restore_file_map_from_db()

    await startup_tg_clients()
    yield
    await shutdown_tg_clients()


async def _restore_file_map_from_db():
    """Load persisted WebDAV file mappings from SQLite into in-memory map."""
    global wd_file_map
    files = await db.get_all_webdav_files()
    count = 0
    for f in files:
        wd_file_map[f["sid"]] = {
            "account_id": f["account_id"],
            "path": f["path"],
            "name": f["name"],
            "webdav_base": f["webdav_base"],
            "content_type": f.get("content_type", ""),
        }
        count += 1
    if count:
        logger.info(f"Restored {count} WebDAV file mappings from DB")


app = FastAPI(title="Telegram2.0", lifespan=lifespan)


# ---- CORS: middleware + direct headers on Stremio endpoints ----
CORS_HEADERS = {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, DELETE, OPTIONS",
    "access-control-allow-headers": "*",
}


def _cors(data=None, status=200):
    """Return a JSONResponse with CORS headers baked in."""
    return JSONResponse(content=data, status_code=status, headers=CORS_HEADERS)


@app.middleware("http")
async def cors_middleware(request, call_next):
    """Add CORS headers to every response."""
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=CORS_HEADERS)
    response = await call_next(request)
    for k, v in CORS_HEADERS.items():
        response.headers[k] = v
    return response


# Serve static dashboard
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# ---------- Telegram Bot Webhook ----------
@app.post("/webhook")
async def telegram_webhook(request: Request, background_tasks: BackgroundTasks):
    update = await request.json()
    message = update.get("message", {})
    text = message.get("text", "") or message.get("caption", "")
    chat_id = message.get("chat", {}).get("id")
    from_user = message.get("from", {}).get("id")

    # Security: only OWNER can use the bot
    if OWNER_ID and from_user != OWNER_ID:
        return JSONResponse({"ok": True})

    if not text:
        return JSONResponse({"ok": True})

    text = text.strip()

    if text == "/start":
        async with httpx.AsyncClient() as hc:
            await hc.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": "Send me a magnet link to download and add to your Stremio library.\n\nYou can also paste magnet links in the web dashboard.",
                    "parse_mode": "Markdown",
                },
            )
        return JSONResponse({"ok": True})

    if text.startswith("magnet:"):
        download_id = str(uuid.uuid4())[:8]
        await db.create_download(download_id, text)
        async with httpx.AsyncClient() as hc:
            await hc.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": f"Download started! ID: {download_id}\nCheck the dashboard for progress.",
                },
            )
        background_tasks.add_task(
            process_magnet_link, download_id, text, chat_id
        )
        return JSONResponse({"ok": True})

    return JSONResponse({"ok": True})


# ---------- Web Dashboard Magnet Submit ----------
@app.post("/api/add")
async def add_magnet(request: Request, background_tasks: BackgroundTasks):
    """Submit a magnet link from the web dashboard."""
    data = await request.json()
    magnet = data.get("magnet", "").strip()

    if not magnet.startswith("magnet:"):
        raise HTTPException(400, "Invalid magnet link")

    download_id = str(uuid.uuid4())[:8]
    await db.create_download(download_id, magnet)
    background_tasks.add_task(process_magnet_link, download_id, magnet, None)
    return {"ok": True, "download_id": download_id}


# ---------- Core Pipeline ----------
async def process_magnet_link(download_id: str, magnet: str, notify_chat_id: int = None):
    """Download torrent and upload to Telegram channel."""
    try:
        async def upload_to_tg(channel_id, file_path, caption):
            return await user_client.send_document(
                chat_id=channel_id,
                document=file_path,
                caption=caption,
            )

        result = await downloader.download_and_upload(
            magnet=magnet,
            download_id=download_id,
            tg_upload_func=upload_to_tg,
            channel_id=CHANNEL_ID,
        )

        if result and notify_chat_id:
            async with httpx.AsyncClient() as hc:
                await hc.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": notify_chat_id,
                        "text": f"Done! {result['name']} is now in your Stremio library.",
                    },
                )

        # Trigger TMDb match in background after upload
        if result:
            asyncio.create_task(
                tmdb_match_after_upload(result["name"], result["channel_id"], result["message_id"])
            )

    except Exception as e:
        logger.exception(f"Pipeline failed for {download_id}")
        if notify_chat_id:
            try:
                async with httpx.AsyncClient() as hc:
                    await hc.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": notify_chat_id, "text": f"Error: {str(e)}"},
                    )
            except Exception:
                pass


# ---------- TMDb Matching (optional) ----------
async def tmdb_match_after_upload(file_name: str, channel_id: int, message_id: int):
    """Try to match uploaded file with TMDb for better Stremio integration."""
    if not TMDB_API_KEY:
        return

    import httpx
    media_info = parse_media_info(file_name)
    query = media_info.get("clean_name", file_name)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://api.themoviedb.org/3/search/"
                + ("tv" if media_info["media_type"] == "series" else "movie"),
                params={"api_key": TMDB_API_KEY, "query": query},
            )
            data = resp.json()
            results = data.get("results", [])
            if not results:
                return

            top = results[0]
            tmdb_id = top["id"]
            imdb_id = None
            poster = top.get("poster_path", "")
            description = top.get("overview", "")
            year = None

            if poster:
                poster = f"https://image.tmdb.org/t/p/w500{poster}"

            # Get IMDB ID
            details_resp = await client.get(
                f"https://api.themoviedb.org/3/" 
                + ("tv" if media_info["media_type"] == "series" else "movie")
                + f"/{tmdb_id}",
                params={"api_key": TMDB_API_KEY},
            )
            details = details_resp.json()
            imdb_id = details.get("imdb_id")
            if details.get("first_air_date"):
                year = int(details["first_air_date"][:4])
            elif details.get("release_date"):
                year = int(details["release_date"][:4])

            # Update the file record directly
            import aiosqlite
            async with aiosqlite.connect(db.DB_PATH) as conn:
                await conn.execute(
                    "UPDATE files SET imdb_id=?, tmdb_id=?, poster_url=?, description=?, year=? WHERE channel_id=? AND message_id=?",
                    (imdb_id, tmdb_id, poster, description, year, channel_id, message_id),
                )
                await conn.commit()

            logger.info(f"TMDb matched: {file_name} -> {imdb_id or tmdb_id}")
    except Exception as e:
        logger.warning(f"TMDb match failed for {file_name}: {e}")


# ---------- TMDb match helper ----------
async def update_file_metadata(
    channel_id: int, message_id: int,
    imdb_id: str = None, tmdb_id: int = None,
    poster_url: str = None, description: str = None, year: int = None,
):
    import aiosqlite
    async with aiosqlite.connect(db.DB_PATH) as conn:
        parts, vals = [], []
        if imdb_id is not None:
            parts.append("imdb_id=?"); vals.append(imdb_id)
        if tmdb_id is not None:
            parts.append("tmdb_id=?"); vals.append(tmdb_id)
        if poster_url is not None:
            parts.append("poster_url=?"); vals.append(poster_url)
        if description is not None:
            parts.append("description=?"); vals.append(description)
        if year is not None:
            parts.append("year=?"); vals.append(year)
        if not parts:
            return
        vals.extend([channel_id, message_id])
        await conn.execute(
            f"UPDATE files SET {', '.join(parts)} WHERE channel_id=? AND message_id=?",
            vals,
        )
        await conn.commit()


# ========== STREMIO ADDON ==========

ADDON_ID = "community.telegram2"


@app.get("/manifest.json")
async def stremio_manifest():
    """Stremio addon manifest."""
    return _cors({
        "id": ADDON_ID,
        "version": "2.0.0",
        "name": "Telegram2.0",
        "description": "Personal Telegram torrent library streamed via Stremio",
        "logo": "https://img.icons8.com/color/512/telegram-app.png",
        "resources": ["catalog", "stream"],
        "types": ["movie"],
        "catalogs": [
            {"id": "telegram_movies", "name": "Telegram Movies", "type": "movie"},
            {"id": "torrin_files", "name": "Torrin Files", "type": "movie"},
        ],
        "idPrefixes": ["tt", "local:", "wd:"],
        "behaviorHints": {"configurable": True, "configurationRequired": False},
    })


# ---------- WebDAV in-memory cache ----------
wd_cache = {}  # account_id -> {"files": [...], "updated_at": float}
WD_CACHE_TTL = 600  # 10 minutes

# Mapping: short_id -> {account_id, path, name, webdav_base}
wd_file_map = {}


async def get_webdav_files_cached(account_id: int, account: dict) -> list:
    """Return cached WebDAV files or browse fresh."""
    now = _time.time()
    cached = wd_cache.get(account_id)
    if cached and (now - cached["updated_at"]) < WD_CACHE_TTL:
        return cached["files"]

    logger.info(f"Browsing WebDAV account {account['name']} ({account['url']})...")
    files = await list_video_files(account["url"], account["username"], account["password"])
    wd_cache[account_id] = {"files": files, "updated_at": now}

    # Clear old DB entries for this account, then rebuild
    await db.clear_webdav_files(account_id)
    for f in files:
        sid = str(_uuid.uuid4())[:8]
        wd_file_map[sid] = {
            "account_id": account_id,
            "path": f["path"],
            "name": f["name"],
            "webdav_base": f["webdav_base"],
            "content_type": f.get("content_type", ""),
        }
        # Persist to DB so stream lookups survive restarts
        await db.upsert_webdav_file(
            sid=sid,
            account_id=account_id,
            path=f["path"],
            name=f["name"],
            webdav_base=f["webdav_base"],
            content_type=f.get("content_type", ""),
            size=f.get("size", 0),
        )

    logger.info(f"WebDAV {account['name']}: {len(files)} video files found")
    return files


@app.get("/catalog/{type}/{catalog_id}.json")
async def stremio_catalog(type: str, catalog_id: str):
    """Return catalog of available files for Stremio."""
    try:
        if catalog_id == "torrin_files":
            data = await _catalog_torrin()
            return _cors(data)

        # Default: telegram_movies
        files = await db.get_all_files()
        metas = []
        for f in files:
            item_id = f["imdb_id"] if f.get("imdb_id") else f"local:{f['id']}"
            meta = {
                "id": item_id,
                "name": f["name"],
                "type": "movie",
            }
            if f.get("poster_url"):
                meta["poster"] = f["poster_url"]
            else:
                meta["poster"] = "https://img.icons8.com/color/512/telegram-app.png"
            meta["posterShape"] = "poster"
            if f.get("description"):
                meta["description"] = f["description"]
            if f.get("year"):
                meta["year"] = f["year"]
            metas.append(meta)
        return _cors({"metas": metas})
    except Exception as e:
        logger.exception("Catalog error")
        return _cors({"metas": []})


async def _catalog_torrin():
    """Build catalog from all WebDAV accounts."""
    accounts = await db.get_webdav_accounts()
    metas = []

    for acc in accounts:
        files = await get_webdav_files_cached(acc["id"], acc)
        for f in files:
            # Find the short_id we assigned in get_webdav_files_cached
            sid = None
            for k, v in wd_file_map.items():
                if v["account_id"] == acc["id"] and v["path"] == f["path"]:
                    sid = k
                    break
            if not sid:
                sid = str(_uuid.uuid4())[:8]
                wd_file_map[sid] = {
                    "account_id": acc["id"],
                    "path": f["path"],
                    "name": f["name"],
                    "webdav_base": f["webdav_base"],
                    "content_type": f.get("content_type", ""),
                }

            meta = {
                "id": f"wd:{sid}",
                "name": f["name"],
                "type": "movie",
            }
            # Use account icon as poster
            meta["poster"] = "https://img.icons8.com/color/512/cloud.png"
            meta["posterShape"] = "poster"
            metas.append(meta)

    return {"metas": metas}


@app.get("/stream/{type}/{item_id}.json")
async def stremio_stream(type: str, item_id: str):
    """Return stream URL for a given IMDB/local/webdav ID."""
    try:
        # WebDAV file
        if item_id.startswith("wd:"):
            sid = item_id[3:]
            info = wd_file_map.get(sid)

            # Fallback: look up from DB if in-memory map missed
            if not info:
                db_row = await db.get_webdav_file(sid)
                if db_row:
                    info = {
                        "account_id": db_row["account_id"],
                        "path": db_row["path"],
                        "name": db_row["name"],
                        "webdav_base": db_row["webdav_base"],
                        "content_type": db_row.get("content_type", ""),
                    }
                    wd_file_map[sid] = info  # restore to memory
                    logger.info(f"Restored wd:{sid} from DB")

            if not info:
                logger.warning(f"Stream lookup failed for wd:{sid}")
                return _cors({"streams": []})

            # Return TWO stream entries:
            # 1) Proxy stream (works in Stremio Web browser player AND Desktop)
            # 2) Same proxy with notWebReady for Desktop mpv (better compatibility)
            proxy_url = f"{BASE_URL}/play/wd/{sid}"
            logger.info(f"Proxy stream for Stremio: {info['name']}")
            return _cors({"streams": [
                {
                    "name": "Torrin",
                    "title": info["name"],
                    "url": proxy_url,
                },
                {
                    "name": "Torrin (Desktop)",
                    "title": info["name"],
                    "url": proxy_url,
                    "behaviorHints": {
                        "notWebReady": True,
                    },
                },
            ]})

        # Telegram file
        file_record = None
        if item_id.startswith("local:"):
            fid = int(item_id.split(":")[1])
            file_record = await db.get_by_id(fid)
        else:
            file_record = await db.get_by_imdb_id(item_id)

        if not file_record:
            return _cors({"streams": []})

        stream_url = f"{BASE_URL}/play/{file_record['message_id']}"
        return _cors({"streams": [{"title": "Telegram Stream", "url": stream_url}]})
    except Exception as e:
        logger.exception("Stream error")
        return _cors({"streams": []})


# Cache for direct CDN URLs: sid -> {"url": str, "expires_at": float}
_cdn_url_cache: dict = {}
_CDN_CACHE_TTL = 900  # 15 min — beam URLs expire in ~20 min


async def _try_get_direct_url(info: dict) -> str | None:
    """Get a cached Torrin CDN URL (used for non-critical lookups)."""
    cache_key = f"{info['account_id']}:{info['path']}"
    cached = _cdn_url_cache.get(cache_key)
    if cached and _time.time() < cached["expires_at"]:
        return cached["url"]
    # Cache miss — discover fresh
    return await _try_get_direct_url_fresh(info)


async def _try_get_direct_url_fresh(info: dict) -> str | None:
    """Always discover a fresh Torrin CDN URL (HEAD with auth, follow 307).
    Torrin WebDAV returns 307 -> beam-*.torrin.app CDN.  Returns the final CDN URL."""
    cache_key = f"{info['account_id']}:{info['path']}"

    try:
        account = await db.get_webdav_account(info["account_id"])
        if not account:
            return None

        url = account["url"].rstrip("/") + info["path"]

        # HEAD with redirect following — Torrin 307s to beam-*.torrin.app CDN
        async with httpx.AsyncClient(
            auth=(account["username"], account["password"]),
            verify=False,
            follow_redirects=True,
            timeout=httpx.Timeout(10, connect=5, read=15, write=5),
        ) as client:
            resp = await client.head(url, headers={"Range": "bytes=0-1"})
            final_url = str(resp.url)

            # beam-eu.torrin.app, beam-in.torrin.app, beam-us.torrin.app, etc.
            if "beam-" in final_url or ("torrin" in final_url and "webdav" not in final_url):
                _cdn_url_cache[cache_key] = {
                    "url": final_url,
                    "expires_at": _time.time() + _CDN_CACHE_TTL,
                }
                return final_url

    except Exception as e:
        logger.debug(f"Direct URL discovery failed: {e}")

    return None


# ---------- Stream Proxies ----------

@app.get("/play/wd/{sid}")
async def play_webdav(sid: str, request: Request):
    """Stream a file from WebDAV (Torrin) to the client."""
    info = wd_file_map.get(sid)

    # Fallback: look up from DB
    if not info:
        db_row = await db.get_webdav_file(sid)
        if db_row:
            info = {
                "account_id": db_row["account_id"],
                "path": db_row["path"],
                "name": db_row["name"],
                "webdav_base": db_row["webdav_base"],
                "content_type": db_row.get("content_type", ""),
            }
            wd_file_map[sid] = info

    if not info:
        raise HTTPException(404, "WebDAV file not found. Open catalog to refresh.")

    account = await db.get_webdav_account(info["account_id"])
    if not account:
        raise HTTPException(404, "WebDAV account not found")

    range_header = request.headers.get("range")
    logger.info(f"WebDAV play: {info['name']} range={range_header}")

    # Proxy through Render (for /files page browser playback)
    url = account["url"].rstrip("/") + info["path"]
    req_headers = {}
    if range_header:
        req_headers["Range"] = range_header

    client = httpx.AsyncClient(
        auth=(account["username"], account["password"]),
        verify=False,
        follow_redirects=True,
        timeout=httpx.Timeout(30, connect=10, read=300, write=10),
    )

    try:
        resp = await client.send(
            client.build_request("GET", url, headers=req_headers),
            stream=True,
        )

        if resp.status_code not in (200, 206):
            await resp.aclose()
            await client.aclose()
            raise HTTPException(resp.status_code, f"WebDAV returned {resp.status_code}")

        # Determine content type
        ct = resp.headers.get("content-type", "")
        if not ct or ct == "application/octet-stream":
            from webdav_client import _guess_content_type
            ct = _guess_content_type(info["path"])

        resp_headers = {
            "Content-Type": ct,
            "Accept-Ranges": "bytes",
        }
        cr = resp.headers.get("content-range")
        cl = resp.headers.get("content-length")
        if cr:
            resp_headers["Content-Range"] = cr
        if cl:
            resp_headers["Content-Length"] = cl

        code = 206 if range_header and resp.status_code == 206 else 200

        async def generate():
            try:
                async for chunk in resp.aiter_bytes(512 * 1024):
                    yield chunk
            finally:
                await resp.aclose()
                await client.aclose()

        return StreamingResponse(generate(), status_code=code, headers=resp_headers)

    except HTTPException:
        raise
    except Exception as e:
        await client.aclose()
        logger.error(f"WebDAV stream error for {sid} ({info.get('name','?')}): {e}")
        raise HTTPException(500, f"WebDAV stream error: {e}")


@app.get("/play/{message_id}")
async def play_telegram(message_id: int, request: Request):
    """Stream a file from Telegram channel to the client."""
    range_header = request.headers.get("range")

    try:
        msg = await user_client.get_messages(CHANNEL_ID, message_id)
        file = msg.document or msg.video
        if not file:
            raise HTTPException(404, "File not found")

        file_size = file.file_size
        file_name = file.file_name or "stream.mkv"

        start = 0
        end = file_size - 1
        if range_header:
            range_match = __import__("re").search(r"bytes=(\d+)-(\d*)", range_header)
            if range_match:
                start = int(range_match.group(1))
                end = int(range_match.group(2)) if range_match.group(2) else file_size - 1

        content_length = end - start + 1

        async def generate():
            bytes_sent = 0
            async for chunk in user_client.stream_media(msg, limit=content_length):
                if isinstance(chunk, bytes):
                    if bytes_sent >= content_length:
                        break
                    yield chunk
                    bytes_sent += len(chunk)

        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Type": "video/mp4",
            "Content-Disposition": f'inline; filename="{file_name}"',
        }

        if range_header:
            return StreamingResponse(generate(), status_code=206, headers=headers)
        return StreamingResponse(generate(), headers=headers)

    except Exception as e:
        logger.error(f"Stream error for message {message_id}: {e}")
        raise HTTPException(500, f"Stream error: {e}")


# ========== CONFIGURE PAGE (Stremio opens this) ==========

@app.get("/configure")
async def configure_page():
    return HTMLResponse(open(os.path.join(static_dir, "configure.html")).read())


# ========== WEBDAV ACCOUNT API ==========

@app.get("/api/webdav/accounts")
async def api_webdav_accounts():
    accounts = await db.get_webdav_accounts()
    return {"accounts": accounts}


@app.post("/api/webdav/accounts")
async def api_add_webdav_account(request: Request):
    data = await request.json()
    name = data.get("name", "").strip()
    url = data.get("url", "").strip().rstrip("/")
    username = data.get("username", "").strip()
    password = data.get("password", "")

    if not url or not username:
        raise HTTPException(400, "URL and username are required")

    if not name:
        name = url.split("//")[1].split("/")[0] if "//" in url else url

    aid = await db.add_webdav_account(name, url, username, password)
    # Clear cache so next catalog request re-browses
    wd_cache.pop(aid, None)
    return {"ok": True, "id": aid, "name": name}


@app.delete("/api/webdav/accounts/{account_id}")
async def api_delete_webdav_account(account_id: int):
    await db.delete_webdav_account(account_id)
    wd_cache.pop(account_id, None)
    # Clean file map entries for this account
    to_remove = [k for k, v in wd_file_map.items() if v["account_id"] == account_id]
    for k in to_remove:
        del wd_file_map[k]
    return {"ok": True}


@app.post("/api/webdav/test")
async def api_test_webdav(request: Request):
    data = await request.json()
    result = await webdav_test(
        data.get("url", ""),
        data.get("username", ""),
        data.get("password", ""),
    )
    return result


@app.post("/api/webdav/refresh")
async def api_webdav_refresh():
    """Force refresh all WebDAV caches."""
    accounts = await db.get_webdav_accounts()
    total = 0
    errors = []
    for acc in accounts:
        try:
            full = await db.get_webdav_account(acc["id"])
            if not full:
                continue
            wd_cache.pop(acc["id"], None)
            files = await get_webdav_files_cached(acc["id"], full)
            total += len(files)
        except Exception as e:
            logger.error(f"WebDAV refresh failed for {acc['name']}: {e}")
            errors.append(f"{acc['name']}: {str(e)}")
    return {"ok": True, "files_found": total, "errors": errors}


# ========== DASHBOARD API ==========

@app.get("/")
async def dashboard():
    return HTMLResponse(open(os.path.join(static_dir, "index.html")).read())


@app.get("/api/files")
async def api_files():
    files = await db.get_all_files()
    return {"files": files}


@app.get("/api/downloads")
async def api_downloads():
    downloads = await db.get_downloads()
    active = await db.get_active_downloads()
    return {"downloads": downloads, "active": active}


@app.get("/api/status")
async def api_status():
    return {
        "total_files": await db.get_file_count(),
        "active_downloads": len(await db.get_active_downloads()),
        "channel_id": CHANNEL_ID,
    }


@app.delete("/api/files/{file_id}")
async def api_delete_file(file_id: int):
    await db.delete_file(file_id)
    return {"ok": True}


# ---------- Stremio Catalog for Dashboard ----------
@app.get("/api/catalog")
async def api_catalog():
    """Return the catalog as seen by Stremio (for dashboard display)."""
    files = await db.get_all_files()
    items = []
    for f in files:
        item_id = f["imdb_id"] if f.get("imdb_id") else f"local:{f['id']}"
        item = {
            "id": item_id,
            "name": f["name"],
            "type": "movie",
            "size": f["size"],
            "added_at": f.get("added_at", ""),
        }
        if f.get("poster_url"):
            item["poster"] = f["poster_url"]
        if f.get("year"):
            item["year"] = f["year"]
        items.append(item)
    return {"catalog": items, "total": len(items)}


# ---------- Stremio Addon URL helper ----------
@app.get("/addon-url")
async def addon_url():
    """Return the manifest URL to paste into Stremio."""
    manifest = f"{BASE_URL}/manifest.json"
    return {"addon_url": manifest}


# ========== FILES PAGE (Torrin / WebDAV files browser) ==========

@app.get("/file")
async def files_page():
    return HTMLResponse(open(os.path.join(static_dir, "files.html")).read())


@app.get("/api/torrin/files")
async def api_torrin_files():
    """Return all cached WebDAV/Torrin files for the files page."""
    accounts = await db.get_webdav_accounts()
    all_files = []
    for acc in accounts:
        try:
            full = await db.get_webdav_account(acc["id"])
            if not full:
                continue
            files = await get_webdav_files_cached(acc["id"], full)
            for f in files:
                # Find the short_id for this file
                sid = None
                for k, v in wd_file_map.items():
                    if v["account_id"] == acc["id"] and v["path"] == f["path"]:
                        sid = k
                        break
                all_files.append({
                    "name": f["name"],
                    "size": f["size"],
                    "path": f["path"],
                    "account": acc["name"],
                    "account_id": acc["id"],
                    "sid": sid,
                    "stream_url": f"{BASE_URL}/play/wd/{sid}" if sid else None,
                })
        except Exception as e:
            logger.error(f"Failed to get files for {acc['name']}: {e}")
    return {"files": all_files, "total": len(all_files), "accounts": len(accounts)}


@app.get("/api/torrin/test-stream/{sid}")
async def api_test_torrin_stream(sid: str):
    """Test what URL Torrin returns for a file — discover CDN redirect."""
    info = wd_file_map.get(sid)
    if not info:
        return {"error": "File not found in cache"}
    account = await db.get_webdav_account(info["account_id"])
    if not account:
        return {"error": "Account not found"}

    url = account["url"].rstrip("/") + info["path"]
    result = {"webdav_url": url, "name": info["name"]}

    try:
        async with httpx.AsyncClient(
            auth=(account["username"], account["password"]),
            verify=False,
            follow_redirects=True,
            timeout=httpx.Timeout(10, connect=5, read=15, write=5),
        ) as client:
            # Try HEAD
            resp = await client.head(url)
            result["head_status"] = resp.status_code
            result["head_final_url"] = str(resp.url)
            result["head_all_headers"] = dict(resp.headers)

            # Try GET without following redirects to see the redirect target
            async with httpx.AsyncClient(
                auth=(account["username"], account["password"]),
                verify=False,
                follow_redirects=False,
                timeout=httpx.Timeout(10, connect=5, read=15, write=5),
            ) as client2:
                resp2 = await client2.get(url, headers={"Range": "bytes=0-0"})
                result["get_status"] = resp2.status_code
                result["get_location"] = resp2.headers.get("location", "none")
                result["get_all_headers"] = dict(resp2.headers)
    except Exception as e:
        result["error"] = str(e)

    return result


# ---------- Compare our stream vs Torrin official ----------
@app.get("/api/torrin/compare/{sid}")
async def api_torrin_compare(sid: str):
    """Compare our addon stream response vs Torrin's official API response."""
    info = wd_file_map.get(sid)
    if not info:
        return {"error": "File not found"}
    account = await db.get_webdav_account(info["account_id"])
    if not account:
        return {"error": "Account not found"}

    result = {"our_stream": {}, "torrin_official": {}}

    # Our addon stream response
    our_url = f"{BASE_URL}/stream/movie/wd:{sid}.json"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(our_url)
            result["our_stream"] = resp.json()
    except Exception as e:
        result["our_stream_error"] = str(e)

    # Torrin official API — try to find a matching stream
    api_key = account["username"]  # tr_xxx is both WebDAV user and API key
    torrin_url = f"https://stremio.torrin.app/{api_key}/stream/movie/tt33347879.json"
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.get(torrin_url)
            data = resp.json()
            # Find matching stream by filename
            for s in data.get("streams", []):
                if info["name"] in s.get("title", ""):
                    result["torrin_official"] = s
                    break
            if not result["torrin_official"]:
                # Return first stream as reference
                if data.get("streams"):
                    result["torrin_official"] = data["streams"][0]
                    result["torrin_official_note"] = "First stream (may not match this file)"
                else:
                    result["torrin_official"] = data
    except Exception as e:
        result["torrin_official_error"] = str(e)

    # Also test: can our proxy actually serve data?
    proxy_url = f"{BASE_URL}/play/wd/{sid}"
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=False) as c:
            resp = await c.head(proxy_url, headers={"Range": "bytes=0-0"})
            result["proxy_test"] = {
                "status": resp.status_code,
                "headers": dict(resp.headers),
            }
    except Exception as e:
        result["proxy_test_error"] = str(e)

    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)