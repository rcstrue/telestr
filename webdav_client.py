"""
WebDAV client for Torrin / any WebDAV server.
Browses directories, finds video files, streams content.
Uses raw httpx — no extra WebDAV library needed.
"""

import xml.etree.ElementTree as ET
import logging
import httpx
from urllib.parse import unquote

logger = logging.getLogger(__name__)

NS = {"D": "DAV:"}

PROPFIND_BODY = """<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:displayname/>
    <D:getcontentlength/>
    <D:getcontenttype/>
    <D:getlastmodified/>
    <D:resourcetype/>
  </D:prop>
</D:propfind>"""

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".webm", ".mov",
    ".wmv", ".flv", ".m4v", ".ts", ".mpg", ".mpeg",
}

CONTENT_TYPE_MAP = {
    ".mkv": "video/x-matroska",
    ".mp4": "video/mp4",
    ".avi": "video/x-msvideo",
    ".webm": "video/webm",
    ".mov": "video/quicktime",
    ".wmv": "video/x-ms-wmv",
    ".flv": "video/x-flv",
    ".m4v": "video/mp4",
    ".ts": "video/mp2t",
    ".mpg": "video/mpeg",
    ".mpeg": "video/mpeg",
}


def _guess_content_type(path: str) -> str:
    """Guess content type from file extension."""
    # Decode path first to get clean extension
    decoded = unquote(path)
    ext = "." + decoded.rsplit(".", 1)[-1].lower() if "." in decoded else ""
    return CONTENT_TYPE_MAP.get(ext, "video/mp4")


async def list_video_files(url: str, username: str, password: str, max_depth: int = 3) -> list:
    """Recursively browse a WebDAV server and return video files."""
    files = []
    base = url.rstrip("/")
    auth = (username, password)

    async with httpx.AsyncClient(auth=auth, verify=False, timeout=30, follow_redirects=True) as client:
        await _browse(client, base, base, files, max_depth, 0)

    return files


async def _browse(client, base_url: str, current_url: str, files: list, max_depth: int, depth: int):
    if depth >= max_depth:
        return

    try:
        resp = await client.request(
            "PROPFIND",
            current_url,
            headers={"Depth": "1", "Content-Type": "application/xml"},
            content=PROPFIND_BODY,
        )

        if resp.status_code not in (207, 200):
            logger.warning(f"PROPFIND {current_url} -> {resp.status_code}")
            return

        root = ET.fromstring(resp.text)

        for response in root.findall(".//D:response", NS):
            href_el = response.find("D:href", NS)
            if href_el is None:
                continue

            href = href_el.text or ""

            # Normalize href — make it absolute
            if not href.startswith("/"):
                href = "/" + href

            # Skip the current directory
            norm_current = current_url.replace(base_url, "") or "/"
            if href == norm_current or href == norm_current.rstrip("/"):
                continue

            # Get properties
            prop = response.find("D:propstat/D:prop", NS)
            if prop is None:
                continue

            # Check if collection (directory)
            rt = prop.find("D:resourcetype", NS)
            is_dir = rt is not None and rt.find("D:collection", NS) is not None

            if is_dir:
                next_url = base_url + href
                await _browse(client, base_url, next_url, files, max_depth, depth + 1)
            else:
                raw_name = href.rstrip("/").split("/")[-1]
                name = unquote(raw_name)
                ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""

                if ext in VIDEO_EXTENSIONS:
                    size_el = prop.find("D:getcontentlength", NS)
                    size = int(size_el.text) if size_el is not None and size_el.text else 0

                    # Get content-type from WebDAV if available
                    ct_el = prop.find("D:getcontenttype", NS)
                    content_type = ct_el.text if ct_el is not None and ct_el.text else ""

                    files.append({
                        "name": name,
                        "path": href,
                        "size": size,
                        "webdav_base": base_url,
                        "content_type": content_type,
                    })

    except ET.ParseError:
        logger.error(f"XML parse error for {current_url}")
    except Exception as e:
        logger.error(f"WebDAV browse error at {current_url}: {e}")


async def stream_webdav_file(
    base_url: str, path: str,
    username: str, password: str,
    range_header: str = None,
):
    """
    Stream a file from WebDAV.
    Returns (status_code, headers_dict, async_generator).
    """
    url = base_url.rstrip("/") + path
    auth = (username, password)
    headers = {}
    if range_header:
        headers["Range"] = range_header

    async with httpx.AsyncClient(auth=auth, verify=False, timeout=httpx.Timeout(30, connect=15, read=120, write=15), follow_redirects=True) as client:
        async with client.stream("GET", url, headers=headers) as resp:
            status = resp.status_code
            # Use WebDAV content-type or guess from extension
            ct = resp.headers.get("content-type", "")
            if not ct or ct == "application/octet-stream":
                ct = _guess_content_type(path)
            resp_headers = {
                "Content-Type": ct,
                "Accept-Ranges": resp.headers.get("accept-ranges", "bytes"),
            }
            cr = resp.headers.get("content-range")
            cl = resp.headers.get("content-length")
            if cr:
                resp_headers["Content-Range"] = cr
            if cl:
                resp_headers["Content-Length"] = cl

            async def generate():
                async for chunk in resp.aiter_bytes(512 * 1024):
                    yield chunk

            yield status, resp_headers, generate


async def test_connection(url: str, username: str, password: str) -> dict:
    """Test WebDAV connection. Returns {ok, message, file_count}."""
    try:
        async with httpx.AsyncClient(auth=(username, password), verify=False, timeout=15, follow_redirects=True) as client:
            resp = await client.request(
                "PROPFIND",
                url.rstrip("/"),
                headers={"Depth": "1", "Content-Type": "application/xml"},
                content=PROPFIND_BODY,
            )
            if resp.status_code in (207, 200):
                root = ET.fromstring(resp.text)
                count = len(root.findall(".//D:response", NS)) - 1  # minus self
                return {"ok": True, "message": f"Connected. {count} items found.", "file_count": count}
            else:
                return {"ok": False, "message": f"HTTP {resp.status_code} — check credentials"}
    except httpx.ConnectError:
        return {"ok": False, "message": "Cannot connect to server"}
    except Exception as e:
        return {"ok": False, "message": str(e)}
