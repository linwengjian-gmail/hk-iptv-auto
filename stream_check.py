"""Lightweight playback probes for IPTV URLs.

An HTTP 200 response is not enough for a live stream: an HLS playlist can be
present while every media segment has already expired.  This module follows
the first few HLS entries and validates that at least one media payload can be
downloaded.
"""

import re
import time
from urllib.parse import urljoin

import requests


USER_AGENT = "hk-iptv-auto/1.0 (+https://github.com/linwengjian-gmail/hk-iptv-auto)"
MAX_PROBE_BYTES = 256 * 1024
MAX_PLAYLIST_DEPTH = 2
MAX_HLS_ENTRIES = 3


def _strip_m3u_suffix(url):
    """Remove the optional IPTV `$label` suffix before making an HTTP request."""
    return url.split("$", 1)[0].strip()


def _download_probe(url, timeout):
    """Download a bounded prefix and return data, final URL, and headers."""
    data = bytearray()
    try:
        with requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
            timeout=(timeout, timeout),
            stream=True,
            allow_redirects=True,
        ) as response:
            if response.status_code not in (200, 206):
                return None

            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    data.extend(chunk)
                if len(data) >= MAX_PROBE_BYTES:
                    break

            if not data:
                return None
            return bytes(data), response.url, dict(response.headers)
    except requests.exceptions.RequestException:
        return None


def _text_prefix(data):
    return data[:4096].decode("utf-8-sig", errors="ignore").lstrip()


def _is_playlist(data, headers):
    text = _text_prefix(data)
    content_type = headers.get("Content-Type", "").lower()
    return (
        text.startswith(("#EXTM3U", "#EXT-X-", "#EXTINF"))
        or "mpegurl" in content_type
        or "dash+xml" in content_type
    )


def _playlist_entries(text, base_url):
    """Resolve a small set of media/variant URLs from an HLS playlist."""
    entries = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Low-latency HLS may put a playable partial segment in an attribute.
        if line.startswith(("#EXT-X-PART:", "#EXT-X-PRELOAD-HINT:")):
            match = re.search(r'URI="([^"]+)"', line)
            if match:
                line = match.group(1).strip()
            else:
                continue
        elif line.startswith("#"):
            continue

        line = _strip_m3u_suffix(line)
        child_url = urljoin(base_url, line)
        if child_url.startswith(("http://", "https://")) and child_url not in entries:
            entries.append(child_url)
    return entries


def _has_mpeg_ts_signature(data):
    if len(data) < 188:
        return False
    for offset in range(min(4, len(data))):
        if data[offset] != 0x47:
            continue
        if len(data) < offset + 376 or data[offset + 188] == 0x47:
            return True
    return False


def _looks_like_media_payload(data, headers, url):
    """Reject HTML/JSON error pages and accept common stream payloads."""
    if not data:
        return False

    prefix = data[:256].lstrip().lower()
    if prefix.startswith((b"<!doctype html", b"<html", b"{", b"[", b"error", b"not found")):
        return False

    if _has_mpeg_ts_signature(data):
        return True
    if data.startswith((b"FLV", b"\x1a\x45\xdf\xa3", b"ID3")):
        return True
    if b"ftyp" in data[:64] or b"moof" in data[:64] or b"mdat" in data[:64]:
        return True
    if len(data) >= 2 and data[0] == 0xFF and (data[1] & 0xE0) == 0xE0:
        return True

    content_type = headers.get("Content-Type", "").lower()
    media_type = (
        content_type.startswith("video/")
        or content_type.startswith("audio/")
        or "octet-stream" in content_type
    )
    if media_type and len(data) >= 512:
        return True

    # Some providers return text/plain for binary .ts/.mp4 responses.
    path = url.lower().split("?", 1)[0]
    return len(data) >= 512 and path.endswith((".ts", ".mp4", ".m4s", ".mkv", ".webm", ".flv"))


def _check_hls_playlist(url, data, headers, timeout, depth=0):
    if depth > MAX_PLAYLIST_DEPTH or not _is_playlist(data, headers):
        return False

    entries = _playlist_entries(_text_prefix(data), url)
    if len(entries) > MAX_HLS_ENTRIES:
        entries_to_check = entries[:1] + entries[-(MAX_HLS_ENTRIES - 1):]
    else:
        entries_to_check = entries

    for entry in entries_to_check:
        result = _download_probe(entry, timeout)
        if not result:
            continue

        child_data, child_url, child_headers = result
        if _is_playlist(child_data, child_headers):
            if _check_hls_playlist(child_url, child_data, child_headers, timeout, depth + 1):
                return True
        elif _looks_like_media_payload(child_data, child_headers, child_url):
            return True
    return False


def check_url(url, retries=2, timeout=5):
    """Return True only when the URL exposes a playable media payload."""
    url = _strip_m3u_suffix(url)
    if not url.startswith(("http://", "https://")):
        return False

    for attempt in range(retries + 1):
        result = _download_probe(url, timeout)
        if result:
            data, final_url, headers = result
            if _is_playlist(data, headers):
                valid = _check_hls_playlist(urljoin(url, final_url), data, headers, timeout)
            else:
                valid = _looks_like_media_payload(data, headers, final_url)
            if valid:
                return True

        if attempt < retries:
            time.sleep(1)
    return False
