"""Shared cache contract for the weather-stations subsystem.

Home of the Redis cache-key builders, the S3 snapshot-key parsing helpers, and
the tileset-listing computation. Imported by **both** the scraper (write side)
and the read service (read side) so neither depends on the other and the S3
layout knowledge lives in one place.

Redis keys (binary `decode_responses=False`, JSON payloads):
    cache:ws:latest                       raw latest.json bytes
    cache:ws:tilesets                      assembled tilesets list (JSON)
    cache:ws:registry                      raw stations.json bytes
    cache:ws:snapshot:{tileset_id}:n{N}    resolved /{tileset_id}?N= snapshot bytes
"""

import asyncio
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from clients.s3_client import S3Client

# A LIST function: prefix -> keys. The read service passes its in-process
# cached lister; the scraper lets it default to a direct S3 LIST.
ListKeysFn = Callable[[str], Awaitable[List[str]]]

logger = logging.getLogger(__name__)

# Mirror the scraper's S3 layout (kept here, not imported from the scraper, so
# the read path doesn't depend on the scraper module loading).
_S3_PREFIX = "weather-stations"
SNAPSHOTS_PREFIX = f"{_S3_PREFIX}/snapshots/"
SNAPSHOT_META_SUFFIX = ".meta.json"
# `YYYYMMDDTHHMMSSZ` parsed back to a UTC datetime.
_SNAPSHOT_KEY_RE = re.compile(r".*/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z\.json$")
_TILESET_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})Z$")


class TilesetIdFormatError(Exception):
    """Raised when a tilesetId path param doesn't match `YYYYMMDDTHHMMZ`."""


# --------------------------------------------------------------- Redis key builders


def latest_key() -> str:
    """Cache key for the most recent snapshot (`/latest`)."""
    return "cache:ws:latest"


def tilesets_key() -> str:
    """Cache key for the assembled tilesets listing (`/tilesets`)."""
    return "cache:ws:tilesets"


def registry_key() -> str:
    """Cache key for the station registry (`/stations`)."""
    return "cache:ws:registry"


def snapshot_key(tileset_id: str, n: float) -> str:
    """Cache key for a resolved `/{tileset_id}?N=` snapshot.

    `N` is normalised with `:g` so `3`, `3.0` and `3` collapse to one key and
    don't fragment the cache.
    """
    return f"cache:ws:snapshot:{tileset_id}:n{n:g}"


# ---------------------------------------------------------- S3 snapshot-key parsing


def parse_snapshot_key(key: str) -> Optional[datetime]:
    """Parse `YYYYMMDDTHHMMSSZ` out of a snapshot key into a UTC datetime."""
    match = _SNAPSHOT_KEY_RE.match(key)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(p) for p in match.groups())
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_tileset_id(tileset_id: str) -> datetime:
    """Parse a `YYYYMMDDTHHMMZ` tilesetId into a UTC datetime."""
    match = _TILESET_RE.match(tileset_id)
    if not match:
        raise TilesetIdFormatError(
            f"Invalid tilesetId {tileset_id!r}; expected YYYYMMDDTHHMMZ "
            "(e.g. 20260517T1400Z)"
        )
    year, month, day, hour, minute = (int(p) for p in match.groups())
    try:
        return datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)
    except ValueError as exc:
        raise TilesetIdFormatError(f"Invalid tilesetId {tileset_id!r}: {exc}") from exc


def tileset_id_for(ts: datetime) -> str:
    """Round a snapshot timestamp down to its hour-bucket tilesetId."""
    return ts.strftime("%Y%m%dT%H00Z")


def day_prefixes_covering(start: datetime, end: datetime) -> List[str]:
    """Return `weather-stations/snapshots/YYYY/MM/DD/` prefixes covering [start, end]."""
    if end < start:
        start, end = end, start
    prefixes: List[str] = []
    day = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_day = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    while day <= end_day:
        prefixes.append(f"{SNAPSHOTS_PREFIX}{day.strftime('%Y/%m/%d')}/")
        day = day + timedelta(days=1)
    return prefixes


def parse_json_or_none(body: bytes, key: str) -> Optional[dict]:
    """Decode JSON bytes into a dict, logging and returning None on bad data."""
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Could not parse object %s: %s", key, exc)
        return None
    if not isinstance(parsed, dict):
        logger.warning("Object %s is not a JSON object", key)
        return None
    return parsed


# ------------------------------------------------------ tileset listing computation


async def _read_station_count(s3: S3Client, snapshot_key_: str) -> int:
    """Read `station_count` from a snapshot's cheap sibling `.meta.json`."""
    meta_key = snapshot_key_[: -len(".json")] + SNAPSHOT_META_SUFFIX
    body = await s3.download_tile(meta_key)
    if body is None:
        return 0
    meta = parse_json_or_none(body, meta_key)
    if not isinstance(meta, dict):
        return 0
    count = meta.get("station_count")
    return int(count) if isinstance(count, int) else 0


async def compute_tilesets_entries(
    s3: S3Client, list_keys: Optional[ListKeysFn] = None
) -> List[dict]:
    """Group snapshot keys into hour buckets and return tileset entries.

    Walks the snapshots prefix, groups keys into hour buckets
    (`YYYYMMDDTHH00Z`), keeps the latest snapshot per bucket, and returns its
    `scraped_at` (ISO-8601 string, so the result is JSON-serialisable for Redis
    and re-parses cleanly via `TilesetsResponse.model_validate`) plus the
    `station_count` from the cheap sibling `.meta.json` (0 when missing).

    `list_keys` lets the read service inject its in-process cached lister (so a
    burst collapses to one S3 LIST); the scraper omits it for a direct LIST.
    """
    lister = list_keys or s3.list_object_keys
    keys = await lister(SNAPSHOTS_PREFIX)
    per_bucket: Dict[str, Tuple[datetime, str]] = {}
    for key in keys:
        if key.endswith(SNAPSHOT_META_SUFFIX):
            continue
        ts = parse_snapshot_key(key)
        if ts is None:
            continue
        bucket = tileset_id_for(ts)
        existing = per_bucket.get(bucket)
        if existing is None or ts > existing[0]:
            per_bucket[bucket] = (ts, key)

    if not per_bucket:
        return []

    # Sort by scrape time so the meta GETs pair with the right tilesets in zip.
    items = sorted(per_bucket.items(), key=lambda kv: kv[1][0])
    counts = await asyncio.gather(
        *(_read_station_count(s3, key) for _, (_, key) in items)
    )
    return [
        {
            "tileset_id": tileset_id,
            "scraped_at": ts.isoformat().replace("+00:00", "Z"),
            "station_count": count,
        }
        for (tileset_id, (ts, _key)), count in zip(items, counts)
    ]
