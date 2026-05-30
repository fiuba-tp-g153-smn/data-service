"""Read-side service for the weather-stations endpoints (S3 only, no Redis)."""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from clients.s3_client import S3Client

logger = logging.getLogger(__name__)

# Mirror the scraper's S3 layout. Kept in this module rather than imported from
# the scraper so the read path doesn't depend on the scraper module loading.
_S3_PREFIX = "weather-stations"
_LATEST_KEY = f"{_S3_PREFIX}/latest.json"
_REGISTRY_KEY = f"{_S3_PREFIX}/stations.json"
_SNAPSHOTS_PREFIX = f"{_S3_PREFIX}/snapshots/"
# `YYYYMMDDTHHMMSSZ` parsed back to a UTC datetime.
_SNAPSHOT_KEY_RE = re.compile(
    r".*/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z\.json$"
)
_SNAPSHOT_META_SUFFIX = ".meta.json"
_TILESET_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})Z$")


class WeatherStationsNotConfiguredError(Exception):
    """Raised when read endpoints are called but no S3 client is attached."""


class TilesetIdFormatError(Exception):
    """Raised when a tilesetId path param doesn't match `YYYYMMDDTHHMMZ`."""


class WeatherStationsService:
    """
    Read service backing the public /weather-stations/* endpoints.

    Resolves every read directly against S3 (no Redis). A tiny TTL cache on
    LIST results absorbs request bursts: at 2-day retention × 5-min cadence
    the bucket has ~576 snapshot keys, so a single LIST is cheap, but cached
    answers keep the endpoints sub-millisecond under load.

    Service is constructed at import time as a module-level singleton and
    populated via `configure(...)` from the lifespan, mirroring `BasemapService`.
    """

    def __init__(self) -> None:
        self._s3: Optional[S3Client] = None
        self._list_cache_ttl: float = 0.0
        # Per-prefix cached LIST result: prefix -> (expires_monotonic, keys).
        self._list_cache: Dict[str, Tuple[float, List[str]]] = {}
        self._list_cache_lock = asyncio.Lock()

    def configure(self, s3_client: Optional[S3Client], list_cache_ttl: float) -> None:
        """Attach runtime dependencies. Pass `None` for `s3_client` when disabled."""
        self._s3 = s3_client
        self._list_cache_ttl = list_cache_ttl

    # ------------------------------------------------------------------ /latest

    async def get_latest_snapshot(self) -> Optional[dict]:
        """Return the most recent snapshot (parsed JSON) or `None` if not yet scraped."""
        s3 = self._require_s3()
        body = await s3.download_tile(_LATEST_KEY)
        if body is None:
            return None
        return _parse_json_or_none(body, _LATEST_KEY)

    # ---------------------------------------------------------------- /tilesets

    async def list_tilesets(self) -> List[dict]:
        """
        Return the hour-bucketed list of available snapshots.

        Walks the day prefixes covering the configured retention window and
        groups snapshot keys into hour buckets (`YYYYMMDDTHH00Z`). For each
        bucket, picks the most recent snapshot and returns its scraped_at +
        station_count (the latter is taken from the cheap sibling `.meta.json`
        when available, falling back to 0 if neither sibling nor body parse).
        """
        keys = await self._list_snapshot_keys_recent()
        # Group by hour bucket; per bucket keep the latest snapshot key.
        per_bucket: Dict[str, Tuple[datetime, str]] = {}
        for key in keys:
            if key.endswith(_SNAPSHOT_META_SUFFIX):
                continue
            ts = _parse_snapshot_key(key)
            if ts is None:
                continue
            bucket = _tileset_id(ts)
            existing = per_bucket.get(bucket)
            if existing is None or ts > existing[0]:
                per_bucket[bucket] = (ts, key)

        if not per_bucket:
            return []

        # Sort by scrape time first so we fetch the cheap meta GETs in the
        # same order we'll emit them — otherwise zip pairs counts with the
        # wrong tilesets.
        items = sorted(per_bucket.items(), key=lambda kv: kv[1][0])
        counts = await asyncio.gather(
            *(self._read_station_count(key) for _, (_, key) in items)
        )
        return [
            {
                "tileset_id": tileset_id,
                "scraped_at": ts,
                "station_count": count,
            }
            for (tileset_id, (ts, _key)), count in zip(items, counts)
        ]

    # -------------------------------------------------------- /{tilesetId}?N=...

    async def get_snapshot_for_tileset(
        self, tileset_id: str, tolerance_hours: float
    ) -> Optional[dict]:
        """
        Resolve a tilesetId + N-hour tolerance to a snapshot.

        Picks the latest snapshot whose scraped_at falls in
        `[T - N*3600, T]` where T = tileset_id parsed as UTC. Returns the
        parsed snapshot JSON, or `None` if no snapshot exists in the window.
        Raises `TilesetIdFormatError` if `tileset_id` doesn't match the
        `YYYYMMDDTHHMMZ` shape.
        """
        if tolerance_hours < 0:
            raise ValueError("tolerance_hours must be >= 0")
        target = _parse_tileset_id(tileset_id)
        window_start = target - timedelta(hours=tolerance_hours)
        keys = await self._list_snapshot_keys_for_window(window_start, target)

        best_ts: Optional[datetime] = None
        best_key: Optional[str] = None
        for key in keys:
            if key.endswith(_SNAPSHOT_META_SUFFIX):
                continue
            ts = _parse_snapshot_key(key)
            if ts is None or ts < window_start or ts > target:
                continue
            if best_ts is None or ts > best_ts:
                best_ts = ts
                best_key = key

        if best_key is None:
            return None

        s3 = self._require_s3()
        body = await s3.download_tile(best_key)
        if body is None:
            return None
        return _parse_json_or_none(body, best_key)

    # ----------------------------------------------------------------- /stations

    async def get_stations_registry(self) -> Optional[dict]:
        """Return the parsed station registry, or `None` if not yet populated."""
        s3 = self._require_s3()
        body = await s3.download_tile(_REGISTRY_KEY)
        if body is None:
            return None
        return _parse_json_or_none(body, _REGISTRY_KEY)

    # --------------------------------------------------------------- internals

    def _require_s3(self) -> S3Client:
        if self._s3 is None:
            raise WeatherStationsNotConfiguredError(
                "Weather stations service is not configured (no S3 client attached)."
            )
        return self._s3

    async def _read_station_count(self, snapshot_key: str) -> int:
        meta_key = snapshot_key[: -len(".json")] + _SNAPSHOT_META_SUFFIX
        s3 = self._require_s3()
        body = await s3.download_tile(meta_key)
        if body is None:
            return 0
        meta = _parse_json_or_none(body, meta_key)
        if not isinstance(meta, dict):
            return 0
        count = meta.get("station_count")
        return int(count) if isinstance(count, int) else 0

    async def _list_snapshot_keys_recent(self) -> List[str]:
        """LIST every snapshot key under the snapshots prefix (cached)."""
        return await self._cached_list(_SNAPSHOTS_PREFIX)

    async def _list_snapshot_keys_for_window(
        self, window_start: datetime, target: datetime
    ) -> List[str]:
        """LIST every snapshot key covering the day prefixes the window touches."""
        prefixes = _day_prefixes_covering(window_start, target)
        # Run per-day LISTs in parallel; concat the results.
        per_day = await asyncio.gather(*(self._cached_list(p) for p in prefixes))
        merged: List[str] = []
        for chunk in per_day:
            merged.extend(chunk)
        return merged

    async def _cached_list(self, prefix: str) -> List[str]:
        now = time.monotonic()
        cached = self._list_cache.get(prefix)
        if cached is not None and cached[0] > now:
            return cached[1]
        async with self._list_cache_lock:
            # Re-check under the lock so a stampede on the same prefix collapses
            # to a single S3 call.
            cached = self._list_cache.get(prefix)
            if cached is not None and cached[0] > time.monotonic():
                return cached[1]
            keys = await self._require_s3().list_object_keys(prefix)
            self._list_cache[prefix] = (
                time.monotonic() + self._list_cache_ttl,
                keys,
            )
            return keys


def _parse_snapshot_key(key: str) -> Optional[datetime]:
    """Parse `YYYYMMDDTHHMMSSZ` out of a snapshot key into a UTC datetime."""
    match = _SNAPSHOT_KEY_RE.match(key)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(p) for p in match.groups())
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_tileset_id(tileset_id: str) -> datetime:
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
        raise TilesetIdFormatError(
            f"Invalid tilesetId {tileset_id!r}: {exc}"
        ) from exc


def _tileset_id(ts: datetime) -> str:
    """Round a snapshot timestamp down to its hour-bucket tilesetId."""
    return ts.strftime("%Y%m%dT%H00Z")


def _day_prefixes_covering(start: datetime, end: datetime) -> List[str]:
    """Return `weather-stations/snapshots/YYYY/MM/DD/` prefixes covering [start, end]."""
    if end < start:
        start, end = end, start
    prefixes: List[str] = []
    day = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_day = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    while day <= end_day:
        prefixes.append(
            f"{_SNAPSHOTS_PREFIX}{day.strftime('%Y/%m/%d')}/"
        )
        day = day + timedelta(days=1)
    return prefixes


def _parse_json_or_none(body: bytes, key: str) -> Optional[dict]:
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Could not parse S3 object %s: %s", key, exc)
        return None
    if not isinstance(parsed, dict):
        logger.warning("S3 object %s is not a JSON object", key)
        return None
    return parsed


# Module-level singleton (configured via `configure(...)` in the lifespan).
weather_stations_service = WeatherStationsService()
