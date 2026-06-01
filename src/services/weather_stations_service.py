"""Read-side service for the weather-stations endpoints.

Redis-first with S3 fallback. The scrape loop write-throughs the hot keys
(`cache:ws:latest` / `cache:ws:tilesets` / `cache:ws:registry`) so reads almost
always hit Redis; on a cold boot (or for the parametrised `?N` historical query,
which the scraper can't pre-warm) the read path falls back to S3 and writes the
result back. S3 stays the source of truth — a Redis outage degrades to the
previous S3-only behaviour (read errors treated as a miss; writes fire-and-forget).
"""

import asyncio
import json
import logging
import time
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.weather_stations_cache import (
    SNAPSHOT_META_SUFFIX,
    TilesetIdFormatError,
    compute_tilesets_entries,
    day_prefixes_covering,
    extract_station_series,
    latest_key,
    parse_json_or_none,
    parse_snapshot_key,
    parse_tileset_id,
    recent_snapshot_keys,
    registry_key,
    series_key,
    snap_body_key,
    tilesets_key,
)

logger = logging.getLogger(__name__)

# S3 keys for the always-overwritten singletons (snapshot layout lives in
# weather_stations_cache). Re-export TilesetIdFormatError so existing imports
# from this module (routes, tests) keep working after the helper move.
_S3_PREFIX = "weather-stations"
_LATEST_KEY = f"{_S3_PREFIX}/latest.json"
_REGISTRY_KEY = f"{_S3_PREFIX}/stations.json"

__all__ = [
    "TilesetIdFormatError",
    "WeatherStationsNotConfiguredError",
    "WeatherStationsService",
    "weather_stations_service",
]


class WeatherStationsNotConfiguredError(Exception):
    """Raised when read endpoints are called but no S3 client is attached."""


class WeatherStationsService:  # pylint: disable=too-many-instance-attributes
    """
    Read service backing the public /weather-stations/* endpoints.

    Service is constructed at import time as a module-level singleton and
    populated via `configure(...)` from the lifespan, mirroring `BasemapService`.
    """

    def __init__(self) -> None:
        self._s3: Optional[S3Client] = None
        self._redis: Optional[RedisClient] = None
        self._cache_enabled: bool = False
        self._latest_ttl: int = 600
        self._tilesets_ttl: int = 600
        self._snapshot_ttl: int = 3600
        self._registry_ttl: int = 3600
        self._series_ttl: int = 3600
        self._list_cache_ttl: float = 0.0
        # Per-prefix cached LIST result: prefix -> (expires_monotonic, keys).
        self._list_cache: Dict[str, Tuple[float, List[str]]] = {}
        self._list_cache_lock = asyncio.Lock()

    def configure(  # pylint: disable=too-many-arguments
        self,
        s3_client: Optional[S3Client],
        list_cache_ttl: float,
        *,
        redis_client: Optional[RedisClient] = None,
        cache_enabled: bool = True,
        latest_ttl: int = 600,
        tilesets_ttl: int = 600,
        snapshot_ttl: int = 3600,
        registry_ttl: int = 3600,
        series_ttl: int = 3600,
    ) -> None:
        """Attach runtime dependencies. Pass `None` for `s3_client` when disabled.

        Redis is optional: when `redis_client` is `None` (or `cache_enabled` is
        `False`) the service resolves every read directly against S3.
        """
        self._s3 = s3_client
        self._list_cache_ttl = list_cache_ttl
        self._redis = redis_client
        self._cache_enabled = cache_enabled and redis_client is not None
        self._latest_ttl = latest_ttl
        self._tilesets_ttl = tilesets_ttl
        self._snapshot_ttl = snapshot_ttl
        self._registry_ttl = registry_ttl
        self._series_ttl = series_ttl

    # ------------------------------------------------------------------ /latest

    async def get_latest_snapshot(self) -> Optional[dict]:
        """Return the most recent snapshot (parsed JSON) or `None` if not yet scraped."""
        return await self._get_cached_object(
            latest_key(), _LATEST_KEY, self._latest_ttl
        )

    # ----------------------------------------------------------------- /stations

    async def get_stations_registry(self) -> Optional[dict]:
        """Return the parsed station registry, or `None` if not yet populated."""
        return await self._get_cached_object(
            registry_key(), _REGISTRY_KEY, self._registry_ttl
        )

    # ---------------------------------------------------------------- /tilesets

    async def list_tilesets(self) -> List[dict]:
        """
        Return the hour-bucketed list of available snapshots.

        Redis-first; on a miss recompute from S3 (shared with the scraper's
        write-through) and cache the assembled list. `scraped_at` is an ISO-8601
        string so the route's `TilesetsResponse.model_validate` re-parses it.
        """
        cache_key = tilesets_key()
        cached = await self._cache_get(cache_key)
        if cached is not None:
            try:
                entries = json.loads(cached)
                if isinstance(entries, list):
                    return entries
            except json.JSONDecodeError:
                pass  # corrupt cache → recompute

        entries = await compute_tilesets_entries(
            self._require_s3(), list_keys=self._cached_list
        )
        self._cache_set_bg(
            cache_key, json.dumps(entries).encode("utf-8"), self._tilesets_ttl
        )
        return entries

    # -------------------------------------------------------- /{tilesetId}?N=...

    async def get_snapshot_for_tileset(
        self, tileset_id: str, tolerance_hours: float
    ) -> Optional[dict]:
        """
        Resolve a tilesetId + N-hour tolerance to a snapshot.

        Picks the latest snapshot whose scraped_at falls in
        `[T - N*3600, T]` where T = tileset_id parsed as UTC. Resolves the
        matching snapshot key via the (in-process cached) window LIST, then
        serves its body from Redis (`cache:ws:snap:{key}`, N-independent) with an
        S3 fallback + write-back. Raises `TilesetIdFormatError` on a malformed id.
        """
        if tolerance_hours < 0:
            raise ValueError("tolerance_hours must be >= 0")
        target = parse_tileset_id(tileset_id)
        window_start = target - timedelta(hours=tolerance_hours)
        keys = await self._list_snapshot_keys_for_window(window_start, target)

        best_ts = None
        best_key: Optional[str] = None
        for key in keys:
            if key.endswith(SNAPSHOT_META_SUFFIX):
                continue
            ts = parse_snapshot_key(key)
            if ts is None or ts < window_start or ts > target:
                continue
            if best_ts is None or ts > best_ts:
                best_ts = ts
                best_key = key

        if best_key is None:
            return None

        # Body cache keyed by the resolved S3 object — shared across all tolerances
        # (and pre-warmed by the scraper for the animation window).
        return await self._get_snapshot_body(best_key)

    # ----------------------------------------------- /station/{id}/series?hours=

    async def get_station_series(self, station_id: int, hours: int) -> dict:
        """Bundle one station's last-`hours` history into a single response dict.

        The whole feature is one payload: the pre-pivoted points (Redis-first via
        `series_key`, rebuilt from the recent snapshot bodies on a cold miss),
        plus the station's name/province and the `latest` point — so the frontend
        makes exactly one request. `points: []` when the station has no readings.
        """
        points = await self._station_series_points(station_id, hours)
        name, province = await self._station_meta(station_id)
        return {
            "station_id": station_id,
            "station_name": name,
            "province": province,
            "hours": hours,
            "points": points,
            "latest": points[-1] if points else None,
        }

    async def _station_series_points(self, station_id: int, hours: int) -> List[dict]:
        """Redis-first list of series points; rebuild + write-back on a cold miss."""
        cache_key = series_key(station_id)
        cached = await self._cache_get(cache_key)
        if cached is not None:
            try:
                points = json.loads(cached)
                if isinstance(points, list):
                    return points
            except json.JSONDecodeError:
                pass  # corrupt cache → rebuild

        keys = await recent_snapshot_keys(
            self._require_s3(), hours, list_keys=self._cached_list
        )
        bodies = await asyncio.gather(*(self._get_snapshot_body(k) for k in keys))
        points = extract_station_series(
            [b for b in bodies if b is not None], station_id
        )
        self._cache_set_bg(
            cache_key, json.dumps(points).encode("utf-8"), self._series_ttl
        )
        return points

    async def _station_meta(
        self, station_id: int
    ) -> Tuple[Optional[str], Optional[str]]:
        """Look up a station's (name, province) from the warm registry cache."""
        registry = await self.get_stations_registry()
        if not isinstance(registry, dict):
            return None, None
        for entry in registry.get("stations", []):
            if isinstance(entry, dict) and entry.get("station_id") == station_id:
                return entry.get("name"), entry.get("province")
        return None, None

    # --------------------------------------------------------------- internals

    async def _get_snapshot_body(self, snapshot_key: str) -> Optional[dict]:
        """Redis-first read of one snapshot body (`cache:ws:snap:{key}`) + S3 fallback.

        Shared by the `?N` resolution and the series rebuild; the cached body is
        pre-warmed by the scraper for the animation window.
        """
        cache_key = snap_body_key(snapshot_key)
        cached = await self._cache_get(cache_key)
        if cached is not None:
            parsed = parse_json_or_none(cached, cache_key)
            if parsed is not None:
                return parsed
        body = await self._require_s3().download_tile(snapshot_key)
        if body is None:
            return None
        self._cache_set_bg(cache_key, body, self._snapshot_ttl)
        return parse_json_or_none(body, snapshot_key)

    async def _get_cached_object(
        self, cache_key: str, s3_key: str, ttl: int
    ) -> Optional[dict]:
        """Redis-first read of a single JSON object, with S3 fallback + write-back."""
        cached = await self._cache_get(cache_key)
        if cached is not None:
            parsed = parse_json_or_none(cached, cache_key)
            if parsed is not None:
                return parsed

        body = await self._require_s3().download_tile(s3_key)
        if body is None:
            return None
        self._cache_set_bg(cache_key, body, ttl)
        return parse_json_or_none(body, s3_key)

    async def _cache_get(self, cache_key: str) -> Optional[bytes]:
        """Read a cached value, treating any Redis error as a miss."""
        if not self._cache_enabled or self._redis is None:
            return None
        try:
            return await self._redis.get_cached_listing(cache_key)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(
                "Redis read failed for %s: %s (falling back to S3)", cache_key, exc
            )
            return None

    def _cache_set_bg(self, cache_key: str, data: bytes, ttl: int) -> None:
        """Fire-and-forget write-back; never blocks or fails the read path."""
        if not self._cache_enabled or self._redis is None:
            return
        asyncio.create_task(self._cache_set(cache_key, data, ttl))

    async def _cache_set(self, cache_key: str, data: bytes, ttl: int) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.cache_listing(cache_key, data, ttl)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Redis write failed for %s: %s", cache_key, exc)

    def _require_s3(self) -> S3Client:
        if self._s3 is None:
            raise WeatherStationsNotConfiguredError(
                "Weather stations service is not configured (no S3 client attached)."
            )
        return self._s3

    async def _list_snapshot_keys_for_window(self, window_start, target) -> List[str]:
        """LIST every snapshot key covering the day prefixes the window touches."""
        prefixes = day_prefixes_covering(window_start, target)
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


# Module-level singleton (configured via `configure(...)` in the lifespan).
weather_stations_service = WeatherStationsService()
