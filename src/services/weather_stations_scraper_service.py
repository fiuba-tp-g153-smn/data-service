"""Periodic scraper for SMN weather stations (observations + registry)."""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from clients.smn_api_client import SmnApiClient, SmnApiError
from clients.smn_registry_client import SmnRegistryClient, SmnRegistryError
from services.base_sync_service import BaseSyncService
from services.smn_stations_registry import (
    parse_estaciones_txt,
    station_metadata_to_jsonable,
)
from services.weather_stations_cache import (
    compute_tilesets_entries,
    latest_key,
    recent_snapshot_keys,
    registry_key,
    snap_body_key,
    tilesets_key,
)
from settings import Settings

logger = logging.getLogger(__name__)

# S3 key conventions for this subsystem. All under the `weather-stations/`
# prefix so a future bucket-shared deployment can coexist with other domains.
_S3_PREFIX = "weather-stations"
_LATEST_KEY = f"{_S3_PREFIX}/latest.json"
_REGISTRY_KEY = f"{_S3_PREFIX}/stations.json"
_REGISTRY_META_KEY = f"{_S3_PREFIX}/stations.meta.json"
_LIFECYCLE_RULE_ID = "weather-stations-expiration"


def _snapshot_key(ts: datetime) -> str:
    """`weather-stations/snapshots/YYYY/MM/DD/HH/YYYYMMDDTHHMMSSZ.json`."""
    base = ts.strftime("%Y%m%dT%H%M%SZ")
    return f"{_S3_PREFIX}/snapshots/{ts.strftime('%Y/%m/%d/%H')}/{base}.json"


def _snapshot_meta_key(snapshot_key: str) -> str:
    return snapshot_key[: -len(".json")] + ".meta.json"


class WeatherStationsScraperService(  # pylint: disable=too-many-instance-attributes
    BaseSyncService
):
    """
    Periodic 5-min scraper: fetches `/weather/station` (observations) +
    refreshes the public EMA registry, persisting both to S3 and write-through
    to Redis so the read path's shared cache stays warm.

    Lightweight by design: one provider, one endpoint, no cursor, no circuit
    breaker. On HTTP failure for the observations endpoint we skip the cycle and
    try again next tick. The registry refresh is fully independent and fail-soft.
    All Redis write-throughs are fail-soft — a Redis hiccup never aborts a scrape.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        settings: Settings,
        s3_client: S3Client,
        smn_client: SmnApiClient,
        registry_client: SmnRegistryClient,
        redis_client: Optional[RedisClient] = None,
    ):
        super().__init__(
            settings=settings,
            sync_interval=settings.weather_stations_scrape_interval_seconds,
            service_name="WeatherStationsScraperService",
        )
        self._s3 = s3_client
        self._smn = smn_client
        self._registry = registry_client
        self._source_url = f"{settings.smn_api_base_url.rstrip('/')}/weather/station"
        self._s3_object_ttl_days = settings.weather_stations_s3_object_ttl_days
        # Write-through cache. Optional: when absent, the read path still warms
        # the cache lazily on cold-boot reads.
        self._redis = redis_client
        self._cache_enabled = (
            settings.weather_stations_redis_cache_enabled and redis_client is not None
        )
        self._latest_ttl = settings.weather_stations_redis_latest_ttl_seconds
        self._tilesets_ttl = settings.weather_stations_redis_tilesets_ttl_seconds
        self._registry_ttl = settings.weather_stations_redis_registry_ttl_seconds
        self._snapshot_ttl = settings.weather_stations_redis_snapshot_ttl_seconds
        self._animation_warm_buckets = (
            settings.weather_stations_redis_animation_warm_buckets
        )
        # Lazy-applied lifecycle policy (self-heals from S3-down boot).
        self._lifecycle_applied = False
        # In-memory cache of the registry hash so we only PUT when the upstream
        # actually changed. Bootstrapped from S3 on the first cycle.
        self._registry_hash: Optional[str] = None
        self._registry_bootstrapped = False

    def _get_lock_path(self) -> str:
        return self._settings.weather_stations_scrape_lock_path

    async def _run_sync(self) -> None:
        """Execute one scrape cycle."""
        cycle_start = time.monotonic()
        await self._ensure_lifecycle_applied()
        await self._maybe_bootstrap_registry_hash()

        # Registry refresh is independent of the observations scrape.
        await self._refresh_registry_if_changed()

        try:
            raw_observations = await self._smn.fetch_current_weather_stations()
        except SmnApiError as exc:
            logger.warning("Weather stations scrape skipped: SMN fetch failed: %s", exc)
            return

        scraped_at = datetime.now(timezone.utc).replace(microsecond=0)
        snapshot = self._build_snapshot(scraped_at, raw_observations)
        snapshot_bytes = json.dumps(snapshot, separators=(",", ":")).encode("utf-8")

        try:
            await self._upload_snapshot(
                scraped_at, snapshot_bytes, len(snapshot["stations"])
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Weather stations scrape: S3 upload failed: %s", exc)
            return

        elapsed = time.monotonic() - cycle_start
        logger.info(
            "Weather stations scrape complete: %d stations in %.1fs",
            len(snapshot["stations"]),
            elapsed,
        )

        # Write-through so the read path's shared cache stays warm (fail-soft;
        # runs after the timing log so it doesn't skew the scrape duration).
        await self._warm_observation_cache(snapshot_bytes)
        # Pre-warm the animation window's snapshot bodies so timeline playback
        # of the latest N buckets is served entirely from Redis (no S3 reads).
        await self._warm_recent_snapshot_bodies()

    async def _ensure_lifecycle_applied(self) -> None:
        """Idempotently set the bucket lifecycle rule; latches on success."""
        if self._lifecycle_applied:
            return
        if await self._s3.ensure_lifecycle_expiration(
            self._s3_object_ttl_days, rule_id=_LIFECYCLE_RULE_ID
        ):
            self._lifecycle_applied = True

    async def _maybe_bootstrap_registry_hash(self) -> None:
        """One-shot read of the stored registry meta so we can no-op on match."""
        if self._registry_bootstrapped:
            return
        meta_bytes = await self._s3.download_tile(_REGISTRY_META_KEY)
        if meta_bytes is not None:
            try:
                meta = json.loads(meta_bytes.decode("utf-8"))
                stored = meta.get("source_hash")
                if isinstance(stored, str):
                    self._registry_hash = stored
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                logger.warning("Stored registry meta unreadable, will rewrite: %s", exc)
        self._registry_bootstrapped = True

    async def _refresh_registry_if_changed(self) -> None:
        """Fail-soft: skip if download/unzip fails; carry on with existing copy."""
        try:
            text = await self._registry.fetch_registry_text()
        except SmnRegistryError as exc:
            logger.warning("Registry refresh skipped: %s", exc)
            return

        source_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if source_hash == self._registry_hash:
            return

        stations = parse_estaciones_txt(text)
        if not stations:
            logger.warning("Registry refresh skipped: parser returned 0 stations")
            return

        registry_payload = {
            "fetched_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "source_url": self._settings.smn_stations_registry_url,
            "stations": station_metadata_to_jsonable(stations),
        }
        meta_payload = {
            "source_hash": source_hash,
            "station_count": len(stations),
            "updated_at": registry_payload["fetched_at"],
        }
        registry_bytes = json.dumps(registry_payload, separators=(",", ":")).encode(
            "utf-8"
        )
        meta_bytes = json.dumps(meta_payload, separators=(",", ":")).encode("utf-8")

        try:
            await self._s3.upload_tile(
                _REGISTRY_KEY, registry_bytes, content_type="application/json"
            )
            await self._s3.upload_tile(
                _REGISTRY_META_KEY, meta_bytes, content_type="application/json"
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Registry upload to S3 failed: %s", exc)
            return

        previous = self._registry_hash
        self._registry_hash = source_hash
        logger.info(
            "Registry refreshed: %d stations (hash %s -> %s)",
            len(stations),
            (previous or "<none>")[:8],
            source_hash[:8],
        )
        # Write-through the fresh registry so /stations reads stay warm.
        await self._redis_set(registry_key(), registry_bytes, self._registry_ttl)

    async def _warm_observation_cache(self, snapshot_bytes: bytes) -> None:
        """Write-through latest + recomputed tilesets so reads stay warm (fail-soft)."""
        if not self._cache_enabled:
            return
        await self._redis_set(latest_key(), snapshot_bytes, self._latest_ttl)
        try:
            entries = await compute_tilesets_entries(self._s3)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Tilesets cache warm skipped (recompute failed): %s", exc)
            return
        await self._redis_set(
            tilesets_key(), json.dumps(entries).encode("utf-8"), self._tilesets_ttl
        )

    async def _warm_recent_snapshot_bodies(self) -> None:
        """Pre-warm the latest N buckets' snapshot bodies for animation playback.

        Bodies are immutable, so re-warming each cycle just refreshes the TTL so
        the animation window stays resident. Fail-soft per key — a Redis or S3
        hiccup on one body never aborts the scrape.
        """
        if not self._cache_enabled or self._animation_warm_buckets <= 0:
            return
        try:
            keys = await recent_snapshot_keys(self._s3, self._animation_warm_buckets)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Animation cache warm skipped (listing failed): %s", exc)
            return
        for key in keys:
            try:
                body = await self._s3.download_tile(key)
            except Exception as exc:  # pylint: disable=broad-exception-caught
                logger.warning(
                    "Animation cache warm: S3 read failed for %s: %s", key, exc
                )
                continue
            if body is not None:
                await self._redis_set(snap_body_key(key), body, self._snapshot_ttl)

    async def _redis_set(self, key: str, data: bytes, ttl: int) -> None:
        """Fail-soft write-through; a Redis error never aborts the scrape."""
        if not self._cache_enabled or self._redis is None:
            return
        try:
            await self._redis.cache_listing(key, data, ttl)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Redis write-through failed for %s: %s", key, exc)

    def _build_snapshot(
        self, scraped_at: datetime, raw: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Normalize SMN observation rows into our snapshot shape."""
        scraped_iso = scraped_at.isoformat().replace("+00:00", "Z")
        normalized: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            station_id = item.get("station_id")
            if not isinstance(station_id, int):
                continue
            normalized.append(
                {
                    "station_id": station_id,
                    "observed_at": item.get("date"),
                    "temperature": item.get("temperature"),
                    "feels_like": item.get("feels_like"),
                    "humidity": item.get("humidity"),
                    "pressure": item.get("pressure"),
                    "visibility": item.get("visibility"),
                    "weather": item.get("weather"),
                    "wind": item.get("wind"),
                }
            )
        return {
            "scraped_at": scraped_iso,
            "source_url": self._source_url,
            "stations": normalized,
        }

    async def _upload_snapshot(
        self, scraped_at: datetime, snapshot_bytes: bytes, station_count: int
    ) -> None:
        snapshot_key = _snapshot_key(scraped_at)
        meta_key = _snapshot_meta_key(snapshot_key)
        scraped_iso = scraped_at.isoformat().replace("+00:00", "Z")
        meta = {"scraped_at": scraped_iso, "station_count": station_count}
        meta_bytes = json.dumps(meta, separators=(",", ":")).encode("utf-8")

        await self._s3.upload_tile(
            snapshot_key, snapshot_bytes, content_type="application/json"
        )
        await self._s3.upload_tile(
            meta_key, meta_bytes, content_type="application/json"
        )
        await self._s3.upload_tile(
            _LATEST_KEY, snapshot_bytes, content_type="application/json"
        )
