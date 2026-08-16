"""
Redis Client.

Provides async Redis operations for tile storage, index management,
and sync observability. Replaces filesystem storage for satellite tiles
and provides a shared cache for radar tiles.
"""

import logging
from typing import Any, AsyncIterator, Dict, List, Optional

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)


class RedisClient:  # pylint: disable=too-many-positional-arguments,too-many-public-methods
    """
    Async Redis client for tile storage and sync metadata.

    Uses binary mode (decode_responses=False) to handle raw webp tile bytes.
    Provides domain-specific methods for satellite tiles, radar tiles,
    index management, and sync status tracking.
    """

    def __init__(self, redis_url: str):
        self._redis_url = redis_url
        self._redis: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        """Connect to Redis."""
        self._redis = aioredis.from_url(self._redis_url, decode_responses=False)
        logger.info("Connected to Redis at %s", self._redis_url)

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._redis:
            await self._redis.close()
            logger.info("Redis connection closed")

    async def health_check(self) -> bool:
        """Check if Redis is reachable."""
        try:
            if self._redis:
                await self._redis.ping()  # type: ignore[misc]
                return True
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Redis health check failed: %s", e)
        return False

    @property
    def _conn(self) -> aioredis.Redis:
        """Return the Redis connection, raising if not connected."""
        if self._redis is None:
            raise RuntimeError("Redis not connected")

        return self._redis

    # ============== Satellite Tile Operations ==============

    async def store_satellite_tile(
        self,
        channel_dir: str,
        tileset_id: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Store a satellite tile in Redis, optionally with TTL."""
        key = f"tile:sat:{channel_dir}/{tileset_id}/{z}/{x}/{y}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_satellite_tile(
        self, channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get a satellite tile from Redis."""
        key = f"tile:sat:{channel_dir}/{tileset_id}/{z}/{x}/{y}"
        return await self._conn.get(key)

    # ============== Satellite Index Operations ==============

    async def add_satellite_tileset(
        self,
        channel_dir: str,
        tileset_id: str,
        score: float,
        ttl: Optional[int] = None,
    ) -> None:
        """Add a tileset to the satellite index (sorted set, score = timestamp)."""
        key = f"idx:sat:{channel_dir}"
        await self._conn.zadd(key, {tileset_id.encode(): score})
        if ttl:
            await self._conn.expire(key, ttl)

    async def get_satellite_tilesets(self, channel_dir: str) -> List[str]:
        """Get all tileset IDs for a channel, ordered by score (oldest first)."""
        key = f"idx:sat:{channel_dir}"
        members = await self._conn.zrange(key, 0, -1)
        return [m.decode() for m in members]

    async def delete_satellite_tileset(self, channel_dir: str, tileset_id: str) -> None:
        """Delete a tileset from Redis: remove index entry and all tile keys."""
        # Remove from index
        idx_key = f"idx:sat:{channel_dir}"
        await self._conn.zrem(idx_key, tileset_id.encode())

        # Delete all tile keys for this tileset
        pattern = f"tile:sat:{channel_dir}/{tileset_id}/*"
        cursor = 0
        while True:
            cursor, keys = await self._conn.scan(cursor, match=pattern, count=500)
            if keys:
                await self._conn.delete(*keys)
            if cursor == 0:
                break

    async def trim_satellite_index(self, channel_dir: str, min_score: float) -> int:
        """Drop tilesets older than min_score (epoch seconds) from the index.

        Keeps the index bounded to the live-tile window: tiles are stored with
        ex=tile_ttl at insertion, so members scored before now-tile_ttl no longer
        have data behind them. Returns the number of members removed.
        """
        key = f"idx:sat:{channel_dir}"
        return await self._conn.zremrangebyscore(key, "-inf", f"({min_score}")

    async def satellite_tileset_exists(self, channel_dir: str, tileset_id: str) -> bool:
        """Check if a tileset exists in the satellite index."""
        key = f"idx:sat:{channel_dir}"
        score = await self._conn.zscore(key, tileset_id.encode())
        return score is not None

    # ============== Radar Tile Operations ==============

    async def store_radar_tile(
        self,
        radar_id: str,
        variable_id: str,
        tileset_id: str,
        elevation_id: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        ttl: int = 3600,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Store a radar tile in Redis with TTL."""
        key = f"tile:radar:{radar_id}/{variable_id}/{tileset_id}_{elevation_id}/{z}/{x}/{y}"
        await self._conn.set(key, data, ex=ttl)

    async def get_radar_tile(
        self,
        radar_id: str,
        variable_id: str,
        tileset_id: str,
        elevation_id: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments
        """Get a radar tile from Redis."""
        key = f"tile:radar:{radar_id}/{variable_id}/{tileset_id}_{elevation_id}/{z}/{x}/{y}"
        return await self._conn.get(key)

    # ============== Radar Index Operations ==============

    async def add_radar_index(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        tileset_id: str,
        score: float,
        ttl: int = 3600,
    ) -> None:
        """Add entries to radar index sets with TTL.

        The tilesets axis is a sorted set scored by insertion time so it can be
        trimmed to the live-tile window (see trim_radar_index); the radar/variable/
        elevation dimensions are low-cardinality plain sets.
        """
        pipe = await self._conn.pipeline()

        radars_key = "idx:radar:radars"
        pipe.sadd(radars_key, radar_id.encode())
        pipe.expire(radars_key, ttl)

        vars_key = f"idx:radar:{radar_id}:variables"
        pipe.sadd(vars_key, variable_id.encode())
        pipe.expire(vars_key, ttl)

        elevs_key = f"idx:radar:{radar_id}:{variable_id}:elevations"
        pipe.sadd(elevs_key, elevation_id.encode())
        pipe.expire(elevs_key, ttl)

        tilesets_key = f"idx:radar:{radar_id}:{variable_id}:{elevation_id}:tilesets"
        pipe.zadd(tilesets_key, {tileset_id.encode(): score})
        pipe.expire(tilesets_key, ttl)

        await pipe.execute()

    async def get_radar_radars(self) -> List[str]:
        """Get all radar IDs."""
        members = await self._conn.smembers("idx:radar:radars")  # type: ignore[misc]
        return sorted(m.decode() for m in members)

    async def get_radar_variables(self, radar_id: str) -> List[str]:
        """Get all variable IDs for a radar."""
        members = await self._conn.smembers(f"idx:radar:{radar_id}:variables")  # type: ignore[misc]
        return sorted(m.decode() for m in members)

    async def get_radar_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        """Get all elevation IDs for a radar/variable."""
        members = await self._conn.smembers(  # type: ignore[misc]
            f"idx:radar:{radar_id}:{variable_id}:elevations"
        )
        return sorted(m.decode() for m in members)

    async def get_radar_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ) -> List[str]:
        """Get all tileset IDs for a radar/variable/elevation (newest first).

        Self-heals legacy plain-set keys left by the set→zset index migration:
        a WRONGTYPE means a pre-migration key, which we drop so the next sync
        recreates it as a sorted set.
        """
        key = f"idx:radar:{radar_id}:{variable_id}:{elevation_id}:tilesets"
        try:
            members = await self._conn.zrange(key, 0, -1)
        except ResponseError as exc:
            if "WRONGTYPE" not in str(exc):
                raise
            await self._conn.delete(key)
            return []
        return sorted((m.decode() for m in members), reverse=True)

    async def trim_radar_index(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        min_score: float,
    ) -> int:
        """Drop tilesets older than min_score (epoch seconds) from a radar elevation.

        Keeps the index bounded to the live-tile window, the radar analogue of
        trim_satellite_index. Returns the number of members removed.
        """
        key = f"idx:radar:{radar_id}:{variable_id}:{elevation_id}:tilesets"
        return await self._conn.zremrangebyscore(key, "-inf", f"({min_score}")

    # ============== ECMWF Total Precipitation Tile Operations ==============

    async def store_ecmwf_tp_tile(
        self,
        forecast_ts: str,
        period_ts: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Store an ECMWF total precipitation tile in Redis, optionally with TTL."""
        key = f"tile:ecmwf_tp:{forecast_ts}/{period_ts}/{z}/{x}/{y}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_ecmwf_tp_tile(
        self, forecast_ts: str, period_ts: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get an ECMWF total precipitation tile from Redis."""
        key = f"tile:ecmwf_tp:{forecast_ts}/{period_ts}/{z}/{x}/{y}"
        return await self._conn.get(key)

    # ============== ECMWF Total Precipitation Index Operations ==============

    async def store_ecmwf_tp_index(
        self, forecast_ts: str, periods: List[str], ttl: int
    ) -> None:
        """Add forecast and its periods to the ECMWF total precipitation Redis index."""
        pipe = await self._conn.pipeline()

        forecasts_key = "idx:ecmwf_tp:forecasts"
        pipe.sadd(forecasts_key, forecast_ts.encode())
        pipe.expire(forecasts_key, ttl)

        periods_key = f"idx:ecmwf_tp:{forecast_ts}:periods"
        for period in periods:
            pipe.sadd(periods_key, period.encode())
        if periods:
            pipe.expire(periods_key, ttl)

        await pipe.execute()

    async def get_ecmwf_tp_forecasts(self) -> List[str]:
        """Get all available forecast timestamps, sorted descending."""
        members = await self._conn.smembers("idx:ecmwf_tp:forecasts")  # type: ignore[misc]
        return sorted((m.decode() for m in members), reverse=True)

    async def get_ecmwf_tp_periods(self, forecast_ts: str) -> List[str]:
        """Get all period timestamps for a forecast, sorted ascending."""
        members = await self._conn.smembers(  # type: ignore[misc]
            f"idx:ecmwf_tp:{forecast_ts}:periods"
        )
        return sorted(m.decode() for m in members)

    async def _prune_index_set(self, key: str, keep: List[str]) -> int:
        """Remove members of a plain-set index not present in `keep`.

        Reconciles a forecast set to the currently-active forecasts so it stays
        bounded; stale per-forecast sub-keys self-expire once no longer re-synced.
        Returns the number of members removed.
        """
        keepset = {k.encode() for k in keep}
        members = await self._conn.smembers(key)  # type: ignore[misc]
        stale = [m for m in members if m not in keepset]
        if stale:
            await self._conn.srem(key, *stale)  # type: ignore[misc]
        return len(stale)

    async def _prune_sorted_index(self, key: str, keep: List[str]) -> int:
        """Remove members of a *sorted-set* index not present in `keep`.

        The sorted-set sibling of `_prune_index_set`: applying the plain-set
        commands to a ZSET makes Redis answer WRONGTYPE, which the sync loop
        swallows as a per-domain error — the index then never shrinks, because
        its whole-key TTL is refreshed on every pass. Returns members removed.
        """
        keepset = {k.encode() for k in keep}
        members = await self._conn.zrange(key, 0, -1)  # type: ignore[misc]
        stale = [m for m in members if m not in keepset]
        if stale:
            await self._conn.zrem(key, *stale)  # type: ignore[misc]
        return len(stale)

    async def prune_ecmwf_tp_forecasts(self, keep: List[str]) -> int:
        """Reconcile the ECMWF-TP forecasts index to the active forecasts."""
        return await self._prune_index_set("idx:ecmwf_tp:forecasts", keep)

    # ============== ECMWF Mean Sea Level Pressure GeoJSON Operations ==============

    async def store_ecmwf_mslp_geojson(
        self,
        forecast_ts: str,
        timestamp_ts: str,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        """Store an ECMWF MSLP isobars GeoJSON in Redis, optionally with TTL."""
        key = f"geojson:ecmwf_mslp:{forecast_ts}/{timestamp_ts}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_ecmwf_mslp_geojson(
        self, forecast_ts: str, timestamp_ts: str
    ) -> Optional[bytes]:
        """Get an ECMWF MSLP isobars GeoJSON from Redis."""
        key = f"geojson:ecmwf_mslp:{forecast_ts}/{timestamp_ts}"
        return await self._conn.get(key)

    # ============== ECMWF Mean Sea Level Pressure Index Operations ==============

    async def store_ecmwf_mslp_index(
        self, forecast_ts: str, timestamps: List[str], ttl: int
    ) -> None:
        """Add forecast and its timestamps to the ECMWF MSLP Redis index."""
        pipe = await self._conn.pipeline()

        forecasts_key = "idx:ecmwf_mslp:forecasts"
        pipe.sadd(forecasts_key, forecast_ts.encode())
        pipe.expire(forecasts_key, ttl)

        timestamps_key = f"idx:ecmwf_mslp:{forecast_ts}:timestamps"
        for timestamp_ts in timestamps:
            pipe.sadd(timestamps_key, timestamp_ts.encode())
        if timestamps:
            pipe.expire(timestamps_key, ttl)

        await pipe.execute()

    async def get_ecmwf_mslp_forecasts(self) -> List[str]:
        """Get all available MSLP forecast timestamps, sorted descending."""
        members = await self._conn.smembers("idx:ecmwf_mslp:forecasts")  # type: ignore[misc]
        return sorted((m.decode() for m in members), reverse=True)

    async def get_ecmwf_mslp_timestamps(self, forecast_ts: str) -> List[str]:
        """Get all MSLP timestamps for a forecast, sorted ascending."""
        members = await self._conn.smembers(  # type: ignore[misc]
            f"idx:ecmwf_mslp:{forecast_ts}:timestamps"
        )
        return sorted(m.decode() for m in members)

    # ============== WRF Tile Operations ==============

    async def store_wrf_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Store a WRF tile in Redis, optionally with TTL."""
        key = f"tile:wrf:{product_id}/{init_tag}/{fxxx}/{z}/{x}/{y}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_wrf_tile(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments
        """Get a WRF tile from Redis."""
        key = f"tile:wrf:{product_id}/{init_tag}/{fxxx}/{z}/{x}/{y}"
        return await self._conn.get(key)

    # ============== WRF Index Operations ==============

    async def add_wrf_index(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        init_score: float,
        step_score: float,
        ttl: int,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Add entries to WRF index sorted sets with TTL."""
        pipe = await self._conn.pipeline()

        init_key = f"idx:wrf:{product_id}:init_runs"
        pipe.zadd(init_key, {init_tag.encode(): init_score})
        pipe.expire(init_key, ttl)

        steps_key = f"idx:wrf:{product_id}:{init_tag}:steps"
        pipe.zadd(steps_key, {fxxx.encode(): step_score})
        pipe.expire(steps_key, ttl)

        await pipe.execute()

    async def get_wrf_init_runs(self, product_id: str) -> List[str]:
        """Get all init run tags for a product, sorted descending (newest first)."""
        key = f"idx:wrf:{product_id}:init_runs"
        members = await self._conn.zrevrange(key, 0, -1)  # type: ignore[misc]
        return [m.decode() for m in members]

    async def get_wrf_steps(self, product_id: str, init_tag: str) -> List[str]:
        """Get all forecast steps for a product/init_tag, sorted ascending."""
        key = f"idx:wrf:{product_id}:{init_tag}:steps"
        members = await self._conn.zrange(key, 0, -1)  # type: ignore[misc]
        return [m.decode() for m in members]

    async def prune_wrf_inits(self, product_id: str, keep: List[str]) -> int:
        """Remove init_tag members of the WRF index not present in `keep`.

        idx:wrf:{product}:init_runs is a sorted set whose whole-key TTL is
        refreshed on every new step, so old inits never expire on their own.
        Reconcile it to the actively-synced inits; the per-init sub-keys
        (:steps, :layers, :overlays_done) self-expire once no longer re-synced.
        Returns the number of inits removed.
        """
        return await self._prune_sorted_index(f"idx:wrf:{product_id}:init_runs", keep)

    # ============== WRF GeoJSON / Layer Operations ==============

    async def store_wrf_geojson(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        layer: str,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Store a WRF GeoJSON layer (barbs / contours) in Redis."""
        key = f"geojson:wrf:{product_id}/{init_tag}/{fxxx}/{layer}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_wrf_geojson(
        self, product_id: str, init_tag: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Retrieve a WRF GeoJSON layer from Redis."""
        key = f"geojson:wrf:{product_id}/{init_tag}/{fxxx}/{layer}"
        return await self._conn.get(key)

    async def add_wrf_layers(
        self,
        product_id: str,
        init_tag: str,
        fxxx: str,
        layers: List[str],
        ttl: int,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Register the GeoJSON layers available for a WRF forecast step."""
        if not layers:
            return
        key = f"idx:wrf:{product_id}:{init_tag}:{fxxx}:layers"
        pipe = await self._conn.pipeline()
        for layer in layers:
            pipe.sadd(key, layer.encode())
        pipe.expire(key, ttl)
        await pipe.execute()

    async def get_wrf_layers(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> List[str]:
        """Get all GeoJSON layer names indexed for a forecast step."""
        key = f"idx:wrf:{product_id}:{init_tag}:{fxxx}:layers"
        members = await self._conn.smembers(key)  # type: ignore[misc]
        return sorted(m.decode() for m in members)

    @staticmethod
    def _wrf_overlays_done_key(product_id: str, init_tag: str, fxxx: str) -> str:
        return f"idx:wrf:{product_id}:{init_tag}:{fxxx}:overlays_done"

    async def set_wrf_overlays_complete(
        self, product_id: str, init_tag: str, fxxx: str, ttl: int
    ) -> None:
        """Mark a step's GeoJSON overlays as fully mirrored, so later cycles
        skip the per-step S3 layer LIST. Self-expires with the overlay TTL."""
        key = self._wrf_overlays_done_key(product_id, init_tag, fxxx)
        await self._conn.set(key, b"1", ex=ttl)

    async def is_wrf_overlays_complete(
        self, product_id: str, init_tag: str, fxxx: str
    ) -> bool:
        """True if a step's overlays were already fully mirrored to Redis."""
        key = self._wrf_overlays_done_key(product_id, init_tag, fxxx)
        return bool(await self._conn.exists(key))

    async def prune_ecmwf_mslp_forecasts(self, keep: List[str]) -> int:
        """Reconcile the ECMWF-MSLP forecasts index to the active forecasts."""
        return await self._prune_index_set("idx:ecmwf_mslp:forecasts", keep)

    # ============== GFS Index Operations ==============

    async def add_gfs_index(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        cycle_score: float,
        step_score: float,
        ttl: int,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Add a (cycle, step) pair to the GFS index sorted sets with TTL."""
        pipe = await self._conn.pipeline()

        cycles_key = f"idx:gfs:{product_id}:cycles"
        pipe.zadd(cycles_key, {cycle.encode(): cycle_score})
        pipe.expire(cycles_key, ttl)

        steps_key = f"idx:gfs:{product_id}:{cycle}:steps"
        pipe.zadd(steps_key, {fxxx.encode(): step_score})
        pipe.expire(steps_key, ttl)

        await pipe.execute()

    async def get_gfs_cycles(self, product_id: str) -> List[str]:
        """Get every indexed cycle for a product, newest first."""
        key = f"idx:gfs:{product_id}:cycles"
        members = await self._conn.zrevrange(key, 0, -1)  # type: ignore[misc]
        return [m.decode() for m in members]

    async def get_gfs_steps(self, product_id: str, cycle: str) -> List[str]:
        """Get every indexed forecast step of a cycle, ascending."""
        key = f"idx:gfs:{product_id}:{cycle}:steps"
        members = await self._conn.zrange(key, 0, -1)  # type: ignore[misc]
        return [m.decode() for m in members]

    async def prune_gfs_cycles(self, product_id: str, keep: List[str]) -> int:
        """Reconcile a product's cycle index to the actively-synced cycles.

        `idx:gfs:{product}:cycles` is a sorted set, so this must use the
        sorted-set prune; the plain-set one raises WRONGTYPE.
        """
        return await self._prune_sorted_index(f"idx:gfs:{product_id}:cycles", keep)

    # ============== GFS GeoJSON / Layer Operations ==============

    async def store_gfs_geojson(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        layer: str,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Store a single-file GFS overlay (isobars, thickness, heights, ...)."""
        key = f"geojson:gfs:{product_id}/{cycle}/{fxxx}/{layer}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_gfs_geojson(
        self, product_id: str, cycle: str, fxxx: str, layer: str
    ) -> Optional[bytes]:
        """Retrieve a single-file GFS overlay from Redis."""
        key = f"geojson:gfs:{product_id}/{cycle}/{fxxx}/{layer}"
        return await self._conn.get(key)

    async def add_gfs_layers(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        layers: List[str],
        ttl: int,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Register which overlay layers a forecast step actually has."""
        if not layers:
            return
        key = f"idx:gfs:{product_id}:{cycle}:{fxxx}:layers"
        pipe = await self._conn.pipeline()
        for layer in layers:
            pipe.sadd(key, layer.encode())
        pipe.expire(key, ttl)
        await pipe.execute()

    async def get_gfs_layers(self, product_id: str, cycle: str, fxxx: str) -> List[str]:
        """Get the overlay layers indexed for a forecast step."""
        key = f"idx:gfs:{product_id}:{cycle}:{fxxx}:layers"
        members = await self._conn.smembers(key)  # type: ignore[misc]
        return sorted(m.decode() for m in members)

    # ============== GFS Tile Operations (lazy, on-demand cache) ==============

    async def store_gfs_tile(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Cache one raster tile fetched on demand from S3."""
        key = f"tile:gfs:{product_id}/{cycle}/{fxxx}/{z}/{x}/{y}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_gfs_tile(
        self,
        product_id: str,
        cycle: str,
        fxxx: str,
        z: int,
        x: int,
        y: int,
    ) -> Optional[bytes]:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Get a cached GFS raster tile."""
        key = f"tile:gfs:{product_id}/{cycle}/{fxxx}/{z}/{x}/{y}"
        return await self._conn.get(key)

    # ============== Base Map Tile Operations ==============

    async def store_basemap_tile(
        self,
        provider_id: str,
        z: int,
        x: int,
        y: int,
        data: bytes,
        ttl: Optional[int] = None,
    ) -> None:
        """Store a base map tile in Redis, optionally with TTL."""
        key = f"tile:basemap:{provider_id}:{z}:{x}:{y}"
        if ttl:
            await self._conn.set(key, data, ex=ttl)
        else:
            await self._conn.set(key, data)

    async def get_basemap_tile(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get a base map tile from Redis."""
        key = f"tile:basemap:{provider_id}:{z}:{x}:{y}"
        return await self._conn.get(key)

    # ============== Base Map Tile Negative Cache ==============

    @staticmethod
    def _basemap_miss_key(provider_id: str, z: int, x: int, y: int) -> str:
        return f"tile:basemap:miss:{provider_id}:{z}:{x}:{y}"

    async def get_basemap_tile_miss(
        self, provider_id: str, z: int, x: int, y: int
    ) -> bool:
        """True if a recent miss tombstone exists for this tile."""
        return bool(
            await self._conn.exists(self._basemap_miss_key(provider_id, z, x, y))
        )

    async def mark_basemap_tile_miss(
        self, provider_id: str, z: int, x: int, y: int, ttl: int
    ) -> None:
        """Record a short-lived tombstone so subsequent lookups short-circuit."""
        await self._conn.set(self._basemap_miss_key(provider_id, z, x, y), b"1", ex=ttl)

    async def clear_basemap_tile_miss(
        self, provider_id: str, z: int, x: int, y: int
    ) -> None:
        """Remove the miss tombstone (called on positive writes, idempotent)."""
        await self._conn.delete(self._basemap_miss_key(provider_id, z, x, y))

    # ============== Base Map Provider Availability Cache ==============

    @staticmethod
    def _basemap_availability_key(provider_id: str) -> str:
        return f"basemap:availability:{provider_id}"

    async def set_basemap_provider_availability(
        self, provider_id: str, available: bool, ttl: int
    ) -> None:
        """Cache whether a basemap provider is currently available to serve."""
        await self._conn.set(
            self._basemap_availability_key(provider_id),
            b"1" if available else b"0",
            ex=ttl,
        )

    async def get_basemap_provider_availability(
        self, provider_id: str
    ) -> Optional[bool]:
        """Return cached provider-availability flag, or None on miss."""
        raw = await self._conn.get(self._basemap_availability_key(provider_id))
        if raw is None:
            return None
        return raw == b"1"

    # ============== Listing Cache Operations ==============

    async def cache_listing(self, cache_key: str, data: bytes, ttl: int) -> None:
        """Cache a JSON listing with TTL."""
        await self._conn.set(cache_key, data, ex=ttl)

    async def get_cached_listing(self, cache_key: str) -> Optional[bytes]:
        """Get a cached JSON listing."""
        return await self._conn.get(cache_key)

    # ============== Sync Status Operations ==============

    async def update_sync_status(self, fields: Dict[str, str]) -> None:
        """Update sync status hash fields."""
        if fields:
            encoded = {k.encode(): str(v).encode() for k, v in fields.items()}
            await self._conn.hset("sync:status", mapping=encoded)  # type: ignore[misc]

    async def get_sync_status(self) -> Dict[str, str]:
        """Get all sync status fields."""
        raw = await self._conn.hgetall("sync:status")  # type: ignore[misc]
        return {k.decode(): v.decode() for k, v in raw.items()}

    async def update_domain_sync_status(
        self, domain: str, fields: Dict[str, str]
    ) -> None:
        """Update the live status hash for a single product's sync loop.

        Each per-product loop owns its own ``sync:status:{domain}`` key, so the
        five loops never clobber one another's status.
        """
        if fields:
            encoded = {k.encode(): str(v).encode() for k, v in fields.items()}
            await self._conn.hset(  # type: ignore[misc]
                f"sync:status:{domain}", mapping=encoded
            )

    async def get_domain_sync_status(self, domain: str) -> Dict[str, str]:
        """Get the live status hash for a single product's sync loop."""
        raw = await self._conn.hgetall(f"sync:status:{domain}")  # type: ignore[misc]
        return {k.decode(): v.decode() for k, v in raw.items()}

    # ============== Introspection / Metrics Operations ==============

    async def scan_keys(
        self, match: Optional[str] = None, count: int = 1000
    ) -> AsyncIterator[bytes]:
        """Yield every key (raw bytes) matching `match`, scanning in batches.

        Cursor-based and non-blocking — safe to run against a live keyspace.
        """
        async for key in self._conn.scan_iter(match=match, count=count):
            yield key

    async def memory_usage_batch(self, keys: List[bytes]) -> List[Optional[int]]:
        """Return `MEMORY USAGE` (bytes) for each key, pipelined in one round-trip.

        An entry is `None` when the key vanished between SCAN and this call.
        """
        if not keys:
            return []
        pipe = self._conn.pipeline(transaction=False)
        for key in keys:
            pipe.memory_usage(key)
        return await pipe.execute()

    async def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """Return parsed Redis `INFO` (optionally a single section)."""
        if section:
            return await self._conn.info(section)  # type: ignore[misc]
        return await self._conn.info()  # type: ignore[misc]

    async def dbsize(self) -> int:
        """Return the number of keys in the current database."""
        return await self._conn.dbsize()  # type: ignore[misc]
