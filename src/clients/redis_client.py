"""
Redis Client.

Provides async Redis operations for tile storage, index management,
and sync observability. Replaces filesystem storage for satellite tiles
and provides a shared cache for radar tiles.
"""

import logging
from typing import Dict, List, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisClient:
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
        logger.info(f"Connected to Redis at {self._redis_url}")

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._redis:
            await self._redis.close()
            logger.info("Redis connection closed")

    async def health_check(self) -> bool:
        """Check if Redis is reachable."""
        try:
            if self._redis:
                await self._redis.ping()
                return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
        return False

    # ============== Satellite Tile Operations ==============

    async def store_satellite_tile(
        self, channel_dir: str, tileset_id: str, z: int, x: int, y: int, data: bytes
    ) -> None:
        """Store a satellite tile in Redis."""
        key = f"tile:sat:{channel_dir}/{tileset_id}/{z}/{x}/{y}"
        await self._redis.set(key, data)

    async def get_satellite_tile(
        self, channel_dir: str, tileset_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Get a satellite tile from Redis."""
        key = f"tile:sat:{channel_dir}/{tileset_id}/{z}/{x}/{y}"
        return await self._redis.get(key)

    # ============== Satellite Index Operations ==============

    async def add_satellite_tileset(
        self, channel_dir: str, tileset_id: str, score: float
    ) -> None:
        """Add a tileset to the satellite index (sorted set, score = timestamp)."""
        key = f"idx:sat:{channel_dir}"
        await self._redis.zadd(key, {tileset_id.encode(): score})

    async def get_satellite_tilesets(self, channel_dir: str) -> List[str]:
        """Get all tileset IDs for a channel, ordered by score (oldest first)."""
        key = f"idx:sat:{channel_dir}"
        members = await self._redis.zrange(key, 0, -1)
        return [m.decode() for m in members]

    async def delete_satellite_tileset(self, channel_dir: str, tileset_id: str) -> None:
        """Delete a tileset from Redis: remove index entry and all tile keys."""
        # Remove from index
        idx_key = f"idx:sat:{channel_dir}"
        await self._redis.zrem(idx_key, tileset_id.encode())

        # Delete all tile keys for this tileset
        pattern = f"tile:sat:{channel_dir}/{tileset_id}/*"
        cursor = 0
        while True:
            cursor, keys = await self._redis.scan(cursor, match=pattern, count=500)
            if keys:
                await self._redis.delete(*keys)
            if cursor == 0:
                break

    async def satellite_tileset_exists(self, channel_dir: str, tileset_id: str) -> bool:
        """Check if a tileset exists in the satellite index."""
        key = f"idx:sat:{channel_dir}"
        score = await self._redis.zscore(key, tileset_id.encode())
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
        """Store a radar tile in Redis with TTL."""
        key = f"tile:radar:{radar_id}/{variable_id}/{tileset_id}_{elevation_id}/{z}/{x}/{y}"
        await self._redis.set(key, data, ex=ttl)

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
        """Get a radar tile from Redis."""
        key = f"tile:radar:{radar_id}/{variable_id}/{tileset_id}_{elevation_id}/{z}/{x}/{y}"
        return await self._redis.get(key)

    # ============== Radar Index Operations ==============

    async def add_radar_index(
        self,
        radar_id: str,
        variable_id: str,
        elevation_id: str,
        tileset_id: str,
        ttl: int = 3600,
    ) -> None:
        """Add entries to radar index sets with TTL."""
        pipe = await self._redis.pipeline()

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
        pipe.sadd(tilesets_key, tileset_id.encode())
        pipe.expire(tilesets_key, ttl)

        await pipe.execute()

    async def get_radar_radars(self) -> List[str]:
        """Get all radar IDs."""
        members = await self._redis.smembers("idx:radar:radars")
        return sorted(m.decode() for m in members)

    async def get_radar_variables(self, radar_id: str) -> List[str]:
        """Get all variable IDs for a radar."""
        members = await self._redis.smembers(f"idx:radar:{radar_id}:variables")
        return sorted(m.decode() for m in members)

    async def get_radar_elevations(self, radar_id: str, variable_id: str) -> List[str]:
        """Get all elevation IDs for a radar/variable."""
        members = await self._redis.smembers(
            f"idx:radar:{radar_id}:{variable_id}:elevations"
        )
        return sorted(m.decode() for m in members)

    async def get_radar_tilesets(
        self, radar_id: str, variable_id: str, elevation_id: str
    ) -> List[str]:
        """Get all tileset IDs for a radar/variable/elevation."""
        members = await self._redis.smembers(
            f"idx:radar:{radar_id}:{variable_id}:{elevation_id}:tilesets"
        )
        return sorted((m.decode() for m in members), reverse=True)

    # ============== Sync Status Operations ==============

    async def update_sync_status(self, fields: Dict[str, str]) -> None:
        """Update sync status hash fields."""
        if fields:
            encoded = {k.encode(): str(v).encode() for k, v in fields.items()}
            await self._redis.hset("sync:status", mapping=encoded)

    async def get_sync_status(self) -> Dict[str, str]:
        """Get all sync status fields."""
        raw = await self._redis.hgetall("sync:status")
        return {k.decode(): v.decode() for k, v in raw.items()}
