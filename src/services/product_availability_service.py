"""One snapshot of which products currently have data.

The frontend greys out products with nothing to show, and it used to learn that
by probing every product separately: 18 radars x 6 variables alone is 108 GETs,
re-run on a timer, per client. Every one of those answers comes from an index
already in Redis, so the whole fleet is a handful of pipelined reads — this
module gathers them into a single response.

Contributors are registered, not branched on: a new data domain implements
`AvailabilityContributor` and is passed in at startup.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Sequence, Tuple

from clients.redis_client import RedisClient

RADAR_DOMAIN = "radar-sinarame"
SATELLITE_DOMAIN = "goes19"
ECMWF_DOMAIN = "ecmwf-ifs"
WRF_DOMAIN = "wrf-arg4k"
GFS_DOMAIN = "gfs"


class AvailabilityContributor(Protocol):
    """One data domain's answer to "which of my products have data?"."""

    @property
    def domain(self) -> str:
        """Leading path segment shared by every key this contributor emits."""

    async def availability(self) -> Dict[str, bool]:
        """Map of product path -> has data. Keys are the product's API path."""


class RadarAvailability:
    """Radar, walked from the index rather than a static fleet list.

    Three pipelined round trips (variables, elevations, tileset counts) for the
    whole fleet, whatever its size — the index is the source of truth for which
    combinations exist, so a decommissioned radar drops out on its own.
    """

    domain = RADAR_DOMAIN

    def __init__(self, redis_client: RedisClient) -> None:
        self._redis = redis_client

    async def availability(self) -> Dict[str, bool]:
        """Every indexed radar/variable/elevation, by whether it has tilesets."""
        combos = await self._combinations()
        if not combos:
            return {}
        counts = await self._redis.count_radar_tilesets_bulk(combos)
        return {
            f"{self.domain}/{radar}/{variable}/{elevation}": counts.get(combo, 0) > 0
            for combo in combos
            for radar, variable, elevation in [combo]
        }

    async def _combinations(self) -> List[Tuple[str, str, str]]:
        """Every indexed (radar, variable, elevation), two round trips deep."""
        radars = await self._redis.get_radar_radars()
        if not radars:
            return []
        by_radar = await self._redis.get_radar_variables_bulk(radars)
        pairs = [(r, v) for r in radars for v in by_radar.get(r, [])]
        by_pair = await self._redis.get_radar_elevations_bulk(pairs)
        return [(r, v, e) for (r, v) in pairs for e in by_pair.get((r, v), [])]


class SatelliteAvailability:
    """GOES-19 ABI + GLM, whose channel catalogue is static and tiny."""

    domain = SATELLITE_DOMAIN

    def __init__(self, redis_client: RedisClient, channel_dirs: Sequence[str]) -> None:
        self._redis = redis_client
        # The channel dir IS the product path (`goes19/abi/c13`), so no mapping.
        self._channel_dirs = list(channel_dirs)

    async def availability(self) -> Dict[str, bool]:
        """Each catalogued channel, by whether its index holds tilesets."""
        counts = await self._redis.count_satellite_tilesets_bulk(self._channel_dirs)
        return {d: counts.get(d, 0) > 0 for d in self._channel_dirs}


class EcmwfTpAvailability:
    """ECMWF total precipitation — a single product, so a single read."""

    domain = ECMWF_DOMAIN

    def __init__(self, redis_client: RedisClient) -> None:
        self._redis = redis_client

    async def availability(self) -> Dict[str, bool]:
        """Total precipitation, by whether any forecast is indexed."""
        forecasts = await self._redis.get_ecmwf_tp_forecasts()
        return {f"{self.domain}/total-precipitation": bool(forecasts)}


class WrfAvailability:
    """WRF, enumerated from the index because its products come from S3."""

    domain = WRF_DOMAIN

    def __init__(self, redis_client: RedisClient) -> None:
        self._redis = redis_client

    async def availability(self) -> Dict[str, bool]:
        """Each indexed WRF product, by whether it has init runs."""
        products = await self._redis.get_wrf_products()
        if not products:
            return {}
        counts = await self._redis.count_wrf_init_runs_bulk(products)
        return {f"{self.domain}/{p}": counts.get(p, 0) > 0 for p in products}


class GfsAvailability:
    """GFS, whose three products are declared in `gfs_config`."""

    domain = GFS_DOMAIN

    def __init__(self, redis_client: RedisClient, product_ids: Sequence[str]) -> None:
        self._redis = redis_client
        self._product_ids = list(product_ids)

    async def availability(self) -> Dict[str, bool]:
        """Each catalogued GFS product, by whether it has cycles."""
        counts = await self._redis.count_gfs_cycles_bulk(self._product_ids)
        return {f"{self.domain}/{p}": counts.get(p, 0) > 0 for p in self._product_ids}


@dataclass(frozen=True, slots=True)
class ProductAvailabilitySnapshot:
    """One gathered answer: the product map plus the domains behind it."""

    products: Dict[str, bool] = field(default_factory=dict)
    domains: List[str] = field(default_factory=list)


class ProductAvailabilityService:
    """Gathers every contributor into one snapshot, memoised briefly.

    The memo is what makes this scale with clients rather than with clients x
    products: a room full of forecasters refreshing at the same moment collapses
    onto one walk of the indexes. It is deliberately short — availability
    changes when a sync cycle lands, and a few seconds of staleness on a greyed
    row costs nothing, while the ETag means an unchanged snapshot is a 304
    anyway.
    """

    def __init__(
        self, contributors: Sequence[AvailabilityContributor], ttl_seconds: float
    ) -> None:
        self._contributors = list(contributors)
        self._ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()
        self._cached: Optional[ProductAvailabilitySnapshot] = None
        self._cached_at = 0.0

    async def snapshot(self) -> ProductAvailabilitySnapshot:
        """The current map, recomputed at most once per `ttl_seconds`."""
        fresh = self._fresh()
        if fresh is not None:
            return fresh
        async with self._lock:
            # Another waiter may have refreshed it while we queued for the lock.
            fresh = self._fresh()
            if fresh is not None:
                return fresh
            snapshot = await self._gather()
            self._cached = snapshot
            self._cached_at = time.monotonic()
            return snapshot

    def _fresh(self) -> Optional[ProductAvailabilitySnapshot]:
        if self._cached is None:
            return None
        if time.monotonic() - self._cached_at >= self._ttl_seconds:
            return None
        return self._cached

    async def _gather(self) -> ProductAvailabilitySnapshot:
        """Ask every contributor, in parallel — there are a handful of them.

        A domain that returns nothing is reported as *not covered* rather than
        as a set of empty products. The distinction matters to the client: an
        index the sync loop has not populated yet means "unknown", and greying
        every WRF product out because the service restarted a minute ago would
        be a worse answer than saying nothing.
        """
        results = await asyncio.gather(*(c.availability() for c in self._contributors))
        products: Dict[str, bool] = {}
        domains: List[str] = []
        for contributor, result in zip(self._contributors, results):
            if not result:
                continue
            products.update(result)
            domains.append(contributor.domain)
        return ProductAvailabilitySnapshot(products, sorted(domains))

    def configure(
        self, contributors: Sequence[AvailabilityContributor], ttl_seconds: float
    ) -> None:
        """Attach the contributors at startup (DI by method call, as elsewhere)."""
        self._contributors = list(contributors)
        self._ttl_seconds = ttl_seconds
        self._cached = None
        self._cached_at = 0.0


def build_contributors(
    redis_client: RedisClient,
    satellite_channel_dirs: Sequence[str],
    gfs_product_ids: Sequence[str],
) -> List[AvailabilityContributor]:
    """Every domain that can answer for its products, in a stable order."""
    return [
        RadarAvailability(redis_client),
        SatelliteAvailability(redis_client, satellite_channel_dirs),
        EcmwfTpAvailability(redis_client),
        WrfAvailability(redis_client),
        GfsAvailability(redis_client, gfs_product_ids),
    ]


# Singleton instance, configured in the FastAPI lifespan.
product_availability_service = ProductAvailabilityService([], 0.0)
