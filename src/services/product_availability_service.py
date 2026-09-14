"""One snapshot of which products currently have data.

The frontend greys out products with nothing to show, and it used to learn that
by probing every product separately: 18 radars x 6 variables alone is 108 GETs,
re-run on a timer, per client. This gathers the same answers into one response.

**Every contributor reads through the same strategy the individual endpoint
reads through.** That is the whole design. An earlier version read the Redis
indexes directly and so answered from a different source than the endpoints it
summarised — which meant it could not be trusted, in either direction: reporting
emptiness greyed out products whose data was in S3 waiting for a sync, and
refusing to report emptiness left the client probing all 125 anyway. Going
through the strategies makes the snapshot *the probes, batched*: identical
answers by construction, so absence means empty and the client asks nothing.

The strategies are Redis-first with an S3 fallback, so a warm sweep is index
reads and a cold one does the S3 walk exactly once — server-side, memoised, and
shared by every client, instead of once per client per product.

Contributors are registered, not branched on: a new data domain implements
`AvailabilityContributor` and is passed in at startup.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import (
    Awaitable,
    Callable,
    List,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
)

RADAR_DOMAIN = "radar-sinarame"
SATELLITE_DOMAIN = "goes19"
ECMWF_DOMAIN = "ecmwf-ifs"
WRF_DOMAIN = "wrf-arg4k"
GFS_DOMAIN = "gfs"

# Cap on concurrent listing lookups inside one contributor. The strategies hit
# Redis when warm and S3 when cold; either way an 18-radar fleet must not put
# hundreds of calls in flight at once against a pool every domain shares.
_LOOKUP_CONCURRENCY = 8

T = TypeVar("T")
R = TypeVar("R")


async def _map_bounded(
    items: Sequence[T], call: Callable[[T], Awaitable[R]]
) -> List[R]:
    """Apply an async lookup to every item, at most N in flight."""
    if not items:
        return []
    semaphore = asyncio.Semaphore(_LOOKUP_CONCURRENCY)

    async def run(item: T) -> R:
        async with semaphore:
            return await call(item)

    return list(await asyncio.gather(*(run(item) for item in items)))


class AvailabilityContributor(Protocol):
    """One data domain's answer to "which of my products have data?"."""

    @property
    def domain(self) -> str:
        """Leading path segment shared by every path this contributor emits."""

    async def available(self) -> List[str]:
        """Product API paths that have data, read exactly as the endpoint reads."""


class RadarAvailability:
    """The radar fleet, enumerated the way `/products/radar-sinarame/...` is.

    Four levels — radars, variables, elevations, tilesets — each Redis-first
    with an S3 fallback, so the answer matches an individual probe whether the
    index is warm, cold, or half-written mid-sync.
    """

    domain = RADAR_DOMAIN

    def __init__(self, strategy) -> None:
        self._strategy = strategy

    async def available(self) -> List[str]:
        """Radar/variable/elevation combinations that have at least one tileset."""
        combos = await self._combinations()
        tilesets = await _map_bounded(
            combos, lambda c: self._strategy.list_tilesets(*c)
        )
        return [
            f"{self.domain}/{radar}/{variable}/{elevation}"
            for (radar, variable, elevation), found in zip(combos, tilesets)
            if found
        ]

    async def _combinations(self) -> List[Tuple[str, str, str]]:
        """Every (radar, variable, elevation) the listing endpoints would serve."""
        radars = await self._strategy.list_radars()
        variables = await _map_bounded(radars, self._strategy.list_variables)
        pairs = [(r, v) for r, vs in zip(radars, variables) for v in vs]
        elevations = await _map_bounded(
            pairs, lambda p: self._strategy.list_elevations(*p)
        )
        return [(r, v, e) for (r, v), es in zip(pairs, elevations) for e in es]


class SatelliteAvailability:
    """GOES-19 ABI + GLM. The channel catalogue is static; the data is not."""

    domain = SATELLITE_DOMAIN

    def __init__(self, strategy, channel_dirs: Sequence[str]) -> None:
        self._strategy = strategy
        # The channel dir IS the product path (`goes19/abi/c13`), so no mapping.
        self._channel_dirs = list(channel_dirs)

    async def available(self) -> List[str]:
        """Catalogued channels with at least one tileset."""
        tilesets = await _map_bounded(self._channel_dirs, self._strategy.get_tilesets)
        return [d for d, found in zip(self._channel_dirs, tilesets) if found]


class EcmwfTpAvailability:
    """ECMWF total precipitation — a single product, so a single lookup."""

    domain = ECMWF_DOMAIN

    def __init__(self, strategy) -> None:
        self._strategy = strategy

    async def available(self) -> List[str]:
        """Total precipitation, when it has at least one forecast."""
        forecasts = await self._strategy.list_forecasts()
        return [f"{self.domain}/total-precipitation"] if forecasts else []


class WrfAvailability:
    """WRF, whose products are discovered rather than declared."""

    domain = WRF_DOMAIN

    def __init__(self, strategy) -> None:
        self._strategy = strategy

    async def available(self) -> List[str]:
        """WRF products with at least one initialization run."""
        products = await self._strategy.list_products()
        init_runs = await _map_bounded(products, self._strategy.list_init_runs)
        return [f"{self.domain}/{p}" for p, found in zip(products, init_runs) if found]


class GfsAvailability:
    """GFS, whose three products are declared in `gfs_config`."""

    domain = GFS_DOMAIN

    def __init__(self, strategy, product_ids: Sequence[str]) -> None:
        self._strategy = strategy
        self._product_ids = list(product_ids)

    async def available(self) -> List[str]:
        """Catalogued GFS products with at least one cycle."""
        cycles = await _map_bounded(self._product_ids, self._strategy.list_cycles)
        return [
            f"{self.domain}/{p}" for p, found in zip(self._product_ids, cycles) if found
        ]


@dataclass(frozen=True, slots=True)
class ProductAvailabilitySnapshot:
    """The products that have data, and which domains reported any.

    `available` is complete: it is what the individual endpoints would say, so
    a product missing from it has no data and needs no probe.

    `domains` is diagnostic — which domains contributed at least one product.
    Useful for spotting a domain whose backing store is unreachable.
    """

    available: List[str] = field(default_factory=list)
    domains: List[str] = field(default_factory=list)


class ProductAvailabilityService:
    """Gathers every contributor into one snapshot, memoised briefly.

    The memo is what makes this scale with clients rather than with clients x
    products: a room full of forecasters refreshing at the same moment collapses
    onto one sweep. It is deliberately short — availability changes when a sync
    cycle lands, and a few seconds of staleness on a greyed row costs nothing,
    while the ETag means an unchanged snapshot is a 304 anyway.
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
        """Ask every contributor, in parallel — there are a handful of them."""
        results = await asyncio.gather(*(c.available() for c in self._contributors))
        available: List[str] = []
        domains: List[str] = []
        for contributor, paths in zip(self._contributors, results):
            if not paths:
                continue
            available.extend(paths)
            domains.append(contributor.domain)
        return ProductAvailabilitySnapshot(sorted(available), sorted(domains))

    def configure(
        self, contributors: Sequence[AvailabilityContributor], ttl_seconds: float
    ) -> None:
        """Attach the contributors at startup (DI by method call, as elsewhere)."""
        self._contributors = list(contributors)
        self._ttl_seconds = ttl_seconds
        self._cached = None
        self._cached_at = 0.0


def build_contributors(
    *,
    radar_strategy,
    satellite_strategy,
    satellite_channel_dirs: Sequence[str],
    ecmwf_tp_strategy,
    wrf_strategy,
    gfs_strategy,
    gfs_product_ids: Sequence[str],
) -> List[AvailabilityContributor]:
    """Every domain that can answer for its products, in a stable order."""
    return [
        RadarAvailability(radar_strategy),
        SatelliteAvailability(satellite_strategy, satellite_channel_dirs),
        EcmwfTpAvailability(ecmwf_tp_strategy),
        WrfAvailability(wrf_strategy),
        GfsAvailability(gfs_strategy, gfs_product_ids),
    ]


# Singleton instance, configured in the FastAPI lifespan.
product_availability_service = ProductAvailabilityService([], 0.0)
