"""Service for base map tile retrieval and provider listing."""

import asyncio
import logging
from typing import List, Optional

from clients.http_tile_client import HttpTileClient, ProviderUnavailableError
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from models.basemap import BasemapProviderInfo, BasemapProvidersResponse
from services.basemap_config import BasemapProvider, build_source_url
from services.basemap_tile_reader import BasemapTileReader

logger = logging.getLogger(__name__)


class BasemapNotConfiguredError(Exception):
    """Raised when basemap tiles are requested but the service is not configured."""


class BasemapService:
    # pylint: disable=too-many-instance-attributes
    """Service managing base map tile access.

    Constructed once at module import as a dependency-injection singleton
    (see `dependencies.py`) and populated during the FastAPI lifespan via
    `configure(...)` — mirroring how `RedisClient` is constructed at import
    and connected in lifespan.

    Before `configure` runs (or when basemap is disabled), the service
    answers `/basemap/providers` with an empty list and raises
    `BasemapNotConfiguredError` on tile requests.

    `/basemap/providers` actively probes each upstream in parallel and
    falls back to checking S3 when a probe fails. A provider is reported
    available when **either** the probe succeeds OR S3 has cached tiles
    for it. Results are cached in Redis under
    `basemap:availability:{provider_id}` so bursts of listing calls don't
    re-probe; per-provider single-flight collapses stampedes on TTL expiry.
    """

    def __init__(self) -> None:
        self._reader: Optional[BasemapTileReader] = None
        self._providers: dict[str, BasemapProvider] = {}
        self._online_fallback: bool = True
        self._s3: Optional[S3Client] = None
        self._redis: Optional[RedisClient] = None
        self._http: Optional[HttpTileClient] = None
        self._availability_ttl: int = 240
        self._availability_inflight: dict[str, "asyncio.Future[bool]"] = {}

    def configure(
        self,
        reader: BasemapTileReader,
        providers: dict[str, BasemapProvider],
        online_fallback: bool,
        s3_client: Optional[S3Client],
        redis_client: RedisClient,
        http_client: HttpTileClient,
        availability_ttl: int,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Attach runtime dependencies at startup."""
        self._reader = reader
        self._providers = providers
        self._online_fallback = online_fallback
        self._s3 = s3_client
        self._redis = redis_client
        self._http = http_client
        self._availability_ttl = availability_ttl

    async def list_providers(self) -> BasemapProvidersResponse:
        """Return base map providers visible to the frontend.

        Each enabled provider is checked in parallel: upstream is actively
        probed (and S3 fallback queried concurrently). A provider is listed
        when the upstream is healthy OR S3 has at least one cached tile.
        """
        candidates = list(self._providers.values())
        flags = await asyncio.gather(
            *(self._check_provider_available(p) for p in candidates)
        )

        providers: List[BasemapProviderInfo] = [
            BasemapProviderInfo(
                id=p.provider_id,
                name=p.name,
                min_zoom=p.min_zoom,
                max_zoom=p.max_zoom,
                cache_max_zoom=p.cache_max_zoom,
                attribution=p.attribution,
            )
            for p, available in zip(candidates, flags)
            if available
        ]
        return BasemapProvidersResponse(providers=providers)

    async def _check_provider_available(self, provider: BasemapProvider) -> bool:
        """Decide availability via Redis cache → single-flight probe + S3."""
        # Offline-read mode predates active probing: keep the legacy "S3 has
        # any tile?" gate since upstream is forbidden anyway.
        if not self._online_fallback:
            if self._s3 is None:
                return False
            try:
                return await self._s3.has_any_object(
                    f"basemap/{provider.provider_id}/"
                )
            except (OSError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "S3 fallback check failed for %s: %s", provider.provider_id, exc
                )
                return False

        if self._redis is not None:
            try:
                cached = await self._redis.get_basemap_provider_availability(
                    provider.provider_id
                )
            except (OSError, asyncio.TimeoutError) as exc:
                logger.debug(
                    "Availability cache read failed for %s: %s",
                    provider.provider_id,
                    exc,
                )
                cached = None
            if cached is not None:
                return cached

        existing = self._availability_inflight.get(provider.provider_id)
        if existing is not None:
            return await existing

        fut: "asyncio.Future[bool]" = asyncio.get_event_loop().create_future()
        self._availability_inflight[provider.provider_id] = fut
        try:
            available = await self._refresh_availability(provider)
            if not fut.done():
                fut.set_result(available)
            return available
        except BaseException as exc:  # pylint: disable=broad-exception-caught
            if not fut.done():
                fut.set_exception(exc)
            raise
        finally:
            self._availability_inflight.pop(provider.provider_id, None)

    async def _refresh_availability(self, provider: BasemapProvider) -> bool:
        """Probe upstream + S3 in parallel, write the result to Redis."""
        probe_task = self._probe_provider(provider)
        s3_task = self._has_any_s3_tile(provider)
        probe_ok, s3_ok = await asyncio.gather(
            probe_task, s3_task, return_exceptions=False
        )
        available = bool(probe_ok or s3_ok)

        if self._redis is not None:
            try:
                await self._redis.set_basemap_provider_availability(
                    provider.provider_id, available, self._availability_ttl
                )
            except (OSError, asyncio.TimeoutError) as exc:
                logger.debug(
                    "Availability cache write failed for %s: %s",
                    provider.provider_id,
                    exc,
                )

        logger.debug(
            "basemap provider %s availability=%s (probe_ok=%s, s3_ok=%s, ttl=%ds)",
            provider.provider_id,
            available,
            probe_ok,
            s3_ok,
            self._availability_ttl,
        )
        return available

    async def _probe_provider(self, provider: BasemapProvider) -> bool:
        """Single tile probe at (min_zoom, 0, 0). Bytes back → reachable."""
        if self._http is None:
            return False
        url = build_source_url(provider, provider.min_zoom, 0, 0)
        try:
            data = await self._http.download_tile(url)
        except ProviderUnavailableError as exc:
            logger.info(
                "Provider probe unavailable for %s: %s", provider.provider_id, exc.cause
            )
            return False
        return bool(data)

    async def _has_any_s3_tile(self, provider: BasemapProvider) -> bool:
        """True when S3 has at least one tile under the provider's prefix."""
        if self._s3 is None:
            return False
        try:
            return await self._s3.has_any_object(f"basemap/{provider.provider_id}/")
        except (OSError, asyncio.TimeoutError) as exc:
            logger.warning(
                "S3 fallback check failed for %s: %s", provider.provider_id, exc
            )
            return False

    async def get_tile_data(
        self, provider_id: str, z: int, x: int, y: int
    ) -> Optional[bytes]:
        """Fetch tile bytes via the configured reader.

        Raises BasemapNotConfiguredError if the service has no reader attached
        (e.g. basemap disabled at startup because S3 was unconfigured).
        """
        if not self._reader:
            raise BasemapNotConfiguredError(
                "Basemap service is not configured (no reader attached)."
            )
        return await self._reader.get_tile(provider_id, z, x, y)

    def validate_provider(self, provider_id: str) -> bool:
        """Check if a provider ID is valid and enabled."""
        return provider_id in self._providers

    def validate_zoom(self, provider_id: str, z: int) -> bool:
        """Check if a zoom level is valid for the given provider."""
        provider = self._providers.get(provider_id)
        if not provider:
            return False
        return provider.min_zoom <= z <= provider.max_zoom
