"""Main entrypoint for the data-service application."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

import uvloop
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from clients.basemap_state_store import BasemapStateStore
from clients.http_tile_client import HttpTileClient
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from clients.smn_api_client import SmnApiClient
from clients.smn_registry_client import SmnRegistryClient
from clients.weather_stations_keystore import WeatherStationsKeystore
from controller import general
from db.migrate import ensure_migrations
from dependencies import (
    basemap_service,
    logger,
    metrics_store,
    redis_client,
    set_basemap_state_store,
    set_weather_stations_keystore,
    settings,
)
from gdal_config import configure_gdal_vsi_s3
from routes import (
    basemap,
    ecmwf_mslp,
    ecmwf_tp,
    metrics,
    radar,
    satellite,
    sync,
    weather_stations,
    gfs,
    wrf,
)
from services.basemap_config import BoundingBox, load_providers
from services.basemap_scraper_service import BasemapScraperService
from services.basemap_tile_reader import BasemapTileReader
from services.ecmwf_mslp_service import ecmwf_mslp_service
from services.ecmwf_mslp_sync_strategy import (
    EcmwfMslpFullSyncStrategy,
    EcmwfMslpOnDemandStrategy,
    EcmwfMslpSyncStrategy,
)
from services.ecmwf_tp_service import ecmwf_tp_service
from services.ecmwf_tp_sync_strategy import (
    EcmwfTpFullSyncStrategy,
    EcmwfTpOnDemandStrategy,
    EcmwfTpSyncStrategy,
)
from services.point_value_service import point_value_service
from services.point_value_strategy import S3CogPointValueStrategy
from services.radar_service import radar_service
from services.radar_sync_strategy import (
    RadarFullSyncStrategy,
    RadarOnDemandStrategy,
    RadarSyncStrategy,
)
from services.redis_metrics_service import RedisMetricsService
from services.satellite_service import satellite_service
from services.satellite_sync_strategy import (
    SatelliteFullSyncStrategy,
    SatelliteOnDemandStrategy,
    SatelliteSyncStrategy,
)
from services.ecmwf_mslp_sync_service import ecmwf_mslp_sync_service
from services.ecmwf_tp_sync_service import ecmwf_tp_sync_service
from services.radar_sync_service import radar_sync_service
from services.satellite_sync_service import satellite_sync_service
from services.wrf_sync_service import wrf_sync_service
from services.gfs_sync_service import gfs_sync_service
from services.weather_stations_scraper_service import WeatherStationsScraperService
from services.weather_stations_service import weather_stations_service
from services.wrf_service import wrf_service
from services.gfs_service import gfs_service
from services.gfs_sync_strategy import (
    GfsFullSyncStrategy,
    GfsOnDemandStrategy,
    GfsSyncStrategy,
)
from services.wrf_sync_strategy import (
    WrfFullSyncStrategy,
    WrfOnDemandStrategy,
    WrfSyncStrategy,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Per-product background sync loops, started/stopped together in `full` mode.
_SYNC_SERVICES = (
    satellite_sync_service,
    radar_sync_service,
    ecmwf_tp_sync_service,
    ecmwf_mslp_sync_service,
    wrf_sync_service,
    gfs_sync_service,
)


def _runs_background_jobs() -> bool:
    """Whether this process starts the background sync/scrape/metrics loops.

    The "web" role serves HTTP only; "worker" runs the background jobs; "all"
    (default) does both. Read-side configuration always runs regardless of role.
    """
    return settings.app_role in ("worker", "all")


@dataclass(slots=True)
class WeatherStationsRuntime:
    """Lifecycle holder for weather-stations resources owned by the app lifespan."""

    # `keystore` and `api_keys_s3_client` are only None in the degenerate
    # local-dev case where auth is disabled AND S3 is not configured.
    keystore: Optional[WeatherStationsKeystore] = None
    api_keys_s3_client: Optional[S3Client] = None
    # All four are absent when sync_mode == "disabled" (keystore stays for
    # read-side auth gating, but nothing scrapes).
    s3_client: Optional[S3Client] = None
    smn_client: Optional[SmnApiClient] = None
    registry_client: Optional[SmnRegistryClient] = None
    scraper: Optional[WeatherStationsScraperService] = None


@dataclass(slots=True)
class BasemapRuntime:
    """Lifecycle holder for basemap-scoped resources owned by the app lifespan."""

    reader_http_client: HttpTileClient
    reader: BasemapTileReader
    # S3 is absent in relay_only mode.
    s3_client: Optional[S3Client] = None
    # Scraper-only resources. Absent in relay_only mode.
    scraper_http_client: Optional[HttpTileClient] = None
    state_store: Optional[BasemapStateStore] = None
    scraper: Optional[BasemapScraperService] = None


async def configure_strategies(
    client_redis: RedisClient,
) -> tuple[
    SatelliteSyncStrategy,
    RadarSyncStrategy,
    EcmwfTpSyncStrategy,
    EcmwfMslpSyncStrategy,
    S3CogPointValueStrategy,
    Optional[S3Client],
    WrfSyncStrategy,
    GfsSyncStrategy,
]:
    """Configure and return sync strategies based on settings."""
    s3_client = None
    sat_strategy: SatelliteSyncStrategy
    radar_strategy: RadarSyncStrategy
    ecmwf_tp_strategy: EcmwfTpSyncStrategy
    ecmwf_mslp_strategy: EcmwfMslpSyncStrategy
    wrf_strategy: WrfSyncStrategy
    gfs_strategy: GfsSyncStrategy

    if settings.is_s3_configured():
        s3_client = S3Client(
            endpoint=settings.s3_tiles_data_endpoint,
            access_key=settings.s3_tiles_data_access_key,
            secret_key=settings.s3_tiles_data_secret_key,
            bucket=settings.s3_tiles_data_bucket_name,
            secure=settings.s3_tiles_data_secure,
            max_concurrent_downloads=settings.s3_max_concurrent_downloads,
            connect_timeout=settings.s3_connect_timeout_seconds,
            read_timeout=settings.s3_read_timeout_seconds,
            max_attempts=settings.s3_max_attempts,
        )
        await s3_client.connect()

    point_value_strategy = S3CogPointValueStrategy(s3_client)

    if settings.sync_mode == "full":
        # Background sync mode (default). Reads are still Redis-first, but each
        # strategy is given the S3 client + TTLs so a tile miss / evicted index
        # falls back to S3 (and re-warms Redis) instead of returning empty.
        sat_strategy = SatelliteFullSyncStrategy(
            client_redis, s3_client, settings.tile_ttl, settings.tileset_listing_ttl
        )
        radar_strategy = RadarFullSyncStrategy(
            client_redis,
            s3_client,
            settings.radar_tile_ttl,
            settings.tileset_listing_ttl,
        )
        ecmwf_tp_strategy = EcmwfTpFullSyncStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_tile_ttl,
            settings.tileset_listing_ttl,
        )
        ecmwf_mslp_strategy = EcmwfMslpFullSyncStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_mslp_geojson_ttl,
            settings.tileset_listing_ttl,
        )
        wrf_strategy = WrfFullSyncStrategy(
            client_redis,
            s3_client,
            settings.wrf_tile_ttl,
            settings.wrf_geojson_ttl,
            settings.tileset_listing_ttl,
        )
        gfs_strategy = GfsFullSyncStrategy(
            client_redis,
            s3_client,
            settings.gfs_tile_ttl,
            settings.gfs_geojson_ttl,
            settings.tileset_listing_ttl,
            settings.gfs_cycles_to_keep,
        )

        # Each product syncs on its own independent loop (own flock + S3 client
        # + watchdog) so no product can monopolize another's scheduling or S3
        # budget. WRF (the heaviest) runs on its own longer cadence. Only the
        # worker role starts them — the web role builds the (Full) read
        # strategies above but never runs the loops, so sync CPU can't starve
        # request serving.
        if _runs_background_jobs():
            for service in _SYNC_SERVICES:
                service.set_redis_client(client_redis)
                if settings.metrics_enabled:
                    service.set_metrics_store(metrics_store)
                await service.start(logger)
    else:
        # On-demand mode: lazy fetch + cache
        logger.info("Starting in on-demand sync mode")

        sat_strategy = SatelliteOnDemandStrategy(
            client_redis,
            s3_client,
            settings.tile_ttl,
            settings.tileset_listing_ttl,
        )
        radar_strategy = RadarOnDemandStrategy(
            client_redis,
            s3_client,
            settings.radar_tile_ttl,
            settings.tileset_listing_ttl,
        )
        ecmwf_tp_strategy = EcmwfTpOnDemandStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_tile_ttl,
            settings.tileset_listing_ttl,
        )
        ecmwf_mslp_strategy = EcmwfMslpOnDemandStrategy(
            client_redis,
            s3_client,
            settings.ecmwf_mslp_geojson_ttl,
            settings.tileset_listing_ttl,
        )
        wrf_strategy = WrfOnDemandStrategy(
            client_redis,
            s3_client,
            settings.wrf_tile_ttl,
            settings.wrf_geojson_ttl,
            settings.tileset_listing_ttl,
        )
        gfs_strategy = GfsOnDemandStrategy(
            client_redis,
            s3_client,
            settings.gfs_tile_ttl,
            settings.gfs_geojson_ttl,
            settings.tileset_listing_ttl,
            settings.gfs_cycles_to_keep,
        )

    return (
        sat_strategy,
        radar_strategy,
        ecmwf_tp_strategy,
        ecmwf_mslp_strategy,
        point_value_strategy,
        s3_client,
        wrf_strategy,
        gfs_strategy,
    )


async def configure_basemap(
    client_redis: RedisClient,
) -> Optional[BasemapRuntime]:
    """Bring up the basemap subsystem using the active `basemap_sync_mode`.

    Modes (set via `settings.json::basemap_sync_mode` or `BASEMAP_SYNC_MODE`):
      * ``full``       — scraper on (writes Redis + S3); reader tries
                         upstream first, then falls back to Redis, then S3.
      * ``on_demand``  — scraper on but writes only S3; reader tries
                         upstream first, then Redis (lazily populated by
                         the reader on hits), then S3.
      * ``no_cache``   — scraper on, S3-only. Reader skips Redis tier
                         entirely (upstream → S3).
      * ``relay_only`` — scraper off, Redis off, S3 off. Reader is a pure
                         provider proxy.

    `relay_only` is the only mode that skips S3 reads, so an S3 backend
    outage doesn't take the service down under `relay_only`. For every
    other mode, S3 must be configured — a cold backup without S3 storage
    defeats the point of having a backup.

    When enabled, populates the module-level `basemap_service` singleton via
    `configure()` and returns its backing runtime for lifespan-scoped shutdown.
    When disabled, the singleton keeps its empty default state.
    """
    providers = load_providers(settings.basemap_providers)

    if not providers:
        logger.info("Basemap disabled: no providers enabled in settings.json")
        return None

    mode = settings.basemap_sync_mode
    # The reader (serving) is always built below; the scraper only runs in a
    # background-job role so the web role doesn't scrape.
    run_scraper = mode in ("full", "on_demand", "no_cache") and _runs_background_jobs()
    scraper_writes_redis = mode == "full"
    redis_cache_enabled = mode in ("full", "on_demand")
    s3_cache_enabled = mode in ("full", "on_demand", "no_cache")

    if s3_cache_enabled and not settings.is_s3_configured():
        logger.error(
            "Basemap refused to start: S3 is not configured but basemap "
            "mode=%s requires S3 storage. Configure S3 credentials, switch "
            "to basemap_sync_mode=relay_only, or disable basemap_providers "
            "in settings.json.",
            mode,
        )
        return None

    logger.info(
        "Basemap mode=%s (scraper=%s, scraper_redis=%s, "
        "reader_redis=%s, reader_s3=%s)",
        mode,
        "on" if run_scraper else "off",
        "on" if scraper_writes_redis else "off",
        "on" if redis_cache_enabled else "off",
        "on" if s3_cache_enabled else "off",
    )

    basemap_s3: Optional[S3Client] = None
    if s3_cache_enabled:
        basemap_s3 = S3Client(
            endpoint=settings.s3_tiles_data_endpoint,
            access_key=settings.s3_tiles_data_access_key,
            secret_key=settings.s3_tiles_data_secret_key,
            bucket=settings.s3_basemap_bucket_name,
            secure=settings.s3_tiles_data_secure,
            max_concurrent_downloads=settings.s3_max_concurrent_downloads,
            connect_timeout=settings.s3_connect_timeout_seconds,
            read_timeout=settings.s3_read_timeout_seconds,
            max_attempts=settings.s3_max_attempts,
        )
        await basemap_s3.connect()
        # Lifecycle policy application is delegated to the scraper loop so
        # a transient S3 outage at boot can self-heal on the next sweep
        # instead of leaving the bucket with the wrong (or no) rule.

    # Separate pool for user-facing reads so tile requests can't queue behind
    # the scraper's retry loop. Tight budget: short timeout, minimal retries.
    reader_http_client = HttpTileClient(
        max_concurrent=settings.basemap_reader_http_concurrent,
        delay_ms=0,
        timeout_seconds=settings.basemap_reader_http_timeout_seconds,
        max_retries=settings.basemap_reader_http_max_retries,
    )
    await reader_http_client.connect()

    reader = BasemapTileReader(
        redis_client=client_redis,
        s3_client=basemap_s3,
        http_client=reader_http_client,
        providers=providers,
        tile_ttl=settings.basemap_tile_ttl,
        cache_concurrent=settings.basemap_cache_concurrent,
        online_fallback=settings.basemap_online_fallback_enabled,
        request_deadline_seconds=settings.basemap_request_deadline_seconds,
        redis_cache_enabled=redis_cache_enabled,
        s3_cache_enabled=s3_cache_enabled,
    )

    scraper_http_client: Optional[HttpTileClient] = None
    state_store: Optional[BasemapStateStore] = None
    scraper: Optional[BasemapScraperService] = None

    # Open the scrape-state SQLite whenever the mode involves scraping,
    # regardless of this process's role. The worker writes it; the web role
    # only reads it to serve /metrics/basemap/providers. WAL mode lets the web
    # reader and the worker writer share the file across processes/containers.
    if s3_cache_enabled:  # mode in (full, on_demand, no_cache) — not relay_only
        state_store = BasemapStateStore(settings.basemap_scrape_state_db_path)
        await state_store.connect()
        set_basemap_state_store(state_store)

    if run_scraper:
        scraper_http_client = HttpTileClient(
            max_concurrent=settings.basemap_scrape_concurrent,
            delay_ms=settings.basemap_scrape_delay_ms,
            timeout_seconds=settings.basemap_http_timeout_seconds,
            max_retries=settings.basemap_http_max_retries,
            per_host_concurrent=settings.basemap_scrape_per_host_concurrent,
        )
        await scraper_http_client.connect()

        bbox = BoundingBox(
            lat_min=settings.basemap_bbox_lat_min,
            lat_max=settings.basemap_bbox_lat_max,
            lon_min=settings.basemap_bbox_lon_min,
            lon_max=settings.basemap_bbox_lon_max,
        )

        # run_scraper implies s3_cache_enabled by construction, so basemap_s3
        # and state_store are guaranteed non-None here. Assert for mypy.
        assert basemap_s3 is not None and state_store is not None
        scraper = BasemapScraperService(
            settings=settings,
            s3_client=basemap_s3,
            redis_client=client_redis,
            http_client=scraper_http_client,
            state_store=state_store,
            providers=providers,
            bbox=bbox,
            tile_ttl=settings.basemap_tile_ttl,
            s3_object_ttl_days=settings.basemap_s3_object_ttl_days,
            redis_writes_enabled=scraper_writes_redis,
            parallelism_mode=settings.basemap_scrape_parallelism_mode,
            metrics_store=metrics_store if settings.metrics_enabled else None,
        )
        await scraper.start(logger)

    basemap_service.configure(
        reader=reader,
        providers=providers,
        online_fallback=settings.basemap_online_fallback_enabled,
        s3_client=basemap_s3,
        redis_client=client_redis,
        http_client=reader_http_client,
        availability_ttl=settings.basemap_provider_availability_ttl,
    )
    return BasemapRuntime(
        s3_client=basemap_s3,
        reader_http_client=reader_http_client,
        reader=reader,
        scraper_http_client=scraper_http_client,
        state_store=state_store,
        scraper=scraper,
    )


async def configure_weather_stations() -> WeatherStationsRuntime:
    """Bring up the weather-stations subsystem.

    The keystore is built on a dedicated S3 bucket (separate from the
    weather-stations data bucket) and gates the read endpoints' API-key auth
    even when no scraper runs. When `weather_stations_sync_mode == "full"`
    the scraper + its S3/SMN clients are also built and started. Subsystem
    is S3-only by design — no Redis.
    """
    api_keys_s3: Optional[S3Client] = None
    keystore: Optional[WeatherStationsKeystore] = None
    if settings.is_s3_configured():
        api_keys_s3 = S3Client(
            endpoint=settings.s3_tiles_data_endpoint,
            access_key=settings.s3_tiles_data_access_key,
            secret_key=settings.s3_tiles_data_secret_key,
            bucket=settings.s3_api_keys_bucket_name,
            secure=settings.s3_tiles_data_secure,
            max_concurrent_downloads=settings.s3_max_concurrent_downloads,
            connect_timeout=settings.s3_connect_timeout_seconds,
            read_timeout=settings.s3_read_timeout_seconds,
            max_attempts=settings.s3_max_attempts,
        )
        await api_keys_s3.connect()
        # Bucket is dedicated to this subsystem and the only writer here is the
        # keystore itself, so creating it on cold start keeps deploys to fresh
        # S3/MinIO instances zero-touch (no `aws s3 mb` step).
        await api_keys_s3.ensure_bucket()
        keystore = WeatherStationsKeystore(api_keys_s3)
        set_weather_stations_keystore(keystore)
    else:
        # Validator already forbids auth_enabled=true without S3, so reaching
        # here implies auth_enabled=false — admin endpoints will 500 via the
        # dep, but the read path is open and works without a keystore.
        logger.warning(
            "Weather-stations keystore not built: S3 is not configured. "
            "Admin endpoints (/weather-stations/admin/*) will return 500. "
            "Configure S3 to enable them."
        )

    if settings.weather_stations_sync_mode == "disabled":
        logger.info("Weather stations scraper disabled (sync_mode=disabled)")
        weather_stations_service.configure(
            s3_client=None,
            list_cache_ttl=settings.weather_stations_list_cache_ttl_seconds,
        )
        return WeatherStationsRuntime(keystore=keystore, api_keys_s3_client=api_keys_s3)

    if not settings.is_s3_configured():
        logger.error(
            "Weather stations refused to start: S3 is not configured but "
            "weather_stations_sync_mode=full requires S3. Configure S3 "
            "credentials or set WEATHER_STATIONS_SYNC_MODE=disabled."
        )
        weather_stations_service.configure(
            s3_client=None,
            list_cache_ttl=settings.weather_stations_list_cache_ttl_seconds,
        )
        return WeatherStationsRuntime(keystore=keystore, api_keys_s3_client=api_keys_s3)

    weather_s3 = S3Client(
        endpoint=settings.s3_tiles_data_endpoint,
        access_key=settings.s3_tiles_data_access_key,
        secret_key=settings.s3_tiles_data_secret_key,
        bucket=settings.s3_weather_stations_bucket_name,
        secure=settings.s3_tiles_data_secure,
        max_concurrent_downloads=settings.s3_max_concurrent_downloads,
        connect_timeout=settings.s3_connect_timeout_seconds,
        read_timeout=settings.s3_read_timeout_seconds,
        max_attempts=settings.s3_max_attempts,
    )
    await weather_s3.connect()

    # SMN/registry clients + the scraper only run in a background-job role. The
    # web role still builds weather_s3 + configures the read service below so it
    # serves snapshots (Redis cache the worker fills, with S3 fallback).
    smn_client: Optional[SmnApiClient] = None
    registry_client: Optional[SmnRegistryClient] = None
    scraper: Optional[WeatherStationsScraperService] = None
    if _runs_background_jobs():
        smn_client = SmnApiClient(
            base_url=settings.smn_api_base_url,
            username=settings.smn_api_username,
            password=settings.smn_api_password,
            timeout_seconds=settings.weather_stations_http_timeout_seconds,
            max_retries=settings.weather_stations_http_max_retries,
            token_cache_ttl_seconds=settings.weather_stations_token_cache_ttl_seconds,
            token_settling_delay_seconds=settings.smn_api_token_settling_delay_seconds,
            log_requests=settings.smn_api_log_requests,
        )
        registry_client = SmnRegistryClient(
            url=settings.smn_stations_registry_url,
            timeout_seconds=settings.weather_stations_http_timeout_seconds,
            max_retries=settings.weather_stations_http_max_retries,
        )
        scraper = WeatherStationsScraperService(
            settings=settings,
            s3_client=weather_s3,
            smn_client=smn_client,
            registry_client=registry_client,
            redis_client=redis_client,
            metrics_store=metrics_store if settings.metrics_enabled else None,
        )
        await scraper.start(logger)

    weather_stations_service.configure(
        s3_client=weather_s3,
        list_cache_ttl=settings.weather_stations_list_cache_ttl_seconds,
        redis_client=redis_client,
        cache_enabled=settings.weather_stations_redis_cache_enabled,
        latest_ttl=settings.weather_stations_redis_latest_ttl_seconds,
        tilesets_ttl=settings.weather_stations_redis_tilesets_ttl_seconds,
        snapshot_ttl=settings.weather_stations_redis_snapshot_ttl_seconds,
        registry_ttl=settings.weather_stations_redis_registry_ttl_seconds,
        series_ttl=settings.weather_stations_redis_series_ttl_seconds,
    )

    return WeatherStationsRuntime(
        keystore=keystore,
        api_keys_s3_client=api_keys_s3,
        s3_client=weather_s3,
        smn_client=smn_client,
        registry_client=registry_client,
        scraper=scraper,
    )


async def shutdown_weather_stations(runtime: WeatherStationsRuntime) -> None:
    """Tear down weather-stations resources in reverse startup order."""
    if runtime.scraper is not None:
        await runtime.scraper.stop(logger)
    if runtime.smn_client is not None:
        await runtime.smn_client.close()
    if runtime.registry_client is not None:
        await runtime.registry_client.close()
    if runtime.s3_client is not None:
        await runtime.s3_client.close()
    if runtime.keystore is not None:
        await runtime.keystore.close()
    if runtime.api_keys_s3_client is not None:
        await runtime.api_keys_s3_client.close()


async def shutdown_basemap(runtime: Optional[BasemapRuntime]) -> None:
    """Tear down basemap resources in reverse startup order."""
    if not runtime:
        return
    if runtime.scraper is not None:
        await runtime.scraper.stop(logger)
    if runtime.state_store is not None:
        await runtime.state_store.close()
    await runtime.reader.close()
    await runtime.reader_http_client.close()
    if runtime.scraper_http_client is not None:
        await runtime.scraper_http_client.close()
    if runtime.s3_client is not None:
        await runtime.s3_client.close()


async def shutdown_services():
    """Stop the per-product background sync loops if sync mode is full."""
    if settings.sync_mode == "full":
        for service in _SYNC_SERVICES:
            await service.stop(logger)


async def _wait_for_s3_reachable() -> None:
    """Block startup until S3 answers, retrying without limit with capped backoff.

    Keeps the process alive instead of crashing on a cold/slow S3. In dev the
    `--reload` reloader does not respawn a child that died, so a hard crash here
    would wedge the container until a file change; waiting lets it self-heal when
    S3 comes up (prod already self-heals via uvicorn's worker respawn).
    """
    probe = S3Client(
        endpoint=settings.s3_tiles_data_endpoint,
        access_key=settings.s3_tiles_data_access_key,
        secret_key=settings.s3_tiles_data_secret_key,
        bucket=settings.s3_tiles_data_bucket_name,
        secure=settings.s3_tiles_data_secure,
        max_concurrent_downloads=settings.s3_max_concurrent_downloads,
        connect_timeout=settings.s3_connect_timeout_seconds,
        read_timeout=settings.s3_read_timeout_seconds,
        max_attempts=settings.s3_max_attempts,
    )
    try:
        await probe.connect()
        backoff = 1.0
        while not await probe.is_reachable():
            logger.warning(
                "S3 endpoint %s not reachable; retrying in %.0fs "
                "(startup blocked until S3 is available)",
                settings.s3_tiles_data_endpoint,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
    finally:
        await probe.close()
    logger.info("S3 endpoint reachable; continuing startup")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage application lifecycle events."""
    logger.info(
        "Starting data-service (role=%s, background jobs=%s)...",
        settings.app_role,
        _runs_background_jobs(),
    )
    configure_gdal_vsi_s3()
    await redis_client.connect()
    # Bring the metrics DB schema to head before opening it (offloaded so the
    # flock + Alembic upgrade never block the event loop). Runs in every worker;
    # the flock serializes them and the upgrade no-ops once stamped. Independent
    # of metrics_enabled — the DB must exist for the /metrics routes regardless.
    await asyncio.to_thread(ensure_migrations, settings)
    await metrics_store.connect()

    # S3 is a hard dependency. Block here until it answers (unlimited retry with
    # capped backoff) instead of crashing — so dev (`--reload`, whose reloader
    # won't respawn a dead child) recovers automatically when S3 returns, the
    # same way prod's worker respawn already does.
    if settings.is_s3_configured():
        await _wait_for_s3_reachable()

    (
        sat_strategy,
        radar_strategy,
        ecmwf_tp_strategy,
        ecmwf_mslp_strategy,
        point_value_strategy,
        s3_client,
        wrf_strategy,
        gfs_strategy,
    ) = await configure_strategies(redis_client)

    satellite_service.set_strategy(sat_strategy)
    radar_service.set_strategy(radar_strategy)
    ecmwf_tp_service.set_strategy(ecmwf_tp_strategy)
    ecmwf_mslp_service.set_strategy(ecmwf_mslp_strategy)
    point_value_service.set_strategy(point_value_strategy)
    wrf_service.set_strategy(wrf_strategy)
    gfs_service.set_strategy(gfs_strategy)

    basemap_runtime = await configure_basemap(redis_client)
    weather_stations_runtime = await configure_weather_stations()

    redis_metrics_service: Optional[RedisMetricsService] = None
    if settings.metrics_enabled and _runs_background_jobs():
        redis_metrics_service = RedisMetricsService(
            settings=settings,
            redis_client=redis_client,
            metrics_store=metrics_store,
        )
        await redis_metrics_service.start(logger)

    yield

    # Shutdown
    logger.info("Shutting down data-service...")
    if redis_metrics_service is not None:
        await redis_metrics_service.stop(logger)
    await shutdown_weather_stations(weather_stations_runtime)
    await shutdown_basemap(basemap_runtime)
    await shutdown_services()

    if s3_client:
        await s3_client.close()
    await metrics_store.close()
    await redis_client.close()


app: FastAPI = FastAPI(
    title="data-service",
    description="Servicio que maneja la gestión de datos",
    contact={
        "name": "FIUBA TPF Team N°153 Altamirano, Diem, Gismondi, Valeriani",
    },
    lifespan=lifespan,
    openapi_tags=[
        {
            "name": "Weather Stations",
            "description": (
                "Public read endpoints serving SMN EMA snapshots scraped "
                "every ~5 minutes. All require the `X-API-Key` header (see "
                "the **Weather Stations · Admin** section for how to mint one)."
            ),
        },
        {
            "name": "Weather Stations · Admin",
            "description": (
                "Operator-only API-key management. Every endpoint requires "
                "the `X-Admin-Password` header matching the "
                "`WEATHER_STATIONS_ADMIN_PASSWORD` env var."
            ),
        },
    ],
)

# Add CORS middleware for tile serving
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(general.router)
app.include_router(basemap.router)  # Base map tile proxy
app.include_router(radar.router)  # Radar routes (most specific)
app.include_router(ecmwf_tp.router)  # ECMWF total precipitation routes
app.include_router(ecmwf_mslp.router)  # ECMWF mean sea level pressure routes
app.include_router(wrf.router)  # WRF model routes
app.include_router(gfs.router)  # GFS model routes
app.include_router(satellite.router)  # Satellite routes
app.include_router(sync.router)  # Sync observability
app.include_router(metrics.router)  # Status/performance dashboard metrics
app.include_router(weather_stations.router)  # SMN weather-stations endpoints
app.include_router(weather_stations.admin_router)  # Admin API-key management
