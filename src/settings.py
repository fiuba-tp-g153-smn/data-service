"""Configuration settings for the application."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

load_dotenv()


class Settings:
    # pylint: disable=too-many-instance-attributes
    """
    Application settings management.

    Loads tunable operational settings from settings.json, then lets
    environment variables override any value for deployment flexibility.
    Secrets and infrastructure URLs are loaded only from env vars.
    """

    # --- Env-only: secrets & infrastructure ---
    log_level: str = ""
    app_env: str = ""
    # Deployment role (env-only, like app_env): "web" serves HTTP only, "worker"
    # runs the background sync/scrape/metrics loops only, "all" does both (the
    # backward-compatible single-container default). Lets the same image run as
    # a serving container and a dedicated worker so background CPU can't starve
    # request handlers.
    app_role: str = "all"
    s3_tiles_data_endpoint: str = ""
    s3_tiles_data_access_key: str = ""
    s3_tiles_data_secret_key: str = ""
    s3_tiles_data_bucket_name: str = ""
    s3_tiles_data_secure: bool = False
    s3_weather_stations_bucket_name: str = "weather-stations-data"
    # Dedicated bucket for hashed API key objects (keys/<sha256>.json). Separate
    # from the weather-stations data bucket so admin keys can survive a wipe
    # of either bucket independently.
    s3_api_keys_bucket_name: str = "api-keys"
    redis_url: str = ""
    # SMN API credentials + base URL (env-only). The base is shared by the
    # token endpoint (`/api-token/auth`) and the stations endpoint
    # (`/weather/station`); SmnApiClient appends the path.
    smn_api_base_url: str = "https://api-test.smn.gob.ar/v1"
    smn_api_username: str = ""
    smn_api_password: str = ""
    # Workarounds for SMN auth-tier quirks. See SmnApiClient docstring.
    # Default 0.0 keeps the original behavior; set to e.g. 1.0 if SMN rejects
    # the freshly-minted JWT due to propagation lag in its validation tier.
    smn_api_token_settling_delay_seconds: float = 0.0
    # Diagnostic: when True, every outbound SMN request is logged in full
    # (URL, every header including JWT, body including auth credentials).
    # Use only for short debugging sessions; never leave on in production.
    smn_api_log_requests: bool = False
    # Public ZIP endpoint serving the canonical station registry (EMA list).
    # Different host + no auth. Refreshed lazily inside each scrape cycle:
    # the scraper compares the downloaded payload's hash against the stored
    # one and only rewrites the registry when it actually changed.
    smn_stations_registry_url: str = (
        "http://ssl.smn.gob.ar/dpd/zipopendata.php?dato=estaciones"
    )
    # Registry copy committed to the repo, used to SEED the bucket when the live
    # fetch fails and nothing is stored yet. SMN sits behind Cloudflare, which
    # challenges datacenter egress IPs with a 403 no retry can clear, so without
    # this a fresh deployment would never obtain a station registry at all.
    # Never overwrites a registry that was fetched live — see
    # WeatherStationsScraperService._refresh_registry_if_changed. Path is
    # relative to the application root (`/app` in the image), not the CWD.
    # Refresh it with `make refresh-stations-registry` from an unblocked network.
    weather_stations_registry_fallback_enabled: bool = True
    weather_stations_registry_fallback_path: str = "resources/estaciones_smn.txt"
    # Master password protecting /weather-stations/admin/* endpoints. Required
    # when weather_stations_api_key_auth_enabled is True (validator enforces).
    weather_stations_admin_password: str = ""
    gdal_disable_readdir_on_open: str = "EMPTY_DIR"
    gdal_curl_use_head: str = "NO"
    gdal_vsi_cache: bool = True
    gdal_vsi_cache_size: str = "50MB"
    gdal_vsicurl_cache_size: str = "200MB"

    # --- Operational tuning (loaded from settings.json, env overrides) ---
    sync_mode: str
    tile_ttl: int
    radar_tile_ttl: int
    tileset_listing_ttl: int
    sync_interval_seconds: int
    # Floor on the inter-cycle sleep so the loop always yields, even when a
    # cycle overruns sync_interval_seconds (otherwise it busy-loops at 0 sleep).
    sync_min_sleep_seconds: int
    # Per-product watchdog: a sync cycle that exceeds this is cancelled, recorded
    # as a "timeout", and retried next cycle. Must exceed one tileset/period's
    # download time so the frontier always advances. WRF uses its own value.
    sync_domain_timeout_seconds: int = 300
    cache_control_config: str
    cache_control_tile: str
    # File locks used by sync services so that only one uvicorn worker
    # runs the background sync task (fcntl exclusive lock).
    sync_lock_path: str = "/tmp/sync.lock"
    radar_lock_path: str = "/tmp/radar_sync.lock"
    # Each product syncs in its own loop under its own flock so they run
    # independently (no product blocks another). Satellite reuses sync_lock_path,
    # radar reuses radar_lock_path; the rest get their own below.
    ecmwf_tp_sync_lock_path: str = "/tmp/ecmwf_tp_sync.lock"
    ecmwf_mslp_sync_lock_path: str = "/tmp/ecmwf_mslp_sync.lock"
    wrf_sync_lock_path: str = "/tmp/wrf_sync.lock"
    gfs_sync_lock_path: str = "/tmp/gfs_sync.lock"
    s3_max_concurrent_downloads: int = 5
    # S3 client timeouts + bounded retry. Without these botocore can hang a
    # request indefinitely on a stalled endpoint, wedging the whole sync loop
    # (no per-request ceiling, no recovery). connect/read are per-attempt;
    # max_attempts bounds total retries (botocore "standard" mode).
    s3_connect_timeout_seconds: float = 5.0
    s3_read_timeout_seconds: float = 30.0
    s3_max_attempts: int = 3
    # ECMWF total precipitation (loaded from settings.json, env overrides)
    ecmwf_tile_ttl: int
    ecmwf_forecasts_to_keep: int
    # ECMWF mean sea level pressure (loaded from settings.json, env overrides)
    ecmwf_mslp_geojson_ttl: int
    # WRF model (loaded from settings.json, env overrides)
    wrf_tile_ttl: int = 86400
    wrf_geojson_ttl: int = 86400
    # WRF is the heaviest product, so it runs on its own loop with a longer
    # cadence and a generous watchdog (a big backfill pass must not be truncated).
    wrf_sync_interval_seconds: int = 120
    wrf_sync_timeout_seconds: int = 1200
    # Cap the WRF init runs walked per product each cycle (newest-first), so the
    # sync scan stays bounded as runs accumulate in S3. Mirrors ecmwf_forecasts_to_keep.
    wrf_inits_to_keep: int = 2
    # How long a step with no overlay layers in S3 skips the per-step overlay
    # LIST before re-checking. Short enough that GeoJSONs uploaded after the
    # tiles (separate pipeline steps) are picked up within the hour; without
    # it, every overlay-less step is re-listed on every cycle forever.
    wrf_overlay_recheck_ttl: int = 3600
    # GFS model (loaded from settings.json, env overrides).
    # Only listings and single-file overlays are mirrored to Redis: only tiles
    # and barb tiles are read on demand from S3. `gfs_tile_ttl` is therefore the
    # TTL of that lazy cache, not of a pre-warmed set.
    gfs_tile_ttl: int = 64800
    gfs_geojson_ttl: int = 64800
    # Cycles walked per product each pass (newest-first), bounding the scan as
    # runs accumulate in S3. NOTE: tiles-processor keeps 3 cycles in S3 and the
    # API deliberately exposes fewer, so the oldest cycle exists but is unlisted.
    gfs_cycles_to_keep: int = 2
    gfs_sync_interval_seconds: int = 120
    gfs_sync_timeout_seconds: int = 900
    # Cache-Control for a *missing* GFS tile / empty barb tile: short and NOT
    # `immutable`, unlike `cache_control_tile`. A step is advertised as soon as
    # its COG lands, often before the raster pyramid is written, so a gap is
    # normal AND temporary.
    gfs_cache_control_tile_miss: str = "public, max-age=300"
    # Basemap scraper (loaded from settings.json, env overrides)
    # Default TTL is 30 days — upstream basemap tiles change at most at the
    # month scale, so long Redis TTL + matching Cache-Control cuts relay
    # pressure when providers flap.
    basemap_tile_ttl: int = 2592000
    basemap_scrape_interval_seconds: int = 604800
    basemap_scrape_concurrent: int = 20
    basemap_scrape_delay_ms: int = 30
    # Max scrape tasks created at once per zoom (chunked fan-out). Bounds the
    # event-loop/memory pressure of a zoom with tens of thousands of tiles; the
    # HTTP semaphore still caps actual in-flight requests at scrape_concurrent.
    basemap_scrape_fanout_window: int = 500
    basemap_cache_max_zoom: int = 11
    basemap_cache_concurrent: int = 10
    basemap_scrape_lock_path: str = "/tmp/basemap_scrape.lock"
    s3_basemap_bucket_name: str = "basemap-tiles"
    basemap_providers: List[Dict[str, Any]] = []
    # Basemap tile bounding box (Argentina + surrounding region by default)
    basemap_bbox_lat_min: float = -60.0
    basemap_bbox_lat_max: float = -15.0
    basemap_bbox_lon_min: float = -85.0
    basemap_bbox_lon_max: float = -40.0
    # Basemap HTTP client tuning
    basemap_http_timeout_seconds: int = 10
    basemap_http_max_retries: int = 3
    # S3 bucket lifecycle: auto-expire tiles after N days. Scrape cadence (7d
    # by default) must remain strictly less than this TTL so weekly re-uploads
    # refresh object age before S3 expires them. With a 30-day Redis TTL we
    # give S3 35 days of runway — one extra scrape cycle of headroom.
    basemap_s3_object_ttl_days: int = 35
    # When False, disables tier-3 provider proxy — service serves only from
    # Redis/S3 (fully offline reads; scraper still pulls from providers).
    basemap_online_fallback_enabled: bool = True
    # TTL for the Redis-backed cache of per-provider availability answers
    # used by /basemap/providers. Each refresh actively probes upstream and
    # checks the S3 fallback, so the TTL is the freshness/cost trade-off.
    basemap_provider_availability_ttl: int = 240
    # Resumable-scrape state (SQLite cold storage + checkpoint knobs)
    basemap_scrape_state_db_path: str = "data/basemap_scraper_state.sqlite"
    basemap_scrape_checkpoint_every: int = 200
    basemap_scrape_checkpoint_seconds: float = 5.0
    # Reader-path HTTP client (isolated from the scraper's pool). Tight budget
    # so user requests can't queue behind the scraper's retry loop.
    basemap_reader_http_concurrent: int = 8
    basemap_reader_http_timeout_seconds: int = 3
    basemap_reader_http_max_retries: int = 1
    # Hard wall-clock deadline per tile request. Bounds single-flight waiters
    # and the relay fallback together.
    basemap_request_deadline_seconds: float = 4.0
    # Cache-Control header returned for missing tiles (datalayer-style: a
    # transparent PNG with a short TTL so the browser stops re-requesting).
    basemap_cache_control_tile_miss: str = "public, max-age=300, immutable"
    # Cache-Control header returned for successful basemap tile hits. Kept
    # separate from `cache_control_tile` because basemap tiles are static
    # (upstream providers refresh on the order of weeks) while satellite /
    # radar / ECMWF tiles rotate every few hours. 2592000s = 30 days matches
    # the Redis TTL (basemap_tile_ttl).
    basemap_cache_control_tile: str = "public, max-age=2592000, immutable"
    # Per-domain sync mode for basemap, independent of `sync_mode`. One of
    # "full" (scraper on + Redis cache on), "on_demand" (scraper off, Redis
    # populated lazily on cold reads), "no_cache" (scraper off, reader
    # streams straight from S3/relay — nothing lands in Redis).
    basemap_sync_mode: str = "full"
    # Scrape parallelism mode controls how providers are dispatched within one
    # scrape cycle. "sequential" runs providers one at a time (default, matches
    # pre-parallelism behavior). "per_origin" groups providers by URL host and
    # runs groups in parallel while keeping providers within a group serial.
    # "full" fans out all providers concurrently, regardless of host.
    basemap_scrape_parallelism_mode: str = "sequential"
    # Max concurrent in-flight requests to a single upstream host. Stacks under
    # the global `basemap_scrape_concurrent` budget so we never hammer one
    # host even in "full" mode when multiple providers share an origin.
    basemap_scrape_per_host_concurrent: int = 8
    # Provider-health circuit breaker. When a provider returns this many
    # consecutive UNAVAILABLE fetches inside a sweep, the scraper trips its
    # circuit, preserves the resume cursor, and skips it for a cooldown
    # chosen from `basemap_provider_cooldown_schedule` (indexed by the
    # consecutive-trip count, capped at the last element). One clean sweep
    # resets the counter and closes the circuit.
    basemap_provider_unhealthy_threshold: int = 5
    basemap_provider_cooldown_schedule: List[int] = [300, 900, 3600, 10800, 21600]
    # Rate-based circuit breaker (supersedes the consecutive-failure trigger
    # above). Within one sweep the scraper pushes through scattered failures and
    # trips a provider only when its error rate (failed / attempted fetches)
    # exceeds `error_rate_threshold`, but not before `error_rate_min_samples`
    # fetches — so a fully-down provider still bails early instead of hammering
    # the whole bbox. `basemap_provider_unhealthy_threshold` is kept for config
    # compatibility but no longer drives tripping.
    basemap_provider_error_rate_threshold: float = 0.05
    basemap_provider_error_rate_min_samples: int = 50

    # --- Weather stations subsystem (loaded from settings.json, env overrides) ---
    # Operational knobs only — secrets (SMN_*, WEATHER_STATIONS_ADMIN_PASSWORD)
    # and the bucket name live above as env-only. Reads are Redis-first (the
    # scrape loop write-throughs the hot keys so the cache stays warm) with an
    # S3 fallback + a tiny in-process LRU on LIST results for the cold path.
    weather_stations_sync_mode: str = "full"
    weather_stations_scrape_interval_seconds: int = 300
    weather_stations_scrape_lock_path: str = "/tmp/weather_stations_scrape.lock"
    weather_stations_http_timeout_seconds: int = 30
    weather_stations_http_max_retries: int = 3
    weather_stations_token_cache_ttl_seconds: int = 3600
    # S3 lifecycle TTL for snapshot objects. Must be >= 1 day. Covers the
    # frontend's longest tolerance option (48 h) with headroom.
    weather_stations_s3_object_ttl_days: int = 2
    # In-process LRU TTL for cached S3 LIST results in the read service.
    weather_stations_list_cache_ttl_seconds: int = 30
    # Per-endpoint Cache-Control, matched to how fresh each response needs to be.
    # latest/tilesets refresh every scrape (~5 min); the registry is near-static;
    # a resolved historical snapshot gets a moderate TTL (safe for the current-hour
    # bucket, which can still gain a newer best-match).
    weather_stations_cache_control_latest: str = (
        "public, max-age=60, stale-while-revalidate=120"
    )
    weather_stations_cache_control_tilesets: str = (
        "public, max-age=60, stale-while-revalidate=120"
    )
    weather_stations_cache_control_registry: str = (
        "public, max-age=3600, stale-while-revalidate=86400"
    )
    weather_stations_cache_control_snapshot: str = "public, max-age=300"
    # A station's bundled 48 h history series (immutable past + current hour).
    weather_stations_cache_control_series: str = "public, max-age=300"
    # Shared Redis cache in front of the read path. The scrape loop write-throughs
    # latest/tilesets/registry each cycle so the cache stays warm; TTLs are
    # safety-net backstops (the writer refreshes faster than they expire). The
    # `?N` historical snapshot is pure read-through (the scraper can't pre-warm
    # every N). Set `redis_cache_enabled=false` to fall back to S3-only.
    weather_stations_redis_cache_enabled: bool = True
    weather_stations_redis_latest_ttl_seconds: int = 600
    weather_stations_redis_tilesets_ttl_seconds: int = 600
    weather_stations_redis_snapshot_ttl_seconds: int = 3600
    weather_stations_redis_registry_ttl_seconds: int = 3600
    weather_stations_redis_series_ttl_seconds: int = 3600
    # How many of the latest hour-buckets the scraper pre-warms into Redis so the
    # frontend's timeline animation (latest 24 frames) never reads them from S3.
    weather_stations_redis_animation_warm_buckets: int = 24
    # History window (hours) for the per-station series endpoint; also the span
    # the scraper pivots + warms (`cache:ws:series:{id}`) every cycle. Bounded by
    # s3_object_ttl_days retention.
    weather_stations_series_hours: int = 48
    # When False, /weather-stations/* read endpoints are open (no X-API-Key).
    # Local-dev only; production must keep this True. Validator forbids
    # enabled=True without a non-empty admin password (otherwise admin endpoints
    # would be unreachable and no keys could ever be issued).
    weather_stations_api_key_auth_enabled: bool = True

    # --- Metrics / observability dashboard (loaded from settings.json, env overrides) ---
    # Backs the data-service status dashboard: per-domain sync-cycle history and
    # periodic Redis memory-by-domain snapshots, persisted in SQLite (cold,
    # survives restarts) alongside the basemap scraper state.
    metrics_enabled: bool = True
    metrics_db_path: str = "data/metrics.sqlite"
    metrics_retention_days: int = 14
    # Per-table row cap, a backstop behind time-based retention so the metrics
    # DB can't grow unbounded if retention is misconfigured. Applied to each of
    # the three tables independently every collector cycle. 0 disables the cap.
    metrics_max_rows: int = 1_000_000
    # File lock so only one uvicorn worker runs the Redis memory collector.
    metrics_lock_path: str = "/tmp/redis_metrics.lock"
    # Redis memory collector cadence. Each cycle SCANs the whole keyspace for
    # exact per-domain key counts, but runs MEMORY USAGE (pipelined) only on a
    # uniform sample of up to memory_sample_per_domain keys per domain and
    # extrapolates the domain's bytes. scan_count bounds the SCAN batch;
    # memory_batch_size bounds the MEMORY USAGE pipeline depth.
    # memory_sample_per_domain=0 disables sampling (exact per-key census).
    redis_metrics_sample_interval_seconds: int = 300
    redis_metrics_scan_count: int = 1000
    redis_metrics_memory_batch_size: int = 500
    redis_metrics_memory_sample_per_domain: int = 2000

    _BASEMAP_SYNC_MODES = ("full", "on_demand", "no_cache", "relay_only")
    _BASEMAP_PARALLELISM_MODES = ("sequential", "per_origin", "full")
    _WEATHER_STATIONS_SYNC_MODES = ("full", "disabled")
    _APP_ROLES = ("web", "worker", "all")
    _JSON_NAMESPACES = (
        "basemap",
        "ecmwf",
        "ecmwf_mslp",
        "gfs",
        "radar",
        "weather_stations",
        "wrf",
    )

    def __init__(self):
        settings_json_path = Path(__file__).resolve().parent.parent / "settings.json"

        self._load_from_json(settings_json_path)
        self._load_from_env()
        self._validate()

    def _load_from_json(self, settings_json_path: Path) -> None:
        """Load operational settings from settings.json."""
        if not settings_json_path.is_file():
            return

        with open(settings_json_path, encoding="utf-8") as f:
            data = json.load(f)

        # Flatten one level of per-domain nesting so the rest of the loader
        # (and every `settings.basemap_*` call site) keeps working unchanged.
        # Nested values win over flat ones when both are present.
        for namespace in self._JSON_NAMESPACES:
            section = data.pop(namespace, None)
            if isinstance(section, dict):
                for inner_key, inner_value in section.items():
                    data[f"{namespace}_{inner_key}"] = inner_value

        json_keys = {
            "sync_mode",
            "tile_ttl",
            "radar_tile_ttl",
            "tileset_listing_ttl",
            "sync_interval_seconds",
            "sync_min_sleep_seconds",
            "sync_domain_timeout_seconds",
            "cache_control_config",
            "cache_control_tile",
            "s3_max_concurrent_downloads",
            "s3_connect_timeout_seconds",
            "s3_read_timeout_seconds",
            "s3_max_attempts",
            "ecmwf_tile_ttl",
            "ecmwf_forecasts_to_keep",
            "ecmwf_mslp_geojson_ttl",
            "wrf_tile_ttl",
            "wrf_geojson_ttl",
            "wrf_inits_to_keep",
            "wrf_overlay_recheck_ttl",
            "wrf_sync_interval_seconds",
            "wrf_sync_timeout_seconds",
            "gfs_tile_ttl",
            "gfs_geojson_ttl",
            "gfs_cycles_to_keep",
            "gfs_sync_interval_seconds",
            "gfs_sync_timeout_seconds",
            "gfs_cache_control_tile_miss",
            "basemap_tile_ttl",
            "basemap_scrape_interval_seconds",
            "basemap_scrape_concurrent",
            "basemap_scrape_delay_ms",
            "basemap_scrape_fanout_window",
            "basemap_cache_max_zoom",
            "basemap_cache_concurrent",
            "basemap_providers",
            "basemap_bbox_lat_min",
            "basemap_bbox_lat_max",
            "basemap_bbox_lon_min",
            "basemap_bbox_lon_max",
            "basemap_http_timeout_seconds",
            "basemap_http_max_retries",
            "basemap_s3_object_ttl_days",
            "basemap_online_fallback_enabled",
            "basemap_provider_availability_ttl",
            "basemap_scrape_state_db_path",
            "basemap_scrape_checkpoint_every",
            "basemap_scrape_checkpoint_seconds",
            "basemap_reader_http_concurrent",
            "basemap_reader_http_timeout_seconds",
            "basemap_reader_http_max_retries",
            "basemap_request_deadline_seconds",
            "basemap_cache_control_tile_miss",
            "basemap_cache_control_tile",
            "basemap_sync_mode",
            "basemap_scrape_parallelism_mode",
            "basemap_scrape_per_host_concurrent",
            "basemap_provider_unhealthy_threshold",
            "basemap_provider_cooldown_schedule",
            "basemap_provider_error_rate_threshold",
            "basemap_provider_error_rate_min_samples",
            "weather_stations_sync_mode",
            "weather_stations_scrape_interval_seconds",
            "weather_stations_scrape_lock_path",
            "weather_stations_http_timeout_seconds",
            "weather_stations_http_max_retries",
            "weather_stations_token_cache_ttl_seconds",
            "weather_stations_s3_object_ttl_days",
            "weather_stations_list_cache_ttl_seconds",
            "weather_stations_cache_control_latest",
            "weather_stations_cache_control_tilesets",
            "weather_stations_cache_control_registry",
            "weather_stations_cache_control_snapshot",
            "weather_stations_cache_control_series",
            "weather_stations_redis_cache_enabled",
            "weather_stations_redis_latest_ttl_seconds",
            "weather_stations_redis_tilesets_ttl_seconds",
            "weather_stations_redis_snapshot_ttl_seconds",
            "weather_stations_redis_registry_ttl_seconds",
            "weather_stations_redis_series_ttl_seconds",
            "weather_stations_redis_animation_warm_buckets",
            "weather_stations_series_hours",
            "weather_stations_api_key_auth_enabled",
            "weather_stations_keystore_db_path",
            "metrics_enabled",
            "metrics_db_path",
            "metrics_retention_days",
            "metrics_max_rows",
            "metrics_lock_path",
            "redis_metrics_sample_interval_seconds",
            "redis_metrics_scan_count",
            "redis_metrics_memory_batch_size",
            "redis_metrics_memory_sample_per_domain",
        }

        for key in json_keys:
            if key in data:
                setattr(self, key, data[key])

    @staticmethod
    def _env_int(key: str, default: int) -> int:
        """Read an env var as int, falling back to default if unset or empty."""
        value = os.getenv(key, "")
        return int(value) if value else default

    @staticmethod
    def _env_float(key: str, default: float) -> float:
        """Read an env var as float, falling back to default if unset or empty."""
        value = os.getenv(key, "")
        return float(value) if value else default

    @staticmethod
    def _env_bool(key: str, default: bool) -> bool:
        """Read an env var as bool (truthy: 1/true/yes), falling back to default."""
        value = os.getenv(key, "")
        if not value:
            return default
        return value.strip().lower() in ("1", "true", "yes", "on")

    @staticmethod
    def _env_int_list(key: str, default: List[int]) -> List[int]:
        """Read an env var as a comma-separated list of ints, falling back to default."""
        value = os.getenv(key, "")
        if not value:
            return list(default)
        return [int(part.strip()) for part in value.split(",") if part.strip()]

    def _load_from_env(self) -> None:
        # pylint: disable=too-many-statements
        """Load from environment variables (overrides JSON values)."""
        self.log_level = os.getenv("LOG_LEVEL", self.log_level)
        self.app_env = os.getenv("APP_ENV", self.app_env)
        self.app_role = os.getenv("APP_ROLE", self.app_role) or self.app_role

        # S3
        self.s3_tiles_data_endpoint = os.getenv(
            "S3_TILES_DATA_ENDPOINT", self.s3_tiles_data_endpoint
        )
        self.s3_tiles_data_access_key = os.getenv(
            "S3_TILES_DATA_ACCESS_KEY", self.s3_tiles_data_access_key
        )
        self.s3_tiles_data_secret_key = os.getenv(
            "S3_TILES_DATA_SECRET_KEY", self.s3_tiles_data_secret_key
        )
        self.s3_tiles_data_bucket_name = os.getenv(
            "S3_TILES_DATA_BUCKET_NAME", self.s3_tiles_data_bucket_name
        )
        self.s3_tiles_data_secure = (
            os.getenv("S3_TILES_DATA_SECURE", "false").lower() == "true"
        )

        # Redis
        self.redis_url = os.getenv("REDIS_URL", self.redis_url)

        # GDAL / VSI S3 tuning
        self.gdal_disable_readdir_on_open = os.getenv(
            "GDAL_DISABLE_READDIR_ON_OPEN", self.gdal_disable_readdir_on_open
        )
        self.gdal_curl_use_head = os.getenv(
            "CPL_VSIL_CURL_USE_HEAD", self.gdal_curl_use_head
        )
        self.gdal_vsi_cache = os.getenv("VSI_CACHE", "TRUE").upper() == "TRUE"
        self.gdal_vsi_cache_size = os.getenv("VSI_CACHE_SIZE", self.gdal_vsi_cache_size)
        self.gdal_vsicurl_cache_size = os.getenv(
            "CPL_VSIL_CURL_CACHE_SIZE", self.gdal_vsicurl_cache_size
        )

        # Operational (env overrides JSON)
        self.sync_interval_seconds = self._env_int(
            "SYNC_INTERVAL_SECONDS", self.sync_interval_seconds
        )
        self.sync_min_sleep_seconds = self._env_int(
            "SYNC_MIN_SLEEP_SECONDS", self.sync_min_sleep_seconds
        )
        self.sync_domain_timeout_seconds = self._env_int(
            "SYNC_DOMAIN_TIMEOUT_SECONDS", self.sync_domain_timeout_seconds
        )
        self.sync_mode = os.getenv("SYNC_MODE", self.sync_mode) or self.sync_mode
        self.tile_ttl = self._env_int("TILE_TTL", self.tile_ttl)
        self.radar_tile_ttl = self._env_int("RADAR_TILE_TTL", self.radar_tile_ttl)
        self.tileset_listing_ttl = self._env_int(
            "TILESET_LISTING_TTL", self.tileset_listing_ttl
        )
        self.cache_control_config = os.getenv(
            "CACHE_CONTROL_CONFIG", self.cache_control_config
        )
        self.cache_control_tile = os.getenv(
            "CACHE_CONTROL_TILE", self.cache_control_tile
        )
        self.s3_max_concurrent_downloads = self._env_int(
            "S3_MAX_CONCURRENT_DOWNLOADS", self.s3_max_concurrent_downloads
        )
        self.s3_connect_timeout_seconds = self._env_float(
            "S3_CONNECT_TIMEOUT_SECONDS", self.s3_connect_timeout_seconds
        )
        self.s3_read_timeout_seconds = self._env_float(
            "S3_READ_TIMEOUT_SECONDS", self.s3_read_timeout_seconds
        )
        self.s3_max_attempts = self._env_int("S3_MAX_ATTEMPTS", self.s3_max_attempts)
        self.ecmwf_tile_ttl = self._env_int("ECMWF_TILE_TTL", self.ecmwf_tile_ttl)
        self.ecmwf_forecasts_to_keep = self._env_int(
            "ECMWF_FORECASTS_TO_KEEP", self.ecmwf_forecasts_to_keep
        )
        self.ecmwf_mslp_geojson_ttl = self._env_int(
            "ECMWF_MSLP_GEOJSON_TTL", self.ecmwf_mslp_geojson_ttl
        )
        self.wrf_tile_ttl = self._env_int("WRF_TILE_TTL", self.wrf_tile_ttl)
        self.wrf_geojson_ttl = self._env_int("WRF_GEOJSON_TTL", self.wrf_geojson_ttl)
        self.wrf_inits_to_keep = self._env_int(
            "WRF_INITS_TO_KEEP", self.wrf_inits_to_keep
        )
        self.wrf_overlay_recheck_ttl = self._env_int(
            "WRF_OVERLAY_RECHECK_TTL", self.wrf_overlay_recheck_ttl
        )
        self.wrf_sync_interval_seconds = self._env_int(
            "WRF_SYNC_INTERVAL_SECONDS", self.wrf_sync_interval_seconds
        )
        self.gfs_tile_ttl = self._env_int("GFS_TILE_TTL", self.gfs_tile_ttl)
        self.gfs_geojson_ttl = self._env_int("GFS_GEOJSON_TTL", self.gfs_geojson_ttl)
        self.gfs_cycles_to_keep = self._env_int(
            "GFS_CYCLES_TO_KEEP", self.gfs_cycles_to_keep
        )
        self.gfs_sync_interval_seconds = self._env_int(
            "GFS_SYNC_INTERVAL_SECONDS", self.gfs_sync_interval_seconds
        )
        self.gfs_cache_control_tile_miss = os.getenv(
            "GFS_CACHE_CONTROL_TILE_MISS", self.gfs_cache_control_tile_miss
        )
        self.gfs_sync_timeout_seconds = self._env_int(
            "GFS_SYNC_TIMEOUT_SECONDS", self.gfs_sync_timeout_seconds
        )
        self.wrf_sync_timeout_seconds = self._env_int(
            "WRF_SYNC_TIMEOUT_SECONDS", self.wrf_sync_timeout_seconds
        )

        # Basemap
        self.s3_basemap_bucket_name = os.getenv(
            "S3_BASEMAP_BUCKET_NAME", self.s3_basemap_bucket_name
        )
        self.basemap_tile_ttl = self._env_int("BASEMAP_TILE_TTL", self.basemap_tile_ttl)
        self.basemap_scrape_interval_seconds = self._env_int(
            "BASEMAP_SCRAPE_INTERVAL_SECONDS", self.basemap_scrape_interval_seconds
        )
        self.basemap_scrape_concurrent = self._env_int(
            "BASEMAP_SCRAPE_CONCURRENT", self.basemap_scrape_concurrent
        )
        self.basemap_scrape_fanout_window = self._env_int(
            "BASEMAP_SCRAPE_FANOUT_WINDOW", self.basemap_scrape_fanout_window
        )
        self.basemap_scrape_delay_ms = self._env_int(
            "BASEMAP_SCRAPE_DELAY_MS", self.basemap_scrape_delay_ms
        )
        self.basemap_cache_max_zoom = self._env_int(
            "BASEMAP_CACHE_MAX_ZOOM", self.basemap_cache_max_zoom
        )
        self.basemap_cache_concurrent = self._env_int(
            "BASEMAP_CACHE_CONCURRENT", self.basemap_cache_concurrent
        )
        self.basemap_bbox_lat_min = self._env_float(
            "BASEMAP_BBOX_LAT_MIN", self.basemap_bbox_lat_min
        )
        self.basemap_bbox_lat_max = self._env_float(
            "BASEMAP_BBOX_LAT_MAX", self.basemap_bbox_lat_max
        )
        self.basemap_bbox_lon_min = self._env_float(
            "BASEMAP_BBOX_LON_MIN", self.basemap_bbox_lon_min
        )
        self.basemap_bbox_lon_max = self._env_float(
            "BASEMAP_BBOX_LON_MAX", self.basemap_bbox_lon_max
        )
        self.basemap_http_timeout_seconds = self._env_int(
            "BASEMAP_HTTP_TIMEOUT_SECONDS", self.basemap_http_timeout_seconds
        )
        self.basemap_http_max_retries = self._env_int(
            "BASEMAP_HTTP_MAX_RETRIES", self.basemap_http_max_retries
        )
        self.basemap_s3_object_ttl_days = self._env_int(
            "BASEMAP_S3_OBJECT_TTL_DAYS", self.basemap_s3_object_ttl_days
        )
        self.basemap_online_fallback_enabled = self._env_bool(
            "BASEMAP_ONLINE_FALLBACK_ENABLED", self.basemap_online_fallback_enabled
        )
        self.basemap_provider_availability_ttl = self._env_int(
            "BASEMAP_PROVIDER_AVAILABILITY_TTL",
            self.basemap_provider_availability_ttl,
        )
        self.basemap_scrape_state_db_path = os.getenv(
            "BASEMAP_SCRAPE_STATE_DB_PATH", self.basemap_scrape_state_db_path
        )
        self.basemap_scrape_checkpoint_every = self._env_int(
            "BASEMAP_SCRAPE_CHECKPOINT_EVERY", self.basemap_scrape_checkpoint_every
        )
        self.basemap_scrape_checkpoint_seconds = self._env_float(
            "BASEMAP_SCRAPE_CHECKPOINT_SECONDS", self.basemap_scrape_checkpoint_seconds
        )
        self.basemap_reader_http_concurrent = self._env_int(
            "BASEMAP_READER_HTTP_CONCURRENT", self.basemap_reader_http_concurrent
        )
        self.basemap_reader_http_timeout_seconds = self._env_int(
            "BASEMAP_READER_HTTP_TIMEOUT_SECONDS",
            self.basemap_reader_http_timeout_seconds,
        )
        self.basemap_reader_http_max_retries = self._env_int(
            "BASEMAP_READER_HTTP_MAX_RETRIES", self.basemap_reader_http_max_retries
        )
        self.basemap_request_deadline_seconds = self._env_float(
            "BASEMAP_REQUEST_DEADLINE_SECONDS", self.basemap_request_deadline_seconds
        )
        self.basemap_cache_control_tile_miss = os.getenv(
            "BASEMAP_CACHE_CONTROL_TILE_MISS", self.basemap_cache_control_tile_miss
        )
        self.basemap_cache_control_tile = os.getenv(
            "BASEMAP_CACHE_CONTROL_TILE", self.basemap_cache_control_tile
        )
        self.basemap_sync_mode = (
            os.getenv("BASEMAP_SYNC_MODE", self.basemap_sync_mode)
            or self.basemap_sync_mode
        )
        self.basemap_scrape_parallelism_mode = (
            os.getenv(
                "BASEMAP_SCRAPE_PARALLELISM_MODE", self.basemap_scrape_parallelism_mode
            )
            or self.basemap_scrape_parallelism_mode
        )
        self.basemap_scrape_per_host_concurrent = self._env_int(
            "BASEMAP_SCRAPE_PER_HOST_CONCURRENT",
            self.basemap_scrape_per_host_concurrent,
        )
        self.basemap_provider_unhealthy_threshold = self._env_int(
            "BASEMAP_PROVIDER_UNHEALTHY_THRESHOLD",
            self.basemap_provider_unhealthy_threshold,
        )
        self.basemap_provider_cooldown_schedule = self._env_int_list(
            "BASEMAP_PROVIDER_COOLDOWN_SCHEDULE",
            self.basemap_provider_cooldown_schedule,
        )
        self.basemap_provider_error_rate_threshold = self._env_float(
            "BASEMAP_PROVIDER_ERROR_RATE_THRESHOLD",
            self.basemap_provider_error_rate_threshold,
        )
        self.basemap_provider_error_rate_min_samples = self._env_int(
            "BASEMAP_PROVIDER_ERROR_RATE_MIN_SAMPLES",
            self.basemap_provider_error_rate_min_samples,
        )

        # SMN API + weather-stations subsystem
        self.s3_weather_stations_bucket_name = os.getenv(
            "S3_WEATHER_STATIONS_BUCKET_NAME", self.s3_weather_stations_bucket_name
        )
        self.smn_api_base_url = os.getenv("SMN_API_BASE_URL", self.smn_api_base_url)
        self.smn_api_username = os.getenv("SMN_API_USERNAME", self.smn_api_username)
        self.smn_api_password = os.getenv("SMN_API_PASSWORD", self.smn_api_password)
        self.smn_api_token_settling_delay_seconds = self._env_float(
            "SMN_API_TOKEN_SETTLING_DELAY_SECONDS",
            self.smn_api_token_settling_delay_seconds,
        )
        self.smn_api_log_requests = self._env_bool(
            "SMN_API_LOG_REQUESTS", self.smn_api_log_requests
        )
        self.smn_stations_registry_url = os.getenv(
            "SMN_STATIONS_REGISTRY_URL", self.smn_stations_registry_url
        )
        self.weather_stations_registry_fallback_enabled = self._env_bool(
            "WEATHER_STATIONS_REGISTRY_FALLBACK_ENABLED",
            self.weather_stations_registry_fallback_enabled,
        )
        self.weather_stations_registry_fallback_path = os.getenv(
            "WEATHER_STATIONS_REGISTRY_FALLBACK_PATH",
            self.weather_stations_registry_fallback_path,
        )
        self.weather_stations_admin_password = os.getenv(
            "WEATHER_STATIONS_ADMIN_PASSWORD", self.weather_stations_admin_password
        )
        self.weather_stations_sync_mode = (
            os.getenv("WEATHER_STATIONS_SYNC_MODE", self.weather_stations_sync_mode)
            or self.weather_stations_sync_mode
        )
        self.weather_stations_scrape_interval_seconds = self._env_int(
            "WEATHER_STATIONS_SCRAPE_INTERVAL_SECONDS",
            self.weather_stations_scrape_interval_seconds,
        )
        self.weather_stations_scrape_lock_path = os.getenv(
            "WEATHER_STATIONS_SCRAPE_LOCK_PATH",
            self.weather_stations_scrape_lock_path,
        )
        self.weather_stations_http_timeout_seconds = self._env_int(
            "WEATHER_STATIONS_HTTP_TIMEOUT_SECONDS",
            self.weather_stations_http_timeout_seconds,
        )
        self.weather_stations_http_max_retries = self._env_int(
            "WEATHER_STATIONS_HTTP_MAX_RETRIES",
            self.weather_stations_http_max_retries,
        )
        self.weather_stations_token_cache_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_TOKEN_CACHE_TTL_SECONDS",
            self.weather_stations_token_cache_ttl_seconds,
        )
        self.weather_stations_s3_object_ttl_days = self._env_int(
            "WEATHER_STATIONS_S3_OBJECT_TTL_DAYS",
            self.weather_stations_s3_object_ttl_days,
        )
        self.weather_stations_list_cache_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_LIST_CACHE_TTL_SECONDS",
            self.weather_stations_list_cache_ttl_seconds,
        )
        self.weather_stations_cache_control_latest = os.getenv(
            "WEATHER_STATIONS_CACHE_CONTROL_LATEST",
            self.weather_stations_cache_control_latest,
        )
        self.weather_stations_cache_control_tilesets = os.getenv(
            "WEATHER_STATIONS_CACHE_CONTROL_TILESETS",
            self.weather_stations_cache_control_tilesets,
        )
        self.weather_stations_cache_control_registry = os.getenv(
            "WEATHER_STATIONS_CACHE_CONTROL_REGISTRY",
            self.weather_stations_cache_control_registry,
        )
        self.weather_stations_cache_control_series = os.getenv(
            "WEATHER_STATIONS_CACHE_CONTROL_SERIES",
            self.weather_stations_cache_control_series,
        )
        self.weather_stations_cache_control_snapshot = os.getenv(
            "WEATHER_STATIONS_CACHE_CONTROL_SNAPSHOT",
            self.weather_stations_cache_control_snapshot,
        )
        self.weather_stations_redis_cache_enabled = self._env_bool(
            "WEATHER_STATIONS_REDIS_CACHE_ENABLED",
            self.weather_stations_redis_cache_enabled,
        )
        self.weather_stations_redis_latest_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_REDIS_LATEST_TTL_SECONDS",
            self.weather_stations_redis_latest_ttl_seconds,
        )
        self.weather_stations_redis_tilesets_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_REDIS_TILESETS_TTL_SECONDS",
            self.weather_stations_redis_tilesets_ttl_seconds,
        )
        self.weather_stations_redis_snapshot_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_REDIS_SNAPSHOT_TTL_SECONDS",
            self.weather_stations_redis_snapshot_ttl_seconds,
        )
        self.weather_stations_redis_registry_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_REDIS_REGISTRY_TTL_SECONDS",
            self.weather_stations_redis_registry_ttl_seconds,
        )
        self.weather_stations_redis_series_ttl_seconds = self._env_int(
            "WEATHER_STATIONS_REDIS_SERIES_TTL_SECONDS",
            self.weather_stations_redis_series_ttl_seconds,
        )
        self.weather_stations_redis_animation_warm_buckets = self._env_int(
            "WEATHER_STATIONS_REDIS_ANIMATION_WARM_BUCKETS",
            self.weather_stations_redis_animation_warm_buckets,
        )
        self.weather_stations_series_hours = self._env_int(
            "WEATHER_STATIONS_SERIES_HOURS",
            self.weather_stations_series_hours,
        )
        self.weather_stations_api_key_auth_enabled = self._env_bool(
            "WEATHER_STATIONS_API_KEY_AUTH_ENABLED",
            self.weather_stations_api_key_auth_enabled,
        )
        self.s3_api_keys_bucket_name = os.getenv(
            "S3_API_KEYS_BUCKET_NAME", self.s3_api_keys_bucket_name
        )

        # Metrics / observability dashboard
        self.metrics_enabled = self._env_bool("METRICS_ENABLED", self.metrics_enabled)
        self.metrics_db_path = os.getenv("METRICS_DB_PATH", self.metrics_db_path)
        self.metrics_retention_days = self._env_int(
            "METRICS_RETENTION_DAYS", self.metrics_retention_days
        )
        self.metrics_max_rows = self._env_int("METRICS_MAX_ROWS", self.metrics_max_rows)
        self.metrics_lock_path = os.getenv("METRICS_LOCK_PATH", self.metrics_lock_path)
        self.redis_metrics_sample_interval_seconds = self._env_int(
            "REDIS_METRICS_SAMPLE_INTERVAL_SECONDS",
            self.redis_metrics_sample_interval_seconds,
        )
        self.redis_metrics_scan_count = self._env_int(
            "REDIS_METRICS_SCAN_COUNT", self.redis_metrics_scan_count
        )
        self.redis_metrics_memory_batch_size = self._env_int(
            "REDIS_METRICS_MEMORY_BATCH_SIZE", self.redis_metrics_memory_batch_size
        )
        self.redis_metrics_memory_sample_per_domain = self._env_int(
            "REDIS_METRICS_MEMORY_SAMPLE_PER_DOMAIN",
            self.redis_metrics_memory_sample_per_domain,
        )

    def _validate(self) -> None:
        # pylint: disable=too-many-branches
        """Fail-fast validation for values with a fixed domain."""
        if self.basemap_sync_mode not in self._BASEMAP_SYNC_MODES:
            raise ValueError(
                f"Invalid basemap_sync_mode={self.basemap_sync_mode!r}; "
                f"expected one of {self._BASEMAP_SYNC_MODES}"
            )
        if (
            self.basemap_sync_mode == "relay_only"
            and not self.basemap_online_fallback_enabled
        ):
            raise ValueError(
                "Invalid combination: basemap_sync_mode='relay_only' requires "
                "basemap_online_fallback_enabled=true; otherwise the service "
                "has no source of tile data."
            )
        if self.basemap_scrape_parallelism_mode not in self._BASEMAP_PARALLELISM_MODES:
            raise ValueError(
                f"Invalid basemap_scrape_parallelism_mode="
                f"{self.basemap_scrape_parallelism_mode!r}; "
                f"expected one of {self._BASEMAP_PARALLELISM_MODES}"
            )
        if self.basemap_scrape_per_host_concurrent < 1:
            raise ValueError(
                "basemap_scrape_per_host_concurrent must be >= 1 "
                f"(got {self.basemap_scrape_per_host_concurrent})"
            )
        if self.basemap_scrape_per_host_concurrent > self.basemap_scrape_concurrent:
            raise ValueError(
                "basemap_scrape_per_host_concurrent "
                f"({self.basemap_scrape_per_host_concurrent}) exceeds global "
                f"basemap_scrape_concurrent ({self.basemap_scrape_concurrent}); "
                "per-host limit larger than the global budget has no effect."
            )
        if self.basemap_provider_unhealthy_threshold < 1:
            raise ValueError(
                "basemap_provider_unhealthy_threshold must be >= 1 "
                f"(got {self.basemap_provider_unhealthy_threshold})"
            )
        if not 0 < self.basemap_provider_error_rate_threshold <= 1:
            raise ValueError(
                "basemap_provider_error_rate_threshold must be in (0, 1] "
                f"(got {self.basemap_provider_error_rate_threshold})"
            )
        if self.basemap_provider_error_rate_min_samples < 1:
            raise ValueError(
                "basemap_provider_error_rate_min_samples must be >= 1 "
                f"(got {self.basemap_provider_error_rate_min_samples})"
            )
        schedule = self.basemap_provider_cooldown_schedule
        if not schedule:
            raise ValueError(
                "basemap_provider_cooldown_schedule must be a non-empty list "
                "of positive seconds (got empty)"
            )
        if any(seconds <= 0 for seconds in schedule):
            raise ValueError(
                "basemap_provider_cooldown_schedule entries must all be > 0 "
                f"(got {schedule})"
            )
        if any(b < a for a, b in zip(schedule, schedule[1:])):
            raise ValueError(
                "basemap_provider_cooldown_schedule must be monotonically "
                f"non-decreasing (got {schedule})"
            )
        if self.weather_stations_sync_mode not in self._WEATHER_STATIONS_SYNC_MODES:
            raise ValueError(
                f"Invalid weather_stations_sync_mode="
                f"{self.weather_stations_sync_mode!r}; "
                f"expected one of {self._WEATHER_STATIONS_SYNC_MODES}"
            )
        if self.app_role not in self._APP_ROLES:
            raise ValueError(
                f"Invalid app_role={self.app_role!r}; "
                f"expected one of {self._APP_ROLES}"
            )
        for _name in ("gfs_cycles_to_keep", "gfs_tile_ttl", "gfs_geojson_ttl"):
            if getattr(self, _name) < 1:
                raise ValueError(f"{_name} must be >= 1 (got {getattr(self, _name)})")
        if self.weather_stations_scrape_interval_seconds < 60:
            raise ValueError(
                "weather_stations_scrape_interval_seconds must be >= 60 "
                f"(got {self.weather_stations_scrape_interval_seconds})"
            )
        if self.weather_stations_s3_object_ttl_days < 1:
            raise ValueError(
                "weather_stations_s3_object_ttl_days must be >= 1 "
                f"(got {self.weather_stations_s3_object_ttl_days})"
            )
        # Admin endpoints mint/revoke API keys; if the gateway is on but no
        # admin password is configured, the system is permanently locked out
        # (no keys can ever be issued). Fail fast at boot.
        if (
            self.weather_stations_api_key_auth_enabled
            and not self.weather_stations_admin_password
        ):
            raise ValueError(
                "weather_stations_api_key_auth_enabled=true requires "
                "WEATHER_STATIONS_ADMIN_PASSWORD to be set; otherwise the "
                "admin endpoints are unreachable and no API keys can be issued."
            )
        # The keystore is S3-backed; without S3 there is nowhere to persist
        # the hashed key objects. Fail fast rather than 500 on first request.
        if self.weather_stations_api_key_auth_enabled and not self.is_s3_configured():
            raise ValueError(
                "weather_stations_api_key_auth_enabled=true requires S3 to be "
                "configured (S3_TILES_DATA_ENDPOINT/ACCESS_KEY/SECRET_KEY); "
                "the API key keystore persists to "
                f"s3://{self.s3_api_keys_bucket_name}/."
            )
        # The scraper cannot mint a JWT without credentials.
        if self.weather_stations_sync_mode == "full" and not (
            self.smn_api_username and self.smn_api_password
        ):
            raise ValueError(
                "weather_stations_sync_mode='full' requires SMN_API_USERNAME "
                "and SMN_API_PASSWORD to be set; the scraper needs them to "
                "mint a JWT for the SMN API."
            )
        # Row cap is a non-negative backstop (0 disables it).
        if self.metrics_max_rows < 0:
            raise ValueError(
                f"metrics_max_rows must be >= 0 (got {self.metrics_max_rows}); "
                "use 0 to disable the per-table row cap."
            )
        if self.redis_metrics_memory_sample_per_domain < 0:
            raise ValueError(
                "redis_metrics_memory_sample_per_domain must be >= 0 "
                f"(got {self.redis_metrics_memory_sample_per_domain}); "
                "use 0 to disable sampling (exact census)."
            )

    def is_s3_configured(self) -> bool:
        """Check if S3 is properly configured."""
        return bool(
            self.s3_tiles_data_endpoint
            and self.s3_tiles_data_access_key
            and self.s3_tiles_data_secret_key
        )

    @staticmethod
    def get_settings() -> "Settings":
        """Factory method to create and return a Settings instance."""
        return Settings()
