# CLAUDE.md

## Collaboration Protocol

1. **Before coding**: Describe approach → wait for approval. Ask clarifying questions if requirements are ambiguous.
2. **>3 file changes**: Stop. Break into smaller tasks first.
3. **After coding**: List what could break and which tests need adding/updating.

## Commands

```bash
make install     # Poetry + all deps (including dev)
make up          # Docker dev (hot-reload, mounts ./src)
make local       # Native dev (uvicorn --reload on :8080, requires make install)
make test        # Tests in Docker (outputs to ./reports/)
make precommit   # Pre-commit hooks (black, pylint, mypy)
make prod        # Docker production build

# Local testing:
poetry run pytest -m "not skip" --cov=src --cov-report=html:reports/coverage
poetry run pytest tests/application/test_basic_endpoints.py::test_root_ok
```

Bare commands require `source .venv/bin/activate && cmd`.

## Architecture

FastAPI microservice (Python 3.13) serving satellite/radar/ECMWF/WRF/GFS tiles and SMN weather-station observations (cached from S3/SeaweedFS) and basemap tiles (backed up from external providers into a dedicated S3 bucket). Redis is the hot cache across all domains.

### Entrypoint & Lifecycle

- `src/main.py` — FastAPI app, CORS middleware, `lifespan` context manager for all background services (six per-product sync loops, basemap scraper, weather-stations scraper, Redis-metrics collector) + `uvloop` event loop policy.
- **`APP_ROLE` (web/worker/all)** — one image, role chosen at startup. `web` serves HTTP only; `worker` runs the background sync/scrape/metrics loops only; `all` (default) does both. The lifespan always builds the read-side config; `main._runs_background_jobs()` gates only the `.start()` of the background loops. Deploy a `web` container + a `worker` container (same image, only env differs — see `docker-compose*.yaml`) so the heavy background sync can't CPU-starve request serving. They communicate only via Redis / S3 / the shared `/app/data` metrics SQLite.
- `src/dependencies.py` — Module-level singletons (`settings`, `logger`, `redis_client`, `metrics_store`, `basemap_service`); `_weather_stations_keystore` and `_basemap_state_store` are stamped in later by the lifespan.
- `src/settings.py` — Plain class reading env vars via `os.getenv` + `python-dotenv`, merged with `settings.json`. Fail-fast `_validate()` runs after load.

### Layered Structure

```
routes/       → API endpoints (FastAPI routers): satellite, radar, ecmwf, basemap, sync
services/     → Business logic (singleton instances + per-domain sync services)
models/       → Pydantic response models
clients/      → External service clients (S3, Redis, HTTP tile client, basemap state store)
controller/   → General endpoints (health, root)
```

### Data Domains

| Domain | Route prefix | Service | Source / storage |
|---|---|---|---|
| **Satellite** | `/products/{product_id}/{instrument_id}/{channel_id}/...` | `SatelliteService` (GOES-19 ABI + GLM) | `tiles-data` bucket, prefixes `tiles/goes19/abi/{c02,c09,c13}` and `tiles/goes19/glm/{fed,toe,mfa}` |
| **Radar** | `/products/radar/{radar_id}/{variable_id}/{elevation_id}/...` | `RadarService` | `tiles-data` bucket, prefix `tiles/radar/sinarame/` |
| **ECMWF** | `/products/ecmwf/total-precipitation/...`, `/products/ecmwf/mean-sea-level-pressure/...` | `EcmwfTotalPrecipitationService`, `EcmwfMslpService` | `tiles-data` bucket, prefixes `tiles/ecmwf-ifs/tp`, `cog/ecmwf-ifs/mslp`, `geojson/ecmwf-ifs/mslp` |
| **WRF** | `/products/wrf/...` | `WrfService` | `tiles-data` bucket, prefixes `tiles/wrf-arg4k/`, `cog/wrf-arg4k/`, `geojson/wrf-arg4k/` |
| **GFS** | `/products/gfs/...` | `GfsService` | `tiles-data` bucket, prefixes `tiles/gfs/`, `cog/gfs/`, `geojson/gfs/` |
| **Weather stations** | `/weather-stations/...`, `/weather-stations/admin/...` | `WeatherStationsService` + `WeatherStationsScraperService` | `weather-stations-data` bucket, prefix `weather-stations/`; hashed API keys in the `api-keys` bucket |
| **Basemap** | `/basemap/{provider_id}/{z}/{x}/{y}.png`, `/basemap/providers` | `BasemapService` + `BasemapTileReader` + `BasemapScraperService` | `basemap-tiles` bucket, prefix `basemap/{provider_id}/` |

Channel mapping (`CHANNEL_DIR_MAPPING`, the path below the `tiles/` and `cog/` roots):
`c02|c09|c13` → `goes19/abi/<id>`, `fed|toe|mfa` → `goes19/glm/<id>`.

### Background sync / scrape

All started in the FastAPI `lifespan`, only when `main._runs_background_jobs()` (`APP_ROLE` ∈ {`worker`, `all`}), each gated by an `fcntl` file lock so only one Uvicorn worker runs it:

- **Per-product sync loops** — one `DomainSyncService` subclass (`services/domain_sync_service.py`) per product: satellite, radar, ECMWF-TP, ECMWF-MSLP, WRF, GFS, listed in `main._SYNC_SERVICES`. Started only in `sync_mode=full`. Each owns its S3 client, interval and watchdog, so no product can monopolize another's scheduling or S3 budget; a cycle that exceeds the watchdog (`sync_domain_timeout_seconds` 300 s; WRF 1200 s, GFS 900 s) is cancelled, recorded as `timeout`, and retried next cycle — a unit is indexed only after it fully downloads, so resuming from the frontier is safe. Prefixes: satellite `tiles/goes19/abi/*` + `tiles/goes19/glm/*`, radar `tiles/radar/sinarame/{radar}/{product}/elev*`, ECMWF total precipitation tiles `tiles/ecmwf-ifs/tp/{forecast_ts}/{period_ts}` and mean sea level pressure GeoJSONs `geojson/ecmwf-ifs/mslp/{forecast_ts}/{timestamp_ts}.json`, WRF `tiles/wrf-arg4k/`, GFS `tiles/gfs/`. `{period_ts}` / `{timestamp_ts}` are end-of-period timestamps (`YYYYMMDDTHHmmZ`, every 3 h, T+6 … T+144 of the run; for TP the value accumulates the previous 6 h, for MSLP it's an instantaneous snapshot). Strategies per domain: `SatelliteFullSyncStrategy` / `SatelliteOnDemandStrategy`, `RadarFullSyncStrategy` / `RadarOnDemandStrategy`, `EcmwfTpFullSyncStrategy` / `EcmwfTpOnDemandStrategy`, `EcmwfMslpFullSyncStrategy` / `EcmwfMslpOnDemandStrategy`, `WrfFullSyncStrategy` / `WrfOnDemandStrategy`, `GfsFullSyncStrategy` / `GfsOnDemandStrategy`. Retention: `ecmwf_forecasts_to_keep`, `wrf_inits_to_keep`, `gfs_cycles_to_keep`.
- **`BasemapScraperService`** — periodic full-sweep scrape of external providers (IGN, ArcGIS, Google) writing to the `basemap-tiles` bucket + Redis. Resumable via SQLite cursor (`basemap_scrape_state_db_path`). Runs in `basemap_sync_mode ∈ {full, on_demand, no_cache}`; only `full` also writes Redis. Per-provider **circuit breaker** (`basemap_provider_health` table) skips flaky providers for an exponential cooldown when `HttpTileClient` surfaces `ProviderUnavailableError` (exhausted retries / network failures) — state survives restarts. One clean sweep resets the trip counter. A **downstream (S3/Redis) outage** is caught as `_TileOutcome.STORAGE_ERROR`: the scraper skips stamping `last_completed` and floors the next sleep to ~60s so storage recovers automatically instead of waiting a full interval. The bucket **lifecycle policy** is also applied lazily from inside the loop so an S3-down startup self-heals on the next sweep (no hard startup dependency).
- **`WeatherStationsScraperService`** — polls the SMN API every `weather_stations_scrape_interval_seconds` (300 s; `_validate` rejects < 60) into the `weather-stations-data` bucket, write-through to the Redis hot keys. Skipped when `weather_stations_sync_mode=disabled`.
- **`RedisMetricsService`** — per-domain Redis memory/key census every `redis_metrics_sample_interval_seconds` (300 s) into the metrics SQLite. Off when `metrics_enabled=false`.

`S3Client` uses `aioboto3` with semaphore-limited concurrency (default 5, `s3_max_concurrent_downloads`). HTTP tile fetches use `HttpTileClient` (`httpx.AsyncClient`) with its own concurrency + retry budget.

### Basemap cache modes

Independent of `sync_mode`. Four values controlling the two orthogonal cache axes (Redis, S3):

| Mode | Scraper runs | Scraper writes S3 | Scraper writes Redis | Reader Redis | Reader S3 | Reader relay |
|---|---|---|---|---|---|---|
| `full` (default) | yes | yes | yes | yes | yes | yes |
| `on_demand` | yes | yes | **no** | yes | yes | yes |
| `no_cache` | yes | yes | no | **no** | yes | yes |
| `relay_only` | **no** | — | — | no | **no** | yes |

Derivation lives in `main.configure_basemap`. `relay_only` requires `basemap_online_fallback_enabled=true` (enforced by `Settings._validate`). Reader tier order is provider → Redis → S3 (`BasemapTileReader`): upstream is authoritative, the caches answer when it fails. Operator-facing rationale per mode: README, "Basemap cache modes".

### Key Patterns

- Services are module-level singletons configured via `.configure(...)` inside `lifespan` (DI via method call, not constructor — see `BasemapService`).
- Blocking I/O offloaded via `asyncio.to_thread()`: every SQLite call (`clients/metrics_store.py`, `clients/basemap_state_store.py`) and the rasterio COG reads in `services/point_value_strategy.py`.
- Tiles served as a plain `Response` over the in-memory bytes (`routes.utils.create_tile_response`) with `image/webp` (satellite/radar/ECMWF/WRF/GFS) or `image/png` (basemap), an ETag and a long `Cache-Control`. A handler that serves a placeholder in place of a missing payload (transparent tile, empty FeatureCollection) MUST label it with the **miss** half of `routes.utils.etag_pair` and a short, non-`immutable` `*_cache_control_tile_miss` — a gap must never share the hit's ETag, or the client's revalidation matches its own cached gap and 304s forever, so the real payload never arrives once the data lands. This holds for basemap, radar, WRF (tiles + barbs) and GFS (tiles + barbs); `tests/application/test_tile_miss_etag.py` parametrizes the invariant over all six.
- Tests use `pytest-socket` — network disabled by default, only `127.0.0.1` allowed.

## Configuration

**Env vars** (`.env`, see `.env.example`):

| Variable | Purpose |
|---|---|
| `S3_TILES_DATA_ENDPOINT/ACCESS_KEY/SECRET_KEY` | S3/SeaweedFS connection |
| `S3_TILES_DATA_BUCKET_NAME` | Satellite/radar/ECMWF/WRF/GFS bucket (default `tiles-data`) |
| `S3_BASEMAP_BUCKET_NAME` | Basemap cold-backup bucket (default `basemap-tiles`) |
| `REDIS_URL` | Redis. No code default — must be set (`.env.example`: `redis://redis:6379/0`) |
| `SYNC_MODE` | `full` or `on_demand` — applies to satellite, radar, ECMWF, WRF, GFS (default: `full`) |
| `BASEMAP_SYNC_MODE` | `full` / `on_demand` / `no_cache` / `relay_only` (default: `full`) |
| `BASEMAP_ONLINE_FALLBACK_ENABLED` | Disable tier-3 provider relay when `false` (default: `true`) |
| `BASEMAP_{PROVIDER}_URL` | Per-provider URL template (URLs only — names/zooms/TMS defaults in `basemap_config.py`) |
| `WEB_CONCURRENCY` | Uvicorn worker count |
| `APP_ENV` | `development` = human logs; `production` = NewRelic formatter |
| `APP_ROLE` | `web` (serve only) / `worker` (background jobs only) / `all` (both, default) |

**Runtime tuning** — `settings.json` is merged with env vars (env wins). `src/settings.py` is the source of truth for defaults, so `settings.json` carries only *overrides* plus the few keys that have no code default (a value equal to its default is omitted). Every key has a matching `UPPERCASE` env override. Per-domain keys nest under a namespace object (`basemap`, `ecmwf`, `wrf`, …) and may nest further (e.g. `basemap.scrape.delay_ms`, `ecmwf.mslp.geojson_ttl`); the loader (`Settings._flatten`) recursively flattens them back to underscore-joined `<namespace>_<key>` names, so Python attrs and env vars stay flat regardless of nesting depth (`basemap.scrape.delay_ms` → `settings.basemap_scrape_delay_ms` / `BASEMAP_SCRAPE_DELAY_MS`). Unrecognized keys (not in `Settings._JSON_KEYS`) are logged as a warning rather than silently dropped. Knobs by group (defaults in `settings.py`; most are omitted from `settings.json` while unchanged):

- Shared: `sync_mode`, `satellite_tile_ttl`, `radar_tile_ttl`, `radar_cache_control_tile_miss`, `tileset_listing_ttl`, `s3_max_concurrent_downloads`, `cache_control_config`, `cache_control_tile`.
- Sync cadence: `sync_interval_seconds` + `sync_min_sleep_seconds` + `sync_domain_timeout_seconds` (satellite, radar, both ECMWF loops; WRF and GFS carry their own interval/timeout).
- ECMWF: `ecmwf_tile_ttl`, `ecmwf_forecasts_to_keep`.
- WRF: `wrf_tile_ttl`, `wrf_geojson_ttl`, `wrf_cache_control_tile_miss`, `wrf_inits_to_keep`, `wrf_overlay_recheck_ttl`, `wrf_sync_interval_seconds`, `wrf_sync_timeout_seconds`.
- GFS: `gfs_tile_ttl`, `gfs_geojson_ttl`, `gfs_cycles_to_keep`, `gfs_sync_interval_seconds`, `gfs_sync_timeout_seconds`, `gfs_cache_control_tile_miss`.
- Weather stations: `weather_stations_sync_mode` (`full` / `disabled`), `weather_stations_scrape_*`, `weather_stations_http_*`, `weather_stations_s3_object_ttl_days`, `weather_stations_cache_control_*`, `weather_stations_redis_*`, `weather_stations_series_hours`, `weather_stations_api_key_auth_enabled`.
- Metrics: `metrics_enabled`, `metrics_db_path`, `metrics_retention_days`, `metrics_max_rows`, `metrics_lock_path`, `redis_metrics_*`.
- Basemap: `basemap_sync_mode`, `basemap_providers`, `basemap_tile_ttl` (30 d default), `basemap_scrape_*` (incl. `basemap_scrape_parallelism_mode` — `sequential`/`per_origin`/`full` — and `basemap_scrape_per_host_concurrent`, a per-host request budget stacked under `basemap_scrape_concurrent`), `basemap_provider_cooldown_schedule` + `basemap_provider_error_rate_*` (circuit breaker — trips on per-sweep UNAVAILABLE error rate, exponential-backoff cooldown, state persisted in SQLite `basemap_provider_health`), `basemap_cache_*`, `basemap_bbox_*`, `basemap_http_*`, `basemap_reader_http_*`, `basemap_request_deadline_seconds`, `basemap_s3_object_ttl_days` (35 d default — strictly greater than scrape interval), `basemap_online_fallback_enabled`, `basemap_provider_availability_ttl`, `basemap_scrape_state_db_path`, `basemap_cache_control_tile_miss`.

## Engineering Rules

### FastAPI Conventions

- All route handlers must be `async def`. Wrap blocking I/O with `asyncio.to_thread()`.
- Use `Depends()` for shared logic — prefer `Depends(get_settings)` over importing module-level singletons in routes.
- Type all endpoints: `response_model`, status codes, Pydantic models. Never return raw dicts.
- Services return `None` or raise domain exceptions — **never `HTTPException`**. Routes translate to HTTP status codes.
- Use `lifespan` pattern only — never deprecated `@app.on_event`.

### Code Style

- Early returns; functions <20 lines; one class per file.
- `handle_` prefix for event handlers; verb-noun naming.
- Routes handle HTTP concerns only — no business logic.
- Immutable by default: `frozen=True`, `slots=True` dataclasses for data containers.
- Fail fast: validate early, domain-specific exceptions, no bare `except`.
- **Minimal changes**: only modify code directly related to the task.

### Design Principles

- **Dependency Injection (DI) via constructor**: Pass deps through `__init__` (as `SyncService` does with `S3Client`). Don't hard-import and instantiate clients internally. No service locator pattern.
- **Abstractions**: Depend on ABC (shared impl) or Protocol (structural typing). Keep interfaces small (ISP).
- **Composition over inheritance**: Prefer has-a over is-a.
- **Open/Closed**: New data domains → new service inheriting `BaseProductService` + register. Don't add conditionals to existing services.
- **Liskov**: `BaseProductService` subclasses must honor the base contract.
- **Typed registries**: `Generic[T]`, validate on registration, scoped not global.

### Extending the Codebase

| Addition | Steps |
|---|---|
| **New data domain** | Create `services/{domain}_service.py` (inherit `BaseProductService`), `models/{domain}.py`, `routes/{domain}.py`, include router in `main.py`. |
| **New external client** | Add to `clients/` following `S3Client`'s async pattern. Connection params via constructor; no business logic. |
| **New config** | Add to `Settings` with sensible default. Centralize in `settings.py` — no scattered `os.getenv()`. |

### Testing

- Test interfaces, not implementations — tests should work with any conforming impl.
- Use DI to make mocking/stubbing easy.
- Mock external services (S3, Redis, the SMN API, basemap providers) — never call them in unit tests; `pytest-socket` blocks the sockets anyway.
- Use Protocol for lightweight test doubles.

## Resource Management

### Memory
- Stream large files (generators / async iteration); context managers (`with`/`async with`) for all cleanup.
- Bounded buffers: `asyncio.Queue(maxsize=N)`. Chunk-process large datasets.
- `weakref` for caches that shouldn't prevent GC. `memory_profiler` for suspected leaks.

### Concurrency
- `asyncio` for I/O-bound; `concurrent.futures.ThreadPoolExecutor` for blocking I/O in async context.
- `asyncio.Semaphore(N)` to bound concurrent ops — no unbounded task creation.
- Never use blocking I/O in async functions (use `asyncio.to_thread`).
- Connection pooling for HTTP sessions and Redis.
- Batch small operations to reduce overhead; lazy evaluation for expensive computations.

### Infrastructure
- Docker: `mem_limit`, `cpus`, `--memory-swap=0`. Monitor with `docker stats`.
- S3: multipart uploads >5MB, aioboto3 async, exponential backoff retries, stream to disk.
- Monitoring: structured logging with timing (`logger.info("msg", extra={...})`), track queue depth / processing time / error rates, `time.perf_counter()` for measurements.

## Anti-Patterns

- ❌ God objects, circular deps, global mutable state, tight framework coupling
- ❌ Mixing business logic with infrastructure (routes, clients)
- ❌ Unbounded async task creation (use semaphores)
- ❌ Blocking I/O in async functions (use `asyncio.to_thread`)
- ❌ Catching `Exception` without re-raise or proper handling
- ❌ Not cleaning up resources in error paths
- ❌ Ignoring backpressure signals from queues

## CI/CD

- **test.yml** — Push/PR to non-main (also `workflow_call`): gitleaks secret scan, then Python 3.13.13 + Poetry + pytest with coverage.
- **deploy.yml** — Push to main (or manual dispatch): runs `test.yml`, then the Coolify webhook deployment (`needs: [test]`). A parallel Trivy image scan reports HIGH and fails on CRITICAL, but does not gate the deploy job.