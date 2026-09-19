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
| **Radar** | `/products/radar-{sinarame\|inta}/{radar_id}/{variable_id}/{elevation_id}/...` | `RadarService`, one instance per network | `tiles-data` bucket, prefix `tiles/radar/{sinarame\|inta}/` |
| **ECMWF** | `/products/ecmwf-ifs/total-precipitation/...`, `/products/ecmwf-ifs/mean-sea-level-pressure/...` | `EcmwfTotalPrecipitationService`, `EcmwfMslpService` | `tiles-data` bucket, prefixes `tiles/ecmwf-ifs/total-precipitation`, `cog|geojson/ecmwf-ifs/mean-sea-level-pressure` |
| **WRF** | `/products/wrf-arg4k/...` | `WrfService` | `tiles-data` bucket, prefixes `tiles/wrf-arg4k/`, `cog/wrf-arg4k/`, `geojson/wrf-arg4k/` |
| **GFS** | `/products/gfs/{mean-sea-level-pressure,geopotential-500hpa,geopotential-250hpa}/...` | `GfsService` | `tiles-data` bucket, prefixes `tiles|cog|geojson/gfs/<product>` |
| **Weather stations** | `/weather-stations/...`, `/weather-stations/admin/...` | `WeatherStationsService` + `WeatherStationsScraperService` | `weather-stations-data` bucket, prefix `weather-stations/`; hashed API keys in the `api-keys` bucket |
| **Availability** | `/products/availability` | `ProductAvailabilityService` (+ one contributor per domain) | Reads the existing Redis indexes only — no storage of its own |
| **Basemap** | `/basemap/{provider_id}/{z}/{x}/{y}.png`, `/basemap/providers` | `BasemapService` + `BasemapTileReader` + `BasemapScraperService` | `basemap-tiles` bucket, prefix `basemap/{provider_id}/` |

Channel mapping (`CHANNEL_DIR_MAPPING`, the path below the `tiles/` and `cog/` roots):
`c02|c09|c13` → `goes19/abi/<id>`, `fed|toe|mfa` → `goes19/glm/<id>`.

### Background sync / scrape

All started in the FastAPI `lifespan`, only when `main._runs_background_jobs()` (`APP_ROLE` ∈ {`worker`, `all`}), each gated by an `fcntl` file lock so only one Uvicorn worker runs it:

- **Per-product sync loops** — one `DomainSyncService` subclass (`services/domain_sync_service.py`) per product: satellite, radar, ECMWF-TP, ECMWF-MSLP, WRF, GFS, listed in `main._SYNC_SERVICES`. Started only when `sync_prefetch=true`. Each owns its S3 client, interval and watchdog, so no product can monopolize another's scheduling or S3 budget; a cycle that exceeds the watchdog (`sync_domain_timeout_seconds` 300 s; WRF 1200 s, GFS 900 s) is cancelled, recorded as `timeout`, and retried next cycle — a unit is indexed only after it fully downloads, so resuming from the frontier is safe. Prefixes: satellite `tiles/goes19/abi/*` + `tiles/goes19/glm/*`, radar `tiles/radar/{sinarame|inta}/{radar}/{product}/elev*` (one loop per network), ECMWF total precipitation tiles `tiles/ecmwf-ifs/total-precipitation/{forecast_ts}/{period_ts}` and mean sea level pressure GeoJSONs `geojson/ecmwf-ifs/mean-sea-level-pressure/{forecast_ts}/{timestamp_ts}.json`, WRF `tiles/wrf-arg4k/`, GFS `tiles/gfs/`. `{period_ts}` / `{timestamp_ts}` are end-of-period timestamps (`YYYYMMDDTHHmmZ`, every 3 h, T+6 … T+144 of the run; for TP the value accumulates the previous 6 h, for MSLP it's an instantaneous snapshot). Strategies per domain: `SatelliteFullSyncStrategy` / `SatelliteOnDemandStrategy`, `RadarFullSyncStrategy` / `RadarOnDemandStrategy`, `EcmwfTpFullSyncStrategy` / `EcmwfTpOnDemandStrategy`, `EcmwfMslpFullSyncStrategy` / `EcmwfMslpOnDemandStrategy`, `WrfFullSyncStrategy` / `WrfOnDemandStrategy`, `GfsFullSyncStrategy` / `GfsOnDemandStrategy`. Retention: `ecmwf_forecasts_to_keep`, `wrf_inits_to_keep`, `gfs_cycles_to_keep`.
- **`BasemapScraperService`** — periodic full-sweep scrape of external providers (IGN, ArcGIS, Google) writing to the `basemap-tiles` bucket + Redis. Resumable via SQLite cursor (`basemap_scrape_state_db_path`). Runs in `basemap_backup_mode ∈ {backup_and_prefetch, backup_and_cache_on_read, backup_only}`; only `backup_and_prefetch` also writes Redis. Per-provider **circuit breaker** (`basemap_provider_health` table) skips flaky providers for an exponential cooldown when `HttpTileClient` surfaces `ProviderUnavailableError` (exhausted retries / network failures) — state survives restarts. One clean sweep resets the trip counter. A **downstream outage** is gated by a shared `StorageCircuit` (`services/storage_circuit.py`) — one instance per backend (S3, Redis), opening on a rolling-window failure rate and backing off `1 → 2 → 4 → … → 30s` between half-open probes. While the S3 circuit is open the sweep **pauses in place** and skips the upstream fetch entirely (`_TileOutcome.STORAGE_SKIPPED`, not queued as a failed tile), so an outage costs neither provider quota nor a SQLite row per tile; it logs only on state transitions (opened / probe failed / recovered), which is what keeps a multi-hour outage to a handful of lines. A probe success resumes the sweep at full speed. After 6 failed probes the sweep is **abandoned with its cursor preserved** so the next cycle resumes in place. Either way `last_completed` goes unstamped and the next sleep floors to ~60s. A **Redis-only** failure does not fail the tile: the S3 upload has already landed, and the hot-cache entry repopulates on first read. The bucket **lifecycle policy** is also applied lazily from inside the loop so an S3-down startup self-heals on the next sweep (no hard startup dependency).
- **`WeatherStationsScraperService`** — polls the SMN API every `weather_stations_scrape_interval_seconds` (300 s; `_validate` rejects < 60) into the `weather-stations-data` bucket, write-through to the Redis hot keys. Skipped when `weather_stations_sync_enabled=false`.
- **`RedisMetricsService`** — per-domain Redis memory/key census every `redis_metrics_sample_interval_seconds` (300 s) into the metrics SQLite. Off when `metrics_enabled=false`.

`S3Client` uses `aioboto3` with semaphore-limited concurrency (default 5, `s3_max_concurrent_downloads`). HTTP tile fetches use `HttpTileClient` (`httpx.AsyncClient`) with its own concurrency + retry budget.

### Basemap backup modes

Independent of `sync_prefetch`. Four values controlling the S3 backup sweep and the two read tiers (Redis, S3). The S3 bucket is a cold mirror, not a cache — the sweep walks the whole bbox on a timer regardless of traffic, and `basemap_s3_object_ttl_days` (35 d) outlives the scrape interval on purpose. The three backup modes differ only in what Redis does:

| Mode | Sweep runs | Sweep writes S3 | Sweep writes Redis | Reader Redis | Reader S3 | Reader relay |
|---|---|---|---|---|---|---|
| `backup_and_prefetch` (default) | yes | yes | yes | yes | yes | yes |
| `backup_and_cache_on_read` | yes | yes | **no** | yes | yes | yes |
| `backup_only` | yes | yes | no | **no** | yes | yes |
| `relay_only` | **no** | — | — | no | **no** | yes |

Derivation lives in `main.configure_basemap`. `relay_only` requires `basemap_online_fallback_enabled=true` (enforced by `Settings._validate`) and is the only mode that survives an S3 outage. Reader tier order is provider → Redis → S3 (`BasemapTileReader`): upstream is authoritative, the cached/backed-up copies answer when it fails. Operator-facing rationale per mode: README, "Basemap backup modes".

### Key Patterns

- Services are module-level singletons configured via `.configure(...)` inside `lifespan` (DI via method call, not constructor — see `BasemapService`).
- Blocking I/O offloaded via `asyncio.to_thread()`: every SQLite call (`clients/metrics_store.py`, `clients/basemap_state_store.py`) and the rasterio COG reads in `services/point_value_strategy.py`.
- Tiles served as a plain `Response` over the in-memory bytes (`routes.utils.create_tile_response`) with `image/webp` (satellite/radar/ECMWF/WRF/GFS) or `image/png` (basemap), an ETag and a long `Cache-Control`. A handler that serves a placeholder in place of a missing payload (transparent tile, empty FeatureCollection) MUST label it with the **miss** half of `routes.utils.etag_pair` and a short, non-`immutable` `*_cache_control_tile_miss` — a gap must never share the hit's ETag, or the client's revalidation matches its own cached gap and 304s forever, so the real payload never arrives once the data lands. This holds for basemap, radar, WRF (tiles + barbs) and GFS (tiles + barbs); `tests/application/test_tile_miss_etag.py` parametrizes the invariant over all six.
- **A listing must never cost one Redis connection per item.** The pool (`redis_max_connections`, 100) is shared by every domain, so an endpoint that fans out per forecast step takes the *whole service* down, not just itself — WRF is hourly to F073, so three concurrent init-run listings asked for 219 connections and 500ed satellite, radar and basemap along with it. Per-step index reads go through the pipelined bulk readers (`get_wrf_layers_bulk` / `get_gfs_layers_bulk`, one round trip on one connection) and any remaining S3 discovery fan-out carries its own semaphore. The pool itself is a `BlockingConnectionPool`, so a future wide fan-out queues instead of raising `MaxConnectionsError`. `tests/test_wrf_service.py` and `tests/test_gfs_service.py` guard the no-fan-out shape at the service level.
- **Listings are conditional GETs, via `routes.utils.json_listing_response`.** The ETag is the digest of the payload, so it changes when and only when the listing does. Every JSON listing goes through it (radar, ECMWF x2, WRF, GFS, availability) — the frontend re-probes products on a timer, so the steady state must be an empty 304, not a full body. Radar was the one listing without it and answered ~108 full bodies a minute per client.
- **The broad availability check is one request, not one per product** (`/products/availability`, `services/product_availability_service.py`). Each domain registers an `AvailabilityContributor` keyed by the product's own API path, so a client checks the same string it would have put in a probe URL. **Contributors read through the same strategies the per-product endpoints read through** — that is the load-bearing rule: an earlier version read the Redis indexes directly, a different source of truth from the endpoints it summarised, and so was either a liar (greying out products whose data was in S3 awaiting a sync) or mute (confirming nothing, leaving the client to probe all ~125 anyway). Going through the strategies makes the snapshot *the probes, batched*: identical answers by construction, so `available` is complete and absence means empty. Lookups are bounded by `_LOOKUP_CONCURRENCY` and the whole sweep is memoised for `product_availability_ttl_seconds`, so a warm sweep is index reads and a cold one does the S3 walk once, server-side, shared by every client. WRF needs `list_products()` (and the `idx:wrf:products` axis behind it) because its products are discovered from S3 rather than declared. Individual probes remain for the UI's single recheck button and for when the bundled call itself fails.
- Tests use `pytest-socket` — network disabled by default, only `127.0.0.1` allowed.

## Configuration

**Env vars** (`.env`, see `.env.example`):

| Variable | Purpose |
|---|---|
| `S3_TILES_DATA_ENDPOINT/ACCESS_KEY/SECRET_KEY` | S3/SeaweedFS connection |
| `S3_TILES_DATA_BUCKET_NAME` | Satellite/radar/ECMWF/WRF/GFS bucket (default `tiles-data`) |
| `S3_BASEMAP_BUCKET_NAME` | Basemap cold-backup bucket (default `basemap-tiles`) |
| `REDIS_URL` | Redis. No code default — must be set (`.env.example`: `redis://redis:6379/0`) |
| `SYNC_PREFETCH` | `true` or `false` — background prefetch for satellite, radar, ECMWF, WRF, GFS (default: `true`) |
| `BASEMAP_BACKUP_MODE` | `backup_and_prefetch` / `backup_and_cache_on_read` / `backup_only` / `relay_only` (default: `backup_and_prefetch`) |
| `BASEMAP_ONLINE_FALLBACK_ENABLED` | Disable tier-3 provider relay when `false` (default: `true`) |
| `BASEMAP_{PROVIDER}_URL` | Per-provider URL template (URLs only — names/zooms/TMS defaults in `basemap_config.py`) |
| `WEB_CONCURRENCY` | Uvicorn worker count |
| `APP_ENV` | `development` = human logs; `production` = NewRelic formatter |
| `APP_ROLE` | `web` (serve only) / `worker` (background jobs only) / `all` (both, default) |

**Runtime tuning** — `settings.json` is merged with env vars (env wins). `src/settings.py` is the source of truth for defaults, so `settings.json` carries *overrides*, the keys that have no code default (`satellite.tile_ttl`, `radar.tile_ttl`, the `ecmwf` trio, `cache_control_*`, `sync.interval_seconds` / `min_sleep_seconds`, `tileset_listing_ttl` — the service will not start without them), and one complete block per served product so every product is tunable from the file without reading `settings.py` first. That last rule is why `gfs` is present with values equal to its defaults. Every key has a matching `UPPERCASE` env override. Per-domain keys nest under a namespace object (`basemap`, `ecmwf`, `wrf`, …) and may nest further (e.g. `basemap.scrape.delay_ms`, `ecmwf.mslp.geojson_ttl`); the loader (`Settings._flatten`) recursively flattens them back to underscore-joined `<namespace>_<key>` names, so Python attrs and env vars stay flat regardless of nesting depth (`basemap.scrape.delay_ms` → `settings.basemap_scrape_delay_ms` / `BASEMAP_SCRAPE_DELAY_MS`). Unrecognized keys (not in `Settings._JSON_KEYS`) are logged as a warning rather than silently dropped. Knobs by group (defaults in `settings.py`; most are omitted from `settings.json` while unchanged):

- Shared: `sync_prefetch`, `satellite_tile_ttl`, `radar_tile_ttl`, `radar_cache_control_tile_miss`, `tileset_listing_ttl`, `s3_max_concurrent_downloads`, `cache_control_config`, `cache_control_tile`, `product_availability_ttl_seconds`.
- Sync cadence: `sync_interval_seconds` + `sync_min_sleep_seconds` + `sync_domain_timeout_seconds` (satellite, radar, both ECMWF loops; WRF and GFS carry their own interval/timeout).
- ECMWF: `ecmwf_tile_ttl`, `ecmwf_forecasts_to_keep`.
- WRF: `wrf_tile_ttl`, `wrf_geojson_ttl`, `wrf_cache_control_tile_miss`, `wrf_inits_to_keep`, `wrf_overlay_recheck_ttl`, `wrf_sync_interval_seconds`, `wrf_sync_timeout_seconds`.
- GFS: `gfs_tile_ttl`, `gfs_geojson_ttl`, `gfs_cycles_to_keep`, `gfs_sync_interval_seconds`, `gfs_sync_timeout_seconds`, `gfs_cache_control_tile_miss`.
- Weather stations: `weather_stations_sync_enabled` (bool), `weather_stations_scrape_*`, `weather_stations_http_*`, `weather_stations_s3_object_ttl_days`, `weather_stations_cache_control_*`, `weather_stations_redis_*`, `weather_stations_series_hours`, `weather_stations_api_key_auth_enabled`.
- Metrics: `metrics_enabled`, `metrics_db_path`, `metrics_retention_days`, `metrics_max_rows`, `metrics_lock_path`, `redis_metrics_*`.
- Basemap: `basemap_backup_mode`, `basemap_providers`, `basemap_tile_ttl` (30 d default), `basemap_scrape_*` (incl. `basemap_scrape_parallelism_mode` — `sequential`/`per_origin`/`full` — and `basemap_scrape_per_host_concurrent`, a per-host request budget stacked under `basemap_scrape_concurrent`), `basemap_provider_cooldown_schedule` + `basemap_provider_error_rate_*` (circuit breaker — trips on per-sweep UNAVAILABLE error rate, exponential-backoff cooldown, state persisted in SQLite `basemap_provider_health`), `basemap_cache_*`, `basemap_bbox_*`, `basemap_http_*`, `basemap_reader_http_*`, `basemap_request_deadline_seconds`, `basemap_s3_object_ttl_days` (35 d default — strictly greater than scrape interval), `basemap_online_fallback_enabled`, `basemap_provider_availability_ttl`, `basemap_scrape_state_db_path`, `basemap_cache_control_tile_miss`.

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