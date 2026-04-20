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

FastAPI microservice (Python 3.13) serving satellite/radar/ECMWF tiles (cached from S3/SeaweedFS) and basemap tiles (backed up from external providers into a dedicated S3 bucket). Redis is the hot cache across all domains.

### Entrypoint & Lifecycle

- `src/main.py` — FastAPI app, CORS middleware, `lifespan` context manager for all background services (satellite sync, radar sync, ECMWF sync, basemap scraper) + `uvloop` event loop.
- `src/dependencies.py` — Module-level singletons (`settings`, `logger`, `redis_client`, `basemap_service`).
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
| **Satellite** | `/products/{product_id}/{instrument_id}/{channel_id}/...` | `SatelliteService` (GOES-19 ABI + GLM) | `tiles-data` bucket, prefixes `tiles/band_{2,9,13}` and `tiles/glm_{fed,toe,mfa}` |
| **Radar** | `/products/radar/{radar_id}/{variable_id}/{elevation_id}/...` | `RadarService` | `tiles-data` bucket, prefix `tiles/radar/` |
| **ECMWF** | `/ecmwf/...` | `EcmwfService` | `tiles-data` bucket, prefix `tiles/ecmwf/` |
| **Basemap** | `/basemap/{provider_id}/{z}/{x}/{y}.png`, `/basemap/providers` | `BasemapService` + `BasemapTileReader` + `BasemapScraperService` | `basemap-tiles` bucket, prefix `basemap/{provider_id}/` |

Channel mapping: `ch-2` → `band_2`, `ch-9` → `band_9`, `ch-13` → `band_13`.

### Background sync / scrape

Four independent services, all started in the FastAPI `lifespan` and gated by file locks (`fcntl`) so only one Uvicorn worker runs each:

- **`SyncService`** (`services/sync_service.py`) — satellite tile sync from SeaweedFS. Runs in `sync_mode=full`. Strategies: `SatelliteFullSyncStrategy` / `SatelliteOnDemandStrategy`.
- **`RadarService` sync** — same pattern. Strategies: `RadarFullSyncStrategy` / `RadarOnDemandStrategy`.
- **`EcmwfService.start_sync`** — ECMWF precipitation forecast tiles. Strategies: `EcmwfFullSyncStrategy` / `EcmwfOnDemandStrategy`. Retention controlled by `ecmwf_forecasts_to_keep`.
- **`BasemapScraperService`** — periodic full-sweep scrape of external providers (IGN, ArcGIS, Google) writing to the `basemap-tiles` bucket + Redis. Resumable via SQLite cursor (`basemap_scrape_state_db_path`). Runs in `basemap_sync_mode ∈ {full, on_demand, no_cache}`; only `full` also writes Redis.

`S3Client` uses `aioboto3` with semaphore-limited concurrency (default 5, `s3_max_concurrent_downloads`). HTTP tile fetches use `HttpTileClient` (`httpx.AsyncClient`) with its own concurrency + retry budget.

### Basemap cache modes

Independent of `sync_mode`. Four values controlling the two orthogonal cache axes (Redis, S3):

| Mode | Scraper runs | Scraper writes S3 | Scraper writes Redis | Reader Redis | Reader S3 | Reader relay |
|---|---|---|---|---|---|---|
| `full` (default) | yes | yes | yes | yes | yes | yes |
| `on_demand` | yes | yes | **no** | yes | yes | yes |
| `no_cache` | yes | yes | no | **no** | yes | yes |
| `relay_only` | **no** | — | — | no | **no** | yes |

Derivation lives in `main.configure_basemap`. `relay_only` requires `basemap_online_fallback_enabled=true` (enforced by `Settings._validate`).

### Key Patterns

- Services are module-level singletons configured via `.configure(...)` inside `lifespan` (DI via method call, not constructor — see `BasemapService`).
- Blocking I/O offloaded via `asyncio.to_thread()`.
- Tiles served as `FileResponse` with `image/webp` (satellite/radar/ECMWF) or `image/png` (basemap) and aggressive cache headers. Missing basemap tiles return a cached transparent PNG (`routes.utils.TRANSPARENT_PNG_TILE`) with `basemap_cache_control_tile_miss`.
- Tests use `pytest-socket` — network disabled by default, only `127.0.0.1` allowed.

## Configuration

**Env vars** (`.env`, see `.env.example`):

| Variable | Purpose |
|---|---|
| `S3_TILES_DATA_ENDPOINT/ACCESS_KEY/SECRET_KEY` | S3/SeaweedFS connection |
| `S3_TILES_DATA_BUCKET_NAME` | Satellite/radar/ECMWF bucket (default `tiles-data`) |
| `S3_BASEMAP_BUCKET_NAME` | Basemap cold-backup bucket (default `basemap-tiles`) |
| `REDIS_URL` | Redis (default: `redis://localhost:6379/0`) |
| `SYNC_MODE` | `full` or `on_demand` — applies to satellite, radar, ECMWF (default: `full`) |
| `BASEMAP_SYNC_MODE` | `full` / `on_demand` / `no_cache` / `relay_only` (default: `full`) |
| `BASEMAP_ONLINE_FALLBACK_ENABLED` | Disable tier-3 provider relay when `false` (default: `true`) |
| `BASEMAP_{PROVIDER}_URL` | Per-provider URL template (URLs only — names/zooms/TMS defaults in `basemap_config.py`) |
| `WEB_CONCURRENCY` | Uvicorn worker count |
| `APP_ENV` | `development` = human logs; `production` = NewRelic formatter |

**Runtime tuning** — `settings.json` is merged with env vars (env wins). `src/settings.py` is the source of truth for defaults. Every key has a matching `UPPERCASE` env override. Per-domain JSON keys are grouped under a namespace object (`basemap`, `ecmwf`, `radar`); the loader flattens one level back to `<namespace>_<key>` so Python attrs and env vars stay flat (`basemap.tile_ttl` → `settings.basemap_tile_ttl` / `BASEMAP_TILE_TTL`). Groups:

- Shared: `sync_mode`, `tile_ttl`, `radar_tile_ttl`, `tileset_listing_ttl`, `s3_max_concurrent_downloads`, `cache_control_config`, `cache_control_tile`.
- Sync cadence: `sync_interval_seconds`, `radar_sync_interval_seconds`, `ecmwf_sync_interval_seconds`.
- ECMWF: `ecmwf_tile_ttl`, `ecmwf_forecasts_to_keep`.
- Basemap: `basemap_sync_mode`, `basemap_providers`, `basemap_tile_ttl`, `basemap_scrape_*` (incl. `basemap_scrape_parallelism_mode` — `sequential`/`per_origin`/`full` — and `basemap_scrape_per_host_concurrent`, a per-host request budget stacked under `basemap_scrape_concurrent`), `basemap_cache_*`, `basemap_bbox_*`, `basemap_http_*`, `basemap_reader_http_*`, `basemap_request_deadline_seconds`, `basemap_s3_object_ttl_days`, `basemap_online_fallback_enabled`, `basemap_provider_presence_ttl`, `basemap_negative_cache_*`, `basemap_scrape_state_db_path`, `basemap_cache_control_tile_miss`.

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
- Mock external services (S3, Redis, Weather.com) — never call them in unit tests.
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

- **test.yml** — Push/PR to non-main: Python 3.13.12, Poetry, pytest + coverage.
- **deploy.yml** — Push to main: tests → Coolify webhook deployment.