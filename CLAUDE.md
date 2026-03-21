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

FastAPI microservice (Python 3.13) serving satellite tiles, radar data, and weather station info. Syncs tiles from S3/SeaweedFS to local storage via background task.

### Entrypoint & Lifecycle

- `src/main.py` — FastAPI app, CORS middleware, `lifespan` context manager for `SyncService` start/stop, `uvloop` event loop.
- `src/dependencies.py` — Module-level singletons (`settings`, `logger`, `redis_client`).
- `src/settings.py` — Plain class reading env vars via `os.getenv` + `python-dotenv`, merged with `settings.json`.

### Layered Structure

```
routes/       → API endpoints (FastAPI routers)
services/     → Business logic (singleton instances)
models/       → Pydantic response models
clients/      → External service clients (S3, Redis)
controller/   → General endpoints (health, root)
```

### Data Domains

| Domain | Route prefix | Service | Storage |
|---|---|---|---|
| **Satellite** | `/products/{product_id}/{instrument_id}/{channel_id}/...` | `SatelliteService` (GOES-19 ABI) | `data/tmp/band_{N}/tiles/{tileset_id}_tiles/{z}/{x}/{y}.webp` |
| **Radar** | `/products/radar/{radar_id}/{variable_id}/{elevation_id}/...` | `RadarService` | `../output_radar/{radar_id}/{variable_id}/{timestamp}_elev{N}/tiles/{z}/{x}/{y}.webp` |

Channel mapping: `ch-2` → `band_2`, `ch-9` → `band_9`, `ch-13` → `band_13`.

### Background Sync (SyncService)

- Singleton asyncio task started in `lifespan`.
- File locking (`fcntl`) ensures only one Uvicorn worker syncs.
- Syncs S3 prefixes (`band_13/tiles`, `band_9/tiles`, `band_2/tiles`) to `data/tmp/`.
- Retention: keeps latest 26 tilesets per band, deletes older.
- `S3Client` uses `aioboto3` with semaphore-limited concurrency (default 5, configurable via `s3_max_concurrent_downloads`).
- Two strategies: `SatelliteFullSyncStrategy` (background) and `SatelliteOnDemandStrategy` (lazy fetch). Controlled by `sync_mode` in `settings.json`.

### Key Patterns

- Services are module-level singletons.
- Blocking I/O offloaded via `asyncio.to_thread()`.
- Tiles served as `FileResponse` with `image/webp` and aggressive cache headers.
- Tests use `pytest-socket` — network disabled by default, only `127.0.0.1` allowed.

## Configuration

**Env vars** (`.env`, see `.env.example`):

| Variable | Purpose |
|---|---|
| `S3_TILES_DATA_ENDPOINT/ACCESS_KEY/SECRET_KEY` | S3/SeaweedFS connection |
| `REDIS_URL` | Redis (default: `redis://localhost:6379/0`) |
| `SYNC_MODE` | `full` or `on_demand` (default: `full`) |
| `WEB_CONCURRENCY` | Uvicorn worker count |
| `APP_ENV` | `development` = human logs; `production` = NewRelic formatter |

**Runtime tuning** (`settings.json`, overrides env vars): `sync_interval_seconds`, `radar_sync_interval_seconds`, `tile_ttl`, `tileset_listing_ttl`, `s3_max_concurrent_downloads` (default 5), `cache_control_tile`, `cache_control_config`.

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