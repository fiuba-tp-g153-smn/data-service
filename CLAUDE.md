# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Service is a FastAPI microservice (Python 3.13) that serves satellite imagery tiles, radar data, and weather station information. It syncs tile data from an S3/SeaweedFS bucket (populated by a separate `tiles-processor` service) to local storage and serves them via REST API. Built as a university TFI project (FIUBA).

## AI Collaboration Rules

When assisting with this repository, **always follow these rules**:

1. **Before writing any code**, describe your proposed approach and **wait for explicit approval**.
   - If requirements are ambiguous or underspecified, **ask clarifying questions first**.

2. If a task requires changes to **more than 3 files**, **stop** and break the work into smaller, clearly defined tasks before proceeding.

3. **After writing code**, explicitly list:
   - What could break as a result of the change
   - Which tests should be added or updated to cover those risks

## Commands

### Development

```bash
make install     # Install Poetry + all deps (including dev)
make up          # Docker dev (hot-reload, mounts ./src)
make local       # Native dev (requires `make install` first, runs uvicorn with --reload on :8080)
```

### Testing

```bash
make test        # Run tests in Docker (builds Dockerfile.run_test, outputs to ./reports/)

# Local test run:
poetry run pytest -m "not skip" --cov=src --cov-report=html:reports/coverage

# Single test:
poetry run pytest tests/application/test_basic_endpoints.py::test_root_ok
```

### Linting

```bash
make precommit   # Run pre-commit hooks (black, pylint, mypy)
black src/       # Formatter only
pylint src/      # Static analysis only
```

### Production

```bash
make prod        # Docker production build
```

Bare commands require `source .venv/bin/activate && cmd`.

## Architecture

### Entrypoint & Lifecycle

- `src/main.py` — FastAPI app with CORS middleware. Uses `lifespan` context manager to start/stop `SyncService` on startup/shutdown. Uses `uvloop` as the event loop.
- `src/dependencies.py` — Module-level singletons (`settings`, `logger`, `redis_client`) imported throughout.
- `src/settings.py` — Plain class (not Pydantic BaseSettings) that reads env vars via `os.getenv` + `python-dotenv`, and merges with `settings.json`.

### Layered Structure

```
routes/       -> API endpoints (FastAPI routers)
services/     -> Business logic (singleton instances)
models/       -> Pydantic response models
clients/      -> External service clients (S3, Redis)
controller/   -> General endpoints (health, root)
```

### Three Data Domains

1. **Satellite** (`/products/{product_id}/{instrument_id}/{channel_id}/...`)
   - `SatelliteService` manages GOES-19 ABI satellite products.
   - Tiles stored at `data/tmp/band_{N}/tiles/{tileset_id}_tiles/{z}/{x}/{y}.webp`
   - Channel mapping: `ch-2` → `band_2`, `ch-9` → `band_9`, `ch-13` → `band_13`

2. **Radar** (`/products/radar/{radar_id}/{variable_id}/{elevation_id}/...`)
   - `RadarService` reads from `../output_radar/` (parent directory volume mount).
   - Structure: `output_radar/{radar_id}/{variable_id}/{timestamp}_elev{N}/tiles/{z}/{x}/{y}.webp`

3. **Weather** (`/weather/emas`)
   - `WeatherService` proxies Weather.com vector API (product 614) for weather station data.

### Background Sync (SyncService)

- Singleton asyncio background task started in `lifespan`.
- Uses file locking (`fcntl`) so only one Uvicorn worker syncs when running multiple workers.
- Syncs S3 prefixes (`band_13/tiles`, `band_9/tiles`, `band_2/tiles`) to `data/tmp/`.
- Retention policy: keeps only the latest 26 tilesets per band, deletes older ones.
- `S3Client` (`clients/s3_client.py`) uses `aioboto3` with semaphore-limited concurrent downloads (default: 5, configurable via `s3_max_concurrent_downloads` in `settings.json`).
- Two sync strategies: `SatelliteFullSyncStrategy` (background) and `SatelliteOnDemandStrategy` (lazy fetch). Controlled by `sync_mode` in `settings.json`.

### Key Patterns

- Services are module-level singletons (e.g., `satellite_service = SatelliteService()`).
- Blocking I/O is offloaded via `asyncio.to_thread()`.
- Tiles served as `FileResponse` with `image/webp` media type and aggressive cache headers.
- Tests use `pytest-socket` — network disabled by default, only `127.0.0.1` allowed.

## Configuration

Environment variables from `.env` (see `.env.example`). Key vars:

- `S3_TILES_DATA_ENDPOINT`, `S3_TILES_DATA_ACCESS_KEY`, `S3_TILES_DATA_SECRET_KEY` — S3/SeaweedFS connection
- `REDIS_URL` — Redis connection (default: `redis://localhost:6379/0`)
- `SYNC_MODE` — `full` or `on_demand` (default: `full`)
- `WEB_CONCURRENCY` — Uvicorn worker count
- `APP_ENV` — `development` uses human-readable logs; `production` uses NewRelic formatter

Runtime tuning in `settings.json` (overrides env vars):

- `sync_interval_seconds`, `radar_sync_interval_seconds` — Background sync frequencies
- `tile_ttl`, `tileset_listing_ttl` — Redis TTLs
- `s3_max_concurrent_downloads` — S3 download concurrency cap (default: 5)
- `cache_control_tile`, `cache_control_config` — Cache-Control headers

## CI/CD

- **test.yml** — Runs on push/PR to non-main branches. Python 3.13.12, Poetry, pytest with coverage.
- **deploy.yml** — Runs on push to main. Runs tests first, then triggers Coolify deployment via webhook.

## Engineering Standards

### SOLID Principles

- **Single Responsibility**: Each service owns one domain. Routes handle only HTTP concerns — never business logic. If writing domain logic in a route, move it to the service.
- **Open/Closed**: Add new data domains by creating a new service inheriting `BaseProductService` and registering it. Avoid adding conditionals to existing services.
- **Liskov Substitution**: `BaseProductService` subclasses must honor the base contract.
- **Dependency Inversion**: Accept external clients via constructor injection (as `SyncService` does with `S3Client`). Don't hard-import and instantiate them internally.

### FastAPI Best Practices

- **`Depends()` for shared logic**: Inject settings, logger, and auth via FastAPI DI. Prefer `Depends(get_settings)` over importing module-level singletons in routes.
- **Type all endpoints**: Declare `response_model`, status codes, and use Pydantic models. Never return raw dicts when a model exists.
- **Async discipline**: All route handlers must be `async def`. Wrap blocking I/O with `asyncio.to_thread()`.
- **HTTPException at the route level**: Services return `None` or raise domain exceptions — never `HTTPException`. Routes translate to HTTP status codes.
- **Lifespan for startup/shutdown**: Use the `lifespan` pattern in `main.py`. Never use deprecated `@app.on_event`.

### Extending the Codebase

- **New data domain**: Create `services/{domain}_service.py` (inherit `BaseProductService`), `models/{domain}.py`, `routes/{domain}.py`, and include the router in `main.py`.
- **New external client**: Add to `clients/` following `S3Client`'s async pattern. Accept connection params via constructor; no business logic in clients.
- **New config**: Add to `Settings` with a sensible default. Don't scatter `os.getenv()` calls — centralize in `settings.py`.
