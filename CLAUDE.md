# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Service is a FastAPI microservice (Python 3.13) that serves satellite imagery tiles, weather station data, and radar data. It syncs tile data from a Seaweedfs S3 bucket (populated by a separate `tiles-processor` service) to local storage and serves them via REST API. Built as a university TFI project (FIUBA).

## AI Collaboration Rules

When assisting with this repository, **always follow these rules**:

1. **Before writing any code**, describe your proposed approach and **wait for explicit approval**.
   - If requirements are ambiguous or underspecified, **ask clarifying questions first**.

2. If a task requires changes to **more than 3 files**, **stop** and break the work into
   smaller, clearly defined tasks before proceeding.

3. **After writing code**, explicitly list:
   - What could break as a result of the change
   - Which tests should be added or updated to cover those risks

## Commands

### Development

```bash
make up          # Docker dev (hot-reload, mounts ./src)
make local       # Native dev (requires `make install` first, runs uvicorn with --reload on :8080)
make install     # Install Poetry + all deps (including dev)
```

### Testing

```bash
make test        # Run tests in Docker (builds Dockerfile.run_test, outputs to ./reports/)

# Local test run:
poetry run pytest -m "not skip" --cov=src --cov-report=html:reports/coverage

# Single test:
poetry run pytest tests/application/test_basic_endpoints.py::test_root_ok
```

### Linting (pre-commit hooks)

```bash
black src/       # Formatter
pylint src/      # Static analysis
```

### Production

```bash
make prod        # Docker production build
```

## Architecture

### Entrypoint & Lifecycle

- `src/main.py` - FastAPI app with CORS middleware. Uses `lifespan` context manager to start/stop `SyncService` on app startup/shutdown. Uses `uvloop` as the event loop.
- `src/dependencies.py` - Module-level singletons for `settings` and `logger`, imported throughout the codebase.
- `src/settings.py` - Plain class (not Pydantic BaseSettings) that reads env vars via `os.getenv` + `python-dotenv`.

### Layered Structure

```
routes/       -> API endpoints (FastAPI routers)
services/     -> Business logic (singleton instances)
models/       -> Pydantic response models
clients/      -> External service clients (S3)
controller/   -> General endpoints (health, root)
```

### Three Data Domains

1. **Satellite** (`/products/{product_id}/{instrument_id}/{channel_id}/...`)
   - `SatelliteService` manages GOES-19 ABI satellite products. Hardcoded product/instrument/channel config.
   - Tiles stored at `data/tmp/band_{N}/tiles/{tileset_id}_tiles/{z}/{x}/{y}.webp`
   - Channel-to-directory mapping: `ch-2` -> `band_2`, `ch-9` -> `band_9`, `ch-13` -> `band_13`

2. **Radar** (`/products/radar/{radar_id}/{variable_id}/{elevation_id}/...`)
   - `RadarService` reads from `../output_radar/` (parent directory volume mount).
   - Directory structure: `output_radar/{radar_id}/{variable_id}/{timestamp}_elev{N}/tiles/{z}/{x}/{y}.webp`

3. **Weather** (`/weather/emas`)
   - `WeatherService` proxies Weather.com vector API (product 614) for weather station data.

### Background Sync (SyncService)

- `SyncService` is a singleton that runs as an asyncio background task.
- Uses file locking (`fcntl`) so only one worker syncs when running multiple uvicorn workers.
- Syncs S3 prefixes (`band_13/tiles`, `band_9/tiles`, `band_2/tiles`) to `data/tmp/`.
- Enforces retention policy: keeps only the latest 26 tilesets per band, deletes older ones from S3.
- `S3Client` (`clients/s3_client.py`) uses `aioboto3` with semaphore-limited concurrent downloads (max 20).

### Key Patterns

- Services are instantiated as module-level singletons (e.g., `satellite_service = SatelliteService()`).
- Blocking I/O (filesystem operations) is offloaded via `asyncio.to_thread()`.
- Tiles served as `FileResponse` with `image/webp` media type and aggressive cache headers.
- Tests use `pytest` with `pytest-socket` (network disabled by default, only `127.0.0.1` allowed).

## Configuration

Environment variables loaded from `.env` (see `.env.example`). Key vars:

- `S3_TILES_DATA_ENDPOINT`, `S3_TILES_DATA_ACCESS_KEY`, `S3_TILES_DATA_SECRET_KEY` - S3/Seaweedfs connection
- `SYNC_INTERVAL_SECONDS` - Background sync frequency (default 60s)
- `WEB_CONCURRENCY` - Uvicorn worker count
- `APP_ENV` - `development` uses human-readable logs; production uses NewRelic formatter

## CI/CD

- **test.yml**: Runs on push/PR to non-main branches. Python 3.13.8, Poetry, pytest with coverage.
- **deploy.yml**: Runs on push to main. Runs tests first, then triggers Coolify deployment via webhook.

## Engineering Standards

### SOLID Principles

- **Single Responsibility**: Each service class owns exactly one domain (satellite, radar, weather, sync). Routes only handle HTTP concerns (validation, status codes, response formatting) — never business logic. A route should call a service method and return the result; if you find yourself writing domain logic in a route, move it to the service.
- **Open/Closed**: Extend behavior through new classes and composition, not by modifying existing ones. When adding a new data domain (e.g., a new instrument or product type), create a new service that inherits from `BaseProductService` and register it — don't add conditionals to existing services. Configuration dicts (like `SATELLITE_PRODUCTS`) should be the extension point, not scattered if/else chains.
- **Liskov Substitution**: Subclasses of `BaseProductService` must honor the base contract. Any service registered via `register_product()` must work correctly when accessed through `get_all_products()` or `product_exists()`. Don't override base methods in ways that break callers' assumptions.
- **Interface Segregation**: Keep service interfaces focused. A service should expose only the methods its routes need. Don't add methods to `BaseProductService` that only one subclass uses — put them on the subclass instead. Clients (like `S3Client`) should not force consumers to depend on methods they don't use.
- **Dependency Inversion**: High-level services should depend on abstractions, not concrete implementations. When adding new external clients, accept them via constructor injection (as `SyncService` does with `S3Client`) rather than hard-importing and instantiating them internally. This makes testing and swapping implementations straightforward.

### FastAPI Best Practices

- **Use `Depends()` for shared logic**: Inject settings, logger, database sessions, or auth via FastAPI's dependency injection instead of importing module-level singletons directly. This makes routes testable and decouples them from global state. Prefer `Depends(get_settings)` over `from dependencies import settings`.
- **Type all endpoints fully**: Every route must declare `response_model`, proper status codes, and use Pydantic models for both request validation and response serialization. Never return raw dicts from routes when a Pydantic model exists — it bypasses validation and documentation.
- **Use `APIRouter` with clear prefixes and tags**: Group related endpoints on a single router with a shared `prefix` and `tags`. Never register unrelated endpoints on the same router.
- **Async discipline**: All route handlers should be `async def`. Never call blocking I/O (filesystem, subprocess, CPU-heavy work) directly — always wrap with `asyncio.to_thread()`. Forgetting this blocks the entire event loop and kills throughput for all concurrent requests.
- **HTTPException for control flow**: Raise `HTTPException` with precise status codes (400, 404, 409, 422) at the route level. Services should return `None` or raise domain-specific exceptions — never HTTP exceptions, since services shouldn't know about HTTP.
- **Pydantic models in `models/`**: All request bodies, query parameter groups, and response shapes must be Pydantic models defined in `models/`. Keep models close to their domain (e.g., `models/satellite.py` for satellite responses). Reuse shared base models from `models/base.py`.
- **Lifespan for startup/shutdown**: Use the `lifespan` context manager pattern (already in `main.py`) for any resource that needs initialization or cleanup — database connections, background tasks, connection pools. Never use the deprecated `@app.on_event` decorators.

### Designing for Change and Extensibility

- **New data domains**: To add a new product type, create `services/{domain}_service.py` inheriting `BaseProductService`, define Pydantic models in `models/{domain}.py`, add routes in `routes/{domain}.py`, and include the router in `main.py`. Follow the existing satellite/radar pattern exactly.
- **New external clients**: Add to `clients/` with the same async pattern as `S3Client`. Accept connection params via constructor, use `aioboto3` or `httpx.AsyncClient` depending on protocol. Keep client methods focused on I/O — no business logic.
- **Configuration over hardcoding**: When behavior varies by environment or deployment, add it to `Settings` with a sensible default. Don't scatter `os.getenv()` calls across the codebase — centralize in `settings.py`.
- **Small, focused functions**: Each function should do one thing. If a method has multiple levels of indentation or mixes concerns (e.g., validation + I/O + transformation), split it. Aim for functions that are easy to read top-to-bottom without scrolling.
- **Composition over inheritance**: Use inheritance sparingly (only `BaseProductService` and its children). For cross-cutting concerns (caching, logging, retry logic), prefer decorators, middleware, or dependency injection. Don't build deep inheritance hierarchies.
- **Fail fast, fail loud**: Validate inputs at system boundaries (routes, client responses). Don't silently swallow errors deep in services — log them and propagate. Use early returns to handle error cases before the happy path.
- **Keep tests close to the code they verify**: Test files mirror the source structure (`tests/application/`, etc.). Every new service or route should have corresponding tests. Tests must not require network access (enforced by `pytest-socket`).
