# Data Service - SMN

<img src="https://uptime.mapasmn.com/api/badge/5/status?style=flat-square" /> <img src="https://uptime.mapasmn.com/api/badge/5/uptime?style=flat-square" /> <img src="https://uptime.mapasmn.com/api/badge/5/ping?style=flat-square" />

The Data Service is a Python-based microservice built with FastAPI for managing data operations. It handles data CRUD and related functionalities in a RESTful API.

### Team members

| Name                        | Padrón | Email                 |
| --------------------------- | ------ | --------------------- |
| Altamirano, Agustín Gabriel | 110237 | aaltamirano@fi.uba.ar |
| Diem, Walter Gabriel        | 105618 | wdiem@fi.uba.ar       |
| Gismondi, Máximo            | 110119 | magismondi@fi.uba.ar  |
| Valeriani, Matías Gabriel   | 108570 | mvaleriani@fi.uba.ar  |

### Table of Contents

1. [Dependencies](#dependencies)
1. [Setup for development](#Setup-for-development)
1. [MinIO S3 Integration](#minio-s3-integration)
1. [Makefile Commands](#Makefile-Commands)
1. [Running Tests](#Running-Tests)
1. [Dockerfiles](#Dockerfiles)
1. [Environment Variables](#environment-variables)
1. [API Documentation](#API-Documentation)

## Dependencies

You don't really need to have `python` installed to run the project given the project is Dockerized. `python` is required to be installed on the local machine only for certain tasks (e.g. running tests or booting the app natively).

You need to have the next dependencies installed:

- **Docker**: to run the project in a containerized version, like what it will be in the production environment.
- **Make**: to simplify and automate the commands to run.
- **Python v3.13+**: if you decide to run the app natively and not with Docker.

## Setup for development

1. Clone the repository to your local machine.

2. Copy the example environment file:  
   `cp .env.example .env`  
   Edit `.env` to configure your environment variables (e.g., database connections, secrets).

3. For local development:

   With Docker:

   - Run `make dev`.

     The app will be available at http://localhost:8080.

   Without Docker:

   - Create a virtual environment running the following command: `python -m venv .venv`
   - Activate the virtual environment with: `source .venv/bin/activate`
   - Run `make install`
   - Run `make local`

     The app will be available at http://localhost:8080.

## MinIO S3 Integration

The Data Service syncs tile data from a MinIO S3 bucket, typically populated by the `tiles-processor` service. This decouples tile generation from tile serving.

### How It Works

1. **Background Sync Service**: On startup, data-service starts a background task that periodically syncs tiles from MinIO to local storage.
2. **Sync Interval**: Configurable via `SYNC_INTERVAL_SECONDS` (default: 60 seconds).
3. **Incremental Sync**: Only downloads new or changed files, deletes local files removed from S3.
4. **Graceful Handling**: If MinIO is not configured or unavailable, the service continues without sync (uses existing local tiles).

### S3 Bucket Structure

The service expects tiles in the following structure:

```
tiles-data/                              # Bucket name
├── band_13/
│   └── tiles/
│       └── {tileset_id}_tiles/
│           └── {z}/{x}/{y}.webp
└── band_9/
    └── tiles/
        └── {tileset_id}_tiles/
            └── {z}/{x}/{y}.webp
```

### Connecting to tiles-processor MinIO

When running both services separately:

1. **Start tiles-processor** (includes MinIO):
   ```bash
   cd ../tiles-processor
   docker compose up -d
   ```
   MinIO will be available at `localhost:9000` (S3 API) and `localhost:9001` (Console).

2. **Configure data-service** to connect:
   ```bash
   # In data-service/.env
   MINIO_ENDPOINT=host.docker.internal:9000
   MINIO_ACCESS_KEY=minioadmin
   MINIO_SECRET_KEY=minioadmin
   MINIO_BUCKET=tiles-data
   ```

3. **Start data-service**:
   ```bash
   docker compose up -d
   ```

The data-service will sync tiles from tiles-processor's MinIO and serve them via REST API.

### Architecture

```
┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐
│   tiles-processor   │ upload  │        MinIO        │  sync   │    data-service     │
│                     │ ──────> │    (S3 Bucket)      │ <────── │                     │
│ Generates tiles     │         │ Port 9000 (S3 API)  │         │ Serves tiles via    │
│ from GOES-19        │         │ Port 9001 (Console) │         │ REST API            │
└─────────────────────┘         └─────────────────────┘         └─────────────────────┘
```

## Makefile Commands

The `Makefile` provides convenient targets for common tasks. Run them from the project root:

- `make dev`:  
  Builds the development image (`Dockerfile.dev`) and runs the container.

  - Mounts `./src` for live reloading.
  - Mounts `.env` for configuration.
  - Exposes the app at http://localhost:8080.  
    Stop with Ctrl+C or `docker stop <container_id>`.

- `make test`:
  Builds the test image (`Dockerfile.run_test`) and runs all tests.

  - Mounts `./reports/` to persist outputs.
  - Exposes port 8080 (for any test servers).
    Test results and coverage reports are saved in `./reports/`.

- `make local`:
  Runs the application locally using Uvicorn.
  - Requires Python and dependencies installed (via `make install`).
  - Enables auto-reload for development.
  - Exposes the app at http://localhost:8080.
    Stop with Ctrl+C.

## Running Tests

Tests use pytest and are located in the `tests/` directory.

- **With Docker (Recommended)**:  
  `make test`  
  This runs:

  - The tests with `pytest`.
  - Skips tests marked `@pytest.mark.skip`.
  - Generates:
    - Terminal coverage summary.
    - JUnit XML report: `reports/junit_report.xml` (for CI integration).
    - HTML coverage report: Open `reports/coverage/index.html` in a browser.

- **Locally (with Poetry)**:  
  `poetry run pytest -m "not skip" --cov=src --cov-report=html:reports/coverage`  
  Ensure `make install` has been run to install dev dependencies.

## Dockerfiles

The project includes three Dockerfiles for different environments:

- **Dockerfile** (Production):  
  Builds a production image based on Python 3.13 slim.

  - Installs only runtime dependencies (skips dev/test deps).
  - Copies the source code (`./src`) into the container.
  - Runs the app with Uvicorn on port 8080.  
    Use this for **deployment**.

- **Dockerfile.dev** (Development):  
  Similar to production but optimized for development.

  - Does not copy source code (mount `./src` as a volume for live code changes and hot-reloading).
  - Enables Uvicorn's `--reload` flag for automatic restarts on code changes.
  - Mount `.env` for environment variables.  
    Ideal for local **development** workflows.

- **Dockerfile.run_test** (Testing):  
  Builds an image for running tests.
  - Installs all dependencies, including dev/test ones.
  - Copies the entire project (`.`).
  - Runs `pytest` with coverage reporting, JUnit XML output, and ignores deprecation warnings.
  - Generates reports in `/app/reports` (mounted to `./reports` on host).  
    Use this to execute **tests in an isolated environment** in a production-like environment.

All images use Python 3.13.8-slim-trixie as the base for minimal size.

## Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `LOG_LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR). | `INFO` |
| `APP_ENV` | Application environment (development, production). | `production` |
| `APP_HOST_PORT` | Host port for the API service. | `6006` |
| `MINIO_ENDPOINT` | MinIO S3 endpoint (host:port). Use `host.docker.internal:9000` for local. | Required for sync |
| `MINIO_ACCESS_KEY` | MinIO access key (username). | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key (password). | `minioadmin` |
| `MINIO_BUCKET` | S3 bucket name for tile storage. | `tiles-data` |
| `MINIO_SECURE` | Use HTTPS for MinIO connection (`true`/`false`). | `false` |
| `SYNC_INTERVAL_SECONDS` | Interval between sync operations (seconds). | `60` |

## API Documentation

- **Swagger UI**: http://localhost:8080/docs (when the app is running)  
  Explore endpoints, try requests, and view schemas interactively.

In production, replace `localhost:8080` with your deployed URL (e.g., https://api.example.com/docs).
