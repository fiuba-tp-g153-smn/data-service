# Users Service - SMN

## Overview

The Users Service is a Python-based microservice built with FastAPI for managing user operations. It handles user creation, authentication, and related functionalities in a RESTful API.

This service is part of a larger system and uses Poetry for dependency management and Docker for containerization.

## Prerequisites

- Docker (recommended for setup and running)
- Poetry (for local dependency management, optional with Docker)
- Python 3.13 (if running locally without Docker)

## Setup

1. Clone the repository to your local machine.

2. Copy the example environment file:  
   `cp .env.example .env`  
   Edit `.env` to configure your environment variables (e.g., database connections, secrets).

3. For local development without Docker:  
   - Install Poetry: `pip install poetry`  
   - Install dependencies: `poetry install` (includes dev dependencies)  
   - Run the application:  
     `poetry run uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload`  
     The app will be available at http://localhost:8080.

4. For Docker-based setup, use the provided Makefile commands (see below). This is the recommended approach for consistency.

Ensure the `./reports/` directory exists for test outputs (create it if needed: `mkdir -p reports`).

## Dockerfiles

The project includes three Dockerfiles for different environments:

- **Dockerfile** (Production):  
  Builds a lightweight production image based on Python 3.13 slim.  
  - Installs only runtime dependencies (skips dev/test deps).  
  - Copies the source code (`./src`) into the container.  
  - Runs the app with Uvicorn on port 8080.  
  Use this for deployment.

- **Dockerfile.dev** (Development):  
  Similar to production but optimized for development.  
  - Does not copy source code (mount `./src` as a volume for live code changes and hot-reloading).  
  - Enables Uvicorn's `--reload` flag for automatic restarts on code changes.  
  - Mount `.env` for environment variables.  
  Ideal for local development workflows.

- **Dockerfile.run_test** (Testing):  
  Builds an image for running tests.  
  - Installs all dependencies, including dev/test ones.  
  - Copies the entire project (`.`).  
  - Runs `pytest` with coverage reporting, JUnit XML output, and ignores deprecation warnings.  
  - Generates reports in `/app/reports` (mounted to `./reports` on host).  
  Use this to execute tests in an isolated environment.

All images use Python 3.13.8-slim-trixie as the base for minimal size and security.

## Makefile Commands

The `Makefile` provides convenient targets for common tasks. Run them from the project root:

- `make run-dev`:  
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

View the full `Makefile` for detailed comments on each command.

## Running Tests

Tests use pytest and are located in the `tests/` directory. They cover basic endpoints and application logic.

- **With Docker (Recommended)**:  
  `make test`  
  This runs:  
  - `pytest -m "not skip" --color=yes --junitxml=reports/junit_report.xml --cov=src --cov-report=term --cov-report=html:reports/coverage -W ignore::DeprecationWarning`  
  - Skips tests marked `@pytest.mark.skip`.  
  - Generates:  
    - Terminal coverage summary.  
    - JUnit XML report: `reports/junit_report.xml` (for CI integration).  
    - HTML coverage report: Open `reports/coverage/index.html` in a browser.

- **Locally (with Poetry)**:  
  `poetry run pytest -m "not skip" --cov=src --cov-report=html:reports/coverage`  
  Ensure `poetry install` has been run to install dev dependencies.

If tests fail, check the output for errors and ensure your `.env` is properly configured.

## API Documentation

FastAPI provides automatic interactive API documentation via Swagger UI and ReDoc.

- **Swagger UI**: http://localhost:8080/docs (when the app is running)  
  Explore endpoints, try requests, and view schemas interactively.

- **ReDoc**: http://localhost:8080/redoc  

In production, replace `localhost:8080` with your deployed URL (e.g., https://api.example.com/docs).

## Additional Notes

- The service listens on port 8080 by default.  
- For production deployment, consider using a reverse proxy (e.g., Nginx) and orchestrating with Docker Compose or Kubernetes.  
- Environment variables are loaded from `.env` (see `src/settings.py` for configuration).  
- Protobuf is used with Python implementation (no binary deps).  

For issues or contributions, refer to the project's GitHub repository.
