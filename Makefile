# Makefile for managing the Users Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: run-dev test

install:
	pip install poetry
	poetry install

dev:
# Build the development Docker image and run the service in development mode.
# - Uses Dockerfile.dev for a lightweight setup without copying source code (mounted as volume).
# - Mounts ./src to /app for live code reloading during development.
# - Mounts .env file for environment variables.
# - Exposes port 8080 for access at http://localhost:8080
	docker build . -f Dockerfile.dev -t users-service && docker run -p 8080:8080 -v ./src:/app -v ./.env:/app/.env users-service

local:
	cd ./src && uvicorn main:app --host 0.0.0.0 --port 8080 --reload

test:
# Build the test Docker image and run the tests.
# - Uses Dockerfile.run_test which installs dev dependencies and copies the full project.
# - Runs pytest with coverage reporting and JUnit XML output.
# - Mounts ./reports to /app/reports to persist test reports and coverage HTML.
# - Port 8080 is exposed but primarily used for any test server needs; reports are generated in ./reports.
	docker build . -f Dockerfile.run_test -t users-service && docker run -p 8080:8080 -v ./reports/:/app/reports users-service
