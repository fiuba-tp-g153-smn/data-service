# Makefile for managing the Data Service application

# Declare phony targets to avoid conflicts with files of the same name
.PHONY: up prod dev test local down

install:
	pip install poetry
	poetry install

up:
	docker compose -f docker-compose-dev.yaml up --build

prod:
	docker compose up --build

local:
	cd ./src && uvicorn main:app --host 0.0.0.0 --port 8080 --reload

test:
# Build the test Docker image and run the tests.
# - Uses Dockerfile.run_test which installs dev dependencies and copies the full project.
# - Runs pytest with coverage reporting and JUnit XML output.
# - Mounts ./reports to /app/reports to persist test reports and coverage HTML.
# - Port 8080 is exposed but primarily used for any test server needs; reports are generated in ./reports.
	docker build . -f Dockerfile.run_test -t data-service && docker run -p 8080:8080 -v ./reports/:/app/reports data-service

down:
	docker compose down
	docker compose -f docker-compose-dev.yaml down --remove-orphans

clean:
	# Stop both stacks and drop their named volumes (redis + dataservice_data).
	docker compose -f docker-compose-dev.yaml down -v --remove-orphans || true
	docker compose down -v --remove-orphans || true
	# Drop the orphaned pre-rename data volume too (old "data" volume), best-effort.
	docker volume rm data-service_data data-service_dataservice_data data-service_redis_dev_data data-service_redis_data 2>/dev/null || true
	# Dev /app/data is a host BIND mount (./data), not a volume — `docker volume rm`
	# never touches it. Wipe its contents (metrics + basemap scrape state +
	# keystore SQLite) via a throwaway container so root-owned files are removed.
	docker run --rm -v "$$(pwd)/data:/data" alpine sh -c 'rm -rf /data/* /data/.[!.]* 2>/dev/null' || true

precommit:
	pre-commit run --all-files
