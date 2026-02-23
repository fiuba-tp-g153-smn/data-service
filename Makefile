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
	docker volume rm data-service_redis_dev_data || true
	docker volume rm data-service_redis_data || true
	docker volume rm data-service_data_service_data

precommit:
	pre-commit run --all-files
