.PHONY: run-dev

run-dev:
	docker build . -f Dockerfile.dev -t users-service && docker run -p 8080:8080 -v ./src:/app -v ./.env:/app/.env users-service

down:
	docker compose down

test:
	make down
	docker build . -f Dockerfile.run_test -t users-service && docker run -p 8080:8080 -v ./reports/:/app/reports users-service
