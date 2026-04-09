################################
# Stage 1: Builder
################################
FROM python:3.13.12-slim-trixie AS builder

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libexpat1 \
	&& rm -rf /var/lib/apt/lists/*

# Copy only dependency manifests first (to leverage Docker build cache for faster builds)
COPY pyproject.toml poetry.lock /app/

# Install Poetry, disable venvs to install into system site-packages
RUN pip install --no-cache-dir "poetry==2.3.2" && poetry config virtualenvs.create false

# Re-generate lock file if it is outdated, then install all dependencies (except dev/test deps)
RUN (poetry check --lock || poetry lock) && poetry install --without dev --no-root --no-ansi --no-cache

################################
# Stage 2: Runtime
################################
FROM python:3.13.12-slim-trixie AS runner

WORKDIR /app

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libexpat1 \
	&& rm -rf /var/lib/apt/lists/*

# Use python implementation of protobuf instead of binary
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# Copy installed Python packages from builder
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the actual application code into /app
COPY ./src /app
COPY ./settings.json /settings.json

EXPOSE 8080

ARG WEB_CONCURRENCY
ENV WEB_CONCURRENCY=${WEB_CONCURRENCY}

# Run the app with uvicorn
# - "main:app" : entrypoint -> file main.py, ASGI app instance "app"
# - host=0.0.0.0 : bind to all network interfaces (needed in containers)
# - port=8080 : matches EXPOSE above
# - workers: use WEB_CONCURRENCY env var
CMD ["sh", "-c", "exec uvicorn main:app --host=0.0.0.0 --port=8080 --workers=${WEB_CONCURRENCY}"]

HEALTHCHECK --interval=10s --timeout=10s --retries=5 CMD python -c 'import urllib.request; urllib.request.urlopen("http://localhost:8080/health")'
