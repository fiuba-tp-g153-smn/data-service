# Based on slim Debian 13 Trixie
FROM python:3.13.8-slim-trixie

# Set working directory inside the container
WORKDIR /app

# Copy only dependency manifests first (to leverage Docker build cache for faster builds)
COPY pyproject.toml poetry.lock /app/

# Use python implementation of protobuf instead of binary
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# Install Poetry (dependency manager) globally, disable venvs to install into system site-packages
RUN pip install "poetry" && poetry config virtualenvs.create false

# Re-generate lock file if it is outdated, then install all dependencies (except dev/test deps)
# "--without dev": keep container smaller by skipping development deps
# "--no-root": don’t install this project as a package itself, we run code mounted in /app
RUN (poetry check --lock || poetry lock --no-update) && poetry install --without dev --no-root

# Copy the actual application code into /app
COPY ./src /app

# Expose port 8080 (FastAPI main.py sets default to 8080)
EXPOSE 8080

# Run the app with uvicorn
# - "main:app" : entrypoint -> file main.py, ASGI (Asynchronous Server Gateway Interface) app instance "app"
# - host=0.0.0.0 : bind to all network interfaces (needed in containers)
# - port=8080 : matches EXPOSE above
CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--port", "8080"]
