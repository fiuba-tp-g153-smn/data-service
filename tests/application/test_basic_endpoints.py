import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def init_env_vars(monkeypatch):
    # Automatically applied to all tests
    monkeypatch.setenv("LOG_LEVEL", "INFO")


@pytest.fixture
def app_client(init_env_vars):
    from main import app

    return TestClient(app)


def test_root_ok(app_client):
    response = app_client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "users-service"}


def test_health_check(app_client):
    response = app_client.get("/health")
    assert response.status_code == 200
