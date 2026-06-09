"""Tests for the APP_ROLE web/worker/all split (settings + lifespan gating)."""

import pytest

import main
from settings import Settings


def test_app_role_defaults_to_all(monkeypatch):
    monkeypatch.delenv("APP_ROLE", raising=False)
    assert Settings().app_role == "all"


@pytest.mark.parametrize("role", ["web", "worker", "all"])
def test_valid_app_roles_accepted(monkeypatch, role):
    monkeypatch.setenv("APP_ROLE", role)
    assert Settings().app_role == role


def test_invalid_app_role_raises(monkeypatch):
    monkeypatch.setenv("APP_ROLE", "frontend")
    with pytest.raises(ValueError, match="Invalid app_role"):
        Settings()


@pytest.mark.parametrize(
    "role,expected",
    [("web", False), ("worker", True), ("all", True)],
)
def test_runs_background_jobs_gating(monkeypatch, role, expected):
    """The web role must NOT run background jobs; worker/all must."""
    monkeypatch.setattr(main.settings, "app_role", role)
    assert main._runs_background_jobs() is expected
