"""Tests for the APP_ROLE web/worker/all split (settings + lifespan gating)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import dependencies
import main
from settings import Settings


class _FakeS3Client:
    """Stand-in for S3Client so configure_basemap doesn't touch aioboto3/network."""

    def __init__(self, **_kwargs):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass


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


@pytest.mark.asyncio
async def test_web_role_opens_state_store_for_reads(tmp_path, monkeypatch):
    """Regression: the web role must open (and register) the scrape-state SQLite
    so /metrics/basemap/providers reflects the worker's progress — even though
    the web role never runs the scraper itself.

    Before the fix the state store was built only inside `if run_scraper:`, so
    the web container's dashboard endpoint read a None store and reported every
    provider as 'pendiente' despite the worker scraping all night.
    """
    monkeypatch.setattr(main.settings, "app_role", "web")
    monkeypatch.setattr(main.settings, "basemap_sync_mode", "full")
    monkeypatch.setattr(
        main.settings,
        "basemap_scrape_state_db_path",
        str(tmp_path / "basemap_scraper_state.sqlite"),
    )
    monkeypatch.setattr(
        main,
        "load_providers",
        lambda _cfg: {
            "argenmap": SimpleNamespace(
                provider_id="argenmap",
                name="Argenmap",
                min_zoom=3,
                cache_max_zoom=11,
            )
        },
    )
    monkeypatch.setattr(main, "S3Client", _FakeS3Client)
    # Start from a clean singleton; monkeypatch restores it after the test.
    monkeypatch.setattr(dependencies, "_basemap_state_store", None, raising=False)

    runtime = await main.configure_basemap(MagicMock())
    try:
        assert runtime is not None
        # Web role: the scraper must NOT run...
        assert runtime.scraper is None
        # ...but the read-side state store IS opened and registered for the
        # /metrics/basemap/providers dependency.
        assert runtime.state_store is not None
        assert dependencies.get_basemap_state_store() is runtime.state_store
    finally:
        await main.shutdown_basemap(runtime)
