"""Tests for main's best-effort shutdown (BUG-23).

A single teardown ``close()`` that raises must not skip the remaining steps —
otherwise the other connections leak and SQLite is left unclosed.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

import main


@pytest.mark.asyncio
async def test_safe_shutdown_swallows_and_logs(monkeypatch):
    """_safe_shutdown never propagates; it logs the failing step and returns."""
    monkeypatch.setattr(main, "logger", MagicMock())

    async def _boom():
        raise RuntimeError("close failed")

    await main._safe_shutdown(_boom(), "thing")  # must not raise

    main.logger.warning.assert_called_once()
    assert "thing" in main.logger.warning.call_args.args[1]


@pytest.mark.asyncio
async def test_shutdown_weather_stations_continues_past_a_failure(monkeypatch):
    """A failing close in the middle of teardown still lets the later closes run."""
    monkeypatch.setattr(main, "logger", MagicMock())

    runtime = MagicMock()
    runtime.scraper.stop = AsyncMock()
    runtime.smn_client.close = AsyncMock(side_effect=RuntimeError("smn down"))
    runtime.registry_client.close = AsyncMock()
    runtime.s3_client.close = AsyncMock()
    runtime.keystore.close = AsyncMock()
    runtime.api_keys_s3_client.close = AsyncMock()

    await main.shutdown_weather_stations(runtime)  # must not raise

    # Everything after the failing smn_client close still ran.
    runtime.registry_client.close.assert_awaited_once()
    runtime.s3_client.close.assert_awaited_once()
    runtime.keystore.close.assert_awaited_once()
    runtime.api_keys_s3_client.close.assert_awaited_once()
    main.logger.warning.assert_called()  # the failure was logged
