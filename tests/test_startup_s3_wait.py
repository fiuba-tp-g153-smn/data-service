"""Tests for ``main._wait_for_s3_reachable()`` — the startup wait-for-S3 gate.

The gate blocks (retrying with capped exponential backoff) until S3 reports
reachable, then returns so the rest of startup proceeds. This is what lets dev
(``uvicorn --reload``, whose reloader does not respawn a child that died) recover
automatically when S3 comes back, instead of staying wedged after a startup
crash. The probe S3Client is always cleaned up.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

import main


@pytest.mark.asyncio
async def test_wait_for_s3_reachable_returns_once_reachable(monkeypatch):
    """is_reachable False twice then True -> gate retries with backoff, then
    returns after S3 recovers, and closes the probe."""
    probe = MagicMock()
    probe.connect = AsyncMock()
    probe.close = AsyncMock()
    probe.is_reachable = AsyncMock(side_effect=[False, False, True])
    monkeypatch.setattr(main, "S3Client", MagicMock(return_value=probe))

    sleep_calls: list[float] = []

    async def _fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(main.asyncio, "sleep", _fake_sleep)

    await main._wait_for_s3_reachable()

    assert probe.is_reachable.await_count == 3
    assert probe.close.await_count == 1  # probe cleaned up even after retries
    assert sleep_calls == [1.0, 2.0]  # exponential backoff between the two misses


@pytest.mark.asyncio
async def test_wait_for_s3_reachable_no_wait_when_immediately_reachable(monkeypatch):
    probe = MagicMock()
    probe.connect = AsyncMock()
    probe.close = AsyncMock()
    probe.is_reachable = AsyncMock(return_value=True)
    monkeypatch.setattr(main, "S3Client", MagicMock(return_value=probe))

    slept = False

    async def _fake_sleep(seconds):
        nonlocal slept
        slept = True

    monkeypatch.setattr(main.asyncio, "sleep", _fake_sleep)

    await main._wait_for_s3_reachable()

    assert probe.is_reachable.await_count == 1
    assert slept is False
    assert probe.close.await_count == 1
