"""Tests for DomainSyncService watchdog accounting (BUG-27).

A watchdog timeout preempts a *resumable* cycle (each unit is indexed only once
fully downloaded), so it must be tracked as a pacing signal
(``consecutive_timeouts``) rather than inflating ``consecutive_failures`` or
being recorded as a hard error.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.domain_sync_service import DomainSyncService


class _StubDomain(DomainSyncService):
    """Concrete DomainSyncService whose domain coroutine is supplied per-call."""

    def __init__(self, timeout=10):
        settings = MagicMock()
        settings.sync_min_sleep_seconds = 0
        settings.is_s3_configured.return_value = True
        super().__init__(
            settings,
            domain="satellite",
            lock_path="/tmp/stub-domain.lock",
            interval=60,
            timeout=timeout,
            s3_concurrency=5,
            service_name="Stub domain sync",
        )

    async def _run_sync(self):  # pragma: no cover - not exercised directly
        return None


def _build_svc(timeout=10):
    """A stub wired with mocked collaborators so no real S3/Redis is touched."""
    svc = _StubDomain(timeout=timeout)
    # Pre-set the S3 client so _run_single_domain skips real client construction.
    svc._client = MagicMock()  # pylint: disable=protected-access
    svc._redis_client = AsyncMock()  # pylint: disable=protected-access
    svc._metrics_store = AsyncMock()  # pylint: disable=protected-access
    return svc


async def _returns(result):
    """A domain coroutine that resolves to a fixed (downloaded, errors) tuple."""
    return result


def _last_status(svc):
    """The status dict written at the end of the cycle (last Redis call)."""
    return svc._redis_client.update_domain_sync_status.await_args.args[1]


@pytest.mark.asyncio
async def test_timeout_tracked_separately_not_as_failure(monkeypatch):
    """A watchdog timeout increments consecutive_timeouts, not consecutive_failures,
    and is recorded with errors=0 under a distinct 'timeout' outcome."""
    svc = _build_svc()

    async def _fake_wait_for(coro, timeout):  # noqa: ARG001
        coro.close()  # avoid 'coroutine was never awaited'
        raise asyncio.TimeoutError

    monkeypatch.setattr("services.domain_sync_service.asyncio.wait_for", _fake_wait_for)

    await svc._run_single_domain(_returns((99, 0)))

    assert svc._consecutive_failures == 0  # pylint: disable=protected-access
    assert svc._consecutive_timeouts == 1  # pylint: disable=protected-access

    cycle_args = svc._metrics_store.record_sync_cycle.await_args.args
    assert cycle_args[-1] == "timeout"  # outcome
    assert cycle_args[-2] == 0  # errors, not 1

    status = _last_status(svc)
    assert status["consecutive_failures"] == "0"
    assert status["consecutive_timeouts"] == "1"
    assert status["last_sync_errors"] == "0"


@pytest.mark.asyncio
async def test_clean_cycle_resets_timeout_counter(monkeypatch):
    """A subsequent clean cycle clears both the timeout and failure counters."""
    svc = _build_svc()

    async def _fake_wait_for(coro, timeout):  # noqa: ARG001
        coro.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr("services.domain_sync_service.asyncio.wait_for", _fake_wait_for)
    await svc._run_single_domain(_returns((0, 0)))
    assert svc._consecutive_timeouts == 1  # pylint: disable=protected-access

    # Real wait_for now: a resolved coro returns instantly within the timeout.
    monkeypatch.undo()
    await svc._run_single_domain(_returns((7, 0)))

    assert svc._consecutive_timeouts == 0  # pylint: disable=protected-access
    assert svc._consecutive_failures == 0  # pylint: disable=protected-access
    assert _last_status(svc)["last_sync_downloaded"] == "7"


@pytest.mark.asyncio
async def test_error_cycle_increments_failures_not_timeouts():
    """A cycle that returns errors>0 bumps consecutive_failures, leaving timeouts."""
    svc = _build_svc()

    await svc._run_single_domain(_returns((0, 2)))

    assert svc._consecutive_failures == 1  # pylint: disable=protected-access
    assert svc._consecutive_timeouts == 0  # pylint: disable=protected-access
    status = _last_status(svc)
    assert status["last_sync_errors"] == "2"
    assert status["consecutive_failures"] == "1"
