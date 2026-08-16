"""Tests for the BaseSyncService loop pacing (min-sleep floor + overrun warning)
and lock-acquisition error handling (contention vs a broken lock subsystem)."""

import errno
from unittest.mock import MagicMock, patch

import pytest

from services.base_sync_service import BaseSyncService


class _StubSync(BaseSyncService):
    """Minimal concrete sync service whose cycle does nothing."""

    def __init__(self, sync_interval, min_sleep):
        super().__init__(
            settings=object(),
            sync_interval=sync_interval,
            service_name="Stub",
            min_sleep=min_sleep,
        )

    def _get_lock_path(self):
        return "/tmp/stub.lock"

    async def _run_sync(self):  # pragma: no cover - trivial
        return None


async def _drive_one_cycle(svc, monotonic_values):
    """Run _sync_loop for exactly one iteration and return the captured sleeps.

    time.monotonic is fed controlled values (cycle_start, elapsed-end) to
    simulate a normal or overrunning cycle; the patched asyncio.sleep records
    the requested duration and stops the loop so the real event loop never
    sleeps or touches the network.
    """
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        svc._running = False  # pylint: disable=protected-access

    svc._running = True  # pylint: disable=protected-access
    with (
        patch("services.base_sync_service.asyncio.sleep", side_effect=fake_sleep),
        patch(
            "services.base_sync_service.time.monotonic",
            side_effect=list(monotonic_values),
        ),
    ):
        await svc._sync_loop()  # pylint: disable=protected-access
    return sleeps


@pytest.mark.asyncio
async def test_sleep_floored_to_min_sleep_on_overrun():
    """A cycle that overruns the interval still yields at least _min_sleep."""
    svc = _StubSync(sync_interval=60, min_sleep=10)
    sleeps = await _drive_one_cycle(svc, [0.0, 500.0])
    assert sleeps == [10]  # 60 - 500 -> 0, floored up to min_sleep


@pytest.mark.asyncio
async def test_overrun_emits_warning(caplog):
    """An overrunning cycle logs a WARNING naming the service."""
    svc = _StubSync(sync_interval=60, min_sleep=10)
    with caplog.at_level("WARNING", logger="services.base_sync_service"):
        await _drive_one_cycle(svc, [0.0, 500.0])
    assert any("overran interval" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_no_floor_when_min_sleep_zero_and_normal_cycle():
    """Default min_sleep=0 leaves the interval-based sleep unchanged (basemap path)."""
    svc = _StubSync(sync_interval=60, min_sleep=0)
    sleeps = await _drive_one_cycle(svc, [0.0, 5.0])
    assert sleeps == [55]  # 60 - 5, no flooring


# ── Lock acquisition: contention vs broken subsystem (BUG-32) ─────────────────


def _lockf_raiser(err_no):
    """A fake fcntl.lockf that always fails with the given errno."""

    def _raise(_handle, _flags):
        raise OSError(err_no, "lockf")

    return _raise


@pytest.mark.asyncio
async def test_start_disabled_quietly_on_lock_contention(monkeypatch):
    """EAGAIN/EACCES = a sibling worker holds the lock: disable quietly, no raise."""
    svc = _StubSync(sync_interval=60, min_sleep=0)
    monkeypatch.setattr(
        "services.base_sync_service.fcntl.lockf", _lockf_raiser(errno.EAGAIN)
    )
    app_logger = MagicMock()

    await svc.start(app_logger)  # must not raise

    assert svc._running is False  # pylint: disable=protected-access
    assert svc._lock_file_handle is None  # pylint: disable=protected-access
    app_logger.info.assert_called()  # "another worker is active"
    app_logger.error.assert_not_called()


@pytest.mark.asyncio
async def test_start_raises_on_broken_lock_subsystem(monkeypatch):
    """A non-contention errno (ENOLCK) = the lock subsystem is broken: fail loud."""
    svc = _StubSync(sync_interval=60, min_sleep=0)
    monkeypatch.setattr(
        "services.base_sync_service.fcntl.lockf", _lockf_raiser(errno.ENOLCK)
    )
    app_logger = MagicMock()

    with pytest.raises(OSError):
        await svc.start(app_logger)

    assert svc._running is False  # pylint: disable=protected-access
    assert svc._lock_file_handle is None  # pylint: disable=protected-access
    app_logger.error.assert_called()  # surfaced, not silently disabled
