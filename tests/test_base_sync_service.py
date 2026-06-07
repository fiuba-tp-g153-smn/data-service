"""Tests for the BaseSyncService loop pacing (min-sleep floor + overrun warning)."""

from unittest.mock import patch

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
