"""Concurrency tests for `HttpTileClient`, including the per-host semaphore."""

import asyncio

import pytest

from clients.http_tile_client import HttpTileClient


class _FakeResponse:
    def __init__(self, status_code: int = 200, content: bytes = b"ok"):
        self.status_code = status_code
        self.content = content


class _FakeHttpxClient:
    """Minimal stand-in for `httpx.AsyncClient.get` that reports concurrency."""

    def __init__(self, gate: asyncio.Event, counter: dict[str, int]):
        self._gate = gate
        self._counter = counter
        self._lock = asyncio.Lock()

    async def get(self, url: str):  # noqa: D401
        # Record + update the peak in-flight counter for this URL's host.
        async with self._lock:
            self._counter["in_flight"] += 1
            self._counter["peak"] = max(
                self._counter["peak"], self._counter["in_flight"]
            )
        try:
            await self._gate.wait()
        finally:
            async with self._lock:
                self._counter["in_flight"] -= 1
        return _FakeResponse()

    async def aclose(self):
        return None


@pytest.mark.asyncio
async def test_per_host_semaphore_bounds_concurrent_requests_to_one_host():
    """With per_host=2, five concurrent calls to a single host see at most 2 in flight."""
    client = HttpTileClient(
        max_concurrent=10,
        delay_ms=0,
        timeout_seconds=5,
        max_retries=1,
        per_host_concurrent=2,
    )
    gate = asyncio.Event()
    counter = {"in_flight": 0, "peak": 0}
    client._client = _FakeHttpxClient(gate, counter)  # type: ignore[assignment]

    url = "https://host-a.test/1/2/3.png"
    tasks = [asyncio.create_task(client.download_tile(url)) for _ in range(5)]
    # Let the ones that slipped past the semaphore park at the gate.
    await asyncio.sleep(0.05)
    assert counter["peak"] <= 2, f"peak concurrency {counter['peak']} > 2"

    gate.set()
    results = await asyncio.gather(*tasks)
    assert all(r == b"ok" for r in results)


@pytest.mark.asyncio
async def test_per_host_semaphore_allows_parallel_across_hosts():
    """Five concurrent calls to five distinct hosts all run in parallel under a generous global."""
    client = HttpTileClient(
        max_concurrent=10,
        delay_ms=0,
        timeout_seconds=5,
        max_retries=1,
        per_host_concurrent=1,
    )
    gate = asyncio.Event()
    counter = {"in_flight": 0, "peak": 0}
    client._client = _FakeHttpxClient(gate, counter)  # type: ignore[assignment]

    urls = [f"https://host-{i}.test/1/2/3.png" for i in range(5)]
    tasks = [asyncio.create_task(client.download_tile(u)) for u in urls]
    await asyncio.sleep(0.05)
    assert counter["peak"] == 5, f"expected 5 in flight, got {counter['peak']}"

    gate.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_disabled_per_host_limit_does_not_bottleneck():
    """When per_host_concurrent is None, only the global semaphore applies."""
    client = HttpTileClient(
        max_concurrent=4,
        delay_ms=0,
        timeout_seconds=5,
        max_retries=1,
        per_host_concurrent=None,
    )
    gate = asyncio.Event()
    counter = {"in_flight": 0, "peak": 0}
    client._client = _FakeHttpxClient(gate, counter)  # type: ignore[assignment]

    url = "https://host-x.test/1/2/3.png"
    tasks = [asyncio.create_task(client.download_tile(url)) for _ in range(4)]
    await asyncio.sleep(0.05)
    assert counter["peak"] == 4

    gate.set()
    await asyncio.gather(*tasks)
