"""Concurrency tests for `HttpTileClient`, including the per-host semaphore."""

import asyncio

import httpx
import pytest

from clients.http_tile_client import HttpTileClient, ProviderUnavailableError


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


# --------------------------------------------------------------------------- #
# Failure mode semantics: MISSING (None) vs UNAVAILABLE (exception)
# --------------------------------------------------------------------------- #


class _ScriptedHttpxClient:
    """Minimal httpx stand-in that returns / raises per scripted response."""

    def __init__(self, response_factory):
        self._response_factory = response_factory

    async def get(self, url: str):
        return self._response_factory(url)

    async def aclose(self):
        return None


@pytest.mark.asyncio
async def test_404_returns_none_as_missing():
    """A 404 is a permanent MISS — returns None without raising."""
    client = HttpTileClient(
        max_concurrent=4, delay_ms=0, timeout_seconds=5, max_retries=1
    )
    client._client = _ScriptedHttpxClient(  # type: ignore[assignment]
        lambda _url: _FakeResponse(status_code=404)
    )
    assert await client.download_tile("https://host.test/1/2/3.png") is None


@pytest.mark.asyncio
async def test_403_returns_none_as_missing():
    """403 is treated the same as 404 (permanent MISS)."""
    client = HttpTileClient(
        max_concurrent=4, delay_ms=0, timeout_seconds=5, max_retries=1
    )
    client._client = _ScriptedHttpxClient(  # type: ignore[assignment]
        lambda _url: _FakeResponse(status_code=403)
    )
    assert await client.download_tile("https://host.test/1/2/3.png") is None


@pytest.mark.asyncio
async def test_exhausted_5xx_raises_provider_unavailable():
    """Retry budget exhausted against 500s → UNAVAILABLE (not None)."""
    client = HttpTileClient(
        max_concurrent=4, delay_ms=0, timeout_seconds=1, max_retries=2
    )
    client._client = _ScriptedHttpxClient(  # type: ignore[assignment]
        lambda _url: _FakeResponse(status_code=503)
    )
    with pytest.raises(ProviderUnavailableError) as excinfo:
        await client.download_tile("https://host.test/1/2/3.png")
    assert "host.test" in excinfo.value.url


@pytest.mark.asyncio
async def test_network_error_raises_provider_unavailable():
    """httpx.ConnectError path surfaces as UNAVAILABLE."""
    client = HttpTileClient(
        max_concurrent=4, delay_ms=0, timeout_seconds=1, max_retries=1
    )

    def _boom(_url: str):
        raise httpx.ConnectError("nope", request=httpx.Request("GET", _url))

    client._client = _ScriptedHttpxClient(_boom)  # type: ignore[assignment]
    with pytest.raises(ProviderUnavailableError):
        await client.download_tile("https://host.test/1/2/3.png")


@pytest.mark.asyncio
async def test_retry_log_names_url_and_exception(caplog):
    """A transient ConnectTimeout then success logs a WARNING naming the URL,
    the exception type, and a <no detail> sentinel for the empty message
    (replaces tenacity's opaque '<unknown> ... ConnectTimeout: .')."""
    client = HttpTileClient(
        max_concurrent=4, delay_ms=0, timeout_seconds=1, max_retries=3
    )
    calls = {"n": 0}

    def _factory(url: str):
        calls["n"] += 1
        if calls["n"] == 1:
            # Empty message — exactly the case that produced "ConnectTimeout: ."
            raise httpx.ConnectTimeout("", request=httpx.Request("GET", url))
        return _FakeResponse(status_code=200, content=b"ok")

    client._client = _ScriptedHttpxClient(_factory)  # type: ignore[assignment]
    url = "https://idede.ign.gob.ar/4/7/10.png"
    with caplog.at_level("WARNING", logger="clients.http_tile_client"):
        result = await client.download_tile(url)

    assert result == b"ok"
    msgs = [r.getMessage() for r in caplog.records]
    assert any(url in m and "ConnectTimeout" in m for m in msgs), msgs
    assert any("<no detail>" in m for m in msgs), msgs
    assert not any("<unknown>" in m for m in msgs), msgs


@pytest.mark.asyncio
async def test_success_still_returns_bytes():
    """Happy path unchanged: 200 → raw bytes."""
    client = HttpTileClient(
        max_concurrent=4, delay_ms=0, timeout_seconds=5, max_retries=1
    )
    client._client = _ScriptedHttpxClient(  # type: ignore[assignment]
        lambda _url: _FakeResponse(status_code=200, content=b"bytes")
    )
    assert await client.download_tile("https://host.test/1/2/3.png") == b"bytes"
