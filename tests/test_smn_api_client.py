"""Unit tests for `SmnApiClient` (JWT cache + refresh-on-401)."""

import asyncio
import json
from typing import List

import httpx
import pytest

from clients.smn_api_client import SmnApiClient, SmnApiError


async def _build_client(
    transport: httpx.AsyncBaseTransport, **overrides
) -> SmnApiClient:
    """Construct an SmnApiClient with its inner httpx pool replaced by `transport`."""
    defaults = dict(
        base_url="https://api.test/v1",
        username="u",
        password="p",
        timeout_seconds=5,
        max_retries=2,
        token_cache_ttl_seconds=60,
    )
    defaults.update(overrides)
    client = SmnApiClient(**defaults)
    # Swap the real pool for a MockTransport-backed one so we never hit network.
    await client._client.aclose()
    client._client = httpx.AsyncClient(transport=transport)
    return client


def _make_transport(handler):
    """Wrap a (request) -> Response handler into a MockTransport."""
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_first_call_mints_token_then_fetches_stations():
    calls: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "tk-1"})
        if request.url.path.endswith("/weather/station"):
            assert request.headers.get("api_key") == "tk-1"
            return httpx.Response(200, json=[{"station_id": 1}])
        return httpx.Response(404)

    client = await _build_client(_make_transport(handler))
    try:
        data = await client.fetch_current_weather_stations()
        assert data == [{"station_id": 1}]
        # Auth was called once, then stations was called once.
        assert calls == ["POST /v1/api-token/auth", "GET /v1/weather/station"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cached_token_skips_reauth_on_second_call():
    calls: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(f"{request.method} {request.url.path}")
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "tk-1"})
        return httpx.Response(200, json=[{"station_id": 1}])

    client = await _build_client(_make_transport(handler))
    try:
        await client.fetch_current_weather_stations()
        await client.fetch_current_weather_stations()
        # Auth only once; two GETs.
        assert calls.count("POST /v1/api-token/auth") == 1
        assert calls.count("GET /v1/weather/station") == 2
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_401_triggers_one_refresh_and_retries_stations():
    tokens_issued: List[str] = []
    station_calls: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-token/auth"):
            new = f"tk-{len(tokens_issued) + 1}"
            tokens_issued.append(new)
            return httpx.Response(200, json={"token": new})
        # /weather/station: reject the first token, accept the second.
        presented = request.headers.get("api_key")
        station_calls.append(presented or "")
        if presented == "tk-1":
            return httpx.Response(401, json={"detail": "expired"})
        return httpx.Response(200, json=[{"station_id": 2}])

    client = await _build_client(_make_transport(handler))
    try:
        data = await client.fetch_current_weather_stations()
        assert data == [{"station_id": 2}]
        assert tokens_issued == ["tk-1", "tk-2"]
        # First attempt with tk-1 got 401, second attempt with tk-2 succeeded.
        assert station_calls == ["tk-1", "tk-2"]
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_5xx_is_retried_then_raises_smn_api_error():
    attempts: List[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "t"})
        attempts.append(1)
        return httpx.Response(503, text="upstream down")

    client = await _build_client(_make_transport(handler), max_retries=2)
    # Patch the back-off sleep so the test doesn't actually wait.
    sleep_called: List[float] = []

    async def fake_sleep(d):
        sleep_called.append(d)

    import clients.smn_api_client as smn_module

    original = smn_module.asyncio.sleep
    smn_module.asyncio.sleep = fake_sleep  # type: ignore[assignment]
    try:
        with pytest.raises(SmnApiError, match="failed after 2 attempts"):
            await client.fetch_current_weather_stations()
        assert len(attempts) == 2
        assert len(sleep_called) == 2
    finally:
        smn_module.asyncio.sleep = original  # type: ignore[assignment]
        await client.close()


@pytest.mark.asyncio
async def test_non_list_payload_is_rejected():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "t"})
        return httpx.Response(200, json={"oops": "object not array"})

    client = await _build_client(_make_transport(handler))
    try:
        with pytest.raises(SmnApiError, match="non-list payload"):
            await client.fetch_current_weather_stations()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_auth_endpoint_failure_surfaces_as_smn_api_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="wrong creds")

    client = await _build_client(_make_transport(handler))
    try:
        with pytest.raises(SmnApiError, match="/api-token/auth returned 403"):
            await client.fetch_current_weather_stations()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_concurrent_refreshes_are_serialized():
    """A burst of cold callers mints exactly one token, not N."""
    token_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_count
        if request.url.path.endswith("/api-token/auth"):
            token_count += 1
            await asyncio.sleep(0.01)  # let the burst pile up
            return httpx.Response(200, json={"token": f"tk-{token_count}"})
        return httpx.Response(200, json=[{"station_id": 1}])

    client = await _build_client(_make_transport(handler))
    try:
        results = await asyncio.gather(
            *(client.fetch_current_weather_stations() for _ in range(5))
        )
        assert all(r == [{"station_id": 1}] for r in results)
        assert token_count == 1
    finally:
        await client.close()


def test_module_imports_cleanly():
    """Smoke: importing the module raises nothing (catches syntax/typo regressions)."""
    from clients import smn_api_client  # noqa: F401

    assert json.dumps({"token": "x"})  # silence flake about unused json
