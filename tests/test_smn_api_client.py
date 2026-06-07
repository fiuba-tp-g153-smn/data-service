"""Unit tests for `SmnApiClient` (JWT cache + refresh-on-401)."""

import asyncio
import json
from typing import List

import httpx
import pytest

from clients.smn_api_client import (
    SmnApiClient,
    SmnApiError,
    _redact_body,
    _redact_headers,
)


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
    # Preserve the same client-level headers (User-Agent etc.) the real
    # constructor applies, so tests can assert on them.
    preserved_headers = dict(client._client.headers)
    await client._client.aclose()
    client._client = httpx.AsyncClient(transport=transport, headers=preserved_headers)
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
            # SMN requires the "JWT <token>" scheme on the Authorization header.
            assert request.headers.get("authorization") == "JWT tk-1"
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
        # Both arrive on the Authorization header prefixed as "JWT <token>".
        presented = request.headers.get("authorization")
        station_calls.append(presented or "")
        if presented == "JWT tk-1":
            return httpx.Response(401, json={"detail": "expired"})
        return httpx.Response(200, json=[{"station_id": 2}])

    client = await _build_client(_make_transport(handler))
    try:
        data = await client.fetch_current_weather_stations()
        assert data == [{"station_id": 2}]
        assert tokens_issued == ["tk-1", "tk-2"]
        # First attempt with tk-1 got 401, second attempt with tk-2 succeeded.
        assert station_calls == ["JWT tk-1", "JWT tk-2"]
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
async def test_default_user_agent_and_settling_delay_are_applied():
    """The client sends httpx's default UA (no custom override); the settling
    delay fires after a mint."""
    sent_user_agents: List[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent_user_agents.append(request.headers.get("user-agent", ""))
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "tk-1"})
        return httpx.Response(200, json=[{"station_id": 1}])

    client = await _build_client(
        _make_transport(handler),
        token_settling_delay_seconds=0.5,
    )

    # Patch asyncio.sleep so we observe the delay call without actually waiting.
    sleeps: List[float] = []

    async def fake_sleep(d):
        sleeps.append(d)

    import clients.smn_api_client as smn_module

    original = smn_module.asyncio.sleep
    smn_module.asyncio.sleep = fake_sleep  # type: ignore[assignment]
    try:
        await client.fetch_current_weather_stations()
    finally:
        smn_module.asyncio.sleep = original  # type: ignore[assignment]
        await client.close()

    # Both calls carried httpx's default UA — no curl override.
    assert len(sent_user_agents) == 2
    assert all(
        ua.startswith("python-httpx/") for ua in sent_user_agents
    ), sent_user_agents
    # The 0.5s settling delay was observed exactly once (one mint -> one wait).
    assert 0.5 in sleeps


@pytest.mark.asyncio
async def test_settling_delay_zero_skips_the_sleep_entirely():
    """Default 0 means asyncio.sleep is not called from the refresh path."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "tk-1"})
        return httpx.Response(200, json=[{"station_id": 1}])

    client = await _build_client(_make_transport(handler))  # default delay=0

    sleeps: List[float] = []

    async def fake_sleep(d):
        sleeps.append(d)

    import clients.smn_api_client as smn_module

    original = smn_module.asyncio.sleep
    smn_module.asyncio.sleep = fake_sleep  # type: ignore[assignment]
    try:
        await client.fetch_current_weather_stations()
    finally:
        smn_module.asyncio.sleep = original  # type: ignore[assignment]
        await client.close()

    # No sleeps in the happy path: zero refreshes-with-delay, zero retries.
    assert sleeps == []


@pytest.mark.asyncio
async def test_persistent_401_surfaces_as_smn_api_error():
    """Two 401s in a row → SmnApiError (instead of bare _Unauthorized leaking out)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/api-token/auth"):
            return httpx.Response(200, json={"token": "always-rejected"})
        # Every stations call returns 401 — credentials are bad / no access.
        return httpx.Response(401, json={"detail": "no access"})

    client = await _build_client(_make_transport(handler))
    try:
        with pytest.raises(SmnApiError, match="rejected the freshly-minted token"):
            await client.fetch_current_weather_stations()
    finally:
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


# --------------------------------------------------------------------------- #
# Request-log redaction (SMN_API_LOG_REQUESTS)
# --------------------------------------------------------------------------- #


def test_redact_headers_masks_only_authorization():
    headers = httpx.Headers({"Authorization": "JWT abc", "Accept": "application/json"})
    out = {k.lower(): v for k, v in _redact_headers(headers).items()}
    assert out["authorization"] == "<redacted>"
    assert out["accept"] == "application/json"


def test_redact_body_masks_credentials_and_passes_through_non_json():
    masked = json.loads(_redact_body('{"username": "u", "password": "p", "keep": "v"}'))
    assert masked == {
        "username": "<redacted>",
        "password": "<redacted>",
        "keep": "v",
    }
    # Non-JSON / non-object bodies are passed through untouched.
    assert _redact_body("not json") == "not json"
    assert _redact_body("[1, 2, 3]") == "[1, 2, 3]"


@pytest.mark.asyncio
async def test_request_log_redacts_credentials(caplog):
    """The outbound-request hook never writes the JWT, username or password."""
    request = httpx.Request(
        "POST",
        "https://api.test/v1/api-token/auth",
        json={"username": "topsecretuser", "password": "topsecretpass"},
        headers={"Authorization": "JWT topsecrettoken"},
    )
    with caplog.at_level("INFO", logger="clients.smn_api_client"):
        await SmnApiClient._log_outbound_request(request)

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert "topsecrettoken" not in blob
    assert "topsecretuser" not in blob
    assert "topsecretpass" not in blob
    assert "<redacted>" in blob
    assert "/api-token/auth" in blob  # URL stays visible for debugging


def test_log_requests_registers_the_redacting_hook():
    client = SmnApiClient(
        base_url="https://api.test/v1",
        username="u",
        password="p",
        timeout_seconds=5,
        max_retries=1,
        token_cache_ttl_seconds=60,
        log_requests=True,
    )
    hooks = client._client.event_hooks.get("request", [])
    assert SmnApiClient._log_outbound_request in hooks
