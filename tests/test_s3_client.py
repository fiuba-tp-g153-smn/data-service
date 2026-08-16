"""Unit tests for the `S3Client` helpers that can run without a live backend."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError

from clients.s3_client import S3Client


def _make_client() -> S3Client:
    return S3Client(
        endpoint="http://127.0.0.1:1",  # deliberately unroutable
        access_key="k",
        secret_key="s",
        bucket="basemap-tiles",
        secure=False,
        max_concurrent_downloads=1,
    )


class _FakeClientCtx:
    """Minimal async context manager standing in for an aioboto3 client."""

    def __init__(self, client):
        self._client = client

    async def __aenter__(self):
        return self._client

    async def __aexit__(self, *_exc):
        return False


def _capture_session_client(client: S3Client) -> dict:
    """Patch `client._session.client` to record its call args; return the record."""
    captured: dict = {}

    def _fake(service: str, **kwargs):
        captured["service"] = service
        captured["kwargs"] = kwargs
        return _FakeClientCtx(AsyncMock())

    client._session.client = _fake  # type: ignore[assignment]
    return captured


@pytest.mark.asyncio
async def test_connect_passes_botocore_config_with_timeouts():
    """When timeouts/retries are configured, connect() must pass a botocore Config."""
    client = S3Client(
        endpoint="http://127.0.0.1:1",
        access_key="k",
        secret_key="s",
        bucket="tiles-data",
        secure=False,
        max_concurrent_downloads=1,
        connect_timeout=5,
        read_timeout=30,
        max_attempts=3,
    )
    captured = _capture_session_client(client)

    await client.connect()
    await client.close()

    assert captured["service"] == "s3"
    config = captured["kwargs"]["config"]
    assert config.connect_timeout == 5
    assert config.read_timeout == 30
    assert config.retries == {"max_attempts": 3, "mode": "standard"}


@pytest.mark.asyncio
async def test_connect_omits_config_when_no_timeouts_set():
    """With no timeouts configured, no Config is passed (botocore keeps defaults)."""
    client = _make_client()  # constructed without timeout args
    captured = _capture_session_client(client)

    await client.connect()
    await client.close()

    assert "config" not in captured["kwargs"]


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_swallows_endpoint_connection_error(
    monkeypatch,
    caplog,
):
    """A down S3 backend must not abort startup — warning logged, no raise."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(
        side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        await client.ensure_lifecycle_expiration(days=35)

    assert any(
        "Could not set lifecycle policy" in record.message
        and "basemap-tiles" in record.message
        for record in caplog.records
    ), caplog.text


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_swallows_client_error(monkeypatch, caplog):
    """Backends that return a ClientError (unsupported op) also degrade to a warning."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(
        side_effect=ClientError(
            error_response={"Error": {"Code": "NotImplemented", "Message": "nope"}},
            operation_name="PutBucketLifecycleConfiguration",
        )
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        await client.ensure_lifecycle_expiration(days=14)

    assert any(
        "Could not set lifecycle policy" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_swallows_generic_botocore_error(
    monkeypatch,
    caplog,
):
    """Any other BotoCoreError subclass (timeouts, SSL, etc.) is also non-fatal."""
    client = _make_client()
    fake = MagicMock()

    class _WhateverBotoError(BotoCoreError):
        fmt = "whatever"

    fake.put_bucket_lifecycle_configuration = AsyncMock(
        side_effect=_WhateverBotoError()
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        await client.ensure_lifecycle_expiration(days=7)

    assert any(
        "Could not set lifecycle policy" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_swallows_os_error(monkeypatch, caplog):
    """Socket-level OSErrors (e.g. connection reset) follow the same path."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(
        side_effect=OSError("connection reset")
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        await client.ensure_lifecycle_expiration(days=35)

    assert any(
        "Could not set lifecycle policy" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_success_logs_info(monkeypatch, caplog):
    """Happy path still emits the INFO line so operators see the rule applied."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(return_value=None)
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.INFO):
        await client.ensure_lifecycle_expiration(days=35)

    assert any(
        "S3 lifecycle policy set" in record.message and "35 days" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_scopes_rule_to_given_prefix():
    """A non-empty prefix scopes the rule so root singletons aren't swept."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(return_value=None)
    client._ensure_connected = AsyncMock(return_value=fake)  # type: ignore[assignment]

    await client.ensure_lifecycle_expiration(
        days=2,
        rule_id="weather-stations-expiration",
        prefix="weather-stations/snapshots/",
    )

    _, kwargs = fake.put_bucket_lifecycle_configuration.call_args
    rule = kwargs["LifecycleConfiguration"]["Rules"][0]
    assert rule["ID"] == "weather-stations-expiration"
    assert rule["Filter"] == {"Prefix": "weather-stations/snapshots/"}
    assert rule["Expiration"] == {"Days": 2}


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_defaults_to_bucket_wide_prefix():
    """Default prefix stays bucket-wide so the basemap caller is unchanged."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(return_value=None)
    client._ensure_connected = AsyncMock(return_value=fake)  # type: ignore[assignment]

    await client.ensure_lifecycle_expiration(days=35)

    _, kwargs = fake.put_bucket_lifecycle_configuration.call_args
    rule = kwargs["LifecycleConfiguration"]["Rules"][0]
    assert rule["Filter"] == {"Prefix": ""}


@pytest.mark.asyncio
async def test_ensure_lifecycle_expiration_does_not_swallow_timeout_as_raise():
    """Sanity: asyncio.TimeoutError is caught (belt-and-suspenders with BotoCoreError)."""
    client = _make_client()
    fake = MagicMock()
    fake.put_bucket_lifecycle_configuration = AsyncMock(
        side_effect=asyncio.TimeoutError()
    )
    # Inject via attribute patching (no monkeypatch fixture here).
    client._ensure_connected = AsyncMock(return_value=fake)  # type: ignore[assignment]

    # Must not raise.
    await client.ensure_lifecycle_expiration(days=35)


@pytest.mark.asyncio
async def test_download_tile_returns_none_on_endpoint_connection_error(
    monkeypatch, caplog
):
    """S3 unreachable → tile download degrades to None + WARNING."""
    client = _make_client()
    fake = MagicMock()
    fake.get_object = AsyncMock(
        side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        result = await client.download_tile("basemap/argenmap/3/0/0.png")

    assert result is None
    assert any(
        "S3 unavailable for tile" in record.message
        and "basemap/argenmap/3/0/0.png" in record.message
        for record in caplog.records
    ), caplog.text


@pytest.mark.asyncio
async def test_download_tile_returns_none_on_generic_botocore_error(
    monkeypatch, caplog
):
    """Any other BotoCoreError subclass also degrades to None."""
    client = _make_client()
    fake = MagicMock()

    class _WhateverBotoError(BotoCoreError):
        fmt = "whatever"

    fake.get_object = AsyncMock(side_effect=_WhateverBotoError())
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        result = await client.download_tile("basemap/argenmap/3/0/0.png")

    assert result is None
    assert any("S3 unavailable for tile" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_download_tile_returns_none_when_ensure_connected_raises(
    monkeypatch, caplog
):
    """A connect-time BotoCoreError is also degraded — no exception out."""
    client = _make_client()
    monkeypatch.setattr(
        client,
        "_ensure_connected",
        AsyncMock(
            side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
        ),
    )

    with caplog.at_level(logging.WARNING):
        result = await client.download_tile("basemap/argenmap/3/0/0.png")

    assert result is None
    assert any("S3 unavailable for tile" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_has_any_object_returns_false_on_endpoint_connection_error(
    monkeypatch, caplog
):
    """S3 unreachable → has_any_object returns False instead of raising."""
    client = _make_client()
    fake = MagicMock()
    fake.list_objects_v2 = AsyncMock(
        side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        present = await client.has_any_object("basemap/argenmap/")

    assert present is False
    assert any(
        "S3 unavailable while checking prefix" in record.message
        and "basemap/argenmap/" in record.message
        for record in caplog.records
    ), caplog.text


@pytest.mark.asyncio
async def test_has_any_object_returns_false_on_client_error(monkeypatch, caplog):
    """ClientError used to re-raise; it now degrades to False with a warning."""
    client = _make_client()
    fake = MagicMock()
    fake.list_objects_v2 = AsyncMock(
        side_effect=ClientError(
            error_response={"Error": {"Code": "AccessDenied", "Message": "denied"}},
            operation_name="ListObjectsV2",
        )
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.WARNING):
        present = await client.has_any_object("basemap/argenmap/")

    assert present is False
    assert any(
        "S3 unavailable while checking prefix" in record.message
        for record in caplog.records
    )


# --------------------------------------------------------------------------- #
# Listing contract: raise on infra error (sync counts it) vs tolerant try_* / #
# list_object_keys (read paths degrade to []) — BUG-03.                        #
# --------------------------------------------------------------------------- #


def _client_with_failing_paginator() -> S3Client:
    """S3Client whose paginator errors like a transient S3 outage."""
    client = _make_client()
    fake = MagicMock()
    fake.get_paginator = MagicMock(
        side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
    )
    client._client = fake  # type: ignore[assignment]
    return client


@pytest.mark.asyncio
async def test_list_objects_raises_on_infra_error():
    """A real S3 outage must surface, not be masked as an empty listing."""
    client = _client_with_failing_paginator()
    with pytest.raises(EndpointConnectionError):
        await client._list_objects("tiles/")


@pytest.mark.asyncio
async def test_get_subdirectories_raises_on_infra_error():
    """Sync loops rely on this raising so an outage is counted, not 'no dirs'."""
    client = _client_with_failing_paginator()
    with pytest.raises(EndpointConnectionError):
        await client.get_subdirectories("tiles/")


@pytest.mark.asyncio
async def test_try_get_subdirectories_degrades_to_empty():
    """Read paths degrade to [] rather than 5xx on a transient S3 error."""
    client = _client_with_failing_paginator()
    assert await client.try_get_subdirectories("tiles/") == []


@pytest.mark.asyncio
async def test_list_object_keys_degrades_to_empty():
    """list_object_keys keeps its documented '[] on error' read tolerance."""
    client = _client_with_failing_paginator()
    assert await client.list_object_keys("tiles/") == []
