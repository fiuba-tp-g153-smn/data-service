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
