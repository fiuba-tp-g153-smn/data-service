"""Tests for ``S3Client.is_reachable()`` — the startup reachability probe.

``is_reachable()`` must report ``True`` whenever the endpoint *answers* — even
with an auth/permission error or a missing bucket — and report ``False`` only on
network-level failures. That distinction is what lets the startup gate
block-and-retry on a real S3 outage without spinning forever against a
healthy-but-empty S3.
"""

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from clients.s3_client import S3Client


def _make_client() -> S3Client:
    return S3Client(
        endpoint="http://127.0.0.1:1",  # deliberately unroutable
        access_key="k",
        secret_key="s",
        bucket="tiles-data",
        secure=False,
        max_concurrent_downloads=1,
    )


@pytest.mark.asyncio
async def test_is_reachable_true_when_list_buckets_succeeds(monkeypatch):
    client = _make_client()
    fake = MagicMock()
    fake.list_buckets = AsyncMock(return_value={"Buckets": []})
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    assert await client.is_reachable() is True
    fake.list_buckets.assert_awaited_once()


@pytest.mark.asyncio
async def test_is_reachable_true_on_client_error(monkeypatch):
    """A structured S3 error (AccessDenied / 404 / ...) means the endpoint
    answered, so it counts as reachable."""
    client = _make_client()
    fake = MagicMock()
    fake.list_buckets = AsyncMock(
        side_effect=ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}},
            "ListBuckets",
        )
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    assert await client.is_reachable() is True


@pytest.mark.asyncio
async def test_is_reachable_false_on_endpoint_connection_error(monkeypatch, caplog):
    """A network-level failure (S3 down) is the only thing that counts as
    unreachable, so the startup gate keeps waiting."""
    client = _make_client()
    fake = MagicMock()
    fake.list_buckets = AsyncMock(
        side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
    )
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    with caplog.at_level(logging.DEBUG):
        assert await client.is_reachable() is False


@pytest.mark.asyncio
async def test_is_reachable_false_on_os_error(monkeypatch):
    client = _make_client()
    fake = MagicMock()
    fake.list_buckets = AsyncMock(side_effect=OSError("connection refused"))
    monkeypatch.setattr(client, "_ensure_connected", AsyncMock(return_value=fake))

    assert await client.is_reachable() is False
