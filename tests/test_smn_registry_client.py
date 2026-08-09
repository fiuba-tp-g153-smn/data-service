"""Unit tests for `SmnRegistryClient` — Cloudflare detection and retry budget."""

import io
import zipfile

import httpx
import pytest

from clients.smn_registry_client import (
    SmnRegistryBlockedError,
    SmnRegistryClient,
    SmnRegistryError,
)


def _build_client(handler, max_retries=3) -> SmnRegistryClient:
    """Construct the client with its httpx pool swapped for a MockTransport."""
    client = SmnRegistryClient(
        url="http://reg.test/x", timeout_seconds=5, max_retries=max_retries
    )
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    )
    return client


def _zip_bytes(name: str, body: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, body)
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_cloudflare_challenge_raises_blocked_without_retrying():
    """A challenge can never be cleared by retrying, so it must fail fast."""
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            403,
            headers={
                "cf-mitigated": "challenge",
                "server": "cloudflare",
                "cf-ray": "a288a4ef387c77d1-FRA",
            },
            text="<html>Just a moment...</html>",
        )

    client = _build_client(handler)
    try:
        with pytest.raises(SmnRegistryBlockedError) as excinfo:
            await client.fetch_registry_text()
    finally:
        await client.close()

    assert len(calls) == 1, "challenge must not consume the retry budget"
    message = str(excinfo.value)
    assert "Cloudflare" in message
    assert "a288a4ef387c77d1-FRA" in message


@pytest.mark.asyncio
async def test_cloudflare_detected_without_cf_mitigated_header():
    """Older edges omit cf-mitigated; 403 + server: cloudflare is enough."""
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(403, headers={"server": "cloudflare"}, text="nope")

    client = _build_client(handler)
    try:
        with pytest.raises(SmnRegistryBlockedError):
            await client.fetch_registry_text()
    finally:
        await client.close()

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_plain_origin_error_still_uses_full_retry_budget():
    """A genuine upstream failure keeps its retries — only challenges short-circuit."""
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(500, text="boom")

    client = _build_client(handler, max_retries=3)
    try:
        with pytest.raises(SmnRegistryError) as excinfo:
            await client.fetch_registry_text()
    finally:
        await client.close()

    assert len(calls) == 3
    assert not isinstance(excinfo.value, SmnRegistryBlockedError)


@pytest.mark.asyncio
async def test_plain_403_without_cloudflare_is_not_treated_as_blocked():
    """An origin 403 is a normal error: retried, and not SmnRegistryBlockedError."""
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(403, headers={"server": "nginx"}, text="denied")

    client = _build_client(handler, max_retries=2)
    try:
        with pytest.raises(SmnRegistryError) as excinfo:
            await client.fetch_registry_text()
    finally:
        await client.close()

    assert len(calls) == 2
    assert not isinstance(excinfo.value, SmnRegistryBlockedError)


@pytest.mark.asyncio
async def test_successful_fetch_unzips_latin1_text():
    payload = "NOMBRE\nPEÑA".encode("latin-1")

    def handler(request):
        return httpx.Response(200, content=_zip_bytes("estaciones_smn.txt", payload))

    client = _build_client(handler)
    try:
        text = await client.fetch_registry_text()
    finally:
        await client.close()

    assert "PEÑA" in text
