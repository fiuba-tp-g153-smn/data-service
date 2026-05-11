"""Unit tests for `BasemapService.list_providers` — active-probe + S3 fallback."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.http_tile_client import ProviderUnavailableError
from services.basemap_config import BasemapProvider
from services.basemap_service import BasemapService


def _make_provider(provider_id: str, min_zoom: int = 3) -> BasemapProvider:
    return BasemapProvider(
        provider_id=provider_id,
        name=provider_id.title(),
        source_url_template=f"https://example.test/{provider_id}/{{z}}/{{x}}/{{y}}.png",
        is_tms=False,
        min_zoom=min_zoom,
        max_zoom=18,
        cache_max_zoom=11,
        attribution="",
    )


def _make_redis() -> MagicMock:
    redis = MagicMock()
    redis.get_basemap_provider_availability = AsyncMock(return_value=None)
    redis.set_basemap_provider_availability = AsyncMock()
    return redis


def _make_s3(present: bool = False) -> MagicMock:
    s3 = MagicMock()
    s3.has_any_object = AsyncMock(return_value=present)
    return s3


def _make_http(data=None) -> MagicMock:
    http = MagicMock()
    http.download_tile = AsyncMock(return_value=data)
    return http


def _configure(
    *,
    providers,
    online_fallback=True,
    s3=None,
    redis=None,
    http=None,
    availability_ttl=240,
) -> BasemapService:
    service = BasemapService()
    service.configure(
        reader=MagicMock(),
        providers={p.provider_id: p for p in providers},
        online_fallback=online_fallback,
        s3_client=s3 if s3 is not None else _make_s3(),
        redis_client=redis or _make_redis(),
        http_client=http or _make_http(data=b"\x89PNG"),
        availability_ttl=availability_ttl,
    )
    return service


@pytest.mark.asyncio
async def test_provider_with_healthy_upstream_is_listed():
    """A provider whose upstream returns bytes is included."""
    service = _configure(
        providers=[_make_provider("argenmap")],
        http=_make_http(data=b"\x89PNG"),
        s3=_make_s3(present=False),
    )

    resp = await service.list_providers()
    assert [p.id for p in resp.providers] == ["argenmap"]


@pytest.mark.asyncio
async def test_provider_with_upstream_down_but_s3_present_is_listed():
    """When the probe fails but S3 has tiles, the provider is still listed."""
    http = MagicMock()
    http.download_tile = AsyncMock(
        side_effect=ProviderUnavailableError("https://example.test", "down")
    )
    service = _configure(
        providers=[_make_provider("argenmap")],
        http=http,
        s3=_make_s3(present=True),
    )

    resp = await service.list_providers()
    assert [p.id for p in resp.providers] == ["argenmap"]


@pytest.mark.asyncio
async def test_provider_with_upstream_down_and_s3_empty_is_hidden():
    """No upstream, no S3 cache → provider is excluded."""
    http = MagicMock()
    http.download_tile = AsyncMock(
        side_effect=ProviderUnavailableError("https://example.test", "down")
    )
    service = _configure(
        providers=[_make_provider("argenmap")],
        http=http,
        s3=_make_s3(present=False),
    )

    resp = await service.list_providers()
    assert resp.providers == []


@pytest.mark.asyncio
async def test_availability_cache_short_circuits_probe_and_s3():
    """When Redis has a cached availability bool, neither the probe nor S3 runs."""
    redis = _make_redis()
    redis.get_basemap_provider_availability = AsyncMock(return_value=True)
    http = _make_http(data=b"never")
    s3 = _make_s3(present=False)
    service = _configure(
        providers=[_make_provider("argenmap")],
        redis=redis,
        http=http,
        s3=s3,
    )

    resp = await service.list_providers()
    assert [p.id for p in resp.providers] == ["argenmap"]
    http.download_tile.assert_not_called()
    s3.has_any_object.assert_not_called()
    redis.set_basemap_provider_availability.assert_not_called()


@pytest.mark.asyncio
async def test_availability_result_written_to_redis_with_ttl():
    """A fresh probe writes the boolean to Redis under the configured TTL."""
    redis = _make_redis()
    service = _configure(
        providers=[_make_provider("argenmap")],
        redis=redis,
        http=_make_http(data=b"\x89PNG"),
        s3=_make_s3(present=False),
        availability_ttl=240,
    )

    await service.list_providers()
    redis.set_basemap_provider_availability.assert_awaited_once_with(
        "argenmap", True, 240
    )


@pytest.mark.asyncio
async def test_offline_fallback_falls_back_to_s3_only():
    """online_fallback=False: probe is skipped; S3 presence is the only gate."""
    http = _make_http(data=b"never-called")
    s3 = _make_s3(present=True)
    service = _configure(
        providers=[_make_provider("argenmap"), _make_provider("topographic")],
        online_fallback=False,
        http=http,
        s3=s3,
    )

    resp = await service.list_providers()
    assert {p.id for p in resp.providers} == {"argenmap", "topographic"}
    http.download_tile.assert_not_called()


@pytest.mark.asyncio
async def test_probe_uses_min_zoom_corner_tile():
    """Probe must hit (min_zoom, 0, 0) so it works for every registered provider."""
    captured_urls = []

    async def capture(url):
        captured_urls.append(url)
        return b"\x89PNG"

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=capture)
    service = _configure(
        providers=[_make_provider("argenmap", min_zoom=4)],
        http=http,
        s3=_make_s3(present=False),
    )

    await service.list_providers()
    assert captured_urls == ["https://example.test/argenmap/4/0/0.png"]


@pytest.mark.asyncio
async def test_concurrent_list_providers_single_flight_one_probe_per_provider():
    """Bursts on TTL expiry collapse to one probe per provider via single-flight."""
    started = asyncio.Event()
    release = asyncio.Event()
    call_count = 0

    async def slow_probe(_url):
        nonlocal call_count
        call_count += 1
        started.set()
        await release.wait()
        return b"\x89PNG"

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=slow_probe)
    service = _configure(
        providers=[_make_provider("argenmap")],
        http=http,
        s3=_make_s3(present=False),
    )

    tasks = [asyncio.create_task(service.list_providers()) for _ in range(8)]
    await started.wait()
    release.set()
    results = await asyncio.gather(*tasks)

    assert call_count == 1
    assert all([p.id for p in r.providers] == ["argenmap"] for r in results)


@pytest.mark.asyncio
async def test_providers_listed_in_parallel():
    """When multiple providers are configured, the probes run concurrently."""
    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def gated(_url):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        if in_flight >= 3:
            release.set()
        await release.wait()
        in_flight -= 1
        return b"\x89PNG"

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=gated)
    service = _configure(
        providers=[
            _make_provider("argenmap"),
            _make_provider("satellite"),
            _make_provider("topographic"),
        ],
        http=http,
        s3=_make_s3(present=False),
    )

    resp = await service.list_providers()
    assert peak == 3, f"expected 3 concurrent probes, got peak={peak}"
    assert {p.id for p in resp.providers} == {"argenmap", "satellite", "topographic"}
