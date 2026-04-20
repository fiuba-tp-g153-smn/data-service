"""Unit tests for `BasemapTileReader` — 3-tier fallback, neg-cache, single-flight."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from services.basemap_config import BasemapProvider
from services.basemap_tile_reader import BasemapTileReader


def _make_provider(provider_id: str = "fake") -> BasemapProvider:
    return BasemapProvider(
        provider_id=provider_id,
        name="Fake",
        source_url_template="https://example.test/{z}/{x}/{y}.png",
        is_tms=False,
        min_zoom=0,
        max_zoom=22,
        cache_max_zoom=22,
        attribution="",
    )


def _make_redis() -> MagicMock:
    redis = MagicMock()
    redis.get_basemap_tile = AsyncMock(return_value=None)
    redis.store_basemap_tile = AsyncMock()
    redis.get_basemap_tile_miss = AsyncMock(return_value=False)
    redis.mark_basemap_tile_miss = AsyncMock()
    redis.clear_basemap_tile_miss = AsyncMock()
    return redis


def _make_s3(data=None) -> MagicMock:
    s3 = MagicMock()
    s3.download_tile = AsyncMock(return_value=data)
    s3.upload_tile = AsyncMock()
    return s3


def _make_http(data=None) -> MagicMock:
    http = MagicMock()
    http.download_tile = AsyncMock(return_value=data)
    return http


def _make_reader(
    *,
    redis=None,
    s3=None,
    http=None,
    providers=None,
    online_fallback=True,
    negative_cache_enabled=True,
    negative_cache_ttl=300,
    request_deadline_seconds=4.0,
) -> BasemapTileReader:
    return BasemapTileReader(
        redis_client=redis or _make_redis(),
        s3_client=s3 or _make_s3(),
        http_client=http or _make_http(),
        providers=providers if providers is not None else {"fake": _make_provider()},
        tile_ttl=60,
        cache_concurrent=4,
        online_fallback=online_fallback,
        negative_cache_enabled=negative_cache_enabled,
        negative_cache_ttl=negative_cache_ttl,
        request_deadline_seconds=request_deadline_seconds,
    )


async def _drain(reader: BasemapTileReader) -> None:
    """Yield to the loop so scheduled background writes actually run."""
    await reader.close(timeout=1.0)


@pytest.mark.asyncio
async def test_redis_hit_short_circuits():
    redis = _make_redis()
    redis.get_basemap_tile = AsyncMock(return_value=b"hit")
    s3 = _make_s3()
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"hit"
    s3.download_tile.assert_not_called()
    http.download_tile.assert_not_called()
    redis.mark_basemap_tile_miss.assert_not_called()


@pytest.mark.asyncio
async def test_s3_hit_schedules_redis_write():
    redis = _make_redis()
    s3 = _make_s3(data=b"from-s3")
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-s3"
    http.download_tile.assert_not_called()
    await _drain(reader)
    redis.store_basemap_tile.assert_awaited()
    redis.mark_basemap_tile_miss.assert_not_called()


@pytest.mark.asyncio
async def test_relay_hit_writes_through_and_clears_miss():
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=b"from-relay")
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-relay"
    await _drain(reader)
    s3.upload_tile.assert_awaited_once()
    redis.store_basemap_tile.assert_awaited()
    redis.clear_basemap_tile_miss.assert_awaited_once_with("fake", 5, 10, 20)
    redis.mark_basemap_tile_miss.assert_not_called()


@pytest.mark.asyncio
async def test_negative_cache_hit_short_circuits_s3_and_relay():
    redis = _make_redis()
    redis.get_basemap_tile_miss = AsyncMock(return_value=True)
    s3 = _make_s3(data=None)
    http = _make_http(data=b"never")
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 2, 1, 1)
    assert got is None
    http.download_tile.assert_not_called()
    redis.mark_basemap_tile_miss.assert_not_called()


@pytest.mark.asyncio
async def test_relay_miss_writes_negative_cache():
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=None)
    reader = _make_reader(redis=redis, s3=s3, http=http, negative_cache_ttl=123)

    got = await reader.get_tile("fake", 2, 1, 1)
    assert got is None
    redis.mark_basemap_tile_miss.assert_awaited_once_with("fake", 2, 1, 1, ttl=123)


@pytest.mark.asyncio
async def test_relay_disabled_still_tombstones():
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=b"never")
    reader = _make_reader(redis=redis, s3=s3, http=http, online_fallback=False)

    got = await reader.get_tile("fake", 2, 1, 1)
    assert got is None
    http.download_tile.assert_not_called()
    redis.mark_basemap_tile_miss.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_provider_tombstones_without_relay():
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=b"never")
    reader = _make_reader(redis=redis, s3=s3, http=http, providers={})

    got = await reader.get_tile("missing", 2, 1, 1)
    assert got is None
    http.download_tile.assert_not_called()
    redis.mark_basemap_tile_miss.assert_awaited_once()


@pytest.mark.asyncio
async def test_single_flight_dedupes_concurrent_requests():
    """16 concurrent requests for the same tile must fire S3 exactly once."""
    redis = _make_redis()

    started = asyncio.Event()
    release = asyncio.Event()
    call_count = 0

    async def slow_s3(_key):
        nonlocal call_count
        call_count += 1
        started.set()
        await release.wait()
        return b"payload"

    s3 = MagicMock()
    s3.download_tile = AsyncMock(side_effect=slow_s3)
    s3.upload_tile = AsyncMock()
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http)

    tasks = [asyncio.create_task(reader.get_tile("fake", 4, 5, 9)) for _ in range(16)]
    await started.wait()
    release.set()
    results = await asyncio.gather(*tasks)

    assert call_count == 1
    assert all(r == b"payload" for r in results)
    http.download_tile.assert_not_called()


@pytest.mark.asyncio
async def test_single_flight_propagates_exception():
    redis = _make_redis()

    async def raising_s3(_key):
        raise RuntimeError("boom")

    s3 = MagicMock()
    s3.download_tile = AsyncMock(side_effect=raising_s3)
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http)

    # With negative_cache_enabled=True the gather() catches the exception and
    # the reader degrades to s3_data=None. Disable it to surface the raw path.
    reader._negative_cache_enabled = False  # pylint: disable=protected-access

    tasks = [asyncio.create_task(reader.get_tile("fake", 4, 5, 9)) for _ in range(4)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        assert isinstance(r, RuntimeError)
    assert not reader._inflight  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_negative_cache_check_failure_fails_open():
    """If the miss-check Redis call raises, the reader still consults S3/relay."""
    redis = _make_redis()
    redis.get_basemap_tile_miss = AsyncMock(side_effect=RuntimeError("redis down"))
    s3 = _make_s3(data=b"from-s3")
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-s3"


@pytest.mark.asyncio
async def test_negative_cache_disabled_skips_miss_lookup():
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=None)
    reader = _make_reader(redis=redis, s3=s3, http=http, negative_cache_enabled=False)

    got = await reader.get_tile("fake", 2, 1, 1)
    assert got is None
    redis.get_basemap_tile_miss.assert_not_called()
    redis.mark_basemap_tile_miss.assert_not_called()


@pytest.mark.asyncio
async def test_deadline_returns_none_and_tombstones_fast():
    """A slow S3 call must not block past the deadline; result is None + tombstone."""
    redis = _make_redis()

    async def hanging_s3(_key):
        await asyncio.sleep(5.0)
        return b"never"

    s3 = MagicMock()
    s3.download_tile = AsyncMock(side_effect=hanging_s3)
    s3.upload_tile = AsyncMock()
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http, request_deadline_seconds=0.2)

    loop = asyncio.get_event_loop()
    t0 = loop.time()
    got = await reader.get_tile("fake", 2, 1, 1)
    elapsed = loop.time() - t0

    assert got is None
    assert elapsed < 1.0, f"deadline not enforced: took {elapsed:.2f}s"
    await _drain(reader)
    redis.mark_basemap_tile_miss.assert_awaited()


@pytest.mark.asyncio
async def test_deadline_releases_single_flight_waiters():
    """When the leader trips the deadline, every waiter is released too."""
    redis = _make_redis()

    async def hanging_s3(_key):
        await asyncio.sleep(5.0)
        return b"never"

    s3 = MagicMock()
    s3.download_tile = AsyncMock(side_effect=hanging_s3)
    s3.upload_tile = AsyncMock()
    http = _make_http()
    reader = _make_reader(redis=redis, s3=s3, http=http, request_deadline_seconds=0.2)

    loop = asyncio.get_event_loop()
    t0 = loop.time()
    tasks = [asyncio.create_task(reader.get_tile("fake", 2, 1, 1)) for _ in range(8)]
    results = await asyncio.gather(*tasks)
    elapsed = loop.time() - t0

    assert all(r is None for r in results)
    assert elapsed < 1.5, f"waiters not released promptly: {elapsed:.2f}s"
    assert not reader._inflight  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_s3_nosuchkey_logged_at_debug(caplog):
    """S3 NoSuchKey surfaces as DEBUG, not WARN — validates the s3_client change."""
    from botocore.exceptions import ClientError
    from clients.s3_client import S3Client

    err = ClientError({"Error": {"Code": "NoSuchKey", "Message": "nope"}}, "GetObject")

    s3 = S3Client.__new__(S3Client)
    # pylint: disable=protected-access
    s3._bucket = "b"
    mock_client = MagicMock()
    mock_client.get_object = AsyncMock(side_effect=err)
    s3._ensure_connected = AsyncMock(return_value=mock_client)

    caplog.set_level(logging.DEBUG, logger="clients.s3_client")
    result = await s3.download_tile("basemap/fake/2/1/1.png")

    assert result is None
    warn_messages = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert not warn_messages, f"expected no warnings, got: {warn_messages}"
    debug_messages = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert any("S3 tile miss" in r.message for r in debug_messages)
