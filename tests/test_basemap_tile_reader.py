"""Unit tests for `BasemapTileReader` — prod-first fallback + single-flight."""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from clients.http_tile_client import ProviderUnavailableError
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
    request_deadline_seconds=4.0,
    redis_cache_enabled=True,
    s3_cache_enabled=True,
) -> BasemapTileReader:
    # When s3_cache_enabled is False, pass s3=None so the reader goes
    # straight to the relay (matches production relay_only wiring).
    s3_arg = s3 if s3 is not None else (_make_s3() if s3_cache_enabled else None)
    return BasemapTileReader(
        redis_client=redis or _make_redis(),
        s3_client=s3_arg,
        http_client=http or _make_http(),
        providers=providers if providers is not None else {"fake": _make_provider()},
        tile_ttl=60,
        cache_concurrent=4,
        online_fallback=online_fallback,
        request_deadline_seconds=request_deadline_seconds,
        redis_cache_enabled=redis_cache_enabled,
        s3_cache_enabled=s3_cache_enabled,
    )


async def _drain(reader: BasemapTileReader) -> None:
    """Yield to the loop so scheduled background writes actually run."""
    await reader.close(timeout=1.0)


@pytest.mark.asyncio
async def test_prod_hit_writes_through_to_redis_and_s3():
    """Successful upstream fetch is the hot path; Redis + S3 are populated."""
    redis = _make_redis()
    s3 = _make_s3()
    http = _make_http(data=b"from-prod")
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)

    assert got == b"from-prod"
    http.download_tile.assert_awaited_once()
    # Caches are not read when prod succeeds.
    redis.get_basemap_tile.assert_not_called()
    s3.download_tile.assert_not_called()
    await _drain(reader)
    redis.store_basemap_tile.assert_awaited()
    s3.upload_tile.assert_awaited_once()


@pytest.mark.asyncio
async def test_prod_unavailable_falls_through_to_redis_hit():
    """Upstream outage → fall back to Redis hot cache, S3 untouched."""
    redis = _make_redis()
    redis.get_basemap_tile = AsyncMock(return_value=b"from-redis")
    s3 = _make_s3()
    http = MagicMock()
    http.download_tile = AsyncMock(
        side_effect=ProviderUnavailableError(
            "https://example.test/5/10/20.png", "upstream outage"
        )
    )
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)

    assert got == b"from-redis"
    http.download_tile.assert_awaited_once()
    redis.get_basemap_tile.assert_awaited_once()
    s3.download_tile.assert_not_called()


@pytest.mark.asyncio
async def test_prod_unavailable_redis_miss_falls_through_to_s3():
    """Upstream outage + Redis miss → S3, with Redis write-back on hit."""
    redis = _make_redis()
    s3 = _make_s3(data=b"from-s3")
    http = MagicMock()
    http.download_tile = AsyncMock(
        side_effect=ProviderUnavailableError(
            "https://example.test/5/10/20.png", "upstream outage"
        )
    )
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)

    assert got == b"from-s3"
    s3.download_tile.assert_awaited_once()
    await _drain(reader)
    redis.store_basemap_tile.assert_awaited()


@pytest.mark.asyncio
async def test_prod_404_falls_through_to_redis():
    """A 404 from upstream (download_tile returns None) falls through to caches."""
    redis = _make_redis()
    redis.get_basemap_tile = AsyncMock(return_value=b"from-redis")
    s3 = _make_s3()
    http = _make_http(data=None)
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)

    assert got == b"from-redis"
    http.download_tile.assert_awaited_once()
    redis.get_basemap_tile.assert_awaited_once()
    s3.download_tile.assert_not_called()


@pytest.mark.asyncio
async def test_all_miss_returns_none():
    """Prod fail + Redis miss + S3 miss → None (route serves transparent PNG)."""
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=None)
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 2, 1, 1)
    assert got is None


@pytest.mark.asyncio
async def test_online_fallback_disabled_skips_prod_tier():
    """online_fallback=False means upstream is never contacted; reads come from caches."""
    redis = _make_redis()
    redis.get_basemap_tile = AsyncMock(return_value=b"from-redis")
    s3 = _make_s3()
    http = _make_http(data=b"should-not-be-used")
    reader = _make_reader(redis=redis, s3=s3, http=http, online_fallback=False)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-redis"
    http.download_tile.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_provider_skips_prod_and_returns_none_on_miss():
    """An unregistered provider can't be probed; reader falls through to caches."""
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = _make_http(data=b"never")
    reader = _make_reader(redis=redis, s3=s3, http=http, providers={})

    got = await reader.get_tile("missing", 2, 1, 1)
    assert got is None
    http.download_tile.assert_not_called()


@pytest.mark.asyncio
async def test_single_flight_dedupes_concurrent_prod_requests():
    """16 concurrent requests for the same tile must hit upstream exactly once."""
    redis = _make_redis()
    s3 = _make_s3()

    started = asyncio.Event()
    release = asyncio.Event()
    call_count = 0

    async def slow_prod(_url):
        nonlocal call_count
        call_count += 1
        started.set()
        await release.wait()
        return b"payload"

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=slow_prod)
    reader = _make_reader(redis=redis, s3=s3, http=http)

    tasks = [asyncio.create_task(reader.get_tile("fake", 4, 5, 9)) for _ in range(16)]
    await started.wait()
    release.set()
    results = await asyncio.gather(*tasks)

    assert call_count == 1
    assert all(r == b"payload" for r in results)


@pytest.mark.asyncio
async def test_single_flight_propagates_exception():
    """Non-failure exceptions from the prod path propagate to every waiter."""
    redis = _make_redis()
    s3 = _make_s3()

    async def raising_prod(_url):
        raise RuntimeError("boom")

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=raising_prod)
    reader = _make_reader(redis=redis, s3=s3, http=http)

    tasks = [asyncio.create_task(reader.get_tile("fake", 4, 5, 9)) for _ in range(4)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        assert isinstance(r, RuntimeError)
    assert not reader._inflight  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_redis_get_failure_fails_open_to_s3():
    """A Redis transport error during tier 2 must not break the chain."""
    redis = _make_redis()
    redis.get_basemap_tile = AsyncMock(side_effect=OSError("redis down"))
    s3 = _make_s3(data=b"from-s3")
    http = _make_http(data=None)  # prod miss → cache path
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-s3"


@pytest.mark.asyncio
async def test_deadline_returns_none_when_prod_hangs():
    """A hanging upstream call must be cut off at the deadline → None."""
    redis = _make_redis()
    s3 = _make_s3()

    async def hanging_prod(_url):
        await asyncio.sleep(5.0)
        return b"never"

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=hanging_prod)
    reader = _make_reader(redis=redis, s3=s3, http=http, request_deadline_seconds=0.2)

    loop = asyncio.get_event_loop()
    t0 = loop.time()
    got = await reader.get_tile("fake", 2, 1, 1)
    elapsed = loop.time() - t0

    assert got is None
    assert elapsed < 1.0, f"deadline not enforced: took {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_deadline_releases_single_flight_waiters():
    """When the leader trips the deadline, every waiter is released too."""
    redis = _make_redis()
    s3 = _make_s3()

    async def hanging_prod(_url):
        await asyncio.sleep(5.0)
        return b"never"

    http = MagicMock()
    http.download_tile = AsyncMock(side_effect=hanging_prod)
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
async def test_no_cache_mode_skips_redis_tier_entirely():
    """redis_cache_enabled=False: no GET, no write-through, even when prod hits."""
    redis = _make_redis()
    redis.get_basemap_tile = AsyncMock(return_value=b"should-not-be-read")
    s3 = _make_s3(data=b"from-s3")
    http = _make_http(data=None)  # prod miss, fall to S3
    reader = _make_reader(redis=redis, s3=s3, http=http, redis_cache_enabled=False)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-s3"
    await _drain(reader)
    redis.get_basemap_tile.assert_not_called()
    redis.store_basemap_tile.assert_not_called()


@pytest.mark.asyncio
async def test_no_cache_mode_uploads_s3_but_skips_redis_after_prod_hit():
    """no_cache + prod hit: write-through to S3, but never to Redis."""
    redis = _make_redis()
    s3 = _make_s3()
    http = _make_http(data=b"from-prod")
    reader = _make_reader(redis=redis, s3=s3, http=http, redis_cache_enabled=False)

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-prod"
    await _drain(reader)
    s3.upload_tile.assert_awaited_once()
    redis.store_basemap_tile.assert_not_called()


@pytest.mark.asyncio
async def test_relay_only_bypasses_redis_and_s3_on_hit():
    """relay_only: s3 is None, Redis untouched, relay bytes returned as-is."""
    redis = _make_redis()
    http = _make_http(data=b"from-relay")
    reader = _make_reader(
        redis=redis,
        http=http,
        redis_cache_enabled=False,
        s3_cache_enabled=False,
    )

    got = await reader.get_tile("fake", 5, 10, 20)
    assert got == b"from-relay"
    await _drain(reader)
    http.download_tile.assert_awaited_once()
    redis.get_basemap_tile.assert_not_called()
    redis.store_basemap_tile.assert_not_called()


@pytest.mark.asyncio
async def test_relay_only_returns_none_on_relay_miss():
    """relay_only: relay miss → None, no S3 write."""
    redis = _make_redis()
    http = _make_http(data=None)
    reader = _make_reader(
        redis=redis,
        http=http,
        redis_cache_enabled=False,
        s3_cache_enabled=False,
    )

    got = await reader.get_tile("fake", 2, 1, 1)
    assert got is None


@pytest.mark.asyncio
async def test_relay_only_rejects_s3_cache_enabled_without_s3_client():
    """Guardrail: s3_cache_enabled=True without an s3 client must raise."""
    with pytest.raises(ValueError, match="s3_cache_enabled"):
        BasemapTileReader(
            redis_client=_make_redis(),
            s3_client=None,
            http_client=_make_http(),
            providers={"fake": _make_provider()},
            tile_ttl=60,
            cache_concurrent=4,
            online_fallback=True,
            redis_cache_enabled=True,
            s3_cache_enabled=True,
        )


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


@pytest.mark.asyncio
async def test_prod_hit_succeeds_when_s3_is_down():
    """Prod returns bytes; S3 write-through fails — user must still get the tile."""
    from botocore.exceptions import EndpointConnectionError

    redis = _make_redis()
    s3 = _make_s3()
    s3.upload_tile = AsyncMock(
        side_effect=EndpointConnectionError(endpoint_url="http://127.0.0.1:1")
    )
    http = _make_http(data=b"from-prod")
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)

    assert got == b"from-prod"
    # Draining lets the failing S3 write run; it must be swallowed by
    # _run_throttled and not propagate.
    await _drain(reader)
    s3.upload_tile.assert_awaited_once()


@pytest.mark.asyncio
async def test_provider_unavailable_does_not_write_caches():
    """An UNAVAILABLE upstream must not poison the caches with empty data."""
    redis = _make_redis()
    s3 = _make_s3(data=None)
    http = MagicMock()
    http.download_tile = AsyncMock(
        side_effect=ProviderUnavailableError(
            "https://example.test/5/10/20.png", "upstream outage"
        )
    )
    reader = _make_reader(redis=redis, s3=s3, http=http)

    got = await reader.get_tile("fake", 5, 10, 20)

    assert got is None
    await _drain(reader)
    s3.upload_tile.assert_not_called()
    redis.store_basemap_tile.assert_not_called()
