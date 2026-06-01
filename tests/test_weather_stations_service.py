"""Unit tests for `WeatherStationsService` (S3-direct reads + LIST cache)."""

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import pytest

from services.weather_stations_cache import (
    extract_station_series,
    pivot_station_series,
    series_key,
)
from services.weather_stations_service import (
    TilesetIdFormatError,
    WeatherStationsNotConfiguredError,
    WeatherStationsService,
)


class _FakeS3:
    def __init__(self, objects: Optional[dict[str, bytes]] = None):
        self.objects: dict[str, bytes] = dict(objects or {})
        self.list_calls = 0
        self.download_calls: list[str] = []

    async def download_tile(self, key):
        self.download_calls.append(key)
        return self.objects.get(key)

    async def list_object_keys(self, prefix):
        self.list_calls += 1
        return [k for k in self.objects if k.startswith(prefix)]


def _snap_key(ts: datetime) -> str:
    return (
        f"weather-stations/snapshots/{ts.strftime('%Y/%m/%d/%H')}/"
        f"{ts.strftime('%Y%m%dT%H%M%SZ')}.json"
    )


def _meta_key(snap_key: str) -> str:
    return snap_key[: -len(".json")] + ".meta.json"


def _snap_body(scraped_at: datetime, n_stations: int = 1) -> bytes:
    return json.dumps(
        {
            "scraped_at": scraped_at.isoformat().replace("+00:00", "Z"),
            "source_url": "x",
            "stations": [{"station_id": i} for i in range(n_stations)],
        }
    ).encode()


def _meta_body(scraped_at: datetime, station_count: int) -> bytes:
    return json.dumps(
        {
            "scraped_at": scraped_at.isoformat().replace("+00:00", "Z"),
            "station_count": station_count,
        }
    ).encode()


def _new_service(objects=None, list_cache_ttl: float = 30.0):
    svc = WeatherStationsService()
    svc.configure(_FakeS3(objects), list_cache_ttl=list_cache_ttl)
    return svc


# --------------------------------------------------------------------- /latest


@pytest.mark.asyncio
async def test_get_latest_returns_parsed_snapshot():
    ts = datetime(2026, 5, 17, 14, 5, 0, tzinfo=timezone.utc)
    svc = _new_service({"weather-stations/latest.json": _snap_body(ts, 3)})
    out = await svc.get_latest_snapshot()
    assert out is not None
    assert out["scraped_at"] == "2026-05-17T14:05:00Z"
    assert len(out["stations"]) == 3


@pytest.mark.asyncio
async def test_get_latest_returns_none_on_cold_boot():
    svc = _new_service({})
    assert await svc.get_latest_snapshot() is None


@pytest.mark.asyncio
async def test_get_latest_raises_when_not_configured():
    svc = WeatherStationsService()  # no .configure() call
    with pytest.raises(WeatherStationsNotConfiguredError):
        await svc.get_latest_snapshot()


# ------------------------------------------------------------------- /tilesets


@pytest.mark.asyncio
async def test_tilesets_groups_by_hour_and_pairs_counts_correctly():
    """Regression: counts must align with sorted-by-time tilesets, not dict order."""
    a = datetime(2026, 5, 17, 14, 5, 0, tzinfo=timezone.utc)  # bucket 14
    b = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)  # bucket 14 (older)
    c = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)  # bucket 12

    objs = {
        _snap_key(a): _snap_body(a),
        _meta_key(_snap_key(a)): _meta_body(a, 1),
        _snap_key(b): _snap_body(b),
        _meta_key(_snap_key(b)): _meta_body(b, 2),
        _snap_key(c): _snap_body(c),
        _meta_key(_snap_key(c)): _meta_body(c, 3),
    }
    svc = _new_service(objs)
    entries = await svc.list_tilesets()
    assert [(e["tileset_id"], e["station_count"]) for e in entries] == [
        ("20260517T1200Z", 3),  # only snapshot at 12:00 has count=3
        (
            "20260517T1400Z",
            1,
        ),  # 14:00 bucket picks the LATEST (14:05) which has count=1
    ]


@pytest.mark.asyncio
async def test_tilesets_empty_when_no_snapshots():
    svc = _new_service({})
    assert await svc.list_tilesets() == []


@pytest.mark.asyncio
async def test_tilesets_handles_missing_meta_with_zero_count():
    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    svc = _new_service({_snap_key(ts): _snap_body(ts, 7)})  # no meta key
    [entry] = await svc.list_tilesets()
    assert entry["tileset_id"] == "20260517T1400Z"
    assert entry["station_count"] == 0


# ------------------------------------------------ /{tilesetId}?N= window picking


def _seed_window(svc_factory):
    """Build a service with three snapshots: 14:05, 14:00, 12:00."""
    now = datetime(2026, 5, 17, 14, 5, 0, tzinfo=timezone.utc)
    prev = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    old = datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc)
    objs = {
        _snap_key(now): _snap_body(now, 1),
        _snap_key(prev): _snap_body(prev, 2),
        _snap_key(old): _snap_body(old, 3),
    }
    return svc_factory(objs), now, prev, old


@pytest.mark.asyncio
async def test_tileset_n_zero_picks_exact_hour_or_404():
    svc, now, prev, old = _seed_window(_new_service)

    # N=0 at 14:00 picks the latest snapshot with ts <= 14:00 AND ts >= 14:00 -> exactly 14:00.
    snap = await svc.get_snapshot_for_tileset("20260517T1400Z", 0.0)
    assert snap["scraped_at"] == "2026-05-17T14:00:00Z"

    # N=0 at 13:00 -> nothing matches -> 404
    assert await svc.get_snapshot_for_tileset("20260517T1300Z", 0.0) is None


@pytest.mark.asyncio
async def test_tileset_n_picks_latest_within_window():
    svc, *_ = _seed_window(_new_service)

    # N=3 at 14:00 -> window [11:00, 14:00], latest within = 14:00.
    snap = await svc.get_snapshot_for_tileset("20260517T1400Z", 3.0)
    assert snap["scraped_at"] == "2026-05-17T14:00:00Z"

    # N=3 at 13:30 -> window [10:30, 13:30], latest within = 12:00.
    snap = await svc.get_snapshot_for_tileset("20260517T1330Z", 3.0)
    assert snap["scraped_at"] == "2026-05-17T12:00:00Z"


@pytest.mark.asyncio
async def test_tileset_malformed_id_raises():
    svc, *_ = _seed_window(_new_service)
    with pytest.raises(TilesetIdFormatError):
        await svc.get_snapshot_for_tileset("not-a-tileset", 0.0)
    with pytest.raises(TilesetIdFormatError):
        await svc.get_snapshot_for_tileset("20260517T1400", 0.0)  # no trailing Z


# --------------------------------------------------------------- LIST cache TTL


@pytest.mark.asyncio
async def test_list_cache_collapses_a_burst_to_one_s3_call():
    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    objs = {
        _snap_key(ts): _snap_body(ts),
        _meta_key(_snap_key(ts)): _meta_body(ts, 1),
    }
    svc = WeatherStationsService()
    fake_s3 = _FakeS3(objs)
    svc.configure(fake_s3, list_cache_ttl=60.0)

    await asyncio.gather(*(svc.list_tilesets() for _ in range(10)))
    # 10 callers should produce exactly 1 S3 LIST under the snapshots prefix.
    assert fake_s3.list_calls == 1


@pytest.mark.asyncio
async def test_list_cache_expires_after_ttl():
    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    objs = {
        _snap_key(ts): _snap_body(ts),
        _meta_key(_snap_key(ts)): _meta_body(ts, 1),
    }
    svc = WeatherStationsService()
    fake_s3 = _FakeS3(objs)
    # Tiny TTL so we can wait it out cheaply.
    svc.configure(fake_s3, list_cache_ttl=0.0)

    await svc.list_tilesets()
    await svc.list_tilesets()
    # With TTL=0 every call is a fresh LIST.
    assert fake_s3.list_calls == 2


# ----------------------------------------------------------------- /stations


@pytest.mark.asyncio
async def test_get_registry_returns_parsed():
    body = json.dumps(
        {
            "fetched_at": "2026-05-17T14:00:00Z",
            "source_url": "x",
            "stations": [
                {
                    "station_id": 1,
                    "name": "A",
                    "province": "P",
                    "latitude": -30.0,
                    "longitude": -60.0,
                    "altitude_meters": 50,
                    "oaci_code": None,
                }
            ],
        }
    ).encode()
    svc = _new_service({"weather-stations/stations.json": body})
    out = await svc.get_stations_registry()
    assert out is not None
    assert out["stations"][0]["station_id"] == 1


@pytest.mark.asyncio
async def test_get_registry_returns_none_on_miss():
    svc = _new_service({})
    assert await svc.get_stations_registry() is None


# --------------------------------------------------------------- Redis cache path


class _FakeRedis:
    def __init__(self, store=None, raise_on_get=False):
        self.store: dict[str, bytes] = dict(store or {})
        self.raise_on_get = raise_on_get
        self.sets: list[tuple[str, bytes, int]] = []

    async def get_cached_listing(self, key):
        if self.raise_on_get:
            raise RuntimeError("redis down")
        return self.store.get(key)

    async def cache_listing(self, key, data, ttl):
        self.store[key] = data
        self.sets.append((key, data, ttl))


def _svc_with_redis(redis, objects=None, **ttls):
    svc = WeatherStationsService()
    svc.configure(
        _FakeS3(objects),
        list_cache_ttl=30.0,
        redis_client=redis,
        latest_ttl=ttls.get("latest_ttl", 600),
        tilesets_ttl=ttls.get("tilesets_ttl", 600),
        snapshot_ttl=ttls.get("snapshot_ttl", 3600),
        registry_ttl=ttls.get("registry_ttl", 3600),
    )
    return svc, svc._s3


async def _drain_background_tasks():
    """Yield so fire-and-forget write-back tasks run."""
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_latest_served_from_cache_without_touching_s3():
    ts = datetime(2026, 5, 17, 14, 5, 0, tzinfo=timezone.utc)
    redis = _FakeRedis({"cache:ws:latest": _snap_body(ts, 3)})
    svc, s3 = _svc_with_redis(redis, {})
    out = await svc.get_latest_snapshot()
    assert out["scraped_at"] == "2026-05-17T14:05:00Z"
    assert s3.download_calls == []  # pure Redis hit


@pytest.mark.asyncio
async def test_latest_miss_reads_s3_and_writes_back_with_ttl():
    ts = datetime(2026, 5, 17, 14, 5, 0, tzinfo=timezone.utc)
    redis = _FakeRedis({})
    svc, s3 = _svc_with_redis(
        redis, {"weather-stations/latest.json": _snap_body(ts, 2)}
    )
    out = await svc.get_latest_snapshot()
    assert out is not None and len(out["stations"]) == 2
    assert s3.download_calls == ["weather-stations/latest.json"]
    await _drain_background_tasks()
    assert "cache:ws:latest" in redis.store
    assert any(k == "cache:ws:latest" and ttl == 600 for k, _, ttl in redis.sets)


@pytest.mark.asyncio
async def test_redis_read_error_falls_back_to_s3():
    ts = datetime(2026, 5, 17, 14, 5, 0, tzinfo=timezone.utc)
    redis = _FakeRedis({}, raise_on_get=True)
    svc, s3 = _svc_with_redis(redis, {"weather-stations/latest.json": _snap_body(ts)})
    out = await svc.get_latest_snapshot()  # error treated as a miss
    assert out is not None
    assert s3.download_calls == ["weather-stations/latest.json"]


@pytest.mark.asyncio
async def test_tilesets_cache_roundtrips_iso_and_validates_as_response():
    from models.weather_stations import TilesetsResponse

    entries = [
        {
            "tileset_id": "20260517T1400Z",
            "scraped_at": "2026-05-17T14:05:00Z",
            "station_count": 1,
        }
    ]
    redis = _FakeRedis({"cache:ws:tilesets": json.dumps(entries).encode()})
    svc, s3 = _svc_with_redis(redis, {})
    out = await svc.list_tilesets()
    assert out == entries
    assert s3.list_calls == 0  # served from Redis, no S3 LIST
    # The route validates the cached list unchanged.
    model = TilesetsResponse.model_validate({"tilesets": out})
    assert model.tilesets[0].tileset_id == "20260517T1400Z"


@pytest.mark.asyncio
async def test_tilesets_miss_caches_iso_string_not_datetime():
    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    objs = {_snap_key(ts): _snap_body(ts), _meta_key(_snap_key(ts)): _meta_body(ts, 1)}
    redis = _FakeRedis({})
    svc, _ = _svc_with_redis(redis, objs)
    out = await svc.list_tilesets()
    assert out[0]["scraped_at"] == "2026-05-17T14:00:00Z"  # ISO string
    await _drain_background_tasks()
    cached = json.loads(redis.store["cache:ws:tilesets"])
    assert cached[0]["scraped_at"] == "2026-05-17T14:00:00Z"


@pytest.mark.asyncio
async def test_tileset_snapshot_body_served_from_cache_without_s3_get():
    from services.weather_stations_cache import snap_body_key

    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    s3_key = _snap_key(ts)
    # S3 holds the object (so resolution finds the key), but a DIFFERENT body is
    # seeded in Redis to prove the response came from the cache, not from S3.
    redis = _FakeRedis({snap_body_key(s3_key): _snap_body(ts, 5)})
    svc, s3 = _svc_with_redis(redis, {s3_key: _snap_body(ts, 1)})
    out = await svc.get_snapshot_for_tileset("20260517T1400Z", 0.0)
    assert out is not None and len(out["stations"]) == 5  # from Redis body, not S3
    assert s3.download_calls == []  # no body GET (a resolve LIST is still allowed)


@pytest.mark.asyncio
async def test_tileset_snapshot_miss_caches_body_under_snap_key():
    from services.weather_stations_cache import snap_body_key

    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    s3_key = _snap_key(ts)
    redis = _FakeRedis({})
    svc, s3 = _svc_with_redis(redis, {s3_key: _snap_body(ts, 2)}, snapshot_ttl=3600)
    out = await svc.get_snapshot_for_tileset("20260517T1400Z", 0.0)
    assert out is not None and len(out["stations"]) == 2
    assert s3.download_calls == [s3_key]
    await _drain_background_tasks()
    assert snap_body_key(s3_key) in redis.store
    assert any(k == snap_body_key(s3_key) and ttl == 3600 for k, _, ttl in redis.sets)


@pytest.mark.asyncio
async def test_tileset_malformed_id_raises_even_with_cache():
    redis = _FakeRedis({})
    svc, _ = _svc_with_redis(redis, {})
    with pytest.raises(TilesetIdFormatError):
        await svc.get_snapshot_for_tileset("not-a-tileset", 0.0)


def test_snap_body_key_shape():
    from services.weather_stations_cache import snap_body_key

    key = "weather-stations/snapshots/2026/05/17/14/20260517T140000Z.json"
    assert snap_body_key(key) == f"cache:ws:snap:{key}"


# ----------------------------------------------- per-station series pivot (pure)


def _series_body(stations: list[dict]) -> dict:
    return {
        "scraped_at": "2026-05-17T14:00:00Z",
        "source_url": "x",
        "stations": stations,
    }


def _obs_body(scraped_at: datetime, observations: list[dict]) -> bytes:
    return json.dumps(
        {
            "scraped_at": scraped_at.isoformat().replace("+00:00", "Z"),
            "source_url": "x",
            "stations": observations,
        }
    ).encode()


def test_extract_station_series_dedupes_sorts_and_flattens_wind():
    bodies = [
        _series_body(
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T14:00:00Z",
                    "temperature": 18.0,
                    "wind": {"speed": 8.2, "deg": 5, "direction": "Norte"},
                },
                {"station_id": 2, "observed_at": "2026-05-17T14:00:00Z"},
            ]
        ),
        # Same observed_at for station 1 repeats in an adjacent bucket -> deduped.
        _series_body(
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T14:00:00Z",
                    "temperature": 18.0,
                }
            ]
        ),
        _series_body(
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T13:00:00Z",
                    "temperature": 17.0,
                }
            ]
        ),
    ]
    series = extract_station_series(bodies, 1)
    # Deduped to two distinct readings, sorted oldest -> newest.
    assert [p["observed_at"] for p in series] == [
        "2026-05-17T13:00:00Z",
        "2026-05-17T14:00:00Z",
    ]
    assert series[0]["temperature"] == 17.0
    assert series[1]["wind_speed"] == 8.2
    assert series[1]["wind_deg"] == 5
    assert series[1]["wind_direction"] == "Norte"


def test_extract_station_series_missing_station_is_empty():
    bodies = [_series_body([{"station_id": 9, "observed_at": "2026-05-17T14:00:00Z"}])]
    assert extract_station_series(bodies, 1) == []


def test_extract_station_series_skips_null_observed_at():
    bodies = [
        _series_body([{"station_id": 1, "observed_at": None, "temperature": 5.0}])
    ]
    assert extract_station_series(bodies, 1) == []


def test_pivot_station_series_groups_each_station_sorted():
    bodies = [
        _series_body(
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T14:00:00Z",
                    "temperature": 18.0,
                },
                {
                    "station_id": 2,
                    "observed_at": "2026-05-17T14:00:00Z",
                    "temperature": 10.0,
                },
            ]
        ),
        _series_body(
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T13:00:00Z",
                    "temperature": 17.0,
                }
            ]
        ),
    ]
    pivoted = pivot_station_series(bodies)
    assert set(pivoted) == {1, 2}
    assert [p["observed_at"] for p in pivoted[1]] == [
        "2026-05-17T13:00:00Z",
        "2026-05-17T14:00:00Z",
    ]
    assert [p["temperature"] for p in pivoted[2]] == [10.0]


# ------------------------------------------------- get_station_series (service)


@pytest.mark.asyncio
async def test_station_series_served_from_cache_without_s3():
    points = [
        {
            "observed_at": "2026-05-17T14:00:00Z",
            "temperature": 18.0,
            "feels_like": None,
            "humidity": None,
            "pressure": None,
            "visibility": None,
            "wind_speed": None,
            "wind_deg": None,
            "wind_direction": None,
        }
    ]
    redis = _FakeRedis(
        {
            series_key(1): json.dumps(points).encode(),
            "cache:ws:registry": json.dumps(
                {"stations": [{"station_id": 1, "name": "A", "province": "P"}]}
            ).encode(),
        }
    )
    svc, s3 = _svc_with_redis(redis, {})
    out = await svc.get_station_series(1, 48)
    assert out["station_id"] == 1
    assert out["station_name"] == "A"
    assert out["province"] == "P"
    assert out["points"] == points
    assert out["latest"] == points[-1]
    assert s3.download_calls == []  # pure Redis hit (series + registry)


@pytest.mark.asyncio
async def test_station_series_cold_miss_pivots_from_s3_and_writes_back():
    h14 = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    h13 = datetime(2026, 5, 17, 13, 0, 0, tzinfo=timezone.utc)
    objs = {
        _snap_key(h14): _obs_body(
            h14,
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T14:00:00Z",
                    "temperature": 18.0,
                    "wind": {"speed": 8.0, "deg": 5, "direction": "N"},
                }
            ],
        ),
        _snap_key(h13): _obs_body(
            h13,
            [
                {
                    "station_id": 1,
                    "observed_at": "2026-05-17T13:00:00Z",
                    "temperature": 17.0,
                }
            ],
        ),
    }
    redis = _FakeRedis({})
    svc, _ = _svc_with_redis(redis, objs)
    out = await svc.get_station_series(1, 48)
    assert [p["observed_at"] for p in out["points"]] == [
        "2026-05-17T13:00:00Z",
        "2026-05-17T14:00:00Z",
    ]
    assert out["latest"]["temperature"] == 18.0
    assert out["points"][1]["wind_speed"] == 8.0
    await _drain_background_tasks()
    assert series_key(1) in redis.store  # rebuilt series written back


@pytest.mark.asyncio
async def test_station_series_unknown_station_returns_empty_points():
    h14 = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    objs = {
        _snap_key(h14): _obs_body(
            h14, [{"station_id": 1, "observed_at": "2026-05-17T14:00:00Z"}]
        )
    }
    redis = _FakeRedis({})
    svc, _ = _svc_with_redis(redis, objs)
    out = await svc.get_station_series(999, 48)
    assert out["station_id"] == 999
    assert out["points"] == []
    assert out["latest"] is None
