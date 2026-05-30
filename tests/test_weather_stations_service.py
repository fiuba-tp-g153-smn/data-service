"""Unit tests for `WeatherStationsService` (S3-direct reads + LIST cache)."""

import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import pytest

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
        ("20260517T1400Z", 1),  # 14:00 bucket picks the LATEST (14:05) which has count=1
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
            "stations": [{"station_id": 1, "name": "A", "province": "P",
                          "latitude": -30.0, "longitude": -60.0,
                          "altitude_meters": 50, "oaci_code": None}],
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
