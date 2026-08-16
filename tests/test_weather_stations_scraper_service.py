"""Unit tests for `WeatherStationsScraperService` (one-cycle behavior)."""

import json
from types import SimpleNamespace
from typing import Optional

import pytest

from clients.smn_api_client import SmnApiError
from clients.smn_registry_client import SmnRegistryBlockedError, SmnRegistryError
from datetime import timezone

from services.weather_stations_cache import parse_observed_at, to_float
from services.weather_stations_scraper_service import (
    WeatherStationsScraperService,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        (18.4, 18.4),
        (62, 62.0),
        ("18,4", 18.4),  # es-AR comma decimal
        ("1.013,2", 1013.2),  # es-AR grouped: dot thousands, comma decimal
        ("1013.2", 1013.2),
        ("-", None),
        ("", None),
        ("   ", None),
        ("N/A", None),
        (None, None),
        (True, None),  # bool is guarded out
    ],
)
def test_to_float_normalizes_es_ar_and_rejects_junk(value, expected):
    assert to_float(value) == expected


def test_normalize_wind_coerces_speed_and_deg():
    out = WeatherStationsScraperService._normalize_wind(
        {"direction": "Norte", "deg": "5", "speed": "8,2", "extra": "x"}
    )
    assert out == {"direction": "Norte", "deg": 5.0, "speed": 8.2}


def test_normalize_wind_passes_through_non_dict():
    assert WeatherStationsScraperService._normalize_wind(None) is None


@pytest.mark.parametrize(
    "value,expected_iso",
    [
        ("2026-05-17T13:00:00Z", "2026-05-17T13:00:00+00:00"),
        ("2026-05-17T13:00:00", "2026-05-17T13:00:00+00:00"),  # naive -> assume UTC
        ("2026-05-17T13:00:00-03:00", "2026-05-17T16:00:00+00:00"),  # offset -> UTC
    ],
)
def test_parse_observed_at_always_returns_aware_utc(value, expected_iso):
    """parse_observed_at must never return a naive datetime — a naive value
    reaching the freshness comparison raises TypeError and 500s the tileset."""
    parsed = parse_observed_at(value)
    assert parsed is not None
    assert parsed.tzinfo == timezone.utc
    assert parsed.isoformat() == expected_iso


def test_normalize_observed_at_stamps_naive_dates_with_z():
    """A naive SMN `date` is canonicalized to aware-UTC `...Z` at ingest."""
    out = WeatherStationsScraperService._normalize_observed_at("2026-05-17T13:00:00")
    assert out == "2026-05-17T13:00:00Z"


def test_normalize_observed_at_passes_through_unparseable():
    """Unparseable/missing values are left untouched (no data silently dropped)."""
    assert WeatherStationsScraperService._normalize_observed_at("garbage") == "garbage"
    assert WeatherStationsScraperService._normalize_observed_at(None) is None


class _FakeS3:
    def __init__(self):
        self.uploads: dict[str, tuple[bytes, str]] = {}
        self.downloads: dict[str, Optional[bytes]] = {}
        self.lifecycle_calls: list[tuple[int, str, str]] = []
        self._lifecycle_ok = True

    def disable_lifecycle(self):
        self._lifecycle_ok = False

    async def ensure_lifecycle_expiration(self, days, rule_id, prefix=""):
        self.lifecycle_calls.append((days, rule_id, prefix))
        return self._lifecycle_ok

    async def download_tile(self, key):
        # Seeded downloads win; otherwise fall back to what we've uploaded so
        # the write-through tileset recompute can read its own snapshot metas.
        if key in self.downloads:
            return self.downloads[key]
        up = self.uploads.get(key)
        return up[0] if up else None

    async def upload_tile(self, key, data, content_type="image/png"):
        self.uploads[key] = (data, content_type)

    async def list_object_keys(self, prefix):
        return [k for k in self.uploads if k.startswith(prefix)]


class _FakeRedis:
    def __init__(self, exc=None):
        self.store: dict[str, tuple[bytes, int]] = {}
        self.exc = exc

    async def cache_listing(self, key, data, ttl):
        if self.exc is not None:
            raise self.exc
        self.store[key] = (data, ttl)

    async def get_cached_listing(self, key):
        v = self.store.get(key)
        return v[0] if v else None


class _FakeSmn:
    def __init__(self, stations=None, exc=None):
        self.stations = stations or [
            {
                "station_id": 87344,
                "date": "2026-05-17T13:00:00Z",
                "temperature": 18.4,
                "feels_like": 17.9,
                "humidity": 62.0,
                "pressure": 1013.2,
                "visibility": 10.0,
                "weather": {"id": 1},
                "wind": {"direction": "Norte", "deg": 5, "speed": 8.2},
            }
        ]
        self.exc = exc
        self.calls = 0

    async def fetch_current_weather_stations(self):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.stations


class _FakeRegistry:
    def __init__(self, text="", exc=None):
        self.text = text
        self.exc = exc
        self.calls = 0

    async def fetch_registry_text(self):
        self.calls += 1
        if self.exc is not None:
            raise self.exc
        return self.text


_REGISTRY_TXT = (
    "NOMBRE                         PROVINCIA                              "
    "LATITUD          LONGITUD       ALTURA  NRO   NroOACI\n"
    "                                                                     "
    "[gr]    [min]    [gr]    [min]       [m]\n"
    "CORDOBA AERO                   CORDOBA                              "
    "-31      17       -64      12        495  87344 SACO\n"
)


def _settings(redis_cache_enabled=True, registry_fallback_enabled=True):
    return SimpleNamespace(
        weather_stations_registry_fallback_enabled=registry_fallback_enabled,
        weather_stations_registry_fallback_path="resources/estaciones_smn.txt",
        weather_stations_scrape_interval_seconds=300,
        weather_stations_scrape_lock_path="/tmp/test_ws_scrape.lock",
        weather_stations_s3_object_ttl_days=2,
        smn_api_base_url="https://api.test/v1",
        smn_stations_registry_url="http://reg.test/x",
        weather_stations_redis_cache_enabled=redis_cache_enabled,
        weather_stations_redis_latest_ttl_seconds=600,
        weather_stations_redis_tilesets_ttl_seconds=600,
        weather_stations_redis_registry_ttl_seconds=3600,
        weather_stations_redis_snapshot_ttl_seconds=3600,
        weather_stations_redis_animation_warm_buckets=24,
        weather_stations_series_hours=48,
        weather_stations_redis_series_ttl_seconds=3600,
    )


def _make_scraper(s3, smn, registry, redis_client=None, settings=None):
    return WeatherStationsScraperService(
        settings or _settings(), s3, smn, registry, redis_client=redis_client
    )


@pytest.mark.asyncio
async def test_cold_cycle_writes_registry_snapshot_and_meta():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()

    # Lifecycle applied lazily on the first cycle, scoped to the rolling
    # snapshots prefix so the registry/`latest` singletons are never swept.
    assert s3.lifecycle_calls == [
        (2, "weather-stations-expiration", "weather-stations/snapshots/")
    ]

    # Registry trio: stations.json + stations.meta.json (both with content_type=json).
    assert "weather-stations/stations.json" in s3.uploads
    assert "weather-stations/stations.meta.json" in s3.uploads
    reg_body, reg_ct = s3.uploads["weather-stations/stations.json"]
    assert reg_ct == "application/json"
    parsed = json.loads(reg_body)
    assert parsed["stations"][0]["station_id"] == 87344

    # Snapshot trio: snapshot + .meta.json + latest.json.
    snap_keys = [
        k
        for k in s3.uploads
        if k.startswith("weather-stations/snapshots/") and not k.endswith(".meta.json")
    ]
    meta_keys = [
        k
        for k in s3.uploads
        if k.endswith(".meta.json") and k.startswith("weather-stations/snapshots/")
    ]
    assert len(snap_keys) == 1
    assert len(meta_keys) == 1
    assert "weather-stations/latest.json" in s3.uploads

    snap_body = json.loads(s3.uploads[snap_keys[0]][0])
    assert snap_body["stations"][0]["station_id"] == 87344
    assert snap_body["stations"][0]["observed_at"] == "2026-05-17T13:00:00Z"
    # snapshot meta carries scraped_at + station_count.
    meta_body = json.loads(s3.uploads[meta_keys[0]][0])
    assert meta_body["station_count"] == 1


@pytest.mark.asyncio
async def test_snapshot_normalizes_es_ar_numeric_values():
    """Raw es-AR/placeholder SMN values are coerced to float/None at ingest so
    they can't break the Optional[float] models downstream (BUG-15)."""
    smn = _FakeSmn(
        stations=[
            {
                "station_id": 87344,
                "date": "2026-05-17T13:00:00Z",
                "temperature": "18,4",  # comma decimal
                "pressure": "-",  # placeholder
                "humidity": 62.0,
                "wind": {"direction": "Norte", "deg": 5, "speed": "8,2"},
            }
        ]
    )
    s3, reg = _FakeS3(), _FakeRegistry(_REGISTRY_TXT)
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()

    snap_keys = [
        k
        for k in s3.uploads
        if k.startswith("weather-stations/snapshots/") and not k.endswith(".meta.json")
    ]
    station = json.loads(s3.uploads[snap_keys[0]][0])["stations"][0]
    assert station["temperature"] == 18.4
    assert station["pressure"] is None
    assert station["humidity"] == 62.0
    assert station["wind"]["speed"] == 8.2
    assert station["wind"]["deg"] == 5.0


@pytest.mark.asyncio
async def test_lifecycle_failure_unlatches_for_retry_next_cycle():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    s3.disable_lifecycle()
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()
    await scraper._run_sync()
    # Both cycles tried (no latch).
    assert s3.lifecycle_calls == [
        (2, "weather-stations-expiration", "weather-stations/snapshots/"),
        (2, "weather-stations-expiration", "weather-stations/snapshots/"),
    ]


@pytest.mark.asyncio
async def test_unchanged_registry_skips_rewrite_on_subsequent_cycle():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()
    # Capture hash + clear the uploads to observe what cycle 2 writes.
    assert scraper._registry_hash is not None
    s3.uploads.clear()

    await scraper._run_sync()
    # Snapshot trio yes; registry should NOT be rewritten.
    assert "weather-stations/latest.json" in s3.uploads
    assert "weather-stations/stations.json" not in s3.uploads
    assert "weather-stations/stations.meta.json" not in s3.uploads


@pytest.mark.asyncio
async def test_registry_rewrites_when_hash_changes():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    scraper = _make_scraper(s3, smn, reg)
    await scraper._run_sync()
    s3.uploads.clear()

    # Mutate the registry text (one extra station) — different hash.
    reg.text = _REGISTRY_TXT + (
        "USHUAIA AERO                   TIERRA DEL FUEGO                     "
        "-54      50       -68      18         57  87938 SAWH\n"
    )

    await scraper._run_sync()
    assert "weather-stations/stations.json" in s3.uploads
    new_reg = json.loads(s3.uploads["weather-stations/stations.json"][0])
    assert len(new_reg["stations"]) == 2


@pytest.mark.asyncio
async def test_smn_failure_skips_the_cycle_without_uploads():
    s3, smn, reg = (
        _FakeS3(),
        _FakeSmn(exc=SmnApiError("upstream down")),
        _FakeRegistry(_REGISTRY_TXT),
    )
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()

    # Registry refresh still ran; observation snapshot did not.
    assert "weather-stations/stations.json" in s3.uploads
    assert "weather-stations/latest.json" not in s3.uploads


@pytest.mark.asyncio
async def test_registry_failure_does_not_block_snapshot():
    s3, smn, reg = (
        _FakeS3(),
        _FakeSmn(),
        _FakeRegistry(exc=SmnRegistryError("zip 404")),
    )
    # Fallback off, so this still asserts the original contract: a registry
    # failure is fail-soft and must not stop the observation pipeline.
    scraper = _make_scraper(
        s3, smn, reg, settings=_settings(registry_fallback_enabled=False)
    )

    await scraper._run_sync()

    # Registry skipped, but observations + latest still landed.
    assert "weather-stations/stations.json" not in s3.uploads
    assert "weather-stations/latest.json" in s3.uploads


@pytest.mark.asyncio
async def test_blocked_registry_seeds_from_bundle_when_nothing_stored():
    """Cold start behind Cloudflare: the bundled copy seeds the empty bucket."""
    s3, smn, reg = (
        _FakeS3(),
        _FakeSmn(),
        _FakeRegistry(exc=SmnRegistryBlockedError("cloudflare challenge")),
    )
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()

    registry = json.loads(s3.uploads["weather-stations/stations.json"][0])
    meta = json.loads(s3.uploads["weather-stations/stations.meta.json"][0])
    assert registry["source"] == "bundled"
    assert meta["source"] == "bundled"
    # Parsed from the real committed file, so this also guards against a
    # corrupted or HTML-challenge-page bundle being committed.
    assert meta["station_count"] >= 50
    assert registry["stations"][0]["station_id"] > 0


@pytest.mark.asyncio
async def test_blocked_registry_keeps_existing_copy_instead_of_bundle():
    """A stored registry wins over the bundle — the bundle never overwrites it."""
    s3, smn, reg = (
        _FakeS3(),
        _FakeSmn(),
        _FakeRegistry(exc=SmnRegistryBlockedError("cloudflare challenge")),
    )
    s3.downloads["weather-stations/stations.meta.json"] = json.dumps(
        {"source_hash": "deadbeef", "station_count": 3, "source": "remote"}
    ).encode("utf-8")
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()

    assert "weather-stations/stations.json" not in s3.uploads
    assert scraper._registry_hash == "deadbeef"


@pytest.mark.asyncio
async def test_bundle_seeding_can_be_disabled():
    s3, smn, reg = (
        _FakeS3(),
        _FakeSmn(),
        _FakeRegistry(exc=SmnRegistryBlockedError("cloudflare challenge")),
    )
    scraper = _make_scraper(
        s3, smn, reg, settings=_settings(registry_fallback_enabled=False)
    )

    await scraper._run_sync()

    assert "weather-stations/stations.json" not in s3.uploads


@pytest.mark.asyncio
async def test_live_registry_marked_remote_and_replaces_bundled_seed():
    """Once SMN is reachable again the live copy supersedes the seeded one."""
    s3, smn = _FakeS3(), _FakeSmn()
    blocked = _FakeRegistry(exc=SmnRegistryBlockedError("cloudflare challenge"))
    scraper = _make_scraper(s3, smn, blocked)
    await scraper._run_sync()
    assert json.loads(s3.uploads["weather-stations/stations.json"][0])["source"] == (
        "bundled"
    )

    # Same scraper, registry now reachable: different hash -> rewrite as remote.
    scraper._registry = _FakeRegistry(_REGISTRY_TXT)
    await scraper._run_sync()

    registry = json.loads(s3.uploads["weather-stations/stations.json"][0])
    assert registry["source"] == "remote"
    assert registry["source_url"] == "http://reg.test/x"


@pytest.mark.asyncio
async def test_bootstrap_loads_registry_hash_from_s3_meta():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    # Pre-seed the registry meta on S3 with the same hash the registry text
    # would produce. The scraper should NOT rewrite the registry on cycle 1.
    import hashlib

    seeded_hash = hashlib.sha256(_REGISTRY_TXT.encode("utf-8")).hexdigest()
    s3.downloads["weather-stations/stations.meta.json"] = json.dumps(
        {"source_hash": seeded_hash, "station_count": 1, "updated_at": "x"}
    ).encode()
    scraper = _make_scraper(s3, smn, reg)

    await scraper._run_sync()
    # Bootstrap saw seeded hash; registry refresh saw same hash → no rewrite.
    assert "weather-stations/stations.json" not in s3.uploads
    # But snapshot still landed.
    assert "weather-stations/latest.json" in s3.uploads


# ------------------------------------------------------------ Redis write-through


@pytest.mark.asyncio
async def test_write_through_warms_latest_tilesets_and_registry():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    redis = _FakeRedis()
    scraper = _make_scraper(s3, smn, reg, redis_client=redis)

    await scraper._run_sync()

    # latest: snapshot bytes with the latest TTL.
    assert "cache:ws:latest" in redis.store
    latest_bytes, latest_ttl = redis.store["cache:ws:latest"]
    assert json.loads(latest_bytes)["stations"][0]["station_id"] == 87344
    assert latest_ttl == 600

    # tilesets: assembled list with ISO scraped_at + the tilesets TTL.
    assert "cache:ws:tilesets" in redis.store
    tiles_bytes, tiles_ttl = redis.store["cache:ws:tilesets"]
    entries = json.loads(tiles_bytes)
    assert len(entries) == 1 and entries[0]["station_count"] == 1
    assert isinstance(entries[0]["scraped_at"], str)  # JSON-serialisable, not datetime
    assert tiles_ttl == 600

    # registry: written through with the registry TTL.
    assert "cache:ws:registry" in redis.store
    assert redis.store["cache:ws:registry"][1] == 3600


@pytest.mark.asyncio
async def test_write_through_warms_animation_window_snapshot_bodies():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    redis = _FakeRedis()
    scraper = _make_scraper(s3, smn, reg, redis_client=redis)

    await scraper._run_sync()

    # The cycle's snapshot body is pre-warmed under its S3-object key with the
    # snapshot TTL, so animation playback of recent buckets needs no S3 read.
    snap_keys = [k for k in redis.store if k.startswith("cache:ws:snap:")]
    assert len(snap_keys) == 1
    key = snap_keys[0]
    assert key.startswith("cache:ws:snap:weather-stations/snapshots/")
    body, ttl = redis.store[key]
    assert json.loads(body)["stations"][0]["station_id"] == 87344
    assert ttl == 3600


@pytest.mark.asyncio
async def test_unchanged_registry_still_rewarms_registry_cache():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    redis = _FakeRedis()
    scraper = _make_scraper(s3, smn, reg, redis_client=redis)

    await scraper._run_sync()
    redis.store.clear()  # keep S3 (stations.json persists across cycles); drop only Redis
    await scraper._run_sync()  # same registry hash → S3 not rewritten (see S3-level test)

    # cache:ws:registry is re-warmed each cycle (TTL refresh) from the persisted
    # stations.json, alongside the other always-warm keys — even when unchanged.
    assert "cache:ws:registry" in redis.store
    assert redis.store["cache:ws:registry"][1] == 3600
    assert "cache:ws:latest" in redis.store
    assert "cache:ws:tilesets" in redis.store


@pytest.mark.asyncio
async def test_missing_registry_in_s3_triggers_reupload_self_heal():
    """A vanished `stations.json` must not strand `/stations` forever.

    Reproduces the production failure: the bucket lifecycle swept the static
    registry after its TTL while the long-running worker kept the unchanged
    upstream hash in memory, so the hash-match no-op never recreated it. The
    warm step now detects the absence and forces a re-upload next cycle.
    """
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    redis = _FakeRedis()
    scraper = _make_scraper(s3, smn, reg, redis_client=redis)

    await scraper._run_sync()  # cycle 1: writes stations.json, caches the hash
    assert "weather-stations/stations.json" in s3.uploads
    assert scraper._registry_hash is not None

    # Simulate the lifecycle sweep: the registry objects disappear from S3 while
    # the in-memory hash (worker still up) still matches the unchanged upstream.
    del s3.uploads["weather-stations/stations.json"]
    del s3.uploads["weather-stations/stations.meta.json"]

    # cycle 2: refresh no-ops on the matching hash; the warm read finds the
    # object gone and invalidates the hash so the next cycle re-uploads.
    await scraper._run_sync()
    assert scraper._registry_hash is None
    assert "weather-stations/stations.json" not in s3.uploads

    # cycle 3: invalidated hash → registry rewritten, self-healing /stations.
    await scraper._run_sync()
    assert "weather-stations/stations.json" in s3.uploads
    assert "weather-stations/stations.meta.json" in s3.uploads


@pytest.mark.asyncio
async def test_registry_warmed_from_existing_s3_when_unchanged():
    """Boot case: data already in S3, registry unchanged → still warm the cache."""
    import hashlib

    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    seeded_hash = hashlib.sha256(_REGISTRY_TXT.encode("utf-8")).hexdigest()
    s3.downloads["weather-stations/stations.meta.json"] = json.dumps(
        {"source_hash": seeded_hash, "station_count": 1, "updated_at": "x"}
    ).encode()
    registry_body = json.dumps(
        {"fetched_at": "x", "source_url": "y", "stations": [{"station_id": 87344}]}
    ).encode()
    s3.downloads["weather-stations/stations.json"] = registry_body
    redis = _FakeRedis()
    scraper = _make_scraper(s3, smn, reg, redis_client=redis)

    await scraper._run_sync()

    # Hash matched → stations.json NOT re-uploaded...
    assert "weather-stations/stations.json" not in s3.uploads
    # ...yet cache:ws:registry is warmed from the existing S3 object.
    assert redis.store["cache:ws:registry"] == (registry_body, 3600)


@pytest.mark.asyncio
async def test_cache_disabled_skips_write_through():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    redis = _FakeRedis()
    scraper = _make_scraper(
        s3, smn, reg, redis_client=redis, settings=_settings(redis_cache_enabled=False)
    )

    await scraper._run_sync()

    assert redis.store == {}
    # S3 still written — the cache flag only gates Redis.
    assert "weather-stations/latest.json" in s3.uploads


@pytest.mark.asyncio
async def test_redis_error_does_not_abort_scrape():
    s3, smn, reg = _FakeS3(), _FakeSmn(), _FakeRegistry(_REGISTRY_TXT)
    redis = _FakeRedis(exc=RuntimeError("redis down"))
    scraper = _make_scraper(s3, smn, reg, redis_client=redis)

    await scraper._run_sync()  # must not raise

    # S3 writes still landed despite the Redis outage.
    assert "weather-stations/latest.json" in s3.uploads
    assert "weather-stations/stations.json" in s3.uploads
    assert redis.store == {}
