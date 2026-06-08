"""Integration tests for the /weather-stations/* routes via FastAPI TestClient."""

import json
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from clients.weather_stations_keystore import WeatherStationsKeystore
from dependencies import (
    get_weather_stations_keystore,
    get_weather_stations_service,
    set_weather_stations_keystore,
    settings,
)
from routes.weather_stations import (
    admin_router as weather_stations_admin_router,
    router as weather_stations_router,
)
from services.weather_stations_service import WeatherStationsService


class _FakeS3:
    """Shared in-memory S3 stub for both the read service and the keystore."""

    def __init__(self, objects):
        self.objects: dict[str, bytes] = dict(objects)

    async def upload_tile(self, key, data, content_type="application/octet-stream"):
        del content_type
        self.objects[key] = data

    async def download_tile(self, key):
        return self.objects.get(key)

    async def object_exists(self, key):
        return key in self.objects

    async def list_object_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]

    async def delete_object(self, key):
        return self.objects.pop(key, None) is not None


def _snap_key(ts: datetime) -> str:
    return (
        f"weather-stations/snapshots/{ts.strftime('%Y/%m/%d/%H')}/"
        f"{ts.strftime('%Y%m%dT%H%M%SZ')}.json"
    )


def _snap_body(ts: datetime, n: int = 1) -> bytes:
    return json.dumps(
        {
            "scraped_at": ts.isoformat().replace("+00:00", "Z"),
            "source_url": "x",
            "stations": [
                {
                    "station_id": i,
                    "observed_at": ts.isoformat().replace("+00:00", "Z"),
                }
                for i in range(n)
            ],
        }
    ).encode()


def _meta_body(ts: datetime, count: int) -> bytes:
    return json.dumps(
        {
            "scraped_at": ts.isoformat().replace("+00:00", "Z"),
            "station_count": count,
        }
    ).encode()


@pytest_asyncio.fixture
async def app_and_keystore():
    """
    Build a minimal FastAPI app wired to the weather-stations router, a
    fresh S3-backed keystore, and a populated read service. Both share the
    same in-memory `_FakeS3` (the keystore writes under `keys/`, the read
    service under `weather-stations/` — no collision).
    """
    ts = datetime(2026, 5, 17, 14, 0, 0, tzinfo=timezone.utc)
    s3 = _FakeS3(
        {
            "weather-stations/latest.json": _snap_body(ts, 2),
            _snap_key(ts): _snap_body(ts, 2),
            _snap_key(ts)[: -len(".json")] + ".meta.json": _meta_body(ts, 2),
            "weather-stations/stations.json": json.dumps(
                {
                    "fetched_at": ts.isoformat().replace("+00:00", "Z"),
                    "source_url": "x",
                    "stations": [
                        {
                            "station_id": 0,
                            "name": "A",
                            "province": "P",
                            "latitude": -30.0,
                            "longitude": -60.0,
                            "altitude_meters": 0,
                            "oaci_code": None,
                        }
                    ],
                }
            ).encode(),
        }
    )

    keystore = WeatherStationsKeystore(s3)
    await keystore.connect()
    set_weather_stations_keystore(keystore)

    svc = WeatherStationsService()
    svc.configure(s3, list_cache_ttl=30.0)

    app = FastAPI()
    app.dependency_overrides[get_weather_stations_keystore] = lambda: keystore
    app.dependency_overrides[get_weather_stations_service] = lambda: svc
    app.include_router(weather_stations_router)
    app.include_router(weather_stations_admin_router)

    # Capture/restore settings flags the tests toggle.
    original_auth_enabled = settings.weather_stations_api_key_auth_enabled
    original_admin_pw = settings.weather_stations_admin_password
    settings.weather_stations_api_key_auth_enabled = True
    settings.weather_stations_admin_password = "admin-pw"
    try:
        yield app, keystore
    finally:
        settings.weather_stations_api_key_auth_enabled = original_auth_enabled
        settings.weather_stations_admin_password = original_admin_pw
        await keystore.close()


# ---------------------------------------------------------------- auth gating


@pytest.mark.asyncio
async def test_read_endpoints_reject_missing_api_key(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    for path in (
        "/weather-stations/latest",
        "/weather-stations/tilesets",
        "/weather-stations/stations",
        "/weather-stations/20260517T1400Z",
        "/weather-stations/station/0/series",
    ):
        resp = client.get(path)
        assert resp.status_code == 401, f"{path} should require X-API-Key"


@pytest.mark.asyncio
async def test_read_endpoints_reject_invalid_api_key(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    resp = client.get(
        "/weather-stations/latest", headers={"X-API-Key": "not-a-real-key"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_admin_endpoints_reject_missing_password(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    resp = client.get("/weather-stations/admin/keys")
    assert resp.status_code == 401
    resp = client.post("/weather-stations/admin/keys", json={"label": "x"})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_admin_endpoints_reject_wrong_password(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    resp = client.get(
        "/weather-stations/admin/keys",
        headers={"X-Admin-Password": "wrong"},
    )
    assert resp.status_code == 401


# ------------------------------------------------------------------ read happy


@pytest.mark.asyncio
async def test_latest_returns_snapshot_with_cache_control(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)

    resp = client.get("/weather-stations/latest", headers={"X-API-Key": created.secret})
    assert resp.status_code == 200
    body = resp.json()
    assert body["scraped_at"] == "2026-05-17T14:00:00Z"
    assert len(body["stations"]) == 2
    assert (
        resp.headers["cache-control"] == settings.weather_stations_cache_control_latest
    )


@pytest.mark.asyncio
async def test_tilesets_returns_buckets(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)
    resp = client.get(
        "/weather-stations/tilesets", headers={"X-API-Key": created.secret}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["tilesets"]) == 1
    assert body["tilesets"][0]["tileset_id"] == "20260517T1400Z"
    assert body["tilesets"][0]["station_count"] == 2
    assert (
        resp.headers["cache-control"]
        == settings.weather_stations_cache_control_tilesets
    )


@pytest.mark.asyncio
async def test_tileset_lookup_hits_and_misses(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)

    # Bucket hit: the 14:00 representative, each station flagged is_current
    # (both observed at 14:00, so current even at grace_period_hours=0).
    resp = client.get(
        "/weather-stations/20260517T1400Z?grace_period_hours=0",
        headers={"X-API-Key": created.secret},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["scraped_at"] == "2026-05-17T14:00:00Z"
    assert [s["is_current"] for s in body["stations"]] == [True, True]
    assert (
        resp.headers["cache-control"]
        == settings.weather_stations_cache_control_snapshot
    )

    # Empty bucket (no snapshot in [11:00, 12:00)) -> 404.
    resp = client.get(
        "/weather-stations/20260517T1100Z?grace_period_hours=0",
        headers={"X-API-Key": created.secret},
    )
    assert resp.status_code == 404

    # Malformed tilesetId -> 400.
    resp = client.get("/weather-stations/bogus", headers={"X-API-Key": created.secret})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_station_series_returns_bundled_history(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)
    resp = client.get(
        "/weather-stations/station/0/series?hours=48",
        headers={"X-API-Key": created.secret},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["station_id"] == 0
    # Name/province come bundled from the registry — no companion request needed.
    assert body["station_name"] == "A"
    assert body["province"] == "P"
    assert len(body["points"]) == 1
    assert body["points"][0]["observed_at"] == "2026-05-17T14:00:00Z"
    assert body["latest"]["observed_at"] == "2026-05-17T14:00:00Z"
    assert (
        resp.headers["cache-control"] == settings.weather_stations_cache_control_series
    )


@pytest.mark.asyncio
async def test_station_series_unknown_station_is_empty(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)
    resp = client.get(
        "/weather-stations/station/999/series",
        headers={"X-API-Key": created.secret},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["station_id"] == 999
    assert body["points"] == []
    assert body["latest"] is None


@pytest.mark.asyncio
async def test_registry_returns_stations(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)
    resp = client.get(
        "/weather-stations/stations", headers={"X-API-Key": created.secret}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["stations"][0]["station_id"] == 0
    assert (
        resp.headers["cache-control"]
        == settings.weather_stations_cache_control_registry
    )


# ---------------------------------------------------------------- admin happy


@pytest.mark.asyncio
async def test_admin_create_list_and_revoke_roundtrip(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    headers = {"X-Admin-Password": "admin-pw"}

    # Create.
    resp = client.post(
        "/weather-stations/admin/keys", json={"label": "ci-key"}, headers=headers
    )
    assert resp.status_code == 201
    created = resp.json()
    assert created["label"] == "ci-key"
    assert created["secret"]
    key_id = created["key_id"]
    secret = created["secret"]

    # Issued key works for read endpoints.
    resp = client.get("/weather-stations/latest", headers={"X-API-Key": secret})
    assert resp.status_code == 200

    # List shows the key (no secret).
    resp = client.get("/weather-stations/admin/keys", headers=headers)
    assert resp.status_code == 200
    listed = resp.json()["keys"]
    assert any(k["key_id"] == key_id and "secret" not in k for k in listed)

    # Revoke.
    resp = client.delete(f"/weather-stations/admin/keys/{key_id}", headers=headers)
    assert resp.status_code == 204

    # Read with revoked key now 401s.
    resp = client.get("/weather-stations/latest", headers={"X-API-Key": secret})
    assert resp.status_code == 401

    # Revoking again 404s.
    resp = client.delete(f"/weather-stations/admin/keys/{key_id}", headers=headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_auth_disabled_lets_reads_through_without_header(
    app_and_keystore,
):
    app, _ = app_and_keystore
    settings.weather_stations_api_key_auth_enabled = False
    client = TestClient(app)
    resp = client.get("/weather-stations/latest")
    assert resp.status_code == 200


# ------------------------------------------------------------ admin add-custom


@pytest.mark.asyncio
async def test_admin_add_custom_registers_custom_secret(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    headers = {"X-Admin-Password": "admin-pw"}

    resp = client.post(
        "/weather-stations/admin/keys/add-custom",
        json={"label": "manual", "secret": "hello-world-123"},
        headers=headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["label"] == "manual"
    assert body["secret"] == "hello-world-123"

    # Custom secret immediately works as an X-API-Key.
    resp = client.get(
        "/weather-stations/latest", headers={"X-API-Key": "hello-world-123"}
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_admin_add_custom_rejects_duplicate_secret_with_409(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    headers = {"X-Admin-Password": "admin-pw"}

    resp = client.post(
        "/weather-stations/admin/keys/add-custom",
        json={"label": "a", "secret": "shared-secret"},
        headers=headers,
    )
    assert resp.status_code == 201

    resp = client.post(
        "/weather-stations/admin/keys/add-custom",
        json={"label": "b", "secret": "shared-secret"},
        headers=headers,
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_admin_add_custom_rejects_missing_admin_password(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)

    resp = client.post(
        "/weather-stations/admin/keys/add-custom",
        json={"label": "a", "secret": "x"},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_admin_add_custom_validates_secret_length(app_and_keystore):
    app, _ = app_and_keystore
    client = TestClient(app)
    headers = {"X-Admin-Password": "admin-pw"}

    resp = client.post(
        "/weather-stations/admin/keys/add-custom",
        json={"label": "a", "secret": ""},
        headers=headers,
    )
    assert resp.status_code == 422

    resp = client.post(
        "/weather-stations/admin/keys/add-custom",
        json={"label": "a", "secret": "x" * 129},
        headers=headers,
    )
    assert resp.status_code == 422
