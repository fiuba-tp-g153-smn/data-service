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
from routes.weather_stations import router as weather_stations_router
from services.weather_stations_service import WeatherStationsService


class _FakeS3:
    def __init__(self, objects):
        self.objects: dict[str, bytes] = dict(objects)

    async def download_tile(self, key):
        return self.objects.get(key)

    async def list_object_keys(self, prefix):
        return [k for k in self.objects if k.startswith(prefix)]


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
async def app_and_keystore(tmp_path):
    """
    Build a minimal FastAPI app wired to the weather-stations router, a
    fresh keystore, and a populated read service.
    """
    keystore = WeatherStationsKeystore(str(tmp_path / "k.sqlite"))
    await keystore.connect()
    set_weather_stations_keystore(keystore)

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
    svc = WeatherStationsService()
    svc.configure(s3, list_cache_ttl=30.0)

    app = FastAPI()
    app.dependency_overrides[get_weather_stations_keystore] = lambda: keystore
    app.dependency_overrides[get_weather_stations_service] = lambda: svc
    app.include_router(weather_stations_router)

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
    for path in ("/weather-stations/latest", "/weather-stations/tilesets",
                 "/weather-stations/stations",
                 "/weather-stations/20260517T1400Z"):
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
    resp = client.post(
        "/weather-stations/admin/keys", json={"label": "x"}
    )
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

    resp = client.get(
        "/weather-stations/latest", headers={"X-API-Key": created.secret}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["scraped_at"] == "2026-05-17T14:00:00Z"
    assert len(body["stations"]) == 2
    assert resp.headers["cache-control"] == settings.weather_stations_cache_control_response


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


@pytest.mark.asyncio
async def test_tileset_lookup_hits_and_misses(app_and_keystore):
    app, keystore = app_and_keystore
    created = await keystore.create("test")
    client = TestClient(app)

    # Exact-hour hit.
    resp = client.get(
        "/weather-stations/20260517T1400Z?N=0",
        headers={"X-API-Key": created.secret},
    )
    assert resp.status_code == 200
    assert resp.json()["scraped_at"] == "2026-05-17T14:00:00Z"

    # Out-of-window -> 404.
    resp = client.get(
        "/weather-stations/20260517T1100Z?N=0",
        headers={"X-API-Key": created.secret},
    )
    assert resp.status_code == 404

    # Malformed tilesetId -> 400.
    resp = client.get(
        "/weather-stations/bogus", headers={"X-API-Key": created.secret}
    )
    assert resp.status_code == 400


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
    resp = client.get(
        "/weather-stations/latest", headers={"X-API-Key": secret}
    )
    assert resp.status_code == 200

    # List shows the key (no secret).
    resp = client.get("/weather-stations/admin/keys", headers=headers)
    assert resp.status_code == 200
    listed = resp.json()["keys"]
    assert any(k["key_id"] == key_id and "secret" not in k for k in listed)

    # Revoke.
    resp = client.delete(
        f"/weather-stations/admin/keys/{key_id}", headers=headers
    )
    assert resp.status_code == 204

    # Read with revoked key now 401s.
    resp = client.get(
        "/weather-stations/latest", headers={"X-API-Key": secret}
    )
    assert resp.status_code == 401

    # Revoking again 404s.
    resp = client.delete(
        f"/weather-stations/admin/keys/{key_id}", headers=headers
    )
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
