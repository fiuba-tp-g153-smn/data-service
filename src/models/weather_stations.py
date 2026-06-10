"""Response models for the weather-stations endpoints."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class StationObservation(BaseModel):
    """One station's observation as returned by the SMN /weather/station endpoint."""

    station_id: int
    observed_at: Optional[datetime] = None
    temperature: Optional[float] = None
    feels_like: Optional[float] = None
    humidity: Optional[float] = None
    pressure: Optional[float] = None
    visibility: Optional[float] = None
    # Dew point (°C), derived server-side via the Magnus formula from
    # temperature + humidity; None when either is missing/out of range.
    dew_point: Optional[float] = None
    weather: Optional[dict] = None
    wind: Optional[dict] = None
    # Set only by the `/{tileset_id}` endpoint: True when this station's
    # `observed_at` is within `grace_period_hours` of the selected hour. Absent
    # (None) on `/latest` and raw S3 bodies, which carry no freshness window.
    is_current: Optional[bool] = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "station_id": 87344,
                    "observed_at": "2026-05-17T13:00:00Z",
                    "temperature": 18.4,
                    "feels_like": 17.9,
                    "humidity": 62.0,
                    "pressure": 1013.2,
                    "visibility": 10.0,
                    "dew_point": 10.89,
                    "weather": {"id": 1, "description": "Despejado"},
                    "wind": {"direction": "Norte", "deg": 5, "speed": 8.2},
                }
            ]
        }
    )


class WeatherStationsSnapshot(BaseModel):
    """One scrape cycle's persisted snapshot (matches the on-disk JSON shape)."""

    scraped_at: datetime
    source_url: str
    stations: List[StationObservation]

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "scraped_at": "2026-05-17T14:05:00Z",
                    "source_url": "https://api-test.smn.gob.ar/v1/weather/station",
                    "stations": [
                        {
                            "station_id": 87344,
                            "observed_at": "2026-05-17T13:00:00Z",
                            "temperature": 18.4,
                            "feels_like": 17.9,
                            "humidity": 62.0,
                            "pressure": 1013.2,
                            "visibility": 10.0,
                            "dew_point": 10.89,
                            "weather": {"id": 1, "description": "Despejado"},
                            "wind": {"direction": "Norte", "deg": 5, "speed": 8.2},
                        }
                    ],
                }
            ]
        }
    )


class StationSeriesPoint(BaseModel):
    """One timestamped observation in a station's history series.

    Flattened from `StationObservation` (wind unpacked) so charts can read each
    numeric variable directly. `observed_at` is the SMN reading time, not the
    scrape time.
    """

    observed_at: datetime
    temperature: Optional[float] = None
    feels_like: Optional[float] = None
    humidity: Optional[float] = None
    pressure: Optional[float] = None
    visibility: Optional[float] = None
    # Dew point (°C), computed server-side via the Magnus formula from
    # temperature + humidity; None when either is missing or out of range.
    dew_point: Optional[float] = None
    # SMN weather description (e.g. "Niebla", "Despejado"); None when absent.
    condition: Optional[str] = None
    wind_speed: Optional[float] = None
    wind_deg: Optional[float] = None
    wind_direction: Optional[str] = None


class StationSeriesResponse(BaseModel):
    """A single station's last-`hours` history, bundled in one payload.

    The whole feature is served by this one object: every variable across every
    timestamp, plus the station's name/province and the `latest` point — so the
    frontend needs no companion registry/snapshot request.
    """

    station_id: int
    station_name: Optional[str] = None
    province: Optional[str] = None
    hours: int
    points: List[StationSeriesPoint]
    latest: Optional[StationSeriesPoint] = None

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "station_id": 87344,
                    "station_name": "CORDOBA AERO",
                    "province": "CORDOBA",
                    "hours": 48,
                    "points": [
                        {
                            "observed_at": "2026-05-17T13:00:00Z",
                            "temperature": 18.4,
                            "feels_like": 17.9,
                            "humidity": 62.0,
                            "pressure": 1013.2,
                            "visibility": 10.0,
                            "wind_speed": 8.2,
                            "wind_deg": 5,
                            "wind_direction": "Norte",
                        }
                    ],
                    "latest": {
                        "observed_at": "2026-05-17T13:00:00Z",
                        "temperature": 18.4,
                        "feels_like": 17.9,
                        "humidity": 62.0,
                        "pressure": 1013.2,
                        "visibility": 10.0,
                        "wind_speed": 8.2,
                        "wind_deg": 5,
                        "wind_direction": "Norte",
                    },
                }
            ]
        }
    )


class TilesetEntry(BaseModel):
    """One available time bucket exposed via /weather-stations/tilesets."""

    tileset_id: str = Field(
        ...,
        description=(
            "Hour-bucket identifier, format `YYYYMMDDTHH00Z`. "
            "Use as the `{tilesetId}` path param in `/weather-stations/{tilesetId}`."
        ),
        examples=["20260517T1400Z"],
    )
    scraped_at: datetime
    station_count: int


class TilesetsResponse(BaseModel):
    """Response listing the time buckets currently available in S3."""

    tilesets: List[TilesetEntry]

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "tilesets": [
                        {
                            "tileset_id": "20260517T1300Z",
                            "scraped_at": "2026-05-17T13:55:00Z",
                            "station_count": 142,
                        },
                        {
                            "tileset_id": "20260517T1400Z",
                            "scraped_at": "2026-05-17T14:55:00Z",
                            "station_count": 143,
                        },
                    ]
                }
            ]
        }
    )


class StationRegistryEntry(BaseModel):
    """Metadata for one station from the public SMN EMA registry."""

    station_id: int
    name: str
    province: str
    latitude: float
    longitude: float
    altitude_meters: int
    oaci_code: Optional[str] = None


class StationsRegistryResponse(BaseModel):
    """Full station registry (rarely changes; cache aggressively client-side)."""

    fetched_at: Optional[datetime] = None
    source_url: Optional[str] = None
    stations: List[StationRegistryEntry]

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "fetched_at": "2026-05-17T14:00:00Z",
                    "source_url": "http://ssl.smn.gob.ar/dpd/zipopendata.php?dato=estaciones",
                    "stations": [
                        {
                            "station_id": 87344,
                            "name": "CORDOBA AERO",
                            "province": "CORDOBA",
                            "latitude": -31.2833,
                            "longitude": -64.2,
                            "altitude_meters": 495,
                            "oaci_code": "SACO",
                        }
                    ],
                }
            ]
        }
    )


class AdminKeyCreateRequest(BaseModel):
    """Body for POST /weather-stations/admin/keys."""

    label: str = Field(..., min_length=1, max_length=80)

    model_config = ConfigDict(json_schema_extra={"examples": [{"label": "local-dev"}]})


class AdminKeyCreateResponse(BaseModel):
    """Response carrying the new key's plaintext secret (returned once)."""

    key_id: str
    label: str
    secret: str
    created_at: datetime

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "key_id": "1a2b3c4d5e6f7890",
                    "label": "local-dev",
                    "secret": "kZ8sJ3w...",
                    "created_at": "2026-05-17T14:00:00Z",
                }
            ]
        }
    )


class AdminKeyAddCustomRequest(BaseModel):
    """Body for POST /weather-stations/admin/keys/add-custom."""

    label: str = Field(..., min_length=1, max_length=80)
    secret: str = Field(..., min_length=1, max_length=128)

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"label": "my-custom-key", "secret": "hello-world-123"}]
        }
    )


class AdminKeyListEntry(BaseModel):
    """One row from GET /weather-stations/admin/keys."""

    key_id: str
    label: str
    created_at: datetime
    last_used_at: Optional[datetime] = None


class AdminKeyListResponse(BaseModel):
    """List of active API keys (no secrets)."""

    keys: List[AdminKeyListEntry]

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "keys": [
                        {
                            "key_id": "1a2b3c4d5e6f7890",
                            "label": "local-dev",
                            "created_at": "2026-05-17T14:00:00Z",
                            "last_used_at": "2026-05-17T14:05:12Z",
                        }
                    ]
                }
            ]
        }
    )


def make_admin_create_response(
    key_id: str,
    label: str,
    secret: str,
    created_at_epoch: int,
) -> AdminKeyCreateResponse:
    """Build an `AdminKeyCreateResponse` from a `CreatedApiKey`."""
    return AdminKeyCreateResponse(
        key_id=key_id,
        label=label,
        secret=secret,
        created_at=datetime.fromtimestamp(created_at_epoch),
    )


def make_admin_list_entry(
    key_id: str,
    label: str,
    created_at_epoch: int,
    last_used_at_epoch: Optional[int],
) -> AdminKeyListEntry:
    """Build an `AdminKeyListEntry` from a keystore row."""
    return AdminKeyListEntry(
        key_id=key_id,
        label=label,
        created_at=datetime.fromtimestamp(created_at_epoch),
        last_used_at=(
            datetime.fromtimestamp(last_used_at_epoch)
            if last_used_at_epoch is not None
            else None
        ),
    )


__all__ = [
    "AdminKeyAddCustomRequest",
    "AdminKeyCreateRequest",
    "AdminKeyCreateResponse",
    "AdminKeyListEntry",
    "AdminKeyListResponse",
    "StationObservation",
    "StationRegistryEntry",
    "StationSeriesPoint",
    "StationSeriesResponse",
    "StationsRegistryResponse",
    "TilesetEntry",
    "TilesetsResponse",
    "WeatherStationsSnapshot",
    "make_admin_create_response",
    "make_admin_list_entry",
]
