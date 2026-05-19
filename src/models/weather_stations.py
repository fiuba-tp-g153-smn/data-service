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
    weather: Optional[dict] = None
    wind: Optional[dict] = None

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
                            "weather": {"id": 1, "description": "Despejado"},
                            "wind": {"direction": "Norte", "deg": 5, "speed": 8.2},
                        }
                    ],
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

    label: str = Field(
        ...,
        min_length=1,
        max_length=80,
        description="Human-readable label so you can identify the key later.",
        examples=["local-dev", "visualizer-prod"],
    )

    model_config = ConfigDict(json_schema_extra={"examples": [{"label": "local-dev"}]})


class AdminKeyCreateResponse(BaseModel):
    """One-time response carrying the new key's plaintext secret."""

    key_id: str
    label: str
    secret: str = Field(
        ...,
        description=(
            "Plaintext API key — store it now; the server only keeps a hash. "
            "Use as the `X-API-Key` header on every public read endpoint."
        ),
    )
    created_at: datetime

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "key_id": "1a2b3c4d5e6f7890",
                    "label": "local-dev",
                    "secret": "kZ8sJ3w_token_urlsafe_32_chars_minimum_xyz",
                    "created_at": "2026-05-17T14:00:00Z",
                }
            ]
        }
    )


class AdminKeyInjectRequest(BaseModel):
    """Body for POST /weather-stations/admin/keys/inject."""

    label: str = Field(
        ...,
        min_length=1,
        max_length=80,
        description="Human-readable label so you can identify the key later.",
        examples=["manual-gabriel"],
    )
    secret: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description=(
            "Arbitrary plaintext secret to register as a valid API key. "
            "Any non-empty string up to 128 chars is accepted; charset is not "
            "restricted so human-legible secrets are allowed."
        ),
        examples=["hiImGabriel"],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{"label": "manual-gabriel", "secret": "hiImGabriel"}]
        }
    )


class AdminKeyListEntry(BaseModel):
    """One row from GET /weather-stations/admin/keys (no secrets)."""

    key_id: str
    label: str
    created_at: datetime
    last_used_at: Optional[datetime] = None


class AdminKeyListResponse(BaseModel):
    """Response listing every active API key (without secrets)."""

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
    "AdminKeyCreateRequest",
    "AdminKeyCreateResponse",
    "AdminKeyInjectRequest",
    "AdminKeyListEntry",
    "AdminKeyListResponse",
    "StationObservation",
    "StationRegistryEntry",
    "StationsRegistryResponse",
    "TilesetEntry",
    "TilesetsResponse",
    "WeatherStationsSnapshot",
    "make_admin_create_response",
    "make_admin_list_entry",
]
