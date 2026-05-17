"""Response models for the weather-stations endpoints."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


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


class WeatherStationsSnapshot(BaseModel):
    """One scrape cycle's persisted snapshot (matches the on-disk JSON shape)."""

    scraped_at: datetime
    source_url: str
    stations: List[StationObservation]


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


class AdminKeyCreateRequest(BaseModel):
    """Body for POST /weather-stations/admin/keys."""

    label: str = Field(..., min_length=1, max_length=80)


class AdminKeyCreateResponse(BaseModel):
    """One-time response carrying the new key's plaintext secret."""

    key_id: str
    label: str
    secret: str = Field(
        ...,
        description=(
            "Plaintext API key — store it now; the server only keeps a hash."
        ),
    )
    created_at: datetime


class AdminKeyListEntry(BaseModel):
    """One row from GET /weather-stations/admin/keys (no secrets)."""

    key_id: str
    label: str
    created_at: datetime
    last_used_at: Optional[datetime] = None


class AdminKeyListResponse(BaseModel):
    """Response listing every active API key (without secrets)."""

    keys: List[AdminKeyListEntry]


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
