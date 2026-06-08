"""Shared cache contract for the weather-stations subsystem.

Home of the Redis cache-key builders, the S3 snapshot-key parsing helpers, and
the tileset-listing computation. Imported by **both** the scraper (write side)
and the read service (read side) so neither depends on the other and the S3
layout knowledge lives in one place.

Redis keys (binary `decode_responses=False`, JSON payloads):
    cache:ws:latest                  raw latest.json bytes
    cache:ws:tilesets                 assembled tilesets list (JSON)
    cache:ws:registry                 raw stations.json bytes
    cache:ws:snap:{s3_object_key}     a snapshot body, keyed by its S3 object so one
                                      cached body backs every /{tileset_id} request
                                      (the grace_period_hours flagging is applied
                                      per-request, after the raw body is read)
"""

import asyncio
import json
import logging
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from clients.s3_client import S3Client

# A LIST function: prefix -> keys. The read service passes its in-process
# cached lister; the scraper lets it default to a direct S3 LIST.
ListKeysFn = Callable[[str], Awaitable[List[str]]]

logger = logging.getLogger(__name__)

# Mirror the scraper's S3 layout (kept here, not imported from the scraper, so
# the read path doesn't depend on the scraper module loading).
_S3_PREFIX = "weather-stations"
SNAPSHOTS_PREFIX = f"{_S3_PREFIX}/snapshots/"
SNAPSHOT_META_SUFFIX = ".meta.json"
# `YYYYMMDDTHHMMSSZ` parsed back to a UTC datetime.
_SNAPSHOT_KEY_RE = re.compile(r".*/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z\.json$")
_TILESET_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})Z$")


class TilesetIdFormatError(Exception):
    """Raised when a tilesetId path param doesn't match `YYYYMMDDTHHMMZ`."""


# --------------------------------------------------------------- Redis key builders


def latest_key() -> str:
    """Cache key for the most recent snapshot (`/latest`)."""
    return "cache:ws:latest"


def tilesets_key() -> str:
    """Cache key for the assembled tilesets listing (`/tilesets`)."""
    return "cache:ws:tilesets"


def registry_key() -> str:
    """Cache key for the station registry (`/stations`)."""
    return "cache:ws:registry"


def snap_body_key(s3_key: str) -> str:
    """Cache key for a snapshot body, keyed by its S3 object key.

    Grace-independent: `/{tileset_id}` resolves a bucket to one S3 object and the
    same raw body backs every request for it (the per-station `is_current`
    flagging is applied after the read) — so one cached body backs all of an
    animation's frames regardless of `grace_period_hours`.
    """
    return f"cache:ws:snap:{s3_key}"


def series_key(station_id: int) -> str:
    """Cache key for a single station's pre-pivoted history series.

    Warmed every cycle by the scraper (`pivot_station_series`) so a popover open
    never pivots from S3, and read-through-rebuilt on a cold miss.
    """
    return f"cache:ws:series:{station_id}"


# ---------------------------------------------------------- S3 snapshot-key parsing


def parse_snapshot_key(key: str) -> Optional[datetime]:
    """Parse `YYYYMMDDTHHMMSSZ` out of a snapshot key into a UTC datetime."""
    match = _SNAPSHOT_KEY_RE.match(key)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(p) for p in match.groups())
    try:
        return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_tileset_id(tileset_id: str) -> datetime:
    """Parse a `YYYYMMDDTHHMMZ` tilesetId into a UTC datetime."""
    match = _TILESET_RE.match(tileset_id)
    if not match:
        raise TilesetIdFormatError(
            f"Invalid tilesetId {tileset_id!r}; expected YYYYMMDDTHHMMZ "
            "(e.g. 20260517T1400Z)"
        )
    year, month, day, hour, minute = (int(p) for p in match.groups())
    try:
        return datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)
    except ValueError as exc:
        raise TilesetIdFormatError(f"Invalid tilesetId {tileset_id!r}: {exc}") from exc


def tileset_id_for(ts: datetime) -> str:
    """Round a snapshot timestamp down to its hour-bucket tilesetId."""
    return ts.strftime("%Y%m%dT%H00Z")


def day_prefixes_covering(start: datetime, end: datetime) -> List[str]:
    """Return `weather-stations/snapshots/YYYY/MM/DD/` prefixes covering [start, end]."""
    if end < start:
        start, end = end, start
    prefixes: List[str] = []
    day = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_day = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    while day <= end_day:
        prefixes.append(f"{SNAPSHOTS_PREFIX}{day.strftime('%Y/%m/%d')}/")
        day = day + timedelta(days=1)
    return prefixes


def parse_json_or_none(body: bytes, key: str) -> Optional[dict]:
    """Decode JSON bytes into a dict, logging and returning None on bad data."""
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("Could not parse object %s: %s", key, exc)
        return None
    if not isinstance(parsed, dict):
        logger.warning("Object %s is not a JSON object", key)
        return None
    return parsed


# ------------------------------------------------------ tileset listing computation


async def _read_station_count(s3: S3Client, snapshot_key_: str) -> int:
    """Read `station_count` from a snapshot's cheap sibling `.meta.json`."""
    meta_key = snapshot_key_[: -len(".json")] + SNAPSHOT_META_SUFFIX
    body = await s3.download_tile(meta_key)
    if body is None:
        return 0
    meta = parse_json_or_none(body, meta_key)
    if not isinstance(meta, dict):
        return 0
    count = meta.get("station_count")
    return int(count) if isinstance(count, int) else 0


async def _latest_snapshot_per_bucket(
    s3: S3Client, list_keys: Optional[ListKeysFn]
) -> Dict[str, Tuple[datetime, str]]:
    """LIST the snapshots prefix and keep the latest snapshot key per hour bucket.

    `list_keys` lets the read service inject its in-process cached lister (so a
    burst collapses to one S3 LIST); the scraper omits it for a direct LIST.
    """
    lister = list_keys or s3.list_object_keys
    keys = await lister(SNAPSHOTS_PREFIX)
    per_bucket: Dict[str, Tuple[datetime, str]] = {}
    for key in keys:
        if key.endswith(SNAPSHOT_META_SUFFIX):
            continue
        ts = parse_snapshot_key(key)
        if ts is None:
            continue
        bucket = tileset_id_for(ts)
        existing = per_bucket.get(bucket)
        if existing is None or ts > existing[0]:
            per_bucket[bucket] = (ts, key)
    return per_bucket


async def compute_tilesets_entries(
    s3: S3Client, list_keys: Optional[ListKeysFn] = None
) -> List[dict]:
    """Group snapshot keys into hour buckets and return tileset entries.

    Keeps the latest snapshot per hour bucket (`YYYYMMDDTHH00Z`) and returns its
    `scraped_at` (ISO-8601 string, so the result is JSON-serialisable for Redis
    and re-parses cleanly via `TilesetsResponse.model_validate`) plus the
    `station_count` from the cheap sibling `.meta.json` (0 when missing).
    """
    per_bucket = await _latest_snapshot_per_bucket(s3, list_keys)
    if not per_bucket:
        return []

    # Sort by scrape time so the meta GETs pair with the right tilesets in zip.
    items = sorted(per_bucket.items(), key=lambda kv: kv[1][0])
    counts = await asyncio.gather(
        *(_read_station_count(s3, key) for _, (_, key) in items)
    )
    return [
        {
            "tileset_id": tileset_id,
            "scraped_at": ts.isoformat().replace("+00:00", "Z"),
            "station_count": count,
        }
        for (tileset_id, (ts, _key)), count in zip(items, counts)
    ]


async def recent_snapshot_keys(
    s3: S3Client, window: int, list_keys: Optional[ListKeysFn] = None
) -> List[str]:
    """Return the representative snapshot S3 keys for the latest `window` buckets.

    Newest bucket first. Used by the scraper to pre-warm the animation window's
    snapshot bodies (`cache:ws:snap:{key}`) so playback never reads them from S3.
    """
    if window <= 0:
        return []
    per_bucket = await _latest_snapshot_per_bucket(s3, list_keys)
    # Sort buckets by snapshot time, newest first, take the window.
    newest = sorted(per_bucket.values(), key=lambda tk: tk[0], reverse=True)
    return [key for _ts, key in newest[:window]]


# ----------------------------------------------------- per-station series pivot


def parse_observed_at(value: object) -> Optional[datetime]:
    """Parse an ISO-8601 `observed_at` (with `Z` or offset) into a UTC datetime."""
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


# Magnus (Magnus-Tetens) dew-point: empirically accurate roughly over -45..60 °C.
_DEW_T_MIN, _DEW_T_MAX = -45.0, 60.0
_DEW_HR_TOLERANCE = 100.5  # accept + clamp slight sensor over-read (e.g. 100.3 %)
_DEW_B, _DEW_C = 17.625, 243.04


def magnus_dew_point(temperature: Any, humidity: Any) -> Optional[float]:
    """Dew point (°C) from air temperature + relative humidity via the Magnus formula.

    Fail-soft for batch use: returns `None` (never raises) for missing, non-numeric,
    non-finite, or out-of-range inputs, so one bad reading can't break a series.
    Humidity must be in `(0, 100]`; values up to 100.5 % are clamped to 100 (sensor
    noise), and temperature must fall within the formula's valid `[-45, 60]` range.
    """
    if temperature is None or humidity is None:
        return None
    try:
        t = float(temperature)
        hr = float(humidity)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(t) and math.isfinite(hr)):
        return None
    if hr <= 0 or hr > _DEW_HR_TOLERANCE:
        return None
    hr = min(hr, 100.0)
    if not _DEW_T_MIN <= t <= _DEW_T_MAX:
        return None
    gamma = math.log(hr / 100.0) + (_DEW_B * t) / (_DEW_C + t)
    return round((_DEW_C * gamma) / (_DEW_B - gamma), 2)


def _observation_to_point(obs: dict) -> dict:
    """Flatten one `StationObservation` dict into a series point (wind unpacked)."""
    raw_wind = obs.get("wind")
    wind = raw_wind if isinstance(raw_wind, dict) else {}
    raw_weather = obs.get("weather")
    weather = raw_weather if isinstance(raw_weather, dict) else {}
    return {
        "observed_at": obs.get("observed_at"),
        "temperature": obs.get("temperature"),
        "feels_like": obs.get("feels_like"),
        "humidity": obs.get("humidity"),
        "pressure": obs.get("pressure"),
        "visibility": obs.get("visibility"),
        "dew_point": magnus_dew_point(obs.get("temperature"), obs.get("humidity")),
        "condition": weather.get("description"),
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "wind_direction": wind.get("direction"),
    }


def _sorted_points(by_observed_at: Dict[str, dict]) -> List[dict]:
    """Drop points with an unparseable timestamp, sort ascending by reading time."""
    parsed = [
        (ts, point)
        for raw, point in by_observed_at.items()
        if (ts := parse_observed_at(raw)) is not None
    ]
    parsed.sort(key=lambda tp: tp[0])
    return [point for _ts, point in parsed]


def extract_station_series(bodies: List[dict], station_id: int) -> List[dict]:
    """Pull one station's history out of a list of snapshot bodies.

    Each body holds all stations at one scrape time; we keep the matching
    observation, dedupe by `observed_at` (adjacent hour buckets repeat the same
    hourly/3-hourly SMN reading), and return points sorted oldest→newest.
    """
    by_observed_at: Dict[str, dict] = {}
    for body in bodies:
        if not isinstance(body, dict):
            continue
        stations = body.get("stations")
        if not isinstance(stations, list):
            continue
        for obs in stations:
            if not isinstance(obs, dict) or obs.get("station_id") != station_id:
                continue
            observed_at = obs.get("observed_at")
            if isinstance(observed_at, str) and observed_at not in by_observed_at:
                by_observed_at[observed_at] = _observation_to_point(obs)
            break  # one observation per station per body
    return _sorted_points(by_observed_at)


def pivot_station_series(bodies: List[dict]) -> Dict[int, List[dict]]:
    """Pivot a list of snapshot bodies into `station_id -> sorted series` in one pass.

    Used by the scraper to warm every station's `series_key` per cycle (one S3
    read pass, then a single in-memory pivot), mirroring `extract_station_series`
    for the read path's cold-miss rebuild.
    """
    per_station: Dict[int, Dict[str, dict]] = {}
    for body in bodies:
        if not isinstance(body, dict):
            continue
        stations = body.get("stations")
        if not isinstance(stations, list):
            continue
        for obs in stations:
            if not isinstance(obs, dict):
                continue
            station_id = obs.get("station_id")
            observed_at = obs.get("observed_at")
            if not isinstance(station_id, int) or not isinstance(observed_at, str):
                continue
            bucket = per_station.setdefault(station_id, {})
            if observed_at not in bucket:
                bucket[observed_at] = _observation_to_point(obs)
    return {sid: _sorted_points(points) for sid, points in per_station.items()}
