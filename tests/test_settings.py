"""Tests for the settings.json loader's per-domain namespace flattening."""

import json
from pathlib import Path

from settings import Settings


def _write_json(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _load(tmp_path: Path, data: dict) -> Settings:
    """Build a bare Settings (skipping __init__) and run the JSON loader on it."""
    settings = Settings.__new__(Settings)
    settings._load_from_json(
        _write_json(tmp_path, data)
    )  # pylint: disable=protected-access
    return settings


def test_nested_basemap_loads_into_flat_attrs(tmp_path):
    s = _load(tmp_path, {"basemap": {"tile_ttl": 42, "sync_mode": "on_demand"}})
    assert s.basemap_tile_ttl == 42
    assert s.basemap_sync_mode == "on_demand"


def test_nested_ecmwf_and_radar_load(tmp_path):
    s = _load(
        tmp_path,
        {
            "ecmwf": {
                "tile_ttl": 86400,
                "forecasts_to_keep": 3,
                "sync_interval_seconds": 120,
            },
            "radar": {"tile_ttl": 999, "sync_interval_seconds": 7},
        },
    )
    assert s.ecmwf_tile_ttl == 86400
    assert s.ecmwf_forecasts_to_keep == 3
    assert s.ecmwf_sync_interval_seconds == 120
    assert s.radar_tile_ttl == 999
    assert s.radar_sync_interval_seconds == 7


def test_legacy_flat_keys_still_work(tmp_path):
    s = _load(tmp_path, {"basemap_tile_ttl": 7, "ecmwf_forecasts_to_keep": 5})
    assert s.basemap_tile_ttl == 7
    assert s.ecmwf_forecasts_to_keep == 5


def test_nested_overrides_flat_when_both_present(tmp_path):
    s = _load(
        tmp_path,
        {"basemap_tile_ttl": 7, "basemap": {"tile_ttl": 99}},
    )
    assert s.basemap_tile_ttl == 99


def test_unknown_namespace_is_ignored(tmp_path):
    s = _load(
        tmp_path,
        {"basemap": {"tile_ttl": 11}, "weather": {"foo": "bar"}},
    )
    assert s.basemap_tile_ttl == 11
    assert not hasattr(s, "weather_foo")
    assert not hasattr(s, "foo")


def test_providers_list_loads_from_nested(tmp_path):
    s = _load(
        tmp_path,
        {
            "basemap": {
                "providers": [
                    {"id": "argenmap", "enabled": True},
                    {"id": "satellite", "enabled": False},
                ],
            }
        },
    )
    assert s.basemap_providers == [
        {"id": "argenmap", "enabled": True},
        {"id": "satellite", "enabled": False},
    ]


def test_top_level_shared_keys_unchanged(tmp_path):
    s = _load(
        tmp_path,
        {
            "sync_mode": "on_demand",
            "tile_ttl": 1234,
            "cache_control_tile": "no-store",
            "basemap": {"tile_ttl": 1},
        },
    )
    assert s.sync_mode == "on_demand"
    assert s.tile_ttl == 1234
    assert s.cache_control_tile == "no-store"
    assert s.basemap_tile_ttl == 1


def test_real_settings_json_round_trip():
    """Smoke check the actual repo settings.json loads without surprise."""
    repo_path = Path(__file__).resolve().parent.parent / "settings.json"
    s = Settings.__new__(Settings)
    s._load_from_json(repo_path)  # pylint: disable=protected-access
    assert s.basemap_tile_ttl == 604800
    assert s.basemap_sync_mode == "no_cache"
    assert s.ecmwf_tile_ttl == 86400
    assert s.radar_tile_ttl == 2592000
    assert s.sync_mode == "full"
    assert isinstance(s.basemap_providers, list) and s.basemap_providers
