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
            },
            "radar": {"tile_ttl": 999},
        },
    )
    assert s.ecmwf_tile_ttl == 86400
    assert s.ecmwf_forecasts_to_keep == 3
    assert s.radar_tile_ttl == 999


def test_legacy_flat_keys_still_work(tmp_path):
    s = _load(tmp_path, {"basemap_tile_ttl": 7, "ecmwf_forecasts_to_keep": 5})
    assert s.basemap_tile_ttl == 7
    assert s.ecmwf_forecasts_to_keep == 5


def test_sync_min_sleep_seconds_loads(tmp_path):
    s = _load(tmp_path, {"sync_min_sleep_seconds": 15})
    assert s.sync_min_sleep_seconds == 15


def test_nested_wrf_inits_to_keep_flattens(tmp_path):
    s = _load(tmp_path, {"wrf": {"tile_ttl": 111, "inits_to_keep": 4}})
    assert s.wrf_tile_ttl == 111
    assert s.wrf_inits_to_keep == 4


def test_nested_basemap_scrape_fanout_window_flattens(tmp_path):
    s = _load(tmp_path, {"basemap": {"scrape_fanout_window": 250}})
    assert s.basemap_scrape_fanout_window == 250


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
    assert s.basemap_tile_ttl == 2592000
    assert s.basemap_sync_mode == "no_cache"
    assert s.ecmwf_tile_ttl == 86400
    assert s.radar_tile_ttl == 2592000
    assert s.sync_mode == "full"
    assert s.sync_min_sleep_seconds == 20
    assert s.wrf_inits_to_keep == 3
    assert s.basemap_scrape_fanout_window == 500
    assert s.basemap_scrape_per_host_concurrent == 8
    assert isinstance(s.basemap_providers, list) and s.basemap_providers


def _built_settings(tmp_path: Path, data: dict) -> Settings:
    """Build a full Settings, bypassing env reads and triggering _validate."""
    s = Settings.__new__(Settings)
    s._load_from_json(_write_json(tmp_path, data))  # pylint: disable=protected-access
    # Env-only fields the JSON loader doesn't populate. The weather-stations
    # validator hard-requires the admin password AND S3 creds when auth is on;
    # seed placeholders so basemap-focused tests don't trip unrelated checks.
    s.weather_stations_admin_password = "x"
    s.smn_api_username = "u"
    s.smn_api_password = "p"
    s.s3_tiles_data_endpoint = "ep"
    s.s3_tiles_data_access_key = "ak"
    s.s3_tiles_data_secret_key = "sk"
    s.s3_api_keys_bucket_name = "api-keys"
    s._validate()  # pylint: disable=protected-access
    return s


def test_scrape_parallelism_mode_round_trips(tmp_path):
    s = _built_settings(
        tmp_path,
        {
            "basemap": {
                "sync_mode": "full",
                "scrape_parallelism_mode": "per_origin",
                "scrape_per_host_concurrent": 4,
                "scrape_concurrent": 20,
            }
        },
    )
    assert s.basemap_scrape_parallelism_mode == "per_origin"
    assert s.basemap_scrape_per_host_concurrent == 4


def test_invalid_scrape_parallelism_mode_rejected(tmp_path):
    import pytest  # local import; this file uses plain asserts elsewhere

    with pytest.raises(ValueError, match="basemap_scrape_parallelism_mode"):
        _built_settings(
            tmp_path,
            {
                "basemap": {
                    "sync_mode": "full",
                    "scrape_parallelism_mode": "bogus",
                }
            },
        )


def test_per_host_concurrent_exceeding_global_rejected(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="exceeds global"):
        _built_settings(
            tmp_path,
            {
                "basemap": {
                    "sync_mode": "full",
                    "scrape_parallelism_mode": "sequential",
                    "scrape_concurrent": 4,
                    "scrape_per_host_concurrent": 8,
                }
            },
        )


def test_provider_cooldown_schedule_round_trips(tmp_path):
    s = _built_settings(
        tmp_path,
        {
            "basemap": {
                "sync_mode": "full",
                "provider_unhealthy_threshold": 7,
                "provider_cooldown_schedule": [60, 120, 300],
            }
        },
    )
    assert s.basemap_provider_unhealthy_threshold == 7
    assert s.basemap_provider_cooldown_schedule == [60, 120, 300]


def test_provider_unhealthy_threshold_must_be_positive(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="basemap_provider_unhealthy_threshold"):
        _built_settings(
            tmp_path,
            {
                "basemap": {
                    "sync_mode": "full",
                    "provider_unhealthy_threshold": 0,
                }
            },
        )


def test_provider_error_rate_round_trips(tmp_path):
    s = _built_settings(
        tmp_path,
        {
            "basemap": {
                "sync_mode": "full",
                "provider_error_rate_threshold": 0.1,
                "provider_error_rate_min_samples": 25,
            }
        },
    )
    assert s.basemap_provider_error_rate_threshold == 0.1
    assert s.basemap_provider_error_rate_min_samples == 25


def test_provider_error_rate_threshold_out_of_range_rejected(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="basemap_provider_error_rate_threshold"):
        _built_settings(
            tmp_path,
            {"basemap": {"sync_mode": "full", "provider_error_rate_threshold": 1.5}},
        )


def test_provider_error_rate_min_samples_must_be_positive(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="basemap_provider_error_rate_min_samples"):
        _built_settings(
            tmp_path,
            {"basemap": {"sync_mode": "full", "provider_error_rate_min_samples": 0}},
        )


def test_provider_cooldown_schedule_rejects_empty(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="non-empty"):
        _built_settings(
            tmp_path,
            {
                "basemap": {
                    "sync_mode": "full",
                    "provider_cooldown_schedule": [],
                }
            },
        )


def test_provider_cooldown_schedule_rejects_non_positive(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="must all be"):
        _built_settings(
            tmp_path,
            {
                "basemap": {
                    "sync_mode": "full",
                    "provider_cooldown_schedule": [60, 0, 300],
                }
            },
        )


def test_provider_cooldown_schedule_rejects_non_monotonic(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="monotonically"):
        _built_settings(
            tmp_path,
            {
                "basemap": {
                    "sync_mode": "full",
                    "provider_cooldown_schedule": [600, 300, 900],
                }
            },
        )
