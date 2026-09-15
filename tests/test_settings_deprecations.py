"""Tests for the one-release deprecation shim on renamed settings keys.

A settings rename changes the attribute, the settings.json key and the env
override derived from it. `_load_from_json` drops unknown keys with a warning
rather than failing, so without the shim a deployment still carrying the old
env var would fall back to the code default and nothing would fail — the
basemap sweep quietly switching on, a TTL quietly reverting. These tests pin
that the old spelling still lands on the new attribute and says so in the log.
"""

import json
import logging
from pathlib import Path

import pytest

from settings import Settings


def _write_json(tmp_path: Path, data: dict) -> Path:
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _load(tmp_path: Path, data: dict) -> Settings:
    """Build a bare Settings (skipping __init__) and run the JSON loader on it."""
    settings = Settings.__new__(Settings)
    settings._load_from_json(  # pylint: disable=protected-access
        _write_json(tmp_path, data)
    )
    return settings


def _env_settings(monkeypatch, **env) -> Settings:
    """Build a bare Settings and run only the env loader against `env`."""
    for key in ("BASEMAP_SYNC_MODE", "BASEMAP_BACKUP_MODE"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    settings = Settings.__new__(Settings)
    settings.basemap_backup_mode = Settings.basemap_backup_mode
    return settings


# --- settings.json path -----------------------------------------------------


@pytest.mark.parametrize(
    "old_value,new_value",
    [
        ("full", "backup_and_prefetch"),
        ("on_demand", "backup_and_cache_on_read"),
        ("no_cache", "backup_only"),
        # Already accurate before the rename, so it carries over untouched.
        ("relay_only", "relay_only"),
    ],
)
def test_deprecated_basemap_values_migrate(tmp_path, old_value, new_value):
    s = _load(tmp_path, {"basemap": {"sync_mode": old_value}})
    assert s.basemap_backup_mode == new_value


def test_deprecated_json_key_warns_naming_its_replacement(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="settings"):
        _load(tmp_path, {"basemap": {"sync_mode": "no_cache"}})
    assert "basemap_sync_mode" in caplog.text
    assert "basemap_backup_mode" in caplog.text


def test_deprecated_json_key_is_not_reported_as_unrecognized(tmp_path, caplog):
    """The rename warning replaces the generic unknown-key warning."""
    with caplog.at_level(logging.WARNING, logger="settings"):
        _load(tmp_path, {"basemap": {"sync_mode": "no_cache"}})
    assert "unrecognized" not in caplog.text.lower()


def test_new_json_key_wins_over_the_deprecated_one(tmp_path, caplog):
    """A file carrying both spellings is resolved, not ambiguous."""
    with caplog.at_level(logging.WARNING, logger="settings"):
        s = _load(
            tmp_path,
            {"basemap": {"sync_mode": "full", "backup_mode": "relay_only"}},
        )
    assert s.basemap_backup_mode == "relay_only"
    assert "ignored" in caplog.text


# --- environment path -------------------------------------------------------


def test_deprecated_env_var_migrates_key_and_value(monkeypatch, caplog):
    s = _env_settings(monkeypatch, BASEMAP_SYNC_MODE="no_cache")
    with caplog.at_level(logging.WARNING, logger="settings"):
        resolved = s._env("BASEMAP_BACKUP_MODE")  # pylint: disable=protected-access
    assert resolved == "backup_only"
    assert "BASEMAP_SYNC_MODE" in caplog.text
    assert "BASEMAP_BACKUP_MODE" in caplog.text


def test_new_env_var_wins_over_the_deprecated_one(monkeypatch):
    s = _env_settings(
        monkeypatch,
        BASEMAP_SYNC_MODE="no_cache",
        BASEMAP_BACKUP_MODE="relay_only",
    )
    assert s._env("BASEMAP_BACKUP_MODE") == "relay_only"  # pylint: disable=W0212


def test_unset_deprecated_env_var_resolves_to_nothing(monkeypatch):
    """An absent alias must not shadow the caller's own default."""
    s = _env_settings(monkeypatch)
    assert s._env("BASEMAP_BACKUP_MODE") == ""  # pylint: disable=protected-access


# --- phase 2: string modes that only ever had two values --------------------


@pytest.mark.parametrize(
    "old_value,prefetch",
    [("full", True), ("on_demand", False)],
)
def test_deprecated_sync_mode_becomes_a_boolean(tmp_path, old_value, prefetch):
    s = _load(tmp_path, {"sync_mode": old_value})
    assert s.sync_prefetch is prefetch


@pytest.mark.parametrize(
    "old_value,enabled",
    [("full", True), ("disabled", False)],
)
def test_deprecated_weather_stations_mode_becomes_a_boolean(
    tmp_path, old_value, enabled
):
    s = _load(tmp_path, {"weather_stations": {"sync_mode": old_value}})
    assert s.weather_stations_sync_enabled is enabled


@pytest.mark.parametrize(
    "env_value,expected",
    [("full", "true"), ("on_demand", "false")],
)
def test_deprecated_sync_mode_env_var_renders_as_a_boolean(
    monkeypatch, env_value, expected
):
    """`_env_bool` reads strings, so the migrated value has to arrive as one."""
    monkeypatch.delenv("SYNC_PREFETCH", raising=False)
    monkeypatch.setenv("SYNC_MODE", env_value)
    s = Settings.__new__(Settings)
    assert s._env("SYNC_PREFETCH") == expected  # pylint: disable=protected-access


def test_deprecated_sync_mode_env_var_round_trips_through_env_bool(monkeypatch):
    """End to end: the old env var still lands on the new attribute's value."""
    monkeypatch.delenv("SYNC_PREFETCH", raising=False)
    monkeypatch.setenv("SYNC_MODE", "on_demand")
    s = Settings.__new__(Settings)
    assert s._env_bool("SYNC_PREFETCH", True) is False  # pylint: disable=W0212


def test_env_alias_table_covers_every_rename():
    """`_DEPRECATED_ENV` is derived, not maintained by hand alongside renames."""
    import settings as settings_module  # pylint: disable=import-outside-toplevel

    renames = settings_module._DEPRECATED_KEYS  # pylint: disable=protected-access
    expected = {new.upper(): old.upper() for old, (new, _) in renames.items()}
    assert Settings._DEPRECATED_ENV == expected  # pylint: disable=protected-access
