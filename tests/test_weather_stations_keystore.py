"""Unit tests for `WeatherStationsKeystore` (SQLite-backed hashed API keys)."""

import pytest
import pytest_asyncio

from clients.weather_stations_keystore import WeatherStationsKeystore


@pytest_asyncio.fixture
async def keystore(tmp_path):
    """Fresh keystore on a tmp file; closed after each test."""
    ks = WeatherStationsKeystore(str(tmp_path / "k.sqlite"))
    await ks.connect()
    try:
        yield ks
    finally:
        await ks.close()


@pytest.mark.asyncio
async def test_create_returns_secret_only_once(keystore):
    created = await keystore.create("first-key")
    assert created.label == "first-key"
    assert created.secret
    assert created.key_id

    # The secret is not retrievable via list_all — only the record metadata.
    [row] = await keystore.list_all()
    assert row.key_id == created.key_id
    assert row.label == created.label
    assert not hasattr(row, "secret")


@pytest.mark.asyncio
async def test_validate_accepts_real_secret_and_rejects_others(keystore):
    created = await keystore.create("k")
    assert await keystore.is_valid(created.secret) is True
    assert await keystore.is_valid(created.secret + "x") is False
    assert await keystore.is_valid("") is False


@pytest.mark.asyncio
async def test_validate_updates_last_used_at(keystore):
    created = await keystore.create("k")
    [before] = await keystore.list_all()
    assert before.last_used_at is None

    assert await keystore.is_valid(created.secret) is True
    [after] = await keystore.list_all()
    assert after.last_used_at is not None


@pytest.mark.asyncio
async def test_revoke_removes_the_key(keystore):
    created = await keystore.create("k")
    assert await keystore.is_valid(created.secret) is True

    removed = await keystore.revoke(created.key_id)
    assert removed is True
    assert await keystore.is_valid(created.secret) is False
    assert await keystore.list_all() == []


@pytest.mark.asyncio
async def test_revoke_unknown_key_returns_false(keystore):
    removed = await keystore.revoke("does-not-exist")
    assert removed is False


@pytest.mark.asyncio
async def test_multiple_keys_coexist(keystore):
    a = await keystore.create("alpha")
    b = await keystore.create("beta")
    assert a.key_id != b.key_id
    assert a.secret != b.secret

    assert await keystore.is_valid(a.secret) is True
    assert await keystore.is_valid(b.secret) is True

    rows = await keystore.list_all()
    assert {r.label for r in rows} == {"alpha", "beta"}


@pytest.mark.asyncio
async def test_state_survives_reopen(tmp_path):
    path = str(tmp_path / "persist.sqlite")
    ks1 = WeatherStationsKeystore(path)
    await ks1.connect()
    created = await ks1.create("durable")
    await ks1.close()

    ks2 = WeatherStationsKeystore(path)
    await ks2.connect()
    try:
        assert await ks2.is_valid(created.secret) is True
    finally:
        await ks2.close()
