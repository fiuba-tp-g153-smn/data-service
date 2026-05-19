"""Unit tests for the S3-backed `WeatherStationsKeystore`."""

import re
from typing import Dict, List, Optional

import pytest
import pytest_asyncio

from clients import weather_stations_keystore as keystore_module
from clients.weather_stations_keystore import (
    SecretAlreadyInUseError,
    WeatherStationsKeystore,
    _hash_key,
    _object_key,
)


class _FakeS3:
    """In-memory stand-in for the slice of S3Client the keystore touches."""

    def __init__(self) -> None:
        self.objects: Dict[str, bytes] = {}
        self.download_calls: List[str] = []
        self.upload_calls: List[str] = []

    async def upload_tile(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> None:
        del content_type
        self.upload_calls.append(key)
        self.objects[key] = data

    async def download_tile(self, key: str) -> Optional[bytes]:
        self.download_calls.append(key)
        return self.objects.get(key)

    async def object_exists(self, key: str) -> bool:
        return key in self.objects

    async def list_object_keys(self, prefix: str) -> List[str]:
        return [k for k in self.objects if k.startswith(prefix)]

    async def delete_object(self, key: str) -> bool:
        return self.objects.pop(key, None) is not None


@pytest_asyncio.fixture
async def keystore():
    ks = WeatherStationsKeystore(_FakeS3())  # type: ignore[arg-type]
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

    [row] = await keystore.list_all()
    assert row.key_id == created.key_id
    assert row.label == created.label
    assert not hasattr(row, "secret")


@pytest.mark.asyncio
async def test_generated_secret_matches_alphanumeric_43_chars(keystore):
    pattern = re.compile(r"^[A-Za-z0-9]{43}$")
    for _ in range(20):
        created = await keystore.create("k")
        assert pattern.match(created.secret), created.secret


@pytest.mark.asyncio
async def test_create_writes_a_single_hash_named_object(keystore):
    created = await keystore.create("k")
    # pylint: disable=protected-access
    fake: _FakeS3 = keystore._s3  # type: ignore[assignment]
    expected_object_key = _object_key(_hash_key(created.secret))
    assert list(fake.objects) == [expected_object_key]


@pytest.mark.asyncio
async def test_validate_accepts_real_secret_and_rejects_others(keystore):
    created = await keystore.create("k")
    assert await keystore.is_valid(created.secret) is True
    assert await keystore.is_valid(created.secret + "x") is False
    assert await keystore.is_valid("") is False


@pytest.mark.asyncio
async def test_validate_updates_last_used_at_on_cache_miss(keystore):
    created = await keystore.create("k")
    [before] = await keystore.list_all()
    assert before.last_used_at is None

    assert await keystore.is_valid(created.secret) is True
    [after] = await keystore.list_all()
    assert after.last_used_at is not None


@pytest.mark.asyncio
async def test_validation_cache_hit_skips_s3(keystore, monkeypatch):
    """A second is_valid() within TTL must not GET from S3 again."""
    created = await keystore.create("k")
    # pylint: disable=protected-access
    fake: _FakeS3 = keystore._s3  # type: ignore[assignment]

    # Freeze time so the cache entry stays fresh.
    monkeypatch.setattr(keystore_module.time, "time", lambda: 1_000_000.0)
    assert await keystore.is_valid(created.secret) is True
    downloads_after_first = list(fake.download_calls)

    assert await keystore.is_valid(created.secret) is True
    assert (
        fake.download_calls == downloads_after_first
    ), "second is_valid() should be served from cache, not S3"


@pytest.mark.asyncio
async def test_validation_cache_expires(keystore, monkeypatch):
    created = await keystore.create("k")
    # pylint: disable=protected-access
    fake: _FakeS3 = keystore._s3  # type: ignore[assignment]

    clock = {"now": 1_000_000.0}
    monkeypatch.setattr(keystore_module.time, "time", lambda: clock["now"])

    assert await keystore.is_valid(created.secret) is True
    first_downloads = len(fake.download_calls)

    # Advance past the cache TTL.
    clock["now"] += keystore_module._VALIDATION_CACHE_TTL_SECONDS + 1
    assert await keystore.is_valid(created.secret) is True
    assert len(fake.download_calls) > first_downloads


@pytest.mark.asyncio
async def test_revoke_removes_the_key_and_busts_cache(keystore):
    created = await keystore.create("k")
    assert await keystore.is_valid(created.secret) is True  # warms the cache

    removed = await keystore.revoke(created.key_id)
    assert removed is True
    # Cache was busted on revoke, so the next is_valid() goes to S3, finds
    # the object gone, and returns False.
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
async def test_inject_stores_caller_supplied_secret(keystore):
    created = await keystore.inject("manual", "hiImGabriel")
    assert created.label == "manual"
    assert created.secret == "hiImGabriel"
    assert created.key_id

    assert await keystore.is_valid("hiImGabriel") is True
    [row] = await keystore.list_all()
    assert row.key_id == created.key_id
    assert row.label == "manual"


@pytest.mark.asyncio
async def test_inject_accepts_arbitrary_non_alphanumeric_secrets(keystore):
    weird_secrets = ["hi-Im-Gabriel", "Gabriel.2026", "with spaces!", "Ωemoji🚀"]
    for i, secret in enumerate(weird_secrets):
        created = await keystore.inject(f"label-{i}", secret)
        assert created.secret == secret
        assert await keystore.is_valid(secret) is True


@pytest.mark.asyncio
async def test_inject_rejects_duplicate_secret(keystore):
    await keystore.inject("first", "shared-secret")
    with pytest.raises(SecretAlreadyInUseError):
        await keystore.inject("second", "shared-secret")


@pytest.mark.asyncio
async def test_state_survives_reopen():
    fake = _FakeS3()
    ks1 = WeatherStationsKeystore(fake)  # type: ignore[arg-type]
    await ks1.connect()
    created = await ks1.create("durable")
    await ks1.close()

    ks2 = WeatherStationsKeystore(fake)  # type: ignore[arg-type]
    await ks2.connect()
    try:
        assert await ks2.is_valid(created.secret) is True
    finally:
        await ks2.close()
