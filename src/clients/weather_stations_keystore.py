"""S3-backed store for weather-stations API keys (hashed)."""

import asyncio
import hashlib
import json
import logging
import secrets
import string
import time
from dataclasses import dataclass
from typing import List, Optional

from clients.s3_client import S3Client

logger = logging.getLogger(__name__)

# Charset for server-generated secrets. Restricted to A-Za-z0-9 so the value
# is trivial to copy/paste/type by hand without backslash-escaping or quoting.
_ALPHABET = string.ascii_letters + string.digits  # 62 chars
# 43 chars * log2(62) ≈ 256 bits — matches `secrets.token_urlsafe(32)`'s
# security margin while staying inside the alphanumeric charset.
_DEFAULT_SECRET_LENGTH = 43

_KEY_PREFIX = "keys/"
_OBJECT_SUFFIX = ".json"

# Positive validations are cached in-process to keep `is_valid` off S3 on the
# hot path. Negative results are not cached (revocation must take effect on
# the next miss, and never-issued secrets must not be remembered as invalid).
_VALIDATION_CACHE_TTL_SECONDS = 60


@dataclass(frozen=True, slots=True)
class ApiKeyRecord:
    """One stored API key (never carries the plaintext secret)."""

    key_id: str
    label: str
    created_at: int
    last_used_at: Optional[int]


@dataclass(frozen=True, slots=True)
class CreatedApiKey:
    """Return value of `create`/`add_custom`: includes the plaintext secret."""

    key_id: str
    label: str
    secret: str
    created_at: int


class SecretAlreadyInUseError(Exception):
    """Raised by `add_custom` when the supplied secret is already stored."""


def _hash_key(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _object_key(key_hash: str) -> str:
    return f"{_KEY_PREFIX}{key_hash}{_OBJECT_SUFFIX}"


def _generate_secret() -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(_DEFAULT_SECRET_LENGTH))


class WeatherStationsKeystore:
    """
    Async S3-backed store of hashed API keys.

    Object layout: `s3://<bucket>/keys/<sha256(secret)>.json`, where the JSON
    body holds `{key_id, label, created_at, last_used_at}`. Plaintext secrets
    are returned only at creation time and never persisted; only the hash
    (as the object name) plus metadata is stored.

    Validation is O(1) per hit: `is_valid` hashes the presented secret and
    GETs that exact object. A small in-process positive cache (TTL
    `_VALIDATION_CACHE_TTL_SECONDS`) keeps the hot path off S3 for repeat
    requests.
    """

    def __init__(self, s3_client: S3Client):
        self._s3 = s3_client
        self._validation_cache: dict[str, float] = {}
        self._cache_lock = asyncio.Lock()

    async def connect(self) -> None:
        """No-op: the S3 client is connected by the lifespan that owns it."""

    async def close(self) -> None:
        """No-op: the S3 client is closed by the lifespan that owns it."""

    # ---------------------------------------------------------------- mint

    async def create(self, label: str) -> CreatedApiKey:
        """Mint a new API key. Returns the plaintext secret exactly once."""
        secret = _generate_secret()
        return await self._store_new_secret(label=label, secret=secret)

    async def add_custom(self, label: str, secret: str) -> CreatedApiKey:
        """
        Insert a key with a caller-provided secret.

        Raises `SecretAlreadyInUseError` if the secret's hash is already
        present (same secret previously stored — would orphan a key_id).
        """
        return await self._store_new_secret(
            label=label, secret=secret, allow_new_only=True
        )

    async def _store_new_secret(
        self, label: str, secret: str, allow_new_only: bool = False
    ) -> CreatedApiKey:
        key_hash = _hash_key(secret)
        if allow_new_only and await self._s3.object_exists(_object_key(key_hash)):
            raise SecretAlreadyInUseError("Secret already in use")
        key_id = secrets.token_hex(8)
        created_at = int(time.time())
        body = _encode_record(
            key_id=key_id, label=label, created_at=created_at, last_used_at=None
        )
        await self._s3.upload_tile(
            _object_key(key_hash), body, content_type="application/json"
        )
        return CreatedApiKey(
            key_id=key_id, label=label, secret=secret, created_at=created_at
        )

    # ---------------------------------------------------------------- list

    async def list_all(self) -> List[ApiKeyRecord]:
        """Return every API key record (without secrets)."""
        object_keys = await self._s3.list_object_keys(_KEY_PREFIX)
        if not object_keys:
            return []
        bodies = await asyncio.gather(
            *(self._s3.download_tile(k) for k in object_keys),
            return_exceptions=False,
        )
        records: List[ApiKeyRecord] = []
        for object_key, body in zip(object_keys, bodies):
            if body is None:
                # Object disappeared between LIST and GET — skip silently.
                continue
            record = _decode_record(body, object_key)
            if record is not None:
                records.append(record)
        records.sort(key=lambda r: r.created_at, reverse=True)
        return records

    # ---------------------------------------------------------------- revoke

    async def revoke(self, key_id: str) -> bool:
        """Delete the API key whose `key_id` matches. Returns True on hit."""
        object_keys = await self._s3.list_object_keys(_KEY_PREFIX)
        for object_key in object_keys:
            body = await self._s3.download_tile(object_key)
            if body is None:
                continue
            record = _decode_record(body, object_key)
            if record is None or record.key_id != key_id:
                continue
            await self._s3.delete_object(object_key)
            # Drop any cached positive validation for this hash so the next
            # request observes the revocation immediately on this worker.
            key_hash = _extract_hash_from_object_key(object_key)
            if key_hash is not None:
                async with self._cache_lock:
                    self._validation_cache.pop(key_hash, None)
            return True
        return False

    # ---------------------------------------------------------------- validate

    async def is_valid(self, presented_secret: str) -> bool:
        """
        Check whether the presented secret matches a stored hash.

        Positive results are cached in-process for ~60 s to keep `is_valid`
        off S3 on the hot path. Updates `last_used_at` on cache miss (best
        effort: an S3 hiccup never rejects an otherwise-valid request).
        """
        if not presented_secret:
            return False
        key_hash = _hash_key(presented_secret)

        now = time.time()
        async with self._cache_lock:
            expiry = self._validation_cache.get(key_hash)
            if expiry is not None and expiry > now:
                return True

        body = await self._s3.download_tile(_object_key(key_hash))
        if body is None:
            return False
        record = _decode_record(body, _object_key(key_hash))
        if record is None:
            return False

        async with self._cache_lock:
            self._validation_cache[key_hash] = now + _VALIDATION_CACHE_TTL_SECONDS

        await self._best_effort_touch(key_hash, record, now)
        return True

    async def _best_effort_touch(
        self, key_hash: str, record: ApiKeyRecord, now: float
    ) -> None:
        updated = _encode_record(
            key_id=record.key_id,
            label=record.label,
            created_at=record.created_at,
            last_used_at=int(now),
        )
        try:
            await self._s3.upload_tile(
                _object_key(key_hash), updated, content_type="application/json"
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "Failed to update last_used_at for key_id=%s: %s",
                record.key_id,
                exc,
            )


def _encode_record(
    key_id: str, label: str, created_at: int, last_used_at: Optional[int]
) -> bytes:
    payload = {
        "key_id": key_id,
        "label": label,
        "created_at": created_at,
        "last_used_at": last_used_at,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _decode_record(body: bytes, object_key: str) -> Optional[ApiKeyRecord]:
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        logger.warning("Skipping unparseable keystore object %s: %s", object_key, exc)
        return None
    if not isinstance(data, dict):
        logger.warning("Keystore object %s is not a JSON object; skipping", object_key)
        return None
    try:
        return ApiKeyRecord(
            key_id=str(data["key_id"]),
            label=str(data["label"]),
            created_at=int(data["created_at"]),
            last_used_at=(
                int(data["last_used_at"])
                if data.get("last_used_at") is not None
                else None
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("Malformed keystore object %s: %s", object_key, exc)
        return None


def _extract_hash_from_object_key(object_key: str) -> Optional[str]:
    if not object_key.startswith(_KEY_PREFIX) or not object_key.endswith(
        _OBJECT_SUFFIX
    ):
        return None
    return object_key[len(_KEY_PREFIX) : -len(_OBJECT_SUFFIX)] or None
