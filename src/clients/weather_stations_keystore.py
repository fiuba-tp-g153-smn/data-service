"""SQLite-backed store for weather-stations API keys (hashed)."""

import asyncio
import hashlib
import logging
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ApiKeyRecord:
    """One row in the api_keys table (never carries the plaintext secret)."""

    key_id: str
    label: str
    created_at: int
    last_used_at: Optional[int]


@dataclass(frozen=True, slots=True)
class CreatedApiKey:
    """Return value of `create`: includes the plaintext secret (returned once)."""

    key_id: str
    label: str
    secret: str
    created_at: int


_SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS api_keys (
        key_id        TEXT PRIMARY KEY,
        key_hash      TEXT NOT NULL UNIQUE,
        label         TEXT NOT NULL,
        created_at    INTEGER NOT NULL,
        last_used_at  INTEGER
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
    """,
)


def _hash_key(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


class WeatherStationsKeystore:
    """
    Async wrapper around a single `sqlite3.Connection` storing hashed API keys.

    Plaintext secrets are returned only at creation time; the DB stores only
    the SHA-256 hash. Validation is hash lookup + best-effort `last_used_at`
    update. All SQLite access is serialized by `_access_lock`; I/O is offloaded
    to a thread to keep the event loop responsive.

    Schema matches the basemap state store's conventions (`asyncio.to_thread`
    + `check_same_thread=False` + WAL journaling).
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._access_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Open the SQLite connection and create tables if missing."""
        if self._conn is not None:
            return
        path = Path(self._db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._open_and_init)
        logger.info("Weather stations keystore opened at %s", self._db_path)

    def _open_and_init(self) -> None:
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        for ddl in _SCHEMA_SQL:
            conn.execute(ddl)
        self._conn = conn

    async def close(self) -> None:
        if self._conn is None:
            return
        await asyncio.to_thread(self._conn.close)
        self._conn = None
        logger.info("Weather stations keystore closed")

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("WeatherStationsKeystore not connected")
        return self._conn

    async def create(self, label: str) -> CreatedApiKey:
        """Mint a new API key. Returns the plaintext secret exactly once."""
        secret = secrets.token_urlsafe(32)
        key_id = secrets.token_hex(8)
        key_hash = _hash_key(secret)
        created_at = int(time.time())
        async with self._access_lock:
            await asyncio.to_thread(
                self._insert_sync, key_id, key_hash, label, created_at
            )
        return CreatedApiKey(
            key_id=key_id, label=label, secret=secret, created_at=created_at
        )

    def _insert_sync(
        self, key_id: str, key_hash: str, label: str, created_at: int
    ) -> None:
        self._require_conn().execute(
            """
            INSERT INTO api_keys (key_id, key_hash, label, created_at, last_used_at)
            VALUES (?, ?, ?, ?, NULL)
            """,
            (key_id, key_hash, label, created_at),
        )

    async def list_all(self) -> List[ApiKeyRecord]:
        """Return every API key record (without secrets)."""
        async with self._access_lock:
            return await asyncio.to_thread(self._list_all_sync)

    def _list_all_sync(self) -> List[ApiKeyRecord]:
        rows = (
            self._require_conn()
            .execute(
                "SELECT key_id, label, created_at, last_used_at "
                "FROM api_keys ORDER BY created_at DESC"
            )
            .fetchall()
        )
        return [
            ApiKeyRecord(
                key_id=str(r[0]),
                label=str(r[1]),
                created_at=int(r[2]),
                last_used_at=int(r[3]) if r[3] is not None else None,
            )
            for r in rows
        ]

    async def revoke(self, key_id: str) -> bool:
        """Delete an API key by id. Returns True if a row was removed."""
        async with self._access_lock:
            return await asyncio.to_thread(self._revoke_sync, key_id)

    def _revoke_sync(self, key_id: str) -> bool:
        cursor = self._require_conn().execute(
            "DELETE FROM api_keys WHERE key_id = ?", (key_id,)
        )
        return cursor.rowcount > 0

    async def is_valid(self, presented_secret: str) -> bool:
        """
        Check whether the presented secret matches a stored hash.

        Updates `last_used_at` on success (best-effort, ignores failures so a
        DB hiccup never rejects an otherwise-valid request).
        """
        if not presented_secret:
            return False
        key_hash = _hash_key(presented_secret)
        async with self._access_lock:
            return await asyncio.to_thread(self._is_valid_sync, key_hash)

    def _is_valid_sync(self, key_hash: str) -> bool:
        row = (
            self._require_conn()
            .execute("SELECT key_id FROM api_keys WHERE key_hash = ?", (key_hash,))
            .fetchone()
        )
        if row is None:
            return False
        try:
            self._require_conn().execute(
                "UPDATE api_keys SET last_used_at = ? WHERE key_id = ?",
                (int(time.time()), str(row[0])),
            )
        except sqlite3.Error as exc:
            logger.warning("Failed to update last_used_at for %s: %s", row[0], exc)
        return True
