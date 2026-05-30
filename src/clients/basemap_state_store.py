"""SQLite-backed cold storage for basemap scraper resume state."""

import asyncio
import logging
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Cursor:
    """Resume position for a single provider: next `(zoom, tile_index)` to process."""

    zoom: int
    tile_index: int


@dataclass(frozen=True, slots=True)
class ProviderHealth:
    """
    Circuit-breaker state for a single provider.

    When `cooldown_until` is in the future, the scraper skips the provider
    for this cycle; the number of `consecutive_trips` drives exponential
    backoff in :meth:`BasemapScraperService._compute_cooldown`. A clean
    sweep completion deletes the row (state = closed).
    """

    consecutive_trips: int
    cooldown_until: int
    last_tripped_at: int
    last_reason: str


_SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS basemap_scrape_cursor (
        provider_id TEXT PRIMARY KEY,
        zoom        INTEGER NOT NULL,
        tile_index  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS basemap_scrape_failed (
        provider_id TEXT NOT NULL,
        zoom        INTEGER NOT NULL,
        x           INTEGER NOT NULL,
        y           INTEGER NOT NULL,
        PRIMARY KEY (provider_id, zoom, x, y)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_failed_scope
        ON basemap_scrape_failed(provider_id, zoom);
    """,
    """
    CREATE TABLE IF NOT EXISTS basemap_scrape_last_completed (
        provider_id  TEXT PRIMARY KEY,
        completed_at INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS basemap_provider_health (
        provider_id       TEXT PRIMARY KEY,
        consecutive_trips INTEGER NOT NULL,
        cooldown_until    INTEGER NOT NULL,
        last_tripped_at   INTEGER NOT NULL,
        last_reason       TEXT    NOT NULL,
        updated_at        INTEGER NOT NULL
    );
    """,
)


class BasemapStateStore:
    """
    Async wrapper over a single `sqlite3.Connection` persisting scraper progress.

    State is cold (on-disk) so it survives process restarts and Redis flushes.
    A completed sweep clears its rows; an interrupted sweep keeps them until
    resumed. All SQLite access is serialized by `_access_lock`: the connection
    is opened with `check_same_thread=False` and shared across the thread
    pool, so concurrent reads on a single statement handle can corrupt
    `fetchone()`/`fetchall()` results. I/O is offloaded via `asyncio.to_thread`
    to avoid blocking the event loop.
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
        logger.info("Basemap state store opened at %s", self._db_path)

    def _open_and_init(self) -> None:
        """Blocking: open connection, apply pragmas, create schema."""
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
        """Close the SQLite connection."""
        if self._conn is None:
            return
        await asyncio.to_thread(self._conn.close)
        self._conn = None
        logger.info("Basemap state store closed")

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("BasemapStateStore not connected")
        return self._conn

    async def get_cursor(self, provider_id: str) -> Optional[Cursor]:
        """Return the stored resume position for a provider, if any."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_cursor_sync, provider_id)

    def _get_cursor_sync(self, provider_id: str) -> Optional[Cursor]:
        row = (
            self._require_conn()
            .execute(
                "SELECT zoom, tile_index FROM basemap_scrape_cursor WHERE provider_id = ?",
                (provider_id,),
            )
            .fetchone()
        )
        if row is None:
            return None
        return Cursor(zoom=int(row[0]), tile_index=int(row[1]))

    async def set_cursor(self, provider_id: str, zoom: int, tile_index: int) -> None:
        """Upsert the resume position for a provider."""
        async with self._access_lock:
            await asyncio.to_thread(
                self._set_cursor_sync, provider_id, zoom, tile_index
            )

    def _set_cursor_sync(self, provider_id: str, zoom: int, tile_index: int) -> None:
        self._require_conn().execute(
            """
            INSERT INTO basemap_scrape_cursor (provider_id, zoom, tile_index, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(provider_id) DO UPDATE SET
                zoom       = excluded.zoom,
                tile_index = excluded.tile_index,
                updated_at = excluded.updated_at
            """,
            (provider_id, zoom, tile_index, int(time.time())),
        )

    async def clear_cursor(self, provider_id: str) -> None:
        """Delete the cursor row for a provider."""
        async with self._access_lock:
            await asyncio.to_thread(self._clear_cursor_sync, provider_id)

    def _clear_cursor_sync(self, provider_id: str) -> None:
        self._require_conn().execute(
            "DELETE FROM basemap_scrape_cursor WHERE provider_id = ?",
            (provider_id,),
        )

    async def add_failed(self, provider_id: str, zoom: int, x: int, y: int) -> None:
        """Record a failed tile for later retry (idempotent)."""
        async with self._access_lock:
            await asyncio.to_thread(self._add_failed_sync, provider_id, zoom, x, y)

    def _add_failed_sync(self, provider_id: str, zoom: int, x: int, y: int) -> None:
        self._require_conn().execute(
            """
            INSERT OR IGNORE INTO basemap_scrape_failed (provider_id, zoom, x, y)
            VALUES (?, ?, ?, ?)
            """,
            (provider_id, zoom, x, y),
        )

    async def remove_failed(self, provider_id: str, zoom: int, x: int, y: int) -> None:
        """Remove a failed tile entry after a successful retry."""
        async with self._access_lock:
            await asyncio.to_thread(self._remove_failed_sync, provider_id, zoom, x, y)

    def _remove_failed_sync(self, provider_id: str, zoom: int, x: int, y: int) -> None:
        self._require_conn().execute(
            """
            DELETE FROM basemap_scrape_failed
            WHERE provider_id = ? AND zoom = ? AND x = ? AND y = ?
            """,
            (provider_id, zoom, x, y),
        )

    async def list_failed(self, provider_id: str, zoom: int) -> List[Tuple[int, int]]:
        """Return all `(x, y)` pairs previously recorded as failed at this zoom."""
        async with self._access_lock:
            return await asyncio.to_thread(self._list_failed_sync, provider_id, zoom)

    def _list_failed_sync(self, provider_id: str, zoom: int) -> List[Tuple[int, int]]:
        rows = (
            self._require_conn()
            .execute(
                """
            SELECT x, y FROM basemap_scrape_failed
            WHERE provider_id = ? AND zoom = ?
            """,
                (provider_id, zoom),
            )
            .fetchall()
        )
        return [(int(r[0]), int(r[1])) for r in rows]

    async def clear_failed(self, provider_id: str, zoom: int) -> None:
        """Drop all failed-tile entries for a single (provider, zoom)."""
        async with self._access_lock:
            await asyncio.to_thread(self._clear_failed_sync, provider_id, zoom)

    def _clear_failed_sync(self, provider_id: str, zoom: int) -> None:
        self._require_conn().execute(
            """
            DELETE FROM basemap_scrape_failed
            WHERE provider_id = ? AND zoom = ?
            """,
            (provider_id, zoom),
        )

    async def clear_failed_for_provider(self, provider_id: str) -> None:
        """Drop all failed-tile entries for a provider across every zoom."""
        async with self._access_lock:
            await asyncio.to_thread(self._clear_failed_for_provider_sync, provider_id)

    def _clear_failed_for_provider_sync(self, provider_id: str) -> None:
        self._require_conn().execute(
            "DELETE FROM basemap_scrape_failed WHERE provider_id = ?",
            (provider_id,),
        )

    async def get_last_completed(self, provider_id: str) -> Optional[int]:
        """Return the last completion timestamp (unix seconds) for a provider."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_last_completed_sync, provider_id)

    def _get_last_completed_sync(self, provider_id: str) -> Optional[int]:
        row = (
            self._require_conn()
            .execute(
                "SELECT completed_at FROM basemap_scrape_last_completed WHERE provider_id = ?",
                (provider_id,),
            )
            .fetchone()
        )
        if row is None:
            return None
        return int(row[0])

    async def set_last_completed(self, provider_id: str, timestamp: int) -> None:
        """Upsert the last completion timestamp for a provider."""
        async with self._access_lock:
            await asyncio.to_thread(
                self._set_last_completed_sync, provider_id, timestamp
            )

    def _set_last_completed_sync(self, provider_id: str, timestamp: int) -> None:
        self._require_conn().execute(
            """
            INSERT INTO basemap_scrape_last_completed (provider_id, completed_at)
            VALUES (?, ?)
            ON CONFLICT(provider_id) DO UPDATE SET
                completed_at = excluded.completed_at
            """,
            (provider_id, timestamp),
        )

    async def get_health(self, provider_id: str) -> Optional[ProviderHealth]:
        """Return the circuit-breaker state for a provider, or None if closed."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_health_sync, provider_id)

    def _get_health_sync(self, provider_id: str) -> Optional[ProviderHealth]:
        row = (
            self._require_conn()
            .execute(
                """
                SELECT consecutive_trips, cooldown_until, last_tripped_at, last_reason
                FROM basemap_provider_health
                WHERE provider_id = ?
                """,
                (provider_id,),
            )
            .fetchone()
        )
        if row is None:
            return None
        return ProviderHealth(
            consecutive_trips=int(row[0]),
            cooldown_until=int(row[1]),
            last_tripped_at=int(row[2]),
            last_reason=str(row[3]),
        )

    async def open_circuit(
        self,
        provider_id: str,
        consecutive_trips: int,
        cooldown_until: int,
        reason: str,
    ) -> None:
        """Upsert the health row marking the provider's circuit open."""
        async with self._access_lock:
            await asyncio.to_thread(
                self._open_circuit_sync,
                provider_id,
                consecutive_trips,
                cooldown_until,
                reason,
            )

    def _open_circuit_sync(
        self,
        provider_id: str,
        consecutive_trips: int,
        cooldown_until: int,
        reason: str,
    ) -> None:
        now = int(time.time())
        self._require_conn().execute(
            """
            INSERT INTO basemap_provider_health (
                provider_id, consecutive_trips, cooldown_until,
                last_tripped_at, last_reason, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_id) DO UPDATE SET
                consecutive_trips = excluded.consecutive_trips,
                cooldown_until    = excluded.cooldown_until,
                last_tripped_at   = excluded.last_tripped_at,
                last_reason       = excluded.last_reason,
                updated_at        = excluded.updated_at
            """,
            (provider_id, consecutive_trips, cooldown_until, now, reason, now),
        )

    async def close_circuit(self, provider_id: str) -> None:
        """Clear the health row: provider is healthy again."""
        async with self._access_lock:
            await asyncio.to_thread(self._close_circuit_sync, provider_id)

    def _close_circuit_sync(self, provider_id: str) -> None:
        self._require_conn().execute(
            "DELETE FROM basemap_provider_health WHERE provider_id = ?",
            (provider_id,),
        )
