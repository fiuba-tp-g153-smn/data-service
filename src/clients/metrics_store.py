"""SQLite-backed time-series store for data-service observability metrics.

Persists two independent streams so the dashboard can chart trends over time:

* ``sync_cycles`` — one row per domain per background sync/scrape cycle
  (satellite, radar, ecmwf_tp, ecmwf_mslp, wrf, basemap, weather_stations).
  The latest row per domain is the live status; the full series is the history.
* ``redis_memory_samples`` + ``redis_info_samples`` — periodic snapshots written
  by :class:`RedisMetricsService` (memory-by-domain breakdown + overall INFO).

Mirrors the cold-storage pattern of :class:`BasemapStateStore`: a single
``sqlite3.Connection`` in WAL mode, all access serialized by ``_access_lock`` and
offloaded via ``asyncio.to_thread`` so the event loop never blocks. Schema is
embedded (``CREATE TABLE IF NOT EXISTS``) — this service has no Alembic.
"""

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SyncCycleRow:
    """One completed sync/scrape cycle for a single domain."""

    domain: str
    started_at: str
    finished_at: str
    duration_ms: int
    downloaded: int
    errors: int
    outcome: str


@dataclass(frozen=True, slots=True)
class SyncHistoryBucket:
    """Aggregated sync activity for one (time bucket, domain) pair."""

    bucket: str
    domain: str
    cycles: int
    downloaded: int
    errors: int
    avg_duration_ms: float


@dataclass(frozen=True, slots=True)
class MemoryDomainSample:
    """Redis memory usage for a single domain within one sample."""

    domain: str
    key_count: int
    memory_bytes: int


@dataclass(frozen=True, slots=True)
class MemorySamplePoint:
    """A single (timestamp, domain) memory data point for the history series."""

    sampled_at: str
    domain: str
    key_count: int
    memory_bytes: int


@dataclass(frozen=True, slots=True)
class InfoSample:
    """A snapshot of overall Redis INFO stats at one point in time."""

    sampled_at: str
    used_memory: Optional[int]
    used_memory_rss: Optional[int]
    used_memory_peak: Optional[int]
    maxmemory: Optional[int]
    mem_fragmentation_ratio: Optional[float]
    evicted_keys: Optional[int]
    expired_keys: Optional[int]
    keyspace_hits: Optional[int]
    keyspace_misses: Optional[int]
    connected_clients: Optional[int]
    total_keys: Optional[int]


_INFO_FIELDS = (
    "used_memory",
    "used_memory_rss",
    "used_memory_peak",
    "maxmemory",
    "mem_fragmentation_ratio",
    "evicted_keys",
    "expired_keys",
    "keyspace_hits",
    "keyspace_misses",
    "connected_clients",
    "total_keys",
)


_SCHEMA_SQL = (
    """
    CREATE TABLE IF NOT EXISTS sync_cycles (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        domain      TEXT    NOT NULL,
        started_at  TEXT    NOT NULL,
        finished_at TEXT    NOT NULL,
        duration_ms INTEGER NOT NULL,
        downloaded  INTEGER NOT NULL DEFAULT 0,
        errors      INTEGER NOT NULL DEFAULT 0,
        outcome     TEXT    NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sync_cycles_domain_finished
        ON sync_cycles(domain, finished_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_sync_cycles_finished
        ON sync_cycles(finished_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS redis_memory_samples (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        sampled_at   TEXT    NOT NULL,
        domain       TEXT    NOT NULL,
        key_count    INTEGER NOT NULL,
        memory_bytes INTEGER NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_memory_sampled
        ON redis_memory_samples(sampled_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_memory_domain_sampled
        ON redis_memory_samples(domain, sampled_at);
    """,
    """
    CREATE TABLE IF NOT EXISTS redis_info_samples (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        sampled_at              TEXT    NOT NULL,
        used_memory             INTEGER,
        used_memory_rss         INTEGER,
        used_memory_peak        INTEGER,
        maxmemory               INTEGER,
        mem_fragmentation_ratio REAL,
        evicted_keys            INTEGER,
        expired_keys            INTEGER,
        keyspace_hits           INTEGER,
        keyspace_misses         INTEGER,
        connected_clients       INTEGER,
        total_keys              INTEGER
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_info_sampled
        ON redis_info_samples(sampled_at);
    """,
)


class MetricsStore:
    """
    Async wrapper over a single ``sqlite3.Connection`` persisting metrics.

    All SQLite access is serialized by ``_access_lock`` (the connection is opened
    with ``check_same_thread=False`` and shared across the thread pool, so
    concurrent statement handles can corrupt ``fetch*`` results). Blocking I/O is
    offloaded via ``asyncio.to_thread``.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._access_lock = asyncio.Lock()

    async def connect(self) -> None:
        """Open the SQLite connection and create tables if missing."""
        if self._conn is not None:
            return

        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._open_and_init)
        logger.info("Metrics store opened at %s", self._db_path)

    def _open_and_init(self) -> None:
        """Blocking: open connection, apply pragmas, create schema."""
        conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
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
        logger.info("Metrics store closed")

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("MetricsStore not connected")
        return self._conn

    # ============== Writes ==============

    async def record_sync_cycle(  # pylint: disable=too-many-arguments
        self,
        domain: str,
        started_at: str,
        finished_at: str,
        duration_ms: int,
        downloaded: int,
        errors: int,
        outcome: str,
    ) -> None:
        """Append one completed sync cycle row for a domain."""
        async with self._access_lock:
            await asyncio.to_thread(
                self._record_sync_cycle_sync,
                domain,
                started_at,
                finished_at,
                duration_ms,
                downloaded,
                errors,
                outcome,
            )

    def _record_sync_cycle_sync(  # pylint: disable=too-many-arguments
        self,
        domain: str,
        started_at: str,
        finished_at: str,
        duration_ms: int,
        downloaded: int,
        errors: int,
        outcome: str,
    ) -> None:
        self._require_conn().execute(
            """
            INSERT INTO sync_cycles
                (domain, started_at, finished_at, duration_ms,
                 downloaded, errors, outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (domain, started_at, finished_at, duration_ms, downloaded, errors, outcome),
        )

    async def record_memory_sample(
        self, sampled_at: str, rows: Sequence[Tuple[str, int, int]]
    ) -> None:
        """Insert a full memory-by-domain breakdown for one sample timestamp."""
        if not rows:
            return
        async with self._access_lock:
            await asyncio.to_thread(self._record_memory_sample_sync, sampled_at, rows)

    def _record_memory_sample_sync(
        self, sampled_at: str, rows: Sequence[Tuple[str, int, int]]
    ) -> None:
        self._require_conn().executemany(
            """
            INSERT INTO redis_memory_samples
                (sampled_at, domain, key_count, memory_bytes)
            VALUES (?, ?, ?, ?)
            """,
            [(sampled_at, domain, kc, mb) for (domain, kc, mb) in rows],
        )

    async def record_info_sample(
        self, sampled_at: str, info: Dict[str, object]
    ) -> None:
        """Insert one overall Redis INFO snapshot."""
        async with self._access_lock:
            await asyncio.to_thread(self._record_info_sample_sync, sampled_at, info)

    def _record_info_sample_sync(
        self, sampled_at: str, info: Dict[str, object]
    ) -> None:
        values = [sampled_at] + [info.get(field) for field in _INFO_FIELDS]
        placeholders = ", ".join(["?"] * len(values))
        columns = "sampled_at, " + ", ".join(_INFO_FIELDS)
        self._require_conn().execute(
            f"INSERT INTO redis_info_samples ({columns}) VALUES ({placeholders})",
            values,
        )

    async def prune(self, before_iso: str) -> None:
        """Delete rows older than ``before_iso`` across every table."""
        async with self._access_lock:
            await asyncio.to_thread(self._prune_sync, before_iso)

    def _prune_sync(self, before_iso: str) -> None:
        conn = self._require_conn()
        conn.execute("DELETE FROM sync_cycles WHERE finished_at < ?", (before_iso,))
        conn.execute(
            "DELETE FROM redis_memory_samples WHERE sampled_at < ?", (before_iso,)
        )
        conn.execute(
            "DELETE FROM redis_info_samples WHERE sampled_at < ?", (before_iso,)
        )

    # ============== Sync-cycle reads ==============

    async def get_latest_sync_per_domain(self) -> List[SyncCycleRow]:
        """Return the most recent cycle row for each domain."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_latest_sync_per_domain_sync)

    def _get_latest_sync_per_domain_sync(self) -> List[SyncCycleRow]:
        rows = self._require_conn().execute("""
            SELECT domain, started_at, finished_at, duration_ms,
                   downloaded, errors, outcome
            FROM sync_cycles
            WHERE id IN (
                SELECT MAX(id) FROM sync_cycles GROUP BY domain
            )
            ORDER BY domain
            """).fetchall()
        return [self._to_sync_row(r) for r in rows]

    async def get_sync_cycles(
        self,
        since_iso: str,
        domain: Optional[str] = None,
        limit: int = 200,
        before_iso: Optional[str] = None,
    ) -> List[SyncCycleRow]:
        """Return recent raw cycle rows, newest first, optionally domain-filtered.

        `before_iso` bounds the window above (finished_at < before) for lazy
        timeline chunks; `limit <= 0` returns every row in the window.
        """
        async with self._access_lock:
            return await asyncio.to_thread(
                self._get_sync_cycles_sync, since_iso, domain, limit, before_iso
            )

    def _get_sync_cycles_sync(
        self,
        since_iso: str,
        domain: Optional[str],
        limit: int,
        before_iso: Optional[str],
    ) -> List[SyncCycleRow]:
        params: List[object] = [since_iso]
        sql = (
            "SELECT domain, started_at, finished_at, duration_ms, "
            "downloaded, errors, outcome FROM sync_cycles WHERE finished_at >= ?"
        )
        if before_iso:
            sql += " AND finished_at < ?"
            params.append(before_iso)
        if domain:
            sql += " AND domain = ?"
            params.append(domain)
        sql += " ORDER BY finished_at DESC"
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        rows = self._require_conn().execute(sql, params).fetchall()
        return [self._to_sync_row(r) for r in rows]

    async def get_sync_history(
        self, since_iso: str, bucket: str = "hour", domain: Optional[str] = None
    ) -> List[SyncHistoryBucket]:
        """Aggregate cycles into time buckets per domain (for trend charts)."""
        async with self._access_lock:
            return await asyncio.to_thread(
                self._get_sync_history_sync, since_iso, bucket, domain
            )

    def _get_sync_history_sync(
        self, since_iso: str, bucket: str, domain: Optional[str]
    ) -> List[SyncHistoryBucket]:
        # ISO8601 UTC strings bucket cleanly by prefix length: 13 = "YYYY-MM-DDTHH"
        # (hourly), 10 = "YYYY-MM-DD" (daily). Same trick the tiles-processor uses.
        prefix_len = 10 if bucket == "day" else 13
        params: List[object] = [prefix_len, since_iso]
        sql = (
            "SELECT substr(finished_at, 1, ?) AS bucket, domain, "
            "COUNT(*) AS cycles, SUM(downloaded) AS downloaded, "
            "SUM(errors) AS errors, AVG(duration_ms) AS avg_duration_ms "
            "FROM sync_cycles WHERE finished_at >= ?"
        )
        if domain:
            sql += " AND domain = ?"
            params.append(domain)
        sql += " GROUP BY bucket, domain ORDER BY bucket, domain"
        rows = self._require_conn().execute(sql, params).fetchall()
        return [
            SyncHistoryBucket(
                bucket=str(r["bucket"]),
                domain=str(r["domain"]),
                cycles=int(r["cycles"]),
                downloaded=int(r["downloaded"] or 0),
                errors=int(r["errors"] or 0),
                avg_duration_ms=float(r["avg_duration_ms"] or 0.0),
            )
            for r in rows
        ]

    # ============== Redis memory reads ==============

    async def get_latest_memory(
        self,
    ) -> Tuple[Optional[str], List[MemoryDomainSample]]:
        """Return the most recent memory sample as (sampled_at, per-domain rows)."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_latest_memory_sync)

    def _get_latest_memory_sync(
        self,
    ) -> Tuple[Optional[str], List[MemoryDomainSample]]:
        conn = self._require_conn()
        latest = conn.execute(
            "SELECT MAX(sampled_at) AS ts FROM redis_memory_samples"
        ).fetchone()
        sampled_at = latest["ts"] if latest else None
        if not sampled_at:
            return None, []
        rows = conn.execute(
            """
            SELECT domain, key_count, memory_bytes
            FROM redis_memory_samples
            WHERE sampled_at = ?
            ORDER BY memory_bytes DESC
            """,
            (sampled_at,),
        ).fetchall()
        return str(sampled_at), [
            MemoryDomainSample(
                domain=str(r["domain"]),
                key_count=int(r["key_count"]),
                memory_bytes=int(r["memory_bytes"]),
            )
            for r in rows
        ]

    async def get_memory_history(
        self, since_iso: str, domain: Optional[str] = None
    ) -> List[MemorySamplePoint]:
        """Return memory data points since ``since_iso`` for the growth chart."""
        async with self._access_lock:
            return await asyncio.to_thread(
                self._get_memory_history_sync, since_iso, domain
            )

    def _get_memory_history_sync(
        self, since_iso: str, domain: Optional[str]
    ) -> List[MemorySamplePoint]:
        params: List[object] = [since_iso]
        sql = (
            "SELECT sampled_at, domain, key_count, memory_bytes "
            "FROM redis_memory_samples WHERE sampled_at >= ?"
        )
        if domain:
            sql += " AND domain = ?"
            params.append(domain)
        sql += " ORDER BY sampled_at, domain"
        rows = self._require_conn().execute(sql, params).fetchall()
        return [
            MemorySamplePoint(
                sampled_at=str(r["sampled_at"]),
                domain=str(r["domain"]),
                key_count=int(r["key_count"]),
                memory_bytes=int(r["memory_bytes"]),
            )
            for r in rows
        ]

    # ============== Redis INFO reads ==============

    async def get_latest_info(self) -> Optional[InfoSample]:
        """Return the most recent INFO snapshot, if any."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_latest_info_sync)

    def _get_latest_info_sync(self) -> Optional[InfoSample]:
        row = (
            self._require_conn()
            .execute("SELECT * FROM redis_info_samples ORDER BY id DESC LIMIT 1")
            .fetchone()
        )
        return self._to_info_sample(row) if row else None

    async def get_info_history(self, since_iso: str) -> List[InfoSample]:
        """Return INFO snapshots since ``since_iso`` (used_memory trend etc.)."""
        async with self._access_lock:
            return await asyncio.to_thread(self._get_info_history_sync, since_iso)

    def _get_info_history_sync(self, since_iso: str) -> List[InfoSample]:
        rows = (
            self._require_conn()
            .execute(
                "SELECT * FROM redis_info_samples WHERE sampled_at >= ? ORDER BY sampled_at",
                (since_iso,),
            )
            .fetchall()
        )
        return [self._to_info_sample(r) for r in rows]

    # ============== Row mappers ==============

    @staticmethod
    def _to_sync_row(r: sqlite3.Row) -> SyncCycleRow:
        return SyncCycleRow(
            domain=str(r["domain"]),
            started_at=str(r["started_at"]),
            finished_at=str(r["finished_at"]),
            duration_ms=int(r["duration_ms"]),
            downloaded=int(r["downloaded"]),
            errors=int(r["errors"]),
            outcome=str(r["outcome"]),
        )

    @staticmethod
    def _to_info_sample(r: sqlite3.Row) -> InfoSample:
        def _opt_int(key: str) -> Optional[int]:
            value = r[key]
            return int(value) if value is not None else None

        frag = r["mem_fragmentation_ratio"]
        return InfoSample(
            sampled_at=str(r["sampled_at"]),
            used_memory=_opt_int("used_memory"),
            used_memory_rss=_opt_int("used_memory_rss"),
            used_memory_peak=_opt_int("used_memory_peak"),
            maxmemory=_opt_int("maxmemory"),
            mem_fragmentation_ratio=float(frag) if frag is not None else None,
            evicted_keys=_opt_int("evicted_keys"),
            expired_keys=_opt_int("expired_keys"),
            keyspace_hits=_opt_int("keyspace_hits"),
            keyspace_misses=_opt_int("keyspace_misses"),
            connected_clients=_opt_int("connected_clients"),
            total_keys=_opt_int("total_keys"),
        )
