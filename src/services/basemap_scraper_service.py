"""Background scraper that builds the basemap tile backup from external providers."""

# pylint: disable=too-many-lines

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from logging import Logger
from typing import Deque, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx
from botocore.exceptions import BotoCoreError, ClientError

from clients.basemap_state_store import BasemapStateStore
from clients.http_tile_client import HttpTileClient, ProviderUnavailableError
from clients.metrics_store import MetricsStore
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from services.basemap_config import (
    BasemapProvider,
    BoundingBox,
    build_source_url,
    iter_tiles,
)
from services.storage_circuit import CircuitTransition, StorageCircuit
from settings import Settings

_PROGRESS_PCT_STEP = 10
_PROGRESS_TIME_INTERVAL_S = 30.0
_PROGRESS_MIN_INTERVAL_S = 1.0
# Short floor applied to the next-cycle sleep when any provider hit storage
# errors this cycle. Keeps the scraper probing ~every minute so a transient
# S3/Redis outage recovers automatically without waiting a full scrape
# interval (7 days by default).
_STORAGE_RETRY_FLOOR_SECONDS = 60.0
# Cap on the storage circuit's exponential cooldown. A sweep pauses in place
# for at most this long between half-open probes, so a brief S3 blip costs a
# few seconds rather than the sweep's cursor.
_STORAGE_MAX_COOLDOWN_SECONDS = 30.0


class _TileOutcome(Enum):
    """Result shape of a single tile fetch from the scraper's perspective."""

    OK = "ok"  # downloaded + persisted successfully
    MISSING = "missing"  # permanent miss (404/403) — tile legitimately doesn't exist
    UNAVAILABLE = "unavailable"  # provider appears down (network / exhausted retries)
    STORAGE_ERROR = "storage"  # provider returned bytes but the S3 write failed
    STORAGE_SKIPPED = "storage_skipped"  # storage circuit open — never attempted


@dataclass
class _ProviderSweepState:
    """
    Mutable per-provider sweep state threaded through the scrape call chain.

    Accumulates ``attempted`` (OK + UNAVAILABLE fetches) and ``failed``
    (UNAVAILABLE) to drive the rate-based circuit breaker, and carries the final
    "tripped" verdict back up to ``_scrape_provider``. Scoped to one provider's
    sweep — concurrent providers (per_origin / full parallelism modes) each own
    their own instance, so no cross-talk. These same counts feed the per-provider
    last-sweep error-rate stats shown on the dashboard.

    ``storage_errors`` counts tiles whose upstream fetch succeeded but whose
    persistence to S3 failed, plus those skipped outright while the storage
    circuit was open. Non-zero at end-of-sweep = systemic downstream outage
    (not a provider health issue), so the scraper skips stamping
    ``last_completed`` and lets the next cycle retry in ~60s instead of the
    configured scrape interval.

    ``storage_abandoned`` is the harder verdict: the storage circuit stayed
    open across every probe, so the sweep gave up mid-flight. Unlike the
    run-to-the-end case the cursor is *preserved*, so the next cycle resumes
    in place rather than restarting a multi-day sweep.
    """

    attempted: int = 0
    failed: int = 0
    tripped: bool = False
    storage_abandoned: bool = False
    last_reason: str = ""
    failure_samples: List[str] = field(default_factory=list)
    storage_errors: int = 0
    # Rolling window of the most recent fetch outcomes (True = UNAVAILABLE),
    # bounded to `error_rate_min_samples`. The breaker trips on the *recent*
    # failure rate, so an early outage that later recovers ages out of the
    # window instead of pinning a healthy provider tripped for the whole sweep.
    recent: Deque[bool] = field(default_factory=deque)


@dataclass
class _SweepProgress:
    """Mutable accumulators threaded across the chunked fan-out of one zoom.

    Carried by reference through `_sweep_chunk` / `_handle_completion` so the
    watermark, counts and progress-logging cadence survive across chunks.
    """

    start: float
    total: int
    resume_index: int
    watermark: int
    last_flushed: int
    last_flush_time: float
    next_time: float
    last_log: float
    next_pct: int
    ok: int = 0
    failed: int = 0
    processed: int = 0
    done_above: Set[int] = field(default_factory=set)


def _fmt_duration(seconds: float) -> str:
    """Format a duration as e.g. '0.9s', '42s', '3m22s', '1h04m'."""
    if seconds < 60:
        return f"{seconds:.1f}s" if seconds < 10 else f"{int(seconds)}s"
    if seconds < 3600:
        minutes, secs = divmod(int(seconds), 60)
        return f"{minutes}m{secs:02d}s"
    hours, rem = divmod(int(seconds), 3600)
    minutes = rem // 60
    return f"{hours}h{minutes:02d}m"


logger = logging.getLogger(__name__)


class BasemapScraperService(BaseSyncService):
    # pylint: disable=too-many-instance-attributes
    """
    Periodic bounding-box scraper with resumable progress tracking.

    Walks the "bounding_box x zoom" range for every enabled provider and
    writes tiles into S3 (and Redis when `redis_writes_enabled=True`).
    Progress is persisted to a SQLite cold
    store so process restarts resume where the previous run left off,
    and tiles that failed download are retried on the next cycle.
    A fully-completed sweep clears all persistent state, so the next
    interval-triggered cycle starts as a fresh full scrape.

    Driven by `settings.basemap_sync_mode` (independent of the global
    `sync_mode`, which only controls satellite/radar/ECMWF). Runs in
    ``"full"``, ``"on_demand"``, and ``"no_cache"`` — only ``"relay_only"``
    skips the scraper entirely. The `redis_writes_enabled` flag controls
    whether the scraper populates Redis during the sweep; it's only True
    in ``"full"`` mode.
    """

    def __init__(
        self,
        settings: Settings,
        s3_client: S3Client,
        redis_client: RedisClient,
        http_client: HttpTileClient,
        state_store: BasemapStateStore,
        providers: dict[str, BasemapProvider],
        bbox: BoundingBox,
        tile_ttl: int,
        s3_object_ttl_days: int,
        redis_writes_enabled: bool = True,
        parallelism_mode: str = "sequential",
        metrics_store: Optional[MetricsStore] = None,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        super().__init__(
            settings=settings,
            sync_interval=settings.basemap_scrape_interval_seconds,
            service_name="BasemapScraperService",
        )
        self._s3 = s3_client
        self._redis = redis_client
        self._http = http_client
        self._state = state_store
        self._metrics_store = metrics_store
        self._providers = providers
        self._bbox = bbox
        self._tile_ttl = tile_ttl
        self._s3_object_ttl_days = s3_object_ttl_days
        self._redis_writes_enabled = redis_writes_enabled
        self._parallelism_mode = parallelism_mode
        self._cache_max_zoom = settings.basemap_cache_max_zoom
        self._checkpoint_every = settings.basemap_scrape_checkpoint_every
        self._checkpoint_seconds = settings.basemap_scrape_checkpoint_seconds
        # Chunk size for the per-zoom fan-out: caps tasks created at once so a
        # huge zoom can't flood the event loop (HTTP concurrency stays capped
        # by the client semaphore regardless).
        self._fanout_window = settings.basemap_scrape_fanout_window
        # Rate-based circuit breaker. Within one sweep the scraper pushes through
        # scattered failures and trips a provider only when its error rate
        # (failed / attempted) exceeds `error_rate_threshold`, but not before
        # `error_rate_min_samples` fetches (so a fully-down provider bails early).
        # Cooldown schedule is indexed by consecutive trip count (capped at the
        # last element) so a repeatedly-flapping provider backs off exponentially.
        self._error_rate_threshold = settings.basemap_provider_error_rate_threshold
        self._error_rate_min_samples = settings.basemap_provider_error_rate_min_samples
        self._error_rate_window = settings.basemap_provider_error_rate_window
        self._cooldown_schedule = list(settings.basemap_provider_cooldown_schedule)
        # Lifecycle policy is applied lazily inside the scrape loop (instead of
        # once at startup) so a transient S3 outage at boot self-heals on the
        # next cycle. Once the put_bucket_lifecycle_configuration call succeeds
        # the flag latches and we stop re-applying.
        self._lifecycle_applied = False
        # Set when any provider in the current cycle reports storage errors
        # so _compute_next_sleep can floor the next sleep to ~60s regardless
        # of last_completed. Reset at the top of every _run_sync.
        self._storage_retry_due = False
        # Exponential-backoff gates around the two storage backends. Shared
        # across providers because the backends are: once S3 is down, every
        # provider would otherwise rediscover it one wasted upstream fetch at
        # a time. The S3 circuit gates the whole tile (skipping the provider
        # download too); the Redis one gates only the hot-cache write-through,
        # which is not what the sweep exists to produce.
        self._s3_circuit = StorageCircuit(
            "S3", max_cooldown=_STORAGE_MAX_COOLDOWN_SECONDS
        )
        self._redis_circuit = StorageCircuit(
            "Redis", max_cooldown=_STORAGE_MAX_COOLDOWN_SECONDS
        )
        # Latched for the rest of a cycle once one provider abandons its sweep
        # to a dead storage backend, so the remaining providers skip straight
        # past instead of each burning their own probe budget.
        self._storage_down_this_cycle = False

    def _get_lock_path(self) -> str:
        return self._settings.basemap_scrape_lock_path

    def _pre_start_check(self, app_logger: Logger) -> bool:
        """Refuse to start when there are no enabled providers to scrape."""
        if not self._providers:
            app_logger.info(
                "%s not started: no enabled providers to scrape", self._service_name
            )
            return False
        return True

    def _log_started(self, app_logger: Logger) -> None:
        """Log a status summary alongside the default started message."""
        app_logger.info("%s started", self._service_name)
        asyncio.create_task(self._log_startup_summary(app_logger))

    async def _log_startup_summary(self, app_logger: Logger) -> None:
        """Emit a one-line summary of which providers are due / waiting."""
        try:
            now = int(time.time())
            interval = self._sync_interval
            due = 0
            waiting = 0
            soonest_remaining: float = float(interval)
            for pid in self._providers:
                if await self._state.get_cursor(pid) is not None:
                    due += 1
                    soonest_remaining = 0.0
                    continue
                last = await self._state.get_last_completed(pid)
                # A never-scraped provider (last is None) is due NOW — otherwise
                # the summary misreports a fresh provider as "waiting ~interval".
                remaining = 0.0 if last is None else max(0.0, (last + interval) - now)
                if remaining <= 0:
                    due += 1
                else:
                    waiting += 1
                soonest_remaining = min(soonest_remaining, remaining)
            app_logger.info(
                "Basemap scraper: %d provider(s) due, %d waiting (next due in %s)",
                due,
                waiting,
                _fmt_duration(soonest_remaining),
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            app_logger.warning("Basemap startup summary failed: %s", exc)

    async def _compute_next_sleep(self, default: float) -> float:
        """Sleep only until the soonest-due provider, floored at 60s.

        A provider in cooldown (circuit open) doesn't count as "due" — the
        soonest time we'd consider it ready again is its ``cooldown_until``.
        A cursored provider that's also in cooldown waits for the cooldown;
        a cursored healthy provider is due now.

        When the previous cycle hit storage errors (e.g. S3 down),
        short-circuit to ``_STORAGE_RETRY_FLOOR_SECONDS`` so we probe for
        recovery on a per-minute cadence instead of the full scrape interval.
        """
        if self._storage_retry_due:
            return _STORAGE_RETRY_FLOOR_SECONDS
        now = int(time.time())
        interval = self._sync_interval
        soonest = default
        any_ready = False
        for pid in self._providers:
            health = await self._state.get_health(pid)
            cooldown_remaining = (
                float(health.cooldown_until - now)
                if health and health.cooldown_until > now
                else 0.0
            )
            if cooldown_remaining > 0:
                soonest = min(soonest, cooldown_remaining)
                continue
            if await self._state.get_cursor(pid) is not None:
                any_ready = True
                continue
            last = await self._state.get_last_completed(pid)
            # last is None => never scraped => due now (don't defer a full interval).
            remaining = 0.0 if last is None else max(0.0, (last + interval) - now)
            if remaining <= 0:
                any_ready = True
                continue
            soonest = min(soonest, remaining)
        if any_ready:
            return 0.0
        return max(60.0, soonest)

    async def _run_sync(self) -> None:
        """Execute a single scrape cycle across all providers."""
        start = time.monotonic()
        # Reset the transient storage-retry flag. Any provider that hits a
        # storage error during this cycle will set it back to True and the
        # next scheduled sleep will be floored to _STORAGE_RETRY_FLOOR_SECONDS.
        self._storage_retry_due = False
        self._storage_down_this_cycle = False
        # Apply the bucket lifecycle policy if it hasn't succeeded yet. This
        # recovers from an S3-down startup: the scraper keeps probing until
        # the policy sticks, then latches and stops retrying.
        await self._ensure_lifecycle_applied()

        groups = self._build_scrape_groups()
        logger.info(
            "Basemap scrape starting: mode=%s, %d group(s): %s",
            self._parallelism_mode,
            len(groups),
            [[p.provider_id for p in g] for g in groups],
        )

        results = await asyncio.gather(*(self._scrape_group(g) for g in groups))
        total_downloaded = sum(ok for ok, _ in results)
        total_failed = sum(f for _, f in results)

        elapsed = time.monotonic() - start
        logger.info(
            "Basemap scrape complete: %d tiles downloaded, %d failed, %.1fs elapsed",
            total_downloaded,
            total_failed,
            elapsed,
        )
        # Per-provider dashboard cycles are recorded in _scrape_group so basemap
        # progress is visible *during* a sweep instead of only on full-sweep
        # completion (which can take hours and may never complete if a provider
        # like googleSatellite keeps failing).

    async def _record_cycle(
        self, started_at: datetime, downloaded: int, errors: int
    ) -> None:
        """Persist one cycle row for the dashboard. Never aborts the scrape."""
        if self._metrics_store is None:
            return
        end = datetime.now(timezone.utc)
        duration_ms = int((end - started_at).total_seconds() * 1000)
        outcome = "error" if errors else "ok"
        try:
            await self._metrics_store.record_sync_cycle(
                "basemap",
                started_at.isoformat(),
                end.isoformat(),
                duration_ms,
                downloaded,
                errors,
                outcome,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to record basemap metrics")

    async def _ensure_lifecycle_applied(self) -> None:
        """Idempotently (re)try the S3 bucket lifecycle rule until it sticks."""
        if self._lifecycle_applied:
            return
        if await self._s3.ensure_lifecycle_expiration(self._s3_object_ttl_days):
            self._lifecycle_applied = True

    def _build_scrape_groups(self) -> List[List[BasemapProvider]]:
        """Bucket enabled providers into parallel groups per the active mode."""
        providers = list(self._providers.values())
        if not providers:
            return []
        if self._parallelism_mode == "full":
            return [[p] for p in providers]
        if self._parallelism_mode == "per_origin":
            by_host: dict[str, list[BasemapProvider]] = defaultdict(list)
            for provider in providers:
                host = urlparse(provider.source_url_template).netloc
                by_host[host].append(provider)
            return list(by_host.values())
        # "sequential" (default): one group, all providers serial within it.
        return [providers]

    async def _scrape_group(self, group: List[BasemapProvider]) -> Tuple[int, int]:
        """Run a group of providers serially and aggregate their counts.

        Records a per-provider dashboard cycle so basemap progress shows up as
        each provider is scraped, rather than only when the whole sweep finishes.
        """
        ok = 0
        failed = 0
        for provider in group:
            started_at = datetime.now(timezone.utc)
            downloaded, group_failed = await self._scrape_provider(provider)
            # Skip the ones that did no work (circuit-open / not-due return (0, 0))
            # so the dashboard reflects real scraping, not idle checks.
            if downloaded or group_failed:
                await self._record_cycle(started_at, downloaded, group_failed)
            ok += downloaded
            failed += group_failed
        return ok, failed

    async def _scrape_provider(self, provider: BasemapProvider) -> tuple[int, int]:
        """Scrape all tiles for a single provider within the bounding box."""
        # pylint: disable=too-many-locals,too-many-return-statements
        # pylint: disable=too-many-statements
        if self._storage_down_this_cycle:
            # An earlier provider already exhausted the storage circuit's probe
            # schedule this cycle. Storage is shared, so the rest would each
            # rediscover the same outage at the cost of their own backoff.
            logger.info(
                "Skipping %s: storage unavailable this cycle", provider.provider_id
            )
            return 0, 0

        now = int(time.time())

        # Circuit-breaker gate: skip providers whose cooldown hasn't expired.
        # Persistent across restarts — state lives in SQLite.
        health = await self._state.get_health(provider.provider_id)
        if health is not None and health.cooldown_until > now:
            remaining = health.cooldown_until - now
            logger.info(
                "Skipping %s: circuit open (trips=%d), reopens in %s — last: %s",
                provider.provider_id,
                health.consecutive_trips,
                _fmt_duration(remaining),
                health.last_reason,
            )
            return 0, 0

        max_zoom = min(provider.cache_max_zoom, self._cache_max_zoom)
        downloaded = 0
        failed = 0

        cursor = await self._state.get_cursor(provider.provider_id)
        if cursor is None:
            last_completed = await self._state.get_last_completed(provider.provider_id)
            if last_completed is not None:
                remaining = (last_completed + self._sync_interval) - now
                if remaining > 0:
                    logger.info(
                        "Skipping %s: next scrape in %s",
                        provider.provider_id,
                        _fmt_duration(remaining),
                    )
                    return 0, 0

        zoom_start = cursor.zoom if cursor else provider.min_zoom
        index_start = cursor.tile_index if cursor else 0

        if cursor:
            logger.info(
                "Scraping %s (zoom %d-%d) — resuming at z=%d, index=%d",
                provider.provider_id,
                provider.min_zoom,
                max_zoom,
                zoom_start,
                index_start,
            )
        else:
            logger.info(
                "Scraping %s (zoom %d-%d)",
                provider.provider_id,
                provider.min_zoom,
                max_zoom,
            )

        sweep_state = _ProviderSweepState()
        for zoom in range(zoom_start, max_zoom + 1):
            resume_index = index_start if zoom == zoom_start else 0
            zoom_ok, zoom_failed = await self._scrape_zoom(
                provider, zoom, resume_index, sweep_state
            )
            downloaded += zoom_ok
            failed += zoom_failed
            if sweep_state.tripped or sweep_state.storage_abandoned:
                break

        if sweep_state.tripped:
            # Preserve cursor + failed queue so the next cycle (after the
            # cooldown) resumes from exactly where we left off.
            prior_trips = health.consecutive_trips if health else 0
            # A cycle that still moved the frontier forward (downloaded new
            # tiles) is a large-but-healthy provider, not a worsening one — reset
            # the escalation to the base cooldown instead of ratcheting toward
            # hours. Only a cycle that tripped without any progress escalates.
            trips = 1 if downloaded > 0 else prior_trips + 1
            cooldown_seconds = self._compute_cooldown(trips)
            cooldown_until = int(time.time()) + cooldown_seconds
            await self._state.open_circuit(
                provider.provider_id,
                consecutive_trips=trips,
                cooldown_until=cooldown_until,
                reason=sweep_state.last_reason,
            )
            logger.warning(
                "Provider %s circuit opened (trips=%d): cooldown %s — %s",
                provider.provider_id,
                trips,
                _fmt_duration(cooldown_seconds),
                sweep_state.last_reason,
            )
            await self._record_sweep_stats(
                provider.provider_id, downloaded, failed, completed=False
            )
            return downloaded, failed

        if sweep_state.storage_abandoned:
            await self._finish_abandoned_sweep(
                provider, sweep_state, downloaded, failed
            )
            return downloaded, failed

        if sweep_state.storage_errors > 0:
            # Sweep "completed" but downstream persistence was broken for at
            # least one tile. Don't stamp last_completed — otherwise we'd
            # silently push the next real attempt out by a full scrape
            # interval (7 days by default). Clear the cursor so the next
            # cycle starts a fresh sweep rather than picking up at
            # max_zoom+1 (which would be a no-op), but keep the failed
            # queue as a retry hint. Flag the storage-retry gate so the
            # scheduler polls on a short cadence until S3/Redis recover.
            await self._state.clear_cursor(provider.provider_id)
            self._storage_retry_due = True
            logger.warning(
                "Provider %s sweep incomplete: %d storage failures "
                "(downstream S3/Redis likely unavailable). Skipping "
                "last_completed stamp; next cycle retries in ~%ds.",
                provider.provider_id,
                sweep_state.storage_errors,
                int(_STORAGE_RETRY_FLOOR_SECONDS),
            )
            await self._record_sweep_stats(
                provider.provider_id, downloaded, failed, completed=False
            )
            return downloaded, failed

        # Provider fully scraped — clear resume state, stamp completion, and
        # clear any stale health row so the next cycle starts with a closed
        # circuit.
        await self._state.clear_cursor(provider.provider_id)
        await self._state.clear_failed_for_provider(provider.provider_id)
        await self._state.set_last_completed(provider.provider_id, int(time.time()))
        await self._state.close_circuit(provider.provider_id)
        await self._record_sweep_stats(
            provider.provider_id, downloaded, failed, completed=True
        )

        logger.info(
            "Provider %s: %d downloaded, %d failed",
            provider.provider_id,
            downloaded,
            failed,
        )
        return downloaded, failed

    async def _finish_abandoned_sweep(
        self,
        provider: BasemapProvider,
        sweep_state: _ProviderSweepState,
        downloaded: int,
        failed: int,
    ) -> None:
        """Book-keeping for a sweep that gave up on a dead storage backend.

        Unlike the ran-to-the-end case the cursor is *preserved*: the sweep
        stopped at a real position, so the next cycle resumes there instead of
        restarting a multi-day scrape. Provider health is deliberately left
        alone — the upstream did nothing wrong.
        """
        # pylint: disable=too-many-arguments
        self._storage_retry_due = True
        self._storage_down_this_cycle = True
        logger.warning(
            "Provider %s sweep abandoned: storage unavailable across every "
            "probe (%d tiles unwritten). Cursor preserved; next cycle "
            "retries in ~%ds.",
            provider.provider_id,
            sweep_state.storage_errors,
            int(_STORAGE_RETRY_FLOOR_SECONDS),
        )
        await self._record_sweep_stats(
            provider.provider_id, downloaded, failed, completed=False
        )

    def _compute_cooldown(self, consecutive_trips: int) -> int:
        """Lookup the cooldown (seconds) for the current trip count, capped."""
        if not self._cooldown_schedule:
            return 300  # defensive; schedule validation should prevent this
        idx = min(consecutive_trips - 1, len(self._cooldown_schedule) - 1)
        return int(self._cooldown_schedule[max(idx, 0)])

    async def _record_sweep_stats(
        self, provider_id: str, downloaded: int, failed: int, completed: bool
    ) -> None:
        """Persist this sweep's outcome for the dashboard error rate."""
        await self._state.set_scrape_stats(
            provider_id,
            attempted=downloaded + failed,
            ok=downloaded,
            failed=failed,
            completed=completed,
        )

    async def _scrape_zoom(
        self,
        provider: BasemapProvider,
        zoom: int,
        resume_index: int,
        sweep_state: _ProviderSweepState,
    ) -> tuple[int, int]:
        """Scrape one zoom level for a provider with retry + checkpointing."""
        retry_ok, retry_failed = await self._retry_failed_tiles(
            provider, zoom, sweep_state
        )

        if sweep_state.tripped or sweep_state.storage_abandoned:
            # Circuit tripped during failed-tile retry — don't start the
            # main sweep; _scrape_provider handles cooldown bookkeeping.
            return retry_ok, retry_failed

        coords = list(iter_tiles(zoom, self._bbox))
        total = len(coords)
        if resume_index > 0:
            logger.info(
                "%s z=%d: resuming at index %d/%d",
                provider.provider_id,
                zoom,
                resume_index,
                total,
            )
        else:
            logger.info(
                "%s z=%d: starting (%d tiles in bbox)",
                provider.provider_id,
                zoom,
                total,
            )

        sweep_ok, sweep_failed = await self._run_indexed_sweep(
            provider, zoom, coords, resume_index, sweep_state
        )

        # End-of-zoom cleanup: if no failures remain, drop the zoom's failed rows.
        remaining = await self._state.list_failed(provider.provider_id, zoom)
        if not remaining:
            await self._state.clear_failed(provider.provider_id, zoom)

        ok = retry_ok + sweep_ok
        failed = retry_failed + sweep_failed
        processed = total  # count_tiles-equivalent; for the final log line
        logger.info(
            "%s z=%d: done (%d tiles swept, %d ok, %d failed incl. %d retry-hits)",
            provider.provider_id,
            zoom,
            processed,
            ok,
            failed,
            retry_ok,
        )
        return ok, failed

    async def _retry_failed_tiles(
        self,
        provider: BasemapProvider,
        zoom: int,
        sweep_state: _ProviderSweepState,
    ) -> tuple[int, int]:
        """Drain previously-failed tiles for this (provider, zoom). Returns (ok, failed).

        Short-circuits when the circuit-breaker trips so we stop pounding a
        known-unhealthy provider.
        """
        failed_tiles = await self._state.list_failed(provider.provider_id, zoom)
        if not failed_tiles:
            return 0, 0

        logger.info(
            "%s z=%d: retrying %d previously-failed tiles",
            provider.provider_id,
            zoom,
            len(failed_tiles),
        )
        ok = 0
        failed = 0
        for x, y in failed_tiles:
            outcome = await self._download_and_store(provider, zoom, x, y)
            self._update_sweep_state(sweep_state, outcome, zoom, x, y)
            if outcome is _TileOutcome.OK:
                await self._state.remove_failed(provider.provider_id, zoom, x, y)
                ok += 1
            else:
                failed += 1
            if sweep_state.tripped or sweep_state.storage_abandoned:
                break
        return ok, failed

    def _update_sweep_state(
        self,
        sweep_state: _ProviderSweepState,
        outcome: _TileOutcome,
        z: int,
        x: int,
        y: int,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Accrue the recent-window error rate and flip ``tripped`` once exceeded.

        Trips only once the rolling window holds `error_rate_min_samples` fetches
        AND their failure rate exceeds `error_rate_threshold` — so scattered
        failures ride through, a genuinely-broken provider still bails early, and
        an early outage that recovers ages out of the window instead of pinning
        the provider tripped for the rest of the sweep.
        """
        if outcome is _TileOutcome.MISSING:
            # Legitimately-missing tiles (404/403) aren't health signals — a
            # sparse bbox would distort the rate otherwise. Excluded entirely.
            return
        if outcome in (_TileOutcome.STORAGE_ERROR, _TileOutcome.STORAGE_SKIPPED):
            # Downstream persistence issue (S3), not provider health. Track
            # separately so the post-sweep accounting can defer the
            # last_completed stamp instead of letting a storage outage
            # silently push the next sweep out by a full interval.
            sweep_state.storage_errors += 1
            if self._s3_circuit.exhausted():
                # Every probe across the backoff schedule failed. Waiting
                # longer in-sweep buys nothing; hand back to the cycle
                # scheduler, which retries at the ~60s storage floor.
                sweep_state.storage_abandoned = True
            return

        # OK and UNAVAILABLE are both definitive fetch attempts.
        is_failure = outcome is _TileOutcome.UNAVAILABLE
        sweep_state.attempted += 1
        if is_failure:
            sweep_state.failed += 1
            if len(sweep_state.failure_samples) < 3:
                sweep_state.failure_samples.append(f"z={z} x={x} y={y}")

        window = sweep_state.recent
        window.append(is_failure)
        if len(window) > self._error_rate_window:
            window.popleft()

        if (
            not sweep_state.tripped
            and len(window) >= self._error_rate_min_samples
            and sum(window) / len(window) > self._error_rate_threshold
        ):
            sweep_state.tripped = True
            recent_failed = sum(window)
            rate = recent_failed / len(window)
            sweep_state.last_reason = (
                f"tasa de error reciente {rate:.1%} "
                f"({recent_failed}/{len(window)} de las últimas fetches; "
                f"samples: {', '.join(sweep_state.failure_samples)})"
            )

    async def _run_indexed_sweep(
        self,
        provider: BasemapProvider,
        zoom: int,
        coords: List[Tuple[int, int, int]],
        resume_index: int,
        sweep_state: _ProviderSweepState,
    ) -> tuple[int, int]:
        """Fan out the main sweep in bounded chunks with watermark checkpointing.

        Tiles are dispatched ``_fanout_window`` at a time instead of all at once
        so a huge zoom can't flood the event loop with tasks. Each chunk drains
        fully before the next starts, so the watermark advances contiguously and
        resume stays correct.
        """
        # pylint: disable=too-many-arguments
        total = len(coords)
        if resume_index >= total:
            # Nothing left at this zoom; advance cursor to next zoom boundary.
            await self._state.set_cursor(provider.provider_id, zoom + 1, 0)
            return 0, 0

        start = time.monotonic()
        progress = _SweepProgress(
            start=start,
            total=total,
            resume_index=resume_index,
            watermark=resume_index,
            last_flushed=resume_index,
            last_flush_time=start,
            next_time=start + _PROGRESS_TIME_INTERVAL_S,
            last_log=start,
            next_pct=_PROGRESS_PCT_STEP,
        )

        try:
            for chunk_start in range(resume_index, total, self._fanout_window):
                # Pause in place while storage is down rather than spinning
                # through the remaining tiles as instant skips — that would
                # burn the whole sweep during a blip S3 recovers from in
                # seconds. Chunk boundaries are the natural pause point: the
                # previous chunk's tasks have already drained.
                await self._await_storage_cooldown()
                if sweep_state.storage_abandoned:
                    break
                chunk_end = min(chunk_start + self._fanout_window, total)
                await self._sweep_chunk(
                    provider,
                    zoom,
                    coords,
                    chunk_start,
                    chunk_end,
                    sweep_state,
                    progress,
                )
                if sweep_state.tripped or sweep_state.storage_abandoned:
                    break
        finally:
            # Cancellation-safe: checkpoint the current watermark before
            # propagating CancelledError. Also advances cursor to next zoom
            # on a clean finish (watermark == total).
            next_cursor_zoom = zoom + 1 if progress.watermark >= total else zoom
            next_cursor_index = 0 if progress.watermark >= total else progress.watermark
            await self._state.set_cursor(
                provider.provider_id, next_cursor_zoom, next_cursor_index
            )

        elapsed = time.monotonic() - start
        rate = progress.processed / elapsed if elapsed > 0 else 0.0
        logger.info(
            "%s z=%d: swept %d (%d ok, %d failed, %s, %.1f tiles/s)",
            provider.provider_id,
            zoom,
            progress.processed,
            progress.ok,
            progress.failed,
            _fmt_duration(elapsed),
            rate,
        )
        return progress.ok, progress.failed

    async def _await_storage_cooldown(self) -> None:
        """Block until the S3 circuit admits its next half-open probe.

        Re-checks after each sleep rather than assuming one was enough. A timer
        that fires a hair early leaves the circuit still closed, and every tile
        in the chunk that follows is then skipped without a fetch — uvloop's
        timers have ~1 ms granularity and do wake early, which is how this was
        found. Looping on `cooldown_remaining()` is exact, not a safety margin:
        it returns 0 on precisely the condition `_in_probe_window()` tests, the
        same clock against the same deadline.

        Terminates because each pass sleeps toward a fixed deadline, and nothing
        can push that deadline out mid-wait: only a failed write re-arms the
        circuit, and callers wait at chunk boundaries where no write is in
        flight. Bounded by the circuit's cooldown cap per pass, so this sleeps
        for at most `_STORAGE_MAX_COOLDOWN_SECONDS` at a time and only while the
        circuit is actually open.
        """
        while (remaining := self._s3_circuit.cooldown_remaining()) > 0:
            await asyncio.sleep(remaining)

    async def _sweep_chunk(
        self,
        provider: BasemapProvider,
        zoom: int,
        coords: List[Tuple[int, int, int]],
        chunk_start: int,
        chunk_end: int,
        sweep_state: _ProviderSweepState,
        progress: _SweepProgress,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Dispatch and drain one chunk [chunk_start, chunk_end) of the sweep."""
        tasks = [
            asyncio.create_task(self._download_indexed(provider, idx, *coords[idx]))
            for idx in range(chunk_start, chunk_end)
        ]
        try:
            for fut in asyncio.as_completed(tasks):
                idx, z_done, x_done, y_done, outcome = await fut
                await self._handle_completion(
                    provider,
                    zoom,
                    idx,
                    (z_done, x_done, y_done),
                    outcome,
                    sweep_state,
                    progress,
                )
                if sweep_state.tripped or sweep_state.storage_abandoned:
                    # Stop consuming the moment we trip; the finally block
                    # cancels everything still in-flight so we stop hitting
                    # the unhealthy upstream immediately.
                    break
        finally:
            # Cancel any still-running tile tasks in this chunk.
            pending = [t for t in tasks if not t.done()]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    async def _handle_completion(
        self,
        provider: BasemapProvider,
        zoom: int,
        idx: int,
        tile: Tuple[int, int, int],
        outcome: "_TileOutcome",
        sweep_state: _ProviderSweepState,
        progress: _SweepProgress,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Fold one completed tile into the sweep progress + watermark."""
        z, x, y = tile
        self._update_sweep_state(sweep_state, outcome, z, x, y)
        if outcome is _TileOutcome.OK:
            progress.ok += 1
        else:
            progress.failed += 1
            if outcome is not _TileOutcome.STORAGE_SKIPPED:
                # A skipped tile was never attempted, and the sweep that
                # resumes will reach it again anyway — queueing it would just
                # write one SQLite row per tile for the whole outage.
                await self._state.add_failed(provider.provider_id, z, x, y)
        progress.processed += 1

        watermark, flushed = await self._advance_watermark(
            provider,
            zoom,
            idx,
            progress.watermark,
            progress.done_above,
            progress.last_flushed,
            progress.last_flush_time,
        )
        progress.watermark = watermark
        if flushed:
            progress.last_flushed = watermark
            progress.last_flush_time = time.monotonic()

        self._maybe_log_progress(provider, zoom, progress)

    def _maybe_log_progress(
        self, provider: BasemapProvider, zoom: int, progress: _SweepProgress
    ) -> None:
        """Emit a throttled in-zoom progress line when due."""
        now = time.monotonic()
        denom = progress.total - progress.resume_index
        pct = (progress.processed * 100 // denom) if denom > 0 else 100
        due = pct >= progress.next_pct or now >= progress.next_time
        if not (
            due
            and progress.processed < denom
            and now - progress.last_log >= _PROGRESS_MIN_INTERVAL_S
        ):
            return
        self._log_zoom_progress(
            provider,
            zoom,
            progress.resume_index + progress.processed,
            progress.total,
            now - progress.start,
        )
        while progress.next_pct <= pct:
            progress.next_pct += _PROGRESS_PCT_STEP
        progress.next_time = now + _PROGRESS_TIME_INTERVAL_S
        progress.last_log = now

    async def _advance_watermark(
        self,
        provider: BasemapProvider,
        zoom: int,
        idx: int,
        watermark: int,
        done_above: Set[int],
        last_flushed: int,
        last_flush_time: float,
    ) -> Tuple[int, bool]:
        # pylint: disable=too-many-arguments
        """
        Advance the watermark past all consecutively-completed indices and,
        if enough progress has accumulated, flush the cursor to SQLite.

        Returns `(new_watermark, flushed)`.
        """
        if idx == watermark:
            watermark += 1
            while watermark in done_above:
                done_above.remove(watermark)
                watermark += 1
        elif idx > watermark:
            done_above.add(idx)
        # idx < watermark would mean a duplicate; ignore.

        advanced = watermark - last_flushed
        elapsed_since_flush = time.monotonic() - last_flush_time
        should_flush = (
            advanced >= self._checkpoint_every
            or elapsed_since_flush >= self._checkpoint_seconds
        ) and advanced > 0

        if should_flush:
            await self._state.set_cursor(provider.provider_id, zoom, watermark)
            return watermark, True
        return watermark, False

    def _log_zoom_progress(
        self,
        provider: BasemapProvider,
        zoom: int,
        processed: int,
        total: int,
        elapsed: float,
    ) -> None:
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Emit one in-zoom progress line with rate + ETA."""
        pct = (processed * 100 // total) if total else 100
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining = max(total - processed, 0)
        eta = remaining / rate if rate > 0 else 0.0
        logger.info(
            "%s z=%d: %d/%d (%d%%) @ %.1f tiles/s, ETA %s",
            provider.provider_id,
            zoom,
            processed,
            total,
            pct,
            rate,
            _fmt_duration(eta),
        )

    async def _download_indexed(
        self, provider: BasemapProvider, idx: int, z: int, x: int, y: int
    ) -> Tuple[int, int, int, int, _TileOutcome]:
        # pylint: disable=too-many-arguments
        """Wrap `_download_and_store` so completions carry their absolute index."""
        outcome = await self._download_and_store(provider, z, x, y)
        return idx, z, x, y, outcome

    async def _download_and_store(
        self, provider: BasemapProvider, z: int, x: int, y: int
    ) -> _TileOutcome:
        """Download a single tile from the external provider and store in S3 + Redis.

        Returns the fetch outcome so the caller can drive the circuit breaker.
        Storage-side failures are reported separately from provider health —
        they say nothing about the upstream.

        Skips the upstream download entirely while the S3 circuit is open:
        fetching a tile we already know we can't persist costs provider quota
        and produces nothing.
        """
        if not self._s3_circuit.allows_write():
            return _TileOutcome.STORAGE_SKIPPED

        url = build_source_url(provider, z, x, y)
        try:
            data = await self._http.download_tile(url)
        except ProviderUnavailableError as exc:
            logger.warning(
                "Provider unavailable for tile %s/%d/%d/%d: %s",
                provider.provider_id,
                z,
                x,
                y,
                exc.cause,
            )
            return _TileOutcome.UNAVAILABLE

        if not data:
            return _TileOutcome.MISSING

        if not await self._persist_to_s3(provider, z, x, y, data):
            return _TileOutcome.STORAGE_ERROR

        await self._write_through_redis(provider, z, x, y, data)
        # The durable backup landed. A Redis hiccup leaves a cold hot-cache
        # entry that the first read repopulates, so it must not mark the sweep
        # incomplete and cost a full re-scrape.
        return _TileOutcome.OK

    async def _persist_to_s3(
        self, provider: BasemapProvider, z: int, x: int, y: int, data: bytes
    ) -> bool:
        """Write the tile to the durable bucket. False on failure.

        botocore (ClientError/BotoCoreError, incl. EndpointConnectionError)
        covers the S3 outage shapes; OSError/TimeoutError the socket-level ones.
        """
        # pylint: disable=too-many-arguments
        try:
            s3_key = S3Client.build_basemap_tile_key(provider.provider_id, z, x, y)
            await self._s3.upload_tile(s3_key, data)
        except (ClientError, BotoCoreError, asyncio.TimeoutError, OSError) as exc:
            self._log_circuit(self._s3_circuit.record_failure(), exc)
            return False
        self._log_circuit(self._s3_circuit.record_success())
        return True

    async def _write_through_redis(
        self, provider: BasemapProvider, z: int, x: int, y: int, data: bytes
    ) -> None:
        """Warm the hot cache. Best-effort: never fails the tile."""
        # pylint: disable=too-many-arguments
        if not self._redis_writes_enabled or not self._redis_circuit.allows_write():
            return
        try:
            await self._redis.store_basemap_tile(
                provider.provider_id, z, x, y, data, ttl=self._tile_ttl
            )
            await self._redis.clear_basemap_tile_miss(provider.provider_id, z, x, y)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
            self._log_circuit(self._redis_circuit.record_failure(), exc)
            return
        self._log_circuit(self._redis_circuit.record_success())

    @staticmethod
    def _log_circuit(
        transition: Optional[CircuitTransition],
        exc: Optional[BaseException] = None,
    ) -> None:
        """Emit the single line a circuit state change is worth.

        Every other outcome returns ``None`` and logs nothing, which is what
        keeps a storage outage to a handful of lines instead of one per tile.
        """
        if transition is None:
            return
        if transition.opened:
            logger.warning(
                "%s circuit opened after %d failed writes (%s) — "
                "skipping writes for %s",
                transition.name,
                transition.failures,
                exc,
                _fmt_duration(transition.cooldown),
            )
        elif transition.probe_failed:
            logger.warning(
                "%s still unavailable (%s) — next probe in %s",
                transition.name,
                exc,
                _fmt_duration(transition.cooldown),
            )
        elif transition.recovered:
            logger.info(
                "%s recovered after %s and %d failed writes — resuming",
                transition.name,
                _fmt_duration(transition.downtime),
                transition.failures,
            )
