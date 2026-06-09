"""Base class for per-product background sync loops.

Each product (satellite, radar, ECMWF-TP, ECMWF-MSLP, WRF) runs in its OWN
independent, flock-gated loop with its OWN S3 client (independent download
budget), interval, and watchdog timeout — so no product can monopolize another
product's scheduling or S3 budget. A cycle that exceeds the watchdog is
cancelled, recorded as a ``timeout`` outcome, and retried next cycle; because a
unit (tileset / period / WRF step) is indexed only after it fully downloads, a
preempted cycle is always safe to resume from its frontier.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from logging import Logger
from typing import Awaitable, Optional, Tuple

from clients.metrics_store import MetricsStore
from clients.redis_client import RedisClient
from clients.s3_client import S3Client
from services.base_sync_service import BaseSyncService
from settings import Settings

logger = logging.getLogger(__name__)


class DomainSyncService(BaseSyncService):  # pylint: disable=abstract-method
    """One independent background sync loop for a single product/domain.

    Subclasses set the domain logic by implementing ``_run_sync`` (typically a
    single ``_run_single_domain(self._sync_x())`` call) plus the ``_sync_x``
    coroutine that returns ``(downloaded, errors)``.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        domain: str,
        lock_path: str,
        interval: int,
        timeout: int,
        s3_concurrency: int,
        service_name: str,
    ):
        # pylint: disable=too-many-arguments
        super().__init__(
            settings=settings,
            sync_interval=interval,
            service_name=service_name,
            min_sleep=settings.sync_min_sleep_seconds,
        )
        self._domain = domain
        self._lock_path = lock_path
        self._timeout = timeout
        self._s3_concurrency = s3_concurrency
        self._client: Optional[S3Client] = None
        self._redis_client: Optional[RedisClient] = None
        self._metrics_store: Optional[MetricsStore] = None
        self._consecutive_failures = 0
        self._total_cycles = 0

    def set_redis_client(self, redis_client: RedisClient) -> None:
        """Set the Redis client (called during app startup)."""
        self._redis_client = redis_client

    def set_metrics_store(self, metrics_store: MetricsStore) -> None:
        """Set the metrics store for per-domain cycle history (app startup)."""
        self._metrics_store = metrics_store

    def _get_lock_path(self) -> str:
        return self._lock_path

    def _on_sync_error(self, error: Exception) -> None:
        self._consecutive_failures += 1

    def _log_started(self, app_logger: Logger) -> None:
        app_logger.info(
            "%s started (lock acquired). Interval: %ss, timeout: %ss, "
            "S3 concurrency: %s",
            self._service_name,
            self._sync_interval,
            self._timeout,
            self._s3_concurrency,
        )

    def _create_client(self) -> S3Client:
        """Create this loop's own S3 client (independent download budget)."""
        s = self._settings
        return S3Client(
            endpoint=s.s3_tiles_data_endpoint,
            access_key=s.s3_tiles_data_access_key,
            secret_key=s.s3_tiles_data_secret_key,
            bucket=s.s3_tiles_data_bucket_name,
            max_concurrent_downloads=self._s3_concurrency,
            secure=s.s3_tiles_data_secure,
            connect_timeout=s.s3_connect_timeout_seconds,
            read_timeout=s.s3_read_timeout_seconds,
            max_attempts=s.s3_max_attempts,
        )

    async def _record_cycle(
        self,
        started_at: str,
        finished_at: str,
        duration_ms: int,
        downloaded: int,
        errors: int,
        outcome: Optional[str] = None,
    ) -> None:
        # pylint: disable=too-many-arguments
        """Persist one cycle row for this domain. Never lets metrics break sync."""
        if self._metrics_store is None:
            return
        if outcome is None:
            outcome = "error" if errors else "ok"
        try:
            await self._metrics_store.record_sync_cycle(
                self._domain,
                started_at,
                finished_at,
                duration_ms,
                downloaded,
                errors,
                outcome,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Failed to record sync metrics for %s", self._domain)

    async def _timed_domain(self, coro: Awaitable[Tuple[int, int]]) -> Tuple[int, int]:
        """Run the domain sync under a watchdog and record a cycle row.

        A timeout is recorded as a ``timeout`` outcome and the loop continues —
        the next cycle resumes the un-indexed work. ``CancelledError`` (from
        shutdown) is NOT caught, so ``stop()`` cancels the loop cleanly.
        """
        start = datetime.now(timezone.utc)
        try:
            downloaded, errors = await asyncio.wait_for(coro, timeout=self._timeout)
            outcome: Optional[str] = None
        except asyncio.TimeoutError:
            downloaded, errors, outcome = 0, 1, "timeout"
            logger.warning(
                "%s sync timed out after %ss; recording timeout and continuing",
                self._domain,
                self._timeout,
            )
        end = datetime.now(timezone.utc)
        duration_ms = int((end - start).total_seconds() * 1000)
        await self._record_cycle(
            start.isoformat(),
            end.isoformat(),
            duration_ms,
            downloaded,
            errors,
            outcome,
        )
        return downloaded, errors

    async def _run_single_domain(self, coro: Awaitable[Tuple[int, int]]) -> None:
        """Template ``_run_sync`` body: guard, run with watchdog, write status."""
        if not self._settings.is_s3_configured():
            logger.warning(
                "S3 not configured; %s sync skipped this cycle", self._domain
            )
            return
        if self._client is None:
            self._client = self._create_client()
        if self._redis_client is None:
            return

        sync_start = time.time()
        await self._redis_client.update_domain_sync_status(
            self._domain, {"is_running": "true", "last_sync_start": str(sync_start)}
        )

        downloaded, errors = await self._timed_domain(coro)

        self._total_cycles += 1
        if errors == 0:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1

        sync_end = time.time()
        await self._redis_client.update_domain_sync_status(
            self._domain,
            {
                "is_running": "false",
                "last_sync_end": str(sync_end),
                "last_sync_duration_ms": str(int((sync_end - sync_start) * 1000)),
                "last_sync_downloaded": str(downloaded),
                "last_sync_errors": str(errors),
                "consecutive_failures": str(self._consecutive_failures),
                "total_cycles": str(self._total_cycles),
            },
        )
        if self._consecutive_failures > 0:
            logger.warning(
                "%s sync has %d consecutive failure(s)",
                self._domain,
                self._consecutive_failures,
            )
