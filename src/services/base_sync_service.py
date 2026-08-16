"""
Base class for background sync services.

Provides shared lock acquisition, task lifecycle management,
and fixed-interval sync loop for sync service implementations.
"""

import asyncio
import errno
import fcntl
import logging
import time
from logging import Logger
from typing import IO, Optional

from settings import Settings

logger = logging.getLogger(__name__)


class BaseSyncService:
    """
    Base class for background sync services with file-based locking.

    Handles:
    - File lock so only one worker syncs across multiple uvicorn workers
    - Async task creation and cancellation
    - Fixed-interval sync loop with error handling

    Subclasses must implement:
    - _run_sync(): Execute a single sync cycle
    """

    def __init__(
        self,
        settings: Settings,
        sync_interval: int,
        service_name: str,
        min_sleep: int = 0,
    ):
        self._settings = settings
        self._sync_interval = sync_interval
        self._service_name = service_name
        # Floor on the inter-cycle sleep. Defaults to 0 so subclasses that
        # supply their own pacing (e.g. the basemap scraper's _compute_next_sleep
        # 60s floor) are unaffected unless they opt in.
        self._min_sleep = min_sleep
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._lock_file_handle: Optional[IO[str]] = None

    def _get_lock_path(self) -> str:
        """Return the lock file path. Must be implemented by subclasses."""
        raise NotImplementedError

    def _pre_start_check(  # pylint: disable=unused-argument
        self, app_logger: Logger
    ) -> bool:
        """Override to run checks before starting. Return False to abort."""
        return True

    def _log_started(self, app_logger: Logger) -> None:
        """Override to log a custom start message."""
        app_logger.info("%s started", self._service_name)

    def _on_sync_error(self, error: Exception) -> None:
        """Override to handle sync loop errors (e.g., track consecutive failures)."""

    async def _compute_next_sleep(self, default: float) -> float:
        """Override to shorten sleep when the default would overshoot pending work."""
        return default

    async def start(self, app_logger: Logger) -> None:
        """Start the background sync task."""
        if self._running:
            app_logger.warning("%s is already running", self._service_name)
            return

        if not self._pre_start_check(app_logger):
            return

        # Attempt to acquire the file lock (only one worker syncs). Opening the
        # lock file and taking the lock are handled separately so a broken lock
        # subsystem is never mistaken for either an unwritable path or ordinary
        # single-writer contention.
        try:
            self._lock_file_handle = open(  # pylint: disable=consider-using-with
                self._get_lock_path(), "w", encoding="utf-8"
            )
        except OSError as exc:
            app_logger.error(
                "%s: cannot open lock file %s (%s); sync not started",
                self._service_name,
                self._get_lock_path(),
                exc,
            )
            raise

        try:
            fcntl.lockf(self._lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock_file_handle.close()
            self._lock_file_handle = None
            # EAGAIN/EACCES is the normal "already held by a sibling worker"
            # result of a non-blocking lock — the expected single-writer path.
            # Any other errno (ENOLCK, EINVAL, an NFS mount without lockd) means
            # the locking subsystem itself is broken: fail loudly so the
            # orchestrator restarts/alerts, rather than silently running with no
            # background sync.
            if exc.errno in (errno.EAGAIN, errno.EACCES):
                app_logger.info(
                    "%s disabled (another worker is active).", self._service_name
                )
                return
            app_logger.error(
                "%s: lock subsystem error on %s (errno=%s: %s); sync not started",
                self._service_name,
                self._get_lock_path(),
                exc.errno,
                exc,
            )
            raise

        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        self._log_started(app_logger)

    async def stop(self, app_logger: Logger) -> None:
        """Stop the background sync task."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Release lock
        if self._lock_file_handle:
            try:
                fcntl.lockf(self._lock_file_handle, fcntl.LOCK_UN)
                self._lock_file_handle.close()
            except Exception as e:  # pylint: disable=broad-exception-caught
                app_logger.error("Error releasing %s lock: %s", self._service_name, e)
            self._lock_file_handle = None

        app_logger.info("%s stopped", self._service_name)

    async def _sync_loop(self) -> None:
        """Main sync loop with fixed-interval scheduling."""
        while self._running:
            cycle_start = time.monotonic()
            try:
                await self._run_sync()
            except asyncio.CancelledError:
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("Error in %s sync loop: %s", self._service_name, e)
                self._on_sync_error(e)

            if self._running:
                elapsed = time.monotonic() - cycle_start
                if elapsed > self._sync_interval:
                    logger.warning(
                        "%s cycle overran interval: %.1fs > %ss "
                        "(flooring next sleep to %ss)",
                        self._service_name,
                        elapsed,
                        self._sync_interval,
                        self._min_sleep,
                    )
                default_sleep = max(0, self._sync_interval - elapsed)
                sleep_time = await self._compute_next_sleep(default_sleep)
                # Always yield at least _min_sleep so an overrunning cycle can't
                # busy-loop the event loop at 0s sleep.
                sleep_time = max(self._min_sleep, sleep_time)
                await asyncio.sleep(sleep_time)

    async def _run_sync(self) -> None:
        """Execute a single sync cycle. Must be implemented by subclasses."""
        raise NotImplementedError
