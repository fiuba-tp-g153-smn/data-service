"""
Base class for background sync services.

Provides shared lock acquisition, task lifecycle management,
and fixed-interval sync loop for sync service implementations.
"""

import asyncio
import fcntl
import logging
import time
from logging import Logger
from typing import Optional

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
    ):
        self._settings = settings
        self._sync_interval = sync_interval
        self._service_name = service_name
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._lock_file_handle = None

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

    async def start(self, app_logger: Logger) -> None:
        """Start the background sync task."""
        if self._running:
            app_logger.warning("%s is already running", self._service_name)
            return

        if not self._pre_start_check(app_logger):
            return

        # Attempt to acquire file lock (only one worker syncs)
        try:
            self._lock_file_handle = open(  # pylint: disable=consider-using-with
                self._get_lock_path(), "w", encoding="utf-8"
            )
            fcntl.lockf(self._lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            app_logger.info(
                "%s disabled (another worker is active).", self._service_name
            )
            if self._lock_file_handle:
                self._lock_file_handle.close()
                self._lock_file_handle = None
            return

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
                sleep_time = max(0, self._sync_interval - elapsed)
                await asyncio.sleep(sleep_time)

    async def _run_sync(self) -> None:
        """Execute a single sync cycle. Must be implemented by subclasses."""
        raise NotImplementedError
