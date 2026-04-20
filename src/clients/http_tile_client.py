"""HTTP client for downloading tiles from external map providers."""

import asyncio
import contextlib
import logging
from typing import Dict, Optional
from urllib.parse import urlparse

import httpx
from tenacity import (
    AsyncRetrying,
    RetryError,
    before_sleep_log,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)


class _RetryableHttpStatus(Exception):
    """Raised for HTTP status codes worth retrying (5xx / 429)."""


class ProviderUnavailableError(Exception):
    """
    Raised when a tile URL cannot be served because the upstream provider
    appears to be down or unreachable (DNS/connection failures, timeouts,
    or exhausted retries against 5xx/429). Distinct from a permanent miss
    (404/403), which still returns `None`.

    Callers that care about provider health (the scraper's circuit breaker,
    primarily) catch this to drive their own cool-down logic. Callers that
    only care about "did I get bytes back" (the reader) catch it and
    degrade to a normal miss.
    """

    def __init__(self, url: str, cause: str):
        super().__init__(f"{url}: {cause}")
        self.url = url
        self.cause = cause


class HttpTileClient:
    """
    Async HTTP client for fetching tiles from external providers.

    Bounds concurrency via a semaphore, paces requests with a configurable
    delay, and retries transient failures (network errors, timeouts, 429s,
    5xx) using tenacity with exponential backoff + jitter. 404/403 are
    treated as permanent misses and not retried.

    Failure semantics:
      * 404 / 403 / other non-retryable non-200 → returns ``None`` (MISSING).
      * Exhausted retries, network errors, timeouts → raises
        :class:`ProviderUnavailableError` (UNAVAILABLE). Callers that don't
        care about the distinction (e.g. reader) should catch and degrade.
    """

    def __init__(
        self,
        max_concurrent: int,
        delay_ms: int,
        timeout_seconds: int,
        max_retries: int,
        per_host_concurrent: Optional[int] = None,
    ):
        # pylint: disable=too-many-arguments
        self._max_concurrent = max_concurrent
        self._delay_ms = delay_ms
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._per_host_limit = per_host_concurrent
        self._host_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._client: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        """Create the shared HTTP client."""
        if self._client:
            return
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
        )
        logger.info(
            "HTTP tile client connected (concurrency=%d, per_host=%s, "
            "retries=%d, timeout=%ds)",
            self._max_concurrent,
            self._per_host_limit if self._per_host_limit else "off",
            self._max_retries,
            self._timeout,
        )

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("HTTP tile client closed")

    def _host_semaphore(self, url: str) -> Optional[asyncio.Semaphore]:
        """Return a per-host semaphore for the URL, lazily allocated."""
        if not self._per_host_limit:
            return None
        host = urlparse(url).netloc
        sem = self._host_semaphores.get(host)
        if sem is None:
            sem = asyncio.Semaphore(self._per_host_limit)
            self._host_semaphores[host] = sem
        return sem

    def _overall_budget_seconds(self) -> float:
        """Cap on total time a single download_tile call may hold the semaphore."""
        backoff_total = sum(2**i for i in range(self._max_retries - 1))
        return self._timeout * self._max_retries + backoff_total + 1.0

    async def download_tile(self, url: str) -> Optional[bytes]:
        """
        Download a tile from a URL with rate limiting and retry.

        Returns raw bytes on success, ``None`` for a permanent miss
        (404/403 or other non-retryable non-200). Raises
        :class:`ProviderUnavailableError` when the upstream looks
        unreachable (exhausted retries, network errors, timeouts).
        """
        if not self._client:
            raise RuntimeError("HTTP tile client not connected")

        host_sem = self._host_semaphore(url)
        # Acquire host budget before the global backstop. Consistent order
        # across callers — no deadlock risk (only two sems, one always wider).
        host_guard = host_sem if host_sem is not None else contextlib.nullcontext()
        async with host_guard, self._semaphore:
            try:
                async for attempt in AsyncRetrying(
                    stop=(
                        stop_after_attempt(self._max_retries)
                        | stop_after_delay(self._overall_budget_seconds())
                    ),
                    wait=wait_exponential_jitter(initial=1, max=30),
                    retry=retry_if_exception_type(
                        (httpx.HTTPError, asyncio.TimeoutError, _RetryableHttpStatus)
                    ),
                    before_sleep=before_sleep_log(logger, logging.WARNING),
                    reraise=True,
                ):
                    with attempt:
                        return await self._fetch_once(url)
            except _RetryableHttpStatus as exc:
                logger.error("Gave up on retryable status for %s: %s", url, exc)
                raise ProviderUnavailableError(url, str(exc)) from exc
            except (httpx.HTTPError, asyncio.TimeoutError, RetryError) as exc:
                logger.error("Failed to download tile %s: %s", url, exc)
                raise ProviderUnavailableError(url, str(exc)) from exc

        return None

    async def _fetch_once(self, url: str) -> Optional[bytes]:
        """Single HTTP attempt. Raises for retryable conditions, returns None for 404/403."""
        assert self._client is not None
        response = await self._client.get(url)

        if response.status_code == 200:
            data = response.content
            if self._delay_ms > 0:
                await asyncio.sleep(self._delay_ms / 1000.0)
            return data

        if response.status_code in (404, 403):
            return None

        if response.status_code == 429 or response.status_code >= 500:
            raise _RetryableHttpStatus(f"HTTP {response.status_code} for {url}")

        logger.warning("Non-retryable HTTP %d for %s", response.status_code, url)
        return None
