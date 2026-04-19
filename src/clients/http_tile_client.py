"""HTTP client for downloading tiles from external map providers."""

import asyncio
import logging
from typing import Optional

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


class HttpTileClient:
    """
    Async HTTP client for fetching tiles from external providers.

    Bounds concurrency via a semaphore, paces requests with a configurable
    delay, and retries transient failures (network errors, timeouts, 429s,
    5xx) using tenacity with exponential backoff + jitter. 404/403 are
    treated as permanent misses and not retried.
    """

    def __init__(
        self,
        max_concurrent: int,
        delay_ms: int,
        timeout_seconds: int,
        max_retries: int,
    ):
        self._max_concurrent = max_concurrent
        self._delay_ms = delay_ms
        self._timeout = timeout_seconds
        self._max_retries = max_retries
        self._semaphore = asyncio.Semaphore(max_concurrent)
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
            "HTTP tile client connected (concurrency=%d, retries=%d, timeout=%ds)",
            self._max_concurrent,
            self._max_retries,
            self._timeout,
        )

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("HTTP tile client closed")

    def _overall_budget_seconds(self) -> float:
        """Cap on total time a single download_tile call may hold the semaphore."""
        backoff_total = sum(2**i for i in range(self._max_retries - 1))
        return self._timeout * self._max_retries + backoff_total + 1.0

    async def download_tile(self, url: str) -> Optional[bytes]:
        """
        Download a tile from a URL with rate limiting and retry.

        Returns raw bytes on success, None on permanent failure (404/403,
        exhausted retries, or budget exceeded).
        """
        if not self._client:
            raise RuntimeError("HTTP tile client not connected")

        async with self._semaphore:
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
            except _RetryableHttpStatus:
                logger.error("Gave up on retryable status for %s", url)
                return None
            except (httpx.HTTPError, asyncio.TimeoutError, RetryError) as exc:
                logger.error("Failed to download tile %s: %s", url, exc)
                return None

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
