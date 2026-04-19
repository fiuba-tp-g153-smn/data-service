"""HTTP client for downloading tiles from external map providers."""

import asyncio
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class HttpTileClient:
    """
    Async HTTP client for fetching tiles from external providers.

    Provides rate limiting via semaphore and configurable delay,
    retry with exponential backoff, and connection pooling.
    """

    def __init__(
        self,
        max_concurrent: int = 3,
        delay_ms: int = 200,
        timeout_seconds: int = 10,
        max_retries: int = 3,
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
            headers={"User-Agent": "data-service-basemap-scraper/1.0"},
            follow_redirects=True,
        )
        logger.info("HTTP tile client connected (concurrency=%d)", self._max_concurrent)

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("HTTP tile client closed")

    async def download_tile(self, url: str) -> Optional[bytes]:
        """
        Download a tile from a URL with rate limiting and retry.

        Returns raw bytes on success, None on permanent failure.
        """
        if not self._client:
            raise RuntimeError("HTTP tile client not connected")

        async with self._semaphore:
            for attempt in range(self._max_retries):
                try:
                    response = await self._client.get(url)

                    if response.status_code == 200:
                        data = response.content
                        if self._delay_ms > 0:
                            await asyncio.sleep(self._delay_ms / 1000.0)
                        return data

                    if response.status_code == 429:
                        backoff = 2 ** (attempt + 1)
                        logger.warning(
                            "Rate limited on %s, backing off %ds", url, backoff
                        )
                        await asyncio.sleep(backoff)
                        continue

                    if response.status_code in (404, 403):
                        return None

                    logger.warning(
                        "HTTP %d fetching %s (attempt %d/%d)",
                        response.status_code,
                        url,
                        attempt + 1,
                        self._max_retries,
                    )
                except (httpx.HTTPError, asyncio.TimeoutError) as exc:
                    logger.warning(
                        "Error fetching %s (attempt %d/%d): %s",
                        url,
                        attempt + 1,
                        self._max_retries,
                        exc,
                    )

                if attempt < self._max_retries - 1:
                    await asyncio.sleep(2**attempt)

            logger.error(
                "Failed to download tile after %d attempts: %s", self._max_retries, url
            )
            return None
