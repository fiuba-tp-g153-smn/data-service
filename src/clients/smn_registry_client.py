"""Public ZIP fetcher for the SMN EMA station registry."""

import asyncio
import io
import logging
import zipfile
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class SmnRegistryError(Exception):
    """Registry download or unzip failed."""


class SmnRegistryClient:
    """
    Downloads and unzips the public SMN station registry.

    Different host + no auth (HTTP), so this is intentionally separate from
    `SmnApiClient` (which is HTTPS + JWT). One method only: fetch + unzip,
    returning the inner TXT body as a string. Caller decides whether to
    re-parse or skip based on its own hash check.
    """

    def __init__(
        self,
        url: str,
        timeout_seconds: float,
        max_retries: int,
    ):
        self._url = url
        self._max_retries = max_retries
        self._client = httpx.AsyncClient(timeout=timeout_seconds)

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_registry_text(self) -> str:
        """
        GET the ZIP, unzip in memory, return the single inner TXT decoded as latin-1.

        latin-1 because the source file contains tildes and "PEÑA" — UTF-8
        decoding fails on the legacy encoding the upstream uses. Raises
        `SmnRegistryError` on any failure (network, HTTP, zip, decode).
        """
        zip_bytes = await self._download()
        return self._unzip_first_text(zip_bytes)

    async def _download(self) -> bytes:
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.get(self._url)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                logger.warning(
                    "SMN registry network error (attempt %d/%d): %s",
                    attempt,
                    self._max_retries,
                    exc,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
                continue
            if response.status_code != 200:
                last_exc = SmnRegistryError(
                    f"SMN registry HTTP {response.status_code}"
                )
                logger.warning(
                    "SMN registry HTTP %d (attempt %d/%d)",
                    response.status_code,
                    attempt,
                    self._max_retries,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
                continue
            return response.content
        raise SmnRegistryError(
            f"SMN registry failed after {self._max_retries} attempts: {last_exc}"
        )

    @staticmethod
    def _unzip_first_text(zip_bytes: bytes) -> str:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
                names = archive.namelist()
                if not names:
                    raise SmnRegistryError("SMN registry ZIP is empty")
                # Pick the first file; the upstream archive contains exactly one.
                with archive.open(names[0]) as entry:
                    raw = entry.read()
        except zipfile.BadZipFile as exc:
            raise SmnRegistryError(f"SMN registry not a valid zip: {exc}") from exc

        try:
            return raw.decode("latin-1")
        except UnicodeDecodeError as exc:
            raise SmnRegistryError(
                f"SMN registry decode failed (latin-1): {exc}"
            ) from exc
