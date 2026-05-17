"""HTTP client for the SMN weather API with JWT token cache + refresh-on-401."""

import asyncio
import logging
import time
from typing import Any, List, Optional

import httpx

logger = logging.getLogger(__name__)


class SmnApiError(Exception):
    """SMN API call failed after retries / refresh attempts."""


class SmnApiClient:
    """
    Thin async client around the SMN `/weather/station` + `/api-token/auth` endpoints.

    Owns a single `httpx.AsyncClient` (connection pool) and an in-memory JWT
    cache. On every `fetch_current_weather_stations()` call the client uses the
    cached token; on a 401 it refreshes once and retries. The cache TTL bounds
    how often we proactively re-auth even when SMN hasn't rotated the token.

    Not thread-safe across event loops; meant to be a process-wide singleton
    instantiated in the FastAPI `lifespan`.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout_seconds: float,
        max_retries: int,
        token_cache_ttl_seconds: int,
    ):
        # pylint: disable=too-many-arguments,too-many-positional-arguments
        self._base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._max_retries = max_retries
        self._token_cache_ttl = token_cache_ttl_seconds
        self._client = httpx.AsyncClient(timeout=timeout_seconds)
        self._token: Optional[str] = None
        self._token_minted_at: float = 0.0
        # Serialize concurrent refreshes so a burst of 401s mints a single new
        # token instead of N parallel /api-token/auth calls.
        self._refresh_lock = asyncio.Lock()

    async def close(self) -> None:
        """Release the underlying httpx pool."""
        await self._client.aclose()

    async def fetch_current_weather_stations(self) -> List[dict[str, Any]]:
        """
        GET {base}/weather/station and return the parsed JSON array.

        Raises `SmnApiError` on any non-recoverable failure (auth still bad
        after a refresh, retries exhausted, malformed payload).
        """
        token = await self._get_token()
        try:
            return await self._get_stations(token)
        except _Unauthorized:
            logger.info(
                "SMN token rejected mid-flight; refreshing and retrying once"
            )
            token = await self._refresh_token()
            return await self._get_stations(token)

    async def _get_stations(self, token: str) -> List[dict[str, Any]]:
        url = f"{self._base_url}/weather/station"
        headers = {"api_key": token, "Accept": "application/json"}
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = await self._client.get(url, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                logger.warning(
                    "SMN /weather/station network error (attempt %d/%d): %s",
                    attempt,
                    self._max_retries,
                    exc,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
                continue

            if response.status_code == 401:
                raise _Unauthorized()
            if 500 <= response.status_code < 600:
                last_exc = SmnApiError(
                    f"SMN /weather/station upstream {response.status_code}"
                )
                logger.warning(
                    "SMN /weather/station %d (attempt %d/%d)",
                    response.status_code,
                    attempt,
                    self._max_retries,
                )
                await asyncio.sleep(min(2 ** (attempt - 1), 10))
                continue
            if response.status_code != 200:
                raise SmnApiError(
                    f"SMN /weather/station unexpected status "
                    f"{response.status_code}: {response.text[:200]}"
                )

            payload = response.json()
            if not isinstance(payload, list):
                raise SmnApiError(
                    f"SMN /weather/station returned non-list payload: "
                    f"{type(payload).__name__}"
                )
            return payload

        raise SmnApiError(
            f"SMN /weather/station failed after {self._max_retries} attempts: "
            f"{last_exc}"
        )

    async def _get_token(self) -> str:
        """Return a cached token if still fresh, else mint a new one."""
        if self._token and (time.monotonic() - self._token_minted_at) < self._token_cache_ttl:
            return self._token
        return await self._refresh_token()

    async def _refresh_token(self) -> str:
        """Mint a new JWT via POST /api-token/auth; serialize concurrent refreshes."""
        async with self._refresh_lock:
            # Double-checked: another waiter may have just refreshed.
            if (
                self._token
                and (time.monotonic() - self._token_minted_at) < self._token_cache_ttl
            ):
                return self._token

            url = f"{self._base_url}/api-token/auth"
            try:
                response = await self._client.post(
                    url,
                    json={"username": self._username, "password": self._password},
                    headers={"Accept": "application/json"},
                )
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                raise SmnApiError(f"SMN /api-token/auth network error: {exc}") from exc

            if response.status_code != 200:
                raise SmnApiError(
                    f"SMN /api-token/auth returned {response.status_code}: "
                    f"{response.text[:200]}"
                )

            payload = response.json()
            token = payload.get("token") if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise SmnApiError(
                    f"SMN /api-token/auth response missing 'token' field: "
                    f"{type(payload).__name__}"
                )

            self._token = token
            self._token_minted_at = time.monotonic()
            logger.info("SMN token refreshed")
            return token


class _Unauthorized(Exception):
    """Internal marker raised on 401 so the caller can trigger one refresh."""
